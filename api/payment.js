// /api/payment.js — NeyoMarket Unified Payment + Orders + Disputes Engine
// Replaces: payment.js + orders.js + disputes.js (merged to stay under Vercel 12-function limit)
//
// Route map — all via ?action= query parameter:
//
//   PAYMENT & ESCROW
//   POST ?action=confirm          — verify with Paystack, save order, split commission
//   POST ?action=dvc-release      — seller enters 6-digit code to release physical escrow
//   POST ?action=refund           — admin triggers Paystack refund for disputed order
//   POST ?action=webhook          — Paystack charge.success fallback webhook
//   GET  ?action=order            — fetch single order status
//
//   ORDERS (replaces /api/orders)
//   GET  ?action=orders           — list orders (?userId= for buyer, ?admin=true for all)
//   POST ?action=orders           — create a new order record
//   PATCH ?action=orders          — update order fields (status, collected, disputed, etc.)
//   DELETE ?action=orders         — delete an order by id
//
//   DISPUTES (replaces /api/disputes)
//   GET  ?action=disputes         — list disputed orders (?userId= or ?admin=true)
//   POST ?action=disputes         — buyer raises a dispute with a reason
//   PATCH ?action=disputes        — admin resolves dispute (resolve_seller|resolve_buyer|close)
//
// Commission model:
//   With valid affiliate, digital  → Seller 80%, Platform 15%, Affiliate 5%
//   With valid affiliate, physical → Seller 88%, Platform  7%, Affiliate 5%
//   No referral, digital           → Seller 90%, Platform 10%, Affiliate  0%
//   No referral, physical          → Seller 95%, Platform  5%, Affiliate  0%
//
// Frontend call changes needed:
//   /api/orders  → /api/payment?action=orders
//   /api/disputes → /api/payment?action=disputes

'use strict';

const { neon } = require('@neondatabase/serverless');
const crypto   = require('crypto');

const sql         = neon(process.env.DATABASE_URL);
const PSK         = process.env.PAYSTACK_SECRET_KEY;
const ADMIN_EMAIL = process.env.ADMIN_EMAIL || 'admin@neyomarket.com';

/* ─────────────────────────────────────────────
   SHARED HELPERS
───────────────────────────────────────────── */
function cors(res) {
  res.setHeader('Access-Control-Allow-Origin',  '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PATCH, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, x-paystack-signature');
  res.setHeader('X-Content-Type-Options',       'nosniff');
}

function jsonErr(res, status, msg, detail) {
  return res.status(status).json({ ok: false, error: msg, detail: detail || null });
}

function safeJson(val, fallback) {
  if (!val) return fallback;
  if (typeof val === 'object') return val;
  try { return JSON.parse(val); } catch (e) { return fallback; }
}

function toOrder(r) {
  return {
    id:            r.id,
    userId:        r.user_id,
    customer:      safeJson(r.customer, {}),
    items:         safeJson(r.items, []),
    total:         parseFloat(r.total         || 0),
    platformFee:   parseFloat(r.platform_fee  || 0),
    sellerPayout:  parseFloat(r.seller_payout || 0),
    affiliateFee:  parseFloat(r.affiliate_fee || 0),
    affCode:       r.aff_code       || null,
    status:        r.status         || 'pending',
    collected:     r.collected      || false,
    collectedAt:   r.collected_at   || null,
    disputed:      r.disputed       || false,
    disputeReason: r.dispute_reason || null,
    deliveryCode:  r.delivery_code  || null,
    fileUrl:       r.file_url       || null,
    mode:          r.mode           || 'standard',
    date:          r.date || (r.created_at ? new Date(r.created_at).toLocaleDateString() : ''),
    ref:           r.ref            || '',
    shipping:      safeJson(r.shipping, null),
    createdAt:     r.created_at     || null
  };
}

/* Deterministic 6-digit DVC — MUST match index.html generateDVC() exactly */
function generateDVC(orderId) {
  let hash = 0;
  const str = String(orderId);
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) - hash) + str.charCodeAt(i);
    hash |= 0;
  }
  return String(Math.abs(hash) % 900000 + 100000);
}

/* Tiered commission split — adjusts based on seller membership tier */
function computeSplit(total, hasPhysical, hasValidAff, membershipTier) {
  /* Base platform rates by membership tier */
  const tierRates = {
    free:     { digital: 0.10, physical: 0.05 },
    starter:  { digital: 0.08, physical: 0.04 },
    pro:      { digital: 0.06, physical: 0.03 },
    business: { digital: 0.04, physical: 0.02 },
  };
  const tier  = tierRates[membershipTier] || tierRates.free;
  const baseRate     = hasPhysical ? tier.physical : tier.digital;
  const affiliateRate = hasValidAff ? 0.05 : 0;
  /* If affiliate commission would exceed base, cap platform at 1% */
  const platformRate = Math.max(0.01, baseRate - (hasValidAff ? 0.02 : 0));
  const sellerRate   = 1 - platformRate - affiliateRate;
  const platformFee  = Math.round(total * platformRate);
  const affiliateFee = Math.round(total * affiliateRate);
  const sellerPayout = Math.round(total * sellerRate);
  return { platformFee, affiliateFee, sellerPayout, platformRate };
}

/* Write to admin_transactions — non-fatal if table missing */
async function recordAdminTx(params) {
  try {
    /* Pre-compute conditionals — Neon ternary rule: no ternaries inside sql`` */
    const _orderId      = String(params.orderId);
    const _total        = parseFloat(params.total        || 0);
    const _platformFee  = parseFloat(params.platformFee  || 0);
    const _sellerPayout = parseFloat(params.sellerPayout || 0);
    const _affiliateFee = parseFloat(params.affiliateFee || 0);
    const _affCode      = params.affCode  ? String(params.affCode)  : null;
    const _sellerId     = params.sellerId ? String(params.sellerId) : null;
    const _type         = params.type || 'payment';

    await sql`
      INSERT INTO admin_transactions (
        order_id, total, platform_fee, seller_payout,
        affiliate_fee, aff_code, seller_id, released_by, type, created_at
      ) VALUES (
        ${_orderId},
        ${_total},
        ${_platformFee},
        ${_sellerPayout},
        ${_affiliateFee},
        ${_affCode},
        ${_sellerId},
        ${'payment'},
        ${_type},
        NOW()
      )
      ON CONFLICT (order_id) DO NOTHING
    `;
  } catch (e) {
    if (e.message && e.message.includes('does not exist')) {
      console.warn('[payment] admin_transactions table missing — run migration.');
    } else {
      console.error('[payment] recordAdminTx (non-fatal):', e.message);
    }
  }
}

/* Verify a payment reference with Paystack */
async function verifyPaystackPayment(reference) {
  try {
    const r    = await fetch('https://api.paystack.co/transaction/verify/' + encodeURIComponent(reference), {
      headers: { 'Authorization': 'Bearer ' + PSK }
    });
    const text = await r.text();
    if (!text || !text.trim()) return null;
    const data = JSON.parse(text);
    return (data.status === true && data.data) ? data.data : null;
  } catch (e) {
    console.error('[payment] Paystack verify error:', e.message);
    return null;
  }
}

/* ─────────────────────────────────────────────
   MAIN HANDLER
───────────────────────────────────────────── */
module.exports = async function handler(req, res) {
  cors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action || '';

  /* ══════════════════════════════════════════════════════════════════
     USERS — GET ?action=users  (replaces /api/users which was deleted)
     Admin: all users. Buyer/Seller: own record only.
  ══════════════════════════════════════════════════════════════════ */
  if (action === 'users' && req.method === 'GET') {
    try {
      const userId  = req.query.userId;
      const isAdmin = req.query.admin === 'true';
      let rows;
      if (isAdmin) {
        rows = await sql`
          SELECT id, name, email, role, phone,
                 seller_balance, admin_balance, aff_code,
                 kyc_status, kyc_type, is_verified,
                 subaccount_code, created_at
          FROM users ORDER BY created_at DESC LIMIT 500
        `;
      } else if (userId) {
        rows = await sql`
          SELECT id, name, email, role, phone,
                 seller_balance, admin_balance, aff_code,
                 kyc_status, kyc_type, is_verified,
                 subaccount_code, created_at
          FROM users WHERE id = ${String(userId)} LIMIT 1
        `;
      } else {
        return jsonErr(res, 400, 'userId or ?admin=true required.');
      }
      const users = rows.map(function(r) {
        return {
          id:             r.id,
          name:           r.name           || '',
          email:          r.email          || '',
          role:           r.role           || 'buyer',
          phone:          r.phone          || '',
          sellerBalance:  parseFloat(r.seller_balance || 0),
          adminBalance:   parseFloat(r.admin_balance  || 0),
          affCode:        r.aff_code       || null,
          kycStatus:      r.kyc_status     || null,
          kycType:        r.kyc_type       || null,
          isVerified:     r.is_verified    || false,
          subaccountCode: r.subaccount_code|| null,
          createdAt:      r.created_at     || null
        };
      });
      return res.status(200).json({ ok: true, users });
    } catch (err) {
      console.error('[payment/users GET]', err.message);
      return jsonErr(res, 500, 'Could not fetch users.', err.message);
    }
  }

  /* USERS — PATCH ?action=users  (approve KYC, update role, adjust balance) */
  if (action === 'users' && req.method === 'PATCH') {
    try {
      const body = req.body || {};
      if (!body.id) return jsonErr(res, 400, 'User id required.');
      const uid         = String(body.id);
      const newRole     = body.role          !== undefined ? String(body.role)               : null;
      const newKyc      = body.kycStatus     !== undefined ? String(body.kycStatus)          : null;
      const newVerified = body.isVerified    !== undefined ? Boolean(body.isVerified)        : null;
      const newBalance  = body.sellerBalance !== undefined ? parseFloat(body.sellerBalance)  : null;
      await sql`
        UPDATE users SET
          role           = COALESCE(${newRole},     role),
          kyc_status     = COALESCE(${newKyc},      kyc_status),
          is_verified    = COALESCE(${newVerified}, is_verified),
          seller_balance = COALESCE(${newBalance},  seller_balance)
        WHERE id = ${uid}
      `;
      return res.status(200).json({ ok: true });
    } catch (err) {
      console.error('[payment/users PATCH]', err.message);
      return jsonErr(res, 500, 'Could not update user.', err.message);
    }
  }

  /* ══════════════════════════════════════════════════════════════════
     ORDERS — GET ?action=orders
     ?userId=<id>   → buyer's own orders only
     ?admin=true    → all orders (admin only)
  ══════════════════════════════════════════════════════════════════ */
  if (action === 'orders' && req.method === 'GET') {
    try {
      const userId   = req.query.userId;
      const sellerId = req.query.sellerId;
      const isAdmin  = req.query.admin === 'true';
      const email    = req.query.email;
      let rows;

      if (isAdmin) {
        rows = await sql`SELECT * FROM orders ORDER BY created_at DESC LIMIT 500`;
      } else if (sellerId) {
        rows = await sql`
          SELECT * FROM orders
          WHERE items::text LIKE ${'%"sellerId":"' + String(sellerId) + '"%'}
             OR items::text LIKE ${'%"sellerId":' + String(sellerId) + '%'}
          ORDER BY created_at DESC LIMIT 200
        `;
      } else if (userId) {
        /* STRICT: Only return orders where user_id matches the requester */
        /* DO NOT use OR with email — this leaks orders to other users */
        rows = await sql`
          SELECT * FROM orders
          WHERE user_id = ${String(userId)}
          ORDER BY created_at DESC
        `;
      } else {
        return jsonErr(res, 400, 'userId is required. Use ?userId=<id> or ?admin=true');
      }

      return res.status(200).json({ ok: true, orders: rows.map(toOrder) });
    } catch (err) {
      console.error('[payment/orders GET]', err.message);
      return jsonErr(res, 500, 'Could not fetch orders.', err.message);
    }
  }

  /* ══════════════════════════════════════════════════════════════════
     ORDERS — POST ?action=orders
     Create a new order record. Generates delivery_code automatically.
  ══════════════════════════════════════════════════════════════════ */
  if (action === 'orders' && req.method === 'POST') {
    try {
      const o = req.body || {};
      if (!o.id || !o.total) return jsonErr(res, 400, 'id and total are required.');

      const deliveryCode = generateDVC(String(o.id));
      const affCode    = (o.affCode && String(o.affCode).trim().length > 2)
        ? String(o.affCode).trim() : null;
      const orderCurrency = ['NGN','USD','GBP','EUR','CAD','GHS'].includes(o.currency)
        ? o.currency : 'NGN';

      await sql`
        INSERT INTO orders (
          id, user_id, customer, items, total, amount, platform_fee, seller_payout,
          affiliate_fee, aff_code, seller_id, status, collected, mode, ref,
          shipping, delivery_code, file_url, date, created_at, currency
        ) VALUES (
          ${String(o.id)},
          ${String(o.userId || '')},
          ${JSON.stringify(o.customer || {})},
          ${JSON.stringify(o.items    || [])},
          ${parseFloat(o.total)},
          ${parseFloat(o.total)},
          ${parseFloat(o.platformFee  || 0)},
          ${parseFloat(o.sellerPayout || 0)},
          ${parseFloat(o.affiliateFee || 0)},
          ${affCode},
          ${o.sellerId ? parseInt(o.sellerId) : null},
          ${o.status || 'paid'},
          ${false},
          ${o.mode   || 'standard'},
          ${o.ref    || ''},
          ${JSON.stringify(o.shipping || null)},
          ${deliveryCode},
          ${o.fileUrl || null},
          ${new Date().toLocaleDateString()},
          NOW(),
          ${orderCurrency}
        )
        ON CONFLICT (id) DO UPDATE SET
          status        = EXCLUDED.status,
          ref           = EXCLUDED.ref,
          delivery_code = EXCLUDED.delivery_code
      `;
      /* ── WhatsApp vendor notification ── */
      try {
        if (o.sellerId) {
          const sellerRows = await sql`SELECT name, phone FROM users WHERE id::text = ${String(o.sellerId)} LIMIT 1`;
          if (sellerRows.length && sellerRows[0].phone) {
            const sellerPhone = String(sellerRows[0].phone).replace(/[^0-9]/g, '');
            const itemNames   = (o.items || []).map(function(i){ return i.name; }).join(', ');
            const curr        = o.currency || 'NGN';
            const sym         = curr === 'NGN' ? '₦' : curr === 'USD' ? '$' : curr === 'GBP' ? '£' : curr;
            const waMsg = encodeURIComponent(
              '🛒 *New Order on NeyoMarket!*\n\n'
              + 'Order ID: *' + String(o.id) + '*\n'
              + 'Items: ' + itemNames + '\n'
              + 'Amount: *' + sym + parseFloat(o.total).toLocaleString() + '*\n'
              + 'Status: Paid & in Escrow\n\n'
              + 'Log in to NeyoMarket to process this order.'
            );
            /* Store WA link in order for reference — actual send is client-side */
            console.log('[WA notify seller] https://wa.me/' + sellerPhone + '?text=' + waMsg);
          }
        }
      } catch(waErr) { console.error('[WA notify]', waErr.message); }

      return res.status(201).json({ ok: true, deliveryCode });
    } catch (err) {
      console.error('[payment/orders POST]', err.message);
      return jsonErr(res, 500, 'Could not create order.', err.message);
    }
  }

  /* ══════════════════════════════════════════════════════════════════
     ORDERS — PATCH ?action=orders
     Update any order fields. orderId extracted from query parameter.
     All conditionals pre-computed before sql template (Neon ternary rule).
  ══════════════════════════════════════════════════════════════════ */
  if (action === 'orders' && req.method === 'PATCH') {
    try {
      const orderId = req.query.id;
      if (!orderId) return jsonErr(res, 400, 'orderId required in query parameter (?id=...).');

      const body = req.body || {};

      const newStatus        = body.status        !== undefined ? String(body.status)                         : null;
      const newCollected     = body.collected      !== undefined ? Boolean(body.collected)                    : null;
      const newCollectedAt   = body.collectedAt    !== undefined ? (body.collectedAt    || null)              : null;
      const newDisputed      = body.disputed       !== undefined ? Boolean(body.disputed)                     : null;
      const newDisputeReason = body.disputeReason  !== undefined ? String(body.disputeReason).slice(0, 1000) : null;
      const newPlatformFee   = body.platformFee    !== undefined ? parseFloat(body.platformFee)               : null;
      const newSellerPayout  = body.sellerPayout   !== undefined ? parseFloat(body.sellerPayout)              : null;
      const newFileUrl       = body.fileUrl        !== undefined ? (body.fileUrl || null)                     : null;
      const newItems         = body.items          !== undefined ? JSON.stringify(body.items)                 : null;
      const orderIdStr       = String(orderId);

      const rawAff          = body.affCode || null;
      const newAffCode      = (rawAff && String(rawAff).trim().length > 2) ? String(rawAff).trim() : null;
      const newAffiliateFee = (body.affiliateFee !== undefined && newAffCode)
        ? parseFloat(body.affiliateFee)
        : (body.affiliateFee !== undefined && body.affiliateFee === 0 ? 0 : null);

      await sql`
        UPDATE orders SET
          status         = COALESCE(${newStatus},        status),
          collected      = COALESCE(${newCollected},     collected),
          collected_at   = COALESCE(${newCollectedAt},   collected_at),
          disputed       = COALESCE(${newDisputed},      disputed),
          dispute_reason = COALESCE(${newDisputeReason}, dispute_reason),
          platform_fee   = COALESCE(${newPlatformFee},   platform_fee),
          seller_payout  = COALESCE(${newSellerPayout},  seller_payout),
          affiliate_fee  = COALESCE(${newAffiliateFee},  affiliate_fee),
          file_url       = COALESCE(${newFileUrl},       file_url),
          items          = COALESCE(${newItems},         items::text)
        WHERE id = ${orderIdStr}
      `;

      /* Credit affiliate ONLY on completion with a valid aff_code */
      if (newAffCode && newAffiliateFee && newAffiliateFee > 0
          && (newStatus === 'completed' || body.collected === true)) {
        try {
          await sql`
            UPDATE users
            SET seller_balance = COALESCE(seller_balance, 0) + ${newAffiliateFee}
            WHERE aff_code = ${newAffCode}
          `;
        } catch (affErr) {
          console.error('[payment/orders PATCH] affiliate credit (non-fatal):', affErr.message);
        }
      }

      return res.status(200).json({ ok: true });
    } catch (err) {
      console.error('[payment/orders PATCH]', err.message);
      return jsonErr(res, 500, 'Could not update order.', err.message);
    }
  }

  /* ══════════════════════════════════════════════════════════════════
     UPDATE ORDER STATUS — seller marks order as preparing/shipped/delivered
  ══════════════════════════════════════════════════════════════════ */
  if (action === 'update-order-status' && req.method === 'POST') {
    try {
      const { orderId, status, sellerId } = req.body || {};
      if (!orderId || !status) return jsonErr(res, 400, 'orderId and status required.');
      const allowed = ['preparing','shipped','delivered'];
      if (!allowed.includes(status)) return jsonErr(res, 400, 'Invalid status.');

      await sql`UPDATE orders SET status = ${status}, updated_at = NOW() WHERE id = ${String(orderId)}`;

      /* Email buyer when shipped */
      if (status === 'shipped') {
        try {
          const SITE = process.env.SITE_URL || 'https://neyomarket.com.ng';
          const oRow = await sql`SELECT customer, seller_id FROM orders WHERE id = ${String(orderId)} LIMIT 1`;
          if (oRow.length) {
            const cust = typeof oRow[0].customer === 'string' ? JSON.parse(oRow[0].customer) : (oRow[0].customer || {});
            if (cust.email) {
              const sRow = await sql`SELECT name, phone FROM users WHERE id = ${String(oRow[0].seller_id||'')} LIMIT 1`;
              const sName = sRow.length ? sRow[0].name : 'Your Seller';
              const sWa   = sRow.length ? (sRow[0].phone||'') : '';
              const html = '<!DOCTYPE html><html><body style="font-family:Arial,sans-serif;max-width:560px;margin:0 auto;padding:20px">'
                + '<div style="background:linear-gradient(135deg,#0a0a1a,#1a1a2e);padding:20px;border-radius:12px 12px 0 0;text-align:center"><div style="font-size:24px;font-weight:900;color:#c9922a;font-family:Georgia,serif">NeyoMarket</div></div>'
                + '<div style="background:#f9fafb;padding:24px;border:1px solid #e5e7eb;border-top:none;border-radius:0 0 12px 12px">'
                + '<h2 style="color:#0a0a1a;margin:0 0 12px">Your Order is On Its Way! 🚚</h2>'
                + '<p style="color:#555">Hi <strong>' + (cust.name||'Customer') + '</strong>, <strong>' + sName + '</strong> has shipped your order.</p>'
                + '<div style="background:#e8f0fe;border-radius:10px;padding:14px;margin:16px 0;font-size:13px;color:#1a56db">📦 Order ID: <strong>' + String(orderId) + '</strong></div>'
                + (sWa ? '<p style="color:#555;font-size:13px">Contact seller on WhatsApp: <a href="https://wa.me/' + sWa + '" style="color:#25d366;font-weight:700">' + sWa + '</a></p>' : '')
                + '<a href="' + SITE + '/?page=profile" style="display:block;background:#c9922a;color:#fff;text-decoration:none;padding:12px;border-radius:10px;font-weight:700;text-align:center;margin-top:16px">Track Order →</a>'
                + '</div></body></html>';
              fetch(SITE + '/api/auth?action=send-email', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ to: cust.email, subject: '🚚 Your Order Has Been Shipped — ' + String(orderId), html }) }).catch(function(){});
            }
          }
        } catch(e) { console.warn('[payment/shipped-email] non-fatal:', e.message); }
      }

      return res.status(200).json({ ok: true, status });
    } catch (err) {
      console.error('[payment/update-order-status]', err.message);
      return jsonErr(res, 500, 'Could not update order status.', err.message);
    }
  }

  /* ══════════════════════════════════════════════════════════════════
     ORDERS — DELETE ?action=orders&id=xxx
  ══════════════════════════════════════════════════════════════════ */
  if (action === 'orders' && req.method === 'DELETE') {
    try {
      const rawId = req.query.id || (req.body && req.body.id);
      if (!rawId) return jsonErr(res, 400, 'Order id required.');
      await sql`DELETE FROM orders WHERE id = ${String(rawId)}`;
      return res.status(200).json({ ok: true });
    } catch (err) {
      console.error('[payment/orders DELETE]', err.message);
      return jsonErr(res, 500, 'Could not delete order.', err.message);
    }
  }

  /* ══════════════════════════════════════════════════════════════════
     DISPUTES — GET ?action=disputes
     ?admin=true → all disputed orders
     ?userId=<id> → buyer's own disputes
  ══════════════════════════════════════════════════════════════════ */
  if (action === 'disputes' && req.method === 'GET') {
    try {
      const isAdmin = req.query.admin === 'true';
      const userId  = req.query.userId;
      let rows;

      if (isAdmin) {
        rows = await sql`
          SELECT * FROM orders
          WHERE disputed = true OR status = 'disputed'
          ORDER BY created_at DESC LIMIT 200
        `;
      } else if (userId) {
        rows = await sql`
          SELECT * FROM orders
          WHERE user_id = ${String(userId)}
            AND (disputed = true OR status = 'disputed')
          ORDER BY created_at DESC
        `;
      } else {
        return jsonErr(res, 400, 'Provide ?userId=<id> or ?admin=true');
      }

      const disputes = rows.map(function(r) {
        return {
          id:            r.id,
          userId:        r.user_id,
          customer:      safeJson(r.customer, {}),
          items:         safeJson(r.items, []),
          total:         parseFloat(r.total || 0),
          status:        r.status         || 'disputed',
          disputed:      r.disputed       || true,
          disputeReason: r.dispute_reason || null,
          ref:           r.ref            || null,
          createdAt:     r.created_at     || null
        };
      });

      return res.status(200).json({ ok: true, disputes });
    } catch (err) {
      console.error('[payment/disputes GET]', err.message);
      return jsonErr(res, 500, 'Could not fetch disputes.', err.message);
    }
  }

  /* ══════════════════════════════════════════════════════════════════
     DISPUTES — POST ?action=disputes
     Buyer raises a dispute. Saves reason to Neon orders table.
     Body: { orderId, userId, reason }
  ══════════════════════════════════════════════════════════════════ */
  if (action === 'disputes' && req.method === 'POST') {
    try {
      const { orderId, reason } = req.body || {};
      if (!orderId) return jsonErr(res, 400, 'orderId is required.');
      if (!reason || String(reason).trim().length < 5)
        return jsonErr(res, 400, 'A dispute reason of at least 5 characters is required.');

      const orderIdStr = String(orderId);
      const safeReason = String(reason).trim().slice(0, 1000);

      const orderRows = await sql`
        SELECT id, status FROM orders WHERE id = ${orderIdStr} LIMIT 1
      `;
      if (!orderRows.length) return jsonErr(res, 404, 'Order not found: ' + orderIdStr);

      const allowedStatuses = ['paid', 'escrow_held', 'success'];
      if (!allowedStatuses.includes(orderRows[0].status)) {
        return jsonErr(res, 400, 'Order cannot be disputed. Status is: ' + orderRows[0].status);
      }

      await sql`
        UPDATE orders SET
          disputed       = true,
          status         = 'disputed',
          dispute_reason = ${safeReason}
        WHERE id = ${orderIdStr}
      `;

      console.log('[payment/disputes POST] raised on', orderIdStr);
      return res.status(200).json({
        ok:      true,
        message: 'Dispute submitted. Admin will review within 24 hours.',
        orderId: orderIdStr
      });
    } catch (err) {
      console.error('[payment/disputes POST]', err.message);
      return jsonErr(res, 500, 'Could not submit dispute.', err.message);
    }
  }

  /* ══════════════════════════════════════════════════════════════════
     DISPUTES — PATCH ?action=disputes
     Admin resolves a dispute.
     Body: { orderId, action: 'resolve_seller'|'resolve_buyer'|'close' }
  ══════════════════════════════════════════════════════════════════ */
  if (action === 'disputes' && req.method === 'PATCH') {
    try {
      const { orderId, action: disputeAction } = req.body || {};
      if (!orderId)       return jsonErr(res, 400, 'orderId is required.');
      if (!disputeAction) return jsonErr(res, 400, 'action is required: resolve_seller | resolve_buyer | close');

      const orderIdStr = String(orderId);
      const rows = await sql`SELECT * FROM orders WHERE id = ${orderIdStr} LIMIT 1`;
      if (!rows.length) return jsonErr(res, 404, 'Order not found.');

      const order = rows[0];

      if (disputeAction === 'resolve_seller') {
        const sellerPayout = parseFloat(order.seller_payout || order.total * 0.85 || 0);

        await sql`
          UPDATE orders SET
            status       = 'completed',
            collected    = true,
            collected_at = NOW(),
            disputed     = false
          WHERE id = ${orderIdStr}
        `;

        const items = safeJson(order.items, []);
        const sellerId = Array.isArray(items) && items[0]
          ? String(items[0].sellerId || items[0].seller_id || '') : '';

        if (sellerId) {
          await sql`
            UPDATE users
            SET seller_balance = COALESCE(seller_balance, 0) + ${sellerPayout}
            WHERE id = ${sellerId}
          `;
        }

        console.log('[payment/disputes PATCH] resolved for seller —', orderIdStr, '₦' + sellerPayout);
        return res.status(200).json({
          ok:      true,
          message: 'Resolved for seller. ₦' + sellerPayout.toLocaleString() + ' released.',
          payout:  sellerPayout
        });

      } else if (disputeAction === 'resolve_buyer') {
        if (!PSK) return jsonErr(res, 500, 'PAYSTACK_SECRET_KEY not configured.');
        if (!order.ref) return jsonErr(res, 400, 'No Paystack reference on this order. Refund manually.');

        const refundAmount = Math.floor(parseFloat(order.total || 0) * 100);
        let refundData;
        try {
          const refundRes = await fetch('https://api.paystack.co/refund', {
            method:  'POST',
            headers: { 'Authorization': 'Bearer ' + PSK, 'Content-Type': 'application/json' },
            body:    JSON.stringify({
              transaction:   order.ref,
              amount:        refundAmount,
              merchant_note: 'Dispute resolved in buyer favour — NeyoMarket'
            })
          });
          refundData = await refundRes.json();
        } catch (fetchErr) {
          return jsonErr(res, 502, 'Could not reach Paystack.', fetchErr.message);
        }

        if (!refundData.status)
          return jsonErr(res, 400, 'Paystack refund failed: ' + (refundData.message || 'Check dashboard'));

        await sql`
          UPDATE orders SET status = 'refunded', disputed = false WHERE id = ${orderIdStr}
        `;

        console.log('[payment/disputes PATCH] refunded buyer —', orderIdStr);
        return res.status(200).json({
          ok:      true,
          message: 'Refund of ₦' + parseFloat(order.total || 0).toLocaleString() + ' initiated.'
        });

      } else if (disputeAction === 'close') {
        await sql`
          UPDATE orders SET disputed = false, status = 'escrow_held' WHERE id = ${orderIdStr}
        `;
        return res.status(200).json({ ok: true, message: 'Dispute closed without action.' });

      } else {
        return jsonErr(res, 400, 'Unknown action: ' + disputeAction);
      }
    } catch (err) {
      console.error('[payment/disputes PATCH]', err.message);
      return jsonErr(res, 500, 'Could not resolve dispute.', err.message);
    }
  }

  /* ══════════════════════════════════════════════════════════════════
     POST ?action=confirm
     Verify payment, save order, split commission, write admin_transactions.
  ══════════════════════════════════════════════════════════════════ */
  if (action === 'confirm' && req.method === 'POST') {
    const { reference, orderId, userId, items, total,
            customer, mode, sellerUserId, affCode, shipping } = req.body || {};

    if (!reference || !orderId || !total)
      return jsonErr(res, 400, 'reference, orderId and total are required.');

    try {
      /* Skip if already processed */
      const existing = await sql`
        SELECT id, status FROM orders WHERE id = ${String(orderId)} LIMIT 1
      `;
      if (existing.length && ['paid','escrow_held','completed'].includes(existing[0].status)) {
        return res.status(200).json({ ok: true, cached: true, orderId, status: existing[0].status });
      }

      /* Verify payment with Paystack */
      let amount = parseFloat(total);
      if (PSK) {
        const txn = await verifyPaystackPayment(reference);
        if (!txn || txn.status !== 'success')
          return jsonErr(res, 402, 'Payment not confirmed by Paystack. Ref: ' + reference);
        amount = txn.amount / 100;
      }

      /* Build item list */
      const itemList    = Array.isArray(items) ? items : [];
      const hasPhysical = itemList.some(function(i) { return i.type === 'physical'; });
      const isAllDigital = itemList.length > 0 && itemList.every(function(i) {
        return i.type === 'digital' || i.type === 'course';
      });

      /* Compute split — fetch seller tier first */
      const rawAff      = (affCode && typeof affCode === 'string') ? affCode.trim() : '';
      const hasValidAff = rawAff.length > 2 && rawAff !== 'GUEST';

      /* Resolve seller */
      const resolvedSellerId = sellerUserId
        ? String(sellerUserId)
        : (itemList[0] && (itemList[0].sellerId || itemList[0].seller_id))
          ? String(itemList[0].sellerId || itemList[0].seller_id) : null;

      let sellerTier = 'free';
      if (resolvedSellerId) {
        try {
          const tierRows = await sql`SELECT membership_tier FROM users WHERE id = ${resolvedSellerId} LIMIT 1`;
          if (tierRows.length) sellerTier = tierRows[0].membership_tier || 'free';
        } catch(e) { /* non-fatal — default to free */ }
      }

      const split = computeSplit(amount, hasPhysical, hasValidAff, sellerTier);

      const orderStatus  = isAllDigital ? 'paid' : 'escrow_held';
      const deliveryCode = generateDVC(String(orderId));
      const cleanAff     = hasValidAff ? rawAff : null;

      /* Fetch digital file URLs */
      let topFileUrl = null;
      if (isAllDigital && itemList.length > 0) {
        const productIds = itemList.map(function(i) { return Number(i.id); })
                                   .filter(function(id) { return !isNaN(id) && id > 0; });
        if (productIds.length) {
          const prods = await sql`SELECT id, file_url FROM products WHERE id = ANY(${productIds})`;
          const first = prods.find(function(p) { return p.file_url; });
          if (first) topFileUrl = first.file_url;
          itemList.forEach(function(item) {
            const p = prods.find(function(p) { return Number(p.id) === Number(item.id); });
            if (p && p.file_url) item.fileUrl = p.file_url;
          });
        }
      }

      /* Save order to database */
      const resolvedSellerIdInt = resolvedSellerId ? parseInt(resolvedSellerId) : null;
      await sql`
        INSERT INTO orders (
          id, user_id, customer, items, total, amount,
          platform_fee, seller_payout, affiliate_fee, aff_code,
          seller_id, status, collected, mode, ref, shipping,
          delivery_code, file_url, date, created_at
        ) VALUES (
          ${String(orderId)}, ${String(userId || '')},
          ${JSON.stringify(customer || {})}, ${JSON.stringify(itemList)},
          ${amount}, ${amount},
          ${split.platformFee}, ${split.sellerPayout},
          ${split.affiliateFee}, ${cleanAff},
          ${resolvedSellerIdInt}, ${orderStatus},
          ${false}, ${mode || 'standard'}, ${reference},
          ${JSON.stringify(shipping || null)}, ${deliveryCode}, ${topFileUrl},
          ${new Date().toLocaleDateString()}, NOW()
        )
        ON CONFLICT (id) DO UPDATE SET
          status        = EXCLUDED.status,
          ref           = EXCLUDED.ref,
          seller_id     = COALESCE(EXCLUDED.seller_id, orders.seller_id),
          amount        = COALESCE(EXCLUDED.amount, orders.amount),
          delivery_code = EXCLUDED.delivery_code,
          file_url      = COALESCE(EXCLUDED.file_url, orders.file_url)
      `;

      /* Credit platform balance */
      if (split.platformFee > 0) {
        await sql`
          UPDATE users SET admin_balance = COALESCE(admin_balance, 0) + ${split.platformFee}
          WHERE LOWER(email) = LOWER(${ADMIN_EMAIL})
        `;
      }

      /* Resolve affiliate user ID from affCode — required before recording commission */
      let affUserId = null;
      if (hasValidAff && rawAff) {
        try {
          const affRows = await sql`SELECT id FROM users WHERE aff_code = ${rawAff} LIMIT 1`;
          if (affRows.length) affUserId = String(affRows[0].id);
        } catch(e) { console.warn('[payment/confirm] affUserId lookup (non-fatal):', e.message); }
      }

      /* Record affiliate commission as PENDING — wallet credited only when order completes */
      if (affUserId && split.affiliateFee > 0) {
        try {
          await sql`
            INSERT INTO affiliate_commissions
              (aff_user_id, aff_code, order_id, order_amount, commission, status, created_at)
            VALUES (${affUserId}, ${rawAff}, ${String(orderId)}, ${amount}, ${split.affiliateFee}, ${'pending'}, NOW())
            ON CONFLICT (order_id) DO NOTHING
          `;
        } catch (e) { console.warn('[payment/confirm] affiliate_commissions (non-fatal):', e.message); }
      }

      /* Credit seller for digital products immediately */
      if (isAllDigital && resolvedSellerId && split.sellerPayout > 0) {
        await sql`
          UPDATE users SET seller_balance = COALESCE(seller_balance, 0) + ${split.sellerPayout}
          WHERE id = ${resolvedSellerId}
        `;
        /* Also upsert into wallets table so analytics can read it */
        await sql`
          INSERT INTO wallets (user_id, balance, pending_balance, referral_earnings, updated_at)
          VALUES (${String(resolvedSellerId)}, ${split.sellerPayout}, 0, 0, NOW())
          ON CONFLICT (user_id) DO UPDATE SET
            balance = wallets.balance + ${split.sellerPayout},
            updated_at = NOW()
        `;
      }

      await recordAdminTx({
        orderId: String(orderId), total: amount,
        platformFee: split.platformFee, sellerPayout: split.sellerPayout,
        affiliateFee: split.affiliateFee, affCode: cleanAff,
        sellerId: resolvedSellerId, type: 'payment'
      });

      console.log('[payment/confirm]', orderId, '₦' + amount, '| status:', orderStatus);

      /* Award buyer 10 loyalty points for purchase */
      try {
        /* Use userId (passed in payload) — more reliable than email which can be masked */
        const buyerLookup = userId
          ? await sql`SELECT id, loyalty_points, loyalty_history FROM users WHERE id::text = ${String(userId)} LIMIT 1`
          : await sql`SELECT id, loyalty_points, loyalty_history FROM users WHERE email = ${String(customer.email || '').toLowerCase().trim()} LIMIT 1`;

        if (buyerLookup.length) {
          const bId      = String(buyerLookup[0].id);
          const currPts  = parseInt(buyerLookup[0].loyalty_points || 0);
          const newPts   = currPts + 10;
          const bHistory = safeJson(buyerLookup[0].loyalty_history, []);
          bHistory.push({ pts: 10, label: 'Purchase: ' + orderId, date: new Date().toLocaleDateString() });
          await sql`UPDATE users SET loyalty_points = ${newPts}, loyalty_history = ${JSON.stringify(bHistory)}::jsonb WHERE id = ${bId}`;
          console.log('[payment/confirm] +10 loyalty pts → userId:', bId, 'total:', newPts);
        } else {
          console.warn('[payment/confirm] buyer not found for loyalty points. userId:', userId, 'email:', customer && customer.email);
        }
      } catch (e) { console.warn('[payment/confirm] buyer loyalty points (non-fatal):', e.message); }

      const buyerEmail   = (customer && customer.email) ? String(customer.email) : '';
      const buyerName    = (customer && customer.name)  ? String(customer.name)  : 'Valued Customer';
      const SITE         = process.env.SITE_URL || 'https://neyomarket.com.ng';
      const sym          = { NGN:'₦', USD:'$', GBP:'£', EUR:'€', CAD:'CA$', GHS:'GH₵' }[(itemList[0] && itemList[0].currency) || 'NGN'] || '₦';
      const itemListHtml = itemList.map(function(i){ return '<li style="padding:4px 0;color:#555">' + (i.emoji||'📦') + ' ' + (i.name||'Product') + (i.selectedVariant ? ' — ' + i.selectedVariant : '') + ' × ' + (i.qty||1) + '</li>'; }).join('');

      /* Email helper */
      function sendNeyoEmail(to, subject, content) {
        const html = '<!DOCTYPE html><html><head><meta charset="utf-8"/></head><body style="margin:0;padding:0;background:#f4f4f4;font-family:Arial,sans-serif">'
          + '<table width="100%" cellpadding="0" cellspacing="0"><tr><td align="center" style="padding:30px 10px">'
          + '<table width="100%" style="max-width:560px;background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,.08)">'
          + '<tr><td style="background:linear-gradient(135deg,#0a0a1a,#1a1a2e);padding:24px 32px;text-align:center">'
          + '<div style="font-size:26px;font-weight:900;color:#c9922a;font-family:Georgia,serif">NeyoMarket</div>'
          + '<div style="color:rgba(255,255,255,.5);font-size:11px;margin-top:3px;letter-spacing:2px;text-transform:uppercase">Nigeria\'s Secure Marketplace</div>'
          + '</td></tr>'
          + '<tr><td style="padding:28px 32px">' + content + '</td></tr>'
          + '<tr><td style="background:#f9f9f9;padding:16px 32px;text-align:center;border-top:1px solid #eee">'
          + '<p style="color:#999;font-size:12px;margin:0">© 2026 NeyoMarket · <a href="' + SITE + '" style="color:#c9922a;text-decoration:none">neyomarket.com.ng</a></p>'
          + '<p style="color:#bbb;font-size:11px;margin:6px 0 0">Support: +2349072212496 or +2349168321317</p>'
          + '</td></tr></table></td></tr></table></body></html>';
        fetch(SITE + '/api/auth?action=send-email', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ to, subject, html })
        }).catch(function(e){ console.warn('[payment/email]', e.message); });
      }

      if (buyerEmail) {
        /* Buyer — order confirmed */
        sendNeyoEmail(buyerEmail, '✅ Order Confirmed — ' + String(orderId),
          '<h2 style="color:#0a0a1a;margin:0 0 8px;font-size:20px">Order Confirmed! ✅</h2>'
          + '<p style="color:#555;line-height:1.7;margin:0 0 16px">Hi <strong>' + buyerName + '</strong>, your payment is secured in escrow.</p>'
          + '<div style="background:#f9f4eb;border-radius:10px;padding:16px;margin-bottom:16px">'
          + '<div style="font-size:11px;color:#999;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px">Order ID</div>'
          + '<div style="font-family:monospace;font-weight:700;color:#c9922a">' + String(orderId) + '</div>'
          + '<ul style="margin:12px 0 8px;padding-left:18px">' + itemListHtml + '</ul>'
          + '<div style="border-top:1px solid #e8d9c0;padding-top:10px;font-weight:700;color:#0a0a1a">Total: ' + sym + Number(amount).toLocaleString() + '</div>'
          + '</div>'
          + '<div style="background:#e8f5e9;border-radius:10px;padding:12px;margin-bottom:16px;font-size:13px;color:#2e7d32">'
          + '🔒 <strong>Escrow Active</strong> — Money is held safely and released only after you confirm receipt.</div>'
          + (isAllDigital && topFileUrl ? '<a href="' + topFileUrl + '" style="display:block;background:#059669;color:#fff;text-decoration:none;padding:12px;border-radius:10px;font-weight:700;font-size:14px;text-align:center;margin-bottom:12px">⬇️ Download Your Product</a>' : '')
          + '<a href="' + SITE + '/?page=profile" style="display:block;background:linear-gradient(135deg,#c9922a,#b45309);color:#fff;text-decoration:none;padding:12px;border-radius:10px;font-weight:700;font-size:14px;text-align:center">Track Your Order →</a>'
        );

        /* Seller — new order */
        try {
          const sellerEmailRow = await sql`SELECT email, name FROM users WHERE id = ${String(resolvedSellerId || '')} LIMIT 1`;
          if (sellerEmailRow.length && sellerEmailRow[0].email) {
            sendNeyoEmail(sellerEmailRow[0].email, '🛍 New Order — ' + String(orderId),
              '<h2 style="color:#0a0a1a;margin:0 0 8px;font-size:20px">New Order Received! 🎉</h2>'
              + '<p style="color:#555;line-height:1.7;margin:0 0 16px">Hi <strong>' + sellerEmailRow[0].name + '</strong>, <strong>' + buyerName + '</strong> just purchased from your store.</p>'
              + '<div style="background:#f9f4eb;border-radius:10px;padding:16px;margin-bottom:16px">'
              + '<div style="font-size:11px;color:#999;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px">Order ID</div>'
              + '<div style="font-family:monospace;font-weight:700;color:#c9922a">' + String(orderId) + '</div>'
              + '<ul style="margin:12px 0 8px;padding-left:18px">' + itemListHtml + '</ul>'
              + '<div style="border-top:1px solid #e8d9c0;padding-top:10px;font-weight:700;color:#0a0a1a">Total: ' + sym + Number(amount).toLocaleString() + '</div>'
              + '</div>'
              + '<p style="color:#555;font-size:13px;line-height:1.6">Prepare and ship promptly. Payment releases after buyer confirms receipt.</p>'
              + '<a href="' + SITE + '/?page=profile" style="display:block;background:linear-gradient(135deg,#c9922a,#b45309);color:#fff;text-decoration:none;padding:12px;border-radius:10px;font-weight:700;font-size:14px;text-align:center;margin-top:16px">View Order →</a>'
            );
          }
        } catch(e) { console.warn('[payment/confirm] seller email (non-fatal):', e.message); }
      }

      return res.status(200).json({
        ok: true, orderId, amount,
        platformFee:  split.platformFee,
        sellerPayout: split.sellerPayout,
        affiliateFee: split.affiliateFee,
        hasValidAff,  status: orderStatus,
        deliveryCode: orderStatus === 'escrow_held' ? deliveryCode : null
      });

    } catch (err) {
      console.error('[payment/confirm]', err.message);
      return jsonErr(res, 500, 'Order save failed.', err.message);
    }
  }

  /* ══════════════════════════════════════════════════════════════════
     POST ?action=dvc-release
     Seller enters 6-digit code → validates → releases escrow to seller.
     Body: { orderId, dvcCode, sellerUserId }
  ══════════════════════════════════════════════════════════════════ */
  if (action === 'dvc-release' && req.method === 'POST') {
    const { orderId, dvcCode, sellerUserId } = req.body || {};
    if (!orderId || !dvcCode)
      return jsonErr(res, 400, 'orderId and dvcCode are required.');

    try {
      const rows = await sql`SELECT * FROM orders WHERE id = ${String(orderId)} LIMIT 1`;
      if (!rows.length) return jsonErr(res, 404, 'Order not found: ' + orderId);

      const order = rows[0];

      if (order.status === 'completed' || order.collected) {
        return res.status(200).json({
          ok: true, cached: true,
          message: 'Order already completed.',
          released: parseFloat(order.seller_payout || 0)
        });
      }

      if (!['escrow_held','paid','success'].includes(order.status))
        return jsonErr(res, 400, 'Order cannot be released. Status: ' + order.status);

      const storedCode   = order.delivery_code ? String(order.delivery_code).trim() : null;
      const expectedCode = storedCode || generateDVC(String(orderId));

      if (String(dvcCode).trim() !== expectedCode)
        return jsonErr(res, 400, 'Incorrect delivery code. Ask the buyer to open their Orders page and share it.');

      const sellerPayout  = parseFloat(order.seller_payout || 0);
      const affiliateFee  = parseFloat(order.affiliate_fee || 0);
      const collectedAt   = new Date().toISOString();
      const items         = safeJson(order.items, []);

      const resolvedSellerId = sellerUserId
        ? String(sellerUserId)
        : (items[0] && (items[0].sellerId || items[0].seller_id))
          ? String(items[0].sellerId || items[0].seller_id) : null;

      if (resolvedSellerId && sellerPayout > 0) {
        await sql`
          UPDATE users SET seller_balance = COALESCE(seller_balance, 0) + ${sellerPayout}
          WHERE id = ${resolvedSellerId}
        `;
      }

      /* Credit affiliate wallet and mark commission as paid — only on order completion */
      const affCode = order.aff_code ? String(order.aff_code).trim() : '';
      if (affCode.length > 2 && affiliateFee > 0) {
        try {
          const affUserRows = await sql`SELECT id FROM users WHERE aff_code = ${affCode} LIMIT 1`;
          if (affUserRows.length) {
            const affId = String(affUserRows[0].id);
            await sql`
              UPDATE users SET seller_balance = COALESCE(seller_balance, 0) + ${affiliateFee}
              WHERE id = ${affId}
            `;
            await sql`
              UPDATE affiliate_commissions SET status = 'paid'
              WHERE order_id = ${String(orderId)} AND aff_user_id = ${affId} AND status = 'pending'
            `;
          }
        } catch (e) { console.error('[payment/dvc-release] affiliate (non-fatal):', e.message); }
      }

      await sql`
        UPDATE orders SET
          status       = 'completed',
          collected    = true,
          collected_at = ${collectedAt}
        WHERE id = ${String(orderId)}
      `;

      await recordAdminTx({
        orderId: String(orderId), total: parseFloat(order.total || 0),
        platformFee: parseFloat(order.platform_fee || 0),
        sellerPayout, affiliateFee, affCode: affCode || null,
        sellerId: resolvedSellerId, type: 'dvc_release'
      });

      /* Award seller 20 loyalty points for confirmed sale */
      try {
        const sellerRows = await sql`SELECT loyalty_points, loyalty_history FROM users WHERE id = ${resolvedSellerId} LIMIT 1`;
        if (sellerRows.length) {
          const currentPts  = parseInt(sellerRows[0].loyalty_points || 0);
          const newPts      = currentPts + 20;
          const history     = safeJson(sellerRows[0].loyalty_history, []);
          history.push({ pts: 20, label: 'Sale confirmed: ' + orderId, date: new Date().toLocaleDateString() });
          await sql`UPDATE users SET loyalty_points = ${newPts}, loyalty_history = ${JSON.stringify(history)}::jsonb WHERE id = ${resolvedSellerId}`;
        }
      } catch (e) { console.warn('[payment/dvc-release] loyalty points (non-fatal):', e.message); }

      console.log('[payment/dvc-release]', orderId, '| seller ₦' + sellerPayout);

      return res.status(200).json({
        ok: true, orderId, released: sellerPayout,
        message: '✅ Delivery confirmed. ₦' + sellerPayout.toLocaleString() + ' released to your wallet.'
      });

    } catch (err) {
      console.error('[payment/dvc-release]', err.message);
      return jsonErr(res, 500, 'DVC release failed.', err.message);
    }
  }

  /* ══════════════════════════════════════════════════════════════════
     POST ?action=refund
     Admin triggers full Paystack refund. Marks order 'refunded'.
     Body: { orderId, reference, amount }
  ══════════════════════════════════════════════════════════════════ */
  if (action === 'refund' && req.method === 'POST') {
    const { orderId, reference, amount } = req.body || {};
    if (!orderId || !reference) return jsonErr(res, 400, 'orderId and reference are required.');
    if (!PSK) return jsonErr(res, 500, 'PAYSTACK_SECRET_KEY not configured.');

    try {
      const body = { transaction: reference };
      if (amount) body.amount = Math.round(parseFloat(amount) * 100);

      const r = await fetch('https://api.paystack.co/refund', {
        method: 'POST',
        headers: { 'Authorization': 'Bearer ' + PSK, 'Content-Type': 'application/json' },
        body:   JSON.stringify(body)
      });

      let data;
      try { data = await r.json(); }
      catch (e) { return jsonErr(res, 502, 'Paystack returned non-JSON.', e.message); }

      if (!data.status)
        return jsonErr(res, 400, 'Paystack refund failed: ' + (data.message || 'Unknown'));

      /* Get order details before updating */
      const orderRows = await sql`SELECT * FROM orders WHERE id = ${String(orderId)} LIMIT 1`;
      const order = orderRows.length ? orderRows[0] : null;

      /* Update order status to refunded */
      await sql`UPDATE orders SET status = 'refunded', collected = false WHERE id = ${String(orderId)}`;

      /* AUTO-REFUND: Reverse balances from seller and affiliate */
      if (order) {
        const sellerPayout = parseFloat(order.seller_payout || 0);
        const affiliateFee = parseFloat(order.affiliate_fee || 0);
        const affCode = order.aff_code || null;

        /* Refund seller balance */
        const itemsData = safeJson(order.items, []);
        if (itemsData.length > 0) {
          const firstItem = itemsData[0];
          const sellerId = firstItem.sellerId;
          if (sellerId) {
            await sql`UPDATE users SET seller_balance = seller_balance - ${sellerPayout} WHERE id = ${String(sellerId)}`;
          }
        }

        /* Refund affiliate balance if affiliate exists */
        if (affCode && affiliateFee > 0) {
          try {
            await sql`UPDATE users SET affiliate_balance = affiliate_balance - ${affiliateFee} WHERE aff_code = ${String(affCode)}`;
          } catch (e) {
            console.warn('[payment/refund] Could not refund affiliate:', e.message);
          }
        }
      }

      console.log('[payment/refund]', orderId, 'ref:', reference);
      return res.status(200).json({
        ok: true, orderId,
        message: 'Refund of ₦' + parseFloat(amount || 0).toLocaleString() + ' initiated. Balances updated.'
      });

    } catch (err) {
      console.error('[payment/refund]', err.message);
      return jsonErr(res, 500, 'Refund failed.', err.message);
    }
  }

  /* ══════════════════════════════════════════════════════════════════
     GET ?action=order&orderId=xxx
     Returns full order — drives download/DVC/confirm buttons.
  ══════════════════════════════════════════════════════════════════ */
  if (action === 'order' && req.method === 'GET') {
    try {
      const orderId = req.query.orderId;
      if (!orderId) return jsonErr(res, 400, 'orderId required.');

      const rows = await sql`SELECT * FROM orders WHERE id = ${String(orderId)} LIMIT 1`;
      if (!rows.length) return jsonErr(res, 404, 'Order not found: ' + orderId);

      const r     = rows[0];
      const items = safeJson(r.items, []);

      return res.status(200).json({
        ok: true,
        order: {
          id:            r.id,
          userId:        r.user_id,
          status:        r.status,
          collected:     r.collected,
          total:         parseFloat(r.total         || 0),
          platformFee:   parseFloat(r.platform_fee  || 0),
          sellerPayout:  parseFloat(r.seller_payout || 0),
          affiliateFee:  parseFloat(r.affiliate_fee || 0),
          affCode:       r.aff_code       || null,
          items,
          ref:           r.ref            || '',
          deliveryCode:  r.delivery_code  || null,
          fileUrl:       r.file_url       || null,
          disputed:      r.disputed       || false,
          disputeReason: r.dispute_reason || null,
          date:          r.date || (r.created_at ? new Date(r.created_at).toLocaleDateString() : ''),
          createdAt:     r.created_at     || null
        }
      });

    } catch (err) {
      console.error('[payment/order GET]', err.message);
      return jsonErr(res, 500, 'Could not fetch order.', err.message);
    }
  }

  /* ══════════════════════════════════════════════════════════════════
     POST ?action=download-digital
     Auto-release escrow when buyer downloads digital file
  ══════════════════════════════════════════════════════════════════ */
  if (action === 'download-digital' && req.method === 'POST') {
    const { orderId } = req.body || {};
    if (!orderId) return jsonErr(res, 400, 'orderId required');

    try {
      const orderRows = await sql`SELECT * FROM orders WHERE id = ${String(orderId)} LIMIT 1`;
      if (!orderRows.length) return jsonErr(res, 404, 'Order not found');

      const order = orderRows[0];
      const itemsData = safeJson(order.items, []);
      const sellerPayout = parseFloat(order.seller_payout || 0);

      /* Mark order as collected and release seller payment */
      await sql`UPDATE orders SET collected = true, collected_at = NOW(), status = 'completed' WHERE id = ${String(orderId)}`;

      /* Add seller payout to seller_balance */
      if (itemsData.length > 0 && sellerPayout > 0) {
        const firstItem = itemsData[0];
        const sellerId = firstItem.sellerId;
        if (sellerId) {
          await sql`UPDATE users SET seller_balance = seller_balance + ${sellerPayout} WHERE id = ${String(sellerId)}`;
        }
      }

      /* Credit affiliate wallet and mark commission paid on digital download */
      const dlAffCode    = order.aff_code ? String(order.aff_code).trim() : '';
      const dlAffFee     = parseFloat(order.affiliate_fee || 0);
      if (dlAffCode.length > 2 && dlAffFee > 0) {
        try {
          const affUserRows = await sql`SELECT id FROM users WHERE aff_code = ${dlAffCode} LIMIT 1`;
          if (affUserRows.length) {
            const affId = String(affUserRows[0].id);
            await sql`
              UPDATE users SET seller_balance = COALESCE(seller_balance, 0) + ${dlAffFee}
              WHERE id = ${affId}
            `;
            await sql`
              UPDATE affiliate_commissions SET status = 'paid'
              WHERE order_id = ${String(orderId)} AND aff_user_id = ${affId} AND status = 'pending'
            `;
          }
        } catch (e) { console.warn('[payment/download-digital] affiliate (non-fatal):', e.message); }
      }

      console.log('[payment/download-digital]', orderId, 'escrow released:', sellerPayout);
      return res.status(200).json({ ok: true, message: 'Seller payment released' });

    } catch (err) {
      console.error('[payment/download-digital]', err.message);
      return jsonErr(res, 500, 'Download failed', err.message);
    }
  }

  /* ══════════════════════════════════════════════════════════════════
     POST ?action=webhook
     Paystack charge.success fallback - AUTO-SAVE ORDER even if window closes
  ══════════════════════════════════════════════════════════════════ */
  if (action === 'webhook' && req.method === 'POST') {
    if (PSK) {
      const sig      = req.headers['x-paystack-signature'] || '';
      const expected = crypto.createHmac('sha512', PSK)
                             .update(JSON.stringify(req.body))
                             .digest('hex');
      if (sig !== expected) {
        console.warn('[payment/webhook] invalid signature');
        return res.status(200).json({ received: false, reason: 'invalid_signature' });
      }
    }

    const event = req.body || {};
    if (event.event === 'charge.success' || event.event === 'dedicated_virtual_account.success') {
      const ref = event.data && event.data.reference;
      const metadata = event.data && event.data.metadata;
      
      if (ref && metadata) {
        try {
          /* Extract order data from metadata */
          const orderId = metadata.orderId;
          const userId = metadata.userId;
          const items = metadata.items;
          const total = event.data.amount / 100;
          const customer = metadata.customer;
          const affCode = metadata.affCode || null;
          const sellerUserId = metadata.sellerUserId;
          const shipping = metadata.shipping;
          const mode = metadata.mode || 'standard';

          if (orderId && total) {
            /* Check if order already exists */
            const existing = await sql`
              SELECT id FROM orders WHERE id = ${String(orderId)} LIMIT 1
            `;

            if (!existing.length) {
              /* AUTO-SAVE ORDER from webhook */
              const itemList = Array.isArray(items) ? items : [];
              const hasPhysical = itemList.some(function(i) { return i.type === 'physical'; });
              const isAllDigital = itemList.length > 0 && itemList.every(function(i) {
                return i.type === 'digital' || i.type === 'course';
              });

              const rawAff = (affCode && typeof affCode === 'string') ? affCode.trim() : '';
              const hasValidAff = rawAff.length > 2 && rawAff !== 'GUEST';

              const webhookSellerId = (itemList[0] && (itemList[0].sellerId || itemList[0].seller_id))
                ? parseInt(itemList[0].sellerId || itemList[0].seller_id) : null;

              let webhookSellerTier = 'free';
              if (webhookSellerId) {
                try {
                  const wTierRows = await sql`SELECT membership_tier FROM users WHERE id = ${String(webhookSellerId)} LIMIT 1`;
                  if (wTierRows.length) webhookSellerTier = wTierRows[0].membership_tier || 'free';
                } catch(e) { /* non-fatal */ }
              }
              const split = computeSplit(total, hasPhysical, hasValidAff, webhookSellerTier);
              await sql`
                INSERT INTO orders (
                  id, user_id, customer, items, total, amount, platform_fee, seller_payout,
                  affiliate_fee, aff_code, seller_id, status, ref, mode, shipping, date, created_at
                ) VALUES (
                  ${String(orderId)},
                  ${String(userId || '')},
                  ${JSON.stringify(customer || {})},
                  ${JSON.stringify(itemList)},
                  ${Math.round(total)},
                  ${Math.round(total)},
                  ${Math.round(split.platformFee)},
                  ${Math.round(split.sellerPayout)},
                  ${Math.round(split.affiliateFee)},
                  ${hasValidAff ? String(rawAff) : null},
                  ${webhookSellerId},
                  ${isAllDigital ? 'paid' : 'escrow_held'},
                  ${String(ref)},
                  ${String(mode)},
                  ${shipping ? JSON.stringify(shipping) : null},
                  ${new Date().toLocaleDateString()},
                  NOW()
                )
              `;

              console.log('[payment/webhook] AUTO-SAVED order:', orderId, 'ref:', ref);
            } else {
              /* Order already exists */
              await sql`
                UPDATE orders SET status = 'escrow_held'
                WHERE id = ${String(orderId)}
                  AND status NOT IN ('paid','escrow_held','completed','refunded')
              `;
              console.log('[payment/webhook] Order already exists:', orderId);
            }
          }
        } catch (e) {
          console.error('[payment/webhook] DB error:', e.message);
        }
      }
    }

    return res.status(200).json({ received: true });
  }

  /* ══════════════════════════════════════════════════════════════════
     GET ?action=check-balance
     Re-verify user balance in database before withdrawal
  ══════════════════════════════════════════════════════════════════ */
  if (action === 'check-balance' && req.method === 'GET') {
    const userId = req.query.userId;
    if (!userId) return jsonErr(res, 400, 'userId required');

    try {
      const rows = await sql`
        SELECT balance FROM users WHERE id = ${String(userId)} LIMIT 1
      `;
      if (!rows.length) return jsonErr(res, 404, 'User not found');
      
      const balance = parseFloat(rows[0].balance || 0);
      return res.status(200).json({ ok: true, balance: balance });
    } catch (err) {
      console.error('[payment/check-balance]', err.message);
      return jsonErr(res, 500, 'Could not fetch balance', err.message);
    }
  }

  /* ══════════════════════════════════════════════════════════════════
     POST ?action=update-withdrawal-status
     Immediately mark withdrawal as 'completed' to prevent double-clicks
  ══════════════════════════════════════════════════════════════════ */
  if (action === 'update-withdrawal-status' && req.method === 'POST') {
    const { withdrawalId, status, amount, sellerEmail, sellerName } = req.body || {};
    if (!withdrawalId || !status) return jsonErr(res, 400, 'withdrawalId and status required');

    try {
      await sql`UPDATE withdrawals SET status = ${String(status)}, updated_at = NOW() WHERE id = ${Number(withdrawalId)}`;

      if (status === 'completed' && amount) {
        const amt = parseFloat(amount);
        try {
          await sql`UPDATE users SET admin_wallet = GREATEST(0, COALESCE(admin_wallet,0) - ${amt}) WHERE role = 'admin'`;
          await sql`INSERT INTO admin_wallet_transactions (type, amount, description, ref, created_at) VALUES ('debit', ${-amt}, ${'Seller withdrawal payout'}, ${String(withdrawalId)}, NOW())`;
        } catch(e) { console.warn('[payment/update-withdrawal-status] admin wallet deduct (non-fatal):', e.message); }

        /* Email seller */
        if (sellerEmail) {
          const SITE = process.env.SITE_URL || 'https://neyomarket.com.ng';
          const html = '<!DOCTYPE html><html><body style="font-family:Arial,sans-serif;max-width:560px;margin:0 auto;padding:20px">'
            + '<div style="background:linear-gradient(135deg,#0a0a1a,#1a1a2e);padding:20px;border-radius:12px 12px 0 0;text-align:center"><div style="font-size:24px;font-weight:900;color:#c9922a;font-family:Georgia,serif">NeyoMarket</div></div>'
            + '<div style="background:#f9fafb;padding:24px;border:1px solid #e5e7eb;border-top:none;border-radius:0 0 12px 12px">'
            + '<h2 style="color:#0a0a1a;margin:0 0 12px">Withdrawal Processed! 💸</h2>'
            + '<p style="color:#555">Hi <strong>' + (sellerName||'Seller') + '</strong>, your withdrawal has been sent to your bank account.</p>'
            + '<div style="background:#f9f4eb;border-radius:10px;padding:16px;margin:16px 0">'
            + '<div style="font-size:13px;color:#666;margin-bottom:6px">Amount</div>'
            + '<div style="font-size:28px;font-weight:900;color:#c9922a;font-family:Georgia,serif">₦' + Number(amt).toLocaleString() + '</div>'
            + '</div>'
            + '<p style="color:#888;font-size:12px">Funds typically arrive within 1-3 business days.</p>'
            + '<a href="' + SITE + '/?page=profile" style="display:block;background:#c9922a;color:#fff;text-decoration:none;padding:12px;border-radius:10px;font-weight:700;text-align:center;margin-top:16px">View Dashboard →</a>'
            + '</div></body></html>';
          fetch(SITE + '/api/auth?action=send-email', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ to: sellerEmail, subject: '💸 Withdrawal of ₦' + Number(amt).toLocaleString() + ' Processed', html }) }).catch(function(){});
        }
      }

      return res.status(200).json({ ok: true });
    } catch (err) {
      console.error('[payment/update-withdrawal-status]', err.message);
      return jsonErr(res, 500, 'Could not update withdrawal status', err.message);
    }
  }

  /* ══════════════════════════════════════════════════════════════════
     POST ?action=refund-balance
     Auto-refund amount back to user balance if Paystack transfer fails
  ══════════════════════════════════════════════════════════════════ */
  if (action === 'refund-balance' && req.method === 'POST') {
    const { userId, amount, reason } = req.body || {};
    if (!userId || !amount) return jsonErr(res, 400, 'userId and amount required');

    try {
      const refundAmt = parseFloat(amount);
      
      /* Add amount back to user balance */
      await sql`
        UPDATE users 
        SET balance = balance + ${refundAmt}
        WHERE id = ${String(userId)}
      `;

      /* Log refund transaction */
      await sql`
        INSERT INTO transactions (user_id, type, amount, description, status, created_at)
        VALUES (${String(userId)}, 'refund', ${refundAmt}, ${String(reason || 'Withdrawal refund')}, 'completed', NOW())
      `;

      return res.status(200).json({ ok: true, refundedAmount: refundAmt });
    } catch (err) {
      console.error('[payment/refund-balance]', err.message);
      return jsonErr(res, 500, 'Could not process refund', err.message);
    }
  }

  /* ══════════════════════════════════════════════════════════════════
     WALLET DEDUCT — deduct from buyer wallet on wallet payment
  ══════════════════════════════════════════════════════════════════ */
  if (action === 'wallet-deduct' && req.method === 'POST') {
    try {
      const { userId, amount, ref, description } = req.body || {};
      if (!userId || !amount) return jsonErr(res, 400, 'userId and amount required.');
      const amt = parseFloat(amount);
      if (isNaN(amt) || amt <= 0) return jsonErr(res, 400, 'Invalid amount.');

      /* Check balance first */
      const userRows = await sql`SELECT buyer_wallet FROM users WHERE id = ${String(userId)} LIMIT 1`;
      if (!userRows.length) return jsonErr(res, 404, 'User not found.');
      const currentBal = parseFloat(userRows[0].buyer_wallet || 0);
      if (currentBal < amt) return jsonErr(res, 400, 'Insufficient wallet balance.');

      /* Deduct */
      await sql`UPDATE users SET buyer_wallet = buyer_wallet - ${amt} WHERE id = ${String(userId)}`;

      /* Record transaction */
      await sql`
        INSERT INTO wallet_transactions (user_id, type, amount, description, ref, created_at)
        VALUES (${String(userId)}, 'debit', ${-amt}, ${description || 'Purchase'}, ${ref || ''}, NOW())
      `;

      console.log('[payment/wallet-deduct]', userId, '₦' + amt, ref);
      return res.status(200).json({ ok: true, newBalance: currentBal - amt });
    } catch (err) {
      console.error('[payment/wallet-deduct]', err.message);
      return jsonErr(res, 500, 'Could not process wallet payment.', err.message);
    }
  }

  return jsonErr(res, 405, 'Unknown action. Valid: orders | disputes | confirm | dvc-release | refund | order | webhook | check-balance | update-withdrawal-status | refund-balance | wallet-deduct');
};
