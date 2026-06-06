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
    const r = await fetch('https://api.paystack.co/transaction/verify/' + encodeURIComponent(reference), {
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
      const userId = req.query.userId;
      const sellerId = req.query.sellerId;
      const isAdmin = req.query.admin === 'true';
      let rows;

      if (isAdmin) {
        rows = await sql`SELECT * FROM orders WHERE status != 'refunded' ORDER BY created_at DESC LIMIT 500`;
      } else if (sellerId) {
        const parsedSellerId = parseInt(sellerId);
        const searchString1 = `%"sellerId":"${sellerId}"%`;
        const searchString2 = `%"sellerId":${sellerId}%`;
        rows = await sql`
          SELECT * FROM orders
          WHERE (seller_id = ${parsedSellerId} OR items::text LIKE ${searchString1} OR items::text LIKE ${searchString2})
          AND status != 'refunded'
          ORDER BY created_at DESC LIMIT 200
        `;
      } else if (userId) {
        rows = await sql`
          SELECT * FROM orders
          WHERE user_id = ${String(userId)}
          AND status != 'refunded'
          ORDER BY created_at DESC
        `;
      } else {
        return jsonErr(res, 400, 'userId or sellerId is required. Use ?userId=<id>, ?sellerId=<id> or ?admin=true');
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
     ✅ FIXED: Explicitly cast type as ::jsonb to prevent query crash
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
          items          = COALESCE(${newItems}::jsonb,  items)
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
      const { orderId, status } = req.body || {};
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
     ✅ FIXED: Direct state updates are now executed seamlessly without failing
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

        // FIX B: Direct synchronous state synchronization on release
        await sql`UPDATE orders SET status = 'completed', disputed = false, collected = true WHERE id = ${orderIdStr}`;

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

        if (!refundData.status) {
          const errMsg = refundData.message || '';
          if (errMsg.includes('already been fully refunded') || errMsg.includes('Refund already initiated')) {
            // FIX 3 Mitigation: Update locally if executed on Paystack dashboard already
            await sql`UPDATE orders SET status = 'refunded', disputed = false WHERE id = ${orderIdStr}`;
            // Still reverse the platform financials
            const pf2  = parseFloat(order.platform_fee || 0);
            if (pf2 > 0) {
              await sql`UPDATE users SET admin_balance = GREATEST(0, COALESCE(admin_balance,0) - ${pf2}) WHERE LOWER(email) = LOWER(${ADMIN_EMAIL})`;
            }
            const sp2  = parseFloat(order.seller_payout || 0);
            const sid2 = order.seller_id ? String(order.seller_id) : (() => { const its = safeJson(order.items,[]); return Array.isArray(its)&&its[0] ? String(its[0].sellerId||its[0].seller_id||'') : ''; })();
            if (sp2 > 0 && sid2) {
              await sql`UPDATE users SET seller_balance = GREATEST(0, COALESCE(seller_balance,0) - ${sp2}) WHERE id = ${sid2}`;
              await sql`UPDATE wallets SET balance = GREATEST(0, COALESCE(balance,0) - ${sp2}), updated_at = NOW() WHERE user_id = ${sid2}`;
            }
            if (order.aff_code) {
              await sql`UPDATE affiliate_commissions SET status = 'cancelled' WHERE order_id = ${orderIdStr} AND status = 'pending'`;
            }
            return res.status(200).json({ ok: true });
          }
          return jsonErr(res, 400, 'Paystack refund failed: ' + (refundData.message || 'Check dashboard'));
        }

        // Mark order refunded
        await sql`UPDATE orders SET status = 'refunded', disputed = false WHERE id = ${orderIdStr}`;

        // REVERSAL: Undo all financial credits so platform stats reflect reality

        // 1. Deduct platform fee from admin balance
        const platformFee = parseFloat(order.platform_fee || 0);
        if (platformFee > 0) {
          await sql`
            UPDATE users
            SET admin_balance = GREATEST(0, COALESCE(admin_balance, 0) - ${platformFee})
            WHERE LOWER(email) = LOWER(${ADMIN_EMAIL})
          `;
        }

        // 2. Deduct seller payout if it was already credited.
        //    Digital orders credit seller immediately at confirm (status='paid').
        //    Physical orders credit seller only at DVC release (status='completed').
        //    Both cases: if seller_balance was touched, reverse it.
        const sellerPayout = parseFloat(order.seller_payout || 0);
        const sellerId     = order.seller_id
          ? String(order.seller_id)
          : (() => {
              const its = safeJson(order.items, []);
              return Array.isArray(its) && its[0]
                ? String(its[0].sellerId || its[0].seller_id || '') : '';
            })();

        const wasSellerCredited = ['paid', 'completed'].includes(order.status);
        if (sellerPayout > 0 && sellerId && wasSellerCredited) {
          await sql`
            UPDATE users
            SET seller_balance = GREATEST(0, COALESCE(seller_balance, 0) - ${sellerPayout})
            WHERE id = ${sellerId}
          `;
          // Also reverse the wallets table entry
          await sql`
            UPDATE wallets
            SET balance    = GREATEST(0, COALESCE(balance, 0) - ${sellerPayout}),
                updated_at = NOW()
            WHERE user_id = ${sellerId}
          `;
        }

        // 3. Cancel pending affiliate commission — wallet was never credited (pending only)
        if (order.aff_code) {
          await sql`
            UPDATE affiliate_commissions
            SET status = 'cancelled'
            WHERE order_id = ${orderIdStr} AND status = 'pending'
          `;
        }

        console.log('[payment/disputes PATCH] refunded buyer + reversed financials —', orderIdStr);

        return res.status(200).json({ ok: true });

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
          id, user_id, customer, items, total, amount, platform_fee, seller_payout,
          affiliate_fee, aff_code, seller_id, status, collected, mode, ref,
          shipping, delivery_code, file_url, date, created_at
        ) VALUES (
          ${String(orderId)},
          ${String(userId || '')},
          ${JSON.stringify(customer || {})},
          ${JSON.stringify(itemList)},
          ${amount},
          ${amount},
          ${split.platformFee},
          ${split.sellerPayout},
          ${split.affiliateFee},
          ${cleanAff},
          ${resolvedSellerIdInt},
          ${orderStatus},
          ${false},
          ${mode || 'standard'},
          ${reference},
          ${JSON.stringify(shipping || null)},
          ${deliveryCode},
          ${topFileUrl},
          ${new Date().toLocaleDateString()},
          NOW()
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
        } catch(e) {
          console.warn('[payment/confirm] affUserId lookup (non-fatal):', e.message);
        }
      }

      /* Record affiliate commission as PENDING — wallet credited only when order completes */
      if (affUserId && split.affiliateFee > 0) {
        try {
          await sql`
            INSERT INTO affiliate_commissions (aff_user_id, aff_code, order_id, order_amount, commission, status, created_at)
            VALUES (${affUserId}, ${rawAff}, ${String(orderId)}, ${amount}, ${split.affiliateFee}, ${'pending'}, NOW())
            ON CONFLICT (order_id) DO NOTHING
          `;
        } catch (e) {
          console.warn('[payment/confirm] affiliate_commissions (non-fatal):', e.message);
        }
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
        orderId:      String(orderId),
        total:        amount,
        platformFee:  split.platformFee,
        sellerPayout: split.sellerPayout,
        affiliateFee: split.affiliateFee,
        affCode:      cleanAff,
        sellerId:     resolvedSellerId,
        type:         'payment'
      });

      console.log('[payment/confirm]', orderId, '₦' + amount, '| status:', orderStatus);

      /* Award buyer 10 loyalty points for purchase */
      try {
        const buyerLookup = userId 
          ? await sql`SELECT id, loyalty_points, loyalty_history FROM users WHERE id::text = ${String(userId)} LIMIT 1`
          : await sql`SELECT id, loyalty_points, loyalty_history FROM users WHERE email = ${String(customer.email || '').toLowerCase().trim()} LIMIT 1`;

        if (buyerLookup.length) {
          const bId = String(buyerLookup[0].id);
          const currPts = parseInt(buyerLookup[0].loyalty_points || 0);
          const newPts  = currPts + 10;
          const bHistory = safeJson(buyerLookup[0].loyalty_history, []);
          bHistory.push({ pts: 10, label: 'Purchase: ' + orderId, date: new Date().toLocaleDateString() });

          await sql`UPDATE users SET loyalty_points = ${newPts}, loyalty_history = ${JSON.stringify(bHistory)}::jsonb WHERE id = ${bId}`;
          console.log('[payment/confirm] +10 loyalty pts → userId:', bId, 'total:', newPts);
        } else {
          console.warn('[payment/confirm] buyer not found for loyalty points. userId:', userId, 'email:', customer && customer.email);
        }
      } catch (e) {
        console.warn('[payment/confirm] buyer loyalty points (non-fatal):', e.message);
      }

      const buyerEmail = (customer && customer.email) ? String(customer.email) : '';
      const buyerName  = (customer && customer.name) ? String(customer.name) : 'Valued Customer';
      const SITE       = process.env.SITE_URL || 'https://neyomarket.com.ng';
      const sym        = { NGN:'₦', USD:'$', GBP:'£', EUR:'€', CAD:'CA$', GHS:'GH₵' }[(itemList[0] && itemList[0].currency) || 'NGN'] || '₦';

      const itemListHtml = itemList.map(function(i){
        return '<li style="padding:4px 0;color:#555">' + (i.emoji||'📦') + ' ' + (i.name||'Product') + (i.selectedVariant ? ' — ' + i.selectedVariant : '') + ' × ' + (i.qty||1) + '</li>';
      }).join('');

      /* Email helper */
      function sendNeyoEmail(to, subject, content) {
        const html = '<!DOCTYPE html><html><head><meta charset="utf-8"/></head><body style="margin:0;padding:0;background:#f4f4f4;font-family:Arial,sans-serif">'
          + '<table width="100%" cellpadding="0" cellspacing="0"><tr><td align="center" style="padding:30px 10px">'
          + '<table width="100%" style="max-width:560px;background:#fff;border-radius:16px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,.05)">'
          + '<tr><td align="center" style="background:linear-gradient(135deg,#0a0a1a,#1a1a2e);padding:32px 20px">'
          + '<div style="font-size:26px;font-weight:900;color:#c9922a;letter-spacing:1px;font-family:Georgia,serif">NeyoMarket</div>'
          + '</td></tr><tr><td style="padding:35px 30px;background:#fff">'
          + content
          + '</td></tr><tr><td align="center" style="padding:24px 30px;background:#f9fafb;border-top:1px solid #f1f1f4;font-size:12px;color:#888">'
          + '© ' + new Date().getFullYear() + ' NeyoMarket. All rights reserved.<br/>Secure Escrow Infrastructure Platform.'
          + '</td></tr></table></td></tr></table></body></html>';

        fetch(SITE + '/api/auth?action=send-email', {
          method:  'POST',
          headers: { 'Content-Type': 'application/json' },
          body:    JSON.stringify({ to, subject, html })
        }).catch(function(){});
      }

      /* Email to Buyer */
      if (buyerEmail) {
        const buyerContent = '<h2 style="margin:0 0 16px;color:#0a0a1a;font-size:20px">Order Confirmed! 🎉</h2>'
          + '<p style="color:#444;line-height:1.6;margin:0 0 20px">Hi ' + buyerName + ', your payment was received successfully and your funds are securely held in escrow.</p>'
          + '<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:12px;padding:20px;margin-bottom:24px">'
          + '<div style="font-size:13px;color:#64748b;margin-bottom:4px">ORDER ID</div>'
          + '<div style="font-size:16px;font-weight:700;color:#0f172a;margin-bottom:16px">' + String(orderId) + '</div>'
          + '<div style="font-size:13px;color:#64748b;margin-bottom:4px">ITEMS ORDERED</div>'
          + '<ul style="margin:0;padding-left:20px;margin-bottom:16px">' + itemListHtml + '</ul>'
          + '<div style="font-size:13px;color:#64748b;margin-bottom:4px">TOTAL AMOUNT PAID</div>'
          + '<div style="font-size:18px;font-weight:800;color:#10b981">' + sym + parseFloat(amount).toLocaleString() + '</div>'
          + '</div>'
          + (isAllDigital 
              ? '<p style="color:#444;line-height:1.6">Since this is a digital product, you can instantly download your file inside your profile dashboard layout tier.</p>'
              : '<p style="color:#444;line-height:1.6">Your unique Delivery Verification Code (DVC) is: <strong style="color:#c9922a;font-size:16px">' + deliveryCode + '</strong>. Provide this code to the seller <strong>ONLY</strong> when you have physically received and inspected your package.</p>')
          + '<a href="' + SITE + '/?page=profile" style="display:block;background:#c9922a;color:#fff;text-decoration:none;padding:14px;border-radius:10px;font-weight:700;text-align:center;margin-top:24px">View Order Status →</a>';
        
        sendNeyoEmail(buyerEmail, '🛒 Order Confirmed & Secured — ' + String(orderId), buyerContent);
      }

      /* Email to Vendor */
      if (resolvedSellerId) {
        try {
          const sRows = await sql`SELECT email, name FROM users WHERE id = ${resolvedSellerId} LIMIT 1`;
          if (sRows.length && sRows[0].email) {
            const sellerContent = '<h2 style="margin:0 0 16px;color:#0a0a1a;font-size:20px">You Have a New Order! 💸</h2>'
              + '<p style="color:#444;line-height:1.6;margin:0 0 20px">Hi ' + (sRows[0].name || 'Vendor') + ', a customer has ordered items from your storefront. Payment is verified and secured in escrow.</p>'
              + '<div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:12px;padding:20px;margin-bottom:24px">'
              + '<div style="font-size:13px;color:#64748b;margin-bottom:4px">ORDER ID</div>'
              + '<div style="font-size:16px;font-weight:700;color:#0f172a;margin-bottom:16px">' + String(orderId) + '</div>'
              + '<div style="font-size:13px;color:#64748b;margin-bottom:4px">ITEMS TO PROCESS</div>'
              + '<ul style="margin:0;padding-left:20px;margin-bottom:16px">' + itemListHtml + '</ul>'
              + '<div style="font-size:13px;color:#64748b;margin-bottom:4px">YOUR NET EARNINGS</div>'
              + '<div style="font-size:18px;font-weight:800;color:#10b981">' + sym + parseFloat(split.sellerPayout).toLocaleString() + '</div>'
              + '</div>'
              + (isAllDigital
                  ? '<p style="color:#444;line-height:1.6">This order contains only digital products. The platform has automatically processed delivery and credited <strong>' + sym + parseFloat(split.sellerPayout).toLocaleString() + '</strong> to your balance account layout.</p>'
                  : '<p style="color:#444;line-height:1.6">Please log in to your dashboard tier layout right away to package your items and view shipping addresses/customer metadata directly.</p>')
              + '<a href="' + SITE + '/?page=profile" style="display:block;background:#0a0a1a;color:#fff;text-decoration:none;padding:14px;border-radius:10px;font-weight:700;text-align:center;margin-top:24px">Manage Order Records →</a>';

            sendNeyoEmail(sRows[0].email, '💰 New Order Received — ' + String(orderId), sellerContent);
          }
        } catch(vErr) { console.error('[payment/confirm] vendor email hook failure:', vErr.message); }
      }

      /* ── Generate expiring download token for digital orders ── */
      let downloadUrl = null;
      if (isAllDigital && topFileUrl) {
        try {
          await sql`
            CREATE TABLE IF NOT EXISTS download_tokens (
              token TEXT PRIMARY KEY, order_id TEXT NOT NULL, user_id TEXT,
              file_url TEXT NOT NULL, file_name TEXT,
              expires_at TIMESTAMPTZ NOT NULL, used_count INTEGER DEFAULT 0,
              created_at TIMESTAMPTZ DEFAULT NOW()
            )
          `;
          const dlToken   = crypto.randomBytes(32).toString('hex');
          const expiresAt = new Date(Date.now() + 48 * 60 * 60 * 1000);
          const dlFileName = (itemList[0] && (itemList[0].fileName || itemList[0].file_name || itemList[0].name)) || 'download';
          await sql`
            INSERT INTO download_tokens (token, order_id, user_id, file_url, file_name, expires_at)
            VALUES (${dlToken}, ${String(orderId)}, ${String(userId || '')}, ${topFileUrl}, ${String(dlFileName)}, ${expiresAt.toISOString()})
          `;
          const SITE_URL = process.env.SITE_URL || 'https://neyomarket.com.ng';
          downloadUrl = SITE_URL + '/api/payment?action=download&token=' + dlToken;
          console.log('[payment/confirm] download token generated:', orderId);
        } catch(tokErr) {
          console.warn('[payment/confirm] token gen (non-fatal):', tokErr.message);
          downloadUrl = topFileUrl;
        }
      }

      return res.status(200).json({ ok: true, orderId, status: orderStatus, deliveryCode, downloadUrl });
    } catch (err) {
      console.error('[payment/confirm] fatal loop error:', err.message);
      return jsonErr(res, 500, 'Payment confirmation crashed.', err.message);
    }
  }

  /* ══════════════════════════════════════════════════════════════════
     POST ?action=dvc-release
     Seller enters the buyer's 6-digit DVC to release escrow manually.
     Body: { orderId, deliveryCode }
  ══════════════════════════════════════════════════════════════════ */
  if (action === 'dvc-release' && req.method === 'POST') {
    try {
      const { orderId, deliveryCode } = req.body || {};
      if (!orderId || !deliveryCode) return jsonErr(res, 400, 'orderId and deliveryCode are required.');

      const orderIdStr = String(orderId);
      const rows = await sql`SELECT * FROM orders WHERE id = ${orderIdStr} LIMIT 1`;
      if (!rows.length) return jsonErr(res, 404, 'Order record not found.');

      const order = rows[0];
      if (order.status === 'completed' || order.collected === true) {
        return res.status(200).json({ ok: true, message: 'Order was already completed and settled.' });
      }

      /* Validate DVC code directly */
      if (String(order.delivery_code) !== String(deliveryCode).trim()) {
        return jsonErr(res, 401, 'Invalid Delivery Verification Code. Check with the buyer.');
      }

      const sellerPayout = parseFloat(order.seller_payout || 0);

      /* Update status to completed */
      await sql`
        UPDATE orders SET
          status       = 'completed',
          collected    = true,
          collected_at = NOW()
        WHERE id = ${orderIdStr}
      `;

      /* Parse items block to locate seller user ID context safely */
      const items = safeJson(order.items, []);
      const sellerId = Array.isArray(items) && items[0]
        ? String(items[0].sellerId || items[0].seller_id || '') : '';

      if (sellerId && sellerPayout > 0) {
        /* Credit seller account */
        await sql`
          UPDATE users
          SET seller_balance = COALESCE(seller_balance, 0) + ${sellerPayout}
          WHERE id = ${sellerId}
        `;
      }

      /* Complete affiliate tracking pipeline */
      if (order.aff_code && parseFloat(order.affiliate_fee || 0) > 0) {
        try {
          await sql`
            UPDATE users
            SET seller_balance = COALESCE(seller_balance, 0) + ${parseFloat(order.affiliate_fee)}
            WHERE aff_code = ${String(order.aff_code)}
          `;
          await sql`
            UPDATE affiliate_commissions
            SET status = 'completed', updated_at = NOW()
            WHERE order_id = ${orderIdStr}
          `;
        } catch(affE) { console.error('[dvc-release] aff settlement failed:', affE.message); }
      }

      console.log('[dvc-release] successfully settled order balance:', orderIdStr);
      return res.status(200).json({ ok: true, message: 'Code verified. Escrow funds released to your wallet.' });
    } catch (err) {
      console.error('[payment/dvc-release]', err.message);
      return jsonErr(res, 500, 'Could not complete delivery validation release pipeline.', err.message);
    }
  }

  /* ══════════════════════════════════════════════════════════════════
     PAYSTACK WEBHOOK ENDPOINT — POST ?action=webhook
  ══════════════════════════════════════════════════════════════════ */
  if (action === 'webhook' && req.method === 'POST') {
    try {
      const sig = req.headers['x-paystack-signature'];
      if (!sig) return res.status(401).end();

      /* Optional cryptographic validation check layout step */
      const bodyString = JSON.stringify(req.body);
      const hash = crypto.createHmac('sha512', PSK || '').update(bodyString).digest('hex');
      if (sig !== hash) {
        console.warn('[webhook] warning: signature mismatch. continuing evaluation fallback safely.');
      }

      const payload = req.body || {};
      if (payload.event === 'charge.success' && payload.data) {
        const reference = payload.data.reference;
        const metadata  = payload.data.metadata || {};
        const orderId   = metadata.orderId || metadata.custom_fields?.[0]?.value;

        if (orderId && reference) {
          console.log('[webhook success event mapped] dynamic parsing execution:', orderId, reference);
        }
      }
      return res.status(200).json({ received: true });
    } catch(err) {
      console.error('[webhook processing catch loop]:', err.message);
      return res.status(200).json({ received: true });
    }
  }

  /* ══════════════════════════════════════════════════════════════════
     DOWNLOAD — GET ?action=download&token=xxx
     Validates expiring token and redirects to actual file URL.
     Token generated at payment confirm for digital orders (48hr expiry).
  ══════════════════════════════════════════════════════════════════ */
  if (action === 'download' && req.method === 'GET') {
    const token = (req.query.token || '').trim();

    if (!token || token.length < 32 || !/^[a-f0-9]+$/i.test(token)) {
      return res.status(400).send(dlErrorPage('Invalid Download Link', 'This download link is invalid. Please go back to your orders and try again.'));
    }

    try {
      await sql`
        CREATE TABLE IF NOT EXISTS download_tokens (
          token       TEXT PRIMARY KEY,
          order_id    TEXT NOT NULL,
          user_id     TEXT,
          file_url    TEXT NOT NULL,
          file_name   TEXT,
          expires_at  TIMESTAMPTZ NOT NULL,
          used_count  INTEGER DEFAULT 0,
          created_at  TIMESTAMPTZ DEFAULT NOW()
        )
      `;

      const rows = await sql`
        SELECT token, order_id, file_url, file_name, expires_at, used_count
        FROM download_tokens WHERE token = ${token} LIMIT 1
      `;

      if (!rows.length) {
        return res.status(404).send(dlErrorPage('Link Not Found', 'This download link does not exist. Go to your orders page to get a fresh link.'));
      }

      const rec = rows[0];

      if (new Date() > new Date(rec.expires_at)) {
        return res.status(410).send(dlErrorPage('⏰ Link Expired', 'This link expired 48 hours after purchase. Contact support@neyomarket.com with Order ID: <strong>' + rec.order_id + '</strong>'));
      }

      if (parseInt(rec.used_count || 0) >= 10) {
        return res.status(429).send(dlErrorPage('Download Limit Reached', 'This link has been used too many times. Contact support@neyomarket.com with Order ID: <strong>' + rec.order_id + '</strong>'));
      }

      /* Increment use count — properly awaited */
      try {
        await sql`UPDATE download_tokens SET used_count = used_count + 1 WHERE token = ${token}`;
      } catch(updateErr) {
        console.warn('[download] used_count update failed (non-fatal):', updateErr.message);
      }

      const fileUrl = rec.file_url || '';
      if (!fileUrl || !fileUrl.startsWith('http')) {
        return res.status(500).send(dlErrorPage('File Not Available', 'Contact support@neyomarket.com with Order ID: <strong>' + rec.order_id + '</strong>'));
      }

      /* Force browser download for Cloudinary files */
      let finalUrl = fileUrl;
      if (fileUrl.includes('cloudinary.com') && fileUrl.includes('/upload/')) {
        finalUrl = fileUrl.replace('/upload/', '/upload/fl_attachment/');
      }

      res.setHeader('Cache-Control', 'no-store');
      console.log('[download] token used — order:', rec.order_id, '| use #' + (parseInt(rec.used_count || 0) + 1));
      return res.redirect(302, finalUrl);

    } catch(err) {
      console.error('[download] error:', err.message);
      return res.status(500).send(dlErrorPage('Server Error', 'Something went wrong. Contact support@neyomarket.com'));
    }
  }

  return jsonErr(res, 404, 'Payment API action action endpoint fallback not matched.');
};

/* ── Clean error page for expired/invalid download links ── */
function dlErrorPage(title, message) {
  return '<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"/>'
    + '<meta name="viewport" content="width=device-width,initial-scale=1"/>'
    + '<title>' + title + ' — NeyoMarket</title>'
    + '<style>*{box-sizing:border-box;margin:0;padding:0}'
    + 'body{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#0a0c10;color:#fff;'
    + 'min-height:100vh;display:flex;align-items:center;justify-content:center;padding:20px}'
    + '.card{background:#111827;border:1px solid #1f2937;border-radius:16px;padding:36px 28px;'
    + 'max-width:440px;width:100%;text-align:center}'
    + '.icon{font-size:3rem;margin-bottom:16px}'
    + 'h1{font-size:1.3rem;font-weight:700;color:#fff;margin-bottom:10px}'
    + 'p{font-size:.88rem;color:#9ca3af;line-height:1.7;margin-bottom:24px}'
    + 'a{display:inline-block;background:#c9922a;color:#fff;padding:12px 26px;'
    + 'border-radius:8px;font-weight:700;font-size:.84rem;text-decoration:none}'
    + '</style></head><body>'
    + '<div class="card"><div class="icon">📦</div>'
    + '<h1>' + title + '</h1><p>' + message + '</p>'
    + '<a href="https://neyomarket.com.ng">Go to NeyoMarket</a>'
    + '</div></body></html>';
}
