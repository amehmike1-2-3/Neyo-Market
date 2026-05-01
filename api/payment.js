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

/* Tiered commission split — matches index.html markCol/markDL exactly */
function computeSplit(total, hasPhysical, hasValidAff) {
  let platformRate, affiliateRate;
  if (hasValidAff) {
    platformRate  = hasPhysical ? 0.07 : 0.15;
    affiliateRate = 0.05;
  } else {
    platformRate  = hasPhysical ? 0.05 : 0.10;
    affiliateRate = 0;
  }
  const sellerRate   = 1 - platformRate - affiliateRate;
  const platformFee  = Math.round(total * platformRate);
  const affiliateFee = Math.round(total * affiliateRate);
  const sellerPayout = Math.round(total * sellerRate);
  return { platformFee, affiliateFee, sellerPayout };
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
      const userId  = req.query.userId;
      const isAdmin = req.query.admin === 'true';
      let rows;

      if (isAdmin) {
        rows = await sql`SELECT * FROM orders ORDER BY created_at DESC LIMIT 500`;
      } else if (userId) {
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
      const affCode = (o.affCode && String(o.affCode).trim().length > 2)
        ? String(o.affCode).trim() : null;

      await sql`
        INSERT INTO orders (
          id, user_id, customer, items, total, platform_fee, seller_payout,
          affiliate_fee, aff_code, status, collected, mode, ref,
          shipping, delivery_code, file_url, date, created_at
        ) VALUES (
          ${String(o.id)},
          ${String(o.userId || '')},
          ${JSON.stringify(o.customer || {})},
          ${JSON.stringify(o.items    || [])},
          ${parseFloat(o.total)},
          ${parseFloat(o.platformFee  || 0)},
          ${parseFloat(o.sellerPayout || 0)},
          ${parseFloat(o.affiliateFee || 0)},
          ${affCode},
          ${o.status || 'paid'},
          ${false},
          ${o.mode   || 'standard'},
          ${o.ref    || ''},
          ${JSON.stringify(o.shipping || null)},
          ${deliveryCode},
          ${o.fileUrl || null},
          ${new Date().toLocaleDateString()},
          NOW()
        )
        ON CONFLICT (id) DO UPDATE SET
          status        = EXCLUDED.status,
          ref           = EXCLUDED.ref,
          delivery_code = EXCLUDED.delivery_code
      `;
      return res.status(201).json({ ok: true, deliveryCode });
    } catch (err) {
      console.error('[payment/orders POST]', err.message);
      return jsonErr(res, 500, 'Could not create order.', err.message);
    }
  }

  /* ══════════════════════════════════════════════════════════════════
     ORDERS — PATCH ?action=orders
     Update any order fields. orderId extracted from URL path or body.
     All conditionals pre-computed before sql template (Neon ternary rule).
  ══════════════════════════════════════════════════════════════════ */
  if (action === 'orders' && req.method === 'PATCH') {
    try {
      const parts   = (req.url || '').split('/').filter(Boolean);
      const orderId = parts[parts.length - 1].split('?')[0];
      if (!orderId) return jsonErr(res, 400, 'orderId required in URL path.');

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
      const existing = await sql`
        SELECT id, status FROM orders WHERE id = ${String(orderId)} LIMIT 1
      `;
      if (existing.length && ['paid','escrow_held','completed'].includes(existing[0].status)) {
        return res.status(200).json({ ok: true, cached: true, orderId, status: existing[0].status });
      }

      let amount = parseFloat(total);
      if (PSK) {
        const txn = await verifyPaystackPayment(reference);
        if (!txn || txn.status !== 'success')
          return jsonErr(res, 402, 'Payment not confirmed by Paystack. Ref: ' + reference);
        amount = txn.amount / 100;
      }

      const itemList    = Array.isArray(items) ? items : [];
      const hasPhysical = itemList.some(function(i) { return i.type === 'physical'; });
      const isAllDigital = itemList.length > 0 && itemList.every(function(i) {
        return i.type === 'digital' || i.type === 'course';
      });

      const rawAff      = (affCode && typeof affCode === 'string') ? affCode.trim() : '';
      const hasValidAff = rawAff.length > 2 && rawAff !== 'GUEST';
      const split       = computeSplit(amount, hasPhysical, hasValidAff);

      let affUserId = null;
      if (hasValidAff && split.affiliateFee > 0) {
        const affRows = await sql`SELECT id FROM users WHERE aff_code = ${rawAff} LIMIT 1`;
        if (affRows.length) affUserId = String(affRows[0].id);
      }

      const resolvedSellerId = sellerUserId
        ? String(sellerUserId)
        : (itemList[0] && (itemList[0].sellerId || itemList[0].seller_id))
          ? String(itemList[0].sellerId || itemList[0].seller_id) : null;

      const orderStatus  = isAllDigital ? 'paid' : 'escrow_held';
      const deliveryCode = generateDVC(String(orderId));
      const cleanAff     = hasValidAff ? rawAff : null;

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

      await sql`
        INSERT INTO orders (
          id, user_id, customer, items, total,
          platform_fee, seller_payout, affiliate_fee, aff_code,
          status, collected, mode, ref, shipping,
          delivery_code, file_url, date, created_at
        ) VALUES (
          ${String(orderId)}, ${String(userId || '')},
          ${JSON.stringify(customer || {})}, ${JSON.stringify(itemList)},
          ${amount}, ${split.platformFee}, ${split.sellerPayout},
          ${split.affiliateFee}, ${cleanAff}, ${orderStatus},
          ${false}, ${mode || 'standard'}, ${reference},
          ${JSON.stringify(shipping || null)}, ${deliveryCode}, ${topFileUrl},
          ${new Date().toLocaleDateString()}, NOW()
        )
        ON CONFLICT (id) DO UPDATE SET
          status        = EXCLUDED.status,
          ref           = EXCLUDED.ref,
          delivery_code = EXCLUDED.delivery_code,
          file_url      = COALESCE(EXCLUDED.file_url, orders.file_url)
      `;

      if (split.platformFee > 0) {
        await sql`
          UPDATE users SET admin_balance = COALESCE(admin_balance, 0) + ${split.platformFee}
          WHERE LOWER(email) = LOWER(${ADMIN_EMAIL})
        `;
      }

      if (affUserId && split.affiliateFee > 0) {
        await sql`
          UPDATE users SET seller_balance = COALESCE(seller_balance, 0) + ${split.affiliateFee}
          WHERE id = ${affUserId}
        `;
        try {
          await sql`
            INSERT INTO affiliate_commissions
              (aff_user_id, aff_code, order_id, order_amount, commission, status, created_at)
            VALUES (${affUserId}, ${rawAff}, ${String(orderId)}, ${amount}, ${split.affiliateFee}, ${'pending'}, NOW())
            ON CONFLICT (order_id) DO NOTHING
          `;
        } catch (e) { console.warn('[payment/confirm] affiliate_commissions (non-fatal):', e.message); }
      }

      if (isAllDigital && resolvedSellerId && split.sellerPayout > 0) {
        await sql`
          UPDATE users SET seller_balance = COALESCE(seller_balance, 0) + ${split.sellerPayout}
          WHERE id = ${resolvedSellerId}
        `;
      }

      await recordAdminTx({
        orderId: String(orderId), total: amount,
        platformFee: split.platformFee, sellerPayout: split.sellerPayout,
        affiliateFee: split.affiliateFee, affCode: cleanAff,
        sellerId: resolvedSellerId, type: 'payment'
      });

      console.log('[payment/confirm]', orderId, '₦' + amount, '| status:', orderStatus);

      /* ── Email buyer: order confirmation + download link (non-fatal) ──
         Uses fetch to /api/auth?action=send-email so no extra dep needed.
         Falls back gracefully if email service not configured.          */
      const buyerEmail   = (customer && customer.email) ? String(customer.email) : '';
      const buyerName    = (customer && customer.name)  ? String(customer.name)  : 'Valued Customer';
      const productNames = itemList.map(function(i){ return i.name || 'Product'; }).join(', ');
      const SITE         = process.env.SITE_URL || 'https://neyomarket.com.ng';
      if (buyerEmail) {
        try {
          const emailBody = [
            '<!DOCTYPE html><html><body style="font-family:sans-serif;max-width:560px;margin:0 auto;padding:20px">',
            '<div style="background:#c9922a;padding:16px 20px;border-radius:8px 8px 0 0">',
            '<h2 style="color:#fff;margin:0">NeyoMarket — Order Confirmed ✅</h2></div>',
            '<div style="background:#f9fafb;padding:20px;border:1px solid #e5e7eb;border-top:none;border-radius:0 0 8px 8px">',
            '<p>Hi <strong>' + buyerName + '</strong>,</p>',
            '<p>Your order <strong>' + String(orderId) + '</strong> has been confirmed.</p>',
            '<table style="width:100%;border-collapse:collapse;margin:12px 0">',
            '<tr><td style="padding:6px 0;color:#6b7280">Items:</td><td style="padding:6px 0;font-weight:600">' + productNames + '</td></tr>',
            '<tr><td style="padding:6px 0;color:#6b7280">Amount:</td><td style="padding:6px 0;font-weight:600">₦' + amount.toLocaleString() + '</td></tr>',
            '<tr><td style="padding:6px 0;color:#6b7280">Status:</td><td style="padding:6px 0">' + (orderStatus === 'paid' ? '✅ Paid' : '🛡️ Escrow Held') + '</td></tr>',
            '</table>',
            isAllDigital && topFileUrl
              ? '<div style="background:#d1fae5;border:1px solid #a7f3d0;border-radius:8px;padding:14px;margin:14px 0"><p style="margin:0 0 8px;font-weight:700;color:#065f46">⬇️ Your Download Link</p><a href="' + topFileUrl + '" style="background:#059669;color:#fff;padding:10px 20px;border-radius:6px;text-decoration:none;font-weight:700">Download Your Product</a><p style="margin:10px 0 0;font-size:.8rem;color:#6b7280">Link is also available in your Orders page on NeyoMarket.</p></div>'
              : '<p>Track your order at: <a href="' + SITE + '">' + SITE + '</a></p>',
            '<p style="color:#6b7280;font-size:.82rem;margin-top:20px">This is an automated email from NeyoMarket. Reply to this email if you need help.</p>',
            '</div></body></html>'
          ].join('');

          /* Fire-and-forget — don't block the response */
          fetch(SITE + '/api/auth?action=send-email', {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body:    JSON.stringify({
              to:      buyerEmail,
              subject: 'NeyoMarket — Order ' + String(orderId) + ' Confirmed ✅',
              html:    emailBody
            })
          }).catch(function(e){ console.warn('[payment/confirm] email send (non-fatal):', e.message); });

        } catch(emailErr) {
          console.warn('[payment/confirm] email build error (non-fatal):', emailErr.message);
        }
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

      const affCode = order.aff_code ? String(order.aff_code).trim() : '';
      if (affCode.length > 2 && affiliateFee > 0) {
        try {
          await sql`
            UPDATE users SET seller_balance = COALESCE(seller_balance, 0) + ${affiliateFee}
            WHERE aff_code = ${affCode}
          `;
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

      await sql`UPDATE orders SET status = 'refunded', collected = false WHERE id = ${String(orderId)}`;

      console.log('[payment/refund]', orderId, 'ref:', reference);
      return res.status(200).json({
        ok: true, orderId,
        message: 'Refund of ₦' + parseFloat(amount || 0).toLocaleString() + ' initiated.'
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
     POST ?action=webhook
     Paystack charge.success fallback (primary handler is /api/webhook.js).
     Sets order to 'escrow_held' on charge.success event.
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
      if (ref) {
        try {
          await sql`
            UPDATE orders SET status = 'escrow_held'
            WHERE ref = ${ref}
              AND status NOT IN ('paid','escrow_held','completed','refunded')
          `;
          console.log('[payment/webhook] charge.success:', ref);
        } catch (e) {
          console.error('[payment/webhook] DB error:', e.message);
        }
      }
    }

    return res.status(200).json({ received: true });
  }

  return jsonErr(res, 405, 'Unknown action. Valid: orders | disputes | confirm | dvc-release | refund | order | webhook');
};
