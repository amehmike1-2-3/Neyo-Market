// /api/transactions.js — NeyoMarket Escrow & Commission Engine 
// Routes:
//   POST ?action=release        — release escrow from buyer to seller
//   POST ?action=record         — record a payment in admin_transactions
//   GET  ?action=admin          — fetch all transactions for admin dashboard
//   GET  ?action=seller&userId= — fetch transactions for one seller
//
// Commission model:
//   With valid affiliate referral  → Seller 80%, Platform 15%, Affiliate 5%  (digital)
//   With valid affiliate referral  → Seller 88%, Platform  7%, Affiliate 5%  (physical)
//   No referral, digital           → Seller 90%, Platform 10%, Affiliate  0%
//   No referral, physical          → Seller 95%, Platform  5%, Affiliate  0%
//
// Every successful payment writes a row to admin_transactions for full admin visibility.

'use strict';

const { neon } = require('@neondatabase/serverless');
const sql = neon(process.env.DATABASE_URL);

function cors(res) {
  res.setHeader('Access-Control-Allow-Origin',  '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.setHeader('X-Content-Type-Options',       'nosniff');
}

function jsonErr(res, status, msg, detail) {
  return res.status(status).json({ ok: false, error: msg, detail: detail || null });
}

/* Compute revenue split — same logic as frontend markCol/markDL */
function computeSplit(total, hasPhysical, hasValidAff) {
  var platformRate;
  var affiliateRate;
  if (hasValidAff) {
    platformRate  = hasPhysical ? 0.07  : 0.15;
    affiliateRate = 0.05;
  } else {
    platformRate  = hasPhysical ? 0.05  : 0.10;
    affiliateRate = 0;
  }
  var sellerRate   = 1 - platformRate - affiliateRate;
  return {
    platformFee:   Math.round(total * platformRate),
    affiliateFee:  Math.round(total * affiliateRate),
    sellerPayout:  Math.round(total * sellerRate),
    platformRate,
    affiliateRate,
    sellerRate
  };
}

module.exports = async function handler(req, res) {
  cors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action || '';

  try {

    /* ═══════════════════════════════════════════════════════════════
       POST ?action=release
       Triggered when buyer confirms receipt (digital download or
       physical delivery code entry).
       1. Validates order exists and is not already completed
       2. Computes tiered split
       3. Credits seller_balance in users table
       4. Credits affiliate balance if valid aff_code
       5. Writes row to admin_transactions for admin visibility
       6. Updates order status → completed
    ═══════════════════════════════════════════════════════════════ */
    if (req.method === 'POST' && action === 'release') {
      const { orderId, releasedBy } = req.body || {};
      if (!orderId) return jsonErr(res, 400, 'orderId is required.');

      /* Load order */
      const orders = await sql`
        SELECT * FROM orders WHERE id = ${String(orderId)} LIMIT 1
      `;
      if (!orders.length) return jsonErr(res, 404, 'Order not found: ' + orderId);

      const order = orders[0];

      /* Idempotency — don't double-release */
      if (order.status === 'completed' || order.collected) {
        return res.status(200).json({
          ok:      true,
          message: 'Order already completed.',
          orderId: orderId
        });
      }

      /* Parse items */
      let items = order.items;
      if (typeof items === 'string') { try { items = JSON.parse(items); } catch(e) { items = []; } }
      if (!Array.isArray(items)) items = [];

      const hasPhysical = items.some(function(i) { return i.type === 'physical'; });

      /* Validate affiliate code */
      const rawAff = order.aff_code ? String(order.aff_code).trim() : '';
      const hasValidAff = rawAff.length > 2;

      const total  = parseFloat(order.total || 0);
      const split  = computeSplit(total, hasPhysical, hasValidAff);
      const now    = new Date().toISOString();

      /* Find seller from items */
      const sellerIdFromItem = items[0]
        ? String(items[0].sellerId || items[0].seller_id || '')
        : '';

      /* Credit seller balance */
      if (sellerIdFromItem) {
        await sql`
          UPDATE users
          SET seller_balance = COALESCE(seller_balance, 0) + ${split.sellerPayout}
          WHERE id = ${sellerIdFromItem}
        `;
      }

      /* Credit affiliate balance if applicable */
      if (hasValidAff && split.affiliateFee > 0) {
        try {
          await sql`
            UPDATE users
            SET seller_balance = COALESCE(seller_balance, 0) + ${split.affiliateFee}
            WHERE aff_code = ${rawAff}
          `;
        } catch(affErr) {
          console.error('[transactions.js] Affiliate credit error (non-fatal):', affErr.message);
        }
      }

      /* Mark order completed */
      await sql`
        UPDATE orders SET
          status        = 'completed',
          collected     = true,
          collected_at  = ${now},
          platform_fee  = ${split.platformFee},
          seller_payout = ${split.sellerPayout},
          affiliate_fee = ${split.affiliateFee}
        WHERE id = ${String(orderId)}
      `;

      /* Write to admin_transactions for full admin visibility */
      try {
        await sql`
          INSERT INTO admin_transactions (
            order_id, total, platform_fee, seller_payout,
            affiliate_fee, aff_code, seller_id,
            released_by, type, created_at
          ) VALUES (
            ${String(orderId)},
            ${total},
            ${split.platformFee},
            ${split.sellerPayout},
            ${split.affiliateFee},
            ${rawAff || null},
            ${sellerIdFromItem || null},
            ${releasedBy ? String(releasedBy) : 'system'},
            ${'escrow_release'},
            NOW()
          )
          ON CONFLICT DO NOTHING
        `;
      } catch(txErr) {
        /* Non-fatal — log and continue. Table may not exist yet. */
        console.warn('[transactions.js] admin_transactions write failed (non-fatal):', txErr.message);
      }

      console.log('[transactions.js] Released escrow for order', orderId,
        '| seller ₦', split.sellerPayout,
        '| platform ₦', split.platformFee,
        '| affiliate ₦', split.affiliateFee);

      return res.status(200).json({
        ok:           true,
        orderId:      orderId,
        total:        total,
        platformFee:  split.platformFee,
        sellerPayout: split.sellerPayout,
        affiliateFee: split.affiliateFee,
        hasValidAff:  hasValidAff,
        message:      'Escrow released. Seller credited ₦' + split.sellerPayout.toLocaleString()
      });
    }

    /* ═══════════════════════════════════════════════════════════════
       POST ?action=record
       Called by webhook or payment confirm endpoint to record every
       successful payment in admin_transactions table.
    ═══════════════════════════════════════════════════════════════ */
    if (req.method === 'POST' && action === 'record') {
      const { orderId, total, platformFee, sellerPayout, affiliateFee,
              affCode, sellerId, type } = req.body || {};
      if (!orderId || !total) return jsonErr(res, 400, 'orderId and total required.');

      await sql`
        INSERT INTO admin_transactions (
          order_id, total, platform_fee, seller_payout,
          affiliate_fee, aff_code, seller_id, released_by, type, created_at
        ) VALUES (
          ${String(orderId)},
          ${parseFloat(total)},
          ${parseFloat(platformFee || 0)},
          ${parseFloat(sellerPayout || 0)},
          ${parseFloat(affiliateFee || 0)},
          ${affCode  ? String(affCode)  : null},
          ${sellerId ? String(sellerId) : null},
          ${'payment'},
          ${type || 'payment'},
          NOW()
        )
        ON CONFLICT DO NOTHING
      `;

      return res.status(201).json({ ok: true, message: 'Transaction recorded.' });
    }

    /* ═══════════════════════════════════════════════════════════════
       GET ?action=admin
       Returns all transactions for admin dashboard with totals.
    ═══════════════════════════════════════════════════════════════ */
    if (req.method === 'GET' && action === 'admin') {
      let rows;
      try {
        rows = await sql`
          SELECT * FROM admin_transactions ORDER BY created_at DESC LIMIT 500
        `;
      } catch(e) {
        /* Table may not exist yet — return empty gracefully */
        if (e.message && e.message.includes('does not exist')) {
          return res.status(200).json({ ok: true, transactions: [], totals: { revenue: 0, platformFees: 0, sellerPayouts: 0, affiliateFees: 0 } });
        }
        throw e;
      }

      const totals = rows.reduce(function(acc, r) {
        acc.revenue       += parseFloat(r.total          || 0);
        acc.platformFees  += parseFloat(r.platform_fee   || 0);
        acc.sellerPayouts += parseFloat(r.seller_payout  || 0);
        acc.affiliateFees += parseFloat(r.affiliate_fee  || 0);
        return acc;
      }, { revenue: 0, platformFees: 0, sellerPayouts: 0, affiliateFees: 0 });

      return res.status(200).json({ ok: true, transactions: rows, totals });
    }

    /* ═══════════════════════════════════════════════════════════════
       GET ?action=seller&userId=xxx
       Returns transactions for one seller.
    ═══════════════════════════════════════════════════════════════ */
    if (req.method === 'GET' && action === 'seller') {
      const userId = req.query.userId;
      if (!userId) return jsonErr(res, 400, 'userId required.');

      let rows;
      try {
        rows = await sql`
          SELECT * FROM admin_transactions
          WHERE seller_id = ${String(userId)}
          ORDER BY created_at DESC
          LIMIT 200
        `;
      } catch(e) {
        if (e.message && e.message.includes('does not exist')) {
          return res.status(200).json({ ok: true, transactions: [] });
        }
        throw e;
      }

      return res.status(200).json({ ok: true, transactions: rows });
    }

    return jsonErr(res, 400, 'Unknown action. Use: release | record | admin | seller');

  } catch (err) {
    /* Always JSON — never HTML */
    console.error('[transactions.js] ERROR:', err.message);
    return jsonErr(res, 500, 'Internal server error.', err.message);
  }
};

