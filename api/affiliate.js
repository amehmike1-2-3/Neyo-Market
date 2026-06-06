// /api/affiliate.js — NeyoMarket Affiliate + Analytics API (combined)
const { neon } = require('@neondatabase/serverless');

const sql = neon(process.env.DATABASE_URL);

function cors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}

module.exports = async function handler(req, res) {
  cors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action;

  try {

    /* ══════════════════════════════════════════════════
       AFFILIATE — record commission after a sale
    ══════════════════════════════════════════════════ */
    if (action === 'record') {
      if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });
      const { affCode, orderId, amount, commission, productId } = req.body || {};

      if (!affCode || !orderId || !commission)
        return res.status(400).json({ error: 'affCode, orderId and commission required.' });

      const users = await sql`SELECT id, loyalty_points, loyalty_history FROM users WHERE aff_code = ${affCode} LIMIT 1`;
      if (!users.length) return res.status(200).json({ ok: true, skipped: 'Affiliate not found' });

      await sql`
        INSERT INTO affiliate_commissions
          (aff_user_id, aff_code, order_id, order_amount, commission, product_id, status, created_at)
        VALUES
          (${String(users[0].id)}, ${affCode}, ${String(orderId)}, ${amount || 0}, ${commission}, ${String(productId || '')}, ${'pending'}, NOW())
        ON CONFLICT (order_id) DO NOTHING
      `;

      // Update wallet for this affiliate
      await sql`
        INSERT INTO wallets (user_id, referral_earnings, pending_balance, balance, updated_at)
        VALUES (${String(users[0].id)}, ${commission}, ${commission}, 0, NOW())
        ON CONFLICT (user_id) DO UPDATE SET
          referral_earnings = wallets.referral_earnings + ${commission},
          pending_balance   = wallets.pending_balance + ${commission},
          updated_at = NOW()
      `;

      /* Award referrer 50 loyalty points for successful referral */
      try {
        const currentPts = parseInt(users[0].loyalty_points || 0);
        const newPts     = currentPts + 50;
        const history    = users[0].loyalty_history || [];
        history.push({ pts: 50, label: 'Referral bonus: Order ' + orderId, date: new Date().toLocaleDateString() });
        await sql`
          UPDATE users
          SET loyalty_points  = ${newPts},
              loyalty_history = ${JSON.stringify(history)}::jsonb
          WHERE id = ${String(users[0].id)}
        `;
      } catch (e) { console.warn('[affiliate/record] loyalty points (non-fatal):', e.message); }

      return res.status(201).json({ ok: true });
    }

    /* ══════════════════════════════════════════════════
       AFFILIATE — get stats + commission history
    ══════════════════════════════════════════════════ */
    if (action === 'stats') {
      const affCode = req.query.affCode;
      if (!affCode) return res.status(200).json({ totalEarned: 0, pendingComm: 0, sales: 0, referrals: 0, commissions: [] });

      const rows = await sql`
        SELECT * FROM affiliate_commissions ac
        LEFT JOIN orders o ON ac.order_id = o.id
        WHERE ac.aff_code = ${affCode}
          AND (o.status IS NULL OR o.status != 'refunded')
        ORDER BY ac.created_at DESC
      `;

      const totalEarned = rows
        .filter(r => r.status === 'paid')
        .reduce((s, r) => s + parseFloat(r.commission || 0), 0);

      const pendingComm = rows
        .filter(r => r.status === 'pending')
        .reduce((s, r) => s + parseFloat(r.commission || 0), 0);

      return res.status(200).json({
        totalEarned:  Math.round(totalEarned),
        pendingComm:  Math.round(pendingComm),
        sales:        rows.length,
        referrals:    rows.length,
        commissions:  rows
      });
    }

    /* ══════════════════════════════════════════════════
       ANALYTICS — ADMIN (full platform stats)
    ══════════════════════════════════════════════════ */
    if (action === 'analytics-admin') {
      const role = req.query.role || '';
      if (role !== 'admin') return res.status(403).json({ error: 'Admin only.' });

      // Totals (with refunded exclusion)
      const [usersRow]    = await sql`SELECT COUNT(*) AS count FROM users`;
      const [prodsRow]    = await sql`SELECT COUNT(*) AS count FROM products WHERE status = 'active'`;
      const [ordersRow]   = await sql`SELECT COUNT(*) AS count FROM orders WHERE status != 'refunded'`;
      const [revenueRow]  = await sql`
        SELECT COALESCE(SUM(total),0) AS total FROM orders WHERE status != 'refunded'
      `;

      let newUsersRow = { count: 0 };
      try {
        const [r] = await sql`
          SELECT COUNT(*) AS count FROM users
          WHERE created_at >= NOW() - INTERVAL '30 days'
        `;
        newUsersRow = r;
      } catch(e) { /* created_at column absent — skip */ }

      const [affPaidRow]  = await sql`
        SELECT COALESCE(SUM(commission),0) AS total FROM affiliate_commissions ac
        LEFT JOIN orders o ON ac.order_id = o.id
        WHERE ac.status = 'paid' AND (o.status IS NULL OR o.status != 'refunded')
      `;
      const [affPendRow]  = await sql`
        SELECT COALESCE(SUM(commission),0) AS total FROM affiliate_commissions ac
        LEFT JOIN orders o ON ac.order_id = o.id
        WHERE ac.status = 'pending' AND (o.status IS NULL OR o.status != 'refunded')
      `;

      // ... (rest of admin analytics stays the same with filters added where needed)
      const ordersByStatus = await sql`
        SELECT status, COUNT(*) AS count FROM orders WHERE status != 'refunded' GROUP BY status ORDER BY count DESC
      `;

      const topProducts = await sql`
        SELECT p.name, p.price, COUNT(o.id) AS sales,
               COALESCE(SUM(o.total),0) AS revenue
        FROM orders o
        JOIN products p ON o.product_id::text = p.id::text
        WHERE o.status != 'refunded'
        GROUP BY p.id, p.name, p.price
        ORDER BY sales DESC LIMIT 5
      `;

      const dailyOrders = await sql`
        SELECT TO_CHAR(date::date,'Mon DD') AS day,
               COUNT(*) AS orders,
               COALESCE(SUM(total),0) AS revenue
        FROM orders
        WHERE date::date >= (NOW() - INTERVAL '30 days')::date
          AND status != 'refunded'
        GROUP BY date::date, day
        ORDER BY date::date ASC
      `;

      const topAffiliates = await sql`
        SELECT aff_code, COUNT(*) AS referrals,
               COALESCE(SUM(commission),0) AS earned
        FROM affiliate_commissions ac
        LEFT JOIN orders o ON ac.order_id = o.id
        WHERE (o.status IS NULL OR o.status != 'refunded')
        GROUP BY aff_code
        ORDER BY earned DESC LIMIT 5
      `;

      const adminWalletRows = await sql`
        SELECT COALESCE(balance,0) AS total_balance,
               COALESCE(pending_balance,0) AS total_pending
        FROM wallets
        WHERE user_id = 'master_admin_001'
        LIMIT 1
      `;
      const adminWallet = adminWalletRows[0] || {};
      const totalRevenueFromOrders = parseFloat(revenueRow.total || 0);
      const adminBalance = parseFloat(adminWallet.total_balance || 0);
      const adminPending = parseFloat(adminWallet.total_pending || 0);

      const platformRevenue = adminBalance > 0
        ? Math.round(adminBalance)
        : Math.round(totalRevenueFromOrders * 0.10);
      const platformPending = adminPending > 0 ? Math.round(adminPending) : 0;

      return res.status(200).json({
        totalUsers:       parseInt(usersRow.count || 0),
        totalProducts:    parseInt(prodsRow.count || 0),
        totalOrders:      parseInt(ordersRow.count || 0),
        totalRevenue:     Math.round(totalRevenueFromOrders),
        platformRevenue,
        platformPending,
        totalAffPaid:     Math.round(parseFloat(affPaidRow.total || 0)),
        totalAffPending:  Math.round(parseFloat(affPendRow.total || 0)),
        newUsersMonth:    parseInt(newUsersRow.count || 0),
        ordersByStatus,
        topProducts,
        dailyOrders,
        topAffiliates
      });
    }

    /* ══════════════════════════════════════════════════
       ANALYTICS — SELLER (own stats only)
    ══════════════════════════════════════════════════ */
    if (action === 'analytics-seller') {
      const userId = req.query.userId || '';
      if (!userId) return res.status(400).json({ error: 'userId required.' });

      const [myProds] = await sql`
        SELECT COUNT(*) AS count FROM products
        WHERE seller_id::text = ${String(userId)} AND status = 'active'
      `;
      const [myOrders] = await sql`
        SELECT COUNT(*) AS count FROM orders
        WHERE seller_id = ${parseInt(userId)} AND status != 'refunded'
      `;
      const [myPending] = await sql`
        SELECT COUNT(*) AS count FROM orders
        WHERE seller_id = ${parseInt(userId)}
        AND LOWER(status) LIKE '%pending%' AND status != 'refunded'
      `;
      const [myRevenue] = await sql`
        SELECT COALESCE(SUM(amount * 0.9),0) AS total FROM orders
        WHERE seller_id = ${parseInt(userId)} AND status != 'refunded'
      `;

      const walletRows = await sql`
        SELECT COALESCE(balance,0) AS balance,
               COALESCE(pending_balance,0) AS pending_balance
        FROM wallets WHERE user_id::text = ${String(userId)} LIMIT 1
      `;
      const wallet = walletRows[0] || {};
      const walletBalance = parseFloat(wallet.balance || 0);
      const walletPending = parseFloat(wallet.pending_balance || 0);

      const myTopProds = await sql`
        SELECT p.name, p.price, COUNT(o.id) AS sales,
               COALESCE(SUM(o.amount * 0.9),0) AS revenue
        FROM orders o
        JOIN products p ON o.product_id::text = p.id::text
        WHERE o.seller_id = ${parseInt(userId)} AND o.status != 'refunded'
        GROUP BY p.id, p.name, p.price
        ORDER BY sales DESC LIMIT 5
      `;

      const myDailyOrders = await sql`
        SELECT TO_CHAR(date::date,'Mon DD') AS day,
               COUNT(*) AS orders,
               COALESCE(SUM(amount * 0.9),0) AS revenue
        FROM orders
        WHERE seller_id = ${parseInt(userId)}
          AND date::date >= (NOW() - INTERVAL '30 days')::date
          AND status != 'refunded'
        GROUP BY date::date, day
        ORDER BY date::date ASC
      `;

      const affRows = await sql`
        SELECT * FROM affiliate_commissions ac
        LEFT JOIN orders o ON ac.order_id = o.id
        WHERE ac.aff_user_id::text = ${String(userId)}
          AND (o.status IS NULL OR o.status != 'refunded')
        ORDER BY ac.created_at DESC
      `;

      return res.status(200).json({
        totalProducts:  parseInt(myProds.count || 0),
        totalOrders:    parseInt(myOrders.count || 0),
        pendingOrders:  parseInt(myPending.count || 0),
        totalRevenue:   Math.round(parseFloat(myRevenue.total || 0)),
        walletPending:  Math.round(walletPending),
        affEarned:      0, // placeholder if needed
        affPending:     0,
        affReferrals:   affRows.length,
        topProducts:    myTopProds,
        dailyOrders:    myDailyOrders
      });
    }

    /* ══════════════════════════════════════════════════
       ANALYTICS — AFFILIATE (own commission stats) — FIXED
    ══════════════════════════════════════════════════ */
    if (action === 'analytics-affiliate') {
      const userId = req.query.userId || '';
      if (!userId) return res.status(400).json({ error: 'userId required.' });

      const userRows = await sql`SELECT aff_code FROM users WHERE id::text = ${String(userId)} LIMIT 1`;
      const affCode  = userRows[0]?.aff_code || '';

      const [myOrdersRow] = affCode
        ? await sql`SELECT COUNT(*) AS count FROM orders WHERE aff_code = ${affCode} AND status != 'refunded'`
        : [{ count: 0 }];
      const [myPendingRow] = affCode
        ? await sql`SELECT COUNT(*) AS count FROM orders WHERE aff_code = ${affCode} AND status = 'pending' AND status != 'refunded'`
        : [{ count: 0 }];

      const commRows = await sql`
        SELECT * FROM affiliate_commissions ac
        LEFT JOIN orders o ON ac.order_id = o.id
        WHERE ac.aff_user_id::text = ${String(userId)}
          AND (o.status IS NULL OR o.status != 'refunded')
        ORDER BY ac.created_at DESC
      `;

      // Pull directly from wallets table (most reliable)
      const walletRows = await sql`
        SELECT COALESCE(referral_earnings,0) AS referral_earnings,
               COALESCE(pending_balance,0)   AS pending_balance,
               COALESCE(balance,0)           AS balance
        FROM wallets WHERE user_id::text = ${String(userId)} LIMIT 1
      `;
      const wallet = walletRows[0] || {};
      const walletEarned  = parseFloat(wallet.referral_earnings || 0);
      const walletPending = parseFloat(wallet.pending_balance   || 0);
      const walletBalance = parseFloat(wallet.balance           || 0);

      const displayEarned  = Math.round(walletEarned);
      const displayPending = Math.round(walletPending);
      const displayBalance = Math.round(walletBalance);

      const dailyComm = await sql`
        SELECT TO_CHAR(DATE(created_at),'Mon DD') AS day,
               COUNT(*) AS orders,
               COALESCE(SUM(commission),0) AS revenue
        FROM affiliate_commissions ac
        LEFT JOIN orders o ON ac.order_id = o.id
        WHERE ac.aff_user_id::text = ${String(userId)}
          AND ac.created_at >= NOW() - INTERVAL '30 days'
          AND (o.status IS NULL OR o.status != 'refunded')
        GROUP BY DATE(ac.created_at), day
        ORDER BY DATE(ac.created_at) ASC
      `;

      const commByStatus = [
        { status: 'paid',    count: commRows.filter(r=>r.status==='paid').length },
        { status: 'pending', count: commRows.filter(r=>r.status==='pending').length }
      ].filter(r => r.count > 0);

      return res.status(200).json({
        totalOrders:    parseInt(myOrdersRow.count || 0),
        pendingOrders:  parseInt(myPendingRow.count || 0),
        totalReferrals: commRows.length,
        totalEarned:    displayEarned,
        pendingComm:    displayPending,
        walletBalance:  displayBalance,
        commissions:    commRows,
        dailyOrders:    dailyComm,
        commByStatus
      });
    }

    return res.status(400).json({ error: 'Unknown action: ' + action });

  } catch (err) {
    console.error('[affiliate.js] action=' + action, err.message, err.stack);
    return res.status(500).json({ error: err.message || 'Server error.', action: action });
  }
};
