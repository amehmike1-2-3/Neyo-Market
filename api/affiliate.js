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
        SELECT * FROM affiliate_commissions
        WHERE aff_code = ${affCode}
        ORDER BY created_at DESC
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

      // Totals
      const [usersRow]    = await sql`SELECT COUNT(*) AS count FROM users`;
      const [prodsRow]    = await sql`SELECT COUNT(*) AS count FROM products WHERE status = 'active'`;
      const [ordersRow]   = await sql`SELECT COUNT(*) AS count FROM orders`;
      const [revenueRow]  = await sql`
        SELECT COALESCE(SUM(total),0) AS total FROM orders
      `;
      // Wrap in try/catch — created_at may not exist on users table
      let newUsersRow = { count: 0 };
      try {
        const [r] = await sql`
          SELECT COUNT(*) AS count FROM users
          WHERE created_at >= NOW() - INTERVAL '30 days'
        `;
        newUsersRow = r;
      } catch(e) { /* created_at column absent — skip */ }
      const [affPaidRow]  = await sql`
        SELECT COALESCE(SUM(commission),0) AS total FROM affiliate_commissions WHERE status = 'paid'
      `;
      const [affPendRow]  = await sql`
        SELECT COALESCE(SUM(commission),0) AS total FROM affiliate_commissions WHERE status = 'pending'
      `;

      // Orders by status
      const ordersByStatus = await sql`
        SELECT status, COUNT(*) AS count FROM orders GROUP BY status ORDER BY count DESC
      `;

      // Top 5 products by sales
      const topProducts = await sql`
        SELECT p.name, p.price, COUNT(o.id) AS sales,
               COALESCE(SUM(o.total),0) AS revenue
        FROM orders o
        JOIN products p ON o.product_id::text = p.id::text
        GROUP BY p.id, p.name, p.price
        ORDER BY sales DESC LIMIT 5
      `;

      // Daily orders + revenue last 30 days
      const dailyOrders = await sql`
        SELECT TO_CHAR(date::date,'Mon DD') AS day,
               COUNT(*) AS orders,
               COALESCE(SUM(total),0) AS revenue
        FROM orders
        WHERE date::date >= (NOW() - INTERVAL '30 days')::date
        GROUP BY date::date, day
        ORDER BY date::date ASC
      `;

      // Top 5 affiliates
      const topAffiliates = await sql`
        SELECT aff_code, COUNT(*) AS referrals,
               COALESCE(SUM(commission),0) AS earned
        FROM affiliate_commissions
        GROUP BY aff_code
        ORDER BY earned DESC LIMIT 5
      `;

      // Admin wallet balances from wallets table
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

      // Cast both sides to text to avoid int/string mismatch
      const [myProds] = await sql`
        SELECT COUNT(*) AS count FROM products
        WHERE seller_id::text = ${String(userId)} AND status = 'active'
      `;
      const [myOrders] = await sql`
        SELECT COUNT(*) AS count FROM orders
        WHERE seller_id = ${parseInt(userId)}
      `;
      const [myPending] = await sql`
        SELECT COUNT(*) AS count FROM orders
        WHERE seller_id = ${parseInt(userId)}
        AND LOWER(status) LIKE '%pending%'
      `;
      const [myRevenue] = await sql`
        SELECT COALESCE(SUM(amount * 0.9),0) AS total FROM orders
        WHERE seller_id = ${parseInt(userId)}
      `;

      // Also fetch wallet balance for this seller
      const walletRows = await sql`
        SELECT COALESCE(balance,0) AS balance,
               COALESCE(pending_balance,0) AS pending_balance
        FROM wallets WHERE user_id::text = ${String(userId)} LIMIT 1
      `;
      const wallet = walletRows[0] || {};
      const walletBalance = parseFloat(wallet.balance || 0);
      const walletPending = parseFloat(wallet.pending_balance || 0);

      // Top 5 products
      const myTopProds = await sql`
        SELECT p.name, p.price, COUNT(o.id) AS sales,
               COALESCE(SUM(o.amount * 0.9),0) AS revenue
        FROM orders o
        JOIN products p ON o.product_id::text = p.id::text
        WHERE o.seller_id = ${parseInt(userId)}
        GROUP BY p.id, p.name, p.price
        ORDER BY sales DESC LIMIT 5
      `;

      // Daily last 30 days
      const myDailyOrders = await sql`
        SELECT TO_CHAR(date::date,'Mon DD') AS day,
               COUNT(*) AS orders,
               COALESCE(SUM(amount * 0.9),0) AS revenue
        FROM orders
        WHERE seller_id = ${parseInt(userId)}
        AND date::date >= (NOW() - INTERVAL '30 days')::date
        GROUP BY date::date, day
        ORDER BY date::date ASC
      `;

      // Affiliate commissions earned by this seller as an affiliate
      const affRows = await sql`
        SELECT * FROM affiliate_commissions
        WHERE aff_user_id::text = ${String(userId)}
        ORDER BY created_at DESC
      `;
      const affEarned  = affRows.filter(r => r.status === 'paid').reduce((s,r) => s + parseFloat(r.commission||0), 0);
      const affPending = affRows.filter(r => r.status === 'pending').reduce((s,r) => s + parseFloat(r.commission||0), 0);

      return res.status(200).json({
        totalProducts:  parseInt(myProds.count || 0),
        totalOrders:    parseInt(myOrders.count || 0),
        pendingOrders:  parseInt(myPending.count || 0),
        totalRevenue:   Math.round(parseFloat(myRevenue.total || 0)),
        walletPending:  Math.round(walletPending),
        affEarned:      Math.round(affEarned),
        affPending:     Math.round(affPending),
        affReferrals:   affRows.length,
        topProducts:    myTopProds,
        dailyOrders:    myDailyOrders
      });
    }

    /* ══════════════════════════════════════════════════
       ANALYTICS — AFFILIATE (own commission stats)
    ══════════════════════════════════════════════════ */
    if (action === 'analytics-affiliate') {
      const userId = req.query.userId || '';
      if (!userId) return res.status(400).json({ error: 'userId required.' });

      // Look up this user's aff_code — orders track affiliates by aff_code not user id
      const userRows = await sql`SELECT aff_code FROM users WHERE id::text = ${String(userId)} LIMIT 1`;
      const affCode  = userRows[0]?.aff_code || '';

      // Count orders referred via this affiliate's aff_code
      const [myOrdersRow] = affCode
        ? await sql`SELECT COUNT(*) AS count FROM orders WHERE aff_code = ${affCode}`
        : [{ count: 0 }];
      const [myPendingRow] = affCode
        ? await sql`SELECT COUNT(*) AS count FROM orders WHERE aff_code = ${affCode} AND status = 'pending'`
        : [{ count: 0 }];

      // Commission rows from affiliate_commissions table
      const commRows = await sql`
        SELECT * FROM affiliate_commissions
        WHERE aff_user_id::text = ${String(userId)}
        ORDER BY created_at DESC
      `;
      const totalEarned = commRows.filter(r => r.status === 'paid').reduce((s,r) => s + parseFloat(r.commission||0), 0);
      const pendingComm = commRows.filter(r => r.status === 'pending').reduce((s,r) => s + parseFloat(r.commission||0), 0);

      // Wallet for this affiliate
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

      const displayEarned  = walletEarned  > 0 ? Math.round(walletEarned)  : Math.round(totalEarned);
      const displayPending = walletPending > 0 ? Math.round(walletPending) : Math.round(pendingComm);
      const displayBalance = walletBalance > 0 ? Math.round(walletBalance) : Math.round(totalEarned - pendingComm);

      // Daily commission history last 30 days
      const dailyComm = await sql`
        SELECT TO_CHAR(DATE(created_at),'Mon DD') AS day,
               COUNT(*) AS orders,
               COALESCE(SUM(commission),0) AS revenue
        FROM affiliate_commissions
        WHERE aff_user_id::text = ${String(userId)}
        AND created_at >= NOW() - INTERVAL '30 days'
        GROUP BY DATE(created_at), day
        ORDER BY DATE(created_at) ASC
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
