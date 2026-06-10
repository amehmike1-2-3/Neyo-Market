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

      // Only record commission if the order is not refunded
      const orderCheck = await sql`SELECT status FROM orders WHERE id::text = ${String(orderId)} LIMIT 1`;
      if (orderCheck.length && orderCheck[0].status === 'refunded')
        return res.status(200).json({ ok: true, skipped: 'Order is refunded — commission not recorded.' });

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
        SELECT ac.* FROM affiliate_commissions ac
        LEFT JOIN orders o ON o.id::text = ac.order_id::text
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

      // Totals
      const [usersRow]    = await sql`SELECT COUNT(*) AS count FROM users`;
      const [prodsRow]    = await sql`SELECT COUNT(*) AS count FROM products WHERE status = 'active'`;
      const [ordersRow]   = await sql`SELECT COUNT(*) AS count FROM orders WHERE status != 'refunded'`;
      const [revenueRow]  = await sql`
        SELECT COALESCE(SUM(total),0) AS total FROM orders
        WHERE status != 'refunded'
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
        SELECT COALESCE(SUM(ac.commission),0) AS total
        FROM affiliate_commissions ac
        JOIN orders o ON o.id::text = ac.order_id::text
        WHERE ac.status = 'paid' AND o.status != 'refunded'
      `;
      const [affPendRow]  = await sql`
        SELECT COALESCE(SUM(ac.commission),0) AS total
        FROM affiliate_commissions ac
        JOIN orders o ON o.id::text = ac.order_id::text
        WHERE ac.status = 'pending' AND o.status != 'refunded'
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
        WHERE o.status != 'refunded'
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
        AND status != 'refunded'
        GROUP BY date::date, day
        ORDER BY date::date ASC
      `;

      // Top 5 affiliates
      const topAffiliates = await sql`
        SELECT ac.aff_code, COUNT(*) AS referrals,
               COALESCE(SUM(ac.commission),0) AS earned
        FROM affiliate_commissions ac
        JOIN orders o ON o.id::text = ac.order_id::text
        WHERE o.status != 'refunded'
        GROUP BY ac.aff_code
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
        AND status != 'refunded'
      `;
      const [myPending] = await sql`
        SELECT COUNT(*) AS count FROM orders
        WHERE seller_id = ${parseInt(userId)}
        AND LOWER(status) LIKE '%pending%'
        AND status != 'refunded'
      `;
      const [myRevenue] = await sql`
        SELECT COALESCE(SUM(amount * 0.9),0) AS total FROM orders
        WHERE seller_id = ${parseInt(userId)}
        AND status != 'refunded'
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
        AND o.status != 'refunded'
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
        AND status != 'refunded'
        GROUP BY date::date, day
        ORDER BY date::date ASC
      `;

      // Affiliate commissions earned by this seller as an affiliate
      const affRows = await sql`
        SELECT ac.* FROM affiliate_commissions ac
        LEFT JOIN orders o ON o.id::text = ac.order_id::text
        WHERE ac.aff_user_id::text = ${String(userId)}
        AND (o.status IS NULL OR o.status != 'refunded')
        ORDER BY ac.created_at DESC
      `;
      const affEarned  = affRows.filter(r => r.status === 'paid').reduce((s,r) => s + parseFloat(r.commission||0), 0);
      const affPending = affRows.filter(r => r.status === 'pending').reduce((s,r) => s + parseFloat(r.commission||0), 0);

      return res.status(200).json({
        totalProducts:  parseInt(myProds.count || 0),
        totalOrders:    parseInt(myOrders.count || 0),
        pendingOrders:  parseInt(myPending.count || 0),
        totalRevenue:   walletBalance > 0 ? Math.round(walletBalance) : Math.round(parseFloat(myRevenue.total || 0)),
        walletBalance:  Math.round(walletBalance),
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

      // Count orders referred via this affiliate's aff_code (exclude refunded)
      const [myOrdersRow] = affCode
        ? await sql`SELECT COUNT(*) AS count FROM orders WHERE aff_code = ${affCode} AND status != 'refunded'`
        : [{ count: 0 }];
      const [myPendingRow] = affCode
        ? await sql`SELECT COUNT(*) AS count FROM orders WHERE aff_code = ${affCode} AND status = 'pending'`
        : [{ count: 0 }];

      // Commission rows from affiliate_commissions table (exclude refunded orders)
      const commRows = await sql`
        SELECT ac.* FROM affiliate_commissions ac
        LEFT JOIN orders o ON o.id::text = ac.order_id::text
        WHERE ac.aff_user_id::text = ${String(userId)}
        AND (o.status IS NULL OR o.status != 'refunded')
        ORDER BY ac.created_at DESC
      `;
      const totalEarned = commRows.filter(r => r.status === 'paid').reduce((s,r) => s + parseFloat(r.commission||0), 0);
      const pendingComm = commRows.filter(r => r.status === 'pending').reduce((s,r) => s + parseFloat(r.commission||0), 0);

      // Wallet for this specific affiliate — pull directly by user ID
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

      // Use wallet values directly when available, fall back to commission aggregates
      const displayEarned  = walletEarned  > 0 ? Math.round(walletEarned)  : Math.round(totalEarned);
      const displayPending = walletPending > 0 ? Math.round(walletPending) : Math.round(pendingComm);
      const displayBalance = walletBalance > 0 ? Math.round(walletBalance) : Math.round(totalEarned - pendingComm);

      // Daily commission history last 30 days (exclude refunded orders)
      const dailyComm = await sql`
        SELECT TO_CHAR(DATE(ac.created_at),'Mon DD') AS day,
               COUNT(*) AS orders,
               COALESCE(SUM(ac.commission),0) AS revenue
        FROM affiliate_commissions ac
        LEFT JOIN orders o ON o.id::text = ac.order_id::text
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

    /* ══════════════════════════════════════════════════
       STORE VISITOR TRACKING — log a store/product view
       POST ?action=track-store-view
       Body: { sellerId, productId (optional), page: 'store'|'product' }
       Uses IP geolocation (ip-api.com free tier — no key needed).
       Non-fatal: never blocks the page load.
    ══════════════════════════════════════════════════ */
    if (action === 'track-store-view' && req.method === 'POST') {
      try {
        const { sellerId, productId, page } = req.body || {};
        if (!sellerId) return res.status(200).json({ ok: true, skipped: 'no sellerId' });

        /* ── Create table if not exists (runs fast after first time) ── */
        await sql`
          CREATE TABLE IF NOT EXISTS store_views (
            id          BIGSERIAL PRIMARY KEY,
            seller_id   TEXT NOT NULL,
            product_id  TEXT,
            page        TEXT DEFAULT 'store',
            ip          TEXT,
            country     TEXT,
            region      TEXT,
            city        TEXT,
            country_code TEXT,
            lat         NUMERIC,
            lng         NUMERIC,
            created_at  TIMESTAMPTZ DEFAULT NOW()
          )
        `;

        /* ── Extract IP from request ── */
        const rawIp = (req.headers['x-forwarded-for'] || req.socket?.remoteAddress || '').split(',')[0].trim();
        const ip    = rawIp || 'unknown';

        /* ── Geo-lookup via ip-api.com (free, no key, 45 req/min) ── */
        let country = null, region = null, city = null, countryCode = null, lat = null, lng = null;
        if (ip && ip !== 'unknown' && ip !== '127.0.0.1' && !ip.startsWith('::')) {
          try {
            const geoRes  = await fetch('http://ip-api.com/json/' + ip + '?fields=status,country,regionName,city,countryCode,lat,lon', { signal: AbortSignal.timeout(2000) });
            const geoData = await geoRes.json();
            if (geoData.status === 'success') {
              country     = geoData.country     || null;
              region      = geoData.regionName  || null;
              city        = geoData.city         || null;
              countryCode = geoData.countryCode  || null;
              lat         = geoData.lat          || null;
              lng         = geoData.lon          || null;
            }
          } catch (geoErr) {
            /* geo lookup failed — save view anyway without location */
            console.warn('[track-store-view] geo lookup failed (non-fatal):', geoErr.message);
          }
        }

        const _sellerId   = String(sellerId);
        const _productId  = productId  ? String(productId)  : null;
        const _page       = page === 'product' ? 'product' : 'store';

        await sql`
          INSERT INTO store_views (seller_id, product_id, page, ip, country, region, city, country_code, lat, lng, created_at)
          VALUES (${_sellerId}, ${_productId}, ${_page}, ${ip}, ${country}, ${region}, ${city}, ${countryCode}, ${lat}, ${lng}, NOW())
        `;

        return res.status(200).json({ ok: true });
      } catch (err) {
        /* Always non-fatal — never break the page */
        console.error('[track-store-view] non-fatal:', err.message);
        return res.status(200).json({ ok: true, skipped: err.message });
      }
    }

    /* ══════════════════════════════════════════════════
       STORE VISITOR STATS — seller sees who viewed their store
       GET ?action=store-visitors&sellerId=xxx
       Returns: totalViews, uniqueVisitors, byCountry[], byCity[], recent[]
    ══════════════════════════════════════════════════ */
    if (action === 'store-visitors' && req.method === 'GET') {
      const sellerId = req.query.sellerId || '';
      if (!sellerId) return res.status(400).json({ error: 'sellerId required.' });

      try {
        /* Ensure table exists before querying */
        await sql`
          CREATE TABLE IF NOT EXISTS store_views (
            id          BIGSERIAL PRIMARY KEY,
            seller_id   TEXT NOT NULL,
            product_id  TEXT,
            page        TEXT DEFAULT 'store',
            ip          TEXT,
            country     TEXT,
            region      TEXT,
            city        TEXT,
            country_code TEXT,
            lat         NUMERIC,
            lng         NUMERIC,
            created_at  TIMESTAMPTZ DEFAULT NOW()
          )
        `;

        const _sellerId = String(sellerId);

        /* Total views */
        const [totRow] = await sql`SELECT COUNT(*) AS count FROM store_views WHERE seller_id = ${_sellerId}`;
        const totalViews = parseInt(totRow.count || 0);

        /* Unique IPs */
        const [uniRow] = await sql`SELECT COUNT(DISTINCT ip) AS count FROM store_views WHERE seller_id = ${_sellerId} AND ip IS NOT NULL AND ip != 'unknown'`;
        const uniqueVisitors = parseInt(uniRow.count || 0);

        /* Views last 7 days */
        const [weekRow] = await sql`SELECT COUNT(*) AS count FROM store_views WHERE seller_id = ${_sellerId} AND created_at >= NOW() - INTERVAL '7 days'`;
        const last7Days = parseInt(weekRow.count || 0);

        /* By country — top 8 */
        const byCountryRaw = await sql`
          SELECT country, country_code, COUNT(*) AS views
          FROM store_views
          WHERE seller_id = ${_sellerId} AND country IS NOT NULL
          GROUP BY country, country_code
          ORDER BY views DESC LIMIT 8
        `;

        /* Back-fill null country_codes using country name lookup */
        const CC_NAME_MAP = {
          'United Kingdom':'GB','United States':'US','Nigeria':'NG','Ghana':'GH',
          'Kenya':'KE','South Africa':'ZA','Canada':'CA','Germany':'DE','France':'FR',
          'India':'IN','China':'CN','Australia':'AU','Brazil':'BR','Pakistan':'PK',
          'Indonesia':'ID','Russia':'RU','Mexico':'MX','Ethiopia':'ET',
          'Philippines':'PH','Egypt':'EG','Vietnam':'VN','Iran':'IR','Turkey':'TR',
          'Thailand':'TH','Tanzania':'TZ','Uganda':'UG','Cameroon':'CM',
          'Senegal':'SN','Zambia':'ZM','Zimbabwe':'ZW','Rwanda':'RW',
          'Italy':'IT','Spain':'ES','Portugal':'PT','Netherlands':'NL',
          'Belgium':'BE','Sweden':'SE','Norway':'NO','Denmark':'DK',
          'Poland':'PL','Ukraine':'UA','Romania':'RO','Czech Republic':'CZ',
          'Hungary':'HU','Greece':'GR','Austria':'AT','Switzerland':'CH',
          'Ireland':'IE','Saudi Arabia':'SA','United Arab Emirates':'AE',
          'Qatar':'QA','Kuwait':'KW','Jordan':'JO','Israel':'IL',
          'Malaysia':'MY','Singapore':'SG','Japan':'JP','South Korea':'KR',
          'Taiwan':'TW','New Zealand':'NZ','Argentina':'AR','Colombia':'CO',
          'Peru':'PE','Chile':'CL','Venezuela':'VE',
        };
        const byCountry = byCountryRaw.map(row => ({
          ...row,
          country_code: row.country_code || CC_NAME_MAP[row.country] || null
        }));

        /* By city — top 8 */
        const byCity = await sql`
          SELECT city, region, country, COUNT(*) AS views
          FROM store_views
          WHERE seller_id = ${_sellerId} AND city IS NOT NULL
          GROUP BY city, region, country
          ORDER BY views DESC LIMIT 8
        `;

        /* Most viewed products */
        const byProduct = await sql`
          SELECT product_id, COUNT(*) AS views
          FROM store_views
          WHERE seller_id = ${_sellerId} AND product_id IS NOT NULL
          GROUP BY product_id
          ORDER BY views DESC LIMIT 5
        `;

        /* Daily views last 14 days */
        const daily = await sql`
          SELECT TO_CHAR(DATE(created_at), 'Mon DD') AS day, COUNT(*) AS views
          FROM store_views
          WHERE seller_id = ${_sellerId}
            AND created_at >= NOW() - INTERVAL '14 days'
          GROUP BY DATE(created_at), day
          ORDER BY DATE(created_at) ASC
        `;

        return res.status(200).json({
          ok: true,
          totalViews,
          uniqueVisitors,
          last7Days,
          byCountry,
          byCity,
          byProduct,
          daily
        });

      } catch (err) {
        console.error('[store-visitors]', err.message);
        return res.status(500).json({ error: err.message });
      }
    }

    return res.status(400).json({ error: 'Unknown action: ' + action });

  } catch (err) {
    console.error('[affiliate.js] action=' + action, err.message, err.stack);
    return res.status(500).json({ error: err.message || 'Server error.', action: action });
  }
};
