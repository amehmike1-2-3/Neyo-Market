// /api/leaderboard.js — NeyoMarket Affiliate Leaderboard
// GET: returns top users ranked by affiliate_earnings (sum of credited affiliate fees)
// All ID and numeric casts use Number() per Neon safety rule

'use strict';

const { neon } = require('@neondatabase/serverless');
const sql = neon(process.env.DATABASE_URL);

function cors(res) {
  res.setHeader('Access-Control-Allow-Origin',  '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}

module.exports = async function handler(req, res) {
  cors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'GET')     return res.status(405).json({ error: 'GET only.' });

  try {
    /* ── Primary: read from wallets.referral_earnings + affiliate_commissions count ──
       This reflects real credited earnings per affiliate regardless of
       whether the original order still exists in the orders table.
    ── */
    const leaderboard = await sql`
      SELECT
        u.id,
        u.name,
        u.aff_code,
        COALESCE(w.referral_earnings, 0)::numeric AS affiliate_earnings,
        COUNT(ac.id)::int                          AS total_referrals
      FROM users u
      LEFT JOIN wallets w
        ON w.user_id::text = u.id::text
      LEFT JOIN affiliate_commissions ac
        ON ac.aff_user_id::text = u.id::text
      WHERE u.aff_code IS NOT NULL
        AND u.aff_code != ''
      GROUP BY u.id, u.name, u.aff_code, w.referral_earnings
      ORDER BY affiliate_earnings DESC
      LIMIT 20
    `;

    return res.status(200).json({
      ok:          true,
      leaderboard: leaderboard.map(function(u) {
        return {
          id:                 u.id,
          name:               u.name || 'Affiliate',
          affCode:            u.aff_code,
          affiliate_earnings: parseFloat(u.affiliate_earnings || 0),
          total_referrals:    parseInt(u.total_referrals   || 0, 10)
        };
      }),
      generatedAt: new Date().toISOString()
    });

  } catch (err) {
    console.error('[leaderboard.js] ERROR:', err.message);
    return res.status(500).json({ error: 'Could not load leaderboard.', detail: err.message });
  }
};

