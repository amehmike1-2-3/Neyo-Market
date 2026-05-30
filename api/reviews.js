// /api/reviews.js — NeyoMarket Reviews API (Neon Postgres)
const { neon } = require('@neondatabase/serverless');

const sql = neon(process.env.DATABASE_URL);

function cors(res) {
  res.setHeader('Access-Control-Allow-Origin',  '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}

module.exports = async function handler(req, res) {
  cors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();

  try {

    /* ── GET /api/reviews?type=announcements ── */
    if (req.method === 'GET' && req.query.type === 'announcements') {
      const rows = await sql`
        SELECT id, title, content, badge_text, created_at
        FROM announcements
        ORDER BY created_at DESC
        LIMIT 20
      `;
      return res.status(200).json({ announcements: rows });
    }

    /* ── POST /api/reviews?type=announcements  (admin only) ── */
    if (req.method === 'POST' && req.query.type === 'announcements') {
      const body = req.body || {};
      const title      = body.title     || '';
      const content    = body.content   || '';
      const badge_text = body.badgeText || body.badge_text || null;
      const adminId    = body.adminId   || '';

      if (!title.trim())
        return res.status(400).json({ ok: false, error: 'Title is required.' });

      let caller = null;
      try {
        const adminRows = await sql`SELECT id, role FROM users WHERE id::text = ${String(adminId)} LIMIT 1`;
        caller = adminRows[0];
      } catch(e) {}

      const isMaster = String(adminId) === 'master_admin_001';
      if (!isMaster && (!caller || caller.role !== 'admin'))
        return res.status(403).json({ ok: false, error: 'Admin only.' });

      await sql`
        INSERT INTO announcements (title, content, badge_text, created_at)
        VALUES (${title.trim()}, ${content.trim() || null}, ${badge_text}, NOW())
      `;
      return res.status(201).json({ ok: true });
    }

    /* ── DELETE /api/reviews?type=announcements&id=X  (admin only) ── */
    if (req.method === 'DELETE' && req.query.type === 'announcements') {
      const annId   = parseInt(req.query.id);
      const adminId = (req.body || {}).adminId || '';
      if (!annId) return res.status(400).json({ ok: false, error: 'id required.' });

      let caller = null;
      try {
        const rows = await sql`SELECT role FROM users WHERE id::text = ${String(adminId)} LIMIT 1`;
        caller = rows[0];
      } catch(e) {}
      const isMaster = String(adminId) === 'master_admin_001';
      if (!isMaster && (!caller || caller.role !== 'admin'))
        return res.status(403).json({ ok: false, error: 'Admin only.' });

      await sql`DELETE FROM announcements WHERE id = ${annId}`;
      return res.status(200).json({ ok: true });
    }

    /* ══════════════════════════════════════════════════════════
       PLATFORM REVIEWS — GET /api/reviews?type=platform
    ══════════════════════════════════════════════════════════ */
    if (req.method === 'GET' && req.query.type === 'platform') {
      await sql`
        CREATE TABLE IF NOT EXISTS platform_reviews (
          id SERIAL PRIMARY KEY,
          user_id TEXT NOT NULL,
          user_name TEXT NOT NULL,
          rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
          comment TEXT,
          created_at TIMESTAMPTZ DEFAULT NOW()
        )
      `;
      const rows = await sql`
        SELECT id, user_name, rating, comment, created_at
        FROM platform_reviews
        ORDER BY created_at DESC
        LIMIT 100
      `;
      return res.status(200).json({ reviews: rows });
    }

    /* ══════════════════════════════════════════════════════════
       PLATFORM REVIEWS — POST /api/reviews?type=platform
    ══════════════════════════════════════════════════════════ */
    if (req.method === 'POST' && req.query.type === 'platform') {
      const { userId, userName, rating, comment } = req.body || {};

      if (!userId || !rating)
        return res.status(400).json({ ok: false, error: 'userId and rating are required.' });
      if (rating < 1 || rating > 5)
        return res.status(400).json({ ok: false, error: 'Rating must be between 1 and 5.' });
      if (!comment || comment.trim().length < 5)
        return res.status(400).json({ ok: false, error: 'Please write a short comment.' });

      /* One review per user */
      const existing = await sql`
        SELECT id FROM platform_reviews WHERE user_id = ${String(userId)} LIMIT 1
      `;
      if (existing.length)
        return res.status(409).json({ ok: false, error: 'You have already reviewed NeyoMarket.' });

      await sql`
        INSERT INTO platform_reviews (user_id, user_name, rating, comment, created_at)
        VALUES (${String(userId)}, ${userName || 'Anonymous'}, ${rating}, ${comment.trim()}, NOW())
      `;
      return res.status(201).json({ ok: true });
    }

    /* ── GET /api/reviews?productId=xxx ── */
    if (req.method === 'GET') {
      const productId = req.query.productId;
      if (!productId) return res.status(400).json({ error: 'productId is required.' });

      const rows = await sql`
        SELECT * FROM reviews WHERE product_id = ${productId} ORDER BY created_at DESC
      `;
      return res.status(200).json({ reviews: rows });
    }

    /* ── POST /api/reviews — submit a product review ── */
    if (req.method === 'POST') {
      const { productId, userId, userName, rating, comment, photo } = req.body || {};
      if (!productId || !userId || !rating)
        return res.status(400).json({ error: 'productId, userId and rating are required.' });
      if (rating < 1 || rating > 5)
        return res.status(400).json({ error: 'Rating must be between 1 and 5.' });

      const existing = await sql`
        SELECT id FROM reviews WHERE product_id = ${productId} AND user_id = ${userId} LIMIT 1
      `;
      if (existing.length)
        return res.status(409).json({ error: 'You have already reviewed this product.' });

      const safePhoto = (photo && typeof photo === 'string' && photo.startsWith('https://')) ? photo : null;

      await sql`
        INSERT INTO reviews (product_id, user_id, user_name, rating, comment, photo, created_at)
        VALUES (${productId}, ${userId}, ${userName || 'Anonymous'}, ${rating}, ${comment || ''}, ${safePhoto}, NOW())
      `;

      const stats = await sql`
        SELECT COUNT(*) as cnt, AVG(rating) as avg FROM reviews WHERE product_id = ${productId}
      `;
      const cnt = parseInt(stats[0].cnt);
      const avg = parseFloat(stats[0].avg).toFixed(1);

      await sql`
        UPDATE products SET rating = ${avg}, reviews = ${cnt} WHERE id = ${productId}
      `;

      return res.status(201).json({ ok: true, rating: avg, reviews: cnt });
    }

    return res.status(405).json({ error: 'Method not allowed' });

  } catch (err) {
    console.error('[reviews.js error]', err);
    return res.status(500).json({ error: 'Internal server error.' });
  }
};
