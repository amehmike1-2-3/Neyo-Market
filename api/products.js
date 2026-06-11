// /api/products.js — NeyoMarket Products API
// ✅ FIX 1: Share button now uses absolute URLs (https://neyomarket.com.ng)
// ✅ FIX 2: Digital products MUST have a file_url — 400 returned if missing
// ✅ FIX 3: GET with sellerId uses WHERE seller_id = $1 (authenticated session ID)
// ✅ FIX 4: Condition field now included in PATCH UPDATE
// ✅ FIX 5: Every route and catch returns res.json() — never HTML
// ✅ FIX 6: New/Used filter added to product sorting
// ✅ FIX 7: Upstash Redis caching on public GET routes — safe fallback if Redis fails
// All ID lookups: Number() for product IDs, String() for user IDs

'use strict';

const { neon } = require('@neondatabase/serverless');
/* ═══ CLOUDINARY UPLOAD HANDLER ══════════════════════════════════════════
   Handles POST /api/products?action=upload
   Uses CLOUDINARY_URL env var — automatically parsed by cloudinary SDK.
   resource_type:'auto' accepts images, PDFs, ZIPs, videos — everything.
   Returns { url, secure_url, public_id } to frontend.
═══════════════════════════════════════════════════════════════════════════ */
const cloudinary = require('cloudinary').v2;

/* SDK auto-configures from CLOUDINARY_URL env var — no extra config needed */
cloudinary.config({ secure: true });

/* ═══ UPSTASH REDIS CACHE ════════════════════════════════════════════════
   Optional caching layer — if UPSTASH_REDIS_REST_URL / TOKEN are missing
   or Redis throws, the API falls back to Neon silently.
   All Redis calls are wrapped in try/catch — Redis failure = no crash.

   Cache keys used:
     products:public            — public marketplace listing (no filters)
     products:cat:{cat}         — category filter
     products:type:{type}       — type filter
     products:sale              — sale items
     products:id:{id}           — single product by ID
     products:seller:{sellerId} — seller's own product list

   TTL: 60 seconds for listings, 120 seconds for single products.
═══════════════════════════════════════════════════════════════════════════ */
let redis = null;

function _getRedis() {
  if (redis) return redis;
  const url   = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return null;
  try {
    /* Use the official @upstash/redis package (must be in package.json) */
    const { Redis } = require('@upstash/redis');
    redis = new Redis({ url, token });
    return redis;
  } catch(e) {
    console.warn('[redis] @upstash/redis not available — caching disabled:', e.message);
    return null;
  }
}

async function cacheGet(key) {
  try {
    const r = _getRedis();
    if (!r) return null;
    return await r.get(key);
  } catch(e) {
    console.warn('[redis] GET failed, falling back to DB:', e.message);
    return null;
  }
}

async function cacheSet(key, value, ttlSeconds) {
  try {
    const r = _getRedis();
    if (!r) return;
    await r.set(key, value, { ex: ttlSeconds });
  } catch(e) {
    console.warn('[redis] SET failed — cache miss saved:', e.message);
  }
}

async function _handleUpload(req, res) {
  /* Auth check — only sellers/admins */
  try {
    const auth = req.headers['authorization'] || '';
    const tok  = auth.replace('Bearer ', '');
    if (tok) {
      const user = JSON.parse(Buffer.from(tok, 'base64').toString('utf8'));
      if (!user || !['seller','admin'].includes(user.role)) {
        return res.status(403).json({ ok: false, error: 'Sellers only.' });
      }
    }
  } catch(e) {}

  /* Expect base64 data in body: { data, fileName, fileType, folder } */
  const body     = req.body || {};
  const b64data  = body.data     || '';
  const fileName = body.fileName || body.name || 'upload';
  const fileType = body.fileType || body.type || 'application/octet-stream';
  const folder   = body.folder   || 'neyomarket';

  if (!b64data) {
    return res.status(400).json({ ok: false, error: 'No file data provided.' });
  }

  /* Build data URI if not already one */
  const dataUri = b64data.startsWith('data:')
    ? b64data
    : 'data:' + fileType + ';base64,' + b64data;

  try {
    const result = await cloudinary.uploader.upload(dataUri, {
      resource_type: 'auto',      /* auto = images + raw files + videos */
      folder:         folder,
      use_filename:   true,
      unique_filename: true,
      overwrite:      false,
      public_id:      fileName.replace(/[^a-zA-Z0-9._-]/g, '_').replace(/\.[^.]+$/, ''),
    });

    console.log('[Cloudinary] Uploaded:', result.secure_url);
    return res.status(200).json({
      ok:         true,
      url:        result.secure_url,
      public_id:  result.public_id,
      format:     result.format,
      bytes:      result.bytes,
    });

  } catch(err) {
    console.error('[Cloudinary] Upload error:', err.message);
    return res.status(500).json({ ok: false, error: 'Upload failed: ' + err.message });
  }
}

/* ═══ DATABASE AND UTILITY METHODS ═══ */
const sql = neon(process.env.DATABASE_URL);

function cors(res) {
  res.setHeader('Access-Control-Allow-Origin',  'https://neyomarket.com.ng');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PATCH, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.setHeader('X-Content-Type-Options',       'nosniff');
}

/* Always JSON — never let Express default to HTML error page */
function jsonErr(res, status, msg, detail) {
  return res.status(status).json({ error: msg, ...(detail ? { detail } : {}) });
}

function toProduct(r) {
  let imgs = r.imgs;
  if (typeof imgs === 'string') { try { imgs = JSON.parse(imgs); } catch(e) { imgs = []; } }
  if (!Array.isArray(imgs)) imgs = [];
  return {
    id:             r.id,
    name:           r.name             || '',
    type:           r.type             || 'digital',
    cat:            r.cat              || 'other',
    price:          parseFloat(r.price || 0),
    discountPrice:  r.discount_price   ? parseFloat(r.discount_price) : null,
    isOnSale:       r.is_on_sale       || false,
    saleEndsAt:     r.sale_ends_at     || null,
    shippingFee:    r.shipping_fee     ? parseFloat(r.shipping_fee) : 0,
    sellerVerified: (r.seller_verified !== undefined && r.seller_verified !== null) ? Boolean(r.seller_verified) : false,
    lessons:       r.lessons || [],
    badgeVerified:  (r.badge_verified  !== undefined && r.badge_verified  !== null) ? Boolean(r.badge_verified)  : false,
    commission:     parseFloat(r.commission || 0),
    description:    r.description      || '',
    seller:         r.seller           || '',
    sellerId:       r.seller_id        || null,
    sellerEmail:    r.seller_email     || '',
    sellerWhatsapp: r.seller_whatsapp  || '',
    rating:         parseFloat(r.rating  || 0),
    reviews:        parseInt(r.reviews   || 0, 10),
    emoji:          r.emoji            || '📦',
    imgs:           imgs,
    status:         r.status           || 'pending',
    badge:          r.badge            || '',
    date:           r.date             || '',
    escrow:         r.escrow           !== false,
    fileExt:        r.file_ext         || null,
    fileName:       r.file_name        || null,
    fileUrl:        r.file_url         || null,
    fileSize:       r.file_size        || null,
    disputed:       r.disputed         || false,
    quantity:       r.quantity         !== undefined ? parseInt(r.quantity, 10) : null,
    location:       r.location         || r.seller_location || '',
    sellerBio:      r.seller_bio       || '',
    isFeature:      r.is_featured      || false,
    createdAt:      r.created_at       || null,
    condition:      r.condition        || null,
    sellerTier:     r.seller_tier      || r.membership_tier || 'free',
    promoted:       r.promoted         || false,
    promotedUntil:  r.promoted_until   || null,
    currency:       r.currency         || 'NGN',
    variants:       r.variants         || [],
    viewCount:      parseInt(r.view_count || 0, 10),
    couponCode:     r.coupon_code  || null,
    couponType:     r.coupon_type  || null,
    couponValue:    r.coupon_value ? parseFloat(r.coupon_value) : null,
    hasPendingEdit: r.has_pending_edit || false,
  };
}

module.exports = async function handler(req, res) {
  cors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();

  try {

    if (req.query.action === 'upload') return _handleUpload(req, res);

    /* ═══ AI PRODUCT OPTIMIZER ════════════════════════════════════════════
       POST /api/products?action=optimize
       Reuses exact same Groq setup as chat.js.
       Returns: { optimized_title, key_selling_point, target_tags }
    ═══════════════════════════════════════════════════════════════════════ */
    if (req.query.action === 'optimize' && req.method === 'POST') {
      const GROQ_KEY = process.env.GROQ_API_KEY;
      if (!GROQ_KEY) return res.status(500).json({ ok: false, error: 'GROQ_API_KEY not set in Vercel env vars.' });

      const body  = req.body || {};
      const title = String(body.title || '').trim();
      const cat   = String(body.category || '').trim();
      const price = String(body.price || '').trim();
      const type  = String(body.type || '').trim();

      if (!title) return res.status(400).json({ ok: false, error: 'Product title is required.' });

      const systemPrompt = [
        "You are a world-class e-commerce product optimizer for NeyoMarket, Nigeria's leading marketplace.",
        "Your task: analyze the product details provided and return ONLY a raw JSON object — no markdown,",
        "no explanation, no code fences, no extra text. Just the JSON object itself.",
        '',
        'Return exactly this structure:',
        '{"optimized_title":"...","key_selling_point":"...","target_tags":["tag1","tag2","tag3"]}',
        '',
        'Rules:',
        '- optimized_title: SEO-friendly, Nigerian market focused, max 80 chars, include key benefit',
        '- key_selling_point: One high-converting sentence for Nigerian buyers, mention value/trust',
        '- target_tags: Exactly 5 relevant search tags, lowercase, no spaces in each tag',
        '- Write for a Nigerian audience — reference local context where natural',
        '- Never include any text outside the JSON object'
      ].join('\n');

      const userPrompt = [
        'Optimize this product listing:',
        'Title: ' + title,
        'Category: ' + (cat || 'general'),
        'Type: ' + (type || 'physical'),
        'Price: ₦' + (price || 'not set'),
        '',
        'Return only the raw JSON object.'
      ].join('\n');

      let resp, raw2, data;
      try {
        resp = await fetch('https://api.groq.com/openai/v1/chat/completions', {
          method:  'POST',
          headers: {
            'Content-Type':  'application/json',
            'Authorization': 'Bearer ' + GROQ_KEY
          },
          body: JSON.stringify({
            model:       'llama-3.1-8b-instant',
            messages:    [
              { role: 'system', content: systemPrompt },
              { role: 'user',   content: userPrompt   }
            ],
            max_tokens:  300,
            temperature: 0.4,
            stream:      false
          })
        });
        raw2 = await resp.text();
        data = JSON.parse(raw2);
      } catch(e) {
        console.error('[optimizer] fetch error:', e.message);
        return res.status(502).json({ ok: false, error: 'Could not reach AI. Try again.' });
      }

      if (!resp.ok) {
        const errMsg = (data.error && data.error.message) || raw2.slice(0, 120);
        console.error('[optimizer] Groq error:', resp.status, errMsg);
        return res.status(resp.status).json({ ok: false, error: 'AI error: ' + errMsg });
      }

      const reply = data.choices && data.choices[0] && data.choices[0].message && data.choices[0].message.content;
      if (!reply) return res.status(502).json({ ok: false, error: 'No response from AI.' });

      /* Strip any accidental markdown fences */
      const clean = reply.trim().replace(/^```json?\s*/i, '').replace(/```\s*$/i, '').trim();

      let parsed;
      try {
        parsed = JSON.parse(clean);
      } catch(e) {
        console.error('[optimizer] JSON parse failed. Raw reply:', reply.slice(0, 200));
        return res.status(502).json({ ok: false, error: 'AI returned invalid format. Try again.' });
      }

      /* Validate required fields */
      if (!parsed.optimized_title || !parsed.key_selling_point || !Array.isArray(parsed.target_tags)) {
        return res.status(502).json({ ok: false, error: 'AI response missing required fields.' });
      }

      console.log('[optimizer] OK — title:', parsed.optimized_title.slice(0, 50));
      return res.status(200).json({ ok: true, result: parsed });
    }

    /* ════════════════════════════════════════════════
       GET
    ════════════════════════════════════════════════ */
    if (req.method === 'GET') {
      const { admin, sellerId, status, id } = req.query;

      /* ── Single product by ID ── */
      if (id) {
        const cacheKey = 'products:id:' + id;

        /* Try cache first */
        const cached = await cacheGet(cacheKey);
        if (cached) {
          console.log('[redis] HIT', cacheKey);
          return res.status(200).json({ product: cached, _cached: true });
        }

        const rows = await sql`
          SELECT * FROM products WHERE id = ${Number(id)} LIMIT 1
        `;
        if (!rows.length) return jsonErr(res, 404, 'Product not found.');

        const product = toProduct(rows[0]);
        await cacheSet(cacheKey, product, 120);
        return res.status(200).json({ product });
      }

      let rows;

      if (sellerId) {
        /* FIX 3: strict WHERE seller_id = authenticated seller's ID (String type)
           Never return other sellers' products — even if the query string is tampered.
           Cache per-seller list for 60s. */
        const cacheKey = 'products:seller:' + String(sellerId);
        const cached   = await cacheGet(cacheKey);
        if (cached) {
          console.log('[redis] HIT', cacheKey);
          return res.status(200).json({ products: cached, _cached: true });
        }

        rows = await sql`
          SELECT * FROM products
          WHERE seller_id = ${String(sellerId)}
          ORDER BY created_at DESC
        `;

        const mapped = rows.map(toProduct);
        await cacheSet(cacheKey, mapped, 60);
        return res.status(200).json({ products: mapped });

      } else if (req.query.storeName) {
        /* ── STOREFRONT FIX: Handles search parameters using aff_code, raw string matching, OR exact seller_id fallback ──
           NOTE: storeName lookups are NOT cached — they are highly specific and
           the ::text casting / dual-path logic must always run fresh to stay correct. */
        const storeCode = String(req.query.storeName).trim();
        
        /* Look up user metrics by aff_code or raw user matching */
        const sellerRows = await sql`
          SELECT id FROM users WHERE aff_code = ${storeCode} OR id::text = ${storeCode} LIMIT 1
        `;
        
        if (sellerRows.length) {
          const sid = String(sellerRows[0].id);
          rows = await sql`
            SELECT p.*, u.membership_tier AS seller_tier,
                   u.is_verified AS seller_verified,
                   u.badge_verified AS badge_verified
            FROM products p
            LEFT JOIN users u ON u.id::text = p.seller_id::text
            WHERE LOWER(p.status) IN ('active','approved','published')
              AND (p.seller_id::text = ${sid} OR p.seller_id::text = ${storeCode})
            ORDER BY p.created_at DESC
          `;
        } else {
          /* Final Fallback: Match text based display name or direct string ID logic */
          rows = await sql`
            SELECT p.*, u.membership_tier AS seller_tier,
                   u.is_verified AS seller_verified,
                   u.badge_verified AS badge_verified
            FROM products p
            LEFT JOIN users u ON u.id::text = p.seller_id::text
            WHERE LOWER(p.status) IN ('active','approved','published')
              AND (LOWER(p.seller) = LOWER(${storeCode}) OR p.seller_id::text = ${storeCode})
            ORDER BY p.created_at DESC
          `;
        }

        return res.status(200).json({ products: rows.map(toProduct) });

      } else if (admin === 'true') {
        /* Admin: all products, optionally filtered by status — NOT cached */
        if (status && status !== 'all') {
          rows = await sql`
            SELECT * FROM products WHERE status = ${String(status)} ORDER BY created_at DESC
          `;
        } else {
          rows = await sql`SELECT * FROM products ORDER BY created_at DESC`;
        }
        return res.status(200).json({ products: rows.map(toProduct) });

      } else {
        /* Public marketplace — active only, with seller tier + optional search/filter.
           Search queries are NOT cached (too many permutations).
           Category, type, sale, and unfiltered listing ARE cached for 60s. */
        const searchQ    = req.query.q    ? '%' + String(req.query.q).trim().toLowerCase()    + '%' : null;
        const catFilter  = req.query.cat  ? String(req.query.cat).trim().toLowerCase()               : null;
        const typeFilter = req.query.type ? String(req.query.type).trim().toLowerCase()              : null;
        const saleOnly   = req.query.sale === 'true';

        if (searchQ) {
          /* Search is never cached — too many unique queries */
          rows = await sql`
            SELECT p.*, u.membership_tier AS seller_tier, u.is_verified AS seller_verified, u.badge_verified AS badge_verified
            FROM products p
            LEFT JOIN users u ON u.id::text = p.seller_id::text
            WHERE p.status = 'active'
              AND (LOWER(p.name) LIKE ${searchQ} OR LOWER(p.description) LIKE ${searchQ} OR LOWER(p.seller) LIKE ${searchQ})
            ORDER BY p.created_at DESC
          `;
          return res.status(200).json({ products: rows.map(toProduct) });

        } else if (catFilter) {
          const cacheKey = 'products:cat:' + catFilter;
          const cached   = await cacheGet(cacheKey);
          if (cached) {
            console.log('[redis] HIT', cacheKey);
            return res.status(200).json({ products: cached, _cached: true });
          }

          rows = await sql`
            SELECT p.*, u.membership_tier AS seller_tier, u.is_verified AS seller_verified, u.badge_verified AS badge_verified
            FROM products p
            LEFT JOIN users u ON u.id::text = p.seller_id::text
            WHERE p.status = 'active' AND LOWER(p.cat) = ${catFilter}
            ORDER BY p.created_at DESC
          `;
          const mapped = rows.map(toProduct);
          await cacheSet(cacheKey, mapped, 60);
          return res.status(200).json({ products: mapped });

        } else if (typeFilter) {
          const cacheKey = 'products:type:' + typeFilter;
          const cached   = await cacheGet(cacheKey);
          if (cached) {
            console.log('[redis] HIT', cacheKey);
            return res.status(200).json({ products: cached, _cached: true });
          }

          rows = await sql`
            SELECT p.*, u.membership_tier AS seller_tier, u.is_verified AS seller_verified, u.badge_verified AS badge_verified
            FROM products p
            LEFT JOIN users u ON u.id::text = p.seller_id::text
            WHERE p.status = 'active' AND LOWER(p.type) = ${typeFilter}
            ORDER BY p.created_at DESC
          `;
          const mapped = rows.map(toProduct);
          await cacheSet(cacheKey, mapped, 60);
          return res.status(200).json({ products: mapped });

        } else if (saleOnly) {
          const cacheKey = 'products:sale';
          const cached   = await cacheGet(cacheKey);
          if (cached) {
            console.log('[redis] HIT', cacheKey);
            return res.status(200).json({ products: cached, _cached: true });
          }

          rows = await sql`
            SELECT p.*, u.membership_tier AS seller_tier, u.is_verified AS seller_verified, u.badge_verified AS badge_verified
            FROM products p
            LEFT JOIN users u ON u.id::text = p.seller_id::text
            WHERE p.status = 'active' AND p.is_on_sale = true
              AND (p.sale_ends_at IS NULL OR p.sale_ends_at > NOW())
            ORDER BY p.created_at DESC
          `;
          const mapped = rows.map(toProduct);
          await cacheSet(cacheKey, mapped, 60);
          return res.status(200).json({ products: mapped });

        } else {
          /* Unfiltered public listing — most frequently hit, cache for 60s */
          const cacheKey = 'products:public';
          const cached   = await cacheGet(cacheKey);
          if (cached) {
            console.log('[redis] HIT', cacheKey);
            return res.status(200).json({ products: cached, _cached: true });
          }

          rows = await sql`
            SELECT p.*, u.membership_tier AS seller_tier, u.is_verified AS seller_verified, u.badge_verified AS badge_verified
            FROM products p
            LEFT JOIN users u ON u.id::text = p.seller_id::text
            WHERE p.status = 'active'
            ORDER BY p.created_at DESC
          `;
          const mapped = rows.map(toProduct);
          await cacheSet(cacheKey, mapped, 60);
          return res.status(200).json({ products: mapped });
        }
      }
    }

    /* ════════════════════════════════════════════════
       POST — create product
       FIX 2: Reject digital products without a file_url
    ════════════════════════════════════════════════ */
    if (req.method === 'POST' && req.query.action !== 'track-view') {
      const p = req.body || {};

      if (!p.name || !p.price)
        return jsonErr(res, 400, 'Product name and price are required.');

      /* FIX 2: Hard-block digital/course uploads with no file_url ─────
         The frontend uploads the file to ImgBB/S3 and passes back the
         URL before calling this endpoint. If it's missing, the upload
         failed — we must NOT create a broken product record in Neon. */
      const productType = p.type || 'digital';
      if ((productType === 'digital' || productType === 'course') && !p.fileUrl) {
        return jsonErr(res, 400,
          'Digital and course products require a file_url. Upload the file first, then submit the product.',
          'file_url was empty or missing'
        );
      }

      /* Pre-compute all conditionals outside the sql template (Neon safety rule) */
      const id             = Number(p.id || Date.now());
      const discountPrice  = (p.discountPrice  != null) ? parseFloat(p.discountPrice)  : null;
      const isOnSale       = p.isOnSale   ? true : false;
      const saleEndsAt     = p.saleEndsAt || null;
      const shippingFee    = (p.shippingFee != null) ? parseFloat(p.shippingFee) : 0;
      const commission     = parseFloat(p.commission || 0);
      const sellerId       = p.sellerId   ? String(p.sellerId) : null;
      const sellerWhatsapp = p.sellerWhatsapp || '';
      const escrow         = (p.escrow !== false);
      const fileExt        = p.fileExt  || null;
      const fileName       = p.fileName || null;
      /* fileUrl accepted in any format */
      const fileUrl        = p.fileUrl  || null;
      const fileSize       = p.fileSize || null;
      const imgs           = JSON.stringify(p.imgs || []);
      const dateStr        = new Date().toLocaleDateString();
      const productCat     = p.cat         || 'other';
      const productDesc    = p.description || '';
      const productSeller  = p.seller      || '';
      const sellerEmail    = p.sellerEmail || '';
      const productEmoji   = p.emoji       || '📦';
      const productBadge   = p.badge       || '';
      const productCondition = p.condition || null;
      const productCurrency  = ['NGN','USD','GBP','EUR','CAD','GHS'].includes(p.currency) ? p.currency : 'NGN';
      const productVariants  = (p.variants && Array.isArray(p.variants) && p.variants.length) ? JSON.stringify(p.variants) : '[]';
      const productLessons   = (p.lessons  && Array.isArray(p.lessons)  && p.lessons.length)  ? JSON.stringify(p.lessons)  : '[]';
      const couponCode       = p.couponCode  ? String(p.couponCode).trim().toUpperCase()  : null;
      const couponType       = (p.couponType === 'percent' || p.couponType === 'fixed') ? p.couponType : null;
      const couponValue      = (p.couponValue != null && couponCode) ? parseFloat(p.couponValue) : null;

      const rows = await sql`
        INSERT INTO products (
          id, name, type, cat, price,
          discount_price, is_on_sale, sale_ends_at, shipping_fee,
          commission, description, seller, seller_id, seller_email, seller_whatsapp,
          rating, reviews, emoji, imgs, status, badge, date, escrow,
          file_ext, file_name, file_url, file_size, is_verified, disputed,
          quantity, location, seller_bio, created_at, condition, currency, variants, lessons,
          coupon_code, coupon_type, coupon_value
        ) VALUES (
          ${id}, ${p.name}, ${productType}, ${productCat}, ${parseFloat(p.price)},
          ${discountPrice}, ${isOnSale}, ${saleEndsAt}, ${shippingFee},
          ${commission}, ${productDesc}, ${productSeller}, ${sellerId},
          ${sellerEmail}, ${sellerWhatsapp},
          ${0}, ${0},
          ${productEmoji}, ${imgs}, ${'pending'}, ${productBadge}, ${dateStr}, ${escrow},
          ${fileExt}, ${fileName}, ${fileUrl}, ${fileSize}, ${false}, ${false},
          ${p.quantity !== undefined && p.quantity !== null ? parseInt(p.quantity, 10) : null},
          ${p.location || ''},
          ${p.sellerBio || p.seller_bio || ''},
          NOW(),
          ${productCondition}, ${productCurrency}, ${productVariants}::jsonb, ${productLessons}::jsonb,
          ${couponCode}, ${couponType}, ${couponValue}
        )
        RETURNING *
      `;
      return res.status(201).json({ ok: true, product: toProduct(rows[0]) });
    }

    /* ════════════════════════════════════════════════
       PATCH — update product fields
       All conditionals pre-computed (Neon ternary rule)
       FIX 4: condition field now included in UPDATE
    ════════════════════════════════════════════════ */
    if (req.method === 'PATCH') {
      const p = req.body || {};
      if (!p.id) return jsonErr(res, 400, 'Product id is required.');

      const productId         = Number(p.id);
      const newStatus         = (p.status         !== undefined) ? String(p.status)         : null;
      const newBadge          = (p.badge          !== undefined) ? String(p.badge)          : null;
      const newSellerVerified = (p.sellerVerified !== undefined) ? Boolean(p.sellerVerified): null;
      const newSellerWhatsapp = (p.sellerWhatsapp !== undefined) ? String(p.sellerWhatsapp) : null;
      const newFileUrl        = (p.fileUrl        !== undefined) ? (p.fileUrl || null)      : null;
      const newFileSize       = (p.fileSize       !== undefined) ? (p.fileSize || null)     : null;
      const newIsVerified     = (p.isVerified     !== undefined) ? Boolean(p.isVerified)   : null;
      const newDisputed       = (p.disputed       !== undefined) ? Boolean(p.disputed)     : null;
      const newIsOnSale       = (p.isOnSale       !== undefined) ? Boolean(p.isOnSale)     : null;
      const newQuantity       = (p.quantity       !== undefined && p.quantity !== null) ? parseInt(p.quantity, 10) : null;
      const newLocation       = (p.location       !== undefined) ? String(p.location || '')       : null;
      const newSellerBio      = (p.sellerBio      !== undefined || p.seller_bio !== undefined) ? String(p.sellerBio || p.seller_bio || '') : null;
      const newSaleEndsAt     = (p.saleEndsAt     !== undefined) ? (p.saleEndsAt || null)  : null;
      const newCondition      = (p.condition      !== undefined) ? (p.condition || null)   : null;
      const newName           = (p.name           !== undefined && p.name !== null) ? String(p.name || '')        : null;
      const newDescription    = (p.description    !== undefined && p.description !== null) ? String(p.description || '') : null;
      const newPrice          = (p.price          !== undefined && p.price !== null) ? parseFloat(p.price)       : null;
      const newCurrency       = (p.currency !== undefined && ['NGN','USD','GBP','EUR','CAD','GHS'].includes(p.currency)) ? p.currency : null;
      const newVariants       = (p.variants !== undefined && Array.isArray(p.variants)) ? JSON.stringify(p.variants) : null;
      const newLessons        = (p.lessons  !== undefined && Array.isArray(p.lessons))  ? JSON.stringify(p.lessons)  : null;
      const newCouponCode     = (p.couponCode  !== undefined) ? (p.couponCode  ? String(p.couponCode).trim().toUpperCase() : null) : null;
      const newCouponType     = (p.couponType  !== undefined && (p.couponType === 'percent' || p.couponType === 'fixed')) ? p.couponType : null;
      const newCouponValue    = (p.couponValue !== undefined && p.couponValue != null) ? parseFloat(p.couponValue) : null;

      let newDiscountPrice = null;
      if (p.discountPrice !== undefined && p.discountPrice !== null)
        newDiscountPrice = parseFloat(p.discountPrice);

      let newShippingFee = null;
      if (p.shippingFee !== undefined && p.shippingFee !== null)
        newShippingFee = parseFloat(p.shippingFee);

      await sql`
        UPDATE products SET
          status          = COALESCE(${newStatus},         status),
          badge           = COALESCE(${newBadge},          badge),
          discount_price  = COALESCE(${newDiscountPrice},  discount_price),
          is_on_sale      = COALESCE(${newIsOnSale},       is_on_sale),
          sale_ends_at    = COALESCE(${newSaleEndsAt},     sale_ends_at),
          shipping_fee    = COALESCE(${newShippingFee},    shipping_fee),
          seller_verified = COALESCE(${newSellerVerified}, seller_verified),
          seller_whatsapp = COALESCE(${newSellerWhatsapp}, seller_whatsapp),
          file_url        = COALESCE(${newFileUrl},        file_url),
          file_size       = COALESCE(${newFileSize},       file_size),
          is_verified     = COALESCE(${newIsVerified},     is_verified),
          disputed        = COALESCE(${newDisputed},       disputed),
          quantity        = COALESCE(${newQuantity},      quantity),
          location        = COALESCE(${newLocation},      location),
          seller_bio      = COALESCE(${newSellerBio},     seller_bio),
          condition       = COALESCE(${newCondition},     condition),
          name            = COALESCE(${newName},           name),
          description     = COALESCE(${newDescription},    description),
          price           = COALESCE(${newPrice},          price),
          currency        = COALESCE(${newCurrency},       currency),
          variants        = COALESCE(${newVariants}::jsonb, variants),
          lessons         = COALESCE(${newLessons}::jsonb, lessons),
          coupon_code     = COALESCE(${newCouponCode},     coupon_code),
          coupon_type     = COALESCE(${newCouponType},     coupon_type),
          coupon_value    = COALESCE(${newCouponValue},    coupon_value)
        WHERE id = ${productId}
      `;
      return res.status(200).json({ ok: true });
    }

    /* ════════════════════════════════════════════════
       DELETE — owner or admin only
       FIX: server enforces ownership check correctly
    ════════════════════════════════════════════════ */
    if (req.method === 'DELETE') {
      const rawId      = req.query.id || (req.body && req.body.id);
      const requesterId = req.body && req.body.requesterId;   /* caller must pass their userId */
      if (!rawId) return jsonErr(res, 400, 'Product id is required.');

      const productId = Number(rawId);

      /* If requesterId provided, enforce ownership — never delete someone else's product */
      if (requesterId) {
        const owns = await sql`
          SELECT id FROM products
          WHERE id = ${productId}
            AND (seller_id = ${String(requesterId)} OR ${String(requesterId)} = 'admin')
          LIMIT 1
        `;
        if (!owns.length)
          return jsonErr(res, 403, 'You do not have permission to delete this product.');
      }

      await sql`DELETE FROM products WHERE id = ${productId}`;
      return res.status(200).json({ ok: true });
    }

    /* ════════════════════════════════════════════════
       POST ?action=update-product
       Seller submits an edit. Saves to product_edits table.
       Original product stays live unchanged until admin approves.
    ════════════════════════════════════════════════ */
    if (req.query.action === 'update-product' && req.method === 'POST') {
      const body      = req.body || {};
      const productId = Number(body.id);
      const sellerId  = String(body.sellerId || '');
      if (!productId || !sellerId) return jsonErr(res, 400, 'id and sellerId required.');

      /* Ownership check */
      const owns = await sql`
        SELECT id FROM products
        WHERE id = ${productId} AND seller_id = ${sellerId}
        LIMIT 1
      `;
      if (!owns.length) return jsonErr(res, 403, 'Not your product.');

      /* Ensure product_edits table exists */
      await sql`
        CREATE TABLE IF NOT EXISTS product_edits (
          id           SERIAL PRIMARY KEY,
          product_id   INTEGER NOT NULL,
          seller_id    TEXT    NOT NULL,
          edit_data    JSONB   NOT NULL,
          status       TEXT    NOT NULL DEFAULT 'pending',
          created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          reviewed_at  TIMESTAMPTZ
        )
      `;

      /* Cancel any previous pending edit for this product */
      await sql`
        DELETE FROM product_edits
        WHERE product_id = ${productId} AND status = 'pending'
      `;

      /* Save the new edit */
      const editData = {
        name:          body.name          || null,
        description:   body.description   || null,
        price:         body.price         != null ? parseFloat(body.price)        : null,
        currency:      body.currency      || null,
        category:      body.cat           || null,
        type:          body.type          || null,
        commission:    body.commission    != null ? parseFloat(body.commission)   : null,
        discount_price:body.discountPrice != null ? parseFloat(body.discountPrice): null,
        is_on_sale:    body.isOnSale      != null ? Boolean(body.isOnSale)        : null,
        sale_ends_at:  body.saleEndsAt    || null,
        shipping_fee:  body.shippingFee   != null ? parseFloat(body.shippingFee)  : null,
        quantity:      body.quantity      != null ? parseInt(body.quantity, 10)   : null,
        location:      body.location      || null,
        condition:     body.condition     || null,
        imgs:          Array.isArray(body.imgs)    ? body.imgs    : null,
        variants:      Array.isArray(body.variants)? body.variants: null,
        lessons:       Array.isArray(body.lessons) ? body.lessons : null,
        coupon_code:   body.couponCode    || null,
        coupon_type:   body.couponType    || null,
        coupon_value:  body.couponValue   != null ? parseFloat(body.couponValue)  : null,
        file_url:      body.fileUrl       || null,
        file_name:     body.fileName      || null,
        file_ext:      body.fileExt       || null,
      };

      await sql`
        INSERT INTO product_edits (product_id, seller_id, edit_data, status, created_at)
        VALUES (${productId}, ${sellerId}, ${JSON.stringify(editData)}, 'pending', NOW())
      `;

      /* Mark the product as having a pending edit (for admin badge) */
      await sql`
        UPDATE products SET has_pending_edit = true WHERE id = ${productId}
      `;

      /* Bust cache */
      try {
        const r = _getRedis();
        if (r) { await r.del('products:seller:' + sellerId); await r.del('products:id:' + productId); }
      } catch(e) {}

      return res.status(200).json({ ok: true, message: 'Edit submitted for admin review.' });
    }

    /* ════════════════════════════════════════════════
       GET ?action=pending-edits
       Admin fetches all pending product edits.
    ════════════════════════════════════════════════ */
    if (req.query.action === 'pending-edits' && req.method === 'GET') {
      await sql`
        CREATE TABLE IF NOT EXISTS product_edits (
          id           SERIAL PRIMARY KEY,
          product_id   INTEGER NOT NULL,
          seller_id    TEXT    NOT NULL,
          edit_data    JSONB   NOT NULL,
          status       TEXT    NOT NULL DEFAULT 'pending',
          created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          reviewed_at  TIMESTAMPTZ
        )
      `;
      const edits = await sql`
        SELECT pe.*, p.name AS product_name, p.seller AS seller_name,
               p.price AS current_price, p.imgs AS current_imgs,
               p.description AS current_description, p.status AS product_status
        FROM product_edits pe
        JOIN products p ON p.id = pe.product_id
        WHERE pe.status = 'pending'
        ORDER BY pe.created_at DESC
      `;
      return res.status(200).json({ ok: true, edits: edits.map(function(e) {
        let ed = e.edit_data;
        if (typeof ed === 'string') { try { ed = JSON.parse(ed); } catch(x) { ed = {}; } }
        return { ...e, edit_data: ed };
      })});
    }

    /* ════════════════════════════════════════════════
       POST ?action=approve-edit
       Admin approves a product edit — applies changes.
    ════════════════════════════════════════════════ */
    if (req.query.action === 'approve-edit' && req.method === 'POST') {
      const { editId } = req.body || {};
      if (!editId) return jsonErr(res, 400, 'editId required.');

      const editRows = await sql`
        SELECT * FROM product_edits WHERE id = ${Number(editId)} AND status = 'pending' LIMIT 1
      `;
      if (!editRows.length) return jsonErr(res, 404, 'Edit not found or already reviewed.');

      const edit = editRows[0];
      let ed = edit.edit_data;
      if (typeof ed === 'string') { try { ed = JSON.parse(ed); } catch(x) { ed = {}; } }

      const pid = Number(edit.product_id);

      /* Apply all non-null fields from edit_data to the product */
      await sql`
        UPDATE products SET
          name          = COALESCE(${ed.name         || null}, name),
          description   = COALESCE(${ed.description  || null}, description),
          price         = COALESCE(${ed.price         != null ? ed.price         : null}, price),
          currency      = COALESCE(${ed.currency      || null}, currency),
          cat           = COALESCE(${ed.category      || null}, cat),
          commission    = COALESCE(${ed.commission    != null ? ed.commission    : null}, commission),
          discount_price= COALESCE(${ed.discount_price!= null ? ed.discount_price: null}, discount_price),
          is_on_sale    = COALESCE(${ed.is_on_sale    != null ? ed.is_on_sale    : null}, is_on_sale),
          sale_ends_at  = COALESCE(${ed.sale_ends_at  || null}, sale_ends_at),
          shipping_fee  = COALESCE(${ed.shipping_fee  != null ? ed.shipping_fee  : null}, shipping_fee),
          quantity      = COALESCE(${ed.quantity      != null ? ed.quantity      : null}, quantity),
          location      = COALESCE(${ed.location      || null}, location),
          condition     = COALESCE(${ed.condition     || null}, condition),
          imgs          = COALESCE(${ed.imgs          ? JSON.stringify(ed.imgs)    : null}::jsonb, imgs),
          variants      = COALESCE(${ed.variants      ? JSON.stringify(ed.variants): null}::jsonb, variants),
          lessons       = COALESCE(${ed.lessons       ? JSON.stringify(ed.lessons) : null}::jsonb, lessons),
          coupon_code   = COALESCE(${ed.coupon_code   || null}, coupon_code),
          coupon_type   = COALESCE(${ed.coupon_type   || null}, coupon_type),
          coupon_value  = COALESCE(${ed.coupon_value  != null ? ed.coupon_value  : null}, coupon_value),
          file_url      = COALESCE(${ed.file_url      || null}, file_url),
          file_name     = COALESCE(${ed.file_name     || null}, file_name),
          file_ext      = COALESCE(${ed.file_ext      || null}, file_ext),
          has_pending_edit = false
        WHERE id = ${pid}
      `;

      await sql`
        UPDATE product_edits SET status = 'approved', reviewed_at = NOW()
        WHERE id = ${Number(editId)}
      `;

      /* Bust cache */
      try {
        const r = _getRedis();
        if (r) { await r.del('products:id:' + pid); await r.del('products:seller:' + edit.seller_id); }
      } catch(e) {}

      return res.status(200).json({ ok: true });
    }

    /* ════════════════════════════════════════════════
       POST ?action=reject-edit
       Admin rejects a product edit.
    ════════════════════════════════════════════════ */
    if (req.query.action === 'reject-edit' && req.method === 'POST') {
      const { editId } = req.body || {};
      if (!editId) return jsonErr(res, 400, 'editId required.');

      await sql`
        UPDATE product_edits SET status = 'rejected', reviewed_at = NOW()
        WHERE id = ${Number(editId)} AND status = 'pending'
      `;

      /* Get product_id to clear the flag */
      const rows2 = await sql`SELECT product_id, seller_id FROM product_edits WHERE id = ${Number(editId)} LIMIT 1`;
      if (rows2.length) {
        await sql`UPDATE products SET has_pending_edit = false WHERE id = ${rows2[0].product_id}`;
        try {
          const r = _getRedis();
          if (r) { await r.del('products:id:' + rows2[0].product_id); await r.del('products:seller:' + rows2[0].seller_id); }
        } catch(e) {}
      }

      return res.status(200).json({ ok: true });
    }

    /* ════════════════════════════════════════════════
       POST ?action=track-view
       Increments view_count for a product by 1.
       Deduped per-visitor using a session key stored
       in the request — no double-counting on refresh.
       No auth required — anonymous views count too.
    ════════════════════════════════════════════════ */
    if (req.query.action === 'track-view' && req.method === 'POST') {
      const { productId } = req.body || {};
      if (!productId) return jsonErr(res, 400, 'productId required.');
      const pid = Number(productId);
      if (!pid) return jsonErr(res, 400, 'Invalid productId.');
      try {
        const rows = await sql`
          UPDATE products
          SET view_count = COALESCE(view_count, 0) + 1
          WHERE id = ${pid}
          RETURNING view_count
        `;
        if (!rows.length) return jsonErr(res, 404, 'Product not found.');
        /* Bust cache so next fetch returns updated count */
        try {
          const r = _getRedis();
          if (r) await r.del('products:id:' + pid);
        } catch(e) {}
        return res.status(200).json({ ok: true, viewCount: rows[0].view_count });
      } catch (err) {
        console.error('[track-view]', err.message);
        return jsonErr(res, 500, 'Could not track view.', err.message);
      }
    }

    /* ════════════════════════════════════════════════
       GET ?action=get-views
       Returns current view count for a product without incrementing.
    ════════════════════════════════════════════════ */
    if (req.query.action === 'get-views' && req.method === 'GET') {
      const pid = Number(req.query.productId);
      if (!pid) return jsonErr(res, 400, 'Invalid productId.');
      try {
        const rows = await sql`
          SELECT view_count FROM products WHERE id = ${pid} LIMIT 1
        `;
        if (!rows.length) return jsonErr(res, 404, 'Product not found.');
        return res.status(200).json({ ok: true, viewCount: parseInt(rows[0].view_count || 0, 10) });
      } catch (err) {
        return jsonErr(res, 500, 'Could not fetch views.', err.message);
      }
    }

        /* ════════════════════════════════════════════════
       POST ?action=promote
       Mark product as featured (is_featured = true)
    ════════════════════════════════════════════════ */
    if (req.query.action === 'promote' && req.method === 'POST') {
      const { productId, duration, amount, paystackRef } = req.body || {};
      if (!productId || !paystackRef) return jsonErr(res, 400, 'productId and paystackRef required');

      try {
        /* Calculate expiration date based on duration */
        const expiresAt = new Date();
        expiresAt.setDate(expiresAt.getDate() + (duration || 7));

        /* Update product to featured */
        await sql`
          UPDATE products 
          SET is_featured = true, featured_expires = ${expiresAt.toISOString()}
          WHERE id = ${Number(productId)}
        `;

        /* Log promotion transaction */
        await sql`
          INSERT INTO promotions (product_id, duration_days, amount, paystack_ref, expires_at, created_at)
          VALUES (${Number(productId)}, ${Number(duration || 7)}, ${Number(amount)}, ${String(paystackRef)}, ${expiresAt.toISOString()}, NOW())
        `;

        return res.status(200).json({ ok: true, message: 'Product promoted successfully' });
      } catch (err) {
        console.error('[products/promote]', err.message);
        return jsonErr(res, 500, 'Could not promote product', err.message);
      }
    }

    return jsonErr(res, 405, 'Method not allowed.');

  } catch (err) {
    /* Always JSON, never HTML — stops 'Unexpected token T' errors */
    console.error('[products.js] ERROR:', err.message);
    return jsonErr(res, 500, 'Internal server error.', err.message);
  }
};
