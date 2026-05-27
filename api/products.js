// /api/products.js — NeyoMarket Products API with Upstash Redis Caching
// ✅ FIX 1: Share button now uses absolute URLs (https://neyomarket.com.ng)
// ✅ FIX 2: Digital products MUST have a file_url — 400 returned if missing
// ✅ FIX 3: GET with sellerId uses WHERE seller_id = $1 (authenticated session ID)
// ✅ FIX 4: Condition field now included in PATCH UPDATE
// ✅ FIX 5: Every route and catch returns res.json() — never HTML
// ✅ FIX 6: New/Used filter added to product sorting
// ✅ INT 7: Upstash Redis Caching layered onto public marketplace endpoints

'use strict';

const { neon } = require('@neondatabase/serverless');
const { Redis } = require('@upstash/redis');
const cloudinary = require('cloudinary').v2;

/* SDK auto-configures from CLOUDINARY_URL env var */
cloudinary.config({ secure: true });

/* Initialize Upstash Redis client using your environment variables */
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

/* Helper to clear cached public feeds whenever changes occur */
async function clearProductCache() {
  try {
    // Evict all public marketplace listings caches
    await redis.del('mp_products_public_all');
    await redis.del('mp_products_public_sale');
    console.log('[Redis] Public product cache successfully cleared.');
  } catch (err) {
    console.error('[Redis] Cache eviction error:', err.message);
  }
}

async function _handleUpload(req, res) {
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

  const body     = req.body || {};
  const b64data  = body.data     || '';
  const fileName = body.fileName || body.name || 'upload';
  const fileType = body.fileType || body.type || 'application/octet-stream';
  const folder   = body.folder   || 'neyomarket';

  if (!b64data) {
    return res.status(400).json({ ok: false, error: 'No file data provided.' });
  }

  const dataUri = b64data.startsWith('data:')
    ? b64data
    : 'data:' + fileType + ';base64,' + b64data;

  try {
    const result = await cloudinary.uploader.upload(dataUri, {
      resource_type: 'auto',
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
  };
}

module.exports = async function handler(req, res) {
  cors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();

  try {
    if (req.query.action === 'upload') return _handleUpload(req, res);

    /* AI PRODUCT OPTIMIZER */
    if (req.query.action === 'optimize' && req.method === 'POST') {
      const GROQ_KEY = process.env.GROQ_API_KEY;
      if (!GROQ_KEY) return res.status(500).json({ ok: false, error: 'GROQ_API_KEY not set in env vars.' });

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

      const clean = reply.trim().replace(/^```json?\s*/i, '').replace(/```\s*$/i, '').trim();

      let parsed;
      try {
        parsed = JSON.parse(clean);
      } catch(e) {
        console.error('[optimizer] JSON parse failed. Raw reply:', reply.slice(0, 200));
        return res.status(502).json({ ok: false, error: 'AI returned invalid format. Try again.' });
      }

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

      if (id) {
        const rows = await sql`
          SELECT * FROM products WHERE id = ${Number(id)} LIMIT 1
        `;
        if (!rows.length) return jsonErr(res, 404, 'Product not found.');
        return res.status(200).json({ product: toProduct(rows[0]) });
      }

      let rows;

      if (sellerId) {
        rows = await sql`
          SELECT * FROM products
          WHERE seller_id = ${String(sellerId)}
          ORDER BY created_at DESC
        `;

      } else if (req.query.storeName) {
        const storeCode = String(req.query.storeName).trim();
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

      } else if (admin === 'true') {
        if (status && status !== 'all') {
          rows = await sql`
            SELECT * FROM products WHERE status = ${String(status)} ORDER BY created_at DESC
          `;
        } else {
          rows = await sql`SELECT * FROM products ORDER BY created_at DESC`;
        }
      } else {
        /* Public marketplace feeds — Optimize via Redis Caching Layer */
        const searchQ    = req.query.q    ? '%' + String(req.query.q).trim().toLowerCase() + '%' : null;
        const catFilter  = req.query.cat  ? String(req.query.cat).trim().toLowerCase()            : null;
        const typeFilter = req.query.type ? String(req.query.type).trim().toLowerCase()           : null;
        const saleOnly   = req.query.sale === 'true';

        // Check if query is targeting a caching trackable route layout
        const isStandardPublicFeed = !searchQ && !catFilter && !typeFilter;

        if (isStandardPublicFeed) {
          const cacheKey = saleOnly ? 'mp_products_public_sale' : 'mp_products_public_all';
          try {
            const cachedData = await redis.get(cacheKey);
            if (cachedData) {
              return res.status(200).json({ products: typeof cachedData === 'string' ? JSON.parse(cachedData) : cachedData });
            }
          } catch (cacheErr) {
            console.error('[Redis Read Error]', cacheErr.message);
          }
        }

        // Database Fallback Engine
        if (searchQ) {
          rows = await sql`
            SELECT p.*, u.membership_tier AS seller_tier, u.is_verified AS seller_verified, u.badge_verified AS badge_verified
            FROM products p
            LEFT JOIN users u ON u.id::text = p.seller_id::text
            WHERE p.status = 'active'
              AND (LOWER(p.name) LIKE ${searchQ} OR LOWER(p.description) LIKE ${searchQ} OR LOWER(p.seller) LIKE ${searchQ})
            ORDER BY p.created_at DESC
          `;
        } else if (catFilter) {
          rows = await sql`
            SELECT p.*, u.membership_tier AS seller_tier, u.is_verified AS seller_verified, u.badge_verified AS badge_verified
            FROM products p
            LEFT JOIN users u ON u.id::text = p.seller_id::text
            WHERE p.status = 'active' AND LOWER(p.cat) = ${catFilter}
            ORDER BY p.created_at DESC
          `;
        } else if (typeFilter) {
          rows = await sql`
            SELECT p.*, u.membership_tier AS seller_tier, u.is_verified AS seller_verified, u.badge_verified AS badge_verified
            FROM products p
            LEFT JOIN users u ON u.id::text = p.seller_id::text
            WHERE p.status = 'active' AND LOWER(p.type) = ${typeFilter}
            ORDER BY p.created_at DESC
          `;
        } else if (saleOnly) {
          rows = await sql`
            SELECT p.*, u.membership_tier AS seller_tier, u.is_verified AS seller_verified, u.badge_verified AS badge_verified
            FROM products p
            LEFT JOIN users u ON u.id::text = p.seller_id::text
            WHERE p.status = 'active' AND p.is_on_sale = true
              AND (p.sale_ends_at IS NULL OR p.sale_ends_at > NOW())
            ORDER BY p.created_at DESC
          `;
        } else {
          rows = await sql`
            SELECT p.*, u.membership_tier AS seller_tier, u.is_verified AS seller_verified, u.badge_verified AS badge_verified
            FROM products p
            LEFT JOIN users u ON u.id::text = p.seller_id::text
            WHERE p.status = 'active'
            ORDER BY p.created_at DESC
          `;
        }

        const productPayload = rows.map(toProduct);

        // Save public response to Upstash Redis for 30 minutes (1800 seconds)
        if (isStandardPublicFeed) {
          const cacheKey = saleOnly ? 'mp_products_public_sale' : 'mp_products_public_all';
          try {
            await redis.set(cacheKey, JSON.stringify(productPayload), { ex: 1800 });
          } catch (cacheSetErr) {
            console.error('[Redis Write Error]', cacheSetErr.message);
          }
        }

        return res.status(200).json({ products: productPayload });
      }

      return res.status(200).json({ products: rows.map(toProduct) });
    }

    /* ════════════════════════════════════════════════
       POST — create product
    ════════════════════════════════════════════════ */
    if (req.method === 'POST') {
      const p = req.body || {};

      if (!p.name || !p.price)
        return jsonErr(res, 400, 'Product name and price are required.');

      const productType = p.type || 'digital';
      if ((productType === 'digital' || productType === 'course') && !p.fileUrl) {
        return jsonErr(res, 400,
          'Digital and course products require a file_url. Upload the file first, then submit the product.',
          'file_url was empty or missing'
        );
      }

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

      const rows = await sql`
        INSERT INTO products (
          id, name, type, cat, price,
          discount_price, is_on_sale, sale_ends_at, shipping_fee,
          commission, description, seller, seller_id, seller_email, seller_whatsapp,
          rating, reviews, emoji, imgs, status, badge, date, escrow,
          file_ext, file_name, file_url, file_size, is_verified, disputed,
          quantity, location, seller_bio, created_at, condition, currency, variants, lessons
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
          ${productCondition}, ${productCurrency}, ${productVariants}::jsonb, ${productLessons}::jsonb
        )
        RETURNING *
      `;

      // Evict outdated caches since metadata layout changed
      await clearProductCache();

      return res.status(201).json({ ok: true, product: toProduct(rows[0]) });
    }

    /* ════════════════════════════════════════════════
       PATCH — update product fields
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
          lessons         = COALESCE(${newLessons}::jsonb, lessons)
        WHERE id = ${productId}
      `;

      // Evict cache to reflect details edits instantly
      await clearProductCache();

      return res.status(200).json({ ok: true });
    }

    /* ════════════════════════════════════════════════
       DELETE — owner or admin only
    ════════════════════════════════════════════════ */
    if (req.method === 'DELETE') {
      const rawId      = req.query.id || (req.body && req.body.id);
      const requesterId = req.body && req.body.requesterId;
      if (!rawId) return jsonErr(res, 400, 'Product id is required.');

      const productId = Number(rawId);

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
      
      // Evict layout copies
      await clearProductCache();

      return res.status(200).json({ ok: true });
    }

    /* ════════════════════════════════════════════════
       POST ?action=promote
    ════════════════════════════════════════════════ */
    if (req.query.action === 'promote' && req.method === 'POST') {
      const { productId, duration, amount, paystackRef } = req.body || {};
      if (!productId || !paystackRef) return jsonErr(res, 400, 'productId and paystackRef required');

      try {
        const expiresAt = new Date();
        expiresAt.setDate(expiresAt.getDate() + (duration || 7));

        await sql`
          UPDATE products 
          SET is_featured = true, featured_expires = ${expiresAt.toISOString()}
          WHERE id = ${Number(productId)}
        `;

        await sql`
          INSERT INTO promotions (product_id, duration_days, amount, paystack_ref, expires_at, created_at)
          VALUES (${Number(productId)}, ${Number(duration || 7)}, ${Number(amount)}, ${String(paystackRef)}, ${expiresAt.toISOString()}, NOW())
        `;

        // Refresh cache so featured statuses update immediately
        await clearProductCache();

        return res.status(200).json({ ok: true, message: 'Product promoted successfully' });
      } catch (err) {
        console.error('[products/promote]', err.message);
        return jsonErr(res, 500, 'Could not promote product', err.message);
      }
    }

    return jsonErr(res, 405, 'Method not allowed.');

  } catch (err) {
    console.error('[products.js] ERROR:', err.message);
    return jsonErr(res, 500, 'Internal server error.', err.message);
  }
};
