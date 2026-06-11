// /api/products.js — NeyoMarket Products API
'use strict';

const { neon } = require('@neondatabase/serverless');
const cloudinary = require('cloudinary').v2;

cloudinary.config({ secure: true });

let redis = null;

function _getRedis() {
  if (redis) return redis;
  const url   = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return null;
  try {
    const { Redis } = require('@upstash/redis');
    redis = new Redis({ url, token });
    return redis;
  } catch(e) {
    console.warn('[redis] Caching disabled:', e.message);
    return null;
  }
}

async function cacheGet(key) {
  try {
    const r = _getRedis();
    if (!r) return null;
    return await r.get(key);
  } catch(e) {
    return null;
  }
}

async function cacheSet(key, value, ttlSeconds) {
  try {
    const r = _getRedis();
    if (!r) return;
    await r.set(key, value, { ex: ttlSeconds });
  } catch(e) {}
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

  const body = req.body || {};
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

    return res.status(200).json({
      ok:         true,
      url:        result.secure_url,
      public_id:  result.public_id,
      format:     result.format,
      bytes:      result.bytes,
    });
  } catch(err) {
    return res.status(500).json({ ok: false, error: 'Upload failed: ' + err.message });
  }
}

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
    lessons:        r.lessons || [],
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

    if (req.query.action === 'optimize' && req.method === 'POST') {
      const GROQ_KEY = process.env.GROQ_API_KEY;
      if (!GROQ_KEY) return res.status(500).json({ ok: false, error: 'GROQ_API_KEY not set.' });

      const body  = req.body || {};
      const title = String(body.title || '').trim();
      const cat   = String(body.category || '').trim();
      const price = String(body.price || '').trim();
      const type  = String(body.type || '').trim();

      if (!title) return res.status(400).json({ ok: false, error: 'Product title is required.' });

      const systemPrompt = "You are a world-class e-commerce product optimizer for NeyoMarket. Analyze details and return ONLY a raw JSON object: {\"optimized_title\":\"...\",\"key_selling_point\":\"...\",\"target_tags\":[\"tag1\",\"tag2\",\"tag3\"]}";
      const userPrompt = `Optimize this: Title: ${title}, Cat: ${cat}, Type: ${type}, Price: ₦${price}`;

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
        return res.status(502).json({ ok: false, error: 'Could not reach AI.' });
      }

      if (!resp.ok) return res.status(resp.status).json({ ok: false, error: 'AI error.' });
      const reply = data.choices?.[0]?.message?.content;
      if (!reply) return res.status(502).json({ ok: false, error: 'No response from AI.' });

      const clean = reply.trim().replace(/^```json?\s*/i, '').replace(/```\s*$/i, '').trim();
      const parsed = JSON.parse(clean);
      return res.status(200).json({ ok: true, result: parsed });
    }

    if (req.method === 'GET') {
      const { admin, sellerId, status, id } = req.query;

      if (id) {
        const cacheKey = 'products:id:' + id;
        const cached = await cacheGet(cacheKey);
        if (cached) return res.status(200).json({ product: cached, _cached: true });

        const rows = await sql`SELECT * FROM products WHERE id = ${Number(id)} LIMIT 1`;
        if (!rows.length) return jsonErr(res, 404, 'Product not found.');

        const product = toProduct(rows[0]);
        await cacheSet(cacheKey, product, 120);
        return res.status(200).json({ product });
      }

      let rows;
      if (sellerId) {
        const cacheKey = 'products:seller:' + String(sellerId);
        const cached   = await cacheGet(cacheKey);
        if (cached) return res.status(200).json({ products: cached, _cached: true });

        rows = await sql`SELECT * FROM products WHERE seller_id = ${String(sellerId)} ORDER BY created_at DESC`;
        const mapped = rows.map(toProduct);
        await cacheSet(cacheKey, mapped, 60);
        return res.status(200).json({ products: mapped });

      } else if (req.query.storeName) {
        const storeCode = String(req.query.storeName).trim();
        const sellerRows = await sql`SELECT id FROM users WHERE aff_code = ${storeCode} OR id::text = ${storeCode} LIMIT 1`;
        
        if (sellerRows.length) {
          const sid = String(sellerRows[0].id);
          rows = await sql`
            SELECT p.*, u.membership_tier AS seller_tier, u.is_verified AS seller_verified, u.badge_verified AS badge_verified
            FROM products p LEFT JOIN users u ON u.id::text = p.seller_id::text
            WHERE LOWER(p.status) IN ('active','approved','published') AND (p.seller_id::text = ${sid} OR p.seller_id::text = ${storeCode})
            ORDER BY p.created_at DESC
          `;
        } else {
          rows = await sql`
            SELECT p.*, u.membership_tier AS seller_tier, u.is_verified AS seller_verified, u.badge_verified AS badge_verified
            FROM products p LEFT JOIN users u ON u.id::text = p.seller_id::text
            WHERE LOWER(p.status) IN ('active','approved','published') AND (LOWER(p.seller) = LOWER(${storeCode}) OR p.seller_id::text = ${storeCode})
            ORDER BY p.created_at DESC
          `;
        }
        return res.status(200).json({ products: rows.map(toProduct) });

      } else if (admin === 'true') {
        if (status && status !== 'all') {
          rows = await sql`SELECT * FROM products WHERE status = ${String(status)} ORDER BY created_at DESC`;
        } else {
          rows = await sql`SELECT * FROM products ORDER BY created_at DESC`;
        }
        return res.status(200).json({ products: rows.map(toProduct) });

      } else {
        const searchQ    = req.query.q    ? '%' + String(req.query.q).trim().toLowerCase() + '%' : null;
        const catFilter  = req.query.cat  ? String(req.query.cat).trim().toLowerCase() : null;
        const typeFilter = req.query.type ? String(req.query.type).trim().toLowerCase() : null;
        const saleOnly   = req.query.sale === 'true';

        if (searchQ) {
          rows = await sql`
            SELECT p.*, u.membership_tier AS seller_tier, u.is_verified AS seller_verified, u.badge_verified AS badge_verified
            FROM products p LEFT JOIN users u ON u.id::text = p.seller_id::text
            WHERE p.status = 'active' AND (LOWER(p.name) LIKE ${searchQ} OR LOWER(p.description) LIKE ${searchQ} OR LOWER(p.seller) LIKE ${searchQ})
            ORDER BY p.created_at DESC
          `;
          return res.status(200).json({ products: rows.map(toProduct) });
        } else if (catFilter) {
          const cacheKey = 'products:cat:' + catFilter;
          const cached   = await cacheGet(cacheKey);
          if (cached) return res.status(200).json({ products: cached, _cached: true });

          rows = await sql`
            SELECT p.*, u.membership_tier AS seller_tier, u.is_verified AS seller_verified, u.badge_verified AS badge_verified
            FROM products p LEFT JOIN users u ON u.id::text = p.seller_id::text
            WHERE p.status = 'active' AND LOWER(p.cat) = ${catFilter} ORDER BY p.created_at DESC
          `;
          const mapped = rows.map(toProduct);
          await cacheSet(cacheKey, mapped, 60);
          return res.status(200).json({ products: mapped });
        } else if (typeFilter) {
          const cacheKey = 'products:type:' + typeFilter;
          const cached   = await cacheGet(cacheKey);
          if (cached) return res.status(200).json({ products: cached, _cached: true });

          rows = await sql`
            SELECT p.*, u.membership_tier AS seller_tier, u.is_verified AS seller_verified, u.badge_verified AS badge_verified
            FROM products p LEFT JOIN users u ON u.id::text = p.seller_id::text
            WHERE p.status = 'active' AND LOWER(p.type) = ${typeFilter} ORDER BY p.created_at DESC
          `;
          const mapped = rows.map(toProduct);
          await cacheSet(cacheKey, mapped, 60);
          return res.status(200).json({ products: mapped });
        } else if (saleOnly) {
          const cacheKey = 'products:sale';
          const cached   = await cacheGet(cacheKey);
          if (cached) return res.status(200).json({ products: cached, _cached: true });

          rows = await sql`
            SELECT p.*, u.membership_tier AS seller_tier, u.is_verified AS seller_verified, u.badge_verified AS badge_verified
            FROM products p LEFT JOIN users u ON u.id::text = p.seller_id::text
            WHERE p.status = 'active' AND p.is_on_sale = true AND (p.sale_ends_at IS NULL OR p.sale_ends_at > NOW())
            ORDER BY p.created_at DESC
          `;
          const mapped = rows.map(toProduct);
          await cacheSet(cacheKey, mapped, 60);
          return res.status(200).json({ products: mapped });
        } else {
          const cacheKey = 'products:public';
          const cached   = await cacheGet(cacheKey);
          if (cached) return res.status(200).json({ products: cached, _cached: true });

          rows = await sql`
            SELECT p.*, u.membership_tier AS seller_tier, u.is_verified AS seller_verified, u.badge_verified AS badge_verified
            FROM products p LEFT JOIN users u ON u.id::text = p.seller_id::text
            WHERE p.status = 'active' ORDER BY p.created_at DESC
          `;
          const mapped = rows.map(toProduct);
          await cacheSet(cacheKey, mapped, 60);
          return res.status(200).json({ products: mapped });
        }
      }
    }

    if (req.method === 'POST' && !req.query.action) {
      const p = req.body || {};
      if (!p.name || !p.price) return jsonErr(res, 400, 'Product name and price are required.');

      const productType = p.type || 'digital';
      if ((productType === 'digital' || productType === 'course') && !p.fileUrl) {
        return jsonErr(res, 400, 'Digital/course products require a file_url.');
      }

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
      const productVariants  = (p.variants && Array.isArray(p.variants)) ? JSON.stringify(p.variants) : '[]';
      const productLessons   = (p.lessons  && Array.isArray(p.lessons))  ? JSON.stringify(p.lessons)  : '[]';
      const couponCode       = p.couponCode  ? String(p.couponCode).trim().toUpperCase()  : null;
      const couponType       = (p.couponType === 'percent' || p.couponType === 'fixed') ? p.couponType : null;
      const couponValue      = (p.couponValue != null && couponCode) ? parseFloat(p.couponValue) : null;

      const rows = await sql`
        INSERT INTO products (
          name, type, cat, price, discount_price, is_on_sale, sale_ends_at, shipping_fee,
          commission, description, seller, seller_id, seller_email, seller_whatsapp,
          rating, reviews, emoji, imgs, status, badge, date, escrow, file_ext, file_name,
          file_url, file_size, is_verified, disputed, quantity, location, seller_bio,
          created_at, condition, currency, variants, lessons, coupon_code, coupon_type, coupon_value
        ) VALUES (
          ${p.name}, ${productType}, ${productCat}, ${parseFloat(p.price)}, ${discountPrice}, ${isOnSale}, ${saleEndsAt}, ${shippingFee},
          ${commission}, ${productDesc}, ${productSeller}, ${sellerId}, ${sellerEmail}, ${sellerWhatsapp},
          0, 0, ${productEmoji}, ${imgs}, 'pending', ${productBadge}, ${dateStr}, ${escrow}, ${fileExt}, ${fileName},
          ${fileUrl}, ${fileSize}, false, false, ${p.quantity != null ? parseInt(p.quantity, 10) : null}, ${p.location || ''}, ${p.sellerBio || ''},
          NOW(), ${productCondition}, ${productCurrency}, ${productVariants}::jsonb, ${productLessons}::jsonb, ${couponCode}, ${couponType}, ${couponValue}
        ) RETURNING *
      `;
      return res.status(201).json({ ok: true, product: toProduct(rows[0]) });
    }

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
      const newQuantity       = (p.quantity       != null) ? parseInt(p.quantity, 10) : null;
      const newLocation       = (p.location       !== undefined) ? String(p.location || '')       : null;
      const newSellerBio      = (p.sellerBio      !== undefined) ? String(p.sellerBio || '') : null;
      const newSaleEndsAt     = (p.saleEndsAt     !== undefined) ? (p.saleEndsAt || null)  : null;
      const newCondition      = (p.condition      !== undefined) ? (p.condition || null)   : null;
      const newName           = (p.name           != null) ? String(p.name)        : null;
      const newDescription    = (p.description    != null) ? String(p.description) : null;
      const newPrice          = (p.price          != null) ? parseFloat(p.price)       : null;
      const newCurrency       = (p.currency !== undefined && ['NGN','USD','GBP','EUR','CAD','GHS'].includes(p.currency)) ? p.currency : null;
      const newVariants       = (p.variants !== undefined && Array.isArray(p.variants)) ? JSON.stringify(p.variants) : null;
      const newLessons        = (p.lessons  !== undefined && Array.isArray(p.lessons))  ? JSON.stringify(p.lessons)  : null;
      const newCouponCode     = (p.couponCode  !== undefined) ? (p.couponCode  ? String(p.couponCode).trim().toUpperCase() : null) : null;
      const newCouponType     = (p.couponType  !== undefined && (p.couponType === 'percent' || p.couponType === 'fixed')) ? p.couponType : null;
      const newCouponValue    = (p.couponValue !== undefined && p.couponValue != null) ? parseFloat(p.couponValue) : null;

      const newDiscountPrice  = (p.discountPrice != null) ? parseFloat(p.discountPrice) : null;
      const newShippingFee    = (p.shippingFee != null) ? parseFloat(p.shippingFee) : null;

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

    if (req.method === 'DELETE') {
      const rawId      = req.query.id || req.body?.id;
      const requesterId = req.body?.requesterId;
      if (!rawId) return jsonErr(res, 400, 'Product id is required.');

      const productId = Number(rawId);
      if (requesterId) {
        const owns = await sql`SELECT id FROM products WHERE id = ${productId} AND (seller_id = ${String(requesterId)} OR ${String(requesterId)} = 'admin') LIMIT 1`;
        if (!owns.length) return jsonErr(res, 403, 'Permission denied.');
      }

      await sql`DELETE FROM products WHERE id = ${productId}`;
      return res.status(200).json({ ok: true });
    }

    if (req.query.action === 'update-product' && req.method === 'POST') {
      const body      = req.body || {};
      const productId = Number(body.id);
      const sellerId  = String(body.sellerId || '');
      if (!productId || !sellerId) return jsonErr(res, 400, 'id and sellerId required.');

      const owns = await sql`SELECT id FROM products WHERE id = ${productId} AND seller_id = ${sellerId} LIMIT 1`;
      if (!owns.length) return jsonErr(res, 403, 'Not your product.');

      await sql`
        CREATE TABLE IF NOT EXISTS product_edits (
          id           SERIAL PRIMARY KEY,
          product_id   BIGINT NOT NULL,
          seller_id    TEXT    NOT NULL,
          edit_data    JSONB   NOT NULL,
          status       TEXT    NOT NULL DEFAULT 'pending',
          created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          reviewed_at  TIMESTAMPTZ
        )
      `;

      await sql`DELETE FROM product_edits WHERE product_id = ${productId} AND status = 'pending'`;

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

      await sql`UPDATE products SET has_pending_edit = true WHERE id = ${productId}`;

      try {
        const r = _getRedis();
        if (r) { await r.del('products:seller:' + sellerId); await r.del('products:id:' + productId); }
      } catch(e) {}

      return res.status(200).json({ ok: true, message: 'Edit submitted for admin review.' });
    }

    if (req.query.action === 'pending-edits' && req.method === 'GET') {
      await sql`
        CREATE TABLE IF NOT EXISTS product_edits (
          id           SERIAL PRIMARY KEY,
          product_id   BIGINT NOT NULL,
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

    if (req.query.action === 'approve-edit' && req.method === 'POST') {
      const { editId } = req.body || {};
      if (!editId) return jsonErr(res, 400, 'editId required.');

      const editRows = await sql`SELECT * FROM product_edits WHERE id = ${Number(editId)} AND status = 'pending' LIMIT 1`;
      if (!editRows.length) return jsonErr(res, 404, 'Edit not found.');

      const edit = editRows[0];
      let ed = edit.edit_data;
      if (typeof ed === 'string') { try { ed = JSON.parse(ed); } catch(x) { ed = {}; } }

      const pid = Number(edit.product_id);

      await sql`
        UPDATE products SET
          name          = COALESCE(${ed.name}, name),
          description   = COALESCE(${ed.description}, description),
          price         = COALESCE(${ed.price != null ? ed.price : null}, price),
          currency      = COALESCE(${ed.currency}, currency),
          cat           = COALESCE(${ed.category}, cat),
          commission    = COALESCE(${ed.commission != null ? ed.commission : null}, commission),
          discount_price= COALESCE(${ed.discount_price != null ? ed.discount_price : null}, discount_price),
          is_on_sale    = COALESCE(${ed.is_on_sale != null ? ed.is_on_sale : null}, is_on_sale),
          sale_ends_at  = COALESCE(${ed.sale_ends_at}, sale_ends_at),
          shipping_fee  = COALESCE(${ed.shipping_fee != null ? ed.shipping_fee : null}, shipping_fee),
          quantity      = COALESCE(${ed.quantity != null ? ed.quantity : null}, quantity),
          location      = COALESCE(${ed.location}, location),
          condition     = COALESCE(${ed.condition}, condition),
          imgs          = COALESCE(${ed.imgs ? JSON.stringify(ed.imgs) : null}::jsonb, imgs),
          variants      = COALESCE(${ed.variants ? JSON.stringify(ed.variants) : null}::jsonb, variants),
          lessons       = COALESCE(${ed.lessons ? JSON.stringify(ed.lessons) : null}::jsonb, lessons),
          coupon_code   = COALESCE(${ed.coupon_code}, coupon_code),
          coupon_type   = COALESCE(${ed.coupon_type}, coupon_type),
          coupon_value  = COALESCE(${ed.coupon_value != null ? ed.coupon_value : null}, coupon_value),
          file_url      = COALESCE(${ed.file_url}, file_url),
          file_name     = COALESCE(${ed.file_name}, file_name),
          file_ext      = COALESCE(${ed.file_ext}, file_ext),
          has_pending_edit = false
        WHERE id = ${pid}
      `;

      await sql`UPDATE product_edits SET status = 'approved', reviewed_at = NOW() WHERE id = ${Number(editId)}`;

      try {
        const r = _getRedis();
        if (r) { await r.del('products:id:' + pid); await r.del('products:seller:' + edit.seller_id); }
      } catch(e) {}

      return res.status(200).json({ ok: true });
    }

    if (req.query.action === 'reject-edit' && req.method === 'POST') {
      const { editId } = req.body || {};
      if (!editId) return jsonErr(res, 400, 'editId required.');

      await sql`UPDATE product_edits SET status = 'rejected', reviewed_at = NOW() WHERE id = ${Number(editId)} AND status = 'pending'`;

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

    if (req.query.action === 'track-view' && req.method === 'POST') {
      const { productId } = req.body || {};
      if (!productId) return jsonErr(res, 400, 'productId required.');
      const pid = Number(productId);
      if (!pid) return jsonErr(res, 400, 'Invalid productId.');
      try {
        const rows = await sql`UPDATE products SET view_count = COALESCE(view_count, 0) + 1 WHERE id = ${pid} RETURNING view_count`;
        if (!rows.length) return jsonErr(res, 404, 'Product not found.');
        try {
          const r = _getRedis();
          if (r) await r.del('products:id:' + pid);
        } catch(e) {}
        return res.status(200).json({ ok: true, viewCount: rows[0].view_count });
      } catch (err) {
        return jsonErr(res, 500, 'Could not track view.', err.message);
      }
    }

    if (req.query.action === 'get-views' && req.method === 'GET') {
      const pid = Number(req.query.productId);
      if (!pid) return jsonErr(res, 400, 'Invalid productId.');
      try {
        const rows = await sql`SELECT view_count FROM products WHERE id = ${pid} LIMIT 1`;
        if (!rows.length) return jsonErr(res, 404, 'Product not found.');
        return res.status(200).json({ ok: true, viewCount: parseInt(rows[0].view_count || 0, 10) });
      } catch (err) {
        return jsonErr(res, 500, 'Could not fetch views.', err.message);
      }
    }

    if (req.query.action === 'promote' && req.method === 'POST') {
      const { productId, duration, amount, paystackRef } = req.body || {};
      if (!productId || !paystackRef) return jsonErr(res, 400, 'productId and paystackRef required');

      try {
        const expiresAt = new Date();
        expiresAt.setDate(expiresAt.getDate() + (duration || 7));

        await sql`UPDATE products SET is_featured = true, featured_expires = ${expiresAt.toISOString()} WHERE id = ${Number(productId)}`;
        await sql`INSERT INTO promotions (product_id, duration_days, amount, paystack_ref, expires_at, created_at) VALUES (${Number(productId)}, ${Number(duration || 7)}, ${Number(amount)}, ${String(paystackRef)}, ${expiresAt.toISOString()}, NOW())`;

        return res.status(200).json({ ok: true, message: 'Product promoted successfully' });
      } catch (err) {
        return jsonErr(res, 500, 'Could not promote product', err.message);
      }
    }

    return jsonErr(res, 405, 'Method not allowed.');
  } catch (err) {
    console.error('[products.js] ERROR:', err.message);
    return jsonErr(res, 500, 'Internal server error.', err.message);
  }
};

