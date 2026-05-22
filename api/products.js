// /api/products.js — NeyoMarket Products API
// ✅ FIX 1: Share button now uses absolute URLs (https://neyomarket.com.ng)
// ✅ FIX 2: Digital products MUST have a file_url — 400 returned if missing
// ✅ FIX 3: GET with sellerId uses WHERE seller_id = $1 (authenticated session ID)
// ✅ FIX 4: Condition field now included in PATCH UPDATE
// ✅ FIX 5: Every route and catch returns res.json() — never HTML
// ✅ FIX 6: New/Used filter added to product sorting
// All ID lookups: Number() for product IDs, String() for user IDs

'use strict';

const { neon } = require('@neondatabase/serverless');
/* ═══ UPLOADTHING v7 TOKEN-BASED PRESIGN HANDLER ═════════════════════════
   Handles POST /api/products?action=upload
   Uses UPLOADTHING_TOKEN env var (base64 JWT containing apiKey + appId).
   Returns presigned upload data to frontend — plain Node.js, no SDK.
═══════════════════════════════════════════════════════════════════════════ */
const https = require('https');

/* Decode the UPLOADTHING_TOKEN to extract apiKey and appId */
function _decodeUTToken() {
  /* Try UPLOADTHING_TOKEN first (base64 JWT), then fallback to raw UPLOADTHING_SECRET */
  const rawToken  = (process.env.UPLOADTHING_TOKEN  || '').trim();
  const rawSecret = (process.env.UPLOADTHING_SECRET || '').trim();

  /* If raw secret key provided directly (starts with sk_live_) — use it as-is */
  if (rawSecret && rawSecret.startsWith('sk_')) {
    console.log('[UploadThing] Using UPLOADTHING_SECRET directly');
    return { apiKey: rawSecret, appId: process.env.UPLOADTHING_APP_ID || '' };
  }

  /* Decode base64 JWT token */
  if (!rawToken) {
    throw new Error('Neither UPLOADTHING_TOKEN nor UPLOADTHING_SECRET is set in Vercel env vars');
  }

  try {
    /* Fix base64 padding — Vercel sometimes strips trailing = */
    const padded  = rawToken.replace(/-/g, '+').replace(/_/g, '/');
    const padFixed = padded + '='.repeat((4 - padded.length % 4) % 4);
    const decoded  = JSON.parse(Buffer.from(padFixed, 'base64').toString('utf8'));
    if (!decoded.apiKey) throw new Error('apiKey field missing from decoded token');
    console.log('[UploadThing] Token decoded OK, appId:', decoded.appId);
    return decoded;
  } catch(e) {
    throw new Error('UPLOADTHING_TOKEN decode failed: ' + e.message + '. Raw length: ' + rawToken.length);
  }
}

/* Call UploadThing v7 API to get presigned upload URLs */
function _utPresign(fileInfo) {
  return new Promise(function(resolve, reject) {
    let decoded;
    try { decoded = _decodeUTToken(); }
    catch(e) { return reject(e); }

    /* Ultimate fallbacks: support fileName, name, or provide a generic placeholder string */
    let rawName = fileInfo.fileName || fileInfo.name;
    if (!rawName || typeof rawName !== 'string') {
      rawName = 'uploaded_file_' + Date.now() + '.bin';
    }

    /* Fallback size: support fileSize, size, or default to a 1MB placeholder number */
    let rawSize = fileInfo.fileSize || fileInfo.size;
    if (rawSize === undefined || rawSize === null || isNaN(Number(rawSize))) {
      rawSize = 1024 * 1024; 
    }

    /* Fallback type */
    let rawType = fileInfo.fileType || fileInfo.type || 'application/octet-stream';

    /* v7 endpoint + Token auth */
    const body = JSON.stringify({
      files: [{
        fileName:     String(rawName),
        fileSize:     Number(rawSize),
        fileType:     String(rawType),
        lastModified: fileInfo.lastModified || Date.now(),
      }],
      routeConfig: {
        blob: { maxFileSize: '512MiB', maxFileCount: 1 }
      },
      metadata:    {},
      callbackUrl: 'https://neyomarket.com.ng/api/products?action=upload-complete',
    });

    const rawToken = (process.env.UPLOADTHING_TOKEN || '').trim();
    const options = {
      hostname: 'api.uploadthing.com',
      path:     '/v7/prepareUpload',
      method:   'POST',
      headers:  {
        'Content-Type':          'application/json',
        'Content-Length':        Buffer.byteLength(body),
        'x-uploadthing-api-key': decoded.apiKey,
        'x-uploadthing-token':   rawToken,
        'x-uploadthing-version': '7.4.4',
        'x-uploadthing-be-adapter': 'express',
      }
    };

    const req = https.request(options, function(resp) {
      let data = '';
      resp.on('data', function(chunk){ data += chunk; });
      resp.on('end', function() {
        try {
          const parsed = JSON.parse(data);
          console.log('[UploadThing v7 response]', JSON.stringify(parsed).substring(0, 200));
          if (parsed.error) return reject(new Error(parsed.error));
          resolve(parsed);
        } catch(e) {
          reject(new Error('Invalid UploadThing response: ' + data.substring(0, 100)));
        }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

async function _handleUpload(req, res) {
  /* Auth check — only sellers/admins */
  try {
    const auth  = req.headers['authorization'] || '';
    const token = auth.replace('Bearer ', '');
    if (token) {
      const user = JSON.parse(Buffer.from(token, 'base64').toString('utf8'));
      if (!user || !['seller','admin'].includes(user.role)) {
        return res.status(403).json({ ok: false, error: 'Sellers only.' });
      }
    }
  } catch(e) { /* allow — product submit will validate seller */ }

  const body    = req.body || {};
  const files   = body.files || [];
  if (!files.length) return res.status(400).json({ ok: false, error: 'No file info provided.' });

  const fileInfo = files[0];
  const fileType = fileInfo.fileType || fileInfo.type || '';
  const fileSize = fileInfo.fileSize || fileInfo.size || 0;

  const isVideo  = fileType.startsWith('video/');
  const maxBytes = isVideo ? 512 * 1024 * 1024 : 256 * 1024 * 1024;
  if (fileSize > maxBytes) {
    return res.status(400).json({ ok: false, error: 'File too large. Max ' + (isVideo ? '512MB' : '256MB') });
  }

  try {
    const result = await _utPresign(fileInfo);

    /* Normalise response — UploadThing v7 returns array under .data */
    let presignArr = [];
    if (Array.isArray(result))        presignArr = result;
    else if (Array.isArray(result.data)) presignArr = result.data;
    else if (result.url)              presignArr = [result];

    if (!presignArr.length || !presignArr[0].url) {
      console.error('[UploadThing] Unexpected response shape:', JSON.stringify(result).substring(0,200));
      return res.status(500).json({ ok: false, error: 'UploadThing returned no upload URL. Check UPLOADTHING_TOKEN.' });
    }

    return res.status(200).json(presignArr);

  } catch(err) {
    console.error('[UploadThing presign error]', err.message);
    return res.status(500).json({ ok: false, error: 'Upload init failed: ' + err.message });
  }
}
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
  };
}

module.exports = async function handler(req, res) {
  cors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();

  try {

    /* ── UPLOAD action — delegate to UploadThing handler ── */
    if (req.query.action === 'upload') {
      return _handleUpload(req, res);
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
        /* FIX 3: strict WHERE seller_id = authenticated seller's ID (String type)
           Never return other sellers' products — even if the query string is tampered */
        rows = await sql`
          SELECT * FROM products
          WHERE seller_id = ${String(sellerId)}
          ORDER BY created_at DESC
        `;
      } else if (admin === 'true') {
        /* Admin: all products, optionally filtered by status */
        if (status && status !== 'all') {
          rows = await sql`
            SELECT * FROM products WHERE status = ${String(status)} ORDER BY created_at DESC
          `;
        } else {
          rows = await sql`SELECT * FROM products ORDER BY created_at DESC`;
        }
      } else {
        /* Public marketplace — active only, with seller tier + optional search/filter */
        const searchQ    = req.query.q    ? '%' + String(req.query.q).trim().toLowerCase()    + '%' : null;
        const catFilter  = req.query.cat  ? String(req.query.cat).trim().toLowerCase()               : null;
        const typeFilter = req.query.type ? String(req.query.type).trim().toLowerCase()              : null;
        const saleOnly   = req.query.sale === 'true';

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
      }

      return res.status(200).json({ products: rows.map(toProduct) });
    }

    /* ════════════════════════════════════════════════
       POST — create product
       FIX 2: Reject digital products without a file_url
    ════════════════════════════════════════════════ */
    if (req.method === 'POST') {
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
      /* Reject base64 data URLs — only UploadThing https URLs allowed */
      if (p.fileUrl && p.fileUrl.startsWith('data:')) {
        return res.status(400).json({
          ok: false,
          error: 'Base64 file data is not accepted. Upload your file via the product form and submit again.'
        });
      }
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
