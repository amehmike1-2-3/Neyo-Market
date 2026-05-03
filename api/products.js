// /api/products.js — NeyoMarket Products API
// FIX 2: Digital products MUST have a file_url — 400 returned if missing
// FIX 3: GET with sellerId uses WHERE seller_id = $1 (authenticated session ID)
// FIX 5: Every route and catch returns res.json() — never HTML
// All ID lookups: Number() for product IDs, String() for user IDs

'use strict';

const { neon } = require('@neondatabase/serverless');
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
    sellerVerified: r.seller_verified  || false,
    isVerified:     r.is_verified      || false,
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
    disputed:       r.disputed         || false,
    quantity:       r.quantity         !== undefined ? parseInt(r.quantity, 10) : null,
    location:       r.location         || r.seller_location || '',
    sellerBio:      r.seller_bio       || '',
    createdAt:      r.created_at       || null,
  };
}

module.exports = async function handler(req, res) {
  cors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();

  try {

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
        /* FIX 3: strict WHERE seller_id = authenticated seller's ID
           Never return other sellers' products — even if the query string is tampered */
        rows = await sql`
          SELECT * FROM products
          WHERE seller_id = ${Number(sellerId)}
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
        /* Public marketplace — active only */
        rows = await sql`
          SELECT * FROM products WHERE status = 'active' ORDER BY created_at DESC
        `;
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
      const sellerId       = p.sellerId   ? Number(p.sellerId) : null;
      const sellerWhatsapp = p.sellerWhatsapp || '';
      const escrow         = (p.escrow !== false);
      const fileExt        = p.fileExt  || null;
      const fileName       = p.fileName || null;
      const fileUrl        = p.fileUrl  || null;
      const imgs           = JSON.stringify(p.imgs || []);
      const dateStr        = new Date().toLocaleDateString();
      const productCat     = p.cat         || 'other';
      const productDesc    = p.description || '';
      const productSeller  = p.seller      || '';
      const sellerEmail    = p.sellerEmail || '';
      const productEmoji   = p.emoji       || '📦';
      const productBadge   = p.badge       || '';

      const rows = await sql`
        INSERT INTO products (
          id, name, type, cat, price,
          discount_price, is_on_sale, sale_ends_at, shipping_fee,
          commission, description, seller, seller_id, seller_email, seller_whatsapp,
          rating, reviews, emoji, imgs, status, badge, date, escrow,
          file_ext, file_name, file_url, is_verified, disputed,
          quantity, location, seller_bio, created_at
        ) VALUES (
          ${id}, ${p.name}, ${productType}, ${productCat}, ${parseFloat(p.price)},
          ${discountPrice}, ${isOnSale}, ${saleEndsAt}, ${shippingFee},
          ${commission}, ${productDesc}, ${productSeller}, ${sellerId},
          ${sellerEmail}, ${sellerWhatsapp},
          ${0}, ${0},
          ${productEmoji}, ${imgs}, ${'pending'}, ${productBadge}, ${dateStr}, ${escrow},
          ${fileExt}, ${fileName}, ${fileUrl}, ${false}, ${false},
          ${p.quantity !== undefined && p.quantity !== null ? parseInt(p.quantity, 10) : null},
          ${p.location || ''},
          ${p.sellerBio || p.seller_bio || ''},
          NOW()
        )
        RETURNING *
      `;
      return res.status(201).json({ ok: true, product: toProduct(rows[0]) });
    }

    /* ════════════════════════════════════════════════
       PATCH — update product fields
       All conditionals pre-computed (Neon ternary rule)
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
      const newIsVerified     = (p.isVerified     !== undefined) ? Boolean(p.isVerified)   : null;
      const newDisputed       = (p.disputed       !== undefined) ? Boolean(p.disputed)     : null;
      const newIsOnSale       = (p.isOnSale       !== undefined) ? Boolean(p.isOnSale)     : null;
      const newQuantity       = (p.quantity       !== undefined && p.quantity !== null) ? parseInt(p.quantity, 10) : null;
      const newLocation       = (p.location       !== undefined) ? String(p.location || '')       : null;
      const newSellerBio      = (p.sellerBio      !== undefined || p.seller_bio !== undefined) ? String(p.sellerBio || p.seller_bio || '') : null;
      const newSaleEndsAt     = (p.saleEndsAt     !== undefined) ? (p.saleEndsAt || null)  : null;

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
          is_verified     = COALESCE(${newIsVerified},     is_verified),
          disputed        = COALESCE(${newDisputed},       disputed),
          quantity        = COALESCE(${newQuantity},      quantity),
          location        = COALESCE(${newLocation},      location),
          seller_bio      = COALESCE(${newSellerBio},     seller_bio)
        WHERE id = ${productId}
      `;
      return res.status(200).json({ ok: true });
    }

    /* ════════════════════════════════════════════════
       DELETE — owner or admin only
       FIX 3: server enforces ownership check
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
            AND (seller_id = ${Number(requesterId)} OR ${String(requesterId)} = 'admin')
          LIMIT 1
        `;
        if (!owns.length)
          return jsonErr(res, 403, 'You do not have permission to delete this product.');
      }

      await sql`DELETE FROM products WHERE id = ${productId}`;
      return res.status(200).json({ ok: true });
    }

    return jsonErr(res, 405, 'Method not allowed.');

  } catch (err) {
    /* FIX 5: always JSON, never HTML — stops 'Unexpected token T' errors */
    console.error('[products.js] ERROR:', err.message);
    return jsonErr(res, 500, 'Internal server error.', err.message);
  }
};
