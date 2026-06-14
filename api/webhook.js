// /api/webhook.js — NeyoMarket Paystack Webhook Handler
// Listens for: charge.success, dedicated_virtual_account.success
// On success: sets order status to 'escrow_held', links digital file_url to order

const { neon } = require('@neondatabase/serverless');
const crypto   = require('crypto');

const sql = neon(process.env.DATABASE_URL);
const PSK = process.env.PAYSTACK_SECRET_KEY;

module.exports = async function handler(req, res) {
  /* ── Only accept POST ── */
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  /* ── 1. VERIFY PAYSTACK SIGNATURE ──────────────────────────────────────
     Paystack signs every webhook with HMAC-SHA512 using your secret key.
     NEVER skip this check — it prevents fake webhook attacks.
  ─────────────────────────────────────────────────────────────────────── */
  const rawBody = await getRawBody(req);
  const signature = req.headers['x-paystack-signature'];

  if (!PSK) {
    console.error('[webhook.js] PAYSTACK_SECRET_KEY not configured');
    return res.status(500).json({ error: 'Webhook secret not configured' });
  }

  const expectedSig = crypto
    .createHmac('sha512', PSK)
    .update(rawBody)
    .digest('hex');

  if (signature !== expectedSig) {
    console.warn('[webhook.js] Invalid signature — possible fake request');
    return res.status(401).json({ error: 'Invalid signature' });
  }

  /* ── 2. PARSE EVENT ── */
  let event;
  try {
    event = JSON.parse(rawBody.toString());
  } catch (e) {
    return res.status(400).json({ error: 'Invalid JSON body' });
  }

  const eventType = event.event;
  const data      = event.data || {};

  console.log('[webhook.js] Received event:', eventType, '| ref:', data.reference);

  /* ── 3. HANDLE PAYMENT SUCCESS EVENTS ───────────────────────────────── */
  const isPaymentSuccess =
    eventType === 'charge.success' ||
    eventType === 'dedicated_virtual_account.success';

  if (!isPaymentSuccess) {
    /* Acknowledge other events immediately — Paystack expects 200 */
    return res.status(200).json({ received: true });
  }

  const reference = data.reference || '';
  if (!reference) {
    console.error('[webhook.js] No reference in payload');
    return res.status(400).json({ error: 'No payment reference' });
  }

  try {
    /* ── 4. FIND THE ORDER by reference OR by our tracking ID (NYO-XXXX) ──
       We store the Paystack reference as `ref` and our tracking ID as `id`.
       The order may have been inserted before this webhook fires (race-safe).
    ──────────────────────────────────────────────────────────────────────── */
    const orders = await sql`
      SELECT * FROM orders
      WHERE ref = ${reference} OR id = ${reference}
      LIMIT 1
    `;

    if (!orders.length) {
      /* ── Order not found — buyer closed browser before callback fired.
         Create the order now using Paystack transaction data as source of truth. ── */
      console.warn('[webhook.js] Order not found for ref:', reference, '— creating from webhook data.');

      try {
        const metadata     = data.metadata || {};
        const customFields = metadata.custom_fields || [];

        const getField = function(varName) {
          const f = customFields.find(function(f){ return f.variable_name === varName; });
          return f ? f.value : '';
        };

        /* Extract order ID (trackId = NYO-XXXX) stored in metadata at checkout */
        const trackingField = customFields.find(function(f){
          return f.variable_name === 'tracking' || f.display_name === 'Tracking ID';
        });
        const orderId = metadata.orderId || (trackingField && trackingField.value) || reference;

        const customerName  = getField('customer') || 'Customer';
        const customerWA    = getField('whatsapp')  || '';
        const customerAddr  = getField('address')   || '';
        const customerEmail = (data.customer && data.customer.email) ? data.customer.email : '';
        const paidAmount    = data.amount ? data.amount / 100 : 0;

        const customer = { name: customerName, email: customerEmail, whatsapp: customerWA, address: customerAddr };

        /* Determine mode from address */
        /* ── Look up saved cart from pending_carts table ── */
        let savedItems     = [];
        let savedSellerId  = '';
        let savedMode      = orderMode;
        let savedTotal     = paidAmount;

        try {
          const cartRows = await sql`
            SELECT items, seller_id, mode, total, user_id
            FROM pending_carts
            WHERE order_id = ${String(orderId)}
            LIMIT 1
          `;
          if (cartRows.length) {
            const cartRow  = cartRows[0];
            savedItems     = typeof cartRow.items === 'string' ? JSON.parse(cartRow.items) : (cartRow.items || []);
            savedSellerId  = cartRow.seller_id || '';
            savedMode      = cartRow.mode      || orderMode;
            savedTotal     = parseFloat(cartRow.total || paidAmount);
            if (!userId && cartRow.user_id) userId = cartRow.user_id;
            console.log('[webhook.js] cart restored —', savedItems.length, 'items for order', orderId);
          }
        } catch(cartErr) {
          console.warn('[webhook.js] pending_carts lookup failed (non-fatal):', cartErr.message);
        }

        const finalItems    = savedItems.length > 0 ? savedItems : [];
        const finalMode     = savedMode;
        const finalIsPhys   = finalMode === 'physical' || (customerAddr && customerAddr !== 'Digital Delivery');
        const finalStatus   = finalIsPhys ? 'escrow_held' : 'paid';
        const finalSellerId = savedSellerId || null;
        const finalSellerInt = finalSellerId ? parseInt(finalSellerId) : null;

        /* Look up buyer userId by email */
        let userId = '';
        if (customerEmail) {
          try {
            const uRows = await sql`SELECT id FROM users WHERE LOWER(email) = LOWER(${customerEmail}) LIMIT 1`;
            if (uRows.length) userId = String(uRows[0].id);
          } catch(e) {}
        }

        const platformRate = finalIsPhys ? 0.05 : 0.10;
        const platformFee  = Math.round(paidAmount * platformRate);
        const sellerPayout = Math.round(paidAmount * (1 - platformRate));

        await sql`
          INSERT INTO orders (
            id, user_id, customer, items, total, amount,
            platform_fee, seller_payout, affiliate_fee,
            seller_id, status, collected, mode, ref,
            shipping, delivery_code, date, created_at
          ) VALUES (
            ${String(orderId)},
            ${String(userId)},
            ${JSON.stringify(customer)},
            ${JSON.stringify(finalItems)},
            ${paidAmount},
            ${paidAmount},
            ${platformFee},
            ${sellerPayout},
            ${0},
            ${finalSellerInt},
            ${finalStatus},
            ${false},
            ${finalMode},
            ${reference},
            ${finalIsPhys ? JSON.stringify({ address: customerAddr, whatsapp: customerWA }) : null},
            ${deliveryCode},
            ${new Date().toLocaleDateString()},
            NOW()
          )
          ON CONFLICT (id) DO UPDATE SET
            status        = EXCLUDED.status,
            ref           = EXCLUDED.ref,
            items         = CASE WHEN EXCLUDED.items::text != '[]' THEN EXCLUDED.items ELSE orders.items END,
            delivery_code = EXCLUDED.delivery_code,
            amount        = COALESCE(EXCLUDED.amount, orders.amount)
        `;

        /* Clean up pending cart */
        try {
          await sql`DELETE FROM pending_carts WHERE order_id = ${String(orderId)}`;
        } catch(e) {}

        console.log('[webhook.js] ✅ Order CREATED from webhook —', orderId, '₦' + paidAmount, 'items:', finalItems.length, 'status:', finalStatus);
        return res.status(200).json({ received: true, orderId, created: true });

      } catch (createErr) {
        console.error('[webhook.js] Failed to create order from webhook:', createErr.message);
        return res.status(200).json({ received: true, error: 'Create failed — logged' });
      }
    }

    const order = orders[0];

    /* ── 5. SKIP IF ALREADY PROCESSED ── */
    if (order.status === 'escrow_held' || order.status === 'completed') {
      console.log('[webhook.js] Order already processed:', order.id, '| status:', order.status);
      return res.status(200).json({ received: true, note: 'Already processed' });
    }

    /* ── 6. LOOK UP DIGITAL FILE URLS FROM PRODUCTS TABLE ───────────────
       Parse the items JSON from the order, find each product in the DB,
       and extract file_url / file_name for digital/course products.
       This is the critical link — it makes the download available to the buyer.
    ──────────────────────────────────────────────────────────────────────── */
    let items = order.items;
    if (typeof items === 'string') {
      try { items = JSON.parse(items); } catch (e) { items = []; }
    }
    if (!Array.isArray(items)) items = [];

    /* Build list of product IDs from order items */
    const productIds = items
      .map(function(i) { return Number(i.id); })   // ← Number() cast — Neon int safety
      .filter(function(id) { return !isNaN(id) && id > 0; });

    let fileUrl      = null;
    let fileName     = null;
    let updatedItems = items;

    if (productIds.length > 0) {
      /* Fetch all products in one query */
      const products = await sql`
        SELECT id, name, type, file_url, file_name, file_ext, product_link, product_link_expiry
        FROM products
        WHERE id = ANY(${productIds})
      `;

      /* Map product data back onto order items */
      updatedItems = items.map(function(item) {
        const prod = products.find(function(p) {
          return Number(p.id) === Number(item.id);   // ← Number() cast both sides
        });
        if (prod && (prod.type === 'digital' || prod.type === 'course')) {
          /* Attach file info directly to the item for the buyer's download */
          return Object.assign({}, item, {
            fileUrl:       prod.file_url           || item.fileUrl       || null,
            fileName:      prod.file_name          || item.fileName      || null,
            fileExt:       prod.file_ext           || item.fileExt       || null,
            productLink:   prod.product_link       || item.productLink   || null,
            productLinkExpiry: prod.product_link_expiry || item.productLinkExpiry || null,
          });
        }
        return item;
      });

      /* For single-product orders, also save top-level file_url on the order */
      const firstDigital = products.find(function(p) {
        return p.type === 'digital' || p.type === 'course';
      });
      if (firstDigital) {
        fileUrl  = firstDigital.file_url  || null;
        fileName = firstDigital.file_name || null;
      }
    }

    /* ── 7. UPDATE ORDER: status → escrow_held, inject file_url ─────────
       Also store the paid amount from Paystack for audit purposes.
    ──────────────────────────────────────────────────────────────────────── */
    const paidAmount = data.amount ? data.amount / 100 : null; /* kobo → naira */

    await sql`
      UPDATE orders
      SET
        status   = 'escrow_held',
        collected = false,
        ref       = ${reference},
        items     = ${JSON.stringify(updatedItems)},
        file_url  = COALESCE(${fileUrl}, file_url)
      WHERE id = ${order.id}
    `;

    console.log('[webhook.js] ✅ Order', order.id, 'updated to escrow_held | file_url:', fileUrl);

    /* ── 8. CREDIT AFFILIATE IF APPLICABLE ──────────────────────────────
       If the order has an aff_code, find the affiliate user and add
       the affiliate fee to their seller_balance.
    ──────────────────────────────────────────────────────────────────────── */
    const affCode = order.aff_code || null;
    if (affCode) {
      try {
        const affiliateUsers = await sql`
          SELECT id, seller_balance FROM users WHERE aff_code = ${affCode} LIMIT 1
        `;
        if (affiliateUsers.length) {
          const affFee = parseFloat(order.affiliate_fee || 0);
          if (affFee > 0) {
            await sql`
              UPDATE users
              SET seller_balance = COALESCE(seller_balance, 0) + ${affFee}
              WHERE id = ${String(affiliateUsers[0].id)}
            `;
            console.log('[webhook.js] Affiliate', affCode, 'credited ₦', affFee);
          }
        }
      } catch (affErr) {
        /* Non-fatal — log and continue */
        console.error('[webhook.js] Affiliate credit error:', affErr.message);
      }
    }

    /* ── 9. RESPOND 200 — Paystack will retry on any non-2xx ── */
    return res.status(200).json({
      received:  true,
      orderId:   order.id,
      status:    'escrow_held',
      fileLinked: !!fileUrl
    });

  } catch (err) {
    console.error('[webhook.js] DB error:', err.message);
    /* Return 200 anyway to prevent Paystack from hammering retries.
       The error is logged — investigate via Vercel logs. */
    return res.status(200).json({ received: true, error: 'Internal error — logged' });
  }
};

/* ─────────────────────────────────────────────────────────────────────────
   HELPER: getRawBody
   Vercel serverless functions do not expose the raw body by default.
   We must stream req manually so the HMAC check uses the exact raw bytes
   Paystack signed — JSON.stringify(req.body) would differ on key order.

   In vercel.json, set: { "api": { "bodyParser": false } }
   OR add this at the top of the file to disable per-route:
     export const config = { api: { bodyParser: false } };
   (Both shown below — use whichever matches your setup.)
─────────────────────────────────────────────────────────────────────────── */
function getRawBody(req) {
  return new Promise(function(resolve, reject) {
    /* If Vercel already parsed the body (bodyParser: true), reconstruct it */
    if (req.body) {
      try {
        const str = typeof req.body === 'string' ? req.body : JSON.stringify(req.body);
        return resolve(Buffer.from(str));
      } catch (e) {
        return reject(e);
      }
    }
    /* Stream the raw bytes */
    const chunks = [];
    req.on('data',  function(chunk) { chunks.push(chunk); });
    req.on('end',   function()      { resolve(Buffer.concat(chunks)); });
    req.on('error', function(err)   { reject(err); });
  });
}

/* ── Disable Vercel's body parser so we get the raw bytes for HMAC ── */
module.exports.config = {
  api: { bodyParser: false }
};
