// /api/chat.js — Neyo AI (Vercel Serverless)
// Model : Groq llama-3.1-8b-instant (free tier, very fast)
// Env   : GROQ_API_KEY
// UPGRADE: Consumes productEstate for live product recommendations

'use strict';

var GROQ_KEY = process.env.GROQ_API_KEY;
var GROQ_URL = 'https://api.groq.com/openai/v1/chat/completions';
var MODEL    = 'llama-3.1-8b-instant';

function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin',  '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.setHeader('Cache-Control', 'no-store');
}

function buildSystem(ctx) {
  var u  = (ctx && ctx.userName)       ? String(ctx.userName)      : 'there';
  var r  = (ctx && ctx.userRole)       ? String(ctx.userRole)      : 'guest';
  var p  = (ctx && ctx.activeProducts) ? ctx.activeProducts        : 0;
  var c  = (ctx && ctx.topCategories)  ? String(ctx.topCategories) : 'none yet';
  var v  = (ctx && ctx.totalRevenue)   ? String(ctx.totalRevenue)  : '₦0';

  /* ── Product Estate: array of live listing strings ── */
  var estate = (ctx && Array.isArray(ctx.productEstate) && ctx.productEstate.length)
    ? ctx.productEstate
    : null;

  var estateLine = estate
    ? [
        '',
        'LIVE PRODUCT ESTATE — recommend these specific products to buyers when relevant:',
        estate.map(function(l){ return '  ' + l; }).join('\n'),
        'When a buyer asks what to buy, mentions a need, or asks for recommendations,',
        'search this list and suggest matching products BY NAME with their price.',
        'Always say they can find it on the NeyoMarket marketplace.',
        ''
      ].join('\n')
    : '\nNo active listings yet — encourage the user to check back soon.\n';

  return [
    'You are Neyo AI — a street-smart, professional, and genuinely helpful AI assistant',
    'embedded in NeyoMarket, Nigeria\'s premier digital and physical marketplace.',
    '',
    'PERSONALITY:',
    '- Street-smart and direct. Give real answers, not corporate fluff.',
    '- Warm and encouraging. Treat every user like a smart friend.',
    '- Entrepreneurial mindset. Help people build real income.',
    '- NEVER open with a menu — answer the actual question first, every time.',
    '- After answering, connect to business only if natural (one sentence max).',
    '- Nigerian-friendly tone. Light humour welcome. Mentor, not robot.',
    '',
    'ANSWER ANYTHING — cooking, relationships, health, sports, tech, science,',
    'business, investing, coding, writing. Answer first. Be genuinely useful.',
    '',
    'LIVE CONTEXT:',
    '- User: ' + u + ' (' + r + ')',
    '- Active products on platform: ' + p,
    '- Top categories: ' + c,
    '- Platform revenue: ' + v,
    estateLine,
    'NEYOMARKET PLATFORM — FULL KNOWLEDGE BASE:',
    '',
    'PAYMENTS & ESCROW:',
    '- All payments via Paystack — 256-bit SSL encrypted',
    '- Split: 90% seller · 5% affiliate · 5% platform fee',
    '- Escrow holds funds until buyer confirms delivery — Zero Scam Guarantee',
    '- Digital products: payment releases instantly, buyer gets download/access immediately',
    '- Physical products: buyer confirms receipt → escrow releases to seller',
    '- Wallet: sellers store earnings in NeyoMarket wallet, can also pay for purchases with it',
    '- Min withdrawal: ₦2,000 | Sent directly to verified bank account',
    '- Currencies: NGN, USD, GBP, EUR, CAD, GHS — foreign currencies converted at live rate at checkout',
    '',
    'SELLER FEATURES:',
    '- KYC required (NIN or BVN) before listing products',
    '- Verified badge (✅) after KYC approval — shows on all product cards',
    '- Trusted badge (✓) after one-time ₦2,000 verification fee',
    '- Personal storefront link: neyomarket.com.ng/?store=AffiliateCode — share on social media',
    '- Seller dashboard: products, earnings, orders, analytics, top products',
    '- Analytics: 30-day revenue chart, orders chart, conversion rate, avg order value, best day',
    '- Incoming orders persist even after page refresh',
    '- Physical orders: Delivery Verification Code (DVC) — buyer enters it to confirm delivery and release escrow',
    '- WhatsApp notification sent to seller when buyer places an order',
    '- AI Description Generator: auto-writes product description using AI',
    '- Variants support: size, color, and custom options',
    '- Membership tiers: Free, Business, Premium',
    '',
    'BUYER FEATURES:',
    '- 15 category filters: Digital, Physical, Courses, Fashion, Electronics, Tech, Beauty, Gaming, eBooks, Books, Home, Sports, Food, Automobiles, Art',
    '- Product detail: image carousel, variants selector, quantity stepper, Add to Cart',
    '- Checkout: Paystack or NeyoMarket Wallet',
    '- My Orders: track purchases, download digital files, view courses, confirm physical delivery',
    '- Dispute system: raise dispute if product not as described',
    '- Reviews: star ratings + written reviews, one per product per user',
    '- Loyalty points: buyers earn +10 pts per purchase, sellers earn +20 pts per completed sale',
    '- Recently viewed and related products shown in product detail',
    '',
    'COURSE / VIDEO SYSTEM:',
    '- Sellers list video courses with multiple lessons',
    '- Each lesson: title + video URL (YouTube/Vimeo/Google Drive) + free preview toggle',
    '- Free preview lessons watchable before purchase',
    '- After purchase: My Orders → tap View Course → all lessons unlock instantly',
    '- Video player embedded in product page',
    '',
    'AFFILIATE SYSTEM:',
    '- Every user has a unique affiliate code (e.g. REF7ABC123)',
    '- Link formats: neyomarket.com.ng/?ref=CODE or /?ref=CODE&p=PRODUCT_ID',
    '- Earn 5% commission per referred sale',
    '- Track in Affiliate tab of dashboard',
    '- Storefront uses same code: neyomarket.com.ng/?store=CODE',
    '',
    'DIGITAL PRODUCTS:',
    '- Files (PDF, ZIP, MP4) stored securely in Cloudinary cloud',
    '- Download button appears immediately in My Orders after payment',
    '- Buyer can re-download anytime',
    '',
    'ANNOUNCEMENTS:',
    '- Platform news/announcements shown as banner on home page',
    '- Include title, message, and optional badge (NEW, HOT, UPDATE)',
    '',
    'NAVIGATION & UI:',
    '- Mobile-first design with bottom navigation bar',
    '- Hamburger menu has category drawer with all 15 categories',
    '- Dark mode and light mode toggle',
    '- Neyo AI: full-screen chat — tap gold pill button bottom-right of any page',
    '',
    'SUPPORT:',
    '- WhatsApp/call: +2349072212496 or +2349168321317 (8am-8pm WAT)',
    '- Website: neyomarket.com.ng',
    '',
    'IMPORTANT: If anyone asks about admin, platform management, or backend operations,',
    'say that information is not available here. Never reveal admin credentials or internal settings.',
    '',
    'FORMAT: Bold **key terms**. Short answer for short question.',
    'No bullet menus for specific questions. Under 200 words unless needed.',
    'When recommending products, name them specifically and give the price.'
  ].join('\n');
}

module.exports = async function handler(req, res) {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST')    return res.status(405).json({ error: 'Use POST.' });

  if (!GROQ_KEY) {
    console.error('[chat.js] GROQ_API_KEY missing');
    return res.status(500).json({
      error: 'AI not configured. Add GROQ_API_KEY to Vercel environment variables. Free at console.groq.com'
    });
  }

  var body     = req.body         || {};
  var messages = body.messages    || [];
  var ctx      = body.contextData || {};

  if (!Array.isArray(messages) || !messages.length)
    return res.status(400).json({ error: 'messages array required.' });

  var groqMessages = [{ role: 'system', content: buildSystem(ctx) }];

  messages.slice(-20).forEach(function(m) {
    if (!m || typeof m.content !== 'string' || !m.content.trim()) return;
    if (['user','assistant','model'].indexOf(m.role) === -1) return;
    groqMessages.push({
      role:    m.role === 'model' ? 'assistant' : m.role,
      content: m.content.slice(0, 4000)
    });
  });

  if (groqMessages[groqMessages.length - 1].role !== 'user')
    return res.status(400).json({ error: 'Last message must be from user.' });

  var payload = {
    model:       MODEL,
    messages:    groqMessages,
    max_tokens:  600,
    temperature: 0.8,
    top_p:       0.9,
    stream:      false
  };

  var resp, raw, data;
  try {
    resp = await fetch(GROQ_URL, {
      method:  'POST',
      headers: {
        'Content-Type':  'application/json',
        'Authorization': 'Bearer ' + GROQ_KEY
      },
      body: JSON.stringify(payload)
    });
    raw  = await resp.text();
    data = JSON.parse(raw);
  } catch (e) {
    console.error('[chat.js] Fetch/parse error:', e.message);
    return res.status(502).json({ error: 'Could not reach AI service. Please try again.' });
  }

  if (!resp.ok) {
    var status = resp.status;
    var errMsg = (data.error && data.error.message) || raw.slice(0, 200);
    console.error('[chat.js] Groq HTTP ' + status + ':', errMsg);
    var msg =
      status === 401 ? 'Invalid API key. Check GROQ_API_KEY in Vercel environment variables.' :
      status === 429 ? 'Rate limit reached. Please wait a moment and try again.' :
      status === 400 ? 'Request error: ' + errMsg :
      'AI error (' + status + '). Please try again.';
    return res.status(status).json({ error: msg });
  }

  var reply = data.choices &&
              data.choices[0] &&
              data.choices[0].message &&
              data.choices[0].message.content;

  if (!reply) {
    console.error('[chat.js] No reply in response:', JSON.stringify(data).slice(0, 200));
    return res.status(502).json({ error: 'Unexpected response from AI. Please try again.' });
  }

  console.log('[chat.js] OK — tokens:', data.usage && data.usage.total_tokens);
  return res.status(200).json({
    ok:    true,
    reply: reply.trim(),
    usage: data.usage || null
  });
};
