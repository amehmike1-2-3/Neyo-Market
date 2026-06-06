// ╔══════════════════════════════════════════════════════════════════════╗
// ║  /api/paystack.js — NeyoMarket Paystack API Handler                 ║
// ║  Handles: KYC, withdrawals, payouts, DVC release, refunds           ║
// ║  Production-hardened — all errors return JSON, never HTML           ║
// ╚══════════════════════════════════════════════════════════════════════╝
'use strict';

// ─── Dependencies ────────────────────────────────────────────────────────────
const { neon } = require('@neondatabase/serverless');

// ─── Environment ─────────────────────────────────────────────────────────────
const sql = neon(process.env.DATABASE_URL);
const PSK = process.env.PAYSTACK_SECRET_KEY;

// ─── Commission Tiers ────────────────────────────────────────────────────────
// digital rate / physical rate per membership tier
const TIER_RATES = {
  free:     { digital: 0.10, physical: 0.05 },
  starter:  { digital: 0.08, physical: 0.04 },
  pro:      { digital: 0.06, physical: 0.03 },
  business: { digital: 0.04, physical: 0.02 },
};

const AFFILIATE_RATE    = 0.05;   // 5% affiliate cut when aff_code present
const AFFILIATE_FEE_ADJ = 0.02;   // platform rate reduced by 2% if affiliate present
const WITHDRAWAL_MIN    = 2000;   // ₦2,000 minimum withdrawal
const PAYOUT_FLAT_FEE   = 100;    // ₦100 platform fee on each approved payout
const LOYALTY_PTS_SALE  = 20;     // loyalty points awarded per completed sale

// ─── Logger ──────────────────────────────────────────────────────────────────
const log = {
  info:  (...a) => console.log ('[NeyoMarket][paystack]', ...a),
  warn:  (...a) => console.warn('[NeyoMarket][paystack]', ...a),
  error: (...a) => console.error('[NeyoMarket][paystack]', ...a),
};

// ─── CORS Headers ────────────────────────────────────────────────────────────
function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin',  '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.setHeader('X-Content-Type-Options',       'nosniff'); // prevent MIME sniff to HTML
}

// ─── Uniform JSON Error Response ─────────────────────────────────────────────
function jsonErr(res, status, msg, detail) {
  return res.status(status).json({
    ok:    false,
    error: msg,
    ...(detail ? { detail } : {}),
  });
}

// ─── Paystack API Helper ──────────────────────────────────────────────────────
// All Paystack calls go through here.
// Accepts an optional override key (used by approve-payout master key flow).
async function callPaystack(path, method = 'GET', body = null, keyOverride = null) {
  const key = keyOverride || PSK;
  try {
    const opts = {
      method,
      headers: {
        Authorization:  'Bearer ' + key,
        'Content-Type': 'application/json',
      },
    };
    if (body) opts.body = JSON.stringify(body);

    const r    = await fetch('https://api.paystack.co' + path, opts);
    const text = await r.text();

    if (!text || text.trim() === '') {
      return { status: false, message: 'Empty response from Paystack' };
    }
    try {
      return JSON.parse(text);
    } catch (_) {
      return { status: false, message: 'Invalid JSON from Paystack: ' + text.substring(0, 120) };
    }
  } catch (err) {
    return { status: false, message: 'Paystack unreachable: ' + err.message };
  }
}

// ─── Input Validators ─────────────────────────────────────────────────────────
function requireFields(obj, fields) {
  // Returns the name of the first missing/falsy field, or null if all present
  return fields.find(f => !obj[f]) || null;
}

function isValidPaystackKey(key) {
  return typeof key === 'string' &&
    (key.startsWith('sk_live_') || key.startsWith('sk_test_'));
}

// ─── Shared: Create Paystack Transfer Recipient ───────────────────────────────
async function createRecipient(user, key) {
  return callPaystack('/transferrecipient', 'POST', {
    type:           'nuban',
    name:           user.payout_aname || user.name,
    account_number: user.payout_acct,
    bank_code:      user.payout_bank,
    currency:       'NGN',
  }, key);
}

// ─── Shared: Execute Paystack Transfer ───────────────────────────────────────
async function executeTransfer(recipientCode, amountNaira, reason, key) {
  return callPaystack('/transfer', 'POST', {
    source:    'balance',
    amount:    Math.floor(amountNaira * 100), // naira → kobo
    recipient: recipientCode,
    reason,
  }, key);
}

// ══════════════════════════════════════════════════════════════════════════════
// MAIN HANDLER
// ══════════════════════════════════════════════════════════════════════════════
module.exports = async function handler(req, res) {
  setCors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action;
  log.info(`→ ${req.method} ?action=${action}`);

  try {

    // ──────────────────────────────────────────────────────────────────────────
    // RESOLVE ACCOUNT
    // GET ?action=resolve-account&accountNumber=...&bankCode=...
    // Looks up account name from Paystack given bank code + account number.
    // ──────────────────────────────────────────────────────────────────────────
    if (action === 'resolve-account') {
      const { accountNumber, bankCode } = req.query;

      if (!accountNumber || !bankCode)
        return jsonErr(res, 400, 'accountNumber and bankCode are required.');
      if (!PSK)
        return res.status(200).json({ error: 'Paystack key not configured — enter name manually.' });

      const result = await callPaystack(
        `/bank/resolve?account_number=${accountNumber}&bank_code=${bankCode}`
      );

      if (result.status && result.data)
        return res.status(200).json({ accountName: result.data.account_name });

      return res.status(200).json({ error: result.message || 'Account not found.' });
    }

    // ──────────────────────────────────────────────────────────────────────────
    // KYC — Validate NIN / BVN via Paystack customer validation
    // POST ?action=kyc  { userId, kycType, kycNumber }
    // ──────────────────────────────────────────────────────────────────────────
    if (action === 'kyc') {
      if (req.method !== 'POST') return jsonErr(res, 405, 'POST only.');

      const { userId, kycType, kycNumber } = req.body || {};
      const missing = requireFields({ userId, kycType, kycNumber }, ['userId', 'kycType', 'kycNumber']);
      if (missing) return jsonErr(res, 400, `${missing} is required.`);

      if (String(kycNumber).length < 10)
        return jsonErr(res, 400, `Invalid ${kycType.toUpperCase()} — must be at least 10 digits.`);
      if (!PSK)
        return jsonErr(res, 500, 'Paystack key not configured. Contact admin.');

      const users = await sql`SELECT * FROM users WHERE id = ${userId} LIMIT 1`;
      if (!users.length) return jsonErr(res, 404, 'User not found.');
      const user = users[0];

      // Submit to Paystack customer validation endpoint
      const paystackRes = await callPaystack('/customer/validate', 'POST', {
        email:      user.email,
        first_name: user.name.split(' ')[0],
        last_name:  user.name.split(' ').slice(1).join(' ') || user.name,
        type:       kycType,
        value:      kycNumber,
        country:    'NG',
      });

      // Service warming up or empty response — treat as pending
      if (!paystackRes || paystackRes.message === 'Empty response from Paystack') {
        return res.status(200).json({
          ok:        true,
          kycStatus: 'pending',
          message:   'Verification service warming up — please retry in a moment.',
        });
      }

      // Determine final KYC status from Paystack response
      let kycStatus = 'pending';
      if (paystackRes.status === true) {
        kycStatus = 'verified';
      } else if ((paystackRes.data && paystackRes.data.identification && paystackRes.data.identification.status) === 'success') {
        kycStatus = 'verified';
      } else if ((paystackRes.message && paystackRes.message.toLowerCase().includes('success'))) {
        kycStatus = 'verified';
      }

      await sql`
        UPDATE users
        SET kyc_status = ${kycStatus},
            kyc_type   = ${kycType},
            kyc_number = ${kycNumber}
        WHERE id = ${userId}
      `;

      log.info(`KYC ${kycStatus} for user ${userId} (${kycType})`);

      return res.status(200).json({
        ok:        true,
        kycStatus,
        message:   kycStatus === 'verified'
          ? 'Identity verified! You can now list products.'
          : 'Submitted! Under review — usually takes a few minutes.',
      });
    }

    // ──────────────────────────────────────────────────────────────────────────
    // SELLER BALANCE
    // GET ?action=balance&userId=...
    // Returns seller's Paystack subaccount balance.
    // ──────────────────────────────────────────────────────────────────────────
    if (action === 'balance') {
      const { userId } = req.query;
      if (!userId) return jsonErr(res, 400, 'userId is required.');

      const users = await sql`SELECT * FROM users WHERE id = ${userId} LIMIT 1`;
      if (!users.length) return jsonErr(res, 404, 'User not found.');
      const user = users[0];

      if (!user.subaccount_code)
        return res.status(200).json({ balance: 0, message: 'No subaccount yet.' });

      const result  = await callPaystack('/subaccount/' + user.subaccount_code);
      const balance = (result.data && result.data.account_balance)
        ? result.data.account_balance / 100
        : 0;

      return res.status(200).json({ balance });
    }

    // ──────────────────────────────────────────────────────────────────────────
    // REQUEST WITHDRAWAL — seller requests, admin approves later
    // POST ?action=request-withdraw  { userId, amount }
    // ──────────────────────────────────────────────────────────────────────────
    if (action === 'request-withdraw') {
      if (req.method !== 'POST') return jsonErr(res, 405, 'POST only.');

      const { userId, amount } = req.body || {};
      if (!userId || !amount) return jsonErr(res, 400, 'userId and amount are required.');

      const amountNum = parseFloat(amount);
      if (isNaN(amountNum) || amountNum < WITHDRAWAL_MIN)
        return jsonErr(res, 400, `Minimum withdrawal is ₦${WITHDRAWAL_MIN.toLocaleString()}.`);

      const users = await sql`SELECT * FROM users WHERE id = ${userId} LIMIT 1`;
      if (!users.length) return jsonErr(res, 404, 'User not found.');
      const user = users[0];

      if (user.kyc_status !== 'verified')
        return jsonErr(res, 403, 'KYC verification required before withdrawals.');
      if (!user.payout_acct || !user.payout_bank)
        return jsonErr(res, 400, 'Add bank details in Payout Settings first.');

      const bal = parseFloat(user.seller_balance || 0);
      if (amountNum > bal)
        return jsonErr(res, 400, `Amount exceeds available balance of ₦${bal.toLocaleString()}.`);

      await sql`
        INSERT INTO withdrawals (user_id, amount, status, reference, created_at)
        VALUES (${userId}, ${amountNum}, 'pending', '', NOW())
      `;

      log.info(`Withdrawal request submitted — user ${userId} ₦${amountNum}`);
      return res.status(201).json({ ok: true, message: 'Withdrawal request submitted! Admin will process it shortly.' });
    }

    // ──────────────────────────────────────────────────────────────────────────
    // APPROVE PAYOUT — admin approves a single pending withdrawal
    // POST ?action=approve-payout
    // { withdrawalId, userId, amount, flatFee?, netAmount?, masterKey? }
    // Flow: create recipient → transfer → deduct balance → mark success
    // ──────────────────────────────────────────────────────────────────────────
    if (action === 'approve-payout') {
      if (req.method !== 'POST') return jsonErr(res, 405, 'POST only.');

      const { withdrawalId, userId, amount, flatFee, netAmount, masterKey } = req.body || {};
      if (!withdrawalId || !userId || !amount)
        return jsonErr(res, 400, 'withdrawalId, userId and amount are required.');

      // Resolve which Paystack key to use (master key injection or env fallback)
      const activeKey = isValidPaystackKey(masterKey) ? masterKey : PSK;
      if (!activeKey)
        return jsonErr(res, 403, 'Master Key required. Inject your Paystack secret key first.');

      const users = await sql`SELECT * FROM users WHERE id = ${String(userId)} LIMIT 1`;
      if (!users.length) return jsonErr(res, 404, 'User not found.');
      const user = users[0];

      if (!user.payout_acct || !user.payout_bank)
        return jsonErr(res, 400, 'Seller has no bank details saved.');

      const grossAmt = parseFloat(amount);
      const fee      = parseFloat(flatFee || PAYOUT_FLAT_FEE);
      const paidOut  = parseFloat(netAmount || (grossAmt - fee));
      const bal      = parseFloat(user.seller_balance || 0);

      if (grossAmt > bal)
        return jsonErr(res, 400, `Seller balance (₦${bal.toLocaleString()}) is less than requested (₦${grossAmt.toLocaleString()}).`);

      // Step 1: Create Paystack transfer recipient
      const recipRes = await createRecipient(user, activeKey);
      if (!recipRes.status)
        return jsonErr(res, 400, 'Could not create recipient: ' + (recipRes.message || 'Check bank details'));

      // Step 2: Execute transfer for NET amount (gross minus flat fee)
      const transferRes = await executeTransfer(
        recipRes.data.recipient_code,
        paidOut,
        `NeyoMarket seller payout — ${user.name}`,
        activeKey
      );
      if (!transferRes.status)
        return jsonErr(res, 400, 'Transfer failed: ' + (transferRes.message || 'Try again'));

      const transferRef = (transferRes.data && transferRes.data.reference) || ('MAN-' + Date.now());

      // Step 3: Deduct GROSS from seller_balance (fee is retained by platform)
      await sql`
        UPDATE users
        SET seller_balance = COALESCE(seller_balance, 0) - ${grossAmt}
        WHERE id = ${String(userId)}
      `;

      // Step 4: Mark withdrawal as success
      await sql`
        UPDATE withdrawals
        SET status    = 'success',
            reference = ${transferRef}
        WHERE id = ${String(withdrawalId)}
      `;

      log.info(`Payout approved — user ${userId} ₦${paidOut} (fee ₦${fee}) ref ${transferRef}`);

      return res.status(200).json({
        ok:        true,
        reference: transferRef,
        grossAmt,
        fee,
        netPaid:   paidOut,
        message:   `₦${paidOut.toLocaleString()} sent to ${user.payout_aname || user.name} (₦${fee} platform fee retained)`,
      });
    }

    // ──────────────────────────────────────────────────────────────────────────
    // BULK PAYOUT — admin approves ALL pending withdrawals in one call
    // POST ?action=bulk-payout  { masterKey? }
    // Processes each pending withdrawal independently — failures don't block others.
    // Returns a detailed per-item results report.
    // ──────────────────────────────────────────────────────────────────────────
    if (action === 'bulk-payout') {
      if (req.method !== 'POST') return jsonErr(res, 405, 'POST only.');

      const { masterKey } = req.body || {};
      const activeKey = isValidPaystackKey(masterKey) ? masterKey : PSK;
      if (!activeKey)
        return jsonErr(res, 403, 'Master Key required. Inject your Paystack secret key first.');

      // Fetch all pending withdrawals with user bank details
      const pending = await sql`
        SELECT w.id AS withdrawal_id, w.user_id, w.amount,
               u.name, u.payout_aname, u.payout_acct, u.payout_bank,
               u.seller_balance, u.kyc_status
        FROM   withdrawals w
        JOIN   users u ON u.id = w.user_id
        WHERE  w.status = 'pending'
        ORDER  BY w.created_at ASC
        LIMIT  50
      `;

      if (!pending.length)
        return res.status(200).json({ ok: true, message: 'No pending withdrawals.', results: [] });

      log.info(`Bulk payout started — ${pending.length} pending withdrawals`);

      const results = [];
      let successCount = 0;
      let failCount    = 0;

      for (const row of pending) {
        const wId      = row.withdrawal_id;
        const userId   = row.user_id;
        const grossAmt = parseFloat(row.amount);
        const fee      = PAYOUT_FLAT_FEE;
        const paidOut  = grossAmt - fee;
        const bal      = parseFloat(row.seller_balance || 0);

        // Per-item validations — skip rather than abort entire batch
        if (row.kyc_status !== 'verified') {
          results.push({ withdrawalId: wId, userId, status: 'skipped', reason: 'KYC not verified' });
          failCount++;
          continue;
        }
        if (!row.payout_acct || !row.payout_bank) {
          results.push({ withdrawalId: wId, userId, status: 'skipped', reason: 'No bank details' });
          failCount++;
          continue;
        }
        if (grossAmt > bal) {
          results.push({ withdrawalId: wId, userId, status: 'skipped', reason: `Insufficient balance (₦${bal})` });
          failCount++;
          continue;
        }
        if (paidOut <= 0) {
          results.push({ withdrawalId: wId, userId, status: 'skipped', reason: 'Net payout ≤ 0 after fee' });
          failCount++;
          continue;
        }

        try {
          // Create recipient
          const recipRes = await createRecipient(row, activeKey);
          if (!recipRes.status) {
            results.push({ withdrawalId: wId, userId, status: 'failed', reason: recipRes.message || 'Recipient creation failed' });
            failCount++;
            continue;
          }

          // Execute transfer
          const transferRes = await executeTransfer(
            recipRes.data.recipient_code,
            paidOut,
            `NeyoMarket bulk payout — ${row.name}`,
            activeKey
          );
          if (!transferRes.status) {
            results.push({ withdrawalId: wId, userId, status: 'failed', reason: transferRes.message || 'Transfer failed' });
            failCount++;
            continue;
          }

          const transferRef = (transferRes.data && transferRes.data.reference) || ('BULK-' + Date.now() + '-' + wId);

          // Deduct from seller balance
          await sql`
            UPDATE users
            SET seller_balance = COALESCE(seller_balance, 0) - ${grossAmt}
            WHERE id = ${String(userId)}
          `;

          // Mark withdrawal success
          await sql`
            UPDATE withdrawals
            SET status    = 'success',
                reference = ${transferRef}
            WHERE id = ${String(wId)}
          `;

          log.info(`Bulk payout OK — withdrawal ${wId} user ${userId} ₦${paidOut} ref ${transferRef}`);
          results.push({
            withdrawalId: wId,
            userId,
            status:    'success',
            grossAmt,
            fee,
            netPaid:   paidOut,
            reference: transferRef,
          });
          successCount++;

        } catch (itemErr) {
          log.error(`Bulk payout item error — withdrawal ${wId}:`, itemErr.message);
          results.push({ withdrawalId: wId, userId, status: 'error', reason: itemErr.message });
          failCount++;
        }
      }

      log.info(`Bulk payout complete — ${successCount} success, ${failCount} failed/skipped`);

      return res.status(200).json({
        ok:           true,
        total:        pending.length,
        successCount,
        failCount,
        message:      `Bulk payout complete: ${successCount} succeeded, ${failCount} failed/skipped.`,
        results,
      });
    }

    // ──────────────────────────────────────────────────────────────────────────
    // WITHDRAW — admin-triggered immediate payout (legacy / direct flow)
    // POST ?action=withdraw  { userId, amount }
    // ──────────────────────────────────────────────────────────────────────────
    if (action === 'withdraw') {
      if (req.method !== 'POST') return jsonErr(res, 405, 'POST only.');

      const { userId, amount } = req.body || {};
      if (!userId || !amount) return jsonErr(res, 400, 'userId and amount are required.');

      const amountNum = parseFloat(amount);
      if (isNaN(amountNum) || amountNum < WITHDRAWAL_MIN)
        return jsonErr(res, 400, `Minimum withdrawal is ₦${WITHDRAWAL_MIN.toLocaleString()}.`);

      const users = await sql`SELECT * FROM users WHERE id = ${userId} LIMIT 1`;
      if (!users.length) return jsonErr(res, 404, 'User not found.');
      const user = users[0];

      if (user.kyc_status !== 'verified')
        return jsonErr(res, 403, 'Complete KYC verification first.');
      if (!user.payout_acct || !user.payout_bank)
        return jsonErr(res, 400, 'Add your bank details in Payout Settings first.');

      const recipientRes = await createRecipient(user);
      if (!recipientRes.status)
        return jsonErr(res, 400, 'Could not create recipient: ' + (recipientRes.message || 'Try again'));

      const transferRes = await executeTransfer(
        recipientRes.data.recipient_code,
        amountNum,
        'NeyoMarket seller payout'
      );
      if (!transferRes.status)
        return jsonErr(res, 400, 'Transfer failed: ' + (transferRes.message || 'Try again'));

      await sql`
        INSERT INTO withdrawals (user_id, amount, status, reference, created_at)
        VALUES (${userId}, ${amountNum}, 'pending', ${(transferRes.data && transferRes.data.reference) || ''}, NOW())
      `;

      log.info(`Direct withdraw — user ${userId} ₦${amountNum} ref ${(transferRes.data && transferRes.data.reference)}`);

      return res.status(200).json({
        ok:        true,
        reference: (transferRes.data && transferRes.data.reference),
        message:   `Payout of ₦${amountNum.toLocaleString()} initiated!`,
      });
    }

    // ──────────────────────────────────────────────────────────────────────────
    // WITHDRAWAL HISTORY
    // GET ?action=withdrawals&userId=...   (userId='all' returns full admin list)
    // ──────────────────────────────────────────────────────────────────────────
    if (action === 'withdrawals') {
      const { userId } = req.query;
      if (!userId) return jsonErr(res, 400, 'userId is required.');

      const rows = userId === 'all'
        ? await sql`SELECT * FROM withdrawals ORDER BY created_at DESC LIMIT 200`
        : await sql`SELECT * FROM withdrawals WHERE user_id = ${userId} ORDER BY created_at DESC LIMIT 50`;

      return res.status(200).json({ withdrawals: rows });
    }

    // ──────────────────────────────────────────────────────────────────────────
    // DVC RELEASE — seller enters 6-digit delivery code to release escrow
    // POST ?action=dvc-release  { orderId, dvcCode, sellerUserId? }
    // Flow: validate code → compute tiered split → credit seller → loyalty points
    //       → credit affiliate (if valid aff_code)
    // ──────────────────────────────────────────────────────────────────────────
    if (action === 'dvc-release') {
      if (req.method !== 'POST') return jsonErr(res, 405, 'POST only.');

      const { orderId, dvcCode, sellerUserId } = req.body || {};
      if (!orderId || !dvcCode) return jsonErr(res, 400, 'orderId and dvcCode are required.');

      // Load order
      const orders = await sql`SELECT * FROM orders WHERE id = ${String(orderId)} LIMIT 1`;
      if (!orders.length) return jsonErr(res, 404, 'Order not found.');
      const order = orders[0];

      // Idempotency guard — already released
      if (order.status === 'completed' || order.collected)
        return res.status(200).json({ ok: true, released: 0, message: 'Already completed.' });

      // ── Validate delivery code ──────────────────────────────────────────────
      const expectedCode = String(order.delivery_code || '').trim();
      const submittedCode = String(dvcCode).trim();

      let codeValid = false;
      if (expectedCode) {
        codeValid = submittedCode === expectedCode;
      } else {
        // Fallback: regenerate from orderId using same deterministic algo as frontend
        const str = String(orderId);
        let hash  = 0;
        for (let i = 0; i < str.length; i++) {
          hash = ((hash << 5) - hash) + str.charCodeAt(i);
          hash |= 0;
        }
        const generated = String(Math.abs(hash) % 900000 + 100000);
        codeValid = submittedCode === generated;
      }

      if (!codeValid)
        return jsonErr(res, 400, 'Incorrect delivery code. Ask the buyer to re-share it.');

      // ── Parse order items ───────────────────────────────────────────────────
      let items = order.items;
      if (typeof items === 'string') {
        try { items = JSON.parse(items); } catch (_) { items = []; }
      }
      if (!Array.isArray(items)) items = [];

      // ── Resolve seller membership tier ──────────────────────────────────────
      const dvcSellerId = sellerUserId || (items[0] && items[0].sellerId) || (items[0] && items[0].seller_id) || null;
      let   dvcTier     = 'free';

      if (dvcSellerId) {
        try {
          const tRows = await sql`
            SELECT membership_tier FROM users WHERE id = ${String(dvcSellerId)} LIMIT 1
          `;
          if (tRows.length) dvcTier = tRows[0].membership_tier || 'free';
        } catch (e) {
          log.warn('Could not fetch membership tier (non-fatal):', e.message);
        }
      }

      // ── Compute revenue split ───────────────────────────────────────────────
      const hasPhysical  = items.some(i => i.type === 'physical');
      const total        = parseFloat(order.total || 0);
      const rates        = TIER_RATES[dvcTier] || TIER_RATES.free;
      const baseRate     = hasPhysical ? rates.physical : rates.digital;
      const hasAff       = order.aff_code && String(order.aff_code).trim().length > 2;
      const platformRate = Math.max(0.01, baseRate - (hasAff ? AFFILIATE_FEE_ADJ : 0));
      const affRate      = hasAff ? AFFILIATE_RATE : 0;
      const sellerRate   = 1 - platformRate - affRate;

      const platformFee  = Math.round(total * platformRate);
      const affiliateFee = Math.round(total * affRate);
      const sellerPayout = Math.round(total * sellerRate);
      const collectedAt  = new Date().toISOString();

      // ── Mark order completed ────────────────────────────────────────────────
      await sql`
        UPDATE orders SET
          status        = 'completed',
          collected     = true,
          collected_at  = ${collectedAt},
          platform_fee  = ${platformFee},
          seller_payout = ${sellerPayout},
          affiliate_fee = ${affiliateFee}
        WHERE id = ${String(orderId)}
      `;

      // ── Credit seller balance ───────────────────────────────────────────────
      let resolvedSellerId = null;

      if (dvcSellerId) {
        await sql`
          UPDATE users
          SET seller_balance = COALESCE(seller_balance, 0) + ${sellerPayout}
          WHERE id = ${String(dvcSellerId)}
        `;
        resolvedSellerId = String(dvcSellerId);
        log.info(`DVC release — seller ${dvcSellerId} credited ₦${sellerPayout} (tier: ${dvcTier})`);
      }

      // ── Award seller loyalty points ─────────────────────────────────────────
      if (resolvedSellerId) {
        try {
          const sRows = await sql`
            SELECT loyalty_points, loyalty_history FROM users WHERE id = ${resolvedSellerId} LIMIT 1
          `;
          if (sRows.length) {
            const newPts = parseInt(sRows[0].loyalty_points || 0) + LOYALTY_PTS_SALE;
            const hist   = sRows[0].loyalty_history || [];
            hist.push({
              pts:   LOYALTY_PTS_SALE,
              label: `Sale confirmed: ${orderId}`,
              date:  new Date().toLocaleDateString(),
            });
            await sql`
              UPDATE users
              SET loyalty_points   = ${newPts},
                  loyalty_history  = ${JSON.stringify(hist)}::jsonb
              WHERE id = ${resolvedSellerId}
            `;
          }
        } catch (e) {
          log.warn('Loyalty points update failed (non-fatal):', e.message);
        }
      }

      // ── Credit affiliate — only if valid aff_code (FIX 4) ──────────────────
      const affCode = order.aff_code ? String(order.aff_code).trim() : '';
      if (affCode.length > 2 && affiliateFee > 0) {
        try {
          await sql`
            UPDATE users
            SET seller_balance = COALESCE(seller_balance, 0) + ${affiliateFee}
            WHERE aff_code = ${affCode}
          `;
          log.info(`Affiliate ${affCode} credited ₦${affiliateFee}`);
        } catch (affErr) {
          log.warn('Affiliate credit failed (non-fatal):', affErr.message);
        }
      }

      return res.status(200).json({
        ok:          true,
        released:    sellerPayout,
        platformFee,
        affiliateFee,
        message:     `✅ Delivery confirmed! ₦${sellerPayout.toLocaleString()} released to your wallet.`,
      });
    }

    // ──────────────────────────────────────────────────────────────────────────
    // REFUND — admin triggers Paystack refund for a disputed order
    // POST ?action=refund  { orderId, reference, amount? }
    // ──────────────────────────────────────────────────────────────────────────
    if (action === 'refund') {
      if (req.method !== 'POST') return jsonErr(res, 405, 'POST only.');

      const { orderId, reference, amount } = req.body || {};
      if (!orderId || !reference) return jsonErr(res, 400, 'orderId and reference are required.');

      const refundKobo = Math.floor(parseFloat(amount || 0) * 100); // naira → kobo

      const refundRes = await callPaystack('/refund', 'POST', {
        transaction:   reference,
        amount:        refundKobo,
        merchant_note: 'Buyer dispute resolved in buyer favour — NeyoMarket admin',
      });

      if (!refundRes.status)
        return jsonErr(res, 400, 'Refund failed: ' + (refundRes.message || 'Check Paystack dashboard'));

      await sql`
        UPDATE orders
        SET status    = 'refunded',
            collected = false
        WHERE id = ${String(orderId)}
      `;

      log.info(`Refund processed — order ${orderId} ₦${parseFloat(amount || 0)} ref ${reference}`);

      return res.status(200).json({
        ok:      true,
        message: `Refund of ₦${parseFloat(amount || 0).toLocaleString()} initiated successfully.`,
      });
    }

    // ── Unknown action ────────────────────────────────────────────────────────
    return jsonErr(res, 400, `Unknown action: ${action}`);

  } catch (err) {
    // Global catch — always returns JSON, never an HTML 500 page
    log.error('Unhandled error:', err.message || err);
    return jsonErr(res, 500, 'Internal server error.', err.message);
  }
};
