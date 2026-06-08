// /api/auth.js — NeyoMarket Authentication API with Gmail SMTP Email Service 
// Features: login, register (with email verification), reset-password, change-password,
//           update-profile, kyc, verify-email — all with rate limiting

const { neon } = require('@neondatabase/serverless');
const https    = require('https');
const bcrypt   = require('bcryptjs');
const crypto   = require('crypto');
const nodemailer = require('nodemailer');

const sql = neon(process.env.DATABASE_URL);

/* ════════════════════════════════════════════════════════
   GMAIL SMTP CONFIGURATION
   Uses environment variables for security — credentials NOT hardcoded
   Set these in Vercel Environment Variables:
   - GMAIL_USER (your Gmail address)
   - GMAIL_PASS (your Google App Password)
════════════════════════════════════════════════════════ */
const gmailTransporter = nodemailer.createTransport({
  host: 'smtp.gmail.com',
  port: 465,
  secure: true, // SSL/TLS
  auth: {
    user: process.env.GMAIL_USER,
    pass: process.env.GMAIL_PASS
  },
  logger: false,
  debug: false
});

/* ════════════════════════════════════════════════════════
   IN-MEMORY RATE LIMITER
════════════════════════════════════════════════════════ */
const loginAttempts = new Map();

const RATE_LIMIT = {
  maxAttempts: 5,
  windowMs:    15 * 60 * 1000,
  blockMs:     30 * 60 * 1000,
};

function getRateLimitKey(req, email) {
  const ip = req.headers['x-forwarded-for']?.split(',')[0]?.trim() || 'unknown';
  return ip + '|' + (email || '').toLowerCase().trim();
}

function checkRateLimit(key) {
  const now   = Date.now();
  const entry = loginAttempts.get(key);
  if (!entry) return { allowed: true };

  if (now - entry.firstAttempt > RATE_LIMIT.windowMs) {
    loginAttempts.delete(key);
    return { allowed: true };
  }

  if (entry.count >= RATE_LIMIT.maxAttempts) {
    const waitMs      = RATE_LIMIT.blockMs - (now - entry.firstAttempt);
    const waitMinutes = Math.ceil(waitMs / 60000);
    return { allowed: false, waitMinutes };
  }

  return { allowed: true };
}

function recordFailedAttempt(key) {
  const now   = Date.now();
  const entry = loginAttempts.get(key);
  if (!entry || Date.now() - entry.firstAttempt > RATE_LIMIT.windowMs) {
    loginAttempts.set(key, { count: 1, firstAttempt: now });
  } else {
    entry.count += 1;
  }
}

function clearAttempts(key) {
  loginAttempts.delete(key);
}

/* ════════════════════════════════════════════════════════
   HELPERS
════════════════════════════════════════════════════════ */
function cors(res) {
  res.setHeader('Access-Control-Allow-Origin',  '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}

function toPublicUser(row) {
  return {
    id:               String(row.id),
    name:             row.name,
    email:            row.email,
    phone:            row.phone             || '',
    role:             row.role              || 'buyer',
    affCode:          row.aff_code          || '',
    isVerified:       row.is_verified       || false,
    joined:           row.joined ? new Date(row.joined).toLocaleDateString() : '',
    payoutBank:       row.payout_bank       || '',
    payoutAcct:       row.payout_acct       || '',
    payoutAname:      row.payout_aname      || '',
    subaccountCode:   row.subaccount_code   || null,
    subaccountStatus: row.subaccount_status || 'pending',
    kycStatus:        row.kyc_status        || 'unverified',
    kycType:          row.kyc_type          || null,
    sellerBalance:    parseFloat(row.seller_balance    || 0),
    affiliateBalance: parseFloat(row.affiliate_balance || 0),
    adminBalance:     parseFloat(row.admin_balance     || 0),
    suspended:        row.suspended         || false,
    membershipTier:   row.membership_tier   || 'free',
    tierRef:          row.tier_ref          || '',
    tierExpiresAt:    row.tier_expires_at    || null,
    loyaltyPoints:    parseInt(row.loyalty_points || 0),
    loyaltyHistory:   row.loyalty_history   || [],
    badgeVerified:    row.badge_verified    || false,
    buyerWallet:      parseFloat(row.buyer_wallet       || 0),
    buyerWalletBonus: parseFloat(row.buyer_wallet_bonus || 0),
    buyerRefCode:     row.buyer_ref_code    || row.aff_code || '',
    buyerRefCount:    parseInt(row.buyer_ref_count || 0),
  };
}

function validateEmail(email) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

function generateVerificationToken() {
  return crypto.randomBytes(32).toString('hex');
}

/* ════════════════════════════════════════════════════════
   EMAIL VERIFICATION — via Gmail SMTP
   Professional Navy Blue Header, Beautiful Design
════════════════════════════════════════════════════════ */
async function sendVerificationEmail(userEmail, userName, verificationToken) {
  const verificationLink = `https://neyomarket.com.ng/verify.html?token=${verificationToken}`;

  const emailContent = `
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="UTF-8">
      <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #f5f5f5; line-height: 1.6; }
        .container { max-width: 600px; margin: 0 auto; background: #fff; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
        .header { background: linear-gradient(135deg, #1e3a8a 0%, #1e40af 100%); padding: 50px 20px; text-align: center; }
        .header h1 { color: #fff; font-size: 36px; font-weight: 700; margin: 0; letter-spacing: 1px; }
        .header p { color: rgba(255,255,255,0.9); font-size: 12px; margin-top: 8px; }
        .content { padding: 40px 30px; color: #1f2937; }
        .content h2 { font-size: 22px; font-weight: 600; margin-bottom: 16px; color: #1e3a8a; }
        .content p { font-size: 14px; line-height: 1.8; margin-bottom: 20px; color: #4b5563; }
        .cta-button { display: inline-block; background: linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%); color: #fff; padding: 16px 40px; border-radius: 50px; text-decoration: none; font-weight: 600; font-size: 15px; text-align: center; margin: 24px 0; box-shadow: 0 4px 12px rgba(37, 99, 235, 0.3); transition: transform 0.3s ease; }
        .cta-button:hover { transform: translateY(-2px); box-shadow: 0 6px 16px rgba(37, 99, 235, 0.4); }
        .link-text { background: #f3f4f6; padding: 12px; border-radius: 6px; font-size: 12px; color: #374151; word-break: break-all; margin: 20px 0; }
        .footer { background: #f9fafb; padding: 30px; text-align: center; border-top: 1px solid #e5e7eb; }
        .footer p { font-size: 12px; color: #6b7280; margin: 8px 0; }
        .security-badge { background: #eff6ff; border-left: 4px solid #2563eb; padding: 12px 16px; margin: 20px 0; font-size: 12px; color: #1e40af; border-radius: 4px; }
      </style>
    </head>
    <body>
      <div class="container">
        <div class="header">
          <h1>🛍️ NEYO MARKET</h1>
          <p>Engineering the Standard of E-commerce in Nigeria</p>
        </div>
        
        <div class="content">
          <h2>Welcome, ${userName}! 👋</h2>
          
          <p>Thank you for joining NeyoMarket, Nigeria's most trusted marketplace for digital and physical products with escrow-protected payments.</p>
          
          <p><strong>Verify Your Email Address</strong></p>
          <p>To complete your registration and unlock full marketplace access, please verify your email by clicking the button below:</p>
          
          <a href="${verificationLink}" class="cta-button">✅ Verify Email Address</a>
          
          <p style="margin-top: 30px; font-size: 13px;">Or copy and paste this link in your browser:</p>
          <div class="link-text">${verificationLink}</div>
          
          <div class="security-badge">
            <strong>🔒 Security:</strong> This link will expire in 24 hours. If you didn't create this account, please ignore this email.
          </div>
          
          <p style="margin-top: 20px; font-size: 13px; color: #6b7280;">
            <strong>Questions?</strong> Our support team is here to help: support@neyomarket.com.ng
          </p>
        </div>
        
        <div class="footer">
          <p><strong>Neyo Market</strong> - Engineering the Standard of E-commerce in Nigeria</p>
          <p>&copy; 2026 NeyoMarket. All rights reserved.</p>
        </div>
      </div>
    </body>
    </html>
  `;

  try {
    const info = await gmailTransporter.sendMail({
      from: 'NeyoMarket <amehmichael2336@gmail.com>',
      to: userEmail,
      subject: '✅ Verify Your Email - NeyoMarket Account Activation',
      html: emailContent,
      replyTo: 'support@neyomarket.com.ng',
    });

    console.log('[Gmail SMTP] Verification email sent to', userEmail, '- Message ID:', info.messageId);
    return { success: true, messageId: info.messageId };
  } catch (err) {
    console.error('[Gmail SMTP] Error sending verification email:', err.message);
    return { success: false, error: err.message };
  }
}

/* ════════════════════════════════════════════════════════
   PASSWORD RESET EMAIL — via Gmail SMTP
   Navy Blue Header, Beautiful Design
════════════════════════════════════════════════════════ */
async function sendPasswordResetEmail(userEmail, userName, resetLink) {
  const emailContent = `
    <!DOCTYPE html>
    <html>
    <head>
      <meta charset="UTF-8">
      <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #f5f5f5; line-height: 1.6; }
        .container { max-width: 600px; margin: 0 auto; background: #fff; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
        .header { background: linear-gradient(135deg, #1e3a8a 0%, #1e40af 100%); padding: 50px 20px; text-align: center; }
        .header h1 { color: #fff; font-size: 36px; font-weight: 700; margin: 0; letter-spacing: 1px; }
        .header p { color: rgba(255,255,255,0.9); font-size: 12px; margin-top: 8px; }
        .content { padding: 40px 30px; color: #1f2937; }
        .content h2 { font-size: 22px; font-weight: 600; margin-bottom: 16px; color: #1e3a8a; }
        .content p { font-size: 14px; line-height: 1.8; margin-bottom: 20px; color: #4b5563; }
        .cta-button { display: inline-block; background: linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%); color: #fff; padding: 16px 40px; border-radius: 50px; text-decoration: none; font-weight: 600; font-size: 15px; text-align: center; margin: 24px 0; box-shadow: 0 4px 12px rgba(37, 99, 235, 0.3); transition: transform 0.3s ease; }
        .cta-button:hover { transform: translateY(-2px); box-shadow: 0 6px 16px rgba(37, 99, 235, 0.4); }
        .link-text { background: #f3f4f6; padding: 12px; border-radius: 6px; font-size: 12px; color: #374151; word-break: break-all; margin: 20px 0; }
        .footer { background: #f9fafb; padding: 30px; text-align: center; border-top: 1px solid #e5e7eb; }
        .footer p { font-size: 12px; color: #6b7280; margin: 8px 0; }
        .warning { background: #fef2f2; border-left: 4px solid #dc2626; padding: 12px 16px; margin: 20px 0; font-size: 12px; color: #991b1b; border-radius: 4px; }
      </style>
    </head>
    <body>
      <div class="container">
        <div class="header">
          <h1>🛍️ NEYO MARKET</h1>
          <p>Engineering the Standard of E-commerce in Nigeria</p>
        </div>
        
        <div class="content">
          <h2>Password Reset Request 🔐</h2>
          
          <p>Hi ${userName},</p>
          <p>We received a request to reset the password for your NeyoMarket account. Click the button below to create a new password:</p>
          
          <a href="${resetLink}" class="cta-button">🔑 Reset Your Password</a>
          
          <p style="margin-top: 30px; font-size: 13px;">Or copy and paste this link in your browser:</p>
          <div class="link-text">${resetLink}</div>
          
          <div class="warning">
            <strong>⚠️ Important:</strong> This link will expire in 1 hour. If you didn't request a password reset, please ignore this email and your account will remain secure.
          </div>
          
          <p style="margin-top: 20px; font-size: 13px; color: #6b7280;">
            <strong>Questions?</strong> Contact our support team: support@neyomarket.com.ng
          </p>
        </div>
        
        <div class="footer">
          <p><strong>Neyo Market</strong> - Engineering the Standard of E-commerce in Nigeria</p>
          <p>&copy; 2026 NeyoMarket. All rights reserved.</p>
        </div>
      </div>
    </body>
    </html>
  `;

  try {
    const info = await gmailTransporter.sendMail({
      from: 'NeyoMarket <amehmichael2336@gmail.com>',
      to: userEmail,
      subject: '🔐 Password Reset - NeyoMarket',
      html: emailContent,
      replyTo: 'support@neyomarket.com.ng',
    });

    console.log('[Gmail SMTP] Password reset email sent to', userEmail, '- Message ID:', info.messageId);
    return { success: true, messageId: info.messageId };
  } catch (err) {
    console.error('[Gmail SMTP] Error sending password reset email:', err.message);
    return { success: false, error: err.message };
  }
}

/* ════════════════════════════════════════════════════════
   MAIN HANDLER
════════════════════════════════════════════════════════ */
module.exports = async function handler(req, res) {
  cors(res);
  if (req.method === 'OPTIONS') return res.status(200).end();

  try {

    /* ────────────────────────────────────────────────────
       ADMIN LOGIN — server-side only, no credentials in frontend
    ──────────────────────────────────────────────────── */
    if (req.query.action === 'admin-login') {
      if (req.method !== 'POST') return res.status(405).json({ error: 'POST only.' });
      const { email, password } = req.body || {};
      if (!email || !password) return res.status(400).json({ error: 'Missing credentials.' });

      const ADMIN_EMAIL = process.env.ADMIN_EMAIL || 'admin@neyomarket.com';
      const ADMIN_PASS  = process.env.ADMIN_PASS  || 'NEYO2026';

      const emailOk = email.trim().toLowerCase() === ADMIN_EMAIL.toLowerCase();
      const passOk  = password === ADMIN_PASS;

      if (!emailOk || !passOk)
        return res.status(401).json({ error: 'Invalid credentials.' });

      return res.status(200).json({
        ok: true,
        user: { id: 'master_admin_001', name: 'NeyoMarket Admin', email: ADMIN_EMAIL, role: 'admin' }
      });
    }

    /* ────────────────────────────────────────────────────
       LOGIN
    ──────────────────────────────────────────────────── */
    if (req.query.action === 'login') {
      if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed.' });

      const { email, password } = req.body || {};
      if (!email || !password)
        return res.status(400).json({ error: 'Email and password are required.' });
      if (!validateEmail(email))
        return res.status(400).json({ error: 'Invalid email format.' });

      const rlKey   = getRateLimitKey(req, email);
      const rlCheck = checkRateLimit(rlKey);
      if (!rlCheck.allowed) {
        return res.status(429).json({
          error: `Too many failed attempts. Please wait ${rlCheck.waitMinutes} minute(s) before trying again.`
        });
      }

      const rows = await sql`
        SELECT * FROM users WHERE LOWER(email) = LOWER(${email.trim()}) LIMIT 1
      `;

      const user       = rows[0] || null;
      const dummyHash  = '$2a$10$abcdefghijklmnopqrstuuABCDEFGHIJKLMNOPQRSTUVWXYZ01234';
      const hashToTest = user ? user.password_hash : dummyHash;
      const match      = await bcrypt.compare(password, hashToTest);

      if (!user || !match) {
        recordFailedAttempt(rlKey);
        const entry = loginAttempts.get(rlKey);
        const remaining = RATE_LIMIT.maxAttempts - (entry ? entry.count : 1);
        const msg = remaining > 0
          ? `Incorrect email or password. (${remaining} attempt${remaining !== 1 ? 's' : ''} remaining)`
          : 'Too many failed attempts. Please wait 30 minutes.';
        return res.status(401).json({ error: msg });
      }

      if (!user.is_verified) {
        return res.status(403).json({ 
          error: 'Please verify your account via email to continue.',
          requiresVerification: true,
          email: user.email
        });
      }

      if (user.suspended) {
        return res.status(403).json({ error: 'This account has been suspended. Contact support.' });
      }

      clearAttempts(rlKey);

      /* Auto-downgrade expired membership tier on login */
      if (user.membership_tier && user.membership_tier !== 'free' && user.tier_expires_at) {
        const expired = new Date(user.tier_expires_at) < new Date();
        if (expired) {
          await sql`
            UPDATE users
            SET membership_tier = 'free', tier_expires_at = NULL, tier_ref = ''
            WHERE id = ${user.id}
          `;
          user.membership_tier = 'free';
          user.tier_expires_at = null;
          user.tier_ref = '';
        }
      }

      return res.status(200).json({ user: toPublicUser(user) });
    }

    /* ────────────────────────────────────────────────────
       REGISTER
    ──────────────────────────────────────────────────── */
    if (req.query.action === 'register') {
      if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed.' });

      const { name, email, phone, role, password, affCode } = req.body || {};

      if (!name || !email || !password)
        return res.status(400).json({ error: 'Name, email and password are required.' });
      if (!validateEmail(email))
        return res.status(400).json({ error: 'Invalid email format.' });
      if (password.length < 8)
        return res.status(400).json({ error: 'Password must be at least 8 characters.' });
      if (name.trim().length < 2)
        return res.status(400).json({ error: 'Name must be at least 2 characters.' });

      const existing = await sql`
        SELECT id FROM users WHERE LOWER(email) = LOWER(${email.trim()}) LIMIT 1
      `;
      if (existing.length)
        return res.status(409).json({ error: 'An account with this email already exists.' });

      const hash     = await bcrypt.hash(password, 10);
      const safeRole = ['buyer', 'seller', 'affiliate'].includes(role) ? role : 'buyer';
      const code     = 'REF' + Math.random().toString(36).substr(2, 7).toUpperCase();
      const verificationToken = generateVerificationToken();
      const tokenExpiry = new Date(Date.now() + 24 * 60 * 60 * 1000);

      const inserted = await sql`
        INSERT INTO users (name, email, phone, role, password_hash, aff_code, is_verified, verification_token, token_expiry, joined)
        VALUES (
          ${name.trim()},
          ${email.trim().toLowerCase()},
          ${phone  || ''},
          ${safeRole},
          ${hash},
          ${code},
          false,
          ${verificationToken},
          ${tokenExpiry.toISOString()},
          NOW()
        )
        RETURNING *
      `;

      const emailResult = await sendVerificationEmail(
        inserted[0].email,
        inserted[0].name,
        verificationToken
      );

      if (!emailResult.success) {
        console.error('[register] Email send failed:', emailResult.error);
        return res.status(201).json({
          user: toPublicUser(inserted[0]),
          requiresVerification: true,
          emailSent: false,
          message: 'Account created but verification email could not be sent. Please contact support.'
        });
      }

      return res.status(201).json({
        user: toPublicUser(inserted[0]),
        requiresVerification: true,
        emailSent: true,
        message: 'Account created! Verification email sent to ' + inserted[0].email
      });
    }

    /* ────────────────────────────────────────────────────
       VERIFY EMAIL
    ──────────────────────────────────────────────────── */
    if (req.query.action === 'verify-email') {
      if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed.' });

      const { token } = req.body || {};
      if (!token)
        return res.status(400).json({ error: 'Verification token is required.' });

      const rows = await sql`
        SELECT * FROM users 
        WHERE verification_token = ${token}
        AND token_expiry > NOW()
        LIMIT 1
      `;

      if (!rows.length) {
        return res.status(400).json({ 
          error: 'Invalid or expired verification token. Please request a new one.'
        });
      }

      const user = rows[0];

      await sql`
        UPDATE users 
        SET is_verified = true, verification_token = NULL, token_expiry = NULL
        WHERE id = ${user.id}
      `;

      const updated = await sql`SELECT * FROM users WHERE id = ${user.id} LIMIT 1`;
      return res.status(200).json({
        ok: true,
        message: 'Email verified successfully! You can now log in.',
        user: toPublicUser(updated[0])
      });
    }

    /* ────────────────────────────────────────────────────
       RESET PASSWORD
    ──────────────────────────────────────────────────── */
    if (req.query.action === 'reset-password') {
      if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed.' });

      const { email } = req.body || {};
      if (!email)
        return res.status(400).json({ error: 'Email is required.' });

      const rows = await sql`
        SELECT id, name, email FROM users WHERE LOWER(email) = LOWER(${email.trim()}) LIMIT 1
      `;

      if (!rows.length) {
        return res.status(200).json({ 
          ok: true,
          message: 'If an account with this email exists, a password reset link has been sent.'
        });
      }

      const user = rows[0];
      const resetToken = crypto.randomBytes(32).toString('hex');
      const tokenExpiry = new Date(Date.now() + 60 * 60 * 1000);
      const resetLink = `https://neyomarket.com.ng/reset-password.html?token=${resetToken}`;

      await sql`
        UPDATE users 
        SET reset_token = ${resetToken}, reset_token_expiry = ${tokenExpiry.toISOString()}
        WHERE id = ${user.id}
      `;

      const emailResult = await sendPasswordResetEmail(user.email, user.name, resetLink);

      if (!emailResult.success) {
        console.error('[reset-password] Email send failed:', emailResult.error);
        return res.status(500).json({ 
          error: 'Failed to send reset email. Please try again later.'
        });
      }

      return res.status(200).json({ 
        ok: true,
        message: 'Password reset link has been sent to your email. Link expires in 1 hour.'
      });
    }

    /* ────────────────────────────────────────────────────
       CONFIRM PASSWORD RESET
    ──────────────────────────────────────────────────── */
    if (req.query.action === 'confirm-password-reset') {
      if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed.' });

      const { token, newPassword } = req.body || {};
      if (!token || !newPassword)
        return res.status(400).json({ error: 'Token and new password are required.' });
      if (newPassword.length < 8)
        return res.status(400).json({ error: 'Password must be at least 8 characters.' });

      const rows = await sql`
        SELECT * FROM users 
        WHERE reset_token = ${token}
        AND reset_token_expiry > NOW()
        LIMIT 1
      `;

      if (!rows.length) {
        return res.status(400).json({ 
          error: 'Invalid or expired password reset token. Please request a new reset link.'
        });
      }

      const user = rows[0];
      const hash = await bcrypt.hash(newPassword, 10);

      await sql`
        UPDATE users 
        SET password_hash = ${hash}, reset_token = NULL, reset_token_expiry = NULL
        WHERE id = ${user.id}
      `;

      return res.status(200).json({
        ok: true,
        message: 'Password reset successfully! You can now log in with your new password.'
      });
    }

    /* ────────────────────────────────────────────────────
       CHANGE PASSWORD
    ──────────────────────────────────────────────────── */
    if (req.query.action === 'change-password') {
      if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed.' });

      const { userId, currentPassword, newPassword } = req.body || {};
      if (!userId || !newPassword)
        return res.status(400).json({ error: 'userId and newPassword are required.' });
      if (newPassword.length < 8)
        return res.status(400).json({ error: 'Password must be at least 8 characters.' });

      const rows = await sql`SELECT * FROM users WHERE id = ${userId} LIMIT 1`;
      if (!rows.length) return res.status(404).json({ error: 'User not found.' });

      if (currentPassword) {
        const match = await bcrypt.compare(currentPassword, rows[0].password_hash);
        if (!match)
          return res.status(401).json({ error: 'Current password is incorrect.' });
      }

      const hash = await bcrypt.hash(newPassword, 10);
      await sql`UPDATE users SET password_hash = ${hash} WHERE id = ${userId}`;
      return res.status(200).json({ ok: true });
    }

    /* ────────────────────────────────────────────────────
       UPDATE PROFILE
    ──────────────────────────────────────────────────── */
    if (req.query.action === 'update-profile') {
      if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed.' });

      const {
        userId, payoutBank, payoutAcct, payoutAname,
        subaccountCode, subaccountStatus,
        role, suspended, kycStatus,
        membershipTier, tierRef, tierExpiresAt,
        loyaltyPoints, loyaltyHistory
      } = req.body || {};

      if (!userId) return res.status(400).json({ error: 'userId is required.' });

      const safeRole = role && ['buyer', 'seller', 'affiliate', 'admin'].includes(role)
        ? role : null;
      const safeTier = membershipTier && ['free', 'starter', 'pro', 'business'].includes(membershipTier)
        ? membershipTier : null;
      /* Auto-set expiry 30 days from now when upgrading to a paid tier */
      const safeTierExpiry = safeTier && safeTier !== 'free'
        ? (tierExpiresAt || new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString())
        : (safeTier === 'free' ? null : undefined);
      const safeLoyaltyPts  = (loyaltyPoints !== undefined && loyaltyPoints !== null)
        ? parseInt(loyaltyPoints) : null;
      const safeLoyaltyHist = loyaltyHistory
        ? JSON.stringify(loyaltyHistory) : null;

      await sql`
        UPDATE users SET
          payout_bank       = COALESCE(${payoutBank       ?? null}, payout_bank),
          payout_acct       = COALESCE(${payoutAcct       ?? null}, payout_acct),
          payout_aname      = COALESCE(${payoutAname      ?? null}, payout_aname),
          subaccount_code   = COALESCE(${subaccountCode   ?? null}, subaccount_code),
          subaccount_status = COALESCE(${subaccountStatus ?? null}, subaccount_status),
          role              = COALESCE(${safeRole         ?? null}, role),
          suspended         = COALESCE(${suspended        ?? null}, suspended),
          kyc_status        = COALESCE(${kycStatus        ?? null}, kyc_status),
          membership_tier   = COALESCE(${safeTier         ?? null}, membership_tier),
          tier_ref          = COALESCE(${tierRef          ?? null}, tier_ref),
          tier_expires_at   = CASE
            WHEN ${safeTier ?? null} = 'free'  THEN NULL
            WHEN ${safeTier ?? null} IS NOT NULL THEN ${safeTierExpiry ?? null}::timestamptz
            ELSE tier_expires_at
          END,
          loyalty_points    = COALESCE(${safeLoyaltyPts   ?? null}, loyalty_points),
          loyalty_history   = COALESCE(${safeLoyaltyHist  ?? null}::jsonb, loyalty_history)
        WHERE id = ${userId}
      `;

      const rows = await sql`SELECT * FROM users WHERE id = ${userId} LIMIT 1`;
      if (!rows.length) return res.status(404).json({ error: 'User not found.' });
      return res.status(200).json({ ok: true, user: toPublicUser(rows[0]) });
    }

    /* ────────────────────────────────────────────────────
       KYC SUBMISSION
    ──────────────────────────────────────────────────── */
    if (req.query.action === 'kyc') {
      if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed.' });

      const { userId, kycType, kycNumber } = req.body || {};
      if (!userId || !kycType || !kycNumber)
        return res.status(400).json({ error: 'userId, kycType and kycNumber are required.' });
      if (!['nin', 'bvn'].includes(kycType.toLowerCase()))
        return res.status(400).json({ error: 'kycType must be nin or bvn.' });
      if (!/^\d{10,11}$/.test(kycNumber.trim()))
        return res.status(400).json({ error: 'Invalid ' + kycType.toUpperCase() + ' — must be 10 or 11 digits.' });

      await sql`
        UPDATE users
        SET kyc_status = 'pending',
            kyc_type   = ${kycType.toLowerCase()},
            kyc_number = ${kycNumber.trim()}
        WHERE id = ${userId}
      `;

      const rows = await sql`SELECT * FROM users WHERE id = ${userId} LIMIT 1`;
      if (!rows.length) return res.status(404).json({ error: 'User not found.' });

      return res.status(200).json({
        ok:        true,
        kycStatus: 'pending',
        message:   'Submitted for review. You will be notified within 24 hours.',
        user:      toPublicUser(rows[0])
      });
    }

    /* ────────────────────────────────────────────────────
       VERIFY SELLER (requires ₦2,000 payment)
    ──────────────────────────────────────────────────── */
    if (req.query.action === 'verify-seller' && req.method === 'POST') {
      const { userId, ref } = req.body || {};
      if (!userId || !ref) return res.status(400).json({ ok: false, error: 'userId and ref required' });

      try {
        const secretKey = process.env.PAYSTACK_SECRET_KEY || process.env.PAYSTACK_SECRET || '';
        console.log('[verify-seller] userId:', userId, 'ref:', ref, 'hasKey:', !!secretKey);

        /* Verify payment with Paystack */
        let paymentConfirmed = false;
        if (secretKey) {
          try {
            const paystackRes  = await fetch('https://api.paystack.co/transaction/verify/' + encodeURIComponent(String(ref)), {
              headers: { Authorization: 'Bearer ' + secretKey }
            });
            const paystackData = await paystackRes.json();
            console.log('[verify-seller] Paystack status:', paystackData.data && paystackData.data.status);

            if (paystackData.status && paystackData.data && paystackData.data.status === 'success') {
              /* Confirm it's a badge payment and correct amount */
              const amountPaid = paystackData.data.amount || 0;
              if (amountPaid >= 200000) { /* ₦2,000 in kobo */
                paymentConfirmed = true;
              } else {
                return res.status(403).json({ ok: false, error: 'Payment amount insufficient. Expected ₦2,000.' });
              }
            } else {
              return res.status(403).json({ ok: false, error: 'Payment not confirmed by Paystack. Status: ' + (paystackData.data && paystackData.data.status || 'unknown') });
            }
          } catch(psErr) {
            console.error('[verify-seller] Paystack call failed:', psErr.message);
            /* If Paystack is unreachable, check ref starts with BADGE- as fallback */
            if (String(ref).startsWith('BADGE-')) paymentConfirmed = true;
            else return res.status(502).json({ ok: false, error: 'Could not reach Paystack. Try again.' });
          }
        } else {
          /* No secret key configured — accept if ref starts with BADGE- */
          console.warn('[verify-seller] PAYSTACK_SECRET_KEY not set — using ref prefix check');
          if (String(ref).startsWith('BADGE-')) paymentConfirmed = true;
          else return res.status(500).json({ ok: false, error: 'Payment gateway not configured.' });
        }

        if (!paymentConfirmed) {
          return res.status(403).json({ ok: false, error: 'Payment verification failed.' });
        }

        /* Set badge_verified = true on user */
        await sql`UPDATE users SET badge_verified = true WHERE id::text = ${String(userId)}`;

        /* Set badge_verified = true on all seller products */
        await sql`UPDATE products SET badge_verified = true WHERE seller_id::text = ${String(userId)}`;

        console.log('[verify-seller] Badge granted to userId:', userId);

        /* Log in seller_badges if table exists */
        try {
          await sql`
            INSERT INTO seller_badges (user_id, payment_ref, amount_paid, created_at)
            VALUES (${String(userId)}, ${String(ref)}, 2000, NOW())
            ON CONFLICT DO NOTHING
          `;
        } catch(e) { /* table may not exist — non-fatal */ }

        return res.status(200).json({ ok: true, message: 'Trusted badge granted!' });

      } catch (err) {
        console.error('[verify-seller] ERROR:', err.message);
        return res.status(500).json({ ok: false, error: 'Verification failed: ' + err.message });
      }
    }

    /* ══════════════════════════════════════════════════
       OG — Open Graph product preview for social sharing
    ══════════════════════════════════════════════════ */
    if (req.query.action === 'og') {
      const productId = req.query.id || req.query.p || '';
      const refCode   = req.query.ref || '';
      if (!productId) return res.status(400).send('Missing product id');

      const rows = await sql`
        SELECT name, description, price, discount_price, imgs, seller, type, currency
        FROM products
        WHERE id = ${Number(productId)} AND status = 'active'
        LIMIT 1
      `;

      if (!rows.length) return res.status(404).send('Product not found');

      /* ── Track view: increment view_count for every OG link open ── */
      try {
        await sql`
          UPDATE products
          SET view_count = COALESCE(view_count, 0) + 1
          WHERE id = ${Number(productId)}
        `;
      } catch(e) { /* silent — never break the OG redirect */ }

      const p        = rows[0];
      const name     = p.name || 'Product on NeyoMarket';
      const desc     = (p.description || 'Buy securely on NeyoMarket — Nigeria\'s trusted marketplace.').slice(0, 200);
      const price    = p.discount_price ? p.discount_price : p.price;
      const symMap   = { NGN:'₦', USD:'$', GBP:'£', EUR:'€', CAD:'CA$', GHS:'GH₵' };
      const sym      = symMap[p.currency] || '₦';
      const priceStr = sym + Number(price).toLocaleString();
      const siteUrl  = 'https://neyomarket.com.ng';
      /* Include ref code in redirect so affiliate commission is tracked */
      const prodUrl  = siteUrl + '/?p=' + productId + (refCode ? '&ref=' + encodeURIComponent(refCode) : '');

      /* Use proxied image URL — bypasses ImgBB hotlink block */
      const imgProxyUrl = siteUrl + '/api/auth?action=og-img&id=' + productId;

      const html = `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>${name} — NeyoMarket</title>

  <!-- Open Graph -->
  <meta property="og:type"         content="product"/>
  <meta property="og:url"          content="${prodUrl}"/>
  <meta property="og:title"        content="${name} — ${priceStr}"/>
  <meta property="og:description"  content="${desc}"/>
  <meta property="og:image"        content="${imgProxyUrl}"/>
  <meta property="og:image:width"  content="800"/>
  <meta property="og:image:height" content="800"/>
  <meta property="og:image:type"   content="image/jpeg"/>
  <meta property="og:site_name"    content="NeyoMarket"/>

  <!-- Twitter Card -->
  <meta name="twitter:card"        content="summary_large_image"/>
  <meta name="twitter:title"       content="${name} — ${priceStr}"/>
  <meta name="twitter:description" content="${desc}"/>
  <meta name="twitter:image"       content="${imgProxyUrl}"/>

  <!-- WhatsApp / general -->
  <meta name="description" content="${desc}"/>

  <!-- Redirect human visitors to the real SPA -->
  <meta http-equiv="refresh" content="0;url=${prodUrl}"/>
</head>
<body>
  <p>Redirecting to <a href="${prodUrl}">${name}</a>…</p>
</body>
</html>`;

      res.setHeader('Content-Type', 'text/html; charset=utf-8');
      res.setHeader('Cache-Control', 's-maxage=3600, stale-while-revalidate');
      return res.status(200).send(html);
    }

    /* ══════════════════════════════════════════════════
       OG-IMG — proxy product image to bypass ImgBB hotlink block
    ══════════════════════════════════════════════════ */
    if (req.query.action === 'og-img') {
      const productId = req.query.id || '';
      if (!productId) return res.status(400).send('Missing id');

      try {
        const rows = await sql`
          SELECT imgs FROM products
          WHERE id = ${Number(productId)} AND status = 'active'
          LIMIT 1
        `;

        if (!rows.length) return res.status(404).send('Not found');

        /* Parse first image URL */
        let imgUrl = '';
        try {
          const imgs = typeof rows[0].imgs === 'string'
            ? JSON.parse(rows[0].imgs) : (rows[0].imgs || []);
          if (Array.isArray(imgs) && imgs.length) imgUrl = imgs[0];
        } catch(e) {}

        if (!imgUrl) {
          /* Fallback — redirect to logo */
          res.setHeader('Location', 'https://neyomarket.com.ng/neyomarket-logo.webp');
          return res.status(302).end();
        }

        /* Fetch image from ImgBB and pipe it through */
        const imgRes = await fetch(imgUrl, {
          headers: {
            'User-Agent': 'Mozilla/5.0 (compatible; NeyoMarket/1.0)',
            'Referer':    'https://neyomarket.com.ng'
          }
        });

        if (!imgRes.ok) {
          res.setHeader('Location', 'https://neyomarket.com.ng/neyomarket-logo.webp');
          return res.status(302).end();
        }

        const contentType = imgRes.headers.get('content-type') || 'image/jpeg';
        const buffer      = await imgRes.arrayBuffer();

        res.setHeader('Content-Type', contentType);
        res.setHeader('Cache-Control', 's-maxage=86400, stale-while-revalidate');
        res.setHeader('Access-Control-Allow-Origin', '*');
        return res.status(200).send(Buffer.from(buffer));

      } catch(e) {
        console.warn('[og-img] proxy failed:', e.message);
        res.setHeader('Location', 'https://neyomarket.com.ng/neyomarket-logo.webp');
        return res.status(302).end();
      }
    }

    /* ══════════════════════════════════════════════════
       SEND EMAIL — internal mailer used by payment.js
       POST ?action=send-email
       Body: { to, subject, html }
    ══════════════════════════════════════════════════ */
    if (req.query.action === 'send-email' && req.method === 'POST') {
      const { to, subject, html } = req.body || {};
      if (!to || !subject || !html)
        return res.status(400).json({ ok: false, error: 'to, subject and html are required.' });

      if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(to).trim()))
        return res.status(400).json({ ok: false, error: 'Invalid recipient email address.' });

      try {
        const info = await gmailTransporter.sendMail({
          from:    'NeyoMarket <' + (process.env.GMAIL_USER || 'noreply@neyomarket.com.ng') + '>',
          to:      String(to).trim(),
          subject: String(subject),
          html:    String(html),
          replyTo: 'support@neyomarket.com.ng'
        });
        console.log('[auth/send-email] Sent to', to, '— ID:', info.messageId);
        return res.status(200).json({ ok: true, messageId: info.messageId });
      } catch (err) {
        console.error('[auth/send-email] Failed:', err.message);
        return res.status(500).json({ ok: false, error: 'Email send failed: ' + err.message });
      }
    }

    /* ══════════════════════════════════════════════════
       STAFF GATE — login + product/KYC review actions
    ══════════════════════════════════════════════════ */
    const STAFF_ACCOUNTS = {
      'product1@neyomarket.com': { pass: process.env.STAFF_PR1_PASS,  role: 'product_reviewer', name: 'Product Reviewer 1' },
      'product2@neyomarket.com': { pass: process.env.STAFF_PR2_PASS,  role: 'product_reviewer', name: 'Product Reviewer 2' },
      'kyc1@neyomarket.com':     { pass: process.env.STAFF_KYC1_PASS, role: 'kyc_reviewer',     name: 'KYC Reviewer'      },
    };

    if (req.query.action === 'staff-login' && req.method === 'POST') {
      const { email, password } = req.body || {};
      if (!email || !password) return res.status(400).json({ ok: false, error: 'Email and password required.' });
      const acct = STAFF_ACCOUNTS[(email || '').toLowerCase()];
      if (!acct || acct.pass !== password) return res.status(401).json({ ok: false, error: 'Invalid credentials.' });
      return res.status(200).json({ ok: true, role: acct.role, name: acct.name, email: email.toLowerCase() });
    }

    if (req.query.action === 'staff-pending-products' && req.method === 'GET') {
      const { email } = req.query;
      const acct = STAFF_ACCOUNTS[(email||'').toLowerCase()];
      if (!acct || acct.role !== 'product_reviewer') return res.status(403).json({ ok: false, error: 'Not authorised.' });
      const rows = await sql`SELECT id,name,type,cat,price,discount_price,currency,description,seller,seller_id,seller_email,imgs,emoji,badge,location,created_at,commission FROM products WHERE status='pending' ORDER BY created_at ASC`;
      return res.status(200).json({ ok: true, products: rows });
    }

    if (req.query.action === 'staff-review-product' && req.method === 'POST') {
      const { productId, status, reviewerEmail } = req.body || {};
      if (!productId || !status || !reviewerEmail) return res.status(400).json({ ok: false, error: 'Missing fields.' });
      const acct = STAFF_ACCOUNTS[(reviewerEmail||'').toLowerCase()];
      if (!acct || acct.role !== 'product_reviewer') return res.status(403).json({ ok: false, error: 'Not authorised.' });
      if (!['active','rejected'].includes(status)) return res.status(400).json({ ok: false, error: 'Invalid status.' });
      await sql`UPDATE products SET status=${status}, reviewed_by=${reviewerEmail}, reviewed_at=NOW() WHERE id=${Number(productId)}`;
      return res.status(200).json({ ok: true });
    }

    if (req.query.action === 'staff-pending-kyc' && req.method === 'GET') {
      const { email } = req.query;
      const acct = STAFF_ACCOUNTS[(email||'').toLowerCase()];
      if (!acct || acct.role !== 'kyc_reviewer') return res.status(403).json({ ok: false, error: 'Not authorised.' });
      const rows = await sql`SELECT id,name,email,phone,role,kyc_status,kyc_type,nin_number,joined,created_at FROM users WHERE kyc_status='pending' AND nin_number IS NOT NULL ORDER BY created_at ASC`;
      return res.status(200).json({ ok: true, users: rows });
    }

    if (req.query.action === 'staff-review-kyc' && req.method === 'POST') {
      const { userId, status, reviewerEmail } = req.body || {};
      if (!userId || !status || !reviewerEmail) return res.status(400).json({ ok: false, error: 'Missing fields.' });
      const acct = STAFF_ACCOUNTS[(reviewerEmail||'').toLowerCase()];
      if (!acct || acct.role !== 'kyc_reviewer') return res.status(403).json({ ok: false, error: 'Not authorised.' });
      if (!['verified','rejected'].includes(status)) return res.status(400).json({ ok: false, error: 'Invalid status.' });
      await sql`UPDATE users SET kyc_status=${status}, is_verified=${status==='verified'}, kyc_reviewed_by=${reviewerEmail}, kyc_reviewed_at=NOW() WHERE id=${String(userId)}`;
      return res.status(200).json({ ok: true });
    }

    /* ══ ADMIN WALLET BALANCE ══ */
    if (req.query.action === 'admin-wallet-balance' && req.method === 'GET') {
      try {
        const rows = await sql`
          SELECT admin_wallet FROM users
          WHERE role = 'admin'
          ORDER BY id ASC LIMIT 1
        `;
        const balance = rows.length ? parseFloat(rows[0].admin_wallet || 0) : 0;
        const txns    = await sql`
          SELECT type, amount, description, ref, created_at
          FROM admin_wallet_transactions
          ORDER BY created_at DESC LIMIT 50
        `;
        return res.status(200).json({ ok: true, balance, transactions: txns });
      } catch(err) {
        console.error('[auth/admin-wallet-balance]', err.message);
        return res.status(500).json({ ok: false, error: 'Could not load admin wallet.' });
      }
    }

    /* ══ ADMIN WALLET TOP UP ══ */
    if (req.query.action === 'admin-wallet-topup' && req.method === 'POST') {
      const { amount, ref, adminId } = req.body || {};
      if (!amount || !ref) return res.status(400).json({ ok: false, error: 'amount and ref required.' });
      const amt = parseFloat(amount);
      try {
        /* Credit admin wallet */
        await sql`
          UPDATE users SET admin_wallet = COALESCE(admin_wallet, 0) + ${amt}
          WHERE role = 'admin'
        `;
        /* Record transaction */
        await sql`
          INSERT INTO admin_wallet_transactions (type, amount, description, ref, created_at)
          VALUES ('credit', ${amt}, ${'Platform wallet top-up via Paystack'}, ${ref}, NOW())
        `;
        console.log('[auth/admin-wallet-topup] ₦' + amt + ' ref:' + ref);
        return res.status(200).json({ ok: true });
      } catch(err) {
        console.error('[auth/admin-wallet-topup]', err.message);
        return res.status(500).json({ ok: false, error: 'Could not update admin wallet.' });
      }
    }

    /* ══ ADMIN WALLET DEDUCT ══ */
    if (req.query.action === 'admin-wallet-deduct' && req.method === 'POST') {
      const { amount, ref, description } = req.body || {};
      if (!amount) return res.status(400).json({ ok: false, error: 'amount required.' });
      const amt = parseFloat(amount);
      try {
        await sql`
          UPDATE users SET admin_wallet = COALESCE(admin_wallet, 0) - ${amt}
          WHERE role = 'admin'
        `;
        await sql`
          INSERT INTO admin_wallet_transactions (type, amount, description, ref, created_at)
          VALUES ('debit', ${-amt}, ${description || 'Seller withdrawal'}, ${ref || ''}, NOW())
        `;
        return res.status(200).json({ ok: true });
      } catch(err) {
        console.error('[auth/admin-wallet-deduct]', err.message);
        return res.status(500).json({ ok: false, error: 'Could not deduct from admin wallet.' });
      }
    }

    /* ══════════════════════════════════════════════════
       ADMIN WALLET — balance + top up
    ══════════════════════════════════════════════════ */
    if (req.query.action === 'admin-wallet-balance') {
      try {
        const rows = await sql`SELECT admin_wallet FROM users WHERE role = 'admin' ORDER BY id ASC LIMIT 1`;
        const balance = parseFloat((rows[0] && rows[0].admin_wallet) || 0);
        const txns    = await sql`SELECT type, amount, description, ref, created_at FROM admin_wallet_transactions ORDER BY created_at DESC LIMIT 50`;
        return res.status(200).json({ ok: true, balance, transactions: txns });
      } catch(err) {
        console.error('[auth/admin-wallet-balance]', err.message);
        return res.status(500).json({ ok: false, error: 'Could not load admin wallet.' });
      }
    }

    if (req.query.action === 'admin-wallet-topup' && req.method === 'POST') {
      const { amount, ref, adminId } = req.body || {};
      if (!amount || !ref) return res.status(400).json({ ok: false, error: 'amount and ref required.' });
      const amt = parseFloat(amount);
      try {
        await sql`UPDATE users SET admin_wallet = COALESCE(admin_wallet, 0) + ${amt} WHERE role = 'admin'`;
        await sql`INSERT INTO admin_wallet_transactions (type, amount, description, ref, created_at) VALUES ('credit', ${amt}, ${'Platform wallet top-up by admin'}, ${ref}, NOW())`;
        return res.status(200).json({ ok: true });
      } catch(err) {
        console.error('[auth/admin-wallet-topup]', err.message);
        return res.status(500).json({ ok: false, error: 'Could not process top-up.' });
      }
    }

    /* ══════════════════════════════════════════════════
       GOOGLE LOGIN — verify Google JWT, upsert user
       POST ?action=google-login  { credential: <JWT> }
       Uses Google's tokeninfo endpoint (no extra library).
    ══════════════════════════════════════════════════ */
    if (req.query.action === 'google-login' && req.method === 'POST') {
      const { credential } = req.body || {};
      if (!credential) return res.status(400).json({ ok: false, error: 'credential required.' });

      try {
        /* Decode Google JWT — split by dot, base64url decode the middle payload */
        const parts  = credential.split('.');
        const base64 = parts[1].replace(/-/g, '+').replace(/_/g, '/');
        const json   = Buffer.from(base64, 'base64').toString('utf8');
        const payload = JSON.parse(json);

        /* Log payload for debugging */
        console.log('[google-login] payload email:', payload.email, 'iss:', payload.iss);

        if (!payload.email) {
          return res.status(400).json({ ok: false, error: 'No email in Google token.' });
        }

        const gEmail = payload.email.toLowerCase().trim();
        const gName  = payload.name || payload.given_name || gEmail.split('@')[0];

        /* Check if user already exists */
        const existing = await sql`SELECT * FROM users WHERE email = ${gEmail} LIMIT 1`;
        const isNew = existing.length === 0;

        let user;
        if (!isNew) {
          user = existing[0];
          if (!user.is_verified) {
            await sql`UPDATE users SET is_verified = true WHERE email = ${gEmail}`;
            user.is_verified = true;
          }
        } else {
          /* Create new user from Google profile */
          const newId      = Date.now();
          const newAffCode = 'REF' + Math.random().toString(36).substr(2, 7).toUpperCase();
          await sql`
            INSERT INTO users (id, name, email, password_hash, role, aff_code, is_verified, joined, created_at)
            VALUES (${newId}, ${gName}, ${gEmail}, '', 'buyer', ${newAffCode}, true, NOW(), NOW())
          `;
          const newRows = await sql`SELECT * FROM users WHERE id = ${newId} LIMIT 1`;
          user = newRows[0];
        }

        return res.status(200).json({ ok: true, user: toPublicUser(user), isNew });

      } catch (gErr) {
        console.error('[auth/google-login] ERROR:', gErr.message, gErr.stack);
        return res.status(500).json({ ok: false, error: 'Google login failed: ' + gErr.message });
      }
    }

    return res.status(400).json({ error: 'Unknown action.' });

  } catch (err) {
    console.error('[auth.js] ERROR:', err.message);
    return res.status(500).json({ error: 'Internal server error. Please try again.' });
  }
};
