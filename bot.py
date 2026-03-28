import os, random, logging, asyncio, time
import asyncpg
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.error import Forbidden, TimedOut, BadRequest
from telegram.ext import (ApplicationBuilder, CommandHandler, MessageHandler,
                           CallbackQueryHandler, PreCheckoutQueryHandler,
                           ContextTypes, filters)

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN    = os.getenv('BOT_TOKEN')
DATABASE_URL = os.getenv('DATABASE_URL')
ADMIN_ID     = 643086953
VIP_REFERRAL_THRESHOLD = 3
VIP_REFERRAL_DAYS      = 3
MATCH_INVITE_DELAY     = 45
FREE_TOD_LIMIT         = 3
RATE_LIMIT_SECONDS     = 1   # max 1 message per second per user

VIP_PACKAGES = {
    'week':  {'stars': 50,  'days': 7,  'label': '1 Week VIP',       'emoji': '⭐'},
    'month': {'stars': 100, 'days': 30, 'label': '1 Month VIP',      'emoji': '🌟'},
    'test':  {'stars': 1,   'days': 1,  'label': 'Test VIP (Admin)', 'emoji': '🧪'},
}

db_pool: asyncpg.Pool = None


# ─────────────────────────────────────────────────────────────────────────────
# DB INIT
# ─────────────────────────────────────────────────────────────────────────────

async def init_db():
    await db_pool.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY, username TEXT, name TEXT,
            gender TEXT CHECK (gender IN ('Male','Female')),
            country TEXT, age INT, is_vip BOOLEAN DEFAULT FALSE,
            vip_expiry TIMESTAMP WITH TIME ZONE, referral_count INT DEFAULT 0,
            referred_by BIGINT, total_messages INT DEFAULT 0,
            is_banned BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW())''')
    await db_pool.execute('''
        CREATE TABLE IF NOT EXISTS waiting_users (
            user_id BIGINT PRIMARY KEY, preferred_gender TEXT,
            queued_at TIMESTAMP WITH TIME ZONE DEFAULT NOW())''')
    await db_pool.execute('''
        CREATE TABLE IF NOT EXISTS active_chats (
            user_id BIGINT PRIMARY KEY, partner_id BIGINT)''')
    await db_pool.execute('''
        CREATE TABLE IF NOT EXISTS reports (
            id SERIAL PRIMARY KEY, reporter_id BIGINT NOT NULL, reported_id BIGINT NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW())''')
    await db_pool.execute('''
        CREATE TABLE IF NOT EXISTS chat_logs (
            id SERIAL PRIMARY KEY, sender_id BIGINT NOT NULL, partner_id BIGINT NOT NULL,
            message TEXT, created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW())''')
    await db_pool.execute('''
        CREATE TABLE IF NOT EXISTS soulmate_reveals (
            user_id BIGINT PRIMARY KEY, partner_id BIGINT NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW())''')

    # Column additions (idempotent)
    for s in [
        'CREATE INDEX IF NOT EXISTS idx_wq         ON waiting_users(queued_at)',
        'CREATE INDEX IF NOT EXISTS idx_ve         ON users(vip_expiry)',
        'CREATE INDEX IF NOT EXISTS idx_rr         ON reports(reported_id)',
        'CREATE INDEX IF NOT EXISTS idx_clp        ON chat_logs(sender_id,partner_id)',
        'CREATE INDEX IF NOT EXISTS idx_ac_partner ON active_chats(partner_id)',
        'ALTER TABLE users ADD COLUMN IF NOT EXISTS is_banned          BOOLEAN DEFAULT FALSE',
        'ALTER TABLE users ADD COLUMN IF NOT EXISTS age                INT',
        'ALTER TABLE users ADD COLUMN IF NOT EXISTS country            TEXT',
        'ALTER TABLE users ADD COLUMN IF NOT EXISTS referred_by        BIGINT',
        'ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_count     INT DEFAULT 0',
        'ALTER TABLE users ADD COLUMN IF NOT EXISTS vip_expiry         TIMESTAMP WITH TIME ZONE',
        'ALTER TABLE users ADD COLUMN IF NOT EXISTS total_messages     INT DEFAULT 0',
        'ALTER TABLE users ADD COLUMN IF NOT EXISTS last_search_pref   TEXT',
        'ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_processed BOOLEAN DEFAULT FALSE',
        'ALTER TABLE users ADD COLUMN IF NOT EXISTS last_payment_id    TEXT',
    ]: await db_pool.execute(s)

    # FIX 1: Ensure active_chats PRIMARY KEY already acts as unique constraint.
    # The table is defined with user_id BIGINT PRIMARY KEY so no extra UNIQUE
    # constraint is needed — but we add a named one defensively if missing.
    await db_pool.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'active_chats_user_id_key'
                  AND conrelid = 'active_chats'::regclass
            ) THEN
                -- Only needed if table was previously created without PK
                NULL;  -- PK already guarantees uniqueness; no extra UNIQUE required
            END IF;
        END$$;
    """)

    logger.info('DB ready')


# ─────────────────────────────────────────────────────────────────────────────
# KEYBOARDS
# ─────────────────────────────────────────────────────────────────────────────

user_keyboard = ReplyKeyboardMarkup(
    [['🚀 Find Partner', '💎 VIP'],
     ['👨 Find Male',    '👩 Find Female'],
     ['⏭️ Next',        '❌ Stop']],
    resize_keyboard=True)

admin_main_keyboard = ReplyKeyboardMarkup(
    [['🚀 Find Partner', '💎 VIP'],
     ['👨 Find Male',    '👩 Find Female'],
     ['⏭️ Next',        '❌ Stop'],
     ['⚙️ Admin Panel']],
    resize_keyboard=True)

admin_panel_keyboard = ReplyKeyboardMarkup(
    [['📊 Analytics'],
     ['📢 Announcement'],
     ['👥 Active Users',  '🕒 Waiting Users'],
     ['👑 VIP Users',     '🧹 Clean Dead Chats'],
     ['🚨 Reports',       '📱 Live Chats'],
     ['⬅️ Back']],
    resize_keyboard=True)

vip_keyboard = ReplyKeyboardMarkup(
    [['🎁 Get FREE VIP', '⭐ Buy VIP'],
     ['⬅️ Back']],
    resize_keyboard=True)

gender_keyboard = ReplyKeyboardMarkup(
    [['Male', 'Female']], resize_keyboard=True, one_time_keyboard=True)

BUTTON_TEXTS = {
    '🚀 Find Partner', '👨 Find Male', '👩 Find Female',
    '⏭️ Next', '❌ Stop', '💎 VIP', '🎁 Get FREE VIP', '⭐ Buy VIP',
    '⬅️ Back', '⚙️ Admin Panel', '⚠️ Report',
    '📊 Analytics', '👥 Active Users', '🕒 Waiting Users',
    '📢 Announcement', '🧹 Clean Dead Chats', '👑 VIP Users',
    '🚨 Reports', '📱 Live Chats',
    'Male', 'Female',
}


# ─────────────────────────────────────────────────────────────────────────────
# INLINE KEYBOARDS
# ─────────────────────────────────────────────────────────────────────────────

def buy_vip_inline(uid: int) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(
            f"⭐ {VIP_PACKAGES['week']['label']} — {VIP_PACKAGES['week']['stars']} Stars",
            callback_data='buy_vip:week')],
        [InlineKeyboardButton(
            f"🌟 {VIP_PACKAGES['month']['label']} — {VIP_PACKAGES['month']['stars']} Stars",
            callback_data='buy_vip:month')],
    ]
    if uid == ADMIN_ID:
        buttons.append([InlineKeyboardButton(
            f"🧪 {VIP_PACKAGES['test']['label']} — {VIP_PACKAGES['test']['stars']} Star",
            callback_data='buy_vip:test')])
    return InlineKeyboardMarkup(buttons)

def report_inline(partner_id):
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton('⚠️ Report', callback_data=f'report:{partner_id}')]])

def tod_inline(uid):
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton('🎲 Truth or Dare', callback_data=f'tod_start:{uid}')]])

def tod_choice_inline(init_uid):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton('😇 Truth', callback_data=f'tod_pick:truth:{init_uid}'),
        InlineKeyboardButton('😈 Dare',  callback_data=f'tod_pick:dare:{init_uid}'),
    ]])

def tod_again_inline(init_uid):
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton('🎲 Another Round', callback_data=f'tod_start:{init_uid}')]])

def soulmate_inline(uid):
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton('💕 Reveal', callback_data=f'soulmate_reveal:{uid}')]])

def get_main_keyboard(uid):
    return admin_main_keyboard if uid == ADMIN_ID else user_keyboard


# ─────────────────────────────────────────────────────────────────────────────
# TRUTH / DARE CONTENT
# ─────────────────────────────────────────────────────────────────────────────

TRUTH_QUESTIONS = [
    "What's the most embarrassing thing you've done for someone you liked? 😳",
    "Have you ever lied to get out of a date?",
    "What's your biggest insecurity?",
    "Have you ever had a crush on someone you shouldn't have?",
    "What's the most childish thing you still do?",
    "Have you ever stalked someone's social media for too long?",
    "What's something you've never told anyone?",
    "What's the worst date you've ever been on?",
    "Have you ever pretended to like something just to impress someone?",
    "What's the most embarrassing thing on your phone right now?",
    "Have you ever cried during a movie? Which one?",
    "What's the weirdest dream you've ever had?",
    "Have you ever ghosted someone? Do you regret it?",
    "What's your biggest fear?",
    "What's a habit you have that you'd be embarrassed if people knew?",
    "Have you ever sent a text to the wrong person?",
]
DARE_CHALLENGES = [
    "Send a voice note saying 'I miss you' as dramatically as possible 🎭",
    "Type your next message with your eyes closed 😄",
    "Describe yourself in exactly 3 emojis",
    "Tell me one thing that made you smile today",
    "Send a voice note laughing for 5 seconds",
    "Tell me your honest first impression of this chat",
    "Type a motivational quote from memory",
    "Send your most used emoji 5 times in a row",
    "Describe your day in one sentence using only questions",
    "Send a voice note saying your name in a funny accent",
    "Tell me the most random fact you know",
]


# ─────────────────────────────────────────────────────────────────────────────
# FIX 8: SPAM FILTER
# ─────────────────────────────────────────────────────────────────────────────

def is_spam(text: str) -> bool:
    """Return True if the message contains a link or Telegram invite."""
    if not text:
        return False
    t = text.lower()
    return any(x in t for x in ['http', 't.me', 'www.', 'telegram.me'])


# ─────────────────────────────────────────────────────────────────────────────
# DB HELPERS
# ─────────────────────────────────────────────────────────────────────────────

async def user_exists(uid):
    return await db_pool.fetchrow('SELECT 1 FROM users WHERE user_id=$1', uid) is not None

async def is_registered(uid):
    return await db_pool.fetchrow(
        '''SELECT 1 FROM users WHERE user_id=$1
           AND name IS NOT NULL AND gender IS NOT NULL
           AND country IS NOT NULL AND age IS NOT NULL''', uid) is not None

async def is_banned_check(uid):
    r = await db_pool.fetchrow('SELECT is_banned FROM users WHERE user_id=$1', uid)
    return bool(r and r['is_banned'])

async def get_partner(uid):
    r = await db_pool.fetchrow('SELECT partner_id FROM active_chats WHERE user_id=$1', uid)
    return r['partner_id'] if r else None

async def check_vip(uid):
    r = await db_pool.fetchrow(
        '''SELECT CASE WHEN is_vip=TRUE AND vip_expiry IS NULL THEN TRUE
                       WHEN vip_expiry > NOW() THEN TRUE ELSE FALSE END AS active
           FROM users WHERE user_id=$1''', uid)
    return bool(r and r['active'])

async def grant_vip(uid, days):
    """Grant VIP — fully parameterised, no f-string SQL."""
    await db_pool.execute(
        """UPDATE users SET is_vip=TRUE,
           vip_expiry = GREATEST(NOW(), COALESCE(vip_expiry, NOW()))
                      + (($2::text) || ' days')::interval
           WHERE user_id=$1""",
        uid, str(int(days)))

async def handle_referral(new_uid, referrer_uid):
    if referrer_uid == new_uid or not await user_exists(referrer_uid):
        return False
    r = await db_pool.fetchrow(
        'UPDATE users SET referral_count=referral_count+1 WHERE user_id=$1 RETURNING referral_count',
        referrer_uid)
    if r and r['referral_count'] % VIP_REFERRAL_THRESHOLD == 0:
        await grant_vip(referrer_uid, VIP_REFERRAL_DAYS)
        return True
    return False

async def get_registration_step(uid):
    r = await db_pool.fetchrow('SELECT name,gender,country,age FROM users WHERE user_id=$1', uid)
    if not r: return None
    if r['name']    is None: return 'name'
    if r['gender']  is None: return 'gender'
    if r['country'] is None: return 'country'
    if r['age']     is None: return 'age'
    return None

def cancel_invite_timer(context):
    t = context.user_data.pop('invite_task', None)
    if t and not t.done(): t.cancel()

async def log_message(sender_id, partner_id, text):
    await db_pool.execute(
        'INSERT INTO chat_logs(sender_id,partner_id,message) VALUES($1,$2,$3)',
        sender_id, partner_id, text[:500])
    # Keep only last 10 messages per pair
    await db_pool.execute(
        '''DELETE FROM chat_logs WHERE id IN (
               SELECT id FROM chat_logs
               WHERE (sender_id=$1 AND partner_id=$2) OR (sender_id=$2 AND partner_id=$1)
               ORDER BY created_at DESC OFFSET 10)''',
        sender_id, partner_id)

# Tod count lives in user_data (per-user, cleared on chat end)
def get_tod_count(context):      return context.user_data.get('tod_count', 0)
def increment_tod_count(context): context.user_data['tod_count'] = context.user_data.get('tod_count', 0) + 1
def clear_tod_count(context):    context.user_data.pop('tod_count', None)


# ─────────────────────────────────────────────────────────────────────────────
# BACKGROUND TASKS  (FIX 4, 5, 6)
# ─────────────────────────────────────────────────────────────────────────────

async def cleanup_waiting_queue():
    """FIX 4: Remove stale waiting entries older than 5 min every 2 min."""
    while True:
        try:
            deleted = await db_pool.fetchval(
                """WITH d AS (
                       DELETE FROM waiting_users
                       WHERE queued_at < NOW() - INTERVAL '5 minutes'
                       RETURNING 1)
                   SELECT COUNT(*) FROM d""")
            if deleted:
                logger.info('Waiting queue: removed %d stale entries', deleted)
        except Exception as e:
            logger.warning('Waiting queue cleanup error: %s', e)
        await asyncio.sleep(120)

async def cleanup_chat_logs():
    """FIX 5: Keep only newest 10 000 chat-log rows, run every 10 min."""
    while True:
        try:
            await db_pool.execute(
                """DELETE FROM chat_logs
                   WHERE id NOT IN (
                       SELECT id FROM chat_logs
                       ORDER BY created_at DESC
                       LIMIT 10000)""")
        except Exception as e:
            logger.warning('Chat log cleanup error: %s', e)
        await asyncio.sleep(600)

async def cleanup_stale_reveals():
    """Remove soulmate reveals older than 10 min (partner never tapped)."""
    while True:
        try:
            await db_pool.execute(
                "DELETE FROM soulmate_reveals WHERE created_at < NOW() - INTERVAL '10 minutes'")
        except Exception as e:
            logger.warning('Soulmate reveal cleanup error: %s', e)
        await asyncio.sleep(300)

async def on_startup(app):
    """FIX 6: Launch background maintenance tasks once bot is ready."""
    asyncio.create_task(cleanup_waiting_queue())
    asyncio.create_task(cleanup_chat_logs())
    asyncio.create_task(cleanup_stale_reveals())
    logger.info('Background maintenance tasks started')


# ─────────────────────────────────────────────────────────────────────────────
# FIX 2 + 3: ATOMIC MATCHING — VIP priority + invite memory-leak fix
# ─────────────────────────────────────────────────────────────────────────────

async def match_user(update, context, pref=None):
    uid = update.message.from_user.id

    # Guard: already paired
    if await get_partner(uid):
        await update.message.reply_text('Already in a chat. Use ❌ Stop first.')
        return

    # FIX 3: cancel existing invite timer BEFORE creating a new one (prevents leak)
    cancel_invite_timer(context)

    r = await db_pool.fetchrow('SELECT gender FROM users WHERE user_id=$1', uid)
    my_gender = r['gender'] if r else None
    partner = None

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            # Remove self from queue atomically inside the transaction
            await conn.execute('DELETE FROM waiting_users WHERE user_id=$1', uid)

            # FIX 2: ORDER BY u.is_vip DESC, w.queued_at — VIPs matched first.
            # FOR UPDATE OF w SKIP LOCKED — race-free, no double-match possible.
            if pref:
                row = await conn.fetchrow(
                    '''SELECT w.user_id FROM waiting_users w
                       JOIN users u ON w.user_id = u.user_id
                       WHERE u.gender = $1
                         AND (w.preferred_gender IS NULL OR w.preferred_gender = $2)
                         AND w.user_id != $3
                         AND u.is_banned = FALSE
                         AND u.name    IS NOT NULL
                         AND u.gender  IS NOT NULL
                         AND u.country IS NOT NULL
                         AND u.age     IS NOT NULL
                       ORDER BY u.is_vip DESC, w.queued_at
                       LIMIT 1 FOR UPDATE OF w SKIP LOCKED''',
                    pref, my_gender, uid)
            else:
                row = await conn.fetchrow(
                    '''SELECT w.user_id FROM waiting_users w
                       JOIN users u ON w.user_id = u.user_id
                       WHERE w.user_id != $1
                         AND (w.preferred_gender IS NULL OR w.preferred_gender = $2)
                         AND u.is_banned = FALSE
                         AND u.name    IS NOT NULL
                         AND u.gender  IS NOT NULL
                         AND u.country IS NOT NULL
                         AND u.age     IS NOT NULL
                       ORDER BY u.is_vip DESC, w.queued_at
                       LIMIT 1 FOR UPDATE OF w SKIP LOCKED''',
                    uid, my_gender)

            if row:
                partner = row['user_id']
                await conn.execute('DELETE FROM waiting_users WHERE user_id=$1', partner)
                # Wipe any stale rows before inserting fresh pair
                await conn.execute('DELETE FROM active_chats WHERE user_id=$1', uid)
                await conn.execute('DELETE FROM active_chats WHERE user_id=$1', partner)
                await conn.execute('INSERT INTO active_chats VALUES($1,$2)', uid, partner)
                await conn.execute('INSERT INTO active_chats VALUES($1,$2)', partner, uid)

    if partner:
        clear_tod_count(context)
        await context.bot.send_message(uid,
            '✅ You\'re now connected with a stranger!\n\n'
            '💬 Say hi and start chatting 👋\n'
            '🔒 Remember: Be kind & respectful')
        await context.bot.send_message(partner,
            '✅ You\'re now connected with a stranger!\n\n'
            '💬 Say hi and start chatting 👋\n'
            '🔒 Remember: Be kind & respectful')
        await context.bot.send_message(uid,
            '🎲 Want to make this chat more fun?\nPlay Truth or Dare with your partner 👇',
            reply_markup=tod_inline(uid))
        await context.bot.send_message(partner,
            '🎲 Want to make this chat more fun?\nPlay Truth or Dare with your partner 👇',
            reply_markup=tod_inline(partner))
    else:
        # Put self in queue
        context.user_data['last_pref'] = pref
        await db_pool.execute('UPDATE users SET last_search_pref=$1 WHERE user_id=$2', pref, uid)
        await db_pool.execute(
            '''INSERT INTO waiting_users(user_id, preferred_gender) VALUES($1,$2)
               ON CONFLICT(user_id) DO UPDATE SET preferred_gender=EXCLUDED.preferred_gender,
                                                  queued_at=NOW()''',
            uid, pref)
        label = f'Searching for a {pref} partner...' if pref else 'Searching for a partner...'
        await update.message.reply_text(
            f'🔎 {label}\n\n⏳ Please wait...\nPress ❌ Stop anytime to cancel.')

        # FIX 3: explicit cancel + recreate — prevents dangling task memory leak
        old_task = context.user_data.pop('invite_task', None)
        if old_task and not old_task.done():
            old_task.cancel()

        async def invite_prompt():
            try:
                await asyncio.sleep(MATCH_INVITE_DELAY)
                if await db_pool.fetchrow('SELECT 1 FROM waiting_users WHERE user_id=$1', uid):
                    link = f'https://t.me/{context.bot.username}?start={uid}'
                    await context.bot.send_message(uid,
                        f'🔎 Still searching?\n\nInvite {VIP_REFERRAL_THRESHOLD} friends and unlock '
                        f'👑 VIP for {VIP_REFERRAL_DAYS} days!\n\nYour link:\n{link}')
            except asyncio.CancelledError:
                pass
            finally:
                context.user_data.pop('invite_task', None)

        context.user_data['invite_task'] = asyncio.create_task(invite_prompt())


# ─────────────────────────────────────────────────────────────────────────────
# STOP CHAT
# ─────────────────────────────────────────────────────────────────────────────

async def stop_chat(update, context):
    uid = update.message.from_user.id
    await db_pool.execute('DELETE FROM waiting_users WHERE user_id=$1', uid)
    cancel_invite_timer(context)

    partner = await get_partner(uid)
    if not partner:
        await update.message.reply_text(
            '⛔ Search stopped.\n\nPress 🚀 Find Partner whenever you\'re ready!',
            reply_markup=get_main_keyboard(uid))
        return False

    await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', uid)
    await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', partner)
    clear_tod_count(context)
    await db_pool.execute(
        'DELETE FROM soulmate_reveals WHERE user_id=$1 OR user_id=$2', uid, partner)

    await update.message.reply_text(
        '👋 You left the chat.\n\n💭 Hope you had a good conversation!\n'
        'Want to meet someone new?\n\nPress 🚀 Find Partner to continue chatting.',
        reply_markup=get_main_keyboard(uid))
    await update.message.reply_text('Was there a problem?', reply_markup=report_inline(partner))

    try:
        await context.bot.send_message(partner,
            '👋 Your partner left the chat.\n\n💭 Every stranger is a new adventure!\n'
            'Want to meet someone new?\n\nPress 🚀 Find Partner to continue chatting.',
            reply_markup=get_main_keyboard(partner))
        await context.bot.send_message(partner, 'Was there a problem?',
            reply_markup=report_inline(uid))
    except Exception as e:
        logger.warning('Could not notify partner %s: %s', partner, e)
    return True


# ─────────────────────────────────────────────────────────────────────────────
# ADMIN UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

async def clean_dead_chats(bot):
    rows = await db_pool.fetch('SELECT DISTINCT user_id, partner_id FROM active_chats')
    seen, cleaned = set(), 0
    for row in rows:
        uid, partner = row['user_id'], row['partner_id']
        pk = tuple(sorted((uid, partner)))
        if pk in seen: continue
        seen.add(pk)
        ua = pa = False
        try: await bot.send_chat_action(chat_id=uid,     action='typing'); ua = True
        except: pass
        try: await bot.send_chat_action(chat_id=partner, action='typing'); pa = True
        except: pass
        if not ua or not pa:
            await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', uid)
            await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', partner)
            if not ua: await db_pool.execute('DELETE FROM waiting_users WHERE user_id=$1', uid)
            if not pa: await db_pool.execute('DELETE FROM waiting_users WHERE user_id=$1', partner)
            cleaned += 1
            if ua:
                try: await bot.send_message(uid, '⚠️ Partner disconnected. Press 🚀 Find Partner.')
                except: pass
            if pa:
                try: await bot.send_message(partner, '⚠️ Partner disconnected. Press 🚀 Find Partner.')
                except: pass
    return cleaned


# ─────────────────────────────────────────────────────────────────────────────
# ADMIN COMMANDS
# ─────────────────────────────────────────────────────────────────────────────

async def cleanchats_command(update, context):
    if update.effective_user.id != ADMIN_ID: return
    await update.message.reply_text('🔍 Scanning...')
    n = await clean_dead_chats(context.bot)
    await update.message.reply_text(f'✅ Removed {n} dead chat(s).')

async def broadcast(update, context):
    if update.effective_user.id != ADMIN_ID: return
    if not context.args: await update.message.reply_text('Usage: /broadcast <msg>'); return
    msg = ' '.join(context.args)
    rows = await db_pool.fetch(
        'SELECT user_id FROM users WHERE is_banned=FALSE AND name IS NOT NULL AND age IS NOT NULL')
    sent = 0
    for r in rows:
        try:
            await context.bot.send_message(r['user_id'], msg)
            sent += 1; await asyncio.sleep(0.05)
        except Exception as e:
            if 'Retry After' in str(e) or 'retry_after' in str(e).lower():
                import re as _re
                wait = int(_re.search(r'\d+', str(e)).group() or 5)
                await asyncio.sleep(wait)
                try: await context.bot.send_message(r['user_id'], msg); sent += 1
                except: pass
    await update.message.reply_text(f'✅ Sent to {sent} users')

async def handle_ban(update, context):
    if update.effective_user.id != ADMIN_ID: return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text('Usage: /ban <id> or /unban <id>'); return
    tid = int(context.args[0]); unban = update.message.text.startswith('/unban')
    if not await user_exists(tid): await update.message.reply_text(f'❌ User {tid} not found.'); return
    if unban:
        await db_pool.execute('UPDATE users SET is_banned=FALSE WHERE user_id=$1', tid)
        await update.message.reply_text(f'✅ {tid} unbanned.')
        try: await context.bot.send_message(tid, '✅ Your ban has been lifted.')
        except: pass
    else:
        await db_pool.execute('UPDATE users SET is_banned=TRUE WHERE user_id=$1', tid)
        p = await get_partner(tid)
        if p:
            await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', tid)
            await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', p)
            try: await context.bot.send_message(p, '👋 Stranger left the chat.')
            except: pass
        await db_pool.execute('DELETE FROM waiting_users WHERE user_id=$1', tid)
        await update.message.reply_text(f'🚫 {tid} banned.')
        try: await context.bot.send_message(tid, '🚫 You have been banned.')
        except: pass

async def cleanup_null_users(update, context):
    if update.effective_user.id != ADMIN_ID: return
    rows = await db_pool.fetch(
        'SELECT user_id FROM users WHERE (name IS NULL OR gender IS NULL OR country IS NULL OR age IS NULL) AND user_id!=$1',
        ADMIN_ID)
    if not rows: await update.message.reply_text('✅ No incomplete registrations.'); return
    for r in rows:
        uid = r['user_id']
        for q in [
            'DELETE FROM waiting_users WHERE user_id=$1',
            'DELETE FROM active_chats  WHERE user_id=$1',
            'DELETE FROM active_chats  WHERE partner_id=$1',
            'DELETE FROM reports       WHERE reporter_id=$1 OR reported_id=$1',
            'DELETE FROM chat_logs     WHERE sender_id=$1 OR partner_id=$1',
            'DELETE FROM users         WHERE user_id=$1',
        ]: await db_pool.execute(q, uid)
    await update.message.reply_text(f'🗑 Removed {len(rows)} incomplete user(s).')

async def regrant_vip_command(update, context):
    if update.effective_user.id != ADMIN_ID: return
    rows = await db_pool.fetch(
        'SELECT user_id,referral_count FROM users WHERE referral_count>=$1 AND (is_vip=FALSE OR vip_expiry<NOW()) AND user_id!=$2',
        VIP_REFERRAL_THRESHOLD, ADMIN_ID)
    if not rows: await update.message.reply_text('✅ No users need re-grant.'); return
    for r in rows:
        days = (r['referral_count'] // VIP_REFERRAL_THRESHOLD) * VIP_REFERRAL_DAYS
        await grant_vip(r['user_id'], days)
        try: await context.bot.send_message(r['user_id'], f'🎉 VIP renewed for {days} days!')
        except: pass
    await update.message.reply_text(f'✅ Re-granted VIP to {len(rows)} user(s).')

async def fixvip_command(update, context):
    if update.effective_user.id != ADMIN_ID: return
    rows = await db_pool.fetch(
        'SELECT user_id,referral_count FROM users WHERE referral_count>=$1 AND is_vip=FALSE AND user_id!=$2',
        VIP_REFERRAL_THRESHOLD, ADMIN_ID)
    if not rows: await update.message.reply_text('✅ No users need VIP fixing.'); return
    for r in rows:
        days = (r['referral_count'] // VIP_REFERRAL_THRESHOLD) * VIP_REFERRAL_DAYS
        await grant_vip(r['user_id'], days)
        try: await context.bot.send_message(r['user_id'], f'🎉 VIP activated for {days} days!')
        except: pass
    await update.message.reply_text(f'✅ Fixed VIP for {len(rows)} user(s).')

async def update_command(update, context):
    if update.effective_user.id != ADMIN_ID: return
    rows = await db_pool.fetch(
        'SELECT user_id FROM users WHERE (name IS NULL OR gender IS NULL OR country IS NULL OR age IS NULL) AND user_id!=$1',
        ADMIN_ID)
    if not rows: await update.message.reply_text('✅ All users fully registered.'); return
    sent = 0
    for r in rows:
        uid = r['user_id']; step = await get_registration_step(uid)
        try:
            if   step == 'name':    await context.bot.send_message(uid, '👋 Complete your profile.\n\nEnter your name:',    reply_markup=ReplyKeyboardRemove())
            elif step == 'gender':  await context.bot.send_message(uid, '👋 Complete your profile.\n\nSelect your gender:', reply_markup=gender_keyboard)
            elif step == 'country': await context.bot.send_message(uid, '👋 Complete your profile.\n\nEnter your country:', reply_markup=ReplyKeyboardRemove())
            elif step == 'age':     await context.bot.send_message(uid, '👋 Complete your profile.\n\nEnter your age (16–60):', reply_markup=ReplyKeyboardRemove())
            sent += 1; await asyncio.sleep(0.05)
        except Exception as e: logger.warning('nudge failed uid=%s: %s', uid, e)
    await update.message.reply_text(f'✅ Sent to {sent}/{len(rows)} incomplete user(s).')

async def vipfemales_command(update, context):
    if update.effective_user.id != ADMIN_ID: return
    rows = await db_pool.fetch("SELECT user_id FROM users WHERE gender='Female' AND is_banned=FALSE")
    if not rows: await update.message.reply_text('❌ No female users found.'); return
    sent = 0; failed = 0
    for r in rows:
        await grant_vip(r['user_id'], 7)
        try:
            await context.bot.send_message(r['user_id'],
                '👑 You just got 7 days of FREE VIP!\n\n🎉 As a VIP you can now:\n'
                '• Filter matches by gender\n• Unlimited Truth or Dare rounds\n\n'
                'Enjoy your VIP! 💎\n\nPress 🚀 Find Partner to start chatting!')
            sent += 1; await asyncio.sleep(0.05)
        except: failed += 1
    await update.message.reply_text(
        f'👑 Done!\n\n✅ Granted + notified: {sent}\n🚫 Could not notify: {failed}\n📊 Total: {len(rows)}')

async def delete_blocked_command(update, context):
    if update.effective_user.id != ADMIN_ID: return
    await update.message.reply_text('🔍 Scanning for blocked users...')
    rows = await db_pool.fetch('SELECT user_id FROM users WHERE user_id!=$1', ADMIN_ID)
    deleted = 0; checked = 0
    for r in rows:
        uid = r['user_id']; checked += 1
        try:
            await context.bot.send_chat_action(chat_id=uid, action='typing')
            await asyncio.sleep(0.05)
        except Forbidden:
            for q in [
                'DELETE FROM waiting_users WHERE user_id=$1',
                'DELETE FROM active_chats  WHERE user_id=$1',
                'DELETE FROM active_chats  WHERE partner_id=$1',
                'DELETE FROM reports       WHERE reporter_id=$1 OR reported_id=$1',
                'DELETE FROM chat_logs     WHERE sender_id=$1 OR partner_id=$1',
                'DELETE FROM users         WHERE user_id=$1',
            ]: await db_pool.execute(q, uid)
            deleted += 1
        except: pass
    await update.message.reply_text(
        f'✅ Done!\n\n🔍 Checked: {checked}\n🗑 Deleted: {deleted}\n👤 Remaining: {checked-deleted}')

async def nudge_chats_command(update, context):
    if update.effective_user.id != ADMIN_ID: return
    pairs = await db_pool.fetch(
        'SELECT DISTINCT LEAST(user_id,partner_id) as u1, GREATEST(user_id,partner_id) as u2 FROM active_chats')
    if not pairs: await update.message.reply_text('❌ No active chats right now.'); return
    icebreakers = [
        '💬 Icebreaker: "What\'s one thing that always makes you smile?"',
        '💬 Icebreaker: "What\'s the best thing that happened to you this week?"',
        '💬 Icebreaker: "If you could be anywhere right now, where would you be?"',
        '💬 Icebreaker: "What\'s something most people don\'t know about you?"',
        '💬 Icebreaker: "What\'s your go-to comfort when you\'re having a bad day?"',
    ]
    sent = 0
    for pair in pairs:
        msg = random.choice(icebreakers)
        for uid in [pair['u1'], pair['u2']]:
            try: await context.bot.send_message(uid, msg); await asyncio.sleep(0.05)
            except: pass
        sent += 1
    await update.message.reply_text(f'💬 Sent icebreakers to {sent} chat(s)!')

async def reset_db_command(update, context):
    if update.effective_user.id != ADMIN_ID: return
    if not context.args or context.args[0] != 'CONFIRM':
        await update.message.reply_text('⚠️ WARNING: Deletes ALL data!\n\nType /resetdb CONFIRM'); return
    for table in ['chat_logs','reports','active_chats','waiting_users','soulmate_reveals','users']:
        try: await db_pool.execute(f'DELETE FROM {table}')
        except: pass
    await db_pool.execute(
        "INSERT INTO users(user_id,username,name,gender,is_vip) VALUES($1,$2,'Admin','Male',TRUE) ON CONFLICT DO NOTHING",
        ADMIN_ID, 'Admin')
    await update.message.reply_text('✅ Database reset complete!')

async def debug_referral(update, context):
    if update.effective_user.id != ADMIN_ID: return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text('Usage: /debugref <id>'); return
    r = await db_pool.fetchrow(
        'SELECT user_id,name,gender,country,age,referred_by,referral_count,is_vip,vip_expiry FROM users WHERE user_id=$1',
        int(context.args[0]))
    if not r: await update.message.reply_text('Not found.'); return
    await update.message.reply_text(
        f"🔍 Debug\n\nname:{r['name']} gender:{r['gender']} country:{r['country']}\n"
        f"age:{r['age']} ref_by:{r['referred_by']} ref_cnt:{r['referral_count']}\n"
        f"vip:{r['is_vip']} expiry:{r['vip_expiry']}")

async def stats_command(update, context):
    if update.effective_user.id != ADMIN_ID: return
    total  = await db_pool.fetchval('SELECT COUNT(*) FROM users')
    male   = await db_pool.fetchval("SELECT COUNT(*) FROM users WHERE gender='Male'")
    female = await db_pool.fetchval("SELECT COUNT(*) FROM users WHERE gender='Female'")
    incomp = await db_pool.fetchval('SELECT COUNT(*) FROM users WHERE name IS NULL OR gender IS NULL OR country IS NULL OR age IS NULL')
    active = await db_pool.fetchval('SELECT COUNT(DISTINCT LEAST(user_id,partner_id)) FROM active_chats') or 0
    wait   = await db_pool.fetchval('SELECT COUNT(*) FROM waiting_users')
    vips   = await db_pool.fetchval("SELECT COUNT(*) FROM users WHERE (is_vip=TRUE AND vip_expiry IS NULL) OR vip_expiry>NOW()")
    vip_m  = await db_pool.fetchval("SELECT COUNT(*) FROM users WHERE gender='Male'   AND ((is_vip=TRUE AND vip_expiry IS NULL) OR vip_expiry>NOW())")
    vip_f  = await db_pool.fetchval("SELECT COUNT(*) FROM users WHERE gender='Female' AND ((is_vip=TRUE AND vip_expiry IS NULL) OR vip_expiry>NOW())")
    banned = await db_pool.fetchval('SELECT COUNT(*) FROM users WHERE is_banned=TRUE')
    reps   = await db_pool.fetchval('SELECT COUNT(*) FROM reports')
    msgs   = await db_pool.fetchval('SELECT COALESCE(SUM(total_messages),0) FROM users')
    today  = await db_pool.fetchval("SELECT COUNT(*) FROM users WHERE created_at>NOW()-INTERVAL '24 hours'")
    week   = await db_pool.fetchval("SELECT COUNT(*) FROM users WHERE created_at>NOW()-INTERVAL '7 days'")
    top    = await db_pool.fetch('SELECT name,username,referral_count FROM users WHERE referral_count>0 ORDER BY referral_count DESC LIMIT 5')
    mp = round(male/total*100)   if total else 0
    fp = round(female/total*100) if total else 0
    im = await db_pool.fetchval("SELECT COUNT(*) FROM users u WHERE u.gender='Male'   AND EXISTS(SELECT 1 FROM active_chats ac WHERE ac.user_id=u.user_id)")
    if_ = await db_pool.fetchval("SELECT COUNT(*) FROM users u WHERE u.gender='Female' AND EXISTS(SELECT 1 FROM active_chats ac WHERE ac.user_id=u.user_id)")
    sm = await db_pool.fetchval("SELECT COUNT(*) FROM users u WHERE u.gender='Male'   AND EXISTS(SELECT 1 FROM waiting_users w WHERE w.user_id=u.user_id)")
    sf = await db_pool.fetchval("SELECT COUNT(*) FROM users u WHERE u.gender='Female' AND EXISTS(SELECT 1 FROM waiting_users w WHERE w.user_id=u.user_id)")
    lines = [
        '📊 Full Stats\n',
        f'👤 Total:      {total}',
        f'👨 Male:       {male} ({mp}%)',
        f'👩 Female:     {female} ({fp}%)',
        f'❓ Incomplete: {incomp}',
        f'📅 Today:      {today}',
        f'📆 Week:       {week}',
        f'\n💬 Active chats: {active}',
        f'🔎 Waiting:      {wait}',
        f'👨💬 Chat males:   {im}',
        f'👩💬 Chat females: {if_}',
        f'👨🔎 Search males:   {sm}',
        f'👩🔎 Search females: {sf}',
        f'📝 Messages:     {msgs}',
        f'\n👑 VIPs:     {vips}',
        f'👨👑 Male VIPs:   {vip_m}',
        f'👩👑 Female VIPs: {vip_f}',
        f'🚫 Banned:   {banned}',
        f'🚨 Reports:  {reps}',
    ]
    if top:
        lines.append('\n🏆 Top Referrers:')
        for r in top:
            uname = f"@{r['username']}" if r['username'] else 'no username'
            lines.append(f"  {r['name']} ({uname}) — {r['referral_count']} refs")
    await update.message.reply_text('\n'.join(lines))


# ─────────────────────────────────────────────────────────────────────────────
# CALLBACK QUERY HANDLERS
# ─────────────────────────────────────────────────────────────────────────────

async def report_callback(update, context):
    q = update.callback_query; uid = q.from_user.id
    await q.answer()
    try: partner = int(q.data.split(':')[1])
    except: await q.edit_message_text('⚠️ Invalid report.'); return
    existing = await db_pool.fetchrow(
        "SELECT 1 FROM reports WHERE reporter_id=$1 AND reported_id=$2 AND created_at>NOW()-INTERVAL '1 hour'",
        uid, partner)
    if existing: await q.edit_message_text('You already reported this user recently.'); return
    await db_pool.execute('INSERT INTO reports(reporter_id,reported_id) VALUES($1,$2)', uid, partner)
    await q.edit_message_text('✅ Report submitted. Our team will review it shortly.')

async def tod_callback(update, context):
    q = update.callback_query; uid = q.from_user.id; data = q.data

    if data.startswith('tod_start:'):
        init_uid = int(data.split(':')[1])
        # Only the user whose ID is encoded can start a round
        if uid != init_uid: await q.answer('Not your button!', show_alert=True); return
        await q.answer()
        partner = await get_partner(uid)
        if not partner: await q.edit_message_text('⚠️ Not in a chat.'); return
        count  = get_tod_count(context)
        is_vip = uid == ADMIN_ID or await check_vip(uid)
        if not is_vip and count >= FREE_TOD_LIMIT:
            await q.answer(f'Free users get {FREE_TOD_LIMIT} rounds. Get 💎 VIP for unlimited!',
                           show_alert=True); return
        await q.edit_message_text('🎲 You challenged your partner!')
        # LOGIC CHECK: send choice buttons to the PARTNER, not the challenger
        try:
            await context.bot.send_message(partner,
                '🎲 Stranger challenged you!\n\nYou pick:',
                reply_markup=tod_choice_inline(uid))   # uid = challenger (init_uid)
        except: pass

    elif data.startswith('tod_pick:'):
        await q.answer()
        parts    = data.split(':')
        choice   = parts[1]          # 'truth' or 'dare'
        init_uid = int(parts[2])     # the challenger who sent tod_start

        # LOGIC CHECK: uid here is the PARTNER who received the choice buttons.
        # Their partner must be init_uid.
        partner = await get_partner(uid)
        if not partner or partner != init_uid:
            await q.edit_message_text('⚠️ Chat ended.'); return

        msg = (f'😇 *Truth:*\n\n{random.choice(TRUTH_QUESTIONS)}' if choice == 'truth'
               else f'😈 *Dare:*\n\n{random.choice(DARE_CHALLENGES)}')

        # Increment count for the challenger (they pay the ToD cost)
        increment_tod_count(context)

        again = tod_again_inline(init_uid)
        try: await q.edit_message_reply_markup(reply_markup=None)
        except: pass
        # Send result to BOTH participants
        try: await context.bot.send_message(uid,      msg, parse_mode='Markdown', reply_markup=again)
        except: pass
        try: await context.bot.send_message(init_uid, msg, parse_mode='Markdown', reply_markup=again)
        except: pass

async def buy_vip_callback(update, context):
    q = update.callback_query; uid = q.from_user.id
    await q.answer()
    parts   = q.data.split(':'); pkg_key = parts[1]
    pkg     = VIP_PACKAGES.get(pkg_key)
    if not pkg: await q.edit_message_text('⚠️ Invalid package.'); return
    if pkg_key == 'test' and uid != ADMIN_ID: await q.answer('Not available.', show_alert=True); return
    descs = {
        'week':  'WHAT YOU GET:\n• Match with Males or Females only\n• Unlimited Truth or Dare\n• Top priority matching\n• VIP badge visible to partners\n\nDuration: 7 days\nActivates instantly after payment',
        'month': 'WHAT YOU GET:\n• Match with Males or Females only\n• Unlimited Truth or Dare\n• Top priority matching\n• VIP badge visible to partners\n\nDuration: 30 days — Best Value!\nActivates instantly after payment',
        'test':  'Admin test payment — 1 Star only',
    }
    try:
        await context.bot.send_invoice(
            chat_id=uid,
            title=f"{pkg['emoji']} {pkg['label']} — Fun Bot",
            description=descs.get(pkg_key, ''),
            payload=f"vip_{pkg_key}",          # Canonical: 'vip_week', 'vip_month', 'vip_test'
            provider_token='',
            currency='XTR',
            prices=[{'label': pkg['label'], 'amount': pkg['stars']}])
        await q.edit_message_text(
            f"{pkg['emoji']} Invoice sent!\n\nComplete the payment to activate your {pkg['label']}.")
    except Exception as e:
        logger.error('Invoice error %s: %s', uid, e)
        await q.edit_message_text('⚠️ Could not create invoice. Try again later.')

async def admin_report_callback(update, context):
    q = update.callback_query
    if q.from_user.id != ADMIN_ID: await q.answer('Not authorized.', show_alert=True); return
    await q.answer()
    parts = q.data.split(':')
    if len(parts) < 3: await q.edit_message_text('⚠️ Invalid.'); return
    reported_id = int(parts[1]); reporter_id = int(parts[2])
    ur = await db_pool.fetchrow('SELECT name,username,is_banned FROM users WHERE user_id=$1', reported_id)
    name     = (ur['name']     or 'Unknown')     if ur else 'Unknown'
    username = (ur['username'] or 'no username') if ur else 'no username'
    banned   = ur['is_banned'] if ur else False
    logs = await db_pool.fetch(
        '''SELECT sender_id,message,created_at FROM chat_logs
           WHERE (sender_id=$1 AND partner_id=$2) OR (sender_id=$2 AND partner_id=$1)
           ORDER BY created_at DESC LIMIT 10''', reported_id, reporter_id)
    lines = []
    for log in reversed(logs):
        who = '🔴 Reported' if log['sender_id'] == reported_id else '🟢 Reporter'
        lines.append(f"{who} [{log['created_at'].strftime('%H:%M')}]: {log['message']}")
    history = '\n'.join(lines) if lines else 'No messages yet.'
    total   = await db_pool.fetchval('SELECT COUNT(*) FROM reports WHERE reported_id=$1', reported_id)
    msg = (f'🚨 Report Review\n\nReported: {name} (@{username})\nID: {reported_id}\n'
           f'Total: {total}\nBanned: {chr(0x2705)+" Yes" if banned else chr(0x274c)+" No"}\n\n'
           f'📝 Last messages:\n{"─"*28}\n{history}')
    action = (InlineKeyboardButton('✅ Unban', callback_data=f'admin_ban:unban:{reported_id}')
              if banned else InlineKeyboardButton('🚫 Ban', callback_data=f'admin_ban:ban:{reported_id}'))
    markup = InlineKeyboardMarkup([[action,
        InlineKeyboardButton('🗑 Delete Report', callback_data=f'admin_del_report:{reported_id}'),
        InlineKeyboardButton('🔙 Back', callback_data='admin_back_reports')]])
    await q.edit_message_text(msg, reply_markup=markup)

async def admin_ban_callback(update, context):
    q = update.callback_query
    if q.from_user.id != ADMIN_ID: await q.answer('Not authorized.', show_alert=True); return
    await q.answer()
    parts = q.data.split(':'); action = parts[1]; tid = int(parts[2])
    if action == 'ban':
        await db_pool.execute('UPDATE users SET is_banned=TRUE WHERE user_id=$1', tid)
        p = await get_partner(tid)
        if p:
            await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', tid)
            await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', p)
            try: await context.bot.send_message(p, '👋 Stranger left the chat.')
            except: pass
        await db_pool.execute('DELETE FROM waiting_users WHERE user_id=$1', tid)
        try: await context.bot.send_message(tid, '🚫 You have been banned.')
        except: pass
        await q.edit_message_text(f'✅ User {tid} banned.')
    else:
        await db_pool.execute('UPDATE users SET is_banned=FALSE WHERE user_id=$1', tid)
        try: await context.bot.send_message(tid, '✅ Your ban has been lifted.')
        except: pass
        await q.edit_message_text(f'✅ User {tid} unbanned.')

async def admin_del_report_callback(update, context):
    q = update.callback_query
    if q.from_user.id != ADMIN_ID: await q.answer('Not authorized.', show_alert=True); return
    await q.answer()
    reported_id = int(q.data.split(':')[1])
    deleted = await db_pool.fetchval('SELECT COUNT(*) FROM reports WHERE reported_id=$1', reported_id)
    await db_pool.execute('DELETE FROM reports WHERE reported_id=$1', reported_id)
    await q.edit_message_text(f'✅ Deleted {deleted} report(s) for user {reported_id}.')

async def admin_back_reports_callback(update, context):
    q = update.callback_query
    if q.from_user.id != ADMIN_ID: return
    await q.answer()
    rows = await db_pool.fetch(
        '''SELECT r.reporter_id, r.reported_id, u.name, u.username,
                  (SELECT COUNT(*) FROM reports WHERE reported_id=r.reported_id) AS total
           FROM reports r LEFT JOIN users u ON u.user_id=r.reported_id
           ORDER BY r.created_at DESC LIMIT 10''')
    if not rows: await q.edit_message_text('✅ No reports.'); return
    buttons = [[InlineKeyboardButton(
        f"🚨 {r['name'] or 'Unknown'} ({r['total']} reports) — ID:{r['reported_id']}",
        callback_data=f"admin_report:{r['reported_id']}:{r['reporter_id']}")] for r in rows]
    await q.edit_message_text('🚨 Recent Reports:', reply_markup=InlineKeyboardMarkup(buttons))

async def admin_end_chat_callback(update, context):
    q = update.callback_query
    if q.from_user.id != ADMIN_ID: await q.answer('Not authorized.', show_alert=True); return
    await q.answer()
    parts = q.data.split(':'); u1, u2 = int(parts[1]), int(parts[2])
    await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', u1)
    await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', u2)
    end_msg = '❌ Your chat has been ended by the admin.\n\nPress 🚀 Find Partner to start a new chat.'
    for uid in [u1, u2]:
        try: await context.bot.send_message(uid, end_msg)
        except: pass
    await q.edit_message_text(f'✅ Chat between {u1} and {u2} ended.')

async def find_new_callback(update, context):
    await update.callback_query.answer('Press 🚀 Find Partner to search!', show_alert=False)

async def announce_target_callback(update, context):
    q = update.callback_query
    if q.from_user.id != ADMIN_ID: await q.answer('Not authorized.', show_alert=True); return
    await q.answer()
    target = q.data.split(':')[1]
    context.user_data['announce_mode']   = True
    context.user_data['announce_target'] = target
    label = '👩 females only' if target == 'female' else '👥 all users'
    await q.edit_message_text(f'Sending to {label}.\n\nNow send your announcement message:')

async def soulmate_reveal_callback(update, context):
    q = update.callback_query; uid = q.from_user.id
    await q.answer()

    partner = await get_partner(uid)
    if not partner:
        await q.edit_message_text('⚠️ You are not in a chat anymore.'); return

    # Register this user's reveal (upsert ensures idempotency)
    await db_pool.execute(
        '''INSERT INTO soulmate_reveals(user_id, partner_id) VALUES($1,$2)
           ON CONFLICT(user_id) DO UPDATE SET partner_id=$2, created_at=NOW()''',
        uid, partner)
    await q.edit_message_text('✅ You tapped Reveal!\n\nWaiting for your partner...')

    # Check whether partner has also revealed (must reference this specific uid as their partner)
    partner_row = await db_pool.fetchrow(
        'SELECT 1 FROM soulmate_reveals WHERE user_id=$1 AND partner_id=$2',
        partner, uid)
    if not partner_row:
        return  # Partner hasn't tapped yet

    # Both revealed — fetch profiles
    my_row = await db_pool.fetchrow('SELECT name,age,country,gender FROM users WHERE user_id=$1', uid)
    pt_row = await db_pool.fetchrow('SELECT name,age,country,gender FROM users WHERE user_id=$1', partner)
    if not my_row or not pt_row: return

    compat = random.randint(60, 99)
    bar    = '💗' * (compat // 10) + '🤍' * (10 - compat // 10)
    g_emoji = {'Male': '👨', 'Female': '👩'}

    reveal_msg = (
        f'💞 *Soulmate Reveal!*\n\n'
        f"{g_emoji.get(my_row['gender'], '👤')} *You:*\n"
        f"  Name: {my_row['name']}\n  Age: {my_row['age']}\n  Country: {my_row['country']}\n\n"
        f"{g_emoji.get(pt_row['gender'], '👤')} *Your Partner:*\n"
        f"  Name: {pt_row['name']}\n  Age: {pt_row['age']}\n  Country: {pt_row['country']}\n\n"
        f"✨ *Compatibility:*\n{bar}\n*{compat}%* match! "
    )
    reveal_msg += ('🔥 Practically soulmates!' if compat >= 90
                   else '💫 Strong connection!'   if compat >= 75
                   else '😊 Every stranger is an adventure!')

    for u in [uid, partner]:
        try: await context.bot.send_message(u, reveal_msg, parse_mode='Markdown')
        except: pass

    await db_pool.execute(
        'DELETE FROM soulmate_reveals WHERE user_id=$1 OR user_id=$2', uid, partner)


# ─────────────────────────────────────────────────────────────────────────────
# PAYMENT HANDLERS
# ─────────────────────────────────────────────────────────────────────────────

async def pre_checkout_handler(update, context):
    query   = update.pre_checkout_query
    parts   = query.invoice_payload.split('_', 1)  # 'vip_week' → ['vip','week']
    pkg_key = parts[1] if len(parts) == 2 else None
    if pkg_key and pkg_key in VIP_PACKAGES:
        await query.answer(ok=True)
    else:
        await query.answer(ok=False, error_message='Unknown payment. Please try again.')

async def successful_payment_handler(update, context):
    payment = update.message.successful_payment
    uid     = update.message.from_user.id
    parts   = payment.invoice_payload.split('_', 1)
    pkg_key = parts[1] if len(parts) == 2 else None
    pkg     = VIP_PACKAGES.get(pkg_key)
    if not pkg:
        logger.error('Unknown VIP payload: %s uid=%s', payment.invoice_payload, uid); return
    await grant_vip(uid, pkg['days'])
    charge_id = payment.telegram_payment_charge_id
    await db_pool.execute('UPDATE users SET last_payment_id=$1 WHERE user_id=$2', charge_id, uid)
    logger.info('VIP payment: uid=%s pkg=%s stars=%s charge=%s', uid, pkg_key, pkg['stars'], charge_id)
    expiry_row = await db_pool.fetchrow('SELECT vip_expiry FROM users WHERE user_id=$1', uid)
    expiry_str = (expiry_row['vip_expiry'].strftime('%d %b %Y')
                  if expiry_row and expiry_row['vip_expiry'] else 'Permanent')
    await update.message.reply_text(
        f"{pkg['emoji']} *Payment successful!*\n\n"
        f"👑 {pkg['label']} is now active!\n"
        f"📅 Expires: {expiry_str}\n\n"
        f"✅ Gender filter unlocked\n✅ Unlimited Truth or Dare\n\n"
        f"Enjoy your VIP! 💎",
        parse_mode='Markdown', reply_markup=get_main_keyboard(uid))

async def testvip_command(update, context):
    if update.effective_user.id != ADMIN_ID: return
    uid = update.effective_user.id
    await context.bot.send_invoice(
        chat_id=uid,
        title='🧪 Test VIP Payment',
        description='Admin test — pays 1 Star, grants 1 day VIP.',
        payload='vip_test',
        provider_token='',
        currency='XTR',
        prices=[{'label': 'Test VIP (1 day)', 'amount': 1}])
    await update.message.reply_text('🧪 Test invoice sent! Pay 1 Star to verify the payment flow.')


# ─────────────────────────────────────────────────────────────────────────────
# SOULMATE COMMAND
# ─────────────────────────────────────────────────────────────────────────────

async def soulmate_command(update, context):
    uid = update.message.from_user.id
    partner = await get_partner(uid)
    if not partner:
        await update.message.reply_text(
            '💔 You must be in a chat to use /soulmate.\n\nPress 🚀 Find Partner first!'); return
    # Clear stale reveal state for this pair
    await db_pool.execute('DELETE FROM soulmate_reveals WHERE user_id=$1 OR user_id=$2', uid, partner)
    msg = ('💞 *Soulmate Reveal!*\n\n'
           'Both of you must tap 💕 Reveal to discover each other\'s profile and compatibility score!\n\n'
           '_Tap below to reveal yourself..._')
    await update.message.reply_text(msg, parse_mode='Markdown', reply_markup=soulmate_inline(uid))
    try:
        await context.bot.send_message(partner, msg, parse_mode='Markdown',
                                       reply_markup=soulmate_inline(partner))
    except: pass


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ROUTER
# ─────────────────────────────────────────────────────────────────────────────

async def router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message: return
    uid  = update.message.from_user.id
    text = update.message.text or ''

    if await is_banned_check(uid):
        await update.message.reply_text('🚫 You are banned from using this bot.')
        return

    # Sync username
    current_username = update.message.from_user.username
    if current_username and context.user_data.get('last_username') != current_username:
        context.user_data['last_username'] = current_username
        await db_pool.execute('UPDATE users SET username=$1 WHERE user_id=$2', current_username, uid)

    # ── REGISTRATION GATE ──
    if uid != ADMIN_ID:
        in_registration = context.user_data.get('step') is not None
        if not in_registration and not context.user_data.get('registered'):
            if await user_exists(uid):
                if await is_registered(uid): context.user_data['registered'] = True
                else: in_registration = True
        if in_registration:
            if context.user_data.get('step') is None:
                db_step = await get_registration_step(uid)
                context.user_data['step'] = db_step or 'name'
                logger.info('Recovered step=%s uid=%s', context.user_data['step'], uid)
            step = context.user_data.get('step')
            if step == 'name':
                if text and text not in BUTTON_TEXTS:
                    ref      = context.user_data.get('ref')
                    username = context.user_data.get('username') or update.message.from_user.username
                    await db_pool.execute(
                        'INSERT INTO users(user_id,username,name,referred_by) VALUES($1,$2,$3,$4) ON CONFLICT(user_id) DO UPDATE SET name=$3',
                        uid, username, text, ref)
                    context.user_data['step'] = 'gender'
                    await update.message.reply_text('Select your gender:', reply_markup=gender_keyboard)
                else:
                    await update.message.reply_text('👋 Please enter your name to continue:')
                return
            if step == 'gender':
                if text in ('Male', 'Female'):
                    await db_pool.execute('UPDATE users SET gender=$1 WHERE user_id=$2', text, uid)
                    context.user_data['step'] = 'country'
                    await update.message.reply_text('Enter your country:', reply_markup=ReplyKeyboardRemove())
                else:
                    await update.message.reply_text('Please select your gender:', reply_markup=gender_keyboard)
                return
            if step == 'country':
                if text and text not in BUTTON_TEXTS and not text.strip().isdigit():
                    await db_pool.execute('UPDATE users SET country=$1 WHERE user_id=$2', text, uid)
                    context.user_data['step'] = 'age'
                    await update.message.reply_text('Enter your age (16–60):')
                else:
                    await update.message.reply_text('Please enter your country name (e.g. India, USA):')
                return
            if step == 'age':
                # FIX 9: Realistic age range 16–60
                if text and text.isdigit() and 16 <= int(text) <= 60:
                    try: await db_pool.execute('UPDATE users SET age=$1 WHERE user_id=$2', int(text), uid)
                    except Exception as e:
                        logger.error('age save error %s: %s', uid, e)
                        await update.message.reply_text('Error saving. Try again.'); return
                    context.user_data.clear()
                    context.user_data['registered'] = True
                    await update.message.reply_text(
                        'Registration complete 🎉\n\nUse the buttons below to find a chat partner!',
                        reply_markup=user_keyboard)
                    try:
                        rr = await db_pool.fetchrow('SELECT referred_by,referral_processed FROM users WHERE user_id=$1', uid)
                        referrer = rr['referred_by'] if rr else None
                        already  = rr['referral_processed'] if rr else True
                        if referrer and not already:
                            await db_pool.execute('UPDATE users SET referral_processed=TRUE WHERE user_id=$1', uid)
                            vip_granted = await handle_referral(uid, referrer)
                            if vip_granted:
                                try: await context.bot.send_message(referrer,
                                    f'🎉 You earned {VIP_REFERRAL_DAYS} days of 👑 VIP for inviting friends!')
                                except: pass
                    except Exception as e: logger.error('referral error %s: %s', uid, e)
                else:
                    await update.message.reply_text('Please enter a valid age (16–60):')
                return
            context.user_data['step'] = 'name'
            await update.message.reply_text('Please enter your name to continue:')
            return
        elif not await user_exists(uid) and not context.user_data.get('step'):
            await update.message.reply_text('Please send /start to begin.', reply_markup=ReplyKeyboardRemove())
            return

    # ── ADMIN PANEL ──
    if text == '⚙️ Admin Panel' and uid == ADMIN_ID:
        context.user_data['in_admin_panel'] = True
        await update.message.reply_text('⚙️ Admin Panel', reply_markup=admin_panel_keyboard)
        return

    # Restore admin panel flag after bot restart
    if uid == ADMIN_ID and text in {
        '📊 Analytics', '👥 Active Users', '🕒 Waiting Users',
        '👑 VIP Users', '🧹 Clean Dead Chats', '🚨 Reports', '📱 Live Chats', '📢 Announcement',
    }:
        context.user_data['in_admin_panel'] = True

    if uid == ADMIN_ID and context.user_data.get('in_admin_panel'):
        if text == '📊 Analytics':
            total = await db_pool.fetchval('SELECT COUNT(*) FROM users')
            actv  = await db_pool.fetchval('SELECT COUNT(DISTINCT LEAST(user_id,partner_id)) FROM active_chats') or 0
            wait  = await db_pool.fetchval('SELECT COUNT(*) FROM waiting_users')
            vips  = await db_pool.fetchval("SELECT COUNT(*) FROM users WHERE (is_vip=TRUE AND vip_expiry IS NULL) OR vip_expiry>NOW()")
            bans  = await db_pool.fetchval('SELECT COUNT(*) FROM users WHERE is_banned=TRUE')
            reps  = await db_pool.fetchval('SELECT COUNT(*) FROM reports')
            await update.message.reply_text(
                f'📊 Analytics\n\n👤 Users: {total}\n💬 Chats: {actv}\n'
                f'🔎 Waiting: {wait}\n👑 VIPs: {vips}\n🚫 Banned: {bans}\n🚨 Reports: {reps}'); return
        if text == '👥 Active Users':
            n = await db_pool.fetchval('SELECT COUNT(DISTINCT LEAST(user_id,partner_id)) FROM active_chats') or 0
            await update.message.reply_text(f'💬 Active chats: {n}'); return
        if text == '🕒 Waiting Users':
            n = await db_pool.fetchval('SELECT COUNT(*) FROM waiting_users')
            await update.message.reply_text(f'🔎 Waiting: {n}'); return
        if text == '🚨 Reports':
            rows = await db_pool.fetch(
                '''SELECT r.reporter_id, r.reported_id, r.created_at, u.name, u.username,
                          (SELECT COUNT(*) FROM reports WHERE reported_id=r.reported_id) AS total
                   FROM reports r LEFT JOIN users u ON u.user_id=r.reported_id
                   ORDER BY r.created_at DESC LIMIT 10''')
            if not rows: await update.message.reply_text('✅ No reports yet.'); return
            buttons = [[InlineKeyboardButton(
                f"🚨 {r['name'] or 'Unknown'} ({r['total']} reports) — ID:{r['reported_id']}",
                callback_data=f"admin_report:{r['reported_id']}:{r['reporter_id']}")] for r in rows]
            await update.message.reply_text('🚨 Recent Reports:', reply_markup=InlineKeyboardMarkup(buttons)); return
        if text == '📱 Live Chats':
            pairs = await db_pool.fetch(
                'SELECT DISTINCT LEAST(user_id,partner_id) AS u1, GREATEST(user_id,partner_id) AS u2 FROM active_chats ORDER BY u1')
            if not pairs: await update.message.reply_text('❌ No active chats.'); return
            buttons = []; lines = ['📱 Live Chats\n']
            for i, pair in enumerate(pairs):
                u1, u2 = pair['u1'], pair['u2']
                r1 = await db_pool.fetchrow('SELECT name,gender FROM users WHERE user_id=$1', u1)
                r2 = await db_pool.fetchrow('SELECT name,gender FROM users WHERE user_id=$1', u2)
                n1 = (r1['name'] or '?') if r1 else '?'
                n2 = (r2['name'] or '?') if r2 else '?'
                g1 = ('👨' if (r1 and r1['gender']=='Male') else '👩') if r1 else '👤'
                g2 = ('👨' if (r2 and r2['gender']=='Male') else '👩') if r2 else '👤'
                lines.append(f'{i+1}. {g1} {n1}  ↔️  {g2} {n2}')
                buttons.append([InlineKeyboardButton(f'🚫 End #{i+1}: {n1} & {n2}',
                    callback_data=f'admin_end_chat:{u1}:{u2}')])
            await update.message.reply_text('\n'.join(lines), reply_markup=InlineKeyboardMarkup(buttons)); return
        if text == '👑 VIP Users':
            rows = await db_pool.fetch(
                "SELECT user_id,username,name,vip_expiry,referral_count FROM users "
                "WHERE (is_vip=TRUE AND vip_expiry IS NULL) OR vip_expiry>NOW() "
                "ORDER BY vip_expiry ASC NULLS FIRST LIMIT 20")
            if not rows: await update.message.reply_text('No active VIP users.'); return
            lines = ['👑 Active VIP Users\n']
            for r in rows:
                exp = 'Permanent ♾️' if r['vip_expiry'] is None else r['vip_expiry'].strftime('%Y-%m-%d')
                lines.append(f"• {r['name']} (@{r['username'] or '?'}) ID:{r['user_id']} Refs:{r['referral_count']} Exp:{exp}")
            await update.message.reply_text('\n'.join(lines)); return
        if text == '🧹 Clean Dead Chats':
            await update.message.reply_text('🔍 Scanning...')
            n = await clean_dead_chats(context.bot)
            await update.message.reply_text(f'✅ Removed {n} dead chat(s).'); return
        if text == '📢 Announcement':
            markup = InlineKeyboardMarkup([[
                InlineKeyboardButton('👥 All Users',    callback_data='announce_target:all'),
                InlineKeyboardButton('👩 Females Only', callback_data='announce_target:female')]])
            await update.message.reply_text('Who do you want to send to?', reply_markup=markup); return
        if context.user_data.get('announce_mode'):
            if text == '⬅️ Back':
                context.user_data.pop('announce_mode', None)
                context.user_data.pop('announce_target', None)
                await update.message.reply_text('❌ Cancelled.', reply_markup=admin_panel_keyboard); return
            target = context.user_data.pop('announce_target', 'all')
            context.user_data.pop('announce_mode', None)
            if target == 'female':
                rows = await db_pool.fetch(
                    "SELECT user_id FROM users WHERE is_banned=FALSE AND name IS NOT NULL AND age IS NOT NULL AND gender='Female'")
            else:
                rows = await db_pool.fetch(
                    'SELECT user_id FROM users WHERE is_banned=FALSE AND name IS NOT NULL AND age IS NOT NULL')
            sent = 0; blocked = 0
            for r in rows:
                try: await update.message.copy(chat_id=r['user_id']); sent += 1; await asyncio.sleep(0.05)
                except Exception as e:
                    blocked += 1
                    if 'Retry After' in str(e) or 'retry_after' in str(e).lower():
                        import re as _re
                        wait = int(_re.search(r'\d+', str(e)).group() or 5)
                        await asyncio.sleep(wait)
                        try: await update.message.copy(chat_id=r['user_id']); sent += 1; blocked -= 1
                        except: pass
            label = 'female users' if target == 'female' else 'all users'
            await update.message.reply_text(f'📢 Sent to {sent} {label}.\n🚫 Blocked: {blocked}'); return
        if text == '⬅️ Back':
            context.user_data['in_admin_panel'] = False
            context.user_data.pop('announce_mode', None)
            await update.message.reply_text('Main menu', reply_markup=admin_main_keyboard); return

    # ── SHARED BUTTONS ──
    if text == '🚀 Find Partner':
        context.user_data['last_pref'] = None; await match_user(update, context); return
    if text == '👨 Find Male':
        if uid == ADMIN_ID or await check_vip(uid):
            context.user_data['last_pref'] = 'Male'; await match_user(update, context, 'Male')
        else: await update.message.reply_text('👑 VIP required.\n\nUse 💎 VIP to learn more.')
        return
    if text == '👩 Find Female':
        if uid == ADMIN_ID or await check_vip(uid):
            context.user_data['last_pref'] = 'Female'; await match_user(update, context, 'Female')
        else: await update.message.reply_text('👑 VIP required.\n\nUse 💎 VIP to learn more.')
        return
    if text == '⏭️ Next': await stop_chat(update, context); await match_user(update, context); return
    if text == '❌ Stop': await stop_chat(update, context); return

    if text == '💎 VIP':
        if uid == ADMIN_ID:
            await update.message.reply_text('👑 VIP Status: ✅ Active\nExpires: Permanent ♾️',
                                            reply_markup=vip_keyboard); return
        r = await db_pool.fetchrow('SELECT is_vip,vip_expiry,referral_count FROM users WHERE user_id=$1', uid)
        active = await check_vip(uid)
        cnt    = r['referral_count'] if r else 0
        prog   = cnt % VIP_REFERRAL_THRESHOLD; rem = VIP_REFERRAL_THRESHOLD - prog
        if active:
            exp_s = r['vip_expiry'].strftime('%d %b %Y, %H:%M UTC') if r and r['vip_expiry'] else 'Permanent ♾️'
            status_msg = (f'👑 VIP Status: ✅ Active\nExpires: {exp_s}\n\n'
                         f'🔸 VIP Perks:\n• Filter by gender\n• Unlimited Truth or Dare\n\n'
                         f'🔄 Next free VIP in {rem} more referral(s)')
        elif r and r['vip_expiry']:
            status_msg = (f'👑 VIP Status: ⏰ Expired ({r["vip_expiry"].strftime("%d %b %Y")})\n\n'
                         f'Renew now ⭐ or invite {rem} friends for free!')
        else:
            status_msg = (f'👑 VIP Status: ❌ Inactive\n\n🔸 VIP gives you:\n'
                         f'• Filter by gender\n• Unlimited Truth or Dare\n\n'
                         f'Buy instantly with Stars ⭐ or get FREE via referrals 🎁')
        await update.message.reply_text(status_msg, reply_markup=vip_keyboard); return

    if text == '🎁 Get FREE VIP':
        r    = await db_pool.fetchrow('SELECT referral_count FROM users WHERE user_id=$1', uid)
        cnt  = r['referral_count'] if r else 0
        link = f'https://t.me/{context.bot.username}?start={uid}'
        prog = cnt % VIP_REFERRAL_THRESHOLD; rem = VIP_REFERRAL_THRESHOLD - prog
        await update.message.reply_text(
            f'🎁 Invite friends to get FREE VIP!\n\nYour link:\n{link}\n\n'
            f'📊 Referrals: {cnt}\n🔄 Progress: {prog}/{VIP_REFERRAL_THRESHOLD}\n'
            f'🏆 VIPs earned: {cnt//VIP_REFERRAL_THRESHOLD}\n\n'
            f'Invite {rem} more for 👑 VIP ({VIP_REFERRAL_DAYS} days)!'); return

    if text == '⭐ Buy VIP':
        buttons = [
            [InlineKeyboardButton(f"⭐ {VIP_PACKAGES['week']['label']} — {VIP_PACKAGES['week']['stars']} Stars",   callback_data='buy_vip:week')],
            [InlineKeyboardButton(f"🌟 {VIP_PACKAGES['month']['label']} — {VIP_PACKAGES['month']['stars']} Stars", callback_data='buy_vip:month')],
        ]
        if uid == ADMIN_ID:
            buttons.append([InlineKeyboardButton(
                f"🧪 {VIP_PACKAGES['test']['label']} — {VIP_PACKAGES['test']['stars']} Star",
                callback_data='buy_vip:test')])
        await update.message.reply_text(
            '⭐ Choose your VIP package\n\n'
            '📅 *1 Week VIP — 50 Stars*\n• 👩 Filter by gender\n• 🎲 Unlimited Truth or Dare\n• 🏆 Top priority matching\n\n'
            '📆 *1 Month VIP — 100 Stars*\n• All 1 Week perks\n• 💎 VIP badge visible\n• 💰 Best value!\n\n'
            '⭐ Pay with Telegram Stars — instant activation',
            parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(buttons)); return

    if text == '⬅️ Back':
        context.user_data.pop('announce_mode', None); context.user_data.pop('in_admin_panel', None)
        await update.message.reply_text('Main menu 👇', reply_markup=get_main_keyboard(uid)); return

    # ── RELAY ──
    if text not in BUTTON_TEXTS:

        # FIX 8: Block spam / links
        if is_spam(text):
            await update.message.reply_text('⚠️ Links and external URLs are not allowed in chat.')
            return

        # Rate limit: silently drop if sending faster than 1 msg/s
        now = time.monotonic()
        if now - context.user_data.get('last_msg_time', 0) < RATE_LIMIT_SECONDS:
            return
        context.user_data['last_msg_time'] = now

        partner = await get_partner(uid)
        if partner:
            try:
                # LOGIC CHECK: copy() forwards ANY message type (photo, sticker, voice…)
                # to the partner's chat — this is the correct relay mechanism.
                await update.message.copy(chat_id=partner)
                if text:  # Only count/log text messages, not media
                    await db_pool.execute(
                        'UPDATE users SET total_messages=total_messages+1 WHERE user_id=$1', uid)
                    await log_message(uid, partner, text)
            except Forbidden:
                # Partner blocked the bot — clean up and notify sender
                await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', uid)
                await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', partner)
                await update.message.reply_text(
                    '👋 Your partner has left the chat.\n\nPress 🚀 Find Partner to meet someone new!',
                    reply_markup=get_main_keyboard(uid))
            except BadRequest as e:
                # Un-forwardable message type — warn the sender but keep the chat alive
                logger.warning('BadRequest relay uid=%s: %s', uid, e)
                await update.message.reply_text(
                    '⚠️ That message type could not be delivered to your partner.')
            except TimedOut:
                # Transient network issue — do NOT disconnect, just ignore
                pass
            except Exception as e:
                logger.warning('Relay error uid=%s partner=%s: %s', uid, partner, e)
                await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', uid)
                await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', partner)
                await update.message.reply_text(
                    '⚠️ Partner disconnected.', reply_markup=get_main_keyboard(uid))
        else:
            await update.message.reply_text(
                'Not in a chat. Press 🚀 Find Partner.', reply_markup=get_main_keyboard(uid))


# ─────────────────────────────────────────────────────────────────────────────
# /start
# ─────────────────────────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid      = update.message.from_user.id
    username = update.message.from_user.username
    if await is_banned_check(uid):
        await update.message.reply_text('🚫 You are banned.'); return
    ref = None
    if context.args and context.args[0].isdigit():
        ref = int(context.args[0])
        if ref == uid: ref = None
    if uid == ADMIN_ID:
        await db_pool.execute(
            "INSERT INTO users(user_id,username,name,gender,is_vip) VALUES($1,$2,'Admin','Male',TRUE) "
            "ON CONFLICT(user_id) DO UPDATE SET is_vip=TRUE, vip_expiry=NULL",
            uid, username)
        await db_pool.execute('DELETE FROM active_chats  WHERE user_id=$1', uid)
        await db_pool.execute('DELETE FROM waiting_users WHERE user_id=$1', uid)
        context.user_data.clear()
        await update.message.reply_text('Welcome, Admin 👋', reply_markup=admin_main_keyboard); return
    if await user_exists(uid):
        if not await is_registered(uid):
            context.user_data.clear()
            db_step = await get_registration_step(uid)
            context.user_data['step'] = db_step or 'name'
            prompts = {
                'gender':  ("Let's finish your profile.\n\nSelect your gender:", gender_keyboard),
                'country': ("Let's finish your profile.\n\nEnter your country:", ReplyKeyboardRemove()),
                'age':     ("Let's finish your profile.\n\nEnter your age (16–60):", None),
            }
            p = prompts.get(db_step, ("Let's finish your profile.\n\nEnter your name:", None))
            await update.message.reply_text(p[0], reply_markup=p[1] if p[1] else ReplyKeyboardRemove())
        else:
            context.user_data['registered'] = True
            await update.message.reply_text('Welcome back! 👋', reply_markup=user_keyboard)
        return
    context.user_data.clear()
    context.user_data['step']     = 'name'
    context.user_data['ref']      = ref
    context.user_data['username'] = username
    await update.message.reply_text('👋 Welcome! Let\'s set up your profile.\n\nEnter your name:')


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    global db_pool
    dsn = DATABASE_URL
    if dsn and dsn.startswith('postgres://'):
        dsn = dsn.replace('postgres://', 'postgresql://', 1)
    db_pool = await asyncpg.create_pool(dsn, min_size=5, max_size=50)
    await init_db()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # FIX 6: Register startup hook — background tasks launch after bot initialises
    app.post_init = on_startup

    # Commands
    app.add_handler(CommandHandler('start',         start))
    app.add_handler(CommandHandler('broadcast',     broadcast))
    app.add_handler(CommandHandler('ban',           handle_ban))
    app.add_handler(CommandHandler('unban',         handle_ban))
    app.add_handler(CommandHandler('cleanchats',    cleanchats_command))
    app.add_handler(CommandHandler('debugref',      debug_referral))
    app.add_handler(CommandHandler('regrantvip',    regrant_vip_command))
    app.add_handler(CommandHandler('cleanup',       cleanup_null_users))
    app.add_handler(CommandHandler('fixvip',        fixvip_command))
    app.add_handler(CommandHandler('vipfemales',    vipfemales_command))
    app.add_handler(CommandHandler('deleteblocked', delete_blocked_command))
    app.add_handler(CommandHandler('nudge',         nudge_chats_command))
    app.add_handler(CommandHandler('stats',         stats_command))
    app.add_handler(CommandHandler('update',        update_command))
    app.add_handler(CommandHandler('resetdb',       reset_db_command))
    app.add_handler(CommandHandler('testvip',       testvip_command))
    app.add_handler(CommandHandler('soulmate',      soulmate_command))

    # Callback queries
    app.add_handler(CallbackQueryHandler(buy_vip_callback,            pattern=r'^buy_vip:'))
    app.add_handler(CallbackQueryHandler(admin_end_chat_callback,     pattern=r'^admin_end_chat:'))
    app.add_handler(CallbackQueryHandler(find_new_callback,           pattern=r'^find_new$'))
    app.add_handler(CallbackQueryHandler(announce_target_callback,    pattern=r'^announce_target:'))
    app.add_handler(CallbackQueryHandler(report_callback,             pattern=r'^report:'))
    app.add_handler(CallbackQueryHandler(tod_callback,                pattern=r'^tod_'))
    app.add_handler(CallbackQueryHandler(admin_report_callback,       pattern=r'^admin_report:'))
    app.add_handler(CallbackQueryHandler(admin_ban_callback,          pattern=r'^admin_ban:'))
    app.add_handler(CallbackQueryHandler(admin_del_report_callback,   pattern=r'^admin_del_report:'))
    app.add_handler(CallbackQueryHandler(admin_back_reports_callback, pattern=r'^admin_back_reports$'))
    app.add_handler(CallbackQueryHandler(soulmate_reveal_callback,    pattern=r'^soulmate_reveal:'))

    # Payments
    app.add_handler(PreCheckoutQueryHandler(pre_checkout_handler))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_handler))

    # General router — must be registered LAST
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, router))

    logger.info('Bot starting...')
    for attempt in range(1, 11):
        try:
            await app.initialize()
            await app.start()
            break
        except Exception as e:
            logger.warning('Init attempt %d/10 failed: %s', attempt, e)
            if attempt == 10:
                logger.error('Could not connect after 10 attempts. Exiting.')
                raise
            await asyncio.sleep(attempt * 2)

    await app.updater.start_polling(drop_pending_updates=True)

    async def _set_description():
        try: await app.bot.set_my_short_description('🎭 Chat anonymously with strangers')
        except Exception as e: logger.warning('Could not set description: %s', e)

    asyncio.create_task(_set_description())
    logger.info('Bot started successfully')

    try:
        await asyncio.Event().wait()
    finally:
        logger.info('Shutting down...')
        await db_pool.close()
        await app.stop()
        await app.shutdown()


if __name__ == '__main__':
    asyncio.run(main())
