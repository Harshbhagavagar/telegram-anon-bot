import os, random, logging, asyncio
import asyncpg
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.error import Forbidden, TimedOut
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, PreCheckoutQueryHandler, ContextTypes, filters, PreCheckoutQueryHandler

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN    = os.getenv('BOT_TOKEN')
DATABASE_URL = os.getenv('DATABASE_URL')
ADMIN_ID     = 643086953
VIP_REFERRAL_THRESHOLD = 3
VIP_REFERRAL_DAYS      = 3
MATCH_INVITE_DELAY     = 45
FREE_TOD_LIMIT         = 3
VIP_WEEK_STARS         = 50    # 1 week VIP price in Stars
VIP_MONTH_STARS        = 100   # 1 month VIP price in Stars
VIP_TEST_STARS         = 1     # admin test payment
VIP_WEEK_PRICE         = 50    # Telegram Stars for 1 week VIP
VIP_MONTH_PRICE        = 100   # Telegram Stars for 1 month VIP
VIP_TEST_PRICE         = 1     # 1 Star for admin test

db_pool: asyncpg.Pool = None

# VIP payment packages (Telegram Stars)
VIP_PACKAGES = {
    'week':  {'stars': 50,  'days': 7,  'label': '1 Week VIP',  'emoji': '⭐'},
    'month': {'stars': 100, 'days': 30, 'label': '1 Month VIP', 'emoji': '🌟'},
    'test':  {'stars': 1,   'days': 7,  'label': 'Test VIP (Admin)', 'emoji': '🧪'},
}
context_tod_counts: dict = {}  # in-memory per session, cleaned on stop — acceptable

async def init_db():
    await db_pool.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY, username TEXT, name TEXT, gender TEXT CHECK (gender IN ('Male','Female')),
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
        CREATE TABLE IF NOT EXISTS active_chats (user_id BIGINT PRIMARY KEY, partner_id BIGINT)''')
    await db_pool.execute('''
        CREATE TABLE IF NOT EXISTS reports (
            id SERIAL PRIMARY KEY, reporter_id BIGINT NOT NULL, reported_id BIGINT NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW())''')
    await db_pool.execute('''
        CREATE TABLE IF NOT EXISTS chat_logs (
            id SERIAL PRIMARY KEY, sender_id BIGINT NOT NULL, partner_id BIGINT NOT NULL,
            message TEXT, created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW())''')
    for s in [
        'CREATE INDEX IF NOT EXISTS idx_wq  ON waiting_users(queued_at)',
        'CREATE INDEX IF NOT EXISTS idx_ve  ON users(vip_expiry)',
        'CREATE INDEX IF NOT EXISTS idx_rr  ON reports(reported_id)',
        'CREATE INDEX IF NOT EXISTS idx_clp ON chat_logs(sender_id,partner_id)',
        'CREATE INDEX IF NOT EXISTS idx_ac_partner ON active_chats(partner_id)',
        'ALTER TABLE users ADD COLUMN IF NOT EXISTS is_banned      BOOLEAN DEFAULT FALSE',
        'ALTER TABLE users ADD COLUMN IF NOT EXISTS age            INT',
        'ALTER TABLE users ADD COLUMN IF NOT EXISTS country        TEXT',
        'ALTER TABLE users ADD COLUMN IF NOT EXISTS referred_by    BIGINT',
        'ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_count INT DEFAULT 0',
        'ALTER TABLE users ADD COLUMN IF NOT EXISTS vip_expiry     TIMESTAMP WITH TIME ZONE',
        'ALTER TABLE users ADD COLUMN IF NOT EXISTS total_messages  INT DEFAULT 0',
        'ALTER TABLE users ADD COLUMN IF NOT EXISTS last_search_pref    TEXT',
        'ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_processed BOOLEAN DEFAULT FALSE',
        'ALTER TABLE users ADD COLUMN IF NOT EXISTS last_payment_id    TEXT',
    ]: await db_pool.execute(s)
    logger.info('DB ready')


user_keyboard = ReplyKeyboardMarkup(
    [['\U0001f680 Find Partner', '\U0001f48e VIP'],
     ['\U0001f468 Find Male',   '\U0001f469 Find Female'],
     ['\u23ed\ufe0f Next',       '\u274c Stop']],
    resize_keyboard=True)
admin_main_keyboard = ReplyKeyboardMarkup(
    [['\U0001f680 Find Partner', '\U0001f48e VIP'],
     ['\U0001f468 Find Male',   '\U0001f469 Find Female'],
     ['\u23ed\ufe0f Next',       '\u274c Stop'],
     ['\u2699\ufe0f Admin Panel']],
    resize_keyboard=True)
admin_panel_keyboard = ReplyKeyboardMarkup(
    [['\U0001f4ca Analytics'],
     ['\U0001f4e2 Announcement'],
     ['\U0001f465 Active Users',   '\U0001f552 Waiting Users'],
     ['\U0001f451 VIP Users',      '\U0001f9f9 Clean Dead Chats'],
     ['\U0001f6a8 Reports',        '\U0001f4f1 Live Chats'],
     ['\u2b05\ufe0f Back']],
    resize_keyboard=True)
vip_keyboard = ReplyKeyboardMarkup(
    [['\U0001f381 Get FREE VIP', '\u2b50 Buy VIP'],
     ['\u2b05\ufe0f Back']],
    resize_keyboard=True)

gender_keyboard = ReplyKeyboardMarkup([['Male','Female']], resize_keyboard=True, one_time_keyboard=True)


def buy_vip_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('⭐ 1 Week VIP — 50 Stars',  callback_data='buy_vip:week')],
        [InlineKeyboardButton('⭐ 1 Month VIP — 100 Stars', callback_data='buy_vip:month')],
    ])

VIP_WEEK_STARS  = 50
VIP_MONTH_STARS = 100

def buy_vip_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('⭐ 1 Week VIP — 50 Stars',  callback_data='buy_vip:week')],
        [InlineKeyboardButton('⭐ 1 Month VIP — 100 Stars', callback_data='buy_vip:month')],
    ])

def buy_vip_inline(uid: int) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(f'⭐ 1 Week VIP — {VIP_WEEK_STARS} Stars',  callback_data='buy_vip:week')],
        [InlineKeyboardButton(f'⭐ 1 Month VIP — {VIP_MONTH_STARS} Stars', callback_data='buy_vip:month')],
    ]
    if uid == ADMIN_ID:
        buttons.append([InlineKeyboardButton(f'🧪 Test — {VIP_TEST_STARS} Star (Admin)', callback_data='buy_vip:test')])
    return InlineKeyboardMarkup(buttons)

def report_inline(partner_id):
    return InlineKeyboardMarkup([[InlineKeyboardButton('\u26a0\ufe0f Report', callback_data=f'report:{partner_id}')]])
def buy_vip_inline(uid: int, is_admin: bool = False) -> InlineKeyboardMarkup:
    """Inline buttons for buying VIP with Telegram Stars."""
    buttons = [
        [InlineKeyboardButton(
            f"⭐ 1 Week VIP — 50 Stars",
            callback_data=f"buy_vip:week:{uid}")],
        [InlineKeyboardButton(
            f"🌟 1 Month VIP — 100 Stars",
            callback_data=f"buy_vip:month:{uid}")],
    ]
    if is_admin:
        buttons.append([InlineKeyboardButton(
            "🧪 Test Payment — 1 Star",
            callback_data=f"buy_vip:test:{uid}")])
    return InlineKeyboardMarkup(buttons)

def tod_inline(uid):
    return InlineKeyboardMarkup([[InlineKeyboardButton('\U0001f3b2 Truth or Dare', callback_data=f'tod_start:{uid}')]])
def tod_choice_inline(init_uid):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton('\U0001f607 Truth', callback_data=f'tod_pick:truth:{init_uid}'),
        InlineKeyboardButton('\U0001f608 Dare',  callback_data=f'tod_pick:dare:{init_uid}'),
    ]])
def tod_again_inline(init_uid):
    return InlineKeyboardMarkup([[InlineKeyboardButton('\U0001f3b2 Another Round', callback_data=f'tod_start:{init_uid}')]])
def get_main_keyboard(uid):
    return admin_main_keyboard if uid == ADMIN_ID else user_keyboard


TRUTH_QUESTIONS = [
    "What's the most embarrassing thing you've done for someone you liked? \U0001f633",
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
    "Send a voice note saying 'I miss you' as dramatically as possible \U0001f3ad",
    "Type your next message with your eyes closed \U0001f604",
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

BUTTON_TEXTS = {
    '\U0001f680 Find Partner', '\U0001f468 Find Male', '\U0001f469 Find Female',
    '\u23ed\ufe0f Next', '\u274c Stop', '\U0001f48e VIP', '\U0001f381 Get FREE VIP', '\u2b50 Buy VIP',
    '\u2b05\ufe0f Back', '\u2699\ufe0f Admin Panel',
    '\u26a0\ufe0f Report',
    '\U0001f4ca Analytics', '\U0001f465 Active Users', '\U0001f552 Waiting Users',
    '\U0001f4e2 Announcement', '\U0001f9f9 Clean Dead Chats', '\U0001f451 VIP Users',
    '\U0001f6a8 Reports', '\U0001f4f1 Live Chats',
    'Male', 'Female',
}


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
    await db_pool.execute(
        f'''UPDATE users SET is_vip=TRUE,
            vip_expiry=GREATEST(NOW(),COALESCE(vip_expiry,NOW()))+INTERVAL '{days} days'
            WHERE user_id=$1''', uid)

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
    await db_pool.execute(
        '''DELETE FROM chat_logs WHERE id IN (
               SELECT id FROM chat_logs
               WHERE (sender_id=$1 AND partner_id=$2) OR (sender_id=$2 AND partner_id=$1)
               ORDER BY created_at DESC OFFSET 10)''',
        sender_id, partner_id)


async def clean_dead_chats(bot):
    rows = await db_pool.fetch('SELECT DISTINCT user_id,partner_id FROM active_chats')
    seen, cleaned = set(), 0
    for row in rows:
        uid, partner = row['user_id'], row['partner_id']
        pk = tuple(sorted((uid, partner)))
        if pk in seen: continue
        seen.add(pk)
        ua = pa = False
        try: await bot.send_chat_action(chat_id=uid, action='typing'); ua = True
        except: pass
        try: await bot.send_chat_action(chat_id=partner, action='typing'); pa = True
        except: pass
        if not ua or not pa:
            await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', uid)
            await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', partner)
            # Also remove dead users from waiting queue
            if not ua:
                await db_pool.execute('DELETE FROM waiting_users WHERE user_id=$1', uid)
            if not pa:
                await db_pool.execute('DELETE FROM waiting_users WHERE user_id=$1', partner)
            cleaned += 1
            if ua:
                try: await bot.send_message(uid, '\u26a0\ufe0f Partner disconnected. Press \U0001f680 Find Partner.')
                except: pass
            if pa:
                try: await bot.send_message(partner, '\u26a0\ufe0f Partner disconnected. Press \U0001f680 Find Partner.')
                except: pass
    return cleaned

async def cleanchats_command(update, context):
    if update.message.from_user.id != ADMIN_ID: return
    await update.message.reply_text('\U0001f50d Scanning...')
    n = await clean_dead_chats(context.bot)
    await update.message.reply_text(f'\u2705 Removed {n} dead chat(s).')

async def broadcast(update, context):
    if update.message.from_user.id != ADMIN_ID: return
    if not context.args: await update.message.reply_text('Usage: /broadcast <msg>'); return
    msg = ' '.join(context.args)
    rows = await db_pool.fetch(
        'SELECT user_id FROM users WHERE is_banned=FALSE AND name IS NOT NULL AND age IS NOT NULL')
    sent = 0
    for r in rows:
        try:
            await context.bot.send_message(r['user_id'], msg)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            if 'Retry After' in str(e) or 'retry_after' in str(e).lower():
                import re
                wait = int(re.search(r'\d+', str(e)).group() or 5)
                await asyncio.sleep(wait)
                try: await context.bot.send_message(r['user_id'], msg); sent += 1
                except: pass
            # else: user blocked bot or deactivated — skip silently
    await update.message.reply_text(f'\u2705 Sent to {sent} users')

async def handle_ban(update, context):
    if update.message.from_user.id != ADMIN_ID: return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text('Usage: /ban <id> or /unban <id>'); return
    tid = int(context.args[0])
    unban = update.message.text.startswith('/unban')
    if not await user_exists(tid): await update.message.reply_text(f'\u274c User {tid} not found.'); return
    if unban:
        await db_pool.execute('UPDATE users SET is_banned=FALSE WHERE user_id=$1', tid)
        await update.message.reply_text(f'\u2705 {tid} unbanned.')
        try: await context.bot.send_message(tid, '\u2705 Your ban has been lifted.')
        except: pass
    else:
        await db_pool.execute('UPDATE users SET is_banned=TRUE WHERE user_id=$1', tid)
        p = await get_partner(tid)
        if p:
            await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', tid)
            await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', p)
            try: await context.bot.send_message(p, '\U0001f44b Stranger left the chat.')
            except: pass
        await db_pool.execute('DELETE FROM waiting_users WHERE user_id=$1', tid)
        await update.message.reply_text(f'\U0001f6ab {tid} banned.')
        try: await context.bot.send_message(tid, '\U0001f6ab You have been banned.')
        except: pass

async def cleanup_null_users(update, context):
    if update.message.from_user.id != ADMIN_ID: return
    rows = await db_pool.fetch(
        'SELECT user_id FROM users WHERE (name IS NULL OR gender IS NULL OR country IS NULL OR age IS NULL) AND user_id!=$1',
        ADMIN_ID)
    if not rows: await update.message.reply_text('\u2705 No incomplete registrations.'); return
    for r in rows:
        uid = r['user_id']
        for q in ['DELETE FROM waiting_users WHERE user_id=$1',
                  'DELETE FROM active_chats WHERE user_id=$1',
                  'DELETE FROM active_chats WHERE partner_id=$1',
                  'DELETE FROM reports WHERE reporter_id=$1 OR reported_id=$1',
                  'DELETE FROM chat_logs WHERE sender_id=$1 OR partner_id=$1',
                  'DELETE FROM users WHERE user_id=$1']:
            await db_pool.execute(q, uid)
    await update.message.reply_text(f'\U0001f5d1 Removed {len(rows)} incomplete user(s) (missing name, gender, country, or age).')

async def regrant_vip_command(update, context):
    if update.message.from_user.id != ADMIN_ID: return
    rows = await db_pool.fetch(
        'SELECT user_id,referral_count FROM users WHERE referral_count>=$1 AND (is_vip=FALSE OR vip_expiry<NOW()) AND user_id!=$2',
        VIP_REFERRAL_THRESHOLD, ADMIN_ID)
    if not rows: await update.message.reply_text('\u2705 No users need re-grant.'); return
    for r in rows:
        days = (r['referral_count']//VIP_REFERRAL_THRESHOLD)*VIP_REFERRAL_DAYS
        await grant_vip(r['user_id'], days)
        try: await context.bot.send_message(r['user_id'], f'\U0001f389 VIP renewed for {days} days!')
        except: pass
    await update.message.reply_text(f'\u2705 Re-granted VIP to {len(rows)} user(s).')

async def fixvip_command(update, context):
    if update.message.from_user.id != ADMIN_ID: return
    rows = await db_pool.fetch(
        'SELECT user_id,referral_count FROM users WHERE referral_count>=$1 AND is_vip=FALSE AND user_id!=$2',
        VIP_REFERRAL_THRESHOLD, ADMIN_ID)
    if not rows: await update.message.reply_text('\u2705 No users need VIP fixing.'); return
    for r in rows:
        days = (r['referral_count']//VIP_REFERRAL_THRESHOLD)*VIP_REFERRAL_DAYS
        await grant_vip(r['user_id'], days)
        try: await context.bot.send_message(r['user_id'], f'\U0001f389 VIP activated for {days} days!')
        except: pass
    await update.message.reply_text(f'\u2705 Fixed VIP for {len(rows)} user(s).')

async def update_command(update, context):
    """Send /start-style re-prompt to all users with NULL name or gender."""
    if update.message.from_user.id != ADMIN_ID: return
    rows = await db_pool.fetch(
        'SELECT user_id FROM users WHERE (name IS NULL OR gender IS NULL OR country IS NULL OR age IS NULL) AND user_id!=$1',
        ADMIN_ID)
    if not rows:
        await update.message.reply_text('\u2705 All users are fully registered.'); return
    sent = 0
    for r in rows:
        uid = r['user_id']
        step = await get_registration_step(uid)
        try:
            if step == 'name':
                await context.bot.send_message(uid,
                    '\U0001f44b Hey! Please complete your profile to use the bot.\n\nEnter your name:',
                    reply_markup=ReplyKeyboardRemove())
            elif step == 'gender':
                await context.bot.send_message(uid,
                    '\U0001f44b Hey! Please complete your profile.\n\nSelect your gender:',
                    reply_markup=ReplyKeyboardMarkup([['Male','Female']], resize_keyboard=True, one_time_keyboard=True))
            elif step == 'country':
                await context.bot.send_message(uid,
                    '\U0001f44b Hey! Please complete your profile.\n\nEnter your country:',
                    reply_markup=ReplyKeyboardRemove())
            elif step == 'age':
                await context.bot.send_message(uid,
                    '\U0001f44b Hey! Please complete your profile.\n\nEnter your age:',
                    reply_markup=ReplyKeyboardRemove())
            sent += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.warning('update nudge failed uid=%s: %s', uid, e)
    await update.message.reply_text(f'\u2705 Sent re-registration prompt to {sent}/{len(rows)} incomplete user(s).')

async def stats_command(update, context):
    if update.message.from_user.id != ADMIN_ID: return
    total      = await db_pool.fetchval('SELECT COUNT(*) FROM users')
    male       = await db_pool.fetchval("SELECT COUNT(*) FROM users WHERE gender='Male'")
    female     = await db_pool.fetchval("SELECT COUNT(*) FROM users WHERE gender='Female'")
    incomplete = await db_pool.fetchval('SELECT COUNT(*) FROM users WHERE name IS NULL OR gender IS NULL OR country IS NULL OR age IS NULL')
    active     = (await db_pool.fetchval('SELECT COUNT(*) FROM active_chats') or 0) // 2
    waiting    = await db_pool.fetchval('SELECT COUNT(*) FROM waiting_users')
    vips          = await db_pool.fetchval("SELECT COUNT(*) FROM users WHERE (is_vip=TRUE AND vip_expiry IS NULL) OR vip_expiry>NOW()")
    vip_male      = await db_pool.fetchval("SELECT COUNT(*) FROM users WHERE gender='Male'   AND ((is_vip=TRUE AND vip_expiry IS NULL) OR vip_expiry>NOW())")
    vip_female    = await db_pool.fetchval("SELECT COUNT(*) FROM users WHERE gender='Female' AND ((is_vip=TRUE AND vip_expiry IS NULL) OR vip_expiry>NOW())")
    banned     = await db_pool.fetchval('SELECT COUNT(*) FROM users WHERE is_banned=TRUE')
    reports    = await db_pool.fetchval('SELECT COUNT(*) FROM reports')
    total_msgs = await db_pool.fetchval('SELECT COALESCE(SUM(total_messages),0) FROM users')
    today      = await db_pool.fetchval("SELECT COUNT(*) FROM users WHERE created_at > NOW() - INTERVAL '24 hours'")
    week       = await db_pool.fetchval("SELECT COUNT(*) FROM users WHERE created_at > NOW() - INTERVAL '7 days'")
    top_refs   = await db_pool.fetch(
        'SELECT name, username, referral_count FROM users WHERE referral_count > 0 ORDER BY referral_count DESC LIMIT 5')

    male_pct   = round(male/total*100)   if total else 0
    female_pct = round(female/total*100) if total else 0

    # Active users = currently in a chat or waiting
    inchat_male   = await db_pool.fetchval(
        """SELECT COUNT(*) FROM users u
           WHERE u.gender='Male'
             AND EXISTS(SELECT 1 FROM active_chats ac WHERE ac.user_id=u.user_id)""")
    inchat_female = await db_pool.fetchval(
        """SELECT COUNT(*) FROM users u
           WHERE u.gender='Female'
             AND EXISTS(SELECT 1 FROM active_chats ac WHERE ac.user_id=u.user_id)""")
    searching_male   = await db_pool.fetchval(
        """SELECT COUNT(*) FROM users u
           WHERE u.gender='Male'
             AND EXISTS(SELECT 1 FROM waiting_users w WHERE w.user_id=u.user_id)""")
    searching_female = await db_pool.fetchval(
        """SELECT COUNT(*) FROM users u
           WHERE u.gender='Female'
             AND EXISTS(SELECT 1 FROM waiting_users w WHERE w.user_id=u.user_id)""")

    lines = [
        '\U0001f4ca Full Stats\n',
        f'\U0001f464 Total users:    {total}',
        f'\U0001f468 Male:           {male} ({male_pct}%)',
        f'\U0001f469 Female:         {female} ({female_pct}%)',
        f'\u2753 Incomplete:     {incomplete}',
        f'\U0001f4c5 Today:          {today}',
        f'\U0001f4c6 This week:      {week}',
        f'\n\U0001f4ac Active chats:  {active}',
        f'\U0001f50e Waiting:        {waiting}',
        f'\U0001f468\U0001f4ac In chat males:   {inchat_male}',
        f'\U0001f469\U0001f4ac In chat females: {inchat_female}',
        f'\U0001f468\U0001f50e Searching males:   {searching_male}',
        f'\U0001f469\U0001f50e Searching females: {searching_female}',
        f'\U0001f4dd Total messages: {total_msgs}',
        f'\n\U0001f451 VIPs total:     {vips}',
        f'\U0001f468\U0001f451 Male VIPs:     {vip_male}',
        f'\U0001f469\U0001f451 Female VIPs:   {vip_female}',
        f'\U0001f6ab Banned:         {banned}',
        f'\U0001f6a8 Reports:        {reports}',
    ]
    if top_refs:
        lines.append('\n\U0001f3c6 Top Referrers:')
        for r in top_refs:
            name = r['name'] or 'Unknown'
            uname = f"@{r['username']}" if r['username'] else 'no username'
            lines.append(f"  {name} ({uname}) — {r['referral_count']} refs")

    await update.message.reply_text('\n'.join(lines))

async def vipfemales_command(update, context):
    """Grant 7 days VIP to all female users and notify them."""
    if update.message.from_user.id != ADMIN_ID: return
    rows = await db_pool.fetch(
        """SELECT user_id FROM users
           WHERE gender='Female' AND is_banned=FALSE""")
    if not rows:
        await update.message.reply_text('\u274c No female users found.'); return
    sent = 0; already = 0; failed = 0
    for r in rows:
        await grant_vip(r['user_id'], 7)
        try:
            await context.bot.send_message(
                r['user_id'],
                '\U0001f451 You just got 7 days of FREE VIP!\n\n'
                '\U0001f389 As a VIP you can now:\n'
                '\u2022 Filter matches by gender (Find Male / Find Female)\n'
                '\u2022 Unlimited Truth or Dare rounds\n\n'
                'Enjoy your VIP! \U0001f48e\n\n'
                'Press \U0001f680 Find Partner to start chatting!')
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1
    await update.message.reply_text(
        f'\U0001f451 Done!\n\n'
        f'\u2705 VIP granted + notified: {sent}\n'
        f'\U0001f6ab Could not notify (blocked): {failed}\n'
        f'\U0001f4ca Total females upgraded: {len(rows)}')

async def delete_blocked_users(update, context):
    """Remove users who have blocked the bot to keep DB clean and fast."""
    if update.message.from_user.id != ADMIN_ID: return
    await update.message.reply_text('\U0001f50d Scanning for blocked users... this may take a while.')

    rows = await db_pool.fetch(
        'SELECT user_id FROM users WHERE user_id != $1', ADMIN_ID)

    blocked = 0; errors = 0
    for r in rows:
        uid = r['user_id']
        try:
            await context.bot.send_chat_action(chat_id=uid, action='typing')
            await asyncio.sleep(0.05)
        except Forbidden:
            # User blocked the bot — remove all their data
            for q in [
                'DELETE FROM waiting_users WHERE user_id=$1',
                'DELETE FROM active_chats  WHERE user_id=$1',
                'DELETE FROM active_chats  WHERE partner_id=$1',
                'DELETE FROM reports WHERE reporter_id=$1 OR reported_id=$1',
                'DELETE FROM chat_logs  WHERE sender_id=$1 OR partner_id=$1',
                'DELETE FROM users      WHERE user_id=$1',
            ]:
                await db_pool.execute(q, uid)
            blocked += 1
        except Exception:
            errors += 1  # deactivated account, network error etc — skip
        await asyncio.sleep(0.05)

    await update.message.reply_text(
        f'\u2705 Done!\n\n'
        f'\U0001f6ab Blocked & removed: {blocked}\n'
        f'\u26a0\ufe0f Skipped (other errors): {errors}\n'
        f'\U0001f4ca Checked: {len(rows)} users')

async def delete_blocked_users(update, context):
    """Delete all users who have blocked the bot."""
    if update.message.from_user.id != ADMIN_ID: return
    await update.message.reply_text('\U0001f50d Scanning for blocked users... This may take a moment.')
    rows = await db_pool.fetch(
        'SELECT user_id FROM users WHERE user_id != $1', ADMIN_ID)
    deleted = 0; checked = 0
    for r in rows:
        checked += 1
        try:
            await context.bot.send_chat_action(chat_id=r['user_id'], action='typing')
        except Forbidden:
            uid = r['user_id']
            # Remove from all tables cleanly
            for q in [
                'DELETE FROM waiting_users  WHERE user_id=$1',
                'DELETE FROM active_chats   WHERE user_id=$1',
                'DELETE FROM active_chats   WHERE partner_id=$1',
                'DELETE FROM chat_logs      WHERE sender_id=$1 OR partner_id=$1',
                'DELETE FROM reports        WHERE reporter_id=$1 OR reported_id=$1',
                'DELETE FROM users          WHERE user_id=$1',
            ]:
                await db_pool.execute(q, uid)
            deleted += 1
        except Exception:
            pass  # other errors (timeout etc) — skip, don't delete
        await asyncio.sleep(0.05)  # avoid hitting Telegram rate limit
    await update.message.reply_text(
        f'\u2705 Done!\n\n'
        f'\U0001f50d Checked:  {checked}\n'
        f'\U0001f5d1 Deleted:  {deleted} blocked user(s)\n'
        f'\U0001f464 Remaining: {checked - deleted}')

async def delete_blocked_command(update, context):
    """Delete all users who have blocked the bot."""
    if update.message.from_user.id != ADMIN_ID: return
    await update.message.reply_text('\U0001f50d Scanning for blocked users... This may take a minute.')
    rows = await db_pool.fetch(
        'SELECT user_id FROM users WHERE user_id != $1', ADMIN_ID)
    deleted = 0; checked = 0
    for r in rows:
        uid = r['user_id']
        try:
            await context.bot.send_chat_action(chat_id=uid, action='typing')
        except Forbidden:
            # User blocked the bot — delete all their data
            for q in [
                'DELETE FROM waiting_users WHERE user_id=$1',
                'DELETE FROM active_chats  WHERE user_id=$1',
                'DELETE FROM active_chats  WHERE partner_id=$1',
                'DELETE FROM reports       WHERE reporter_id=$1 OR reported_id=$1',
                'DELETE FROM chat_logs     WHERE sender_id=$1 OR partner_id=$1',
                'DELETE FROM users         WHERE user_id=$1',
            ]:
                await db_pool.execute(q, uid)
            deleted += 1
        except Exception:
            pass  # TimedOut or other — skip, don't delete
        checked += 1
        await asyncio.sleep(0.05)
    await update.message.reply_text(
        f'\u2705 Done!\n\n'
        f'\U0001f50d Checked: {checked}\n'
        f'\U0001f5d1 Deleted (blocked): {deleted}\n'
        f'\U0001f464 Remaining users: {checked - deleted}')

async def delete_blocked_users(update, context):
    """Remove users who have blocked the bot to keep DB clean and fast."""
    if update.message.from_user.id != ADMIN_ID: return
    await update.message.reply_text('\U0001f50d Scanning for blocked users... This may take a while.')

    rows = await db_pool.fetch(
        'SELECT user_id FROM users WHERE user_id != $1', ADMIN_ID)

    deleted = 0; checked = 0
    for r in rows:
        uid = r['user_id']
        checked += 1
        try:
            # send_chat_action is silent (no visible message to user)
            await context.bot.send_chat_action(chat_id=uid, action='typing')
            await asyncio.sleep(0.05)
        except Forbidden:
            # User blocked the bot — delete all their data
            for q in [
                'DELETE FROM waiting_users WHERE user_id=$1',
                'DELETE FROM active_chats  WHERE user_id=$1',
                'DELETE FROM active_chats  WHERE partner_id=$1',
                'DELETE FROM reports       WHERE reporter_id=$1 OR reported_id=$1',
                'DELETE FROM chat_logs     WHERE sender_id=$1 OR partner_id=$1',
                'DELETE FROM users         WHERE user_id=$1',
            ]:
                await db_pool.execute(q, uid)
            deleted += 1
        except Exception:
            pass  # TimedOut or other — skip, don't delete

    await update.message.reply_text(
        f'\u2705 Done!\n\n'
        f'\U0001f50d Checked: {checked}\n'
        f'\U0001f5d1 Deleted (blocked): {deleted}\n'
        f'\U0001f464 Remaining users: {checked - deleted}')

async def nudge_chats_command(update, context):
    """Send an icebreaker to all currently active chats to spark conversation."""
    if update.message.from_user.id != ADMIN_ID: return

    pairs = await db_pool.fetch(
        'SELECT DISTINCT LEAST(user_id,partner_id) as u1, GREATEST(user_id,partner_id) as u2 FROM active_chats')

    if not pairs:
        await update.message.reply_text('\u274c No active chats right now.'); return

    icebreakers = [
        '\U0001f4ac Icebreaker: Ask your partner — \"What\'s one thing that always makes you smile?\"',
        '\U0001f4ac Icebreaker: Ask your partner — \"What\'s the best thing that happened to you this week?\"',
        '\U0001f4ac Icebreaker: Ask your partner — \"If you could be anywhere right now, where would you be?\"',
        '\U0001f4ac Icebreaker: Ask your partner — \"What\'s something most people don\'t know about you?\"',
        '\U0001f4ac Icebreaker: Ask your partner — \"What\'s your go-to comfort when you\'re having a bad day?\"',
    ]

    import random as _random
    sent = 0
    for pair in pairs:
        msg = _random.choice(icebreakers)
        for uid in [pair['u1'], pair['u2']]:
            try:
                await context.bot.send_message(uid, msg)
                await asyncio.sleep(0.05)
            except Exception:
                pass
        sent += 1

    await update.message.reply_text(
        f'\U0001f4ac Sent icebreakers to {sent} active chat(s)!')

async def reset_db_command(update, context):
    """Nuclear reset — clears ALL user data, keeps table structure intact."""
    if update.message.from_user.id != ADMIN_ID: return

    # Safety confirmation check
    if not context.args or context.args[0] != 'CONFIRM':
        await update.message.reply_text(
            '\u26a0\ufe0f WARNING: This will delete ALL users, chats, reports and logs!\n\n'
            'To confirm type:\n/resetdb CONFIRM')
        return

    await update.message.reply_text('\U0001f5d1 Resetting database...')

    # Delete all data in correct order (FK constraints)
    for table in ['chat_logs', 'reports', 'active_chats', 'waiting_users', 'tod_usage', 'users']:
        await db_pool.execute(f'DELETE FROM {table}')

    # Re-insert admin
    await db_pool.execute(
        """INSERT INTO users(user_id, username, name, gender, is_vip)
           VALUES($1, $2, 'Admin', 'Male', TRUE)
           ON CONFLICT DO NOTHING""",
        ADMIN_ID, 'Admin')

    # Reset all in-memory state
    context_tod_counts.clear()

    await update.message.reply_text(
        '\u2705 Database reset complete!\n\n'
        '\U0001f5d1 All users deleted\n'
        '\U0001f5d1 All chats cleared\n'
        '\U0001f5d1 All reports cleared\n'
        '\U0001f5d1 All logs cleared\n\n'
        '\U0001f680 Bot is ready for fresh start!')

async def reset_database(update, context):
    """Complete database reset — clears all user data for fresh relaunch."""
    if update.message.from_user.id != ADMIN_ID: return

    # Confirm with a keyword to prevent accidental reset
    if not context.args or context.args[0] != 'CONFIRM':
        await update.message.reply_text(
            '\u26a0\ufe0f This will DELETE all users and data!\n\n'
            'To confirm type:\n/resetdb CONFIRM')
        return

    await update.message.reply_text('\U0001f5d1 Resetting database...')

    # Clear all tables except keeping structure
    for table in ['chat_logs', 'reports', 'active_chats', 'waiting_users', 'tod_usage']:
        await db_pool.execute(f'DELETE FROM {table}')

    # Keep admin, delete everyone else
    await db_pool.execute('DELETE FROM users WHERE user_id != $1', ADMIN_ID)

    # Reset admin stats
    await db_pool.execute(
        'UPDATE users SET total_messages=0, referral_count=0 WHERE user_id=$1',
        ADMIN_ID)

    await update.message.reply_text(
        '\u2705 Database reset complete!\n\n'
        '\U0001f5d1 Cleared:\n'
        '\u2022 All users (except admin)\n'
        '\u2022 All active chats\n'
        '\u2022 All waiting users\n'
        '\u2022 All reports\n'
        '\u2022 All chat logs\n\n'
        '\U0001f680 Bot is ready for fresh relaunch!')

async def debug_referral(update, context):
    if update.message.from_user.id != ADMIN_ID: return
    if not context.args or not context.args[0].isdigit():
        await update.message.reply_text('Usage: /debugref <id>'); return
    r = await db_pool.fetchrow(
        'SELECT user_id,name,gender,country,age,referred_by,referral_count,is_vip,vip_expiry FROM users WHERE user_id=$1',
        int(context.args[0]))
    if not r: await update.message.reply_text('Not found.'); return
    await update.message.reply_text(
        f"\U0001f50d Debug\n\nname:    {r['name']}\ngender:  {r['gender']}\n"
        f"country: {r['country']}\nage:     {r['age']}\n"
        f"ref_by:  {r['referred_by']}\nref_cnt: {r['referral_count']}\n"
        f"vip:     {r['is_vip']}\nexpiry:  {r['vip_expiry']}")


async def match_user(update, context, pref=None):
    uid = update.message.from_user.id
    if await get_partner(uid):
        await update.message.reply_text('Already in a chat. Use \u274c Stop first.'); return
    cancel_invite_timer(context)
    await db_pool.execute('DELETE FROM waiting_users WHERE user_id=$1', uid)
    r = await db_pool.fetchrow('SELECT gender FROM users WHERE user_id=$1', uid)
    my_gender = r['gender'] if r else None
    partner = None
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            if pref:
                crows = await conn.fetch(
                    '''SELECT w.user_id FROM waiting_users w JOIN users u ON w.user_id=u.user_id
                       WHERE u.gender=$1 AND (w.preferred_gender IS NULL OR w.preferred_gender=$2)
                         AND w.user_id!=$3 AND u.is_banned=FALSE
                         AND u.name IS NOT NULL AND u.gender IS NOT NULL
                         AND u.country IS NOT NULL AND u.age IS NOT NULL
                       ORDER BY w.queued_at LIMIT 5''',
                    pref, my_gender, uid)
            else:
                crows = await conn.fetch(
                    '''SELECT w.user_id FROM waiting_users w JOIN users u ON w.user_id=u.user_id
                       WHERE w.user_id!=$1 AND (w.preferred_gender IS NULL OR w.preferred_gender=$2)
                         AND u.is_banned=FALSE
                         AND u.name IS NOT NULL AND u.gender IS NOT NULL
                         AND u.country IS NOT NULL AND u.age IS NOT NULL
                       ORDER BY w.queued_at LIMIT 5''',
                    uid, my_gender)
            candidates = [r['user_id'] for r in crows]
            random.shuffle(candidates)
            for c in candidates:
                if not await conn.fetchval('SELECT pg_try_advisory_xact_lock($1)', c): continue
                if not await conn.fetchrow('SELECT 1 FROM waiting_users WHERE user_id=$1', c): continue
                await conn.execute('DELETE FROM waiting_users WHERE user_id=$1', c)
                # Clean any stale chat rows before inserting fresh ones
                await conn.execute('DELETE FROM active_chats WHERE user_id=$1', uid)
                await conn.execute('DELETE FROM active_chats WHERE user_id=$1', c)
                await conn.execute('INSERT INTO active_chats VALUES($1,$2)', uid, c)
                await conn.execute('INSERT INTO active_chats VALUES($1,$2)', c, uid)
                partner = c; break
    if partner:
        cancel_invite_timer(context)
        context_tod_counts.pop(uid, None); context_tod_counts.pop(partner, None)
        await context.bot.send_message(uid,
        '\u2705 You\'re now connected with a stranger!\n\n'
        '\U0001f4ac Say hi and start chatting \U0001f44b\n'
        '\U0001f512 Remember: Be kind & respectful')
        await context.bot.send_message(partner,
        '\u2705 You\'re now connected with a stranger!\n\n'
        '\U0001f4ac Say hi and start chatting \U0001f44b\n'
        '\U0001f512 Remember: Be kind & respectful')
        await context.bot.send_message(uid,
            '\U0001f3b2 Want to make this chat more fun?\n'
            'Play Truth or Dare with your partner \U0001f447',
            reply_markup=tod_inline(uid))
        await context.bot.send_message(partner,
            '\U0001f3b2 Want to make this chat more fun?\n'
            'Play Truth or Dare with your partner \U0001f447',
            reply_markup=tod_inline(partner))
    else:
        context.user_data['last_pref'] = pref  # fast in-session access
        await db_pool.execute('UPDATE users SET last_search_pref=$1 WHERE user_id=$2', pref, uid)
        await db_pool.execute(
            'INSERT INTO waiting_users(user_id,preferred_gender) VALUES($1,$2) ON CONFLICT(user_id) DO UPDATE SET preferred_gender=EXCLUDED.preferred_gender',
            uid, pref)
        label = f'Searching for a {pref} partner...' if pref else 'Searching for a partner...'
        await update.message.reply_text(
            f'\U0001f50e {label}\n\n'
            '\u23f3 Please wait while we find you a match...\n'
            'You can press \u274c Stop anytime to cancel.')
        async def invite_prompt():
            try:
                await asyncio.sleep(MATCH_INVITE_DELAY)
                if await db_pool.fetchrow('SELECT 1 FROM waiting_users WHERE user_id=$1', uid):
                    link = f'https://t.me/{context.bot.username}?start={uid}'
                    await context.bot.send_message(uid,
                        f'\U0001f50e Still searching?\n\nInvite {VIP_REFERRAL_THRESHOLD} friends and unlock '
                        f'\U0001f451 VIP for {VIP_REFERRAL_DAYS} days!\n\nYour link:\n{link}')
            except asyncio.CancelledError: pass
            finally: context.user_data.pop('invite_task', None)
        context.user_data['invite_task'] = asyncio.create_task(invite_prompt())

async def stop_chat(update, context):
    uid = update.message.from_user.id
    await db_pool.execute('DELETE FROM waiting_users WHERE user_id=$1', uid)
    cancel_invite_timer(context)
    partner = await get_partner(uid)
    if not partner:
        await update.message.reply_text(
            '\u26d4 Search stopped.\n\n'
            'Press \U0001f680 Find Partner whenever you\'re ready!',
            reply_markup=get_main_keyboard(uid))
        return False
    await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', uid)
    await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', partner)
    context_tod_counts.pop(uid, None); context_tod_counts.pop(partner, None)
    # Quitter side
    quitter_msg = (
        '\U0001f44b You left the chat.\n\n'
        '💭 Hope you had a good conversation!\n'
        'Want to meet someone new?\n\n'
        'Press \U0001f680 Find Partner to continue chatting.'
    )
    report_markup_for_quitter = InlineKeyboardMarkup([[
        InlineKeyboardButton('\u26a0\ufe0f Report', callback_data=f'report:{partner}'),
    ]])
    await update.message.reply_text(quitter_msg, reply_markup=get_main_keyboard(uid))
    await update.message.reply_text('Was there a problem?', reply_markup=report_markup_for_quitter)

    # Partner side
    partner_msg = (
        '\U0001f44b Your partner left the chat.\n\n'
        '💭 Every stranger is a new adventure!\n'
        'Want to meet someone new?\n\n'
        'Press \U0001f680 Find Partner to continue chatting.'
    )
    report_markup_for_partner = InlineKeyboardMarkup([[
        InlineKeyboardButton('\u26a0\ufe0f Report', callback_data=f'report:{uid}'),
    ]])
    try:
        await context.bot.send_message(partner, partner_msg, reply_markup=get_main_keyboard(partner))
        await context.bot.send_message(partner, 'Was there a problem?', reply_markup=report_markup_for_partner)
    except Exception as e:
        logger.warning('Could not notify partner %s: %s', partner, e)
    return True


async def report_callback(update, context):
    q = update.callback_query; uid = q.from_user.id
    await q.answer()
    try: partner = int(q.data.split(':')[1])
    except: await q.edit_message_text('\u26a0\ufe0f Invalid report.'); return
    existing = await db_pool.fetchrow(
        'SELECT 1 FROM reports WHERE reporter_id=$1 AND reported_id=$2 AND created_at>NOW()-INTERVAL \'1 hour\'',
        uid, partner)
    if existing: await q.edit_message_text('You already reported this user recently.'); return
    await db_pool.execute('INSERT INTO reports(reporter_id,reported_id) VALUES($1,$2)', uid, partner)
    await q.edit_message_text('\u2705 Report submitted. Our team will review it shortly.')

async def tod_callback(update, context):
    q = update.callback_query; uid = q.from_user.id; data = q.data
    if data.startswith('tod_start:'):
        init_uid = int(data.split(':')[1])
        if uid != init_uid:
            await q.answer('Not your button!', show_alert=True); return
        await q.answer()
        partner = await get_partner(uid)
        if not partner: await q.edit_message_text('\u26a0\ufe0f Not in a chat.'); return
        count = context_tod_counts.get(uid, 0)
        is_vip = uid == ADMIN_ID or await check_vip(uid)
        if not is_vip and count >= FREE_TOD_LIMIT:
            await q.answer(f'Free users get {FREE_TOD_LIMIT} rounds per chat. Get \U0001f48e VIP for unlimited!', show_alert=True); return
        await q.edit_message_text('\U0001f3b2 You challenged your partner!')
        try: await context.bot.send_message(partner, '\U0001f3b2 Stranger challenged you!\n\nYou pick:', reply_markup=tod_choice_inline(uid))
        except: pass
    elif data.startswith('tod_pick:'):
        await q.answer()
        parts = data.split(':'); choice = parts[1]; init_uid = int(parts[2])
        partner = await get_partner(uid)
        if not partner or partner != init_uid: await q.edit_message_text('\u26a0\ufe0f Chat ended.'); return
        if choice == 'truth':
            msg = f'\U0001f607 *Truth:*\n\n{random.choice(TRUTH_QUESTIONS)}'
        else:
            msg = f'\U0001f608 *Dare:*\n\n{random.choice(DARE_CHALLENGES)}'
        context_tod_counts[init_uid] = context_tod_counts.get(init_uid, 0) + 1
        again = tod_again_inline(init_uid)
        # Use send_message for BOTH so the result appears as the newest message,
        # not buried in history via edit. Also delete the choice buttons cleanly.
        try: await q.edit_message_reply_markup(reply_markup=None)
        except: pass
        try: await context.bot.send_message(uid,      msg, parse_mode='Markdown', reply_markup=again)
        except: pass
        try: await context.bot.send_message(init_uid, msg, parse_mode='Markdown', reply_markup=again)
        except: pass


async def admin_report_callback(update, context):
    q = update.callback_query
    if q.from_user.id != ADMIN_ID: await q.answer('Not authorized.', show_alert=True); return
    await q.answer()
    parts = q.data.split(':')
    if len(parts) < 3:
        await q.edit_message_text('\u26a0\ufe0f Invalid report data.'); return
    reported_id = int(parts[1]); reporter_id = int(parts[2])
    ur = await db_pool.fetchrow('SELECT name,username,is_banned,total_messages FROM users WHERE user_id=$1', reported_id)
    name     = (ur['name']     or 'Unknown') if ur else 'Unknown'
    username = (ur['username'] or 'no username') if ur else 'no username'
    banned   = ur['is_banned'] if ur else False
    logs = await db_pool.fetch(
        '''SELECT sender_id,message,created_at FROM chat_logs
           WHERE (sender_id=$1 AND partner_id=$2) OR (sender_id=$2 AND partner_id=$1)
           ORDER BY created_at DESC LIMIT 10''',
        reported_id, reporter_id)
    lines = []
    for log in reversed(logs):
        who  = '\U0001f534 Reported' if log['sender_id']==reported_id else '\U0001f7e2 Reporter'
        t    = log['created_at'].strftime('%H:%M')
        lines.append(f'{who} [{t}]: {log["message"]}')
    history = '\n'.join(lines) if lines else 'No messages logged yet.'
    total = await db_pool.fetchval('SELECT COUNT(*) FROM reports WHERE reported_id=$1', reported_id)
    msg = (f'\U0001f6a8 Report Review\n\n'
           f'Reported: {name} (@{username})\n'
           f'User ID: {reported_id}\n'
           f'Total reports: {total}\n'
           f'Banned: {chr(0x2705)+" Yes" if banned else chr(0x274c)+" No"}\n\n'
           f'\U0001f4dd Last messages:\n{"─"*28}\n{history}')
    action = (InlineKeyboardButton('\u2705 Unban', callback_data=f'admin_ban:unban:{reported_id}')
              if banned else
              InlineKeyboardButton('\U0001f6ab Ban', callback_data=f'admin_ban:ban:{reported_id}'))
    markup = InlineKeyboardMarkup([[
        action,
        InlineKeyboardButton('\U0001f5d1 Delete Report', callback_data=f'admin_del_report:{reported_id}'),
        InlineKeyboardButton('\U0001f519 Back', callback_data='admin_back_reports'),
    ]])
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
            try: await context.bot.send_message(p, '\U0001f44b Stranger left the chat.')
            except: pass
        await db_pool.execute('DELETE FROM waiting_users WHERE user_id=$1', tid)
        try: await context.bot.send_message(tid, '\U0001f6ab You have been banned.')
        except: pass
        await q.edit_message_text(f'\u2705 User {tid} banned.')
    else:
        await db_pool.execute('UPDATE users SET is_banned=FALSE WHERE user_id=$1', tid)
        try: await context.bot.send_message(tid, '\u2705 Your ban has been lifted.')
        except: pass
        await q.edit_message_text(f'\u2705 User {tid} unbanned.')

async def admin_del_report_callback(update, context):
    """Delete all reports for a reported user."""
    q = update.callback_query
    if q.from_user.id != ADMIN_ID: await q.answer('Not authorized.', show_alert=True); return
    await q.answer()
    reported_id = int(q.data.split(':')[1])
    deleted = await db_pool.fetchval('SELECT COUNT(*) FROM reports WHERE reported_id=$1', reported_id)
    await db_pool.execute('DELETE FROM reports WHERE reported_id=$1', reported_id)
    await q.edit_message_text(f'\u2705 Deleted {deleted} report(s) for user {reported_id}.')

async def admin_back_reports_callback(update, context):
    q = update.callback_query
    if q.from_user.id != ADMIN_ID: return
    await q.answer()
    rows = await db_pool.fetch(
        '''SELECT r.reporter_id, r.reported_id, u.name, u.username,
                  (SELECT COUNT(*) FROM reports WHERE reported_id=r.reported_id) AS total
           FROM reports r LEFT JOIN users u ON u.user_id=r.reported_id
           ORDER BY r.created_at DESC LIMIT 10''')
    if not rows: await q.edit_message_text('\u2705 No reports.'); return
    buttons = []
    for r in rows:
        n = r['name'] or 'Unknown'
        buttons.append([InlineKeyboardButton(
            f"\U0001f6a8 {n} ({r['total']} reports) \u2014 ID:{r['reported_id']}",
            callback_data=f"admin_report:{r['reported_id']}:{r['reporter_id']}")])
    await q.edit_message_text('\U0001f6a8 Recent Reports:', reply_markup=InlineKeyboardMarkup(buttons))


async def admin_end_chat_callback(update, context):
    """Admin ends a specific active chat from Live Chats panel."""
    q = update.callback_query
    if q.from_user.id != ADMIN_ID:
        await q.answer('Not authorized.', show_alert=True); return
    await q.answer()
    parts = q.data.split(':')
    u1, u2 = int(parts[1]), int(parts[2])
    # End the chat for both users
    await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', u1)
    await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', u2)
    context_tod_counts.pop(u1, None); context_tod_counts.pop(u2, None)
    # Notify both users
    end_msg = ('\u274c Your chat has been ended by the admin.\n\n'
               'Press \U0001f680 Find Partner to start a new chat.')
    for uid in [u1, u2]:
        try:
            r = await db_pool.fetchrow('SELECT name FROM users WHERE user_id=$1', uid)
            await context.bot.send_message(uid, end_msg)
        except Exception:
            pass
    await q.edit_message_text(f'\u2705 Chat between {u1} and {u2} has been ended.')


async def handle_pre_checkout(update, context):
    """Always approve pre-checkout for VIP purchases."""
    query = update.pre_checkout_query
    await query.answer(ok=True)

async def handle_successful_payment(update, context):
    """Grant VIP after successful Stars payment."""
    uid     = update.message.from_user.id
    payment = update.message.successful_payment
    payload = payment.invoice_payload

    if payload == 'vip_week':
        days  = 7
        label = '7 days'
    elif payload == 'vip_month':
        days  = 30
        label = '30 days'
    else:
        return

    await grant_vip(uid, days)
    await update.message.reply_text(
        f'\u2705 Payment received! \u2b50\n\n'
        f'\U0001f451 Your VIP is now active for {label}!\n\n'
        f'You can now:\n'
        f'\u2022 Filter matches by gender\n'
        f'\u2022 Unlimited Truth or Dare\n\n'
        f'Enjoy your VIP! \U0001f48e',
        reply_markup=user_keyboard)


async def precheckout_callback(update, context):
    """Approve all pre-checkout queries."""
    query = update.pre_checkout_query
    await query.answer(ok=True)


async def successful_payment_callback(update, context):
    """Grant VIP after successful Stars payment."""
    uid     = update.message.from_user.id
    payload = update.message.successful_payment.invoice_payload

    # Parse days from payload: vip_week_uid_7 or vip_month_uid_30
    parts = payload.split('_')
    days  = int(parts[-1]) if parts[-1].isdigit() else 7

    await grant_vip(uid, days)

    label = '7 days' if days == 7 else '30 days'
    await update.message.reply_text(
        f'\U0001f389 Payment successful!\n\n'
        f'\U0001f451 VIP activated for {label}!\n\n'
        f'You can now:\n'
        f'\u2022 Filter matches by gender\n'
        f'\u2022 Unlimited Truth or Dare\n'
        f'\u2022 VIP priority matching\n\n'
        f'Enjoy your VIP! \U0001f48e',
        reply_markup=get_main_keyboard(uid))


async def testvip_command(update, context):
    """Admin only — send a 1-Star test invoice to verify payment flow works."""
    if update.message.from_user.id != ADMIN_ID: return
    uid = update.message.from_user.id
    await context.bot.send_invoice(
        chat_id     = uid,
        title       = '\U0001f9ea Test VIP Payment',
        description = 'Admin test — pays 1 Star, grants 1 day VIP to verify payment flow.',
        payload     = f'vip_test_{uid}_1',
        currency    = 'XTR',
        prices      = [{'label': 'Test VIP (1 day)', 'amount': 1}],
    )
    await update.message.reply_text(
        '\U0001f9ea Test invoice sent!\n\n'
        'Pay 1 Star to test the full payment flow.\n'
        'VIP will be granted for 1 day on success.')



async def precheckout_callback(update, context):
    """Approve all VIP pre-checkout queries."""
    query = update.pre_checkout_query
    if query.invoice_payload in ('vip_week', 'vip_month', 'vip_test'):
        await query.answer(ok=True)
    else:
        await query.answer(ok=False, error_message='Unknown payment.')

async def successful_payment_handler(update, context):
    """Grant VIP after successful Stars payment."""
    uid     = update.message.from_user.id
    payment = update.message.successful_payment
    payload = payment.invoice_payload

    if payload == 'vip_week':
        days  = 7
        label = '7 days'
    elif payload == 'vip_month':
        days  = 30
        label = '30 days'
    elif payload == 'vip_test':
        days  = 1
        label = '1 day (test)'
    else:
        return

    await grant_vip(uid, days)

    # Store payment charge ID for potential refunds
    await db_pool.execute(
        'UPDATE users SET last_payment_id=$1 WHERE user_id=$2',
        payment.telegram_payment_charge_id, uid)

    await update.message.reply_text(
        f'\U0001f389 Payment successful!\n\n'
        f'\U0001f451 VIP activated for {label}!\n\n'
        f'You can now:\n'
        f'\u2022 Filter by gender (Find Male / Find Female)\n'
        f'\u2022 Unlimited Truth or Dare \U0001f3b2\n'
        f'\u2022 Priority matching\n\n'
        f'Enjoy your VIP! \U0001f48e',
        reply_markup=get_main_keyboard(uid))

async def buy_vip_callback(update, context):
    """User tapped a VIP package — send Telegram Stars invoice."""
    q = update.callback_query
    uid = q.from_user.id
    await q.answer()

    parts   = q.data.split(':')
    pkg_key = parts[1]   # 'week', 'month', 'test'
    pkg     = VIP_PACKAGES.get(pkg_key)
    if not pkg:
        await q.edit_message_text('\u26a0\ufe0f Invalid package.'); return

    # Only admin can use test package
    if pkg_key == 'test' and uid != ADMIN_ID:
        await q.answer('Not available.', show_alert=True); return

    try:
        # Build description based on package
        if pkg_key == 'week':
            desc = (
                "WHAT YOU GET:\n"
                "• Match with Males or Females only\n"
                "• Unlimited Truth or Dare rounds\n"
                "• Top priority in matching queue\n"
                "• VIP badge shown to your partner\n\n"
                "Duration: 7 days\n"
                "Activates instantly after payment"
            )
        elif pkg_key == 'month':
            desc = (
                "WHAT YOU GET:\n"
                "• Match with Males or Females only\n"
                "• Unlimited Truth or Dare rounds\n"
                "• Top priority in matching queue\n"
                "• VIP badge shown to your partner\n\n"
                "Duration: 30 days — Best Value!\n"
                "Activates instantly after payment"
            )
        else:
            desc = "Admin test payment — 1 Star only"

        await context.bot.send_invoice(
            chat_id=uid,
            title=f"{pkg['emoji']} {pkg['label']} — Fun Bot",
            description=desc,
            payload=f"vip_{pkg_key}_{uid}",
            provider_token="",   # empty for Telegram Stars
            currency="XTR",
            prices=[{"label": pkg['label'], "amount": pkg['stars']}],
        )
        await q.edit_message_text(
            f"{pkg['emoji']} Invoice sent!\n\n"
            f"Complete the payment to activate your {pkg['label']}.")
    except Exception as e:
        logger.error('Invoice error for %s: %s', uid, e)
        await q.edit_message_text('\u26a0\ufe0f Could not create invoice. Try again later.')


async def pre_checkout_handler(update, context):
    """Approve all pre-checkout queries for VIP purchases."""
    query = update.pre_checkout_query
    # Verify payload format
    if query.invoice_payload.startswith('vip_'):
        await query.answer(ok=True)
    else:
        await query.answer(ok=False, error_message='Invalid payment.')


async def successful_payment_handler(update, context):
    """Payment confirmed — grant VIP immediately."""
    payment = update.message.successful_payment
    uid     = update.message.from_user.id
    payload = payment.invoice_payload  # e.g. 'vip_week_12345'

    parts   = payload.split('_')
    pkg_key = parts[1] if len(parts) >= 2 else None
    pkg     = VIP_PACKAGES.get(pkg_key)

    if not pkg:
        logger.error('Unknown VIP payload: %s for uid %s', payload, uid)
        return

    await grant_vip(uid, pkg['days'])

    # Store payment charge ID for potential refunds
    charge_id = payment.telegram_payment_charge_id
    logger.info('VIP payment: uid=%s pkg=%s stars=%s charge=%s',
                uid, pkg_key, pkg['stars'], charge_id)

    expiry_row = await db_pool.fetchrow(
        'SELECT vip_expiry FROM users WHERE user_id=$1', uid)
    expiry_str = (expiry_row['vip_expiry'].strftime('%d %b %Y')
                  if expiry_row and expiry_row['vip_expiry'] else 'Permanent')

    await update.message.reply_text(
        f"{pkg['emoji']} *Payment successful!*\n\n"
        f"\U0001f451 {pkg['label']} is now active!\n"
        f"\U0001f4c5 Expires: {expiry_str}\n\n"
        f"\u2705 Gender filter unlocked\n"
        f"\u2705 Unlimited Truth or Dare\n\n"
        f"Enjoy your VIP! \U0001f48e",
        parse_mode='Markdown',
        reply_markup=get_main_keyboard(uid))

async def find_new_callback(update, context):
    """Find New inline button — just answer the query, user uses keyboard to search."""
    q = update.callback_query
    await q.answer('Press \U0001f680 Find Partner to search!', show_alert=False)

async def announce_target_callback(update, context):
    q = update.callback_query
    if q.from_user.id != ADMIN_ID: await q.answer('Not authorized.', show_alert=True); return
    await q.answer()
    target = q.data.split(':')[1]  # 'all' or 'female'
    context.user_data['announce_mode'] = True
    context.user_data['announce_target'] = target
    label = '👩 females only' if target == 'female' else '👥 all users'
    await q.edit_message_text(f'Sending to {label}.\n\nNow send your announcement message:')

async def router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message: return
    uid  = update.message.from_user.id
    text = update.message.text or ''

    if await is_banned_check(uid):
        await update.message.reply_text('\U0001f6ab You are banned from using this bot.')
        return

    # Keep username in sync — Telegram usernames can change anytime
    current_username = update.message.from_user.username
    if current_username and context.user_data.get('last_username') != current_username:
        context.user_data['last_username'] = current_username
        await db_pool.execute(
            'UPDATE users SET username=$1 WHERE user_id=$2',
            current_username, uid)

    # ── REGISTRATION GATE ── ALL 4 fields required before anything works
    if uid != ADMIN_ID:
        # Gate fires if:
        # (a) user has a row but is incomplete, OR
        # (b) user has no row yet but has a step in context (mid-registration, no DB row yet)
        in_registration = context.user_data.get('step') is not None
        # Use cached registration status to avoid DB hit on every message
        if not in_registration and not context.user_data.get('registered'):
            if await user_exists(uid):
                if await is_registered(uid):
                    context.user_data['registered'] = True  # cache — skip check next time
                else:
                    in_registration = True  # has row but incomplete — enter gate
            # else: no row at all — handled below
        if in_registration:
            # Only recover step from DB if context was wiped (bot restart)
            # Don't overwrite if step is already correctly set in context
            if context.user_data.get('step') is None:
                db_step = await get_registration_step(uid)
                context.user_data['step'] = db_step or 'name'
                logger.info('Recovered step=%s uid=%s', context.user_data['step'], uid)
        if in_registration:
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
                    await update.message.reply_text('\U0001f44b Please enter your name to continue:')
                return
            if step == 'gender':
                if text in ('Male','Female'):
                    await db_pool.execute('UPDATE users SET gender=$1 WHERE user_id=$2', text, uid)
                    context.user_data['step'] = 'country'
                    await update.message.reply_text('Enter your country:', reply_markup=ReplyKeyboardRemove())
                else:
                    await update.message.reply_text('Please select your gender:', reply_markup=gender_keyboard)
                return
            if step == 'country':
                if text and text not in BUTTON_TEXTS:
                    await db_pool.execute('UPDATE users SET country=$1 WHERE user_id=$2', text, uid)
                    context.user_data['step'] = 'age'
                    await update.message.reply_text('Enter your age:')
                else:
                    await update.message.reply_text('Please enter your country:')
                return
            if step == 'age':
                if text and text.isdigit() and 5 <= int(text) <= 120:
                    try: await db_pool.execute('UPDATE users SET age=$1 WHERE user_id=$2', int(text), uid)
                    except Exception as e:
                        logger.error('age save error %s: %s', uid, e)
                        await update.message.reply_text('Error saving. Try again.'); return
                    context.user_data.clear()
                    context.user_data['registered'] = True  # cache — skip DB check on every future message
                    await update.message.reply_text(
                        'Registration complete \U0001f389\n\nUse the buttons below to find a chat partner!',
                        reply_markup=user_keyboard)
                    try:
                        rr = await db_pool.fetchrow('SELECT referred_by, referral_processed FROM users WHERE user_id=$1', uid)
                        referrer = rr['referred_by'] if rr else None
                        already_processed = rr['referral_processed'] if rr else True
                        if referrer and not already_processed:
                            # Mark as processed FIRST to prevent any race condition double-fire
                            await db_pool.execute('UPDATE users SET referral_processed=TRUE WHERE user_id=$1', uid)
                            vip_granted = await handle_referral(uid, referrer)
                            if vip_granted:
                                try: await context.bot.send_message(referrer, f'\U0001f389 You earned {VIP_REFERRAL_DAYS} days of \U0001f451 VIP for inviting friends!')
                                except: pass
                    except Exception as e: logger.error('referral error %s: %s', uid, e)
                else:
                    await update.message.reply_text('Please enter a valid age (5\u2013120):')
                return
            # Unknown step — restart
            context.user_data['step'] = 'name'
            await update.message.reply_text('Please enter your name to continue:')
            return
        elif not await user_exists(uid) and not context.user_data.get('step'):
            await update.message.reply_text('Please send /start to begin.', reply_markup=ReplyKeyboardRemove())
            return

    # ── ADMIN PANEL ──
    if text == '\u2699\ufe0f Admin Panel' and uid == ADMIN_ID:
        context.user_data['in_admin_panel'] = True
        await update.message.reply_text('\u2699\ufe0f Admin Panel', reply_markup=admin_panel_keyboard)
        return

    if uid == ADMIN_ID and context.user_data.get('in_admin_panel'):
        if text == '\U0001f4ca Analytics':
            total  = await db_pool.fetchval('SELECT COUNT(*) FROM users')
            active = await db_pool.fetchval('SELECT COUNT(DISTINCT LEAST(user_id,partner_id)) FROM active_chats') or 0
            wait   = await db_pool.fetchval('SELECT COUNT(*) FROM waiting_users')
            vips   = await db_pool.fetchval("SELECT COUNT(*) FROM users WHERE (is_vip=TRUE AND vip_expiry IS NULL) OR vip_expiry>NOW()")
            bans   = await db_pool.fetchval('SELECT COUNT(*) FROM users WHERE is_banned=TRUE')
            reps   = await db_pool.fetchval('SELECT COUNT(*) FROM reports')
            await update.message.reply_text(
                f'\U0001f4ca Analytics\n\n\U0001f464 Users: {total}\n\U0001f4ac Active chats: {active}\n'
                f'\U0001f50e Waiting: {wait}\n\U0001f451 VIPs: {vips}\n\U0001f6ab Banned: {bans}\n\U0001f6a8 Reports: {reps}')
            return
        if text == '\U0001f465 Active Users':
            n = await db_pool.fetchval('SELECT COUNT(DISTINCT LEAST(user_id,partner_id)) FROM active_chats') or 0
            await update.message.reply_text(f'\U0001f4ac Active chats: {n}'); return
        if text == '\U0001f552 Waiting Users':
            n = await db_pool.fetchval('SELECT COUNT(*) FROM waiting_users')
            await update.message.reply_text(f'\U0001f50e Waiting: {n}'); return
        if text == '\U0001f6a8 Reports':
            rows = await db_pool.fetch(
                '''SELECT r.reporter_id, r.reported_id, r.created_at, u.name, u.username,
                          (SELECT COUNT(*) FROM reports WHERE reported_id=r.reported_id) AS total
                   FROM reports r LEFT JOIN users u ON u.user_id=r.reported_id
                   ORDER BY r.created_at DESC LIMIT 10''')
            if not rows: await update.message.reply_text('\u2705 No reports yet.'); return
            buttons = []
            for r in rows:
                n = r['name'] or 'Unknown'
                buttons.append([InlineKeyboardButton(
                    f"\U0001f6a8 {n} ({r['total']} reports) \u2014 ID:{r['reported_id']}",
                    callback_data=f"admin_report:{r['reported_id']}:{r['reporter_id']}")])
            await update.message.reply_text('\U0001f6a8 Recent Reports (tap to review):', reply_markup=InlineKeyboardMarkup(buttons))
            return
        if text == '\U0001f4f1 Live Chats':
            pairs = await db_pool.fetch(
                '''SELECT DISTINCT LEAST(user_id,partner_id) AS u1,
                          GREATEST(user_id,partner_id) AS u2
                   FROM active_chats ORDER BY u1''')
            if not pairs:
                await update.message.reply_text('\u274c No active chats right now.')
                return
            buttons = []
            lines   = ['\U0001f4f1 Live Chats\n']
            for i, pair in enumerate(pairs):
                u1, u2 = pair['u1'], pair['u2']
                r1 = await db_pool.fetchrow('SELECT name, gender FROM users WHERE user_id=$1', u1)
                r2 = await db_pool.fetchrow('SELECT name, gender FROM users WHERE user_id=$1', u2)
                n1 = (r1['name'] or '?') if r1 else '?'
                n2 = (r2['name'] or '?') if r2 else '?'
                g1 = ('\U0001f468' if (r1 and r1['gender']=='Male') else '\U0001f469') if r1 else '\U0001f464'
                g2 = ('\U0001f468' if (r2 and r2['gender']=='Male') else '\U0001f469') if r2 else '\U0001f464'
                lines.append(f'{i+1}. {g1} {n1}  \u2194\ufe0f  {g2} {n2}')
                buttons.append([InlineKeyboardButton(
                    f'\U0001f6ab End chat #{i+1}: {n1} & {n2}',
                    callback_data=f'admin_end_chat:{u1}:{u2}'
                )])
            await update.message.reply_text(
                '\n'.join(lines),
                reply_markup=InlineKeyboardMarkup(buttons))
            return

        if text == '\U0001f451 VIP Users':
            rows = await db_pool.fetch(
                "SELECT user_id,username,name,vip_expiry,referral_count FROM users WHERE (is_vip=TRUE AND vip_expiry IS NULL) OR vip_expiry>NOW() ORDER BY vip_expiry ASC NULLS FIRST LIMIT 20")
            if not rows: await update.message.reply_text('No active VIP users.'); return
            lines = ['\U0001f451 Active VIP Users\n']
            for r in rows:
                exp = 'Permanent \u267e\ufe0f' if r['vip_expiry'] is None else r['vip_expiry'].strftime('%Y-%m-%d')
                lines.append(f"• {r['name']} (@{r['username'] or '?'})\n  ID:{r['user_id']} Refs:{r['referral_count']} Exp:{exp}")
            await update.message.reply_text('\n'.join(lines)); return
        if text == '\U0001f9f9 Clean Dead Chats':
            await update.message.reply_text('\U0001f50d Scanning...')
            n = await clean_dead_chats(context.bot)
            await update.message.reply_text(f'\u2705 Removed {n} dead chat(s).'); return
        if text == '\U0001f4e2 Announcement':
            # Show target selection inline buttons
            markup = InlineKeyboardMarkup([[
                InlineKeyboardButton('\U0001f465 All Users',    callback_data='announce_target:all'),
                InlineKeyboardButton('\U0001f469 Females Only', callback_data='announce_target:female'),
            ]])
            await update.message.reply_text('Who do you want to send to?', reply_markup=markup); return
        if context.user_data.get('announce_mode'):
            if text == '\u2b05\ufe0f Back':
                context.user_data.pop('announce_mode', None)
                context.user_data.pop('announce_target', None)
                await update.message.reply_text('\u274c Announcement cancelled.', reply_markup=admin_panel_keyboard); return
            target = context.user_data.get('announce_target', 'all')
            context.user_data.pop('announce_mode', None)
            context.user_data.pop('announce_target', None)
            if target == 'female':
                rows = await db_pool.fetch(
                    "SELECT user_id FROM users WHERE is_banned=FALSE AND name IS NOT NULL AND age IS NOT NULL AND gender='Female'")
            else:
                rows = await db_pool.fetch(
                    'SELECT user_id FROM users WHERE is_banned=FALSE AND name IS NOT NULL AND age IS NOT NULL')
            sent = 0
            blocked = 0
            for r in rows:
                try:
                    await update.message.copy(chat_id=r['user_id'])
                    sent += 1
                    await asyncio.sleep(0.05)
                except Exception as e:
                    blocked += 1
                    if 'Retry After' in str(e) or 'retry_after' in str(e).lower():
                        import re as _re
                        wait = int(_re.search(r'\d+', str(e)).group() or 5)
                        await asyncio.sleep(wait)
                        try:
                            await update.message.copy(chat_id=r['user_id'])
                            sent += 1; blocked -= 1
                        except: pass
            label = 'female users' if target == 'female' else 'all users'
            await update.message.reply_text(
                f'\U0001f4e2 Sent to {sent} {label}.\n'
                f'\U0001f6ab Blocked/unreachable: {blocked}'); return
        if text == '\u2b05\ufe0f Back':
            context.user_data['in_admin_panel'] = False
            context.user_data.pop('announce_mode', None)
            await update.message.reply_text('Main menu', reply_markup=admin_main_keyboard); return

    # ── SHARED BUTTONS ──
    if text == '\U0001f680 Find Partner': context.user_data['last_pref'] = None; await match_user(update, context); return
    if text == '\U0001f468 Find Male':
        if uid == ADMIN_ID or await check_vip(uid): context.user_data['last_pref'] = 'Male'; await match_user(update, context, 'Male')
        else: await update.message.reply_text('\U0001f451 VIP required to filter by gender.\n\nUse \U0001f48e VIP to learn more.')
        return
    if text == '\U0001f469 Find Female':
        if uid == ADMIN_ID or await check_vip(uid): context.user_data['last_pref'] = 'Female'; await match_user(update, context, 'Female')
        else: await update.message.reply_text('\U0001f451 VIP required to filter by gender.\n\nUse \U0001f48e VIP to learn more.')
        return
    if text == '\u23ed\ufe0f Next': await stop_chat(update, context); await match_user(update, context); return
    if text == '\u274c Stop': await stop_chat(update, context); return

    if text == '\U0001f48e VIP':
        if uid == ADMIN_ID:
            await update.message.reply_text('\U0001f451 VIP Status: \u2705 Active\nExpires: Permanent \u267e\ufe0f', reply_markup=vip_keyboard); return
        r = await db_pool.fetchrow('SELECT is_vip,vip_expiry,referral_count FROM users WHERE user_id=$1', uid)
        active = await check_vip(uid)
        cnt    = r['referral_count'] if r else 0
        prog   = cnt % VIP_REFERRAL_THRESHOLD
        rem    = VIP_REFERRAL_THRESHOLD - prog
        if active:
            exp_s = r['vip_expiry'].strftime('%d %b %Y, %H:%M UTC') if r and r['vip_expiry'] else 'Permanent \u267e\ufe0f'
            status_msg = (
                f'\U0001f451 VIP Status: \u2705 Active\n'
                f'Expires: {exp_s}\n\n'
                f'\U0001f538 VIP Perks:\n'
                f'\u2022 Filter by gender (Find Male/Female)\n'
                f'\u2022 Unlimited Truth or Dare\n\n'
                f'\U0001f504 Next free VIP in {rem} more referral(s)'
            )
        elif r and r['vip_expiry']:
            status_msg = (
                f'\U0001f451 VIP Status: \u23f0 Expired\n'
                f'Expired: {r["vip_expiry"].strftime("%d %b %Y")}\n\n'
                f'Renew now \u2b50 or invite {rem} friends for free!'
            )
        else:
            status_msg = (
                f'\U0001f451 VIP Status: \u274c Inactive\n\n'
                f'\U0001f538 VIP gives you:\n'
                f'\u2022 Filter by gender (Find Male / Find Female)\n'
                f'\u2022 Unlimited Truth or Dare rounds\n\n'
                f'Buy instantly with Stars \u2b50\n'
                f'Or get FREE via referrals \U0001f381'
            )
        await update.message.reply_text(status_msg, reply_markup=vip_buy_inline())
        await update.message.reply_text('\U0001f381 Want FREE VIP instead? Invite friends:', reply_markup=vip_keyboard)
        return


    if text == '\U0001f381 Get FREE VIP':
        r = await db_pool.fetchrow('SELECT referral_count FROM users WHERE user_id=$1', uid)
        cnt  = r['referral_count'] if r else 0
        link = f'https://t.me/{context.bot.username}?start={uid}'
        prog = cnt % VIP_REFERRAL_THRESHOLD; rem = VIP_REFERRAL_THRESHOLD - prog
        await update.message.reply_text(
            f'\U0001f381 Invite friends to get FREE VIP!\n\nYour link:\n{link}\n\n'
            f'\U0001f4ca Referrals: {cnt}\n\U0001f504 Progress: {prog}/{VIP_REFERRAL_THRESHOLD}\n'
            f'\U0001f3c6 VIPs earned: {cnt//VIP_REFERRAL_THRESHOLD}\n\n'
            f'Invite {rem} more for \U0001f451 VIP ({VIP_REFERRAL_DAYS} days)!')
        return

    if text == '\u2b50 Buy VIP':
        buttons = [
            [InlineKeyboardButton(
                f"\u2b50 {VIP_PACKAGES['week']['label']} — {VIP_PACKAGES['week']['stars']} Stars",
                callback_data='buy_vip:week')],
            [InlineKeyboardButton(
                f"\U0001f31f {VIP_PACKAGES['month']['label']} — {VIP_PACKAGES['month']['stars']} Stars",
                callback_data='buy_vip:month')],
        ]
        if uid == ADMIN_ID:
            buttons.append([InlineKeyboardButton(
                f"\U0001f9ea {VIP_PACKAGES['test']['label']} — {VIP_PACKAGES['test']['stars']} Star",
                callback_data='buy_vip:test')])
        await update.message.reply_text(
            '\u2b50 Choose your VIP package\n\n'
            '\U0001f4c5 *1 Week VIP — 50 Stars*\n'
            '\u2022 \U0001f469 Filter by gender (Male/Female)\n'
            '\u2022 \U0001f3b2 Unlimited Truth or Dare\n'
            '\u2022 \U0001f3c6 Top priority matching\n\n'
            '\U0001f4c6 *1 Month VIP — 100 Stars*\n'
            '\u2022 All 1 Week perks\n'
            '\u2022 \U0001f48e VIP badge visible to partners\n'
            '\u2022 \U0001f4b0 Best value!\n\n'
            '\u2b50 Pay with Telegram Stars — instant activation',
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(buttons))
        return
    if text == '\U0001f451 Contact Admin': await update.message.reply_text('\U0001f451 Contact Admin: @Random1204'); return

    if text == '\U0001f4b3 Buy VIP':
        is_active = await check_vip(uid)
        if is_active:
            r = await db_pool.fetchrow('SELECT vip_expiry FROM users WHERE user_id=$1', uid)
            exp = r['vip_expiry'].strftime('%d %b %Y') if r and r['vip_expiry'] else 'Permanent'
            await update.message.reply_text(
                f'\U0001f451 You already have active VIP!\n'
                f'Expires: {exp}\n\n'
                f'Buying more will extend your current VIP.',
                reply_markup=buy_vip_inline())
        else:
            await update.message.reply_text(
                '\U0001f48e Upgrade to VIP with Telegram Stars!\n\n'
                '\u2b50 VIP gives you:\n'
                '\u2022 Filter by gender (Find Male / Find Female)\n'
                '\u2022 Unlimited Truth or Dare rounds\n'
                '\u2022 Priority matching\n\n'
                'Choose a plan:',
                reply_markup=buy_vip_inline())
        return
    if text == '\u2b05\ufe0f Back':
        context.user_data.pop('announce_mode', None); context.user_data.pop('in_admin_panel', None)
        await update.message.reply_text('Main menu \U0001f447', reply_markup=get_main_keyboard(uid)); return

    # ── RELAY ──
    if text not in BUTTON_TEXTS:
        partner = await get_partner(uid)
        if partner:
            try:
                await update.message.copy(chat_id=partner)
                if text:
                    await db_pool.execute('UPDATE users SET total_messages=total_messages+1 WHERE user_id=$1', uid)
                    await log_message(uid, partner, text)
            except Forbidden:
                await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', uid)
                await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', partner)
                await update.message.reply_text('\u26a0\ufe0f Partner unavailable.', reply_markup=get_main_keyboard(uid))
            except TimedOut: pass
            except Exception as e:
                logger.warning('relay error %s: %s', partner, e)
                await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', uid)
                await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', partner)
                await update.message.reply_text('\u26a0\ufe0f Partner disconnected.', reply_markup=get_main_keyboard(uid))
        else:
            await update.message.reply_text('Not in a chat. Press \U0001f680 Find Partner.', reply_markup=get_main_keyboard(uid))


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.message.from_user.id; username = update.message.from_user.username
    if await is_banned_check(uid): await update.message.reply_text('\U0001f6ab You are banned.'); return
    ref = None
    if context.args and context.args[0].isdigit():
        ref = int(context.args[0])
        if ref == uid: ref = None
    if uid == ADMIN_ID:
        await db_pool.execute(
            '''INSERT INTO users(user_id,username,name,gender,is_vip) VALUES($1,$2,'Admin','Male',TRUE)
               ON CONFLICT(user_id) DO UPDATE SET is_vip=TRUE,vip_expiry=NULL''', uid, username)
        await db_pool.execute('DELETE FROM active_chats  WHERE user_id=$1', uid)
        await db_pool.execute('DELETE FROM waiting_users WHERE user_id=$1', uid)
        context.user_data.clear()
        await update.message.reply_text('Welcome, Admin \U0001f44b', reply_markup=admin_main_keyboard); return
    if await user_exists(uid):
        if not await is_registered(uid):
            context.user_data.clear()
            db_step = await get_registration_step(uid)
            context.user_data['step'] = db_step or 'name'
            prompts = {
                'gender':  ("Let's finish your profile.\n\nSelect your gender:", gender_keyboard),
                'country': ("Let's finish your profile.\n\nEnter your country:", ReplyKeyboardRemove()),
                'age':     ("Let's finish your profile.\n\nEnter your age:", None),
            }
            p = prompts.get(db_step, ("Let's finish your profile.\n\nEnter your name:", None))
            await update.message.reply_text(p[0], reply_markup=p[1] if p[1] else ReplyKeyboardRemove())
        else:
            context.user_data['registered'] = True
            await update.message.reply_text('Welcome back! \U0001f44b', reply_markup=user_keyboard)
        return
    # Don't INSERT yet — store ref in context and only write to DB after all fields collected
    context.user_data.clear()
    context.user_data['step']     = 'name'
    context.user_data['ref']      = ref
    context.user_data['username'] = username
    await update.message.reply_text('\U0001f44b Welcome! Let\'s set up your profile.\n\nEnter your name:')

async def main():
    global db_pool
    dsn = DATABASE_URL
    if dsn and dsn.startswith('postgres://'): dsn = dsn.replace('postgres://', 'postgresql://', 1)
    db_pool = await asyncpg.create_pool(dsn, min_size=5, max_size=50)
    await init_db()
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler('start',      start))
    app.add_handler(CommandHandler('broadcast',  broadcast))
    app.add_handler(CommandHandler('ban',        handle_ban))
    app.add_handler(CommandHandler('unban',      handle_ban))
    app.add_handler(CommandHandler('cleanchats', cleanchats_command))
    app.add_handler(CommandHandler('debugref',   debug_referral))
    app.add_handler(CommandHandler('start',        start))
    app.add_handler(CommandHandler('broadcast',    broadcast))
    app.add_handler(CommandHandler('ban',          handle_ban))
    app.add_handler(CommandHandler('unban',        handle_ban))
    app.add_handler(CommandHandler('cleanchats',   cleanchats_command))
    app.add_handler(CommandHandler('debugref',     debug_referral))
    app.add_handler(CommandHandler('regrantvip',   regrant_vip_command))
    app.add_handler(CommandHandler('cleanup',      cleanup_null_users))
    app.add_handler(CommandHandler('fixvip',       fixvip_command))
    app.add_handler(CommandHandler('vipfemales',   vipfemales_command))
    app.add_handler(CommandHandler('deleteblocked',delete_blocked_users))
    app.add_handler(CommandHandler('nudge',        nudge_chats_command))
    app.add_handler(CommandHandler('stats',        stats_command))
    app.add_handler(CommandHandler('update',       update_command))
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
    app.add_handler(PreCheckoutQueryHandler(precheckout_callback))
    app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment_handler))
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, router))
    async def update_bot_description():
        """Sets a clean bot description once on startup."""
        try:
            await app.bot.set_my_short_description(
                '🎭 Chat anonymously with strangers'
            )
        except Exception as e:
            logger.warning('Could not set bot description: %s', e)

    logger.info('Bot starting...')
    # Retry initialization — handles temporary network issues on Railway
    for attempt in range(1, 11):
        try:
            await app.initialize()
            await app.start()
            break
        except Exception as e:
            logger.warning('Init attempt %d/10 failed: %s', attempt, e)
            if attempt == 10:
                logger.error('Could not connect to Telegram after 10 attempts. Exiting.')
                raise
            await asyncio.sleep(attempt * 2)  # 2s, 4s, 6s... backoff
    await app.updater.start_polling(drop_pending_updates=True)
    asyncio.create_task(update_bot_description())
    logger.info('Bot started successfully')
    try: await asyncio.Event().wait()
    finally:
        logger.info('Shutting down...')
        await db_pool.close(); await app.stop(); await app.shutdown()

if __name__ == '__main__':
    asyncio.run(main())
