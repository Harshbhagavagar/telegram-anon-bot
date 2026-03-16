import os, random, logging, asyncio
import asyncpg
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.error import Forbidden, TimedOut
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN    = os.getenv('BOT_TOKEN')
DATABASE_URL = os.getenv('DATABASE_URL')
ADMIN_ID     = 643086953
VIP_REFERRAL_THRESHOLD = 3
VIP_REFERRAL_DAYS      = 3
MATCH_INVITE_DELAY     = 45
FREE_TOD_LIMIT         = 3

db_pool: asyncpg.Pool = None
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
     ['\U0001f6a8 Reports'],
     ['\u2b05\ufe0f Back']],
    resize_keyboard=True)
vip_keyboard = ReplyKeyboardMarkup(
    [['\U0001f381 Get FREE VIP'],
     ['\U0001f451 Contact Admin'],
     ['\u2b05\ufe0f Back']],
    resize_keyboard=True)
gender_keyboard = ReplyKeyboardMarkup([['Male','Female']], resize_keyboard=True, one_time_keyboard=True)


def report_inline(partner_id):
    return InlineKeyboardMarkup([[InlineKeyboardButton('\u26a0\ufe0f Report', callback_data=f'report:{partner_id}')]])
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
    '\u23ed\ufe0f Next', '\u274c Stop', '\U0001f48e VIP', '\U0001f381 Get FREE VIP',
    '\U0001f451 Contact Admin', '\u2b05\ufe0f Back', '\u2699\ufe0f Admin Panel',
    '\u26a0\ufe0f Report',
    '\U0001f4ca Analytics', '\U0001f465 Active Users', '\U0001f552 Waiting Users',
    '\U0001f4e2 Announcement', '\U0001f9f9 Clean Dead Chats', '\U0001f451 VIP Users',
    '\U0001f6a8 Reports',
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
        await context.bot.send_message(uid,     '\u2705 Connected! Say hi \U0001f44b')
        await context.bot.send_message(partner, '\u2705 Connected! Say hi \U0001f44b')
        await context.bot.send_message(uid,     '\U0001f3b2 Want to break the ice?', reply_markup=tod_inline(uid))
        await context.bot.send_message(partner, '\U0001f3b2 Want to break the ice?', reply_markup=tod_inline(partner))
    else:
        context.user_data['last_pref'] = pref  # fast in-session access
        await db_pool.execute('UPDATE users SET last_search_pref=$1 WHERE user_id=$2', pref, uid)
        await db_pool.execute(
            'INSERT INTO waiting_users(user_id,preferred_gender) VALUES($1,$2) ON CONFLICT(user_id) DO UPDATE SET preferred_gender=EXCLUDED.preferred_gender',
            uid, pref)
        label = f'Searching for a {pref} partner...' if pref else 'Searching for a partner...'
        await update.message.reply_text(f'\U0001f50e {label}')
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
        await update.message.reply_text('\u26d4 Search stopped.', reply_markup=get_main_keyboard(uid))
        return False
    await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', uid)
    await db_pool.execute('DELETE FROM active_chats WHERE user_id=$1', partner)
    context_tod_counts.pop(uid, None); context_tod_counts.pop(partner, None)
    await update.message.reply_text('\u274c Chat ended.', reply_markup=get_main_keyboard(uid))
    await update.message.reply_text('Did something go wrong?', reply_markup=report_inline(partner))
    try:
        await context.bot.send_message(partner, '\U0001f44b Stranger left the chat.', reply_markup=get_main_keyboard(partner))
        await context.bot.send_message(partner, 'Did something go wrong?', reply_markup=report_inline(uid))
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
            msg = f'\U0001f451 VIP: \u2705 Active\nExpires: {exp_s}\n\n\U0001f504 Next VIP in {rem} more referral(s)'
        elif r and r['vip_expiry']:
            msg = f'\U0001f451 VIP: \u23f0 Expired ({r["vip_expiry"].strftime("%d %b %Y")})\n\nInvite {rem} more to unlock \U0001f451 VIP again!'
        else:
            msg = f'\U0001f451 VIP: \u274c Inactive\n\nInvite {VIP_REFERRAL_THRESHOLD} friends to get {VIP_REFERRAL_DAYS} days VIP!\n\nProgress: {prog}/{VIP_REFERRAL_THRESHOLD}'
        await update.message.reply_text(msg, reply_markup=vip_keyboard); return

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
    if text == '\U0001f451 Contact Admin': await update.message.reply_text('\U0001f451 Contact Admin: @Random1204'); return
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
    app.add_handler(CommandHandler('regrantvip', regrant_vip_command))
    app.add_handler(CommandHandler('cleanup',    cleanup_null_users))
    app.add_handler(CommandHandler('fixvip',     fixvip_command))
    app.add_handler(CommandHandler('vipfemales', vipfemales_command))
    app.add_handler(CommandHandler('stats',      stats_command))
    app.add_handler(CommandHandler('update',     update_command))
    app.add_handler(CallbackQueryHandler(announce_target_callback, pattern=r'^announce_target:'))
    app.add_handler(CallbackQueryHandler(report_callback,             pattern=r'^report:'))
    app.add_handler(CallbackQueryHandler(tod_callback,                pattern=r'^tod_'))
    app.add_handler(CallbackQueryHandler(admin_report_callback,       pattern=r'^admin_report:'))
    app.add_handler(CallbackQueryHandler(admin_ban_callback,          pattern=r'^admin_ban:'))
    app.add_handler(CallbackQueryHandler(admin_del_report_callback,   pattern=r'^admin_del_report:'))
    app.add_handler(CallbackQueryHandler(admin_back_reports_callback, pattern=r'^admin_back_reports$'))
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, router))
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
    logger.info('Bot started successfully')
    try: await asyncio.Event().wait()
    finally:
        logger.info('Shutting down...')
        await db_pool.close(); await app.stop(); await app.shutdown()

if __name__ == '__main__':
    asyncio.run(main())
