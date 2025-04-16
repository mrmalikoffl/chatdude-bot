from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice
from telegram.ext import (
    Updater,
    CommandHandler,
    MessageHandler,
    Filters,
    CallbackContext,
    ConversationHandler,
    CallbackQueryHandler,
    PreCheckoutQueryHandler,
)
import logging
import os
import time
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import pool
from urllib.parse import urlparse
import json
import re
from collections import defaultdict
import warnings
import telegram.error
import random

# Suppress ConversationHandler warning
warnings.filterwarnings("ignore", category=UserWarning, module="telegram.ext.conversationhandler")

# Set up logging for Heroku
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Admin configuration
ADMIN_IDS = {5975525252}  # Replace with your Telegram user ID

# Notification channel
NOTIFICATION_CHANNEL_ID = "-1002519617667"  # Your channel ID

# Security configurations
BANNED_WORDS = {
    "spam", "hate", "abuse", "nsfw", "inappropriate", "offensive",
    "harass", "bully", "threat", "sex", "porn", "nude", "violence",
    "scam", "hack", "phish", "malware"
}
BLOCKED_KEYWORDS = [
    r"f[u*]ck|sh[i1][t*]|b[i1]tch|a[s$][s$]h[o0]le",  # Profanities
    r"d[i1]ck|p[u*][s$][s$]y|c[o0]ck|t[i1]t[s$]",      # Sexual
    r"n[i1]gg[e3]r|f[a4]g|r[e3]t[a4]rd",              # Slurs
    r"idi[o0]t|m[o0]r[o0]n|sc[u*]m",                  # Insults
    r"s[e3]x|p[o0]rn|r[a4]p[e3]|h[o0]rny",            # Explicit
    r"h[a4]t[e3]|b[u*]lly|k[i1]ll|di[e3]",            # Harassment/Threats
]
COMMAND_COOLDOWN = 5
MAX_MESSAGES_PER_MINUTE = 15
REPORT_THRESHOLD = 3
TEMP_BAN_DURATION = 24 * 3600  # 1 day
MAX_PROFILE_LENGTH = 500
ALLOWED_TAGS = {"music", "gaming", "movies", "tech", "sports", "art", "travel", "food", "books", "fashion"}

# In-memory storage for runtime (non-persistent)
waiting_users = []
user_pairs = {}
previous_partners = {}
command_timestamps = {}
message_timestamps = defaultdict(list)
chat_histories = {}  # Premium feature: in-memory

# Conversation states
NAME, AGE, GENDER, LOCATION, BIO, CONSENT, VERIFICATION, TAGS = range(8)

# Emoji list for verification
VERIFICATION_EMOJIS = ['😊', '😢', '😡', '😎', '😍', '😉', '😜', '😴']

# Database connection pool
db_pool = None

def init_db_pool():
    global db_pool
    try:
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            logger.error("DATABASE_URL environment variable not set.")
            raise ValueError("DATABASE_URL not set")
        url = urlparse(database_url)
        db_pool = psycopg2.pool.SimpleConnectionPool(
            1, 10,  # Min 1, max 10 connections
            database=url.path[1:],
            user=url.username,
            password=url.password,
            host=url.hostname,
            port=url.port
        )
        logger.info("Database connection pool initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize database pool: {e}")
        raise

def get_db_connection():
    try:
        return db_pool.getconn()
    except Exception as e:
        logger.error(f"Failed to get database connection: {e}")
        return None

def release_db_connection(conn):
    if conn:
        try:
            db_pool.putconn(conn)
        except Exception as e:
            logger.error(f"Failed to release database connection: {e}")

def init_db():
    conn = get_db_connection()
    if not conn:
        logger.error("Could not get database connection for initialization.")
        return
    try:
        with conn.cursor() as c:
            # Create users table with created_at
            c.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    profile JSONB,
                    consent BOOLEAN DEFAULT FALSE,
                    consent_time BIGINT,
                    premium_expiry BIGINT,
                    ban_type TEXT,
                    ban_expiry BIGINT,
                    verified BOOLEAN DEFAULT FALSE,
                    premium_features JSONB DEFAULT '{}',
                    created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())
                )
            """)
            # Add created_at column if missing
            c.execute("""
                ALTER TABLE users 
                ADD COLUMN IF NOT EXISTS created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())
            """)
            # Backfill created_at with consent_time where possible
            c.execute("""
                UPDATE users 
                SET created_at = consent_time 
                WHERE created_at IS NULL AND consent_time IS NOT NULL
            """)
            # Set created_at to now for remaining NULLs
            c.execute("""
                UPDATE users 
                SET created_at = EXTRACT(EPOCH FROM NOW()) 
                WHERE created_at IS NULL
            """)
            # Create reports table
            c.execute("""
                CREATE TABLE IF NOT EXISTS reports (
                    report_id SERIAL PRIMARY KEY,
                    reporter_id BIGINT,
                    reported_id BIGINT,
                    timestamp BIGINT,
                    reason TEXT,
                    reporter_profile JSONB,
                    reported_profile JSONB
                )
            """)
            # Create keyword_violations table
            c.execute("""
                CREATE TABLE IF NOT EXISTS keyword_violations (
                    user_id BIGINT PRIMARY KEY,
                    count INTEGER DEFAULT 1,
                    last_violation TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    keyword TEXT,
                    ban_type VARCHAR(20),
                    ban_expiry INTEGER
                )
            """)
            conn.commit()
            logger.info("Database tables initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}", exc_info=True)
        conn.rollback()
    finally:
        release_db_connection(conn)

try:
    init_db_pool()
    init_db()
except Exception as e:
    logger.error(f"Failed to set up database: {e}")
    exit(1)

def cleanup_in_memory(context: CallbackContext):
    current_time = time.time()
    command_timestamps.clear()
    for user_id in list(message_timestamps.keys()):
        message_timestamps[user_id] = [t for t in message_timestamps[user_id] if current_time - t < 60]
        if not message_timestamps[user_id]:
            del message_timestamps[user_id]
    for user_id in list(chat_histories.keys()):
        if not has_premium_feature(user_id, "vaulted_chats"):
            del chat_histories[user_id]
    # Reset old keyword violations
    conn = get_db_connection()
    try:
        with conn.cursor() as c:
            c.execute("DELETE FROM keyword_violations WHERE last_violation < %s AND count < 3",
                      (datetime.now() - timedelta(hours=48),))
            conn.commit()
    except Exception as e:
        logger.error(f"Error cleaning up keyword violations: {e}")
    finally:
        release_db_connection(conn)
    logger.debug("In-memory data cleaned up.")

def get_user(user_id: int) -> dict:
    conn = get_db_connection()
    if not conn:
        logger.error(f"Failed to get database connection for user_id={user_id}")
        return {}
    try:
        with conn.cursor() as c:
            c.execute(
                "SELECT profile, consent, consent_time, premium_expiry, ban_type, ban_expiry, verified, premium_features, created_at "
                "FROM users WHERE user_id = %s",
                (user_id,)
            )
            result = c.fetchone()
            if result:
                user_data = {
                    "user_id": user_id,
                    "profile": result[0] or {},
                    "consent": result[1],
                    "consent_time": result[2],
                    "premium_expiry": result[3],
                    "ban_type": result[4],
                    "ban_expiry": result[5],
                    "verified": result[6],
                    "premium_features": result[7] or {},
                    "created_at": result[8]
                }
                # Check keyword_violations for active bans
                c.execute("SELECT ban_type, ban_expiry FROM keyword_violations WHERE user_id = %s", (user_id,))
                violation = c.fetchone()
                if violation and violation[0]:
                    user_data["ban_type"] = violation[0]
                    user_data["ban_expiry"] = violation[1]
                return user_data
            # Create new user
            new_user = {
                "user_id": user_id,
                "profile": {},
                "consent": False,
                "consent_time": None,
                "premium_expiry": None,
                "ban_type": None,
                "ban_expiry": None,
                "verified": False,
                "premium_features": {},
                "created_at": int(time.time())
            }
            c.execute("""
                INSERT INTO users (user_id, profile, consent, consent_time, premium_expiry, 
                                 ban_type, ban_expiry, verified, premium_features, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                user_id,
                json.dumps(new_user["profile"]),
                new_user["consent"],
                new_user["consent_time"],
                new_user["premium_expiry"],
                new_user["ban_type"],
                new_user["ban_expiry"],
                new_user["verified"],
                json.dumps(new_user["premium_features"]),
                new_user["created_at"]
            ))
            conn.commit()
            logger.info(f"Created new user with user_id={user_id}")
            return new_user
    except Exception as e:
        logger.error(f"Failed to get or create user {user_id}: {e}")
        return {}
    finally:
        release_db_connection(conn)

def update_user(user_id: int, data: dict) -> bool:
    conn = get_db_connection()
    if not conn:
        logger.error(f"No database connection for update_user {user_id}")
        return False
    retries = 3
    for attempt in range(retries):
        try:
            with conn.cursor() as c:
                # Fetch existing user data to merge
                c.execute(
                    "SELECT profile, consent, consent_time, premium_expiry, ban_type, ban_expiry, verified, premium_features, created_at "
                    "FROM users WHERE user_id = %s",
                    (user_id,)
                )
                existing = c.fetchone()
                if existing:
                    existing_data = {
                        "profile": existing[0] or {},
                        "consent": existing[1],
                        "consent_time": existing[2],
                        "premium_expiry": existing[3],
                        "ban_type": existing[4],
                        "ban_expiry": existing[5],
                        "verified": existing[6],
                        "premium_features": existing[7] or {},
                        "created_at": existing[8]
                    }
                else:
                    existing_data = {
                        "profile": {},
                        "consent": False,
                        "consent_time": None,
                        "premium_expiry": None,
                        "ban_type": None,
                        "ban_expiry": None,
                        "verified": False,
                        "premium_features": {},
                        "created_at": int(time.time())
                    }
                # Merge provided data with existing data
                updated_data = {
                    "profile": data.get("profile", existing_data["profile"]),
                    "consent": data.get("consent", existing_data["consent"]),
                    "consent_time": data.get("consent_time", existing_data["consent_time"]),
                    "premium_expiry": data.get("premium_expiry", existing_data["premium_expiry"]),
                    "ban_type": data.get("ban_type", existing_data["ban_type"]),
                    "ban_expiry": data.get("ban_expiry", existing_data["ban_expiry"]),
                    "verified": data.get("verified", existing_data["verified"]),
                    "premium_features": data.get("premium_features", existing_data["premium_features"]),
                    "created_at": data.get("created_at", existing_data["created_at"])
                }
                # Update database
                c.execute("""
                    INSERT INTO users (user_id, profile, consent, consent_time, premium_expiry, ban_type, ban_expiry, verified, premium_features, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE
                    SET profile = EXCLUDED.profile,
                        consent = EXCLUDED.consent,
                        consent_time = EXCLUDED.consent_time,
                        premium_expiry = EXCLUDED.premium_expiry,
                        ban_type = EXCLUDED.ban_type,
                        ban_expiry = EXCLUDED.ban_expiry,
                        verified = EXCLUDED.verified,
                        premium_features = EXCLUDED.premium_features,
                        created_at = EXCLUDED.created_at
                """, (
                    user_id,
                    json.dumps(updated_data["profile"]),
                    updated_data["consent"],
                    updated_data["consent_time"],
                    updated_data["premium_expiry"],
                    updated_data["ban_type"],
                    updated_data["ban_expiry"],
                    updated_data["verified"],
                    json.dumps(updated_data["premium_features"]),
                    updated_data["created_at"]
                ))
                conn.commit()
                logger.debug(f"Updated user {user_id}: premium_expiry={updated_data['premium_expiry']}, premium_features={updated_data['premium_features']}")
                return True
        except Exception as e:
            logger.error(f"Attempt {attempt + 1}/{retries} failed to update user {user_id}: {e}")
            conn.rollback()
            if attempt < retries - 1:
                time.sleep(1)  # Wait before retrying
        finally:
            release_db_connection(conn)
    logger.error(f"Failed to update user {user_id} after {retries} attempts")
    return False

def delete_user(user_id: int):
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn.cursor() as c:
            c.execute("DELETE FROM users WHERE user_id = %s", (user_id,))
            c.execute("DELETE FROM reports WHERE reporter_id = %s OR reported_id = %s", (user_id, user_id))
            c.execute("DELETE FROM keyword_violations WHERE user_id = %s", (user_id,))
            conn.commit()
            logger.info(f"Deleted user {user_id} from database.")
    except Exception as e:
        logger.error(f"Failed to delete user {user_id}: {e}")
    finally:
        release_db_connection(conn)

def issue_keyword_violation(user_id: int, keyword: str, update: Update, context: CallbackContext) -> str:
    conn = get_db_connection()
    if not conn:
        logger.error("Database error in issue_keyword_violation")
        safe_reply(update, "❌ Internal error.")
        return "error"
    try:
        with conn.cursor() as c:
            # Check if warning reset needed (48 hours)
            c.execute("SELECT last_violation FROM keyword_violations WHERE user_id = %s", (user_id,))
            last = c.fetchone()
            if last and (datetime.now().timestamp() - last[0].timestamp() > 48*3600):
                c.execute("DELETE FROM keyword_violations WHERE user_id = %s", (user_id,))
            # Insert or update violation
            c.execute(
                "INSERT INTO keyword_violations (user_id, count, keyword) VALUES (%s, 1, %s) "
                "ON CONFLICT (user_id) DO UPDATE SET count = keyword_violations.count + 1, "
                "last_violation = CURRENT_TIMESTAMP, keyword = %s RETURNING count",
                (user_id, keyword, keyword)
            )
            count = c.fetchone()[0]
            # Handle escalation
            if count == 1:
                safe_reply(update, 
                    "🚨 *Warning 1/5* 🚨\n"
                    "Your message contained inappropriate content. Please follow the rules to avoid further action."
                )
                logger.warning(f"User {user_id} warned (1/5) for keyword: {keyword}")
                return "warned_1"
            elif count == 2:
                safe_reply(update, 
                    "🚨 *Warning 2/5* 🚨\n"
                    "Another inappropriate message detected. One more violation will result in a 12-hour ban."
                )
                logger.warning(f"User {user_id} warned (2/5) for keyword: {keyword}")
                return "warned_2"
            elif count == 3:
                ban_expiry = int(time.time()) + 12*3600
                c.execute(
                    "UPDATE keyword_violations SET ban_type = 'temporary', ban_expiry = %s WHERE user_id = %s",
                    (ban_expiry, user_id)
                )
                c.execute(
                    "UPDATE users SET ban_type = 'temporary', ban_expiry = %s WHERE user_id = %s",
                    (ban_expiry, user_id)
                )
                safe_reply(update, 
                    "🚫 *Temporary Ban* 🚫\n"
                    "You’ve been banned for 12 hours due to repeated inappropriate messages. "
                    "Contact support if you believe this is an error."
                )
                logger.info(f"User {user_id} banned for 12 hours (3/5) for keyword: {keyword}")
                stop(update, context)
                return "banned_12h"
            elif count == 4:
                ban_expiry = int(time.time()) + 24*3600
                c.execute(
                    "UPDATE keyword_violations SET ban_type = 'temporary', ban_expiry = %s WHERE user_id = %s",
                    (ban_expiry, user_id)
                )
                c.execute(
                    "UPDATE users SET ban_type = 'temporary', ban_expiry = %s WHERE user_id = %s",
                    (ban_expiry, user_id)
                )
                safe_reply(update, 
                    "🚫 *Temporary Ban* 🚫\n"
                    "You’ve been banned for 24 hours due to continued violations. "
                    "Further issues will lead to a permanent ban."
                )
                logger.info(f"User {user_id} banned for 24 hours (4/5) for keyword: {keyword}")
                stop(update, context)
                return "banned_24h"
            else:  # count >= 5
                c.execute(
                    "UPDATE keyword_violations SET ban_type = 'permanent', ban_expiry = NULL WHERE user_id = %s",
                    (user_id,)
                )
                c.execute(
                    "UPDATE users SET ban_type = 'permanent', ban_expiry = NULL WHERE user_id = %s",
                    (user_id,)
                )
                safe_reply(update, 
                    "🚫 *Permanent Ban* 🚫\n"
                    "You’ve been permanently banned for repeated violations. "
                    "Contact support to appeal."
                )
                logger.info(f"User {user_id} permanently banned (5/5) for keyword: {keyword}")
                stop(update, context)
                return "banned_permanent"
            conn.commit()
    except Exception as e:
        logger.error(f"Error issuing keyword violation for {user_id}: {e}")
        safe_reply(update, "❌ Error processing your message.")
        return "error"
    finally:
        release_db_connection(conn)

def is_banned(user_id: int) -> bool:
    user = get_user(user_id)
    if user.get("ban_type"):
        if user["ban_type"] == "permanent" or (user["ban_type"] == "temporary" and user["ban_expiry"] and user["ban_expiry"] > time.time()):
            return True
        if user["ban_type"] == "temporary" and user["ban_expiry"] and user["ban_expiry"] <= time.time():
            update_user(user_id, {"ban_type": None, "ban_expiry": None})
    return False

def has_premium_feature(user_id: int, feature: str) -> bool:
    user = get_user(user_id)
    features = user.get("premium_features", {})
    result = feature in features and (features[feature] is True or (isinstance(features[feature], int) and features[feature] > time.time()))
    logger.debug(f"Checking premium feature '{feature}' for user {user_id}: {result}")
    return result

def is_verified(user_id: int) -> bool:
    return get_user(user_id).get("verified", False)

def check_rate_limit(user_id: int, cooldown: int = COMMAND_COOLDOWN) -> bool:
    current_time = time.time()
    if user_id in command_timestamps and current_time - command_timestamps[user_id] < cooldown:
        return False
    command_timestamps[user_id] = current_time
    return True

def check_message_rate_limit(user_id: int) -> bool:
    current_time = time.time()
    message_timestamps[user_id] = [t for t in message_timestamps[user_id] if current_time - t < 60]
    max_messages = MAX_MESSAGES_PER_MINUTE + 10 if has_premium_feature(user_id, "flare_messages") else MAX_MESSAGES_PER_MINUTE
    if len(message_timestamps[user_id]) >= max_messages:
        return False
    message_timestamps[user_id].append(current_time)
    return True

def is_safe_message(text: str) -> tuple[bool, str]:
    if not text:
        return True, ""
    text_lower = text.lower()
    # Check BANNED_WORDS
    if any(word in text_lower for word in BANNED_WORDS):
        return False, "banned_word"
    # Check BLOCKED_KEYWORDS
    for pattern in BLOCKED_KEYWORDS:
        if re.search(pattern, text_lower):
            return False, pattern
    url_pattern = re.compile(r'http[s]?://|www\.|\.com|\.org|\.net')
    if url_pattern.search(text_lower):
        return False, "url"
    return True, ""

def send_channel_notification(bot, message: str) -> None:
    """Send a notification to the private channel."""
    if not NOTIFICATION_CHANNEL_ID:
        logger.error("NOTIFICATION_CHANNEL_ID not set.")
        return
    try:
        safe_bot_send_message(bot, NOTIFICATION_CHANNEL_ID, message)
        logger.info(f"Sent notification to channel {NOTIFICATION_CHANNEL_ID}: {message[:100]}...")
    except telegram.error.TelegramError as e:
        logger.error(f"Failed to send notification to channel {NOTIFICATION_CHANNEL_ID}: {e}")

def escape_markdown_v2(text: str) -> str:
    """Escape special characters for MarkdownV2, preserving formatting."""
    special_chars = r'_[]()~`>#+-=|{}.!'
    result = []
    i = 0
    while i < len(text):
        char = text[i]
        if char in '*_' and i > 0 and i < len(text) - 1:
            prev_char = text[i - 1]
            next_char = text[i + 1]
            if (char == '*' and prev_char != '\\' and next_char != ' ') or \
               (char == '_' and prev_char != '\\' and next_char not in ' \n'):
                result.append(char)
                i += 1
                continue
        if char in special_chars:
            result.append(f'\\{char}')
        else:
            result.append(char)
        i += 1
    return ''.join(result)

def safe_reply(update: Update, text: str, parse_mode: str = "MarkdownV2", **kwargs) -> None:
    try:
        if parse_mode == "MarkdownV2":
            escaped_text = escape_markdown_v2(text)
            logger.debug(f"Escaped text: {escaped_text}")
        else:
            escaped_text = text
        if update.message:
            update.message.reply_text(escaped_text, parse_mode=parse_mode, **kwargs)
        elif update.callback_query:
            query = update.callback_query
            query.answer()
            query.message.reply_text(escaped_text, parse_mode=parse_mode, **kwargs)
        else:
            logger.error(f"No message or callback query in update: {update}")
    except telegram.error.BadRequest as bre:
        logger.warning(f"MarkdownV2 parsing failed: {bre}. Text: {text[:200]}")
        clean_text = text.replace('*', '').replace('_', '').replace('`', '').replace('[', '(').replace(']', ')')
        try:
            if update.callback_query:
                update.callback_query.message.reply_text(clean_text, parse_mode=None, **kwargs)
            elif update.message:
                update.message.reply_text(clean_text, parse_mode=None, **kwargs)
        except Exception as fallback_bre:
            logger.error(f"Failed to send fallback message: {fallback_bre}")
    except Exception as e:
        logger.error(f"Failed to send message: {e}")
        try:
            error_text = "❌ An error occurred. Please try again."
            if update.callback_query:
                update.callback_query.message.reply_text(error_text, parse_mode=None)
            elif update.message:
                update.message.reply_text(error_text, parse_mode=None)
        except Exception as fallback_e:
            logger.error(f"Failed to send fallback error message: {fallback_e}")

def safe_bot_send_message(bot, chat_id: int, text: str, parse_mode: str = "MarkdownV2", **kwargs):
    try:
        if parse_mode == "MarkdownV2":
            text = escape_markdown_v2(text)
        bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode, **kwargs)
    except telegram.error.BadRequest as e:
        logger.warning(f"MarkdownV2 parsing failed for chat {chat_id}: {e}. Text: {text[:200]}")
        clean_text = re.sub(r'([_*[\]()~`>#+-|=}{.!])', r'\\\1', text)
        clean_text = clean_text.replace('\\*', '*').replace('\\_', '_')
        bot.send_message(chat_id=chat_id, text=clean_text, parse_mode=None, **kwargs)
    except Exception as e:
        logger.error(f"Failed to send message to {chat_id}: {e}")

def start(update: Update, context: CallbackContext) -> int:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        safe_reply(update, ban_msg)
        logger.info(f"Banned user {user_id} attempted to start a chat.")
        return ConversationHandler.END
    if not check_rate_limit(user_id):
        safe_reply(update, f"⏳ Please wait {COMMAND_COOLDOWN} seconds before trying again.")
        return ConversationHandler.END
    if user_id in user_pairs:
        safe_reply(update, "💬 You're already in a chat. Use /next to switch or /stop to end.")
        return ConversationHandler.END
    user = get_user(user_id)
    if not user.get("consent"):
        keyboard = [
            [InlineKeyboardButton("✅ I Agree", callback_data="consent_agree")],
            [InlineKeyboardButton("❌ I Disagree", callback_data="consent_disagree")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        welcome_text = (
            "🌟 *Welcome to Talk2Anyone!* 🌟\n\n"
            "Chat anonymously with people worldwide! 🌍\n"
            "Here are the rules to keep it fun and safe:\n"
            "• No harassment, spam, or inappropriate content\n"
            "• Respect everyone at all times\n"
            "• Report issues with /report\n"
            "• Violations may lead to bans\n\n"
            "🔒 *Privacy*: We only store your user ID, profile, and consent securely. Use /deleteprofile to remove your data.\n\n"
            "Do you agree to the rules?"
        )
        safe_reply(update, welcome_text, reply_markup=reply_markup)
        return CONSENT
    if not user.get("verified"):
        correct_emoji = random.choice(VERIFICATION_EMOJIS)
        context.user_data["correct_emoji"] = correct_emoji
        other_emojis = random.sample([e for e in VERIFICATION_EMOJIS if e != correct_emoji], 3)
        all_emojis = [correct_emoji] + other_emojis
        random.shuffle(all_emojis)
        keyboard = [[InlineKeyboardButton(emoji, callback_data=f"emoji_{emoji}") for emoji in all_emojis]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        safe_reply(update, f"🔐 *Verify Your Profile* 🔐\n\nPlease select this emoji: *{correct_emoji}*", reply_markup=reply_markup)
        return VERIFICATION
    profile = user.get("profile", {})
    required_fields = ["name", "age", "gender", "location"]
    missing_fields = [field for field in required_fields if not profile.get(field)]
    if missing_fields:
        safe_reply(update, "✨ Let’s set up your profile to start chatting! Please enter your name:")
        return NAME
    if user_id not in waiting_users:
        if has_premium_feature(user_id, "shine_profile"):
            waiting_users.insert(0, user_id)
        else:
            waiting_users.append(user_id)
        safe_reply(update, "🔍 Looking for a chat partner... Please wait!")
    match_users(context)
    return ConversationHandler.END

def consent_handler(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
    choice = query.data
    if choice == "consent_agree":
        user = get_user(user_id)
        update_user(user_id, {
            "consent": True,
            "consent_time": int(time.time()),
            "profile": user.get("profile", {}),
            "premium_expiry": user.get("premium_expiry"),
            "premium_features": user.get("premium_features", {}),
            "created_at": user.get("created_at", int(time.time()))
        })
        # Send notification to channel
        notification_message = (
            "🆕 *New User Consented* 🆕\n\n"
            f"👤 *User ID*: {user_id}\n"
            f"📅 *Joined*: {datetime.fromtimestamp(user.get('created_at', int(time.time()))).strftime('%Y-%m-%d %H:%M:%S')}\n"
            "ℹ️ Profile not yet completed"
        )
        send_channel_notification(context.bot, notification_message)
        
        safe_reply(update, "✅ Thank you for agreeing! Let’s verify your profile next.")
        correct_emoji = random.choice(VERIFICATION_EMOJIS)
        context.user_data["correct_emoji"] = correct_emoji
        other_emojis = random.sample([e for e in VERIFICATION_EMOJIS if e != correct_emoji], 3)
        all_emojis = [correct_emoji] + other_emojis
        random.shuffle(all_emojis)
        keyboard = [[InlineKeyboardButton(emoji, callback_data=f"emoji_{emoji}") for emoji in all_emojis]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        safe_reply(update, f"🔐 *Verify Your Profile* 🔐\n\nPlease select this emoji: *{correct_emoji}*", reply_markup=reply_markup)
        return VERIFICATION
    else:
        safe_reply(update, "❌ You must agree to the rules to use this bot. Use /start to try again.")
        logger.info(f"User {user_id} declined rules.")
        return ConversationHandler.END

def verify_emoji(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
    selected_emoji = query.data.replace("emoji_", "")
    correct_emoji = context.user_data.get("correct_emoji")
    if selected_emoji == correct_emoji:
        user = get_user(user_id)
        update_user(user_id, {
            "verified": True,
            "profile": user.get("profile", {}),
            "consent": user.get("consent", False),
            "premium_expiry": user.get("premium_expiry"),
            "premium_features": user.get("premium_features", {}),
            "created_at": user.get("created_at", int(time.time()))
        })
        safe_reply(update, "🎉 Profile verified successfully! Let’s set up your profile.")
        safe_reply(update, "✨ Please enter your name:")
        return NAME
    else:
        correct_emoji = random.choice(VERIFICATION_EMOJIS)
        context.user_data["correct_emoji"] = correct_emoji
        other_emojis = random.sample([e for e in VERIFICATION_EMOJIS if e != correct_emoji], 3)
        all_emojis = [correct_emoji] + other_emojis
        random.shuffle(all_emojis)
        keyboard = [[InlineKeyboardButton(emoji, callback_data=f"emoji_{emoji}") for emoji in all_emojis]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        safe_reply(update, f"❌ Incorrect emoji. Try again!\nPlease select this emoji: *{correct_emoji}*", reply_markup=reply_markup)
        return VERIFICATION

def set_name(update: Update, context: CallbackContext) -> int:
    user_id = update.effective_user.id
    user = get_user(user_id)
    profile = user.get("profile", {})
    name = update.message.text.strip()
    if len(name) > 50:
        safe_reply(update, "⚠️ Name must be under 50 characters.")
        return NAME
    is_safe, _ = is_safe_message(name)
    if not is_safe:
        safe_reply(update, "⚠️ Name contains inappropriate content. Please try again.")
        return NAME
    profile["name"] = name
    update_user(user_id, {
        "profile": profile,
        "consent": user.get("consent", False),
        "verified": user.get("verified", False),
        "premium_expiry": user.get("premium_expiry"),
        "premium_features": user.get("premium_features", {}),
        "created_at": user.get("created_at", int(time.time()))
    })
    safe_reply(update, f"🧑 Name set to: *{name}*! 🎉")
    safe_reply(update, "🎂 Please enter your age (e.g., 25):")
    return AGE

def set_age(update: Update, context: CallbackContext) -> int:
    user_id = update.effective_user.id
    user = get_user(user_id)
    profile = user.get("profile", {})
    age_text = update.message.text.strip()
    try:
        age = int(age_text)
        if age < 13 or age > 120:
            safe_reply(update, "⚠️ Age must be between 13 and 120. Please try again.")
            return AGE
        profile["age"] = age
        update_user(user_id, {
            "profile": profile,
            "consent": user.get("consent", False),
            "verified": user.get("verified", False),
            "premium_expiry": user.get("premium_expiry"),
            "premium_features": user.get("premium_features", {}),
            "created_at": user.get("created_at", int(time.time()))
        })
        safe_reply(update, f"🎂 Age set to: *{age}*! 🎉")
        keyboard = [
            [
                InlineKeyboardButton("👨 Male", callback_data="gender_male"),
                InlineKeyboardButton("👩 Female", callback_data="gender_female"),
                InlineKeyboardButton("🌈 Other", callback_data="gender_other")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        safe_reply(update, "👤 *Set Your Gender* 👤\n\nChoose your gender below:", reply_markup=reply_markup)
        return GENDER
    except ValueError:
        safe_reply(update, "⚠️ Please enter a valid number for your age (e.g., 25).")
        return AGE

def set_gender(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
    data = query.data
    user = get_user(user_id)
    profile = user.get("profile", {})
    if data.startswith("gender_"):
        gender = data.split("_")[1].capitalize()
        profile["gender"] = gender
        update_user(user_id, {
            "profile": profile,
            "consent": user.get("consent", False),
            "verified": user.get("verified", False),
            "premium_expiry": user.get("premium_expiry"),
            "premium_features": user.get("premium_features", {}),
            "created_at": user.get("created_at", int(time.time()))
        })
        safe_reply(update, f"👤 Gender set to: *{gender}*! 🎉")
        safe_reply(update, "📍 Please enter your location (e.g., New York):")
        return LOCATION
    safe_reply(update, "⚠️ Invalid selection. Please choose a gender.")
    return GENDER

def set_location(update: Update, context: CallbackContext) -> int:
    user_id = update.effective_user.id
    user = get_user(user_id)
    profile = user.get("profile", {})
    location = update.message.text.strip()
    if len(location) > 100:
        safe_reply(update, "⚠️ Location must be under 100 characters.")
        return LOCATION
    is_safe, _ = is_safe_message(location)
    if not is_safe:
        safe_reply(update, "⚠️ Location contains inappropriate content. Please try again.")
        return LOCATION
    profile["location"] = location
    update_user(user_id, {
        "profile": profile,
        "consent": user.get("consent", False),
        "verified": user.get("verified", False),
        "premium_expiry": user.get("premium_expiry"),
        "premium_features": user.get("premium_features", {}),
        "created_at": user.get("created_at", int(time.time()))
    })
    safe_reply(update, f"📍 Location set to: *{location}*! 🌍")
    if user_id not in waiting_users:
        if has_premium_feature(user_id, "shine_profile"):
            waiting_users.insert(0, user_id)
        else:
            waiting_users.append(user_id)
        safe_reply(update, "🎉 Profile setup complete! 🔍 Looking for a chat partner... Please wait!")
    
    # Send notification to channel
    notification_message = (
        "🆕 *New User Registered* 🆕\n\n"
        f"👤 *User ID*: {user_id}\n"
        f"🧑 *Name*: {profile.get('name', 'Not set')}\n"
        f"🎂 *Age*: {profile.get('age', 'Not set')}\n"
        f"👤 *Gender*: {profile.get('gender', 'Not set')}\n"
        f"📍 *Location*: {location}\n"
        f"📅 *Joined*: {datetime.fromtimestamp(user.get('created_at', int(time.time()))).strftime('%Y-%m-%d %H:%M:%S')}"
    )
    send_channel_notification(context.bot, notification_message)
    
    match_users(context)
    return ConversationHandler.END

def match_users(context: CallbackContext) -> None:
    if len(waiting_users) < 2:
        return
    i = 0
    while i < len(waiting_users):
        user1 = waiting_users[i]
        j = i + 1
        while j < len(waiting_users):
            user2 = waiting_users[j]
            if can_match(user1, user2):
                waiting_users.remove(user1)
                waiting_users.remove(user2)
                user_pairs[user1] = user2
                user_pairs[user2] = user1

                # Update past_partners in the database
                user1_data = get_user(user1)
                user2_data = get_user(user2)
                user1_profile = user1_data.get("profile", {})
                user2_profile = user2_data.get("profile", {})
                user1_past = user1_profile.get("past_partners", [])
                user2_past = user2_profile.get("past_partners", [])
                if user2 not in user1_past:
                    user1_past.append(user2)
                if user1 not in user2_past:
                    user2_past.append(user1)
                user1_profile["past_partners"] = user1_past[-5:]  # Limit to last 5 partners
                user2_profile["past_partners"] = user2_past[-5:]
                update_user(user1, {
                    "profile": user1_profile,
                    "consent": user1_data.get("consent", False),
                    "verified": user1_data.get("verified", False),
                    "premium_expiry": user1_data.get("premium_expiry"),
                    "premium_features": user1_data.get("premium_features", {}),
                    "created_at": user1_data.get("created_at", int(time.time()))
                })
                update_user(user2, {
                    "profile": user2_profile,
                    "consent": user2_data.get("consent", False),
                    "verified": user2_data.get("verified", False),
                    "premium_expiry": user2_data.get("premium_expiry"),
                    "premium_features": user2_data.get("premium_features", {}),
                    "created_at": user2_data.get("created_at", int(time.time()))
                })

                user1_message = (
                    "✅ *Connected!* Start chatting now! 🗣️\n\n"
                    f"👤 *Partner Details*:\n"
                    f"🧑 *Name*: {user2_profile.get('name', 'Not set')}\n"
                    f"🎂 *Age*: {user2_profile.get('age', 'Not set')}\n"
                    f"👤 *Gender*: {user2_profile.get('gender', 'Not set')}\n"
                    f"📍 *Location*: {user2_profile.get('location', 'Not set')}\n\n"
                    "Use /help for more options."
                ) if has_premium_feature(user1, "partner_details") else (
                    "✅ *Connected!* Start chatting now! 🗣️\n\n"
                    "🔒 *Partner Details*: Upgrade to *Partner Details Reveal* to view your partner’s name, age, gender, and location!\n"
                    "Unlock with /premium.\n\n"
                    "Use /help for more options."
                )
                user2_message = (
                    "✅ *Connected!* Start chatting now! 🗣️\n\n"
                    f"👤 *Partner Details*:\n"
                    f"🧑 *Name*: {user1_profile.get('name', 'Not set')}\n"
                    f"🎂 *Age*: {user1_profile.get('age', 'Not set')}\n"
                    f"👤 *Gender*: {user1_profile.get('gender', 'Not set')}\n"
                    f"📍 *Location*: {user1_profile.get('location', 'Not set')}\n\n"
                    "Use /help for more options."
                ) if has_premium_feature(user2, "partner_details") else (
                    "✅ *Connected!* Start chatting now! 🗣️\n\n"
                    "🔒 *Partner Details*: Upgrade to *Partner Details Reveal* to view your partner’s name, age, gender, and location!\n"
                    "Unlock with /premium.\n\n"
                    "Use /help for more options."
                )
                safe_bot_send_message(context.bot, user1, user1_message)
                safe_bot_send_message(context.bot, user2, user2_message)
                logger.info(f"Matched users {user1} and {user2}.")
                if has_premium_feature(user1, "vaulted_chats"):
                    chat_histories[user1] = []
                if has_premium_feature(user2, "vaulted_chats"):
                    chat_histories[user2] = []
                return
            j += 1
        i += 1

def can_match(user1: int, user2: int) -> bool:
    profile1 = get_user(user1).get("profile", {})
    profile2 = get_user(user2).get("profile", {})
    if not profile1 or not profile2:
        return True
    tags1 = set(profile1.get("tags", []))
    tags2 = set(profile2.get("tags", []))
    if tags1 and tags2 and not tags1.intersection(tags2):
        return False
    age1 = profile1.get("age")
    age2 = profile2.get("age")
    if age1 and age2 and abs(age1 - age2) > (15 if has_premium_feature(user1, "mood_match") or has_premium_feature(user2, "mood_match") else 10):
        return False
    gender_pref1 = profile1.get("gender_preference")
    gender_pref2 = profile2.get("gender_preference")
    gender1 = profile1.get("gender")
    gender2 = profile2.get("gender")
    if gender_pref1 and gender2 and gender_pref1 != gender2:
        return False
    if gender_pref2 and gender1 and gender_pref2 != gender1:
        return False
    if has_premium_feature(user1, "mood_match") and has_premium_feature(user2, "mood_match"):
        mood1 = profile1.get("mood")
        mood2 = profile2.get("mood")
        if mood1 and mood2 and mood1 != mood2:
            return False
    return True

def stop(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        safe_reply(update, ban_msg)
        return
    user = get_user(user_id)
    logger.debug(f"User {user_id} before stop: premium_expiry={user.get('premium_expiry')}, premium_features={user.get('premium_features')}")
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        del user_pairs[user_id]
        if partner_id in user_pairs:
            del user_pairs[partner_id]
        safe_bot_send_message(context.bot, partner_id, "👋 Your partner has left the chat. Use /start to find a new one.")
        safe_reply(update, "👋 Chat ended. Use /start to begin a new chat.")
        logger.info(f"User {user_id} stopped chat with {partner_id}.")
        if user_id in chat_histories and not has_premium_feature(user_id, "vaulted_chats"):
            del chat_histories[user_id]
    else:
        safe_reply(update, "❓ You're not in a chat. Use /start to find a partner.")
    updated_user = get_user(user_id)
    logger.debug(f"User {user_id} after stop: premium_expiry={updated_user.get('premium_expiry')}, premium_features={updated_user.get('premium_features')}")

def next_chat(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        safe_reply(update, ban_msg)
        return
    if not check_rate_limit(user_id):
        safe_reply(update, f"⏳ Please wait {COMMAND_COOLDOWN} seconds before trying again.")
        return
    user = get_user(user_id)
    logger.debug(f"User {user_id} before next_chat: premium_expiry={user.get('premium_expiry')}, premium_features={user.get('premium_features')}")
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        del user_pairs[user_id]
        del user_pairs[partner_id]
        safe_bot_send_message(context.bot, partner_id, "🔌 Your chat partner disconnected.")
    if user_id not in waiting_users:
        waiting_users.append(user_id)
    safe_reply(update, "🔍 Looking for a new chat partner...")
    match_users(context)
    logger.info(f"User {user_id} requested next chat.")
    updated_user = get_user(user_id)
    logger.debug(f"User {user_id} after next_chat: premium_expiry={updated_user.get('premium_expiry')}, premium_features={updated_user.get('premium_features')}")

def help_command(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        safe_reply(update, ban_msg)
        return
    keyboard = [
        [InlineKeyboardButton("💬 Start Chat", callback_data="start_chat"),
         InlineKeyboardButton("🔍 Next Partner", callback_data="next_chat")],
        [InlineKeyboardButton("👋 Stop Chat", callback_data="stop_chat"),
         InlineKeyboardButton("⚙️ Settings", callback_data="settings_menu")],
        [InlineKeyboardButton("🌟 Premium", callback_data="premium_menu"),
         InlineKeyboardButton("📜 History", callback_data="history_menu")],
        [InlineKeyboardButton("🚨 Report", callback_data="report_user"),
         InlineKeyboardButton("🔄 Re-Match", callback_data="rematch_partner")],
        [InlineKeyboardButton("🗑️ Delete Profile", callback_data="delete_profile")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    help_text = (
        "🌟 *Talk2Anyone Help Menu* 🌟\n\n"
        "Explore all the ways to connect and customize your experience:\n\n"
        "💬 *Chat Commands* 💬\n"
        "• /start - Begin a new anonymous chat\n"
        "• /next - Find a new chat partner\n"
        "• /stop - End the current chat\n"
        "━━━━━\n\n"
        "⚙️ *Profile & Settings* ⚙️\n"
        "• /settings - Customize your profile\n"
        "• /deleteprofile - Erase your data\n"
        "━━━━━\n\n"
        "🌟 *Premium Features* 🌟\n"
        "• /premium - Unlock amazing features\n"
        "• /history - View past chats\n"
        "• /rematch - Reconnect with past partners\n"
        "• /shine - Boost your profile\n"
        "• /instant - Instant rematch\n"
        "• /mood - Set chat mood\n"
        "• /vault - Save chats forever\n"
        "• /flare - Add sparkle to messages\n"
        "━━━━━\n\n"
        "🚨 *Safety* 🚨\n"
        "• /report - Report inappropriate behavior\n"
    )
    if user_id in ADMIN_IDS:
        help_text += (
            "━━━━━\n\n"
            "🔐 *Admin Access* 🔐\n"
            "• /admin - View admin tools and commands\n"
        )
    help_text += "\nUse the buttons below to get started! 👇"
    safe_reply(update, help_text, reply_markup=reply_markup)

def premium(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        safe_reply(update, ban_msg)
        return
    keyboard = [
        [InlineKeyboardButton("✨ Flare Messages - 100 ⭐", callback_data="buy_flare"),
         InlineKeyboardButton("🔄 Instant Rematch - 100 ⭐", callback_data="buy_instant")],
        [InlineKeyboardButton("🌟 Shine Profile - 250 ⭐", callback_data="buy_shine"),
         InlineKeyboardButton("😊 Mood Match - 250 ⭐", callback_data="buy_mood")],
        [InlineKeyboardButton("👤 Partner Details - 500 ⭐", callback_data="buy_partner_details"),
         InlineKeyboardButton("📜 Vaulted Chats - 500 ⭐", callback_data="buy_vault")],
        [InlineKeyboardButton("🎉 Premium Pass - 1000 ⭐", callback_data="buy_premium_pass")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    message_text = (
        "🌟 *Unlock Premium Features!* 🌟\n\n"
        "Enhance your chat experience with these exclusive perks:\n\n"
        "• *Flare Messages* - Add sparkle effects for 7 days (100 ⭐)\n"
        "• *Instant Rematch* - Reconnect instantly (100 ⭐)\n"
        "• *Shine Profile* - Priority matching for 24 hours (250 ⭐)\n"
        "• *Mood Match* - Vibe-based matches for 30 days (250 ⭐)\n"
        "• *Partner Details* - View name, age, gender, location for 30 days (500 ⭐)\n"
        "• *Vaulted Chats* - Save chats forever (500 ⭐)\n"
        "• *Premium Pass* - All features for 30 days + 5 Instant Rematches (1000 ⭐)\n\n"
        "Tap a button to purchase with Telegram Stars! 👇"
    )
    safe_reply(update, message_text, reply_markup=reply_markup)

def buy_premium(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        safe_reply(update, ban_msg)
        return
    feature_map = {
        "buy_flare": ("Flare Messages", 100, "Add sparkle effects to your messages for 7 days!", "flare_messages"),
        "buy_instant": ("Instant Rematch", 100, "Reconnect with any past partner instantly!", "instant_rematch"),
        "buy_shine": ("Shine Profile", 250, "Boost your profile to the top for 24 hours!", "shine_profile"),
        "buy_mood": ("Mood Match", 250, "Match with users sharing your vibe for 30 days!", "mood_match"),
        "buy_partner_details": ("Partner Details", 500, "View your partner’s name, age, gender, and location for 30 days!", "partner_details"),
        "buy_vault": ("Vaulted Chats", 500, "Save your chats forever!", "vaulted_chats"),
        "buy_premium_pass": ("Premium Pass", 1000, "Unlock Shine Profile, Mood Match, Partner Details, Vaulted Chats, Flare Messages (30 days), and 5 Instant Rematches!", "premium_pass"),
    }
    choice = query.data
    if choice not in feature_map:
        safe_reply(update, "❌ Invalid feature selected.")
        return
    title, stars, desc, feature_key = feature_map[choice]
    try:
        context.bot.send_invoice(
            chat_id=user_id,
            title=title,
            description=desc,
            payload=f"{feature_key}_{user_id}",
            currency="XTR",
            prices=[LabeledPrice(title, stars)],
            start_parameter=f"buy-{feature_key}",
            provider_token=None
        )
        logger.info(f"Sent Stars invoice for user {user_id}: {title} ({stars} Stars)")
    except Exception as e:
        logger.error(f"Failed to send invoice for user {user_id}: {e}")
        safe_reply(update, "❌ Error generating payment invoice. Please try again.")

def pre_checkout(update: Update, context: CallbackContext) -> None:
    query = update.pre_checkout_query
    if query.currency != "XTR":
        context.bot.answer_pre_checkout_query(
            query.id, ok=False, error_message="Only Telegram Stars payments are supported."
        )
        return
    valid_payloads = [f"{key}_{query.from_user.id}" for key in ["flare_messages", "instant_rematch", "shine_profile", "mood_match", "partner_details", "vaulted_chats", "premium_pass"]]
    if query.invoice_payload in valid_payloads:
        context.bot.answer_pre_checkout_query(query.id, ok=True)
        logger.info(f"Approved pre-checkout for user {query.from_user.id}: {query.invoice_payload}")
    else:
        context.bot.answer_pre_checkout_query(
            query.id, ok=False, error_message="Invalid purchase payload."
        )
        logger.warning(f"Rejected pre-checkout for user {query.from_user.id}: {query.invoice_payload}")

def successful_payment(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    payment = update.message.successful_payment
    if payment.currency != "XTR":
        logger.warning(f"Non-Stars payment received from user {user_id}: {payment.currency}")
        return
    payload = payment.invoice_payload
    current_time = int(time.time())
    feature_map = {
        "flare_messages": (7 * 24 * 3600, "✨ *Flare Messages* activated for 7 days! Your messages will now sparkle!", "Flare Messages", 100),
        "instant_rematch": (None, "🔄 *Instant Rematch* unlocked! Use /instant to reconnect.", "Instant Rematch", 100),
        "shine_profile": (24 * 3600, "🌟 *Shine Profile* activated for 24 hours! You’re now at the top of the match list!", "Shine Profile", 250),
        "mood_match": (30 * 24 * 3600, "😊 *Mood Match* activated for 30 days! Find users with similar vibes!", "Mood Match", 250),
        "partner_details": (30 * 24 * 3600, "👤 *Partner Secret Revealed!* View your partner’s name, age, gender, and location for 30 days!", "Partner Details", 500),
        "vaulted_chats": (None, "📜 *Vaulted Chats* unlocked forever! Save your chats with /vault!", "Vaulted Chats", 500),
        "premium_pass": (30 * 24 * 3600, "🎉 *Premium Pass* activated! Enjoy all features for 30 days + 5 Instant Rematches!", "Premium Pass", 1000),
    }
    user = get_user(user_id)
    features = user.get("premium_features", {})
    premium_expiry = user.get("premium_expiry")
    profile = user.get("profile", {})
    for feature, (duration, message, feature_name, stars) in feature_map.items():
        if payload.startswith(feature):
            logger.debug(f"User {user_id} before purchase: premium_expiry={premium_expiry}, premium_features={features}")
            if feature == "premium_pass":
                new_expiry = max(premium_expiry or current_time, current_time + 30 * 24 * 3600)
                features.update({
                    "shine_profile": current_time + 30 * 24 * 3600,
                    "mood_match": current_time + 30 * 24 * 3600,
                    "partner_details": current_time + 30 * 24 * 3600,
                    "vaulted_chats": True,
                    "flare_messages": current_time + 30 * 24 * 3600,
                    "instant_rematch_count": features.get("instant_rematch_count", 0) + 5
                })
                premium_expiry = new_expiry
            else:
                if feature == "instant_rematch":
                    features["instant_rematch_count"] = features.get("instant_rematch_count", 0) + 1
                elif feature == "vaulted_chats":
                    features["vaulted_chats"] = True
                else:
                    features[feature] = current_time + duration
                if duration and (not premium_expiry or premium_expiry < current_time + duration):
                    premium_expiry = current_time + duration
            if not update_user(user_id, {
                "premium_expiry": premium_expiry,
                "premium_features": features,
                "profile": profile,
                "consent": user.get("consent", False),
                "verified": user.get("verified", False),
                "created_at": user.get("created_at", int(time.time()))
            }):
                logger.error(f"Failed to save purchase for user {user_id}: {feature}")
                safe_reply(update, "❌ Error processing your purchase. Please contact support.")
                return
            safe_reply(update, message)
            
            # Send notification to channel
            expiry_date = (
                datetime.fromtimestamp(current_time + duration).strftime("%Y-%m-%d %H:%M:%S")
                if duration else "No expiry"
            )
            notification_message = (
                "🌟 *New Premium Purchase* 🌟\n\n"
                f"👤 *User ID*: {user_id}\n"
                f"🧑 *Name*: {profile.get('name', 'Not set')}\n"
                f"🎂 *Age*: {profile.get('age', 'Not set')}\n"
                f"👤 *Gender*: {profile.get('gender', 'Not set')}\n"
                f"📍 *Location*: {profile.get('location', 'Not set')}\n"
                f"✨ *Feature*: {feature_name}\n"
                f"💰 *Cost*: {stars} Stars\n"
                f"📅 *Expiry*: {expiry_date}\n"
                f"🕒 *Purchased*: {datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H:%M:%S')}"
            )
            send_channel_notification(context.bot, notification_message)
            
            logger.info(f"User {user_id} purchased {feature} with Stars")
            updated_user = get_user(user_id)
            logger.debug(f"User {user_id} after purchase: premium_expiry={updated_user.get('premium_expiry')}, premium_features={updated_user.get('premium_features')}")
            break
    else:
        logger.warning(f"Unknown payload for user {user_id}: {payload}")
        safe_reply(update, "❌ Unknown purchase error. Please contact support.")

def shine(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        safe_reply(update, ban_msg)
        return
    user = get_user(user_id)
    logger.debug(f"User {user_id} before shine: premium_expiry={user.get('premium_expiry')}, premium_features={user.get('premium_features')}")
    if not has_premium_feature(user_id, "shine_profile"):
        safe_reply(update, "🌟 *Shine Profile* is a premium feature. Buy it with /premium!")
        return
    if user_id not in waiting_users and user_id not in user_pairs:
        waiting_users.insert(0, user_id)
        safe_reply(update, "✨ Your profile is now shining! You’re first in line for matches!")
        match_users(context)
    else:
        safe_reply(update, "❓ You're already in a chat or waiting list.")
    updated_user = get_user(user_id)
    logger.debug(f"User {user_id} after shine: premium_expiry={updated_user.get('premium_expiry')}, premium_features={updated_user.get('premium_features')}")

def instant(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        safe_reply(update, ban_msg)
        return
    user = get_user(user_id)
    logger.debug(f"User {user_id} before instant: premium_expiry={user.get('premium_expiry')}, premium_features={user.get('premium_features')}")
    if not has_premium_feature(user_id, "instant_rematch"):
        safe_reply(update, "🔄 *Instant Rematch* is a premium feature. Buy it with /premium!")
        return
    features = user.get("premium_features", {})
    rematch_count = features.get("instant_rematch_count", 0)
    if rematch_count <= 0:
        safe_reply(update, "🔄 You need an *Instant Rematch*! Buy one with /premium!")
        return
    partners = user.get("profile", {}).get("past_partners", [])
    if not partners:
        safe_reply(update, "❌ No past partners to rematch with.")
        return
    partner_id = partners[-1]
    partner_data = get_user(partner_id)
    if not partner_data:
        safe_reply(update, "❌ Your previous partner is no longer available.")
        return

    # Check if user is already in a chat
    if user_id in user_pairs:
        safe_reply(update, "❓ You're already in a chat. Use /stop to end it first.")
        return

    # Check if partner is in a chat
    if partner_id in user_pairs:
        safe_reply(update, "❌ Your previous partner is currently in another chat.")
        return

    # If partner is in waiting_users, connect immediately
    if partner_id in waiting_users:
        waiting_users.remove(partner_id)
        user_pairs[user_id] = partner_id
        user_pairs[partner_id] = user_id
        # Deduct rematch count
        features["instant_rematch_count"] = rematch_count - 1
        update_user(user_id, {
            "premium_features": features,
            "premium_expiry": user.get("premium_expiry"),
            "profile": user.get("profile", {}),
            "consent": user.get("consent", False),
            "verified": user.get("verified", False),
            "created_at": user.get("created_at", int(time.time()))
        })
        safe_reply(update, "🔄 *Instantly reconnected!* Start chatting! 🗣️")
        safe_bot_send_message(context.bot, partner_id, "🔄 *Instantly reconnected!* Start chatting! 🗣️")
        logger.info(f"User {user_id} used Instant Rematch with {partner_id} (partner was waiting)")
        if has_premium_feature(user_id, "vaulted_chats"):
            chat_histories[user_id] = chat_histories.get(user_id, [])
        if has_premium_feature(partner_id, "vaulted_chats"):
            chat_histories[partner_id] = chat_histories.get(partner_id, [])
        updated_user = get_user(user_id)
        logger.debug(f"User {user_id} after instant: premium_expiry={updated_user.get('premium_expiry')}, premium_features={updated_user.get('premium_features')}")
        return

    # Send a rematch request to the partner
    keyboard = [
        [InlineKeyboardButton("✅ Accept", callback_data=f"rematch_accept_{user_id}"),
         InlineKeyboardButton("❌ Decline", callback_data="rematch_decline")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    user_profile = user.get("profile", {})
    request_message = (
        f"🔄 *Rematch Request* 🔄\n\n"
        f"A user wants to reconnect with you!\n"
        f"🧑 *Name*: {user_profile.get('name', 'Anonymous')}\n"
        f"🎂 *Age*: {user_profile.get('age', 'Not set')}\n"
        f"👤 *Gender*: {user_profile.get('gender', 'Not set')}\n"
        f"📍 *Location*: {user_profile.get('location', 'Not set')}\n\n"
        f"Would you like to chat again?"
    )
    try:
        message = context.bot.send_message(
            chat_id=partner_id,
            text=escape_markdown_v2(request_message),
            parse_mode="MarkdownV2",
            reply_markup=reply_markup
        )
        safe_reply(update, "📩 Rematch request sent to your previous partner. Waiting for their response...")
        # Store the request with message ID for cleanup
        context.bot_data["rematch_requests"] = context.bot_data.get("rematch_requests", {})
        context.bot_data["rematch_requests"][partner_id] = {
            "requester_id": user_id,
            "timestamp": int(time.time()),
            "message_id": message.message_id
        }
        logger.info(f"User {user_id} sent rematch request to {partner_id}")
    except telegram.error.TelegramError as e:
        safe_reply(update, "❌ Unable to reach your previous partner. They may be offline.")
        logger.warning(f"Failed to send rematch request to {partner_id}: {e}")
    updated_user = get_user(user_id)
    logger.debug(f"User {user_id} after instant: premium_expiry={updated_user.get('premium_expiry')}, premium_features={updated_user.get('premium_features')}")

def mood(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        safe_reply(update, ban_msg)
        return
    user = get_user(user_id)
    logger.debug(f"User {user_id} before mood: premium_expiry={user.get('premium_expiry')}, premium_features={user.get('premium_features')}")
    if not has_premium_feature(user_id, "mood_match"):
        safe_reply(update, "😊 *Mood Match* is a premium feature. Buy it with /premium!")
        return
    keyboard = [
        [InlineKeyboardButton("😎 Chill", callback_data="mood_chill"),
         InlineKeyboardButton("🤔 Deep", callback_data="mood_deep")],
        [InlineKeyboardButton("😂 Fun", callback_data="mood_fun"),
         InlineKeyboardButton("❌ Clear Mood", callback_data="mood_clear")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    safe_reply(update, "🎭 Choose your chat mood:", reply_markup=reply_markup)
    updated_user = get_user(user_id)
    logger.debug(f"User {user_id} after mood: premium_expiry={updated_user.get('premium_expiry')}, premium_features={updated_user.get('premium_features')}")

def set_mood(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
    user = get_user(user_id)
    logger.debug(f"User {user_id} before set_mood: premium_expiry={user.get('premium_expiry')}, premium_features={user.get('premium_features')}")
    if not has_premium_feature(user_id, "mood_match"):
        safe_reply(update, "😊 *Mood Match* is a premium feature. Buy it with /premium!")
        return
    choice = query.data
    profile = user.get("profile", {})
    if choice == "mood_clear":
        profile.pop("mood", None)
        safe_reply(update, "❌ Mood cleared successfully.")
    else:
        mood = choice.split("_")[1]
        profile["mood"] = mood
        safe_reply(update, f"🎭 Mood set to: *{mood.capitalize()}*!")
    if not update_user(user_id, {
        "profile": profile,
        "premium_expiry": user.get("premium_expiry"),
        "premium_features": user.get("premium_features", {}),
        "consent": user.get("consent", False),
        "verified": user.get("verified", False),
        "created_at": user.get("created_at", int(time.time()))
    }):
        safe_reply(update, "❌ Error setting mood. Please try again.")
        return
    updated_user = get_user(user_id)
    logger.debug(f"User {user_id} after set_mood: premium_expiry={updated_user.get('premium_expiry')}, premium_features={updated_user.get('premium_features')}")

def vault(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        safe_reply(update, ban_msg)
        return
    user = get_user(user_id)
    logger.debug(f"User {user_id} before vault: premium_expiry={user.get('premium_expiry')}, premium_features={user.get('premium_features')}")
    if not has_premium_feature(user_id, "vaulted_chats"):
        safe_reply(update, "📜 *Vaulted Chats* is a premium feature. Buy it with /premium!")
        return
    if user_id not in user_pairs:
        safe_reply(update, "❓ You're not in a chat. Use /start to begin.")
        return
    if user_id not in chat_histories:
        chat_histories[user_id] = []
    safe_reply(update, "📜 Your current chat is being saved to the vault!")
    updated_user = get_user(user_id)
    logger.debug(f"User {user_id} after vault: premium_expiry={updated_user.get('premium_expiry')}, premium_features={updated_user.get('premium_features')}")

def history(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        safe_reply(update, ban_msg)
        return
    user = get_user(user_id)
    logger.debug(f"User {user_id} before history: premium_expiry={user.get('premium_expiry')}, premium_features={user.get('premium_features')}")
    if not has_premium_feature(user_id, "vaulted_chats"):
        safe_reply(update, "📜 *Chat History* is a premium feature. Buy it with /premium!")
        return
    if user_id not in chat_histories or not chat_histories[user_id]:
        safe_reply(update, "📜 Your chat vault is empty.")
        return
    history_text = "📜 *Your Chat History* 📜\n\n"
    for idx, msg in enumerate(chat_histories[user_id], 1):
        history_text += f"{idx}. {msg}\n"
    safe_reply(update, history_text)
    updated_user = get_user(user_id)
    logger.debug(f"User {user_id} after history: premium_expiry={updated_user.get('premium_expiry')}, premium_features={updated_user.get('premium_features')}")

def rematch(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        safe_reply(update, ban_msg)
        return
    if not check_rate_limit(user_id):
        safe_reply(update, f"⏳ Please wait {COMMAND_COOLDOWN} seconds before trying again.")
        return
    user = get_user(user_id)
    logger.debug(f"User {user_id} before rematch: premium_expiry={user.get('premium_expiry')}, premium_features={user.get('premium_features')}")
    if not has_premium_feature(user_id, "instant_rematch"):
        safe_reply(update, "🔄 *Rematch* is a premium feature. Buy it with /premium!")
        return
    partners = user.get("profile", {}).get("past_partners", [])
    if not partners:
        safe_reply(update, "❌ No past partners to rematch with.")
        return
    keyboard = []
    for partner_id in partners[-5:]:  # Limit to last 5 partners
        partner_data = get_user(partner_id)
        if partner_data:
            partner_name = partner_data.get("profile", {}).get("name", "Anonymous")
            keyboard.append([InlineKeyboardButton(f"Reconnect with {partner_name}", callback_data=f"rematch_request_{partner_id}")])
    if not keyboard:
        safe_reply(update, "❌ No available past partners to rematch with.")
        return
    reply_markup = InlineKeyboardMarkup(keyboard)
    safe_reply(update, "🔄 *Choose a Past Partner to Rematch* 🔄", reply_markup=reply_markup)
    updated_user = get_user(user_id)
    logger.debug(f"User {user_id} after rematch: premium_expiry={updated_user.get('premium_expiry')}, premium_features={updated_user.get('premium_features')}")

def button(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
    data = query.data

    if data == "start_chat":
        start(update, context)
    elif data == "next_chat":
        next_chat(update, context)
    elif data == "stop_chat":
        stop(update, context)
    elif data == "settings_menu":
        settings(update, context)
    elif data == "premium_menu":
        premium(update, context)
    elif data == "history_menu":
        history(update, context)
    elif data == "report_user":
        report(update, context)
    elif data == "rematch_partner":
        rematch(update, context)
    elif data == "delete_profile":
        delete_profile(update, context)
    elif data.startswith("buy_"):
        buy_premium(update, context)
    elif data.startswith("mood_"):
        set_mood(update, context)
    elif data.startswith("rematch_request_"):
        partner_id = int(data.split("_")[-1])
        if is_banned(user_id):
            safe_reply(update, "🚫 You are banned and cannot send rematch requests.")
            return
        user = get_user(user_id)
        if user_id in user_pairs:
            safe_reply(update, "❓ You're already in a chat. Use /stop to end it first.")
            return
        partner_data = get_user(partner_id)
        if not partner_data:
            safe_reply(update, "❌ This user is no longer available.")
            return
        if partner_id in user_pairs:
            safe_reply(update, "❌ This user is currently in another chat.")
            return
        keyboard = [
            [InlineKeyboardButton("✅ Accept", callback_data=f"rematch_accept_{user_id}"),
             InlineKeyboardButton("❌ Decline", callback_data="rematch_decline")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        user_profile = user.get("profile", {})
        request_message = (
            f"🔄 *Rematch Request* 🔄\n\n"
            f"A user wants to reconnect with you!\n"
            f"🧑 *Name*: {user_profile.get('name', 'Anonymous')}\n"
            f"🎂 *Age*: {user_profile.get('age', 'Not set')}\n"
            f"👤 *Gender*: {user_profile.get('gender', 'Not set')}\n"
            f"📍 *Location*: {user_profile.get('location', 'Not set')}\n\n"
            f"Would you like to chat again?"
        )
        try:
            message = context.bot.send_message(
                chat_id=partner_id,
                text=escape_markdown_v2(request_message),
                parse_mode="MarkdownV2",
                reply_markup=reply_markup
            )
            safe_reply(update, "📩 Rematch request sent. Waiting for their response...")
            context.bot_data["rematch_requests"] = context.bot_data.get("rematch_requests", {})
            context.bot_data["rematch_requests"][partner_id] = {
                "requester_id": user_id,
                "timestamp": int(time.time()),
                "message_id": message.message_id
            }
            logger.info(f"User {user_id} sent rematch request to {partner_id}")
        except telegram.error.TelegramError as e:
            safe_reply(update, "❌ Unable to reach this user. They may be offline.")
            logger.warning(f"Failed to send rematch request to {partner_id}: {e}")
    elif data.startswith("rematch_accept_"):
        requester_id = int(data.split("_")[-1])
        if is_banned(user_id):
            safe_reply(update, "🚫 You are banned and cannot accept rematch requests.")
            return
        if user_id in user_pairs:
            safe_reply(update, "❓ You're already in a chat. Use /stop to end it first.")
            return
        if requester_id in user_pairs:
            safe_reply(update, "❌ This user is now in another chat.")
            return
        if user_id in waiting_users:
            waiting_users.remove(user_id)
        user_pairs[user_id] = requester_id
        user_pairs[requester_id] = user_id
        safe_reply(update, "✅ *Reconnected!* Start chatting! 🗣️")
        safe_bot_send_message(context.bot, requester_id, "✅ *Reconnected!* Start chatting! 🗣️")
        logger.info(f"User {user_id} accepted rematch with {requester_id}")
        if has_premium_feature(user_id, "vaulted_chats"):
            chat_histories[user_id] = chat_histories.get(user_id, [])
        if has_premium_feature(requester_id, "vaulted_chats"):
            chat_histories[requester_id] = chat_histories.get(requester_id, [])
        # Delete the rematch request message
        rematch_data = context.bot_data.get("rematch_requests", {}).get(user_id)
        if rematch_data and rematch_data.get("message_id"):
            try:
                context.bot.delete_message(chat_id=user_id, message_id=rematch_data["message_id"])
                logger.info(f"Deleted rematch request message for user {user_id}")
            except telegram.error.TelegramError as e:
                logger.warning(f"Failed to delete rematch request message for {user_id}: {e}")
            context.bot_data["rematch_requests"].pop(user_id, None)
    elif data == "rematch_decline":
        rematch_data = context.bot_data.get("rematch_requests", {}).get(user_id)
        if rematch_data:
            requester_id = rematch_data.get("requester_id")
            safe_reply(update, "❌ Rematch request declined.")
            safe_bot_send_message(context.bot, requester_id, "❌ Your rematch request was declined.")
            logger.info(f"User {user_id} declined rematch with {requester_id}")
            # Delete the rematch request message
            if rematch_data.get("message_id"):
                try:
                    context.bot.delete_message(chat_id=user_id, message_id=rematch_data["message_id"])
                    logger.info(f"Deleted rematch request message for user {user_id}")
                except telegram.error.TelegramError as e:
                    logger.warning(f"Failed to delete rematch request message for {user_id}: {e}")
                context.bot_data["rematch_requests"].pop(user_id, None)
    elif data.startswith("emoji_"):
        verify_emoji(update, context)
    elif data.startswith("consent_"):
        consent_handler(update, context)
    elif data.startswith("gender_"):
        set_gender(update, context)
    elif data.startswith("set_"):
        handle_settings_buttons(update, context)

def cleanup_rematch_requests(context: CallbackContext) -> None:
    current_time = int(time.time())
    rematch_requests = context.bot_data.get("rematch_requests", {})
    for user_id, data in list(rematch_requests.items()):
        if current_time - data["timestamp"] > 24 * 3600:  # 24 hours timeout
            try:
                context.bot.delete_message(chat_id=user_id, message_id=data["message_id"])
                logger.info(f"Deleted timed-out rematch request message for user {user_id}")
            except telegram.error.TelegramError as e:
                logger.warning(f"Failed to delete timed-out rematch request for {user_id}: {e}")
            requester_id = data.get("requester_id")
            safe_bot_send_message(context.bot, requester_id, "⏳ Your rematch request timed out.")
            rematch_requests.pop(user_id, None)
    context.bot_data["rematch_requests"] = rematch_requests

def message_handler(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        safe_reply(update, ban_msg)
        return
    if not check_message_rate_limit(user_id):
        safe_reply(update, "⏳ You're sending messages too fast. Please wait a moment.")
        return
    text = update.message.text.strip()
    is_safe, reason = is_safe_message(text)
    if not is_safe:
        violation_result = issue_keyword_violation(user_id, reason, update, context)
        logger.warning(f"User {user_id} sent unsafe message: {text[:50]}... Result: {violation_result}")
        return
    if user_id not in user_pairs:
        safe_reply(update, "❓ You're not in a chat. Use /start to find a partner.")
        return
    partner_id = user_pairs[user_id]
    formatted_message = f"*Stranger*: {text}"
    if has_premium_feature(user_id, "flare_messages"):
        formatted_message = f"✨ *Stranger*: {text} ✨"
    try:
        safe_bot_send_message(context.bot, partner_id, formatted_message)
        if has_premium_feature(user_id, "vaulted_chats"):
            chat_histories[user_id].append(f"You: {text}")
        if has_premium_feature(partner_id, "vaulted_chats"):
            chat_histories[partner_id].append(f"Stranger: {text}")
        logger.info(f"Message from {user_id} to {partner_id}: {text[:50]}...")
    except telegram.error.TelegramError as e:
        safe_reply(update, "❌ Failed to send message. Your partner may have disconnected.")
        logger.error(f"Failed to send message from {user_id} to {partner_id}: {e}")
        stop(update, context)

def report(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        safe_reply(update, ban_msg)
        return
    if user_id not in user_pairs:
        safe_reply(update, "❓ You're not in a chat. Use /start to begin.")
        return
    partner_id = user_pairs[user_id]
    conn = get_db_connection()
    if not conn:
        safe_reply(update, "❌ Internal error. Please try again later.")
        return
    try:
        with conn.cursor() as c:
            c.execute(
                "INSERT INTO reports (reporter_id, reported_id, timestamp, reason, reporter_profile, reported_profile) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (
                    user_id,
                    partner_id,
                    int(time.time()),
                    "User reported inappropriate behavior",
                    json.dumps(get_user(user_id).get("profile", {})),
                    json.dumps(get_user(partner_id).get("profile", {}))
                )
            )
            c.execute(
                "SELECT COUNT(*) FROM reports WHERE reported_id = %s AND timestamp > %s",
                (partner_id, int(time.time()) - 24 * 3600)
            )
            report_count = c.fetchone()[0]
            conn.commit()
            safe_reply(update, "🚨 Report submitted. Our team will review it.")
            if report_count >= REPORT_THRESHOLD:
                ban_expiry = int(time.time()) + TEMP_BAN_DURATION
                update_user(partner_id, {"ban_type": "temporary", "ban_expiry": ban_expiry})
                safe_bot_send_message(context.bot, partner_id, f"🚫 You've been temporarily banned for 24 hours due to multiple reports.")
                logger.info(f"User {partner_id} banned temporarily due to {report_count} reports.")
                stop(update, context)
            logger.info(f"User {user_id} reported {partner_id}.")
    except Exception as e:
        logger.error(f"Failed to process report from {user_id} against {partner_id}: {e}")
        safe_reply(update, "❌ Error submitting report. Please try again.")
    finally:
        release_db_connection(conn)

def settings(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        safe_reply(update, ban_msg)
        return
    keyboard = [
        [InlineKeyboardButton("🧑 Update Name", callback_data="set_name"),
         InlineKeyboardButton("🎂 Update Age", callback_data="set_age")],
        [InlineKeyboardButton("👤 Update Gender", callback_data="set_gender"),
         InlineKeyboardButton("📍 Update Location", callback_data="set_location")],
        [InlineKeyboardButton("🏷️ Set Tags", callback_data="set_tags")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    safe_reply(update, "⚙️ *Settings Menu* ⚙️\n\nUpdate your profile below:", reply_markup=reply_markup)

def handle_settings_buttons(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
    data = query.data
    if data == "set_name":
        safe_reply(update, "🧑 Please enter your new name:")
        return NAME
    elif data == "set_age":
        safe_reply(update, "🎂 Please enter your new age (e.g., 25):")
        return AGE
    elif data == "set_gender":
        keyboard = [
            [
                InlineKeyboardButton("👨 Male", callback_data="gender_male"),
                InlineKeyboardButton("👩 Female", callback_data="gender_female"),
                InlineKeyboardButton("🌈 Other", callback_data="gender_other")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        safe_reply(update, "👤 *Set Your Gender* 👤\n\nChoose your gender below:", reply_markup=reply_markup)
        return GENDER
    elif data == "set_location":
        safe_reply(update, "📍 Please enter your new location (e.g., New York):")
        return LOCATION
    elif data == "set_tags":
        safe_reply(update, f"🏷️ Enter your interest tags (e.g., music, gaming). Available tags: {', '.join(ALLOWED_TAGS)}")
        return TAGS
    return ConversationHandler.END

def set_tags(update: Update, context: CallbackContext) -> int:
    user_id = update.effective_user.id
    user = get_user(user_id)
    profile = user.get("profile", {})
    tags_input = update.message.text.strip().lower().split()
    tags = [tag for tag in tags_input if tag in ALLOWED_TAGS]
    if not tags:
        safe_reply(update, f"⚠️ No valid tags provided. Available tags: {', '.join(ALLOWED_TAGS)}")
        return TAGS
    profile["tags"] = tags[:5]  # Limit to 5 tags
    update_user(user_id, {
        "profile": profile,
        "consent": user.get("consent", False),
        "verified": user.get("verified", False),
        "premium_expiry": user.get("premium_expiry"),
        "premium_features": user.get("premium_features", {}),
        "created_at": user.get("created_at", int(time.time()))
    })
    safe_reply(update, f"🏷️ Tags set to: *{', '.join(tags)}*!")
    return ConversationHandler.END

def delete_profile(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        del user_pairs[user_id]
        del user_pairs[partner_id]
        safe_bot_send_message(context.bot, partner_id, "👋 Your partner has deleted their profile.")
    if user_id in waiting_users:
        waiting_users.remove(user_id)
    delete_user(user_id)
    if user_id in chat_histories:
        del chat_histories[user_id]
    safe_reply(update, "🗑️ Your profile has been deleted. Use /start to create a new one.")
    logger.info(f"User {user_id} deleted their profile.")

def admin_access(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔐 Unauthorized.")
        logger.info(f"Unauthorized access attempt by user_id={user_id}")
        return
    access_text = (
        "🔐 *Admin Commands* 🔐\n\n"
        "👤 *User Management*\n"
        "• /admin_userslist - List all users\n"
        "• /admin_premiumuserslist - List premium users\n"
        "• /admin_info <user_id> - View user details\n"
        "• /admin_delete <user_id> - Delete a user’s data\n"
        "• /admin_premium <user_id> <days> - Grant premium status\n"
        "• /admin_revoke_premium <user_id> - Revoke premium status\n"
        "━━━━━\n\n"
        "🚫 *Ban Management*\n"
        "• /admin_ban <user_id> <days/permanent> - Ban a user\n"
        "• /admin_unban <user_id> - Unban a user\n"
        "• /admin_violations - List recent keyword violations\n"
        "━━━━━\n\n"
        "📊 *Reports & Stats*\n"
        "• /admin_reports - List reported users\n"
        "• /admin_clear_reports <user_id> - Clear reports\n"
        "• /admin_stats - View bot statistics\n"
        "━━━━━\n\n"
        "📢 *Broadcast*\n"
        "• /admin_broadcast <message> - Send message to all users\n"
    )
    safe_reply(update, access_text)

def admin_delete(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔐 Unauthorized.")
        return
    try:
        target_id = int(context.args[0])
        delete_user(target_id)
        safe_reply(update, f"🗑️ User *{target_id}* data deleted successfully.")
        logger.info(f"Admin {user_id} deleted user {target_id}.")
    except (IndexError, ValueError):
        safe_reply(update, "⚠️ Usage: /admin_delete <user_id>")

def admin_premium(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔐 Unauthorized.")
        return
    try:
        target_id = int(context.args[0])
        days = int(context.args[1])
        if days <= 0:
            raise ValueError("Days must be positive")
        expiry = int(time.time()) + days * 24 * 3600
        logger.debug(f"Calculated premium_expiry for user {target_id}: {expiry} ({datetime.fromtimestamp(expiry).strftime('%Y-%m-%d %H:%M:%S')})")
        user = get_user(target_id)
        logger.debug(f"User {target_id} before admin_premium: premium_expiry={user.get('premium_expiry')}, premium_features={user.get('premium_features')}")
        features = user.get("premium_features", {})
        # Grant all premium features with expiry
        features.update({
            "flare_messages": expiry,
            "instant_rematch_count": features.get("instant_rematch_count", 0) + 5,
            "shine_profile": expiry,
            "mood_match": expiry,
            "partner_details": expiry,
            "vaulted_chats": expiry
        })
        update_user(target_id, {
            "premium_expiry": expiry,
            "premium_features": features,
            "profile": user.get("profile", {}),
            "consent": user.get("consent", False),
            "verified": user.get("verified", False),
            "ban_type": user.get("ban_type"),
            "ban_expiry": user.get("ban_expiry"),
            "created_at": user.get("created_at", int(time.time()))
        })
        updated_user = get_user(target_id)
        expiry_date = datetime.fromtimestamp(updated_user.get('premium_expiry', 0)).strftime('%Y-%m-%d %H:%M:%S') if updated_user.get('premium_expiry') else 'None'
        logger.debug(f"User {target_id} after admin_premium: premium_expiry={updated_user.get('premium_expiry')} ({expiry_date}), premium_features={updated_user.get('premium_features')}")
        safe_reply(update, f"🌟 Premium granted to user *{target_id}* for *{days}* days.")
        safe_bot_send_message(context.bot, target_id, f"🎉 You've been granted Premium status for {days} days!")
        logger.info(f"Admin {user_id} granted premium to {target_id} for {days} days.")
    except (IndexError, ValueError) as e:
        logger.error(f"Error in admin_premium for user {target_id}: {e}")
        safe_reply(update, "⚠️ Usage: /admin_premium <user_id> <days>")

def admin_revoke_premium(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔐 Unauthorized.")
        return
    try:
        target_id = int(context.args[0])
        user = get_user(target_id)
        logger.debug(f"User {target_id} before revoke_premium: premium_expiry={user.get('premium_expiry')}, premium_features={user.get('premium_features')}")
        update_user(target_id, {
            "premium_expiry": None,
            "premium_features": {},
            "profile": user.get("profile", {}),
            "consent": user.get("consent", False),
            "verified": user.get("verified", False),
            "ban_type": user.get("ban_type"),
            "ban_expiry": user.get("ban_expiry"),
            "created_at": user.get("created_at", int(time.time()))
        })
        safe_reply(update, f"❌ Premium status revoked for user *{target_id}*.")
        safe_bot_send_message(context.bot, target_id, "🔌 Your Premium status has been revoked.")
        updated_user = get_user(target_id)
        logger.info(f"Admin {user_id} revoked premium for {target_id}.")
        logger.debug(f"User {target_id} after revoke_premium: premium_expiry={updated_user.get('premium_expiry')}, premium_features={updated_user.get('premium_features')}")
    except (IndexError, ValueError):
        safe_reply(update, "⚠️ Usage: /admin_revoke_premium <user_id>")

def admin_ban(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔐 Unauthorized.")
        return
    try:
        target_id = int(context.args[0])
        ban_type = context.args[1].lower()
        user = get_user(target_id)
        logger.debug(f"User {target_id} before admin_ban: premium_expiry={user.get('premium_expiry')}, premium_features={user.get('premium_features')}")
        if ban_type == "permanent":
            ban_expiry = None
        elif ban_type.isdigit():
            days = int(ban_type)
            if days <= 0:
                raise ValueError("Days must be positive")
            ban_expiry = int(time.time()) + days * 24 * 3600
            ban_type = "temporary"
        else:
            raise ValueError("Invalid ban type")
        update_user(target_id, {
            "ban_type": ban_type,
            "ban_expiry": ban_expiry,
            "profile": user.get("profile", {}),
            "consent": user.get("consent", False),
            "verified": user.get("verified", False),
            "premium_expiry": user.get("premium_expiry"),
            "premium_features": user.get("premium_features", {}),
            "created_at": user.get("created_at", int(time.time()))
        })
        if target_id in user_pairs:
            partner_id = user_pairs[target_id]
            del user_pairs[target_id]
            if partner_id in user_pairs:
                del user_pairs[partner_id]
            safe_bot_send_message(context.bot, partner_id, "👋 Your partner has left the chat.")
        if target_id in waiting_users:
            waiting_users.remove(target_id)
        safe_reply(update, f"🚫 User *{target_id}* has been {ban_type} banned.")
        safe_bot_send_message(context.bot, target_id, f"🚫 You have been {ban_type} banned from Talk2Anyone.")
        updated_user = get_user(target_id)
        logger.info(f"Admin {user_id} banned user {target_id} ({ban_type}).")
        logger.debug(f"User {target_id} after admin_ban: premium_expiry={updated_user.get('premium_expiry')}, premium_features={updated_user.get('premium_features')}")
    except (IndexError, ValueError):
        safe_reply(update, "⚠️ Usage: /admin_ban <user_id> <days/permanent>")

def admin_unban(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔐 Unauthorized.")
        return
    try:
        target_id = int(context.args[0])
        user = get_user(target_id)
        logger.debug(f"User {target_id} before admin_unban: premium_expiry={user.get('premium_expiry')}, premium_features={user.get('premium_features')}")
        update_user(target_id, {
            "ban_type": None,
            "ban_expiry": None,
            "profile": user.get("profile", {}),
            "consent": user.get("consent", False),
            "verified": user.get("verified", False),
            "premium_expiry": user.get("premium_expiry"),
            "premium_features": user.get("premium_features", {}),
            "created_at": user.get("created_at", int(time.time()))
        })
        # Clear keyword violations
        conn = get_db_connection()
        if conn:
            try:
                with conn.cursor() as c:
                    c.execute("DELETE FROM keyword_violations WHERE user_id = %s", (target_id,))
                    conn.commit()
            except Exception as e:
                logger.error(f"Error clearing keyword violations for {target_id}: {e}")
            finally:
                release_db_connection(conn)
        safe_reply(update, f"✅ User *{target_id}* has been unbanned.")
        safe_bot_send_message(context.bot, target_id, "✅ You have been unbanned. Use /start to begin.")
        updated_user = get_user(target_id)
        logger.info(f"Admin {user_id} unbanned user {target_id}.")
        logger.debug(f"User {target_id} after admin_unban: premium_expiry={updated_user.get('premium_expiry')}, premium_features={updated_user.get('premium_features')}")
    except (IndexError, ValueError):
        safe_reply(update, "⚠️ Usage: /admin_unban <user_id>")

def admin_violations(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔐 Unauthorized.")
        return
    conn = get_db_connection()
    if not conn:
        safe_reply(update, "❌ Database error.")
        return
    try:
        with conn.cursor() as c:
            c.execute(
                "SELECT user_id, count, keyword, last_violation, ban_type, ban_expiry "
                "FROM keyword_violations ORDER BY last_violation DESC LIMIT 10"
            )
            violations = c.fetchall()
            if not violations:
                safe_reply(update, "📜 No recent keyword violations.")
                return
            violation_text = "🚨 *Recent Keyword Violations* 🚨\n\n"
            for v in violations:
                user_id, count, keyword, last_violation, ban_type, ban_expiry = v
                ban_status = (
                    "Permanent" if ban_type == "permanent" else
                    f"Temporary until {datetime.fromtimestamp(ban_expiry).strftime('%Y-%m-%d %H:%M')}"
                    if ban_type == "temporary" and ban_expiry else "None"
                )
                violation_text += (
                    f"👤 User ID: *{user_id}*\n"
                    f"⚠️ Violations: *{count}*\n"
                    f"🔍 Keyword: *{keyword}*\n"
                    f"📅 Last: *{last_violation.strftime('%Y-%m-%d %H:%M')}*\n"
                    f"🚫 Ban: *{ban_status}*\n"
                    "━━━━━\n"
                )
            safe_reply(update, violation_text)
    except Exception as e:
        logger.error(f"Error fetching violations: {e}")
        safe_reply(update, "❌ Error fetching violations.")
    finally:
        release_db_connection(conn)

def admin_userslist(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔐 Unauthorized.")
        return
    conn = get_db_connection()
    if not conn:
        safe_reply(update, "❌ Database error.")
        return
    try:
        with conn.cursor() as c:
            c.execute("SELECT user_id, created_at, premium_expiry, ban_type, verified, profile, premium_features FROM users ORDER BY user_id")
            users = c.fetchall()
            if not users:
                safe_reply(update, "📜 No users found.")
                return
            message = "👥 *All Users List* \\(Sorted by ID\\)\n\n"
            user_count = 0
            for user in users:
                user_id, created_at, premium_expiry, ban_type, verified, profile_json, premium_features_json = user
                profile = profile_json or {}
                premium_features = premium_features_json or {}
                created_date = datetime.fromtimestamp(created_at).strftime("%Y-%m-%d") if created_at else "Unknown"
                # Check premium status: either premium_expiry is active or any premium_feature is active
                has_active_features = any(
                    v is True or (isinstance(v, int) and v > time.time())
                    for k, v in premium_features.items()
                    if k != "instant_rematch_count"
                )
                premium_status = (
                    "Premium"
                    if (premium_expiry and premium_expiry > time.time()) or has_active_features
                    else "Not Premium"
                )
                ban_status = ban_type.capitalize() if ban_type else "None"
                verified_status = "Yes" if verified else "No"
                name = profile.get("name", "Not set")
                message += (
                    f"👤 *User ID*: {user_id}\n"
                    f"🧑 *Name*: {name}\n"
                    f"📅 *Created*: {created_date}\n"
                    f"🌟 *Premium*: {premium_status}\n"
                    f"🚫 *Ban*: {ban_status}\n"
                    f"✅ *Verified*: {verified_status}\n"
                    f"━━━━━\n"
                )
                user_count += 1
                if len(message.encode('utf-8')) > 4000:  # Telegram's limit is 4096, leave buffer
                    safe_reply(update, message)
                    message = ""
            if message:
                message += f"🧑‍🤝‍🧑 *Total Users*: {user_count}\n"
                safe_reply(update, message)
            logger.info(f"Admin {user_id} requested users list with {user_count} users.")
            logger.debug(f"Users list generated with {user_count} users.")
    except Exception as e:
        logger.error(f"Error fetching users list: {e}")
        safe_reply(update, "❌ Error retrieving users list.")
    finally:
        release_db_connection(conn)

def admin_premiumuserslist(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔐 Unauthorized.")
        return
    conn = get_db_connection()
    if not conn:
        safe_reply(update, "❌ Database error.")
        return
    try:
        current_time = int(time.time())
        logger.debug(f"Fetching premium users with premium_expiry > {current_time} ({datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H:%M:%S')})")
        with conn.cursor() as c:
            c.execute(
                "SELECT user_id, premium_expiry, premium_features, profile "
                "FROM users "
                "WHERE premium_expiry IS NOT NULL AND premium_expiry > %s "
                "ORDER BY premium_expiry DESC",
                (current_time,)
            )
            users = c.fetchall()
            logger.debug(f"Query returned {len(users)} premium users")
            if not users:
                safe_reply(update, "📜 No premium users found.")
                logger.debug("No premium users found in database")
                return
            message = "🌟 *Premium Users List* \\(Sorted by Expiry\\)\n\n"
            user_count = 0
            for user in users:
                user_id, premium_expiry, premium_features_json, profile_json = user
                profile = profile_json or {}
                premium_features = premium_features_json or {}
                expiry_date = (
                    datetime.fromtimestamp(premium_expiry).strftime("%Y-%m-%d")
                    if premium_expiry and isinstance(premium_expiry, (int, float)) and premium_expiry > current_time
                    else "No expiry set"
                )
                logger.debug(f"User {user_id}: premium_expiry={premium_expiry} ({datetime.fromtimestamp(premium_expiry).strftime('%Y-%m-%d %H:%M:%S') if premium_expiry else 'None'}), expiry_date={expiry_date}")
                name = profile.get("name", "Not set")
                active_features = [
                    k for k, v in premium_features.items()
                    if v is True or (isinstance(v, int) and v > current_time)
                ]
                if "instant_rematch_count" in premium_features and premium_features["instant_rematch_count"] > 0:
                    active_features.append(f"instant_rematch_count: {premium_features['instant_rematch_count']}")
                features_str = ", ".join(active_features) or "None"
                message += (
                    f"👤 *User ID*: {user_id}\n"
                    f"🧑 *Name*: {name}\n"
                    f"📅 *Premium Until*: {expiry_date}\n"
                    f"✨ *Features*: {features_str}\n"
                    f"━━━━━\n"
                )
                user_count += 1
                if len(message.encode('utf-8')) > 4000:
                    safe_reply(update, message)
                    message = ""
            if message:
                message += f"🌟 *Total Premium Users*: {user_count}\n"
                safe_reply(update, message)
            logger.info(f"Admin {user_id} requested premium users list with {user_count} users.")
            logger.debug(f"Premium users list: {[(u[0], u[1]) for u in users]}")
    except Exception as e:
        logger.error(f"Error fetching premium users list: {e}")
        safe_reply(update, "❌ Error retrieving premium users list.")
    finally:
        release_db_connection(conn)

def admin_info(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔐 Unauthorized.")
        return
    try:
        target_id = int(context.args[0])
        user = get_user(target_id)
        if not user:
            safe_reply(update, "❌ User not found.")
            return
        profile = user.get("profile", {})
        consent = "Yes" if user.get("consent") else "No"
        verified = "Yes" if user.get("verified") else "No"
        premium = user.get("premium_expiry")
        premium_status = f"Until {datetime.fromtimestamp(premium).strftime('%Y-%m-%d %H:%M:%S')}" if premium and premium > time.time() else "None"
        ban_status = user.get("ban_type")
        if ban_status == "permanent":
            ban_info = "Permanent"
        elif ban_status == "temporary" and user.get("ban_expiry") > time.time():
            ban_info = f"Until {datetime.fromtimestamp(user.get('ban_expiry')).strftime('%Y-%m-%d %H:%M:%S')}"
        else:
            ban_info = "None"
        created_at = datetime.fromtimestamp(user.get("created_at", int(time.time()))).strftime("%Y-%m-%d %H:%M:%S")
        features = ", ".join([k for k, v in user.get("premium_features", {}).items() if v is True or (isinstance(v, int) and v > time.time())]) or "None"
        # Check keyword violations
        conn = get_db_connection()
        violations_count = 0
        violation_status = "None"
        if conn:
            try:
                with conn.cursor() as c:
                    c.execute("SELECT count, ban_type, ban_expiry FROM keyword_violations WHERE user_id = %s", (target_id,))
                    violation = c.fetchone()
                    if violation:
                        violations_count = violation[0]
                        ban_type, ban_expiry = violation[1], violation[2]
                        violation_status = (
                            "Permanent" if ban_type == "permanent" else
                            f"Temporary until {datetime.fromtimestamp(ban_expiry).strftime('%Y-%m-%d %H:%M')}"
                            if ban_type == "temporary" and ban_expiry else f"{violations_count} warnings"
                        )
            finally:
                release_db_connection(conn)
        message = (
            f"👤 *User Info: {target_id}*\n\n"
            f"🧑 *Name*: {profile.get('name', 'Not set')}\n"
            f"🎂 *Age*: {profile.get('age', 'Not set')}\n"
            f"👥 *Gender*: {profile.get('gender', 'Not set')}\n"
            f"📍 *Location*: {profile.get('location', 'Not set')}\n"
            f"📝 *Tags*: {', '.join(profile.get('tags', [])) or 'None'}\n"
            f"🔐 *Consent*: {consent}\n"
            f"✅ *Verified*: {verified}\n"
            f"🌟 *Premium*: {premium_status}\n"
            f"✨ *Features*: {features}\n"
            f"🚫 *Ban*: {ban_info}\n"
            f"⚠️ *Keyword Violations*: {violation_status}\n"
            f"📅 *Joined*: {created_at}"
        )
        safe_reply(update, message)
    except (IndexError, ValueError):
        safe_reply(update, "⚠️ Usage: /admin_info <user_id>")

def admin_reports(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔐 Unauthorized.")
        return
    conn = get_db_connection()
    if not conn:
        safe_reply(update, "❌ Database error.")
        return
    try:
        with conn.cursor() as c:
            c.execute("SELECT reported_id, COUNT(*) as count FROM reports GROUP BY reported_id ORDER BY count DESC LIMIT 20")
            reports = c.fetchall()
            if not reports:
                safe_reply(update, "📜 No reports found.")
                return
            message = "🚨 *Reported Users* \\(Top 20\\)\n\n"
            for reported_id, count in reports:
                message += f"👤 {reported_id} | Reports: *{count}*\n"
            safe_reply(update, message)
    except Exception as e:
        logger.error(f"Failed to list reports: {e}")
        safe_reply(update, "❌ Error retrieving reports.")
    finally:
        release_db_connection(conn)

def admin_clear_reports(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔐 Unauthorized.")
        return
    try:
        target_id = int(context.args[0])
        conn = get_db_connection()
        if not conn:
            safe_reply(update, "❌ Database error.")
            return
        try:
            with conn.cursor() as c:
                c.execute("DELETE FROM reports WHERE reported_id = %s", (target_id,))
                conn.commit()
                safe_reply(update, f"🗑️ Reports cleared for user *{target_id}*.")
                logger.info(f"Admin {user_id} cleared reports for {target_id}.")
        finally:
            release_db_connection(conn)
    except (IndexError, ValueError):
        safe_reply(update, "⚠️ Usage: /admin_clear_reports <user_id>")

def admin_broadcast(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔐 Unauthorized.")
        return
    if not context.args:
        safe_reply(update, "⚠️ Usage: /admin_broadcast <message>")
        return
    message = "📢 *Announcement*: " + " ".join(context.args)
    conn = get_db_connection()
    if not conn:
        safe_reply(update, "❌ Database error.")
        return
    try:
        with conn.cursor() as c:
            c.execute("SELECT user_id FROM users WHERE consent = TRUE")
            users = c.fetchall()
            sent_count = 0
            for (uid,) in users:
                try:
                    safe_bot_send_message(context.bot, uid, message)
                    sent_count += 1
                except Exception as e:
                    logger.warning(f"Failed to send broadcast to {uid}: {e}")
            safe_reply(update, f"📢 Broadcast sent to *{sent_count}* users.")
            logger.info(f"Admin {user_id} sent broadcast to {sent_count} users.")
    except Exception as e:
        logger.error(f"Failed to send broadcast: {e}")
        safe_reply(update, "❌ Error sending broadcast.")
    finally:
        release_db_connection(conn)

def admin_stats(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔐 Unauthorized.")
        return
    conn = get_db_connection()
    if not conn:
        safe_reply(update, "❌ Database error.")
        return
    try:
        with conn.cursor() as c:
            # Total users
            c.execute("SELECT COUNT(*) FROM users")
            total_users = c.fetchone()[0]
            # Premium users
            current_time = int(time.time())
            c.execute(
                "SELECT COUNT(*) FROM users WHERE premium_expiry IS NOT NULL AND premium_expiry > %s",
                (current_time,)
            )
            premium_users = c.fetchone()[0]
            # Banned users
            c.execute(
                "SELECT COUNT(*) FROM users WHERE ban_type IS NOT NULL AND (ban_type = 'permanent' OR (ban_type = 'temporary' AND ban_expiry > %s))",
                (current_time,)
            )
            banned_users = c.fetchone()[0]
        # Active users (in user_pairs or waiting_users)
        active_users = len(set(user_pairs.keys()).union(waiting_users))
        stats_message = (
            "📊 *Bot Statistics* 📊\n\n"
            f"🧑‍🤝‍🧑 *Total Users*: *{total_users}*\n"
            f"🌟 *Premium Users*: *{premium_users}*\n"
            f"💬 *Active Users*: *{active_users}* \\(in chats or waiting\\)\n"
            f"🚫 *Banned Users*: *{banned_users}*\n"
            f"━━━━━━━━━━━\n"
            f"📅 *Updated*: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        safe_reply(update, stats_message)
        logger.info(f"Admin {user_id} requested bot statistics: total={total_users}, premium={premium_users}, active={active_users}, banned={banned_users}")
    except Exception as e:
        logger.error(f"Error fetching bot statistics: {e}")
        safe_reply(update, "❌ Error retrieving statistics.")
    finally:
        release_db_connection(conn)

def main() -> None:
    token = os.getenv("BOT_TOKEN")
    if not token:
        logger.error("BOT_TOKEN not set.")
        exit(1)

    updater = Updater(token, use_context=True)
    dp = updater.dispatcher

    # Conversation handler
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            CommandHandler("settings", settings)
        ],
        states={
            NAME: [
                MessageHandler(Filters.text & ~Filters.command, set_name),
                CallbackQueryHandler(button)
            ],
            AGE: [
                MessageHandler(Filters.text & ~Filters.command, set_age),
                CallbackQueryHandler(button)
            ],
            GENDER: [
                CallbackQueryHandler(button)
            ],
            LOCATION: [
                MessageHandler(Filters.text & ~Filters.command, set_location),
                CallbackQueryHandler(button)
            ],
            CONSENT: [
                CallbackQueryHandler(consent_handler)
            ],
            VERIFICATION: [
                CallbackQueryHandler(verify_emoji)
            ],
            TAGS: [
                MessageHandler(Filters.text & ~Filters.command, set_tags),
                CallbackQueryHandler(button)
            ]
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CallbackQueryHandler(button)
        ]
    )

    # Add handlers
    dp.add_handler(conv_handler)
    dp.add_handler(CommandHandler("stop", stop))
    dp.add_handler(CommandHandler("next", next_chat))
    dp.add_handler(CommandHandler("help", help_command))
    dp.add_handler(CommandHandler("premium", premium))
    dp.add_handler(CommandHandler("shine", shine))
    dp.add_handler(CommandHandler("instant", instant))
    dp.add_handler(CommandHandler("mood", mood))
    dp.add_handler(CommandHandler("vault", vault))
    dp.add_handler(CommandHandler("history", history))
    dp.add_handler(CommandHandler("report", report))
    dp.add_handler(CommandHandler("deleteprofile", delete_profile))
    dp.add_handler(CallbackQueryHandler(button))
    dp.add_handler(PreCheckoutQueryHandler(pre_checkout))
    dp.add_handler(MessageHandler(Filters.successful_payment, successful_payment))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, message_handler))

    # Admin commands
    dp.add_handler(CommandHandler("admin", admin_access))
    dp.add_handler(CommandHandler("admin_delete", admin_delete))
    dp.add_handler(CommandHandler("admindelete", admin_delete))
    dp.add_handler(CommandHandler("admin_premium", admin_premium))
    dp.add_handler(CommandHandler("adminpremium", admin_premium))
    dp.add_handler(CommandHandler("admin_revoke_premium", admin_revoke_premium))
    dp.add_handler(CommandHandler("adminrevokepremium", admin_revoke_premium))
    dp.add_handler(CommandHandler("admin_ban", admin_ban))
    dp.add_handler(CommandHandler("adminban", admin_ban))
    dp.add_handler(CommandHandler("admin_unban", admin_unban))
    dp.add_handler(CommandHandler("adminunban", admin_unban))
    dp.add_handler(CommandHandler("admin_info", admin_info))
    dp.add_handler(CommandHandler("admininfo", admin_info))
    dp.add_handler(CommandHandler("admin_userslist", admin_userslist))
    dp.add_handler(CommandHandler("adminuserslist", admin_userslist))
    dp.add_handler(CommandHandler("admin_premiumuserslist", admin_premiumuserslist))
    dp.add_handler(CommandHandler("adminpremiumuserslist", admin_premiumuserslist))
    dp.add_handler(CommandHandler("admin_reports", admin_reports))
    dp.add_handler(CommandHandler("adminreports", admin_reports))
    dp.add_handler(CommandHandler("admin_clear_reports", admin_clear_reports))
    dp.add_handler(CommandHandler("adminclearreports", admin_clear_reports))
    dp.add_handler(CommandHandler("admin_broadcast", admin_broadcast))
    dp.add_handler(CommandHandler("adminbroadcast", admin_broadcast))
    dp.add_handler(CommandHandler("admin_violations", admin_violations))
    dp.add_handler(CommandHandler("adminviolations", admin_violations))
    dp.add_handler(CommandHandler("admin_stats", admin_stats))
    dp.add_handler(CommandHandler("adminstats", admin_stats))

    # Error handler
    dp.add_error_handler(error_handler)

    # Periodic cleanup
    updater.job_queue.run_repeating(cleanup_in_memory, interval=300, first=10)
    updater.job_queue.run_repeating(cleanup_rematch_requests, interval=60, first=10)

    # Start the bot
    updater.start_polling(allowed_updates=Update.ALL_TYPES)
    logger.info("Bot started polling.")
    updater.idle()

if __name__ == "__main__":
    main()
