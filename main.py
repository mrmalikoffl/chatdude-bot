from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice, Message
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    PreCheckoutQueryHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)
import logging
import asyncio
import os
import time
import signal
from datetime import datetime, timedelta
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from queue import Queue
from telegram.constants import ParseMode
import re
from collections import defaultdict
import warnings
from telegram.error import TelegramError, BadRequest  # Add BadRequest
import random
from typing import Tuple
import threading
from functools import wraps
from functools import lru_cache
from cachetools import TTLCache

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
ADMIN_IDS = set(int(x) for x in os.getenv("ADMIN_IDS", "5975525252").split(",") if x.isdigit())
NOTIFICATION_CHANNEL_ID = os.getenv("NOTIFICATION_CHANNEL_ID", "-1002519617667")

# Security configurations
BANNED_WORDS = {
    "spam", "hate", "abuse", "nsfw", "inappropriate", "offensive",
    "harass", "bully", "threat", "sex", "porn", "nude", "violence",
    "scam", "hack", "phish", "malware"
}
BLOCKED_KEYWORDS = [
    r"\bf[u*]ck\b|\bsh[i1][t*]\b|\bb[i1]tch\b|\ba[s$][s$]h[o0]le\b",
    r"\bd[i1]ck\b|\bp[u*][s$][s$]y\b|\bc[o0]ck\b|\bt[i1]t[s$]\b",
    r"\bn[i1]gg[e3]r\b|\bf[a4]g\b|\br[e3]t[a4]rd\b",
    r"\bidi[o0]t\b|\bm[o0]r[o0]n\b|\bsc[u*]m\b",
    r"\bs[e3]x\b|\bp[o0]rn\b|\br[a4]p[e3]\b|\bh[o0]rny\b",
    r"\bh[a4]t[e3]\b|\bb[u*]lly\b|\bk[i1]ll\b|\bdi[e3]\b",
]
COMMAND_COOLDOWN = 5
MAX_MESSAGES_PER_MINUTE = 15
REPORT_THRESHOLD = 3
TEMP_BAN_DURATION = 24 * 3600  # 1 day
MAX_PROFILE_LENGTH = 500
ALLOWED_TAGS = {
    "music", "gaming", "movies", "tech", "sports", "art",
    "travel", "food", "books", "fashion", "photography",
    "nature", "science", "history", "coding"
}

# Bot information configuration
BOT_INFO = {
    "name": "Talk2Anyone",
    "tools": "Python, python-telegram-bot, MongoDB, Heroku",
    "created_by": "Mr Malik",
    "language": "Python 3.10",
    "owner": "Malik Infotech Pvt Ltd",
    "version": "1.0.0",
    "support": "@mrmalik_offl"
}

# In-memory storage
waiting_users_lock = threading.Lock()
waiting_users = []
user_pairs = {}
user_activities = {}
command_timestamps = {}
message_timestamps = defaultdict(list)
chat_histories = {}
INACTIVITY_TIMEOUT = 600  # 10 minutes
notification_cache = TTLCache(maxsize=1000, ttl=300)  # 5-minute cooldown
user_pairs_lock = threading.Lock()  # Lock for user_pairs (fixes the error)

# Conversation states
NAME, AGE, GENDER, LOCATION, CONSENT, VERIFICATION, TAGS = range(7)

# Define Telegram MarkdownV2 special characters that need escaping
MARKDOWNV2_SPECIAL_CHARS = r"""_*[]()~`>#+-=|{}.!'"""

# Separate cache for admin notifications (10-minute TTL)
admin_notification_cache = TTLCache(maxsize=1000, ttl=600)

# Emoji list for verification
VERIFICATION_EMOJIS = ['😊', '😢', '😡', '😎', '😍', '😉', '😜', '😴']

# Flag to track shutdown state
_shutdown_initiated = False

# MongoDB client and database
mongo_client = None
db = None
operation_queue = Queue()

def init_mongodb(max_retries=3, retry_delay=5):
    uri = os.getenv("MONGODB_URI")
    db_name = os.getenv("MONGO_DB_NAME", "talk2anyone")
    if not uri:
        logger.error("MONGODB_URI not set")
        raise EnvironmentError("MONGODB_URI not set")

    for attempt in range(max_retries):
        try:
            logger.info(f"Connecting to MongoDB with URI: {uri} (attempt {attempt + 1}/{max_retries})")
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            db = client[db_name]
            db.users.create_index("user_id", unique=True)
            db.reports.create_index("reported_id")
            db.keyword_violations.create_index("user_id")
            logger.info("MongoDB connected and indexes created")
            return db
        except (ConnectionFailure, OperationFailure) as e:
            logger.error(f"MongoDB connection attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
        except Exception as e:
            logger.error(f"Unexpected error initializing MongoDB: {e}")
            raise
    logger.error("Failed to connect to MongoDB after all retries")
    raise Exception("MongoDB connection failed")

def get_db_collection(collection_name):
    if db is None:
        logger.error("Database connection not initialized")
        raise Exception("Database connection not initialized")
    return db[collection_name]

async def process_queued_operations(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Process queued database or notification operations."""
    max_retries = 3
    operation_retries = {}
    
    while not operation_queue.empty():
        op_type, args = operation_queue.get()
        operation_key = (op_type, str(args))
        retries = operation_retries.get(operation_key, 0)
        
        if retries >= max_retries:
            logger.error(f"Operation {op_type} with args {args} failed after {max_retries} attempts")
            operation_retries.pop(operation_key, None)
            continue
        
        try:
            if op_type == "update_user":
                update_user(*args)
            elif op_type == "delete_user":
                delete_user(*args)
            elif op_type == "issue_keyword_violation":
                issue_keyword_violation(*args)
            elif op_type == "send_channel_notification":
                await send_channel_notification(context, *args)
            logger.info(f"Successfully processed queued operation: {op_type}")
            operation_retries.pop(operation_key, None)
        except Exception as e:
            logger.error(f"Failed to process queued operation {op_type}: {e}")
            operation_retries[operation_key] = retries + 1
            if operation_retries[operation_key] < max_retries:
                logger.info(f"Requeuing operation {op_type} (attempt {retries + 1}/{max_retries})")
                operation_queue.put((op_type, args))
            else:
                operation_retries.pop(operation_key, None)

def restrict_access(handler):
    @wraps(handler)
    async def wrapper(update: Update, context: ContextTypes, *args, **kwargs):
        user_id = update.effective_user.id
        
        # Allow /start and /deleteprofile without restrictions
        if handler.__name__ in ["start", "delete_profile"]:
            return await handler(update, context, *args, **kwargs)
        
        # Check user existence in database
        user = get_db_collection("users").find_one({"user_id": user_id})
        if not user:
            await safe_reply(
                update,
                "⚠️ Your profile was deleted or not found\\. Please use /start to register\\.",
                context,
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return ConversationHandler.END
        
        # Ban check
        if is_banned(user_id):
            ban_msg = (
                "🚫 You are permanently banned 🔒\\. Contact support to appeal 📧\\."
                if user.get("ban_type") == "permanent"
                else f"🚫 You are banned until {datetime.fromtimestamp(user.get('ban_expiry')).strftime('%Y-%m-%d %H:%M:%S')} ⏰."
            )
            await safe_reply(update, ban_msg, context, parse_mode=ParseMode.MARKDOWN_V2)
            return ConversationHandler.END
        
        # Admin commands
        if user_id in ADMIN_IDS and handler.__name__.startswith("admin_"):
            return await handler(update, context, *args, **kwargs)
        
        # Consent and verification checks
        if not user.get("consent", False) or not user.get("verified", False):
            logger.warning(f"User {user_id} failed consent/verified check: consent={user.get('consent')}, verified={user.get('verified')}")
            await safe_reply(
                update,
                "⚠️ Please complete your profile setup with /start\\.",
                context,
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return ConversationHandler.END
        
        # Profile completeness check
        if not is_profile_complete(user):
            logger.warning(f"User {user_id} profile incomplete: {user.get('profile')}")
            await safe_reply(
                update,
                "⚠️ Your profile is incomplete\\. Use /start to finish setting it up\\.",
                context,
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return ConversationHandler.END
        
        return await handler(update, context, *args, **kwargs)
    return wrapper

def is_profile_complete(user: dict) -> bool:
    """Check if the user's profile is complete."""
    profile = user.get("profile", {})
    required_fields = ["name", "age", "gender", "location"]
    return all(profile.get(field) for field in required_fields)

async def cleanup_in_memory(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Clean up in-memory data like inactive user pairs and waiting users."""
    logger.info(f"Cleaning up in-memory data. user_pairs: {len(user_pairs)}, waiting_users: {len(waiting_users)}")
    current_time = time.time()
    
    # Clean up inactive user pairs
    for user_id in list(user_pairs.keys()):
        last_activity = user_activities.get(user_id, {}).get("last_activity", 0)
        if current_time - last_activity > INACTIVITY_TIMEOUT:
            partner_id = user_pairs.get(user_id)
            if partner_id:
                await safe_bot_send_message(context.bot, user_id, "🛑 Chat ended due to inactivity\\.", context, parse_mode=ParseMode.MARKDOWN_V2)
                await safe_bot_send_message(context.bot, partner_id, "🛑 Chat ended due to inactivity\\.", context, parse_mode=ParseMode.MARKDOWN_V2)
                remove_pair(user_id, partner_id)
                chat_histories.pop(user_id, None)
                chat_histories.pop(partner_id, None)
    
    # Clean up inactive waiting users
    with waiting_users_lock:
        for user_id in waiting_users[:]:
            last_activity = user_activities.get(user_id, {}).get("last_activity", 0)
            if current_time - last_activity > INACTIVITY_TIMEOUT:
                waiting_users.remove(user_id)
                await safe_bot_send_message(context.bot, user_id, "🛑 Removed from waiting list due to inactivity\\.", context, parse_mode=ParseMode.MARKDOWN_V2)
                chat_histories.pop(user_id, None)
    
    logger.info(f"Cleanup complete. user_pairs: {len(user_pairs)}, waiting_users: {len(waiting_users)}")

def remove_pair(user_id: int, partner_id: int) -> None:
    if user_id in user_pairs:
        del user_pairs[user_id]
    if partner_id in user_pairs:
        del user_pairs[partner_id]
    logger.info(f"Removed pair: {user_id} and {partner_id}")

# Database functions
@lru_cache(maxsize=1000)
def get_user_cached(user_id: int) -> dict:
    logger.info(f"Fetching user {user_id}")
    users = get_db_collection("users")
    user = users.find_one({"user_id": user_id})
    if not user:
        logger.info(f"Creating new user {user_id}")
        user = {
            "user_id": user_id,
            "profile": {},
            "consent": False,
            "verified": False,
            "created_at": int(time.time()),
            "premium_expiry": None,
            "premium_features": {},
            "ban_type": None,
            "ban_expiry": None
        }
        users.insert_one(user)
        user = users.find_one({"user_id": user_id})
    logger.debug(f"Returning user {user_id}: {user}")
    return user

def get_user_with_cache(user_id: int) -> dict:
    return get_user_cached(user_id)


def update_user(user_id: int, data: dict) -> bool:
    retries = 3
    for attempt in range(retries):
        try:
            users = get_db_collection("users")
            result = users.update_one(
                {"user_id": user_id},
                {"$set": data},
                upsert=True
            )
            if result.matched_count > 0 or result.upserted_id:
                logger.info(f"Updated user {user_id}: {data}")
                return True
            logger.warning(f"No user matched for update: user_id={user_id}")
            return False
        except pymongo.errors.PyMongoError as e:
            logger.error(f"Attempt {attempt + 1}/{retries} failed to update user {user_id}: {e}")
            if attempt < retries - 1:
                time.sleep(1)
        except Exception as e:
            logger.error(f"Unexpected error updating user {user_id}: {e}")
            break
    logger.error(f"Failed to update user {user_id} after {retries} attempts")
    return False

def delete_user(user_id: int, context: ContextTypes.DEFAULT_TYPE = None):
    try:
        reports = get_db_collection("reports")
        violations = get_db_collection("keyword_violations")
        users = get_db_collection("users")
        reports.delete_many({"$or": [{"reporter_id": user_id}, {"reported_id": user_id}]})
        violations.delete_many({"user_id": user_id})
        result = users.delete_one({"user_id": user_id})
        if result.deleted_count > 0:
            logger.info(f"Deleted user {user_id} from database")
        else:
            logger.warning(f"No user found with user_id {user_id}")
        
        # Clear in-memory data
        user_activities.pop(user_id, None)
        command_timestamps.pop(user_id, None)
        message_timestamps.pop(user_id, None)
        chat_histories.pop(user_id, None)
        
        with waiting_users_lock:
            if user_id in waiting_users:
                waiting_users.remove(user_id)
        
        with user_pairs_lock:
            if user_id in user_pairs:
                partner_id = user_pairs.pop(user_id)
                user_pairs.pop(partner_id, None)
                if context:
                    chat_key = tuple(sorted([user_id, partner_id]))
                    context.bot_data.get("chat_start_times", {}).pop(chat_key, None)
        
        if context:
            context.bot_data.get("rematch_requests", {}).pop(user_id, None)
            context.bot_data.get("personal_chat_requests", {}).pop(user_id, None)
        
        # Clear cache
        get_user_cached.cache_clear()
        logger.info(f"Cleared cache for user {user_id}")
        
    except (ConnectionError, OperationFailure) as e:
        logger.error(f"Failed to delete user {user_id}: {e}")
        operation_queue.put(("delete_user", (user_id, context)))
        raise
    except Exception as e:
        logger.error(f"Unexpected error deleting user {user_id}: {e}")
        raise

def escape_markdown_v2(text):
    """Escape special characters for Telegram MarkdownV2, preserving formatting markers."""
    if not isinstance(text, str):
        text = str(text)
    
    MARKDOWNV2_SPECIAL_CHARS = r'_*[]()~`>#+\-=|{}.!'
    formatting_pattern = r'(\*[^\*]+\*|_[^_]+_|```[^`]+```|\[[^\]]+\]\([^\)]+\))'
    
    def escape_non_formatting(text_part):
        """Escape all special characters in non-formatted text."""
        return ''.join(f'\\{char}' if char in MARKDOWNV2_SPECIAL_CHARS else char for char in text_part)
    
    parts = re.split(formatting_pattern, text)
    escaped_parts = []
    
    for part in parts:
        if re.match(formatting_pattern, part):
            if part.startswith('*') and part.endswith('*'):
                inner_text = part[1:-1]
                inner_text = escape_non_formatting(inner_text)  # Escape all special chars
                escaped_parts.append(f'*{inner_text}*')
            elif part.startswith('_') and part.endswith('_'):
                inner_text = part[1:-1]
                inner_text = escape_non_formatting(inner_text)  # Escape all special chars
                escaped_parts.append(f'_{inner_text}_')
            elif part.startswith('```') and part.endswith('```'):
                inner_text = part[3:-3]
                inner_text = escape_non_formatting(inner_text)  # Escape all special chars
                escaped_parts.append(f'```{inner_text}```')
            elif part.startswith('[') and ')' in part:
                link_text, url = part[1:].split('](', 1)
                url = url[:-1]
                link_text = escape_non_formatting(link_text)  # Escape all special chars
                url = escape_non_formatting(url)  # Escape all special chars
                escaped_parts.append(f'[{link_text}]({url})')
            else:
                escaped_parts.append(escape_non_formatting(part))
        else:
            escaped_parts.append(escape_non_formatting(part))
    
    return ''.join(escaped_parts)

def format_notification(reason: str, action: str = "wait or stop") -> str:
    """Format a consistent, friendly notification message with emojis."""
    base_message = f"🌈 *Cannot connect fellows*\\: {escape_markdown_v2(reason)}\n\n"
    if action == "wait or stop":
        base_message += "Please wait a moment for a new partner, or use /stop to exit and /next to search again\\! 😊"
    elif action == "update profile":
        base_message += "Update your profile with /settings and try /next to search again\\! 📝"
    return base_message

async def notify_user(user_id: int, message: str, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a notification to a user via Telegram with cooldown."""
    if (user_id, message) in notification_cache:
        logger.debug(f"Skipping duplicate notification for user {user_id}: {message[:50]}...")
        return
    notification_cache[(user_id, message)] = True
    
    try:
        if not message or message.strip() == "":
            message = "⚠️ An error occurred, but no details were provided 🌑."
        
        user_data = get_user_with_cache(user_id) or {}
        chat_id = user_data.get("chat_id")
        if not chat_id:
            logger.warning(f"Cannot notify user {user_id}: No chat_id found")
            admin_key = (user_id, "missing_chat_id")
            if admin_key not in admin_notification_cache:
                admin_notification_cache[admin_key] = True
                await send_channel_notification(
                    context,
                    f"⚠️ Failed to notify user {user_id}: No chat_id found 🌑"
                )
            return
        
        logger.debug(f"Preparing notification for user {user_id}: {message[:200]}")
        await safe_bot_send_message(
            context.bot,
            chat_id=chat_id,
            text=message,
            context=context,
            parse_mode=ParseMode.MARKDOWN_V2
        )
        logger.info(f"Sent notification to user {user_id}: {message[:50]}...")
    except telegram.error.TelegramError as e:
        logger.error(f"Failed to send notification to user {user_id}: {e}")
        fallback_message = f"⚠️ Error sending notification: {escape_markdown_v2(str(e))} 🌑"
        try:
            await safe_bot_send_message(
                context.bot,
                chat_id=chat_id,
                text=fallback_message,
                context=context,
                parse_mode=ParseMode.MARKDOWN_V2
            )
            logger.info(f"Sent fallback notification to user {user_id}: {fallback_message[:50]}...")
        except telegram.error.TelegramError as e2:
            logger.error(f"Failed to send fallback notification to user {user_id}: {e2}")
            admin_key = (user_id, f"fallback_error_{str(e2)}")
            if admin_key not in admin_notification_cache:
                admin_notification_cache[admin_key] = True
                await send_channel_notification(
                    context,
                    f"⚠️ Failed to send fallback notification to user {user_id}: {escape_markdown_v2(str(e2))} 🌑"
                )
    except Exception as e:
        logger.error(f"Unexpected error in notify_user for user {user_id}: {e}")
        fallback_message = f"⚠️ Unexpected error: {escape_markdown_v2(str(e))} 🌑"
        try:
            await safe_bot_send_message(
                context.bot,
                chat_id=chat_id,
                text=fallback_message,
                context=context,
                parse_mode=ParseMode.MARKDOWN_V2
            )
            logger.info(f"Sent fallback notification to user {user_id}: {fallback_message[:50]}...")
        except telegram.error.TelegramError as e2:
            logger.error(f"Failed to send fallback notification to user {user_id}: {e2}")
            admin_key = (user_id, f"unexpected_error_{str(e2)}")
            if admin_key not in admin_notification_cache:
                admin_notification_cache[admin_key] = True
                await send_channel_notification(
                    context,
                    f"⚠️ Failed to send fallback notification to user {user_id}: {escape_markdown_v2(str(e2))} 🌑"
                )

async def safe_reply(
    update: Update,
    text: str,
    context: ContextTypes.DEFAULT_TYPE,
    parse_mode: str = ParseMode.MARKDOWN,
    **kwargs
) -> Message:  # Use Message instead of telegram.Message
    original_text = text
    try:
        logger.debug(f"Sending message with parse_mode={parse_mode}: {text[:200]}")
        if update.callback_query and not kwargs.get("reply_markup"):
            sent_message = await update.callback_query.message.edit_text(text, parse_mode=parse_mode, **kwargs)
        elif update.message:
            sent_message = await update.message.reply_text(text, parse_mode=parse_mode, **kwargs)
        else:
            sent_message = await update.callback_query.message.reply_text(text, parse_mode=parse_mode, **kwargs)
        
        if sent_message is None:
            raise TelegramError(f"Failed to send reply to user {update.effective_user.id}: Telegram API returned None")
        logger.info(f"Sent reply to user {update.effective_user.id}: {text[:50]}... (Message ID: {sent_message.message_id})")
        return sent_message
    except BadRequest as bre:  # Use BadRequest instead of telegram.error.BadRequest
        logger.warning(f"Markdown parsing failed: {bre}. Text: {text[:200]}")
        try:
            clean_text = re.sub(r'([_*[\]()~`>#+-|=}{.!])', '', original_text)
            logger.debug(f"Falling back to plain text: {clean_text[:200]}")
            if update.callback_query and not kwargs.get("reply_markup"):
                sent_message = await update.callback_query.message.edit_text(clean_text, parse_mode=None, **kwargs)
            elif update.message:
                sent_message = await update.message.reply_text(clean_text, parse_mode=None, **kwargs)
            else:
                sent_message = await update.callback_query.message.reply_text(clean_text, parse_mode=None, **kwargs)
            if sent_message is None:
                raise TelegramError(f"Failed to send clean reply to user {update.effective_user.id}: Telegram API returned None")
            logger.info(f"Sent clean reply to user {update.effective_user.id}: {clean_text[:50]}... (Message ID: {sent_message.message_id})")
            return sent_message
        except TelegramError as e:
            logger.error(f"Failed to send clean reply to user {update.effective_user.id}: {e}")
            raise
    except TelegramError as e:
        logger.error(f"Failed to send reply to user {update.effective_user.id}: {e}")
        raise

async def safe_bot_send_message(
    bot,
    chat_id: int,
    text: str,
    context: ContextTypes.DEFAULT_TYPE,
    parse_mode: str = ParseMode.MARKDOWN,
    **kwargs
) -> Message:  # Use Message instead of telegram.Message
    try:
        logger.debug(f"Sending message to chat {chat_id} with parse_mode={parse_mode}: {text[:200]}")
        sent_message = await bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode, **kwargs)
        if sent_message is None:
            raise TelegramError(f"Failed to send message to chat {chat_id}: Telegram API returned None")
        logger.info(f"Sent message to chat {chat_id}: {text[:50]}... (Message ID: {sent_message.message_id})")
        return sent_message
    except BadRequest as e:  # Use BadRequest instead of telegram.error.BadRequest
        logger.warning(f"Markdown parsing failed for chat {chat_id}: {e}. Text: {text[:200]}")
        try:
            clean_text = re.sub(r'([_*[\]()~`>#+-|=}{.!])', '', text)
            logger.debug(f"Falling back to plain text: {clean_text[:200]}")
            sent_message = await bot.send_message(chat_id=chat_id, text=clean_text, parse_mode=None, **kwargs)
            if sent_message is None:
                raise TelegramError(f"Failed to send clean message to chat {chat_id}: Telegram API returned None")
            logger.info(f"Sent clean message to chat {chat_id}: {clean_text[:50]}... (Message ID: {sent_message.message_id})")
            return sent_message
        except TelegramError as e:
            logger.error(f"Failed to send clean message to chat {chat_id}: {e}")
            raise
    except TelegramError as e:
        logger.error(f"Failed to send message to chat {chat_id}: {e}")
        raise
        
async def send_channel_notification(context: ContextTypes.DEFAULT_TYPE, message: str) -> None:
    """Send a notification to the configured Telegram channel."""
    if not NOTIFICATION_CHANNEL_ID:
        logger.warning("Notification channel ID not set, cannot send notification")
        return

    # Apply cooldown to admin notifications (10-minute TTL)
    admin_key = (message,)
    if admin_key in admin_notification_cache:
        logger.debug(f"Skipping duplicate channel notification: {message[:50]}...")
        return
    admin_notification_cache[admin_key] = True

    try:
        if not message or message.strip() == "":
            message = "⚠️ An error occurred, but no details were provided 🌑."
        logger.debug(f"Preparing channel notification: {message[:200]}")
        # Escape the message for MarkdownV2
        escaped_message = escape_markdown_v2(message)
        await safe_bot_send_message(
            context.bot,
            chat_id=NOTIFICATION_CHANNEL_ID,
            text=escaped_message,
            context=context,
            parse_mode=ParseMode.MARKDOWN_V2  # Updated to MarkdownV2
        )
        logger.info(f"Sent channel notification: {message[:50]}...")
    except telegram.error.TelegramError as e:
        logger.error(f"Failed to send channel notification: {e}")
        fallback_message = f"⚠️ Error sending notification: {escape_markdown_v2(str(e))} 🌑"
        try:
            await safe_bot_send_message(
                context.bot,
                chat_id=NOTIFICATION_CHANNEL_ID,
                text=fallback_message,
                context=context,
                parse_mode=ParseMode.MARKDOWN_V2  # Use MarkdownV2 for fallback
            )
            logger.info(f"Sent fallback channel notification: {fallback_message[:50]}...")
        except telegram.error.TelegramError as e2:
            logger.error(f"Failed to send fallback channel notification: {e2}")
    except Exception as e:
        logger.error(f"Unexpected error in send_channel_notification: {e}")
        fallback_message = f"⚠️ Unexpected error: {escape_markdown_v2(str(e))} 🌑"
        try:
            await safe_bot_send_message(
                context.bot,
                chat_id=NOTIFICATION_CHANNEL_ID,
                text=fallback_message,
                context=context,
                parse_mode=ParseMode.MARKDOWN_V2  # Use MarkdownV2 for fallback
            )
            logger.info(f"Sent fallback channel notification: {fallback_message[:50]}...")
        except telegram.error.TelegramError as e2:
            logger.error(f"Failed to send fallback channel notification: {e2}")

@restrict_access
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    logger.info(f"Received /start command from user {user_id} with chat_id {chat_id}")
    
    # Save chat_id
    try:
        user = get_user_with_cache(user_id)
        user_data = user or {}
        user_data["chat_id"] = chat_id
        user_data["created_at"] = user_data.get("created_at", int(time.time()))
        success = update_user(user_id, user_data)
        if not success:
            logger.error(f"Failed to update chat_id for user {user_id}")
            await safe_reply(
                update,
                "⚠️ Error saving your chat ID\\. Please try again or contact support\\.",
                context,
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return ConversationHandler.END
        logger.info(f"Updated user {user_id} with chat_id {chat_id}")
    except pymongo.errors.PyMongoError as e:
        logger.error(f"Database error updating user {user_id}: {e}")
        await safe_reply(
            update,
            "⚠️ Error saving your chat ID\\. Please try again or contact support\\.",
            context,
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return ConversationHandler.END

    # Fetch user data again for consistency
    user = get_user_with_cache(user_id)
    logger.debug(f"User data for {user_id}: {user}")

    # Check if user is banned
    if is_banned(user_id):
        ban_type = user.get("ban_type", "permanent")
        ban_expiry = user.get("ban_expiry")
        ban_msg = (
            "🚫 You are permanently banned 🔒\\. Contact support to appeal 📧\\."
            if ban_type == "permanent"
            else f"🚫 You are banned until {datetime.fromtimestamp(ban_expiry).strftime('%Y-%m-%d %H:%M')} ⏰\\."
        )
        await safe_reply(update, ban_msg, context, parse_mode=ParseMode.MARKDOWN_V2)
        return ConversationHandler.END
    
    # Check rate limit
    if not check_rate_limit(user_id):
        await safe_reply(
            update,
            f"⏳ Please wait {COMMAND_COOLDOWN} seconds before trying again ⏰\\.",
            context,
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return ConversationHandler.END
    
    # Check if user is already in a chat
    if user_id in user_pairs:
        await safe_reply(
            update,
            "💬 You're already in a chat 😔\\. Use /next to switch or /stop to end\\.",
            context,
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return ConversationHandler.END
    
    # Check if user is waiting for a chat partner
    if user_id in waiting_users:
        await safe_reply(
            update,
            "🔍 You're already waiting for a chat partner\\.\\.\\. Please wait\\!",
            context,
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return ConversationHandler.END
    
    # Check if profile is complete
    if user.get("consent") and user.get("verified") and is_profile_complete(user):
        logger.info(f"User {user_id} has completed profile: consent={user.get('consent')}, verified={user.get('verified')}")
        profile = user.get("profile", {})
        profile_text = (
            f"👤 *Your Profile* 👤\n\n"
            f"🧑 *Name*: {escape_markdown_v2(profile.get('name', 'Not set'))}\n"
            f"🎂 *Age*: {escape_markdown_v2(str(profile.get('age', 'Not set')))}\n"
            f"👤 *Gender*: {escape_markdown_v2(profile.get('gender', 'Not set'))}\n"
            f"📍 *Location*: {escape_markdown_v2(profile.get('location', 'Not set'))}\n"
            f"🏷️ *Tags*: {escape_markdown_v2(', '.join(profile.get('tags', [])) or 'None')}\n\n"
            f"🔍 Use /next to find a chat partner\\!\n"
            f"⚙️ Use /settings to update your profile\\."
        )
        await safe_reply(update, profile_text, context, parse_mode=ParseMode.MARKDOWN_V2)
        # Ensure setup_state is cleared
        update_user(user_id, {"setup_state": None})
        context.user_data["state"] = None
        get_user_cached.cache_clear()  # Clear cache at the end
        return ConversationHandler.END
    
    # Check current setup state
    current_state = user.get("setup_state")
    if current_state is not None:
        logger.info(f"User {user_id} has setup_state: {current_state}")
        context.user_data["state"] = current_state
        message = escape_markdown_v2(f"Continuing setup. Please provide the requested information for {current_state}.")
        await safe_reply(update, message, context, parse_mode=ParseMode.MARKDOWN_V2)
        get_user_cached.cache_clear()
        return current_state

    # If no consent, prompt for it
    if not user.get("consent"):
        logger.info(f"User {user_id} has no consent, prompting for consent")
        keyboard = [
            [InlineKeyboardButton("✅ I Agree", callback_data="consent_agree")],
            [InlineKeyboardButton("❌ I Disagree", callback_data="consent_disagree")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        welcome_text = (
            "🌟 *Welcome to Talk2Anyone\\!* 🌟\n\n"
            "Chat anonymously with people worldwide\\! 🌍\n"
            "Rules:\n"
            "• No harassment, spam, or inappropriate content\n"
            "• Respect everyone\n"
            "• Report issues with /report\n"
            "• Violations may lead to bans\n\n"
            "🔒 *Privacy*: We store only your user ID, profile, and consent securely\\. Use /deleteprofile to remove data\\.\n\n"
            "Do you agree to the rules\\?"
        )
        await safe_reply(update, welcome_text, context, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2)
        await send_channel_notification(context, (
            "🆕 *New User Accessed* 🆕\n\n"
            f"👤 *User ID*: {user_id}\n"
            f"📅 *Time*: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
            "ℹ️ Awaiting consent"
        ))
        update_user(user_id, {"setup_state": CONSENT})
        context.user_data["state"] = CONSENT
        get_user_cached.cache_clear()
        return CONSENT
    
    # If not verified, prompt for verification
    if not user.get("verified"):
        logger.info(f"User {user_id} is not verified, prompting for verification")
        correct_emoji = random.choice(VERIFICATION_EMOJIS)
        context.user_data["correct_emoji"] = correct_emoji
        other_emojis = random.sample([e for e in VERIFICATION_EMOJIS if e != correct_emoji], 3)
        all_emojis = [correct_emoji] + other_emojis
        random.shuffle(all_emojis)
        keyboard = [[InlineKeyboardButton(emoji, callback_data=f"emoji_{emoji}") for emoji in all_emojis]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = f"🔐 *Verify Your Profile* 🔐\n\nPlease select this emoji: *{escape_markdown_v2(correct_emoji)}*"
        await safe_reply(update, message, context, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2)
        update_user(user_id, {"setup_state": VERIFICATION})
        context.user_data["state"] = VERIFICATION
        get_user_cached.cache_clear()
        return VERIFICATION
    
    # If profile is incomplete, start profile setup
    profile = user.get("profile", {})
    required_fields = ["name", "age", "gender", "location"]
    missing_fields = [field for field in required_fields if not profile.get(field)]
    if missing_fields:
        logger.info(f"User {user_id} has missing profile fields: {missing_fields}")
        await safe_reply(
            update,
            "✨ Let’s set up your profile\\! Please enter your name:",
            context,
            parse_mode=ParseMode.MARKDOWN_V2
        )
        update_user(user_id, {"setup_state": NAME})
        context.user_data["state"] = NAME
        get_user_cached.cache_clear()
        return NAME
    
    # Fallback: Profile should be complete, but ensure setup_state is cleared
    logger.warning(f"User {user_id} reached fallback case: consent={user.get('consent')}, verified={user.get('verified')}, profile={profile}")
    await safe_reply(
        update,
        "🎉 Your profile is ready\\! Use /next to find a chat partner and start connecting\\! 🚀",
        context,
        parse_mode=ParseMode.MARKDOWN_V2
    )
    update_user(user_id, {"setup_state": None})
    context.user_data["state"] = None
    get_user_cached.cache_clear()
    return ConversationHandler.END

async def consent_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    choice = query.data
    logger.info(f"User {user_id} selected consent choice: {choice}")

    if choice == "consent_agree":
        try:
            user = get_user_with_cache(user_id)
            user_data = {
                "user_id": user_id,
                "consent": True,
                "consent_time": int(time.time()),
                "profile": user.get("profile", {}),
                "premium_expiry": user.get("premium_expiry"),
                "premium_features": user.get("premium_features", {}),
                "created_at": user.get("created_at", int(time.time())),
                "setup_state": VERIFICATION,
                "verified": user.get("verified", False),
                "chat_id": user.get("chat_id")
            }
            success = update_user(user_id, user_data)
            if not success:
                logger.error(f"Failed to update consent for user {user_id}")
                await safe_reply(update, "⚠️ Failed to update consent. Please try again.", context)
                return ConversationHandler.END
            logger.info(f"User {user_id} consent updated successfully")
            await safe_reply(update, "✅ Thank you for agreeing! Let’s verify your profile.", context)
            correct_emoji = random.choice(VERIFICATION_EMOJIS)
            context.user_data["correct_emoji"] = correct_emoji
            other_emojis = random.sample([e for e in VERIFICATION_EMOJIS if e != correct_emoji], 3)
            all_emojis = [correct_emoji] + other_emojis
            random.shuffle(all_emojis)
            keyboard = [[InlineKeyboardButton(emoji, callback_data=f"emoji_{emoji}") for emoji in all_emojis]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            message = f"🔐 *Verify Your Profile* 🔐\n\nPlease select this emoji: *{escape_markdown_v2(correct_emoji)}*"
            await safe_reply(update, message, context, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2)
            context.user_data["state"] = VERIFICATION
            get_user_cached.cache_clear()
            return VERIFICATION
        except pymongo.errors.PyMongoError as e:
            logger.error(f"Database error in consent_handler for user {user_id}: {e}")
            await safe_reply(update, "⚠️ An error occurred. Please try again with /start.", context)
            return ConversationHandler.END
        except Exception as e:
            logger.error(f"Unexpected error in consent_handler for user {user_id}: {e}")
            await safe_reply(update, "⚠️ An error occurred. Please try again with /start.", context)
            return ConversationHandler.END
    else:
        logger.info(f"User {user_id} disagreed to terms")
        await safe_reply(update, "❌ You must agree to the rules to use this bot. Use /start to try again.", context)
        update_user(user_id, {"setup_state": None})
        context.user_data["state"] = None
        get_user_cached.cache_clear()
        return ConversationHandler.END

async def verify_emoji(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    selected_emoji = query.data.replace("emoji_", "")
    correct_emoji = context.user_data.get("correct_emoji")
    if selected_emoji == correct_emoji:
        user = get_user_with_cache(user_id)
        user_data = {
            "user_id": user_id,
            "verified": True,
            "profile": user.get("profile", {}),
            "premium_expiry": user.get("premium_expiry"),
            "premium_features": user.get("premium_features", {}),
            "created_at": user.get("created_at", int(time.time())),
            "setup_state": NAME,
            "chat_id": user.get("chat_id")
        }
        success = update_user(user_id, user_data)
        if not success:
            logger.error(f"Failed to update verification for user {user_id}")
            await safe_reply(update, "⚠️ Failed to verify profile. Please try again.", context)
            return ConversationHandler.END
        await safe_reply(update, "🎉 Profile verified successfully! Let’s set up your profile.", context)
        await safe_reply(update, "✨ Please enter your name:", context)
        context.user_data["state"] = NAME
        get_user_cached.cache_clear()
        return NAME
    else:
        correct_emoji = random.choice(VERIFICATION_EMOJIS)
        context.user_data["correct_emoji"] = correct_emoji
        other_emojis = random.sample([e for e in VERIFICATION_EMOJIS if e != correct_emoji], 3)
        all_emojis = [correct_emoji] + other_emojis
        random.shuffle(all_emojis)
        keyboard = [[InlineKeyboardButton(emoji, callback_data=f"emoji_{emoji}") for emoji in all_emojis]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await safe_reply(update, f"❌ Incorrect emoji. Try again!\nPlease select this emoji: *{escape_markdown_v2(correct_emoji)}*", context, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2)
        get_user_cached.cache_clear()
        return VERIFICATION

async def set_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    user = get_user_with_cache(user_id)
    profile = user.get("profile", {})
    name = update.message.text.strip()
    if not 1 <= len(name) <= 50:
        await safe_reply(update, "⚠️ Name must be 1-50 characters.", context)
        return NAME
    is_safe, reason = is_safe_message(name)
    if not is_safe:
        await safe_reply(update, f"⚠️ Name contains inappropriate content: {reason}", context)
        return NAME
    profile["name"] = name
    user_data = {
        "user_id": user_id,
        "profile": profile,
        "premium_expiry": user.get("premium_expiry"),
        "premium_features": user.get("premium_features", {}),
        "created_at": user.get("created_at", int(time.time())),
        "setup_state": AGE,
        "chat_id": user.get("chat_id")
    }
    success = update_user(user_id, user_data)
    if not success:
        logger.error(f"Failed to update name for user {user_id}")
        await safe_reply(update, "⚠️ Failed to set name. Please try again.", context)
        return NAME
    context.user_data["state"] = AGE
    await safe_reply(update, f"🧑 Name set to: *{escape_markdown_v2(name)}*\\!", context, parse_mode=ParseMode.MARKDOWN_V2)
    await safe_reply(update, "🎂 Please enter your age \\(e.g., 25\\):", context, parse_mode=ParseMode.MARKDOWN_V2)
    get_user_cached.cache_clear()
    return AGE

async def set_age(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    user = get_user_with_cache(user_id)
    profile = user.get("profile", {})
    age_text = update.message.text.strip()
    try:
        age = int(age_text)
        if not 13 <= age <= 120:
            await safe_reply(update, "⚠️ Age must be between 13 and 120.", context)
            return AGE
        profile["age"] = age
        user_data = {
            "user_id": user_id,
            "profile": profile,
            "premium_expiry": user.get("premium_expiry"),
            "premium_features": user.get("premium_features", {}),
            "created_at": user.get("created_at", int(time.time())),
            "setup_state": GENDER,
            "chat_id": user.get("chat_id")
        }
        success = update_user(user_id, user_data)
        if not success:
            logger.error(f"Failed to update age for user {user_id}")
            await safe_reply(update, "⚠️ Failed to set age. Please try again.", context)
            return AGE
        context.user_data["state"] = GENDER
        await safe_reply(update, f"🎂 Age set to: *{age}*\\!", context, parse_mode=ParseMode.MARKDOWN_V2)
        keyboard = [
            [
                InlineKeyboardButton("👨 Male", callback_data="gender_male"),
                InlineKeyboardButton("👩 Female", callback_data="gender_female"),
                InlineKeyboardButton("🌈 Other", callback_data="gender_other")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await safe_reply(update, "👤 *Set Your Gender* 👤\n\nChoose your gender below:", context, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2)
        get_user_cached.cache_clear()
        return GENDER
    except ValueError:
        await safe_reply(update, "⚠️ Please enter a valid number for your age.", context)
        return AGE

async def set_gender(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    user = get_user_with_cache(user_id)
    profile = user.get("profile", {})
    if data.startswith("gender_"):
        gender = data.split("_")[1].capitalize()
        if gender not in ["Male", "Female", "Other"]:
            await safe_reply(update, "⚠️ Invalid gender selection.", context)
            return GENDER
        profile["gender"] = gender
        user_data = {
            "user_id": user_id,
            "profile": profile,
            "premium_expiry": user.get("premium_expiry"),
            "premium_features": user.get("premium_features", {}),
            "created_at": user.get("created_at", int(time.time())),
            "setup_state": LOCATION,
            "chat_id": user.get("chat_id")
        }
        success = update_user(user_id, user_data)
        if not success:
            logger.error(f"Failed to update gender for user {user_id}")
            await safe_reply(update, "⚠️ Failed to set gender. Please try again.", context)
            return GENDER
        context.user_data["state"] = LOCATION
        await safe_reply(update, f"👤 Gender set to: *{escape_markdown_v2(gender)}*\\!", context, parse_mode=ParseMode.MARKDOWN_V2)
        await safe_reply(update, "📍 Please enter your location \\(e.g., New York\\):", context, parse_mode=ParseMode.MARKDOWN_V2)
        get_user_cached.cache_clear()
        return LOCATION
    await safe_reply(update, "⚠️ Invalid selection. Please choose a gender.", context)
    return GENDER

async def set_location(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    user = get_user_with_cache(user_id)
    current_state = context.user_data.get("state", user.get("setup_state"))
    if current_state != LOCATION:
        await safe_reply(update, "⚠️ Please complete the previous steps. Use /start to begin.", context)
        return ConversationHandler.END
    profile = user.get("profile", {})
    location = update.message.text.strip()
    if not 1 <= len(location) <= 100:
        await safe_reply(update, "⚠️ Location must be 1-100 characters.", context)
        return LOCATION
    is_safe, reason = is_safe_message(location)
    if not is_safe:
        await safe_reply(update, f"⚠️ Location contains inappropriate content: {reason}", context)
        return LOCATION
    profile["location"] = location
    user_data = {
        "user_id": user_id,
        "profile": profile,
        "premium_expiry": user.get("premium_expiry"),
        "premium_features": user.get("premium_features", {}),
        "created_at": user.get("created_at", int(time.time())),
        "setup_state": None,
        "chat_id": user.get("chat_id")
    }
    success = update_user(user_id, user_data)
    if not success:
        logger.error(f"Failed to update location for user {user_id}")
        await safe_reply(update, "⚠️ Failed to set location. Please try again.", context)
        return LOCATION
    context.user_data["state"] = None
    await safe_reply(update, "🎉 Congratulations! Profile setup complete\\! 🎉", context, parse_mode=ParseMode.MARKDOWN_V2)
    await safe_reply(update, (
        "🔍 Your profile is ready! 🎉\n\n"
        "🚀 Use `/next` to find a chat partner and start connecting!\n"
        "ℹ️ Sending text messages now won’t start a chat. Use /help for more options."
    ), context, parse_mode=ParseMode.MARKDOWN_V2)
    notification_message = (
        "🆕 *New User Registered* 🆕\n\n"
        f"👤 *User ID*: {user_id}\n"
        f"🧑 *Name*: {escape_markdown_v2(profile.get('name', 'Not set'))}\n"
        f"🎂 *Age*: {profile.get('age', 'Not set')}\n"
        f"👤 *Gender*: {escape_markdown_v2(profile.get('gender', 'Not set'))}\n"
        f"📍 *Location*: {escape_markdown_v2(location)}\n"
        f"📅 *Joined*: {datetime.fromtimestamp(user.get('created_at', int(time.time()))).strftime('%Y-%m-%d %H:%M:%S')}"
    )
    await send_channel_notification(context, notification_message)
    get_user_cached.cache_clear()
    return ConversationHandler.END

async def match_users(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Match waiting users for chats."""
    with waiting_users_lock:
        if len(waiting_users) < 2:
            return
        valid_users = [u for u in waiting_users if not is_banned(u)]
        if len(valid_users) < 2:
            waiting_users[:] = valid_users
            return
        premium_users = [u for u in valid_users if has_premium_feature(u, "shine_profile")]
        regular_users = [u for u in valid_users if u not in premium_users]
        random.shuffle(premium_users)
        random.shuffle(regular_users)
        users_to_match = premium_users + regular_users
        i = 0
        while i < len(users_to_match) - 1:
            user1 = users_to_match[i]
            user2 = users_to_match[i + 1]
            if user1 not in waiting_users or user2 not in waiting_users:
                i += 1
                continue
            if not await can_match(user1, user2, context):  # Changed to await
                i += 2
                continue
            user1_data = get_user_with_cache(user1)
            user2_data = get_user_with_cache(user2)
            if user1_data is None or user2_data is None:
                logger.error(f"Failed to retrieve user data: user1={user1} ({user1_data}), user2={user2} ({user2_data})")
                waiting_users[:] = [u for u in waiting_users if u not in (user1, user2)]
                if user1_data is None:
                    await notify_user(user1, format_notification("Your profile data could not be retrieved. Please update it! 😓", action="update profile"), context)
                if user2_data is None:
                    await notify_user(user2, format_notification("Your profile data could not be retrieved. Please update it! 😓", action="update profile"), context)
                i += 2
                continue
            user1_chat_id = user1_data.get("chat_id")
            user2_chat_id = user2_data.get("chat_id")
            if not user1_chat_id or not user2_chat_id:
                logger.warning(f"Missing chat_id: user1={user1} (chat_id={user1_chat_id}), user2={user2} (chat_id={user2_chat_id})")
                waiting_users[:] = [u for u in waiting_users if u not in (user1, user2)]
                if not user1_chat_id:
                    await send_channel_notification(
                        context,
                        f"⚠️ Failed to notify user {user1}: No chat_id found 🌑"
                    )
                if not user2_chat_id:
                    await send_channel_notification(
                        context,
                        f"⚠️ Failed to notify user {user2}: No chat_id found 🌑"
                    )
                i += 2
                continue
            waiting_users.remove(user1)
            waiting_users.remove(user2)
            user_pairs[user1] = user2
            user_pairs[user2] = user1
            chat_key = tuple(sorted([user1, user2]))
            context.bot_data.setdefault("chat_start_times", {})[chat_key] = int(time.time())
            logger.info(f"Matched users {user1} and {user2}, started chat timer")
            user1_profile = user1_data.get("profile", {})
            user2_profile = user2_data.get("profile", {})
            user1_past = user1_profile.get("past_partners", [])
            user2_past = user2_profile.get("past_partners", [])
            if user2 not in user1_past:
                user1_past.append(user2)
            if user1 not in user2_past:
                user2_past.append(user1)
            user1_profile["past_partners"] = user1_past[-5:]
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
                "✅ *Connected\\!* Start chatting now\\! 🗣️\n\n"
                f"👤 *Partner Details*:\n"
                f"🧑 *Name*: {escape_markdown_v2(user2_profile.get('name', 'Not set'))}\n"
                f"🎂 *Age*: {escape_markdown_v2(str(user2_profile.get('age', 'Not set')))}\n"
                f"👤 *Gender*: {escape_markdown_v2(user2_profile.get('gender', 'Not set'))}\n"
                f"📍 *Location*: {escape_markdown_v2(user2_profile.get('location', 'Not set'))}\n\n"
                "Use /help for more options\\."
            ) if has_premium_feature(user1, "partner_details") else (
                "✅ *Connected\\!* Start chatting now\\! 🗣️\n\n"
                "🔒 *Partner Details*: Upgrade to *Premium* to view your partner’s profile\\!\n"
                "Unlock with /premium\\.\n\n"
                "Use /help for more options\\."
            )
            user2_message = (
                "✅ *Connected\\!* Start chatting now\\! 🗣️\n\n"
                f"👤 *Partner Details*:\n"
                f"🧑 *Name*: {escape_markdown_v2(user1_profile.get('name', 'Not set'))}\n"
                f"🎂 *Age*: {escape_markdown_v2(str(user1_profile.get('age', 'Not set')))}\n"
                f"👤 *Gender*: {escape_markdown_v2(user1_profile.get('gender', 'Not set'))}\n"
                f"📍 *Location*: {escape_markdown_v2(user1_profile.get('location', 'Not set'))}\n\n"
                "Use /help for more options\\."
            ) if has_premium_feature(user2, "partner_details") else (
                "✅ *Connected\\!* Start chatting now\\! 🗣️\n\n"
                "🔒 *Partner Details*: Upgrade to *Premium* to view your partner’s profile\\!\n"
                "Unlock with /premium\\.\n\n"
                "Use /help for more options\\."
            )
            try:
                await safe_bot_send_message(context.bot, user1_chat_id, user1_message, context, parse_mode=ParseMode.MARKDOWN_V2)
                await safe_bot_send_message(context.bot, user2_chat_id, user2_message, context, parse_mode=ParseMode.MARKDOWN_V2)
                if has_premium_feature(user1, "vaulted_chats"):
                    chat_histories[user1] = []
                if has_premium_feature(user2, "vaulted_chats"):
                    chat_histories[user2] = []
                return
            except Exception as e:
                logger.error(f"Failed to send match messages to user1={user1} or user2={user2}: {e}")
                user_pairs.pop(user1, None)
                user_pairs.pop(user2, None)
                context.bot_data["chat_start_times"].pop(chat_key, None)
                waiting_users.extend([user1, user2])
                await send_channel_notification(
                    context,
                    f"⚠️ Failed to send match messages to user1={user1} or user2={user2}: {escape_markdown_v2(str(e))} 🌑"
                )
                return
            i += 2

async def can_match(user1: int, user2: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check if two users can be matched based on their profiles, notifying on mismatches."""
    # Check bans
    if is_banned(user1) or is_banned(user2):
        logger.info(f"Cannot match {user1} and {user2}: one or both users are banned")
        message = format_notification("One or both users are banned. 😔")
        await notify_user(user1, message, context)
        await notify_user(user2, message, context)
        return False
    
    # Retrieve user data
    user1_data = get_user_with_cache(user1) or {}
    user2_data = get_user_with_cache(user2) or {}
    profile1 = user1_data.get("profile", {})
    profile2 = user2_data.get("profile", {})
    
    # Check for incomplete profiles
    if not profile1 or not profile2:
        logger.info(f"Cannot match {user1} and {user2}: incomplete profile data")
        message = format_notification("Profile data is incomplete for one or both users.", action="update profile")
        await notify_user(user1, message, context)
        await notify_user(user2, message, context)
        return True  # Allow match if profiles are incomplete (per original logic)
    
    # Check tags
    tags1 = set(profile1.get("tags", []))
    tags2 = set(profile2.get("tags", []))
    if tags1 and tags2 and not tags1.intersection(tags2):
        logger.info(f"Cannot match {user1} and {user2}: no common tags")
        message = format_notification("No common interests with this user. Try adding more tags! 🌟", action="update profile")
        await notify_user(user1, message, context)
        await notify_user(user2, message, context)
        return False
    
    # Check age difference
    age1 = profile1.get("age")
    age2 = profile2.get("age")
    if age1 and age2 and abs(age1 - age2) > 10:
        logger.info(f"Cannot match {user1} and {user2}: age difference too large ({abs(age1 - age2)} years)")
        message = format_notification(f"Age difference with this user is too large ({abs(age1 - age2)} years). 😅")
        await notify_user(user1, message, context)
        await notify_user(user2, message, context)
        return False
    
    # Check gender preferences
    gender_pref1 = profile1.get("gender_preference")
    gender_pref2 = profile2.get("gender_preference")
    gender1 = profile1.get("gender")
    gender2 = profile2.get("gender")
    if gender_pref1 and gender2 and gender_pref1 != gender2:
        logger.info(f"Cannot match {user1} and {user2}: gender preference mismatch for user1")
        message = format_notification("This user's gender does not match your preference. 🔍")
        await notify_user(user1, message, context)
        message = format_notification("Your gender does not match this user's preference. 🔍")
        await notify_user(user2, message, context)
        return False
    if gender_pref2 and gender1 and gender_pref2 != gender1:
        logger.info(f"Cannot match {user1} and {user2}: gender preference mismatch for user2")
        message = format_notification("Your gender does not match this user's preference. 🔍")
        await notify_user(user1, message, context)
        message = format_notification("This user's gender does not match your preference. 🔍")
        await notify_user(user2, message, context)
        return False
    
    # Check premium mood match
    if has_premium_feature(user1, "mood_match") and has_premium_feature(user2, "mood_match"):
        mood1 = profile1.get("mood")
        mood2 = profile2.get("mood")
        if not mood1 or not mood2:
            logger.info(f"Cannot match {user1} and {user2}: missing mood for mood_match")
            if not mood1:
                message = format_notification("Please set your mood to use the mood match feature. 😊", action="update profile")
                await notify_user(user1, message, context)
            if not mood2:
                message = format_notification("Please set your mood to use the mood match feature. 😊", action="update profile")
                await notify_user(user2, message, context)
            return False
        if mood1 != mood2:
            logger.info(f"Cannot match {user1} and {user2}: mood mismatch ({mood1} vs {mood2})")
            message = format_notification(f"Mood mismatch with this user (*{escape_markdown_v2(mood1)}* vs *{escape_markdown_v2(mood2)}*). 😕")
            await notify_user(user1, message, context)
            await notify_user(user2, message, context)
            return False
    
    return True

@restrict_access
async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user_with_cache(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        await safe_reply(update, ban_msg, context, parse_mode=ParseMode.MARKDOWN_V2)
        return
    
    user_data = get_user_with_cache(user_id)
    chat_id = user_data.get("chat_id") if user_data else None
    if not chat_id:
        await safe_reply(
            update,
            "⚠️ We couldn’t find your chat ID. Please use /start to initialize your profile and try again\\! 😊",
            context,
            parse_mode=ParseMode.MARKDOWN_V2
        )
        logger.warning(f"User {user_id} attempted /stop without a chat_id")
        admin_key = (user_id, "stop_no_chat_id")
        if admin_key not in admin_notification_cache:
            admin_notification_cache[admin_key] = True
            await send_channel_notification(
                context,
                f"⚠️ User {user_id} attempted /stop without a chat_id 🌑"
            )
        return

    if user_id in waiting_users:
        with waiting_users_lock:
            waiting_users.remove(user_id)
        await safe_reply(update, "⏹️ You’ve stopped waiting for a chat partner\\. Use /start to begin again\\.", context, parse_mode=ParseMode.MARKDOWN_V2)
        return
    
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        partner_data = get_user_with_cache(partner_id)
        partner_chat_id = partner_data.get("chat_id") if partner_data else None
        with user_pairs_lock:
            remove_pair(user_id, partner_id)
        if partner_chat_id:
            await safe_bot_send_message(
                context.bot,
                partner_chat_id,
                "👋 Your partner has left the chat\\. Use /start to find a new one\\.",
                context,
                parse_mode=ParseMode.MARKDOWN_V2
            )
        else:
            logger.warning(f"Cannot notify partner {partner_id}: No chat_id found")
            admin_key = (partner_id, "stop_partner_no_chat_id")
            if admin_key not in admin_notification_cache:
                admin_notification_cache[admin_key] = True
                await send_channel_notification(
                    context,
                    f"⚠️ Failed to notify partner {partner_id} of user {user_id} leaving chat: No chat_id found 🌑"
                )
        await safe_reply(update, "👋 Chat ended\\. Use /start to begin a new chat\\.", context, parse_mode=ParseMode.MARKDOWN_V2)
        if user_id in chat_histories and not has_premium_feature(user_id, "vaulted_chats"):
            with chat_histories_lock:
                del chat_histories[user_id]
    else:
        await safe_reply(update, "❓ You're not in a chat or waiting\\. Use /start to find a partner\\.", context, parse_mode=ParseMode.MARKDOWN_V2)

@restrict_access
async def next_chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    user_data = get_user_with_cache(user_id)
    chat_id = user_data.get("chat_id") if user_data else None
    if not chat_id:
        await safe_reply(
            update,
            "⚠️ We couldn’t find your chat ID\\. Please use /start to initialize your profile and try again\\! 😊",
            context,
            parse_mode=ParseMode.MARKDOWN_V2
        )
        logger.warning(f"User {user_id} attempted /next without a chat_id")
        admin_key = (user_id, "next_no_chat_id")
        if admin_key not in admin_notification_cache:
            admin_notification_cache[admin_key] = True
            await send_channel_notification(
                context,
                f"⚠️ User {user_id} attempted /next without a chat_id 🌑"
            )
        return
    
    if is_banned(user_id):
        user = get_user_with_cache(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        await safe_reply(update, ban_msg, context, parse_mode=ParseMode.MARKDOWN_V2)
        return
    
    if not check_rate_limit(user_id):
        await safe_reply(update, f"⏳ Please wait {COMMAND_COOLDOWN} seconds before trying again\\.", context, parse_mode=ParseMode.MARKDOWN_V2)
        return
    
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        remove_pair(user_id, partner_id)
        await safe_bot_send_message(
            context.bot,
            partner_id,
            "🔌 Your chat partner disconnected\\.",
            context,
            parse_mode=ParseMode.MARKDOWN_V2
        )
    
    if user_id not in waiting_users:
        if has_premium_feature(user_id, "shine_profile"):
            waiting_users.insert(0, user_id)
        else:
            waiting_users.append(user_id)
    
    await safe_reply(update, "🔍 Looking for a new chat partner\\.\\.\\.", context, parse_mode=ParseMode.MARKDOWN_V2)
    await match_users(context)

@restrict_access
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display the help menu with chat commands and options."""
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user_with_cache(user_id)
        ban_msg = (
            "🚫 You are permanently banned. Contact support to appeal."
            if user["ban_type"] == "permanent"
            else f"🚫 You are banned until {escape_markdown_v2(datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M'))}."
        )
        logger.debug(f"Prepared ban message for user {user_id}: {ban_msg}")
        await safe_reply(update, ban_msg, context, parse_mode=ParseMode.MARKDOWN)
        return
    keyboard = [
        [
            InlineKeyboardButton("💬 Start Chat", callback_data="start_chat"),
            InlineKeyboardButton("🔍 Next Partner", callback_data="next_chat"),
        ],
        [
            InlineKeyboardButton("👋 Stop Chat", callback_data="stop_chat"),
            InlineKeyboardButton("⚙️ Settings", callback_data="settings_menu"),
        ],
        [
            InlineKeyboardButton("🌟 Premium", callback_data="premium_menu"),
            InlineKeyboardButton("📜 History", callback_data="history_menu"),
        ],
        [
            InlineKeyboardButton("🚨 Report", callback_data="report_user"),
            InlineKeyboardButton("🔄 Re-Match", callback_data="rematch_partner"),
        ],
        [InlineKeyboardButton("🗑️ Delete Profile", callback_data="delete_profile")],
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
    logger.debug(f"Prepared help text for user {user_id}: {help_text[:200]}")
    await safe_reply(update, help_text, context, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)

@restrict_access
async def premium(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user_with_cache(user_id)
        ban_msg = (
            "🚫 You are permanently banned 🔒. Contact support to appeal 📧."
            if user["ban_type"] == "permanent"
            else f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')} ⏰."
        )
        await safe_reply(update, ban_msg, context)
        return
    keyboard = [
        [InlineKeyboardButton("1 Day - 100 Stars 🌟", callback_data="buy_premium_1day"),
         InlineKeyboardButton("7 Days - 250 Stars 🌟", callback_data="buy_premium_7days")],
        [InlineKeyboardButton("1 Month - 500 Stars 🌟", callback_data="buy_premium_1month"),
         InlineKeyboardButton("6 Months - 2500 Stars 🌟", callback_data="buy_premium_6months")],
        [InlineKeyboardButton("12 Months - 3000 Stars 🌟", callback_data="buy_premium_12months"),
         InlineKeyboardButton("🔙 Back to Help", callback_data="help_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    premium_text = (
        "💎 *Unlock Premium Features* 💎\n\n"
        "🌟 Elevate your Talk2Anyone experience with these exclusive features:\n"
        "➡️ *Flare Messages*: Add a sparkling effect to your messages ✨\n"
        "➡️ *Instant Rematch*: Reconnect instantly with past partners 🔄 (3-60 uses based on plan)\n"
        "➡️ *Shine Profile*: Stand out with a highlighted profile in matchmaking 🌟\n"
        "➡️ *Mood Match*: Find partners based on your current mood 😊\n"
        "➡️ *Partner Details*: View detailed profiles of your chat partners 👤\n"
        "➡️ *Vaulted Chats*: Save and review your chat history 📜\n"
        "➡️ *Ultra Premium Personal Chat*: Start private Telegram chats after 5 minutes (6-month/12-month plans only) 🔗\n\n"
        "📅 *Choose Your Plan*:\n"
        "• 1 Day: 100 Stars\n"
        "• 7 Days: 250 Stars\n"
        "• 1 Month: 500 Stars\n"
        "• 6 Months: 2500 Stars (Includes Ultra Premium)\n"
        "• 12 Months: 3000 Stars (Includes Ultra Premium)\n\n"
        "👇 Select a plan below to upgrade!"
    )
    message = await safe_reply(update, premium_text, context, reply_markup=reply_markup)
    try:
        await schedule_message_deletion(context, update.effective_chat.id, message.message_id, delay=60)
    except NameError:
        try:
            await asyncio.sleep(10)
            await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=message.message_id)
            logger.info(f"Deleted premium message {message.message_id} in chat {update.effective_chat.id} after timeout")
        except TelegramError as e:
            logger.error(f"Failed to delete premium message {message.message_id}: {e}")

async def buy_premium(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    
    if data == "buy_premium_1day":
        title = "Premium 1-Day Subscription"
        description = "Unlock instant rematch (3 uses), vaulted chats, partner details, and flare for 1 day."
        payload = "premium_1day"
        prices = [{"label": "Premium 1 Day", "amount": 100}]  # 100 Stars
    elif data == "buy_premium_7days":
        title = "Premium 7-Day Subscription"
        description = "Unlock instant rematch (5 uses), vaulted chats, partner details, and flare for 7 days."
        payload = "premium_7days"
        prices = [{"label": "Premium 7 Days", "amount": 250}]  # 250 Stars
    elif data == "buy_premium_1month":
        title = "Premium 1-Month Subscription"
        description = "Unlock instant rematch (10 uses), vaulted chats, partner details, and flare for 1 month."
        payload = "premium_1month"
        prices = [{"label": "Premium 1 Month", "amount": 500}]  # 500 Stars
    elif data == "buy_premium_6months":
        title = "Ultra Premium 6-Month Subscription"
        description = "Unlock all Premium features plus Personal Chat for 6 months."
        payload = "premium_6months"
        prices = [{"label": "Ultra Premium 6 Months", "amount": 2500}]  # 2500 Stars
    elif data == "buy_premium_12months":
        title = "Ultra Premium 12-Month Subscription"
        description = "Unlock all Premium features plus Personal Chat for 12 months."
        payload = "premium_12months"
        prices = [{"label": "Ultra Premium 12 Months", "amount": 3000}]  # 3000 Stars
    else:
        logger.warning(f"Unknown buy callback: {data} from user {user_id}")
        return
    
    try:
        await context.bot.send_invoice(
            chat_id=user_id,
            title=title,
            description=description,
            payload=payload,
            provider_token="",  # Empty for Telegram Stars
            currency="XTR",  # Telegram Stars currency
            prices=prices,
            start_parameter=f"premium-{user_id}"
        )
        await query.message.delete()
    except TelegramError as e:
        logger.error(f"Failed to send invoice to {user_id}: {e}")
        await safe_reply(update, "❌ Failed to process your purchase. Please try again later.", context)

async def pre_checkout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.pre_checkout_query
    if query.currency != "XTR":
        await context.bot.answer_pre_checkout_query(
            query.id, ok=False, error_message="Only Telegram Stars payments are supported."
        )
        return
    valid_payloads = ["premium_1day", "premium_7days", "premium_1month", "premium_6months", "premium_12months"]
    if query.invoice_payload in valid_payloads:
        await context.bot.answer_pre_checkout_query(query.id, ok=True)
    else:
        await context.bot.answer_pre_checkout_query(
            query.id, ok=False, error_message="Invalid purchase payload."
        )

async def successful_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    payment = update.message.successful_payment
    payload = payment.invoice_payload
    current_time = int(time.time())
    user = get_user_with_cache(user_id)
    features = user.get("premium_features", {})
    expiry_duration = {
        "premium_1day": 1 * 24 * 3600,
        "premium_7days": 7 * 24 * 3600,
        "premium_1month": 30 * 24 * 3600,
        "premium_6months": 180 * 24 * 3600,
        "premium_12months": 365 * 24 * 3600
    }
    rematch_counts = {
        "premium_1day": 3,
        "premium_7days": 5,
        "premium_1month": 10,
        "premium_6months": 30,
        "premium_12months": 60
    }
    plan_names = {
        "premium_1day": "1-Day Premium",
        "premium_7days": "7-Day Premium",
        "premium_1month": "1-Month Premium",
        "premium_6months": "6-Month Ultra Premium",
        "premium_12months": "12-Month Ultra Premium"
    }
    duration = expiry_duration.get(payload, 0)
    new_expiry = current_time + duration
    existing_expiry = user.get("premium_expiry", 0)
    expiry = max(existing_expiry, new_expiry) if existing_expiry > current_time else new_expiry
    instant_rematch_count = rematch_counts.get(payload, 0)
    plan_name = plan_names.get(payload, "Premium")
    features.update({
        "flare": expiry,
        "instant_rematch": expiry,
        "instant_rematch_count": features.get("instant_rematch_count", 0) + instant_rematch_count,
        "shine_profile": expiry,
        "mood_match": expiry,
        "partner_details": expiry,
        "vaulted_chats": expiry
    })
    if payload in ["premium_6months", "premium_12months"]:
        features["personal_chat"] = expiry
    update_user(user_id, {
        "premium_expiry": expiry,
        "premium_features": features,
        "profile": user.get("profile", {}),
        "consent": user.get("consent", False),
        "verified": user.get("verified", False),
        "ban_type": user.get("ban_type"),
        "ban_expiry": user.get("ban_expiry"),
        "created_at": user.get("created_at", current_time)
    })
    await safe_reply(update, f"🎉 Thank you for your purchase! You've unlocked {plan_name} features until {datetime.fromtimestamp(expiry).strftime('%Y-%m-%d')}!", context)
    if has_premium_feature(user_id, "partner_details"):
        rematch_requests = context.bot_data.get("rematch_requests", {}).get(user_id, {})
        requester_id = rematch_requests.get("requester_id")
        if requester_id and requester_id not in user_pairs:
            user_pairs[user_id] = requester_id
            user_pairs[requester_id] = user_id
            await safe_reply(update, "🔄 *Reconnected!* Start chatting! 🗣️", context)
            await safe_bot_send_message(context.bot, requester_id, "🔄 *Reconnected!* Start chatting! 🗣️", context)
            context.bot_data.get("rematch_requests", {}).pop(user_id, None)
            if has_premium_feature(user_id, "vaulted_chats"):
                chat_histories[user_id] = chat_histories.get(user_id, [])
            if has_premium_feature(requester_id, "vaulted_chats"):
                chat_histories[requester_id] = chat_histories.get(requester_id, [])
    notification_message = (
        f"🎉 *Premium Purchased* 🎉\n\n"
        f"👤 *User ID*: {user_id}\n"
        f"📅 *Plan*: {plan_name}\n"
        f"🕒 *Expiry*: {datetime.fromtimestamp(expiry).strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"🕒 *Purchased At*: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    await send_channel_notification(context, notification_message)
    
@restrict_access
async def shine(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user_with_cache(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        await safe_reply(update, ban_msg, context)
        return
    if not has_premium_feature(user_id, "shine_profile"):
        await safe_reply(update, "🌟 *Shine Profile* is a premium feature. Buy it with /premium!", context)
        return
    if user_id not in waiting_users and user_id not in user_pairs:
        waiting_users.insert(0, user_id)
        await safe_reply(update, "✨ Your profile is now shining! You’re first in line for matches!", context)
        await match_users(context)
    else:
        await safe_reply(update, "❓ You're already in a chat or waiting list.", context)

@restrict_access
async def instant(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user_with_cache(user_id)
        ban_msg = (
            "🚫 You are permanently banned 🔒. Contact support to appeal 📧."
            if user["ban_type"] == "permanent"
            else f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')} ⏰."
        )
        await safe_reply(update, ban_msg, context)
        return
    user = get_user_with_cache(user_id)
    if user_id in user_pairs:
        await safe_reply(update, "❓ You're already in a chat 😔. Use /stop to end it first.", context)
        return
    if not has_premium_feature(user_id, "instant_rematch"):
        await safe_reply(update, "❌ You need Premium to use Instant Rematch. Use /premium to upgrade!", context)
        return
    features = user.get("premium_features", {})
    instant_rematch_count = features.get("instant_rematch_count", 0)
    if instant_rematch_count <= 0:
        await safe_reply(update, "❌ You've used all your Instant Rematch attempts. Purchase another Premium plan to get more!", context)
        return
    partner_id = user.get("last_partner")
    if not partner_id:
        await safe_reply(update, "❌ You haven't chatted with anyone yet 😔.", context)
        return
    partner_data = get_user_with_cache(partner_id)
    if not partner_data:
        await safe_reply(update, "❌ This user is no longer available 😓.", context)
        return
    if partner_id in user_pairs:
        await safe_reply(update, "❌ This user is currently in another chat 💬.", context)
        return
    # Decrement instant_rematch_count
    features["instant_rematch_count"] = instant_rematch_count - 1
    update_user(user_id, {
        "premium_features": features,
        "premium_expiry": user.get("premium_expiry"),
        "profile": user.get("profile", {}),
        "consent": user.get("consent", False),
        "verified": user.get("verified", False),
        "ban_type": user.get("ban_type"),
        "ban_expiry": user.get("ban_expiry"),
        "created_at": user.get("created_at", int(time.time()))
    })
    user_pairs[user_id] = partner_id
    user_pairs[partner_id] = user_id
    chat_key = tuple(sorted([user_id, partner_id]))
    context.bot_data["chat_start_times"] = context.bot_data.get("chat_start_times", {})
    context.bot_data["chat_start_times"][chat_key] = int(time.time())
    await safe_reply(update, "🔄 *Reconnected!* Start chatting! 🗣️", context)
    await safe_bot_send_message(context.bot, partner_id, "🔄 *Reconnected!* Start chatting! 🗣️", context)
    if has_premium_feature(user_id, "vaulted_chats"):
        chat_histories[user_id] = chat_histories.get(user_id, [])
    if has_premium_feature(partner_id, "vaulted_chats"):
        chat_histories[partner_id] = chat_histories.get(partner_id, [])
    logger.info(f"User {user_id} used Instant Rematch with {partner_id}, remaining count: {features['instant_rematch_count']}")

@restrict_access
async def mood(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user_with_cache(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        await safe_reply(update, ban_msg, context)
        return
    if not has_premium_feature(user_id, "mood_match"):
        await safe_reply(update, "😊 *Mood Match* is a premium feature. Buy it with /premium!", context)
        return
    context.user_data["settings_state"] = "mood"
    keyboard = [
        [InlineKeyboardButton("😎 Chill", callback_data="mood_chill"),
         InlineKeyboardButton("🤔 Deep", callback_data="mood_deep")],
        [InlineKeyboardButton("😂 Fun", callback_data="mood_fun"),
         InlineKeyboardButton("❌ Clear Mood", callback_data="mood_clear")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await safe_reply(update, "🎭 Choose your chat mood:", context, reply_markup=reply_markup)
    logger.info(f"User {user_id} prompted to select mood")

@restrict_access
async def set_mood(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    if not has_premium_feature(user_id, "mood_match"):
        await safe_reply(update, "😊 *Mood Match* is a premium feature. Buy it with /premium!", context)
        return
    choice = query.data
    user = get_user_with_cache(user_id)
    profile = user.get("profile", {})
    if choice == "mood_clear":
        profile.pop("mood", None)
        await safe_reply(update, "❌ Mood cleared successfully.", context)
    else:
        mood = choice.split("_")[1]
        profile["mood"] = mood
        await safe_reply(update, f"🎭 Mood set to: *{mood.capitalize()}*!", context)
    if not update_user(user_id, {
        "profile": profile,
        "premium_expiry": user.get("premium_expiry"),
        "premium_features": user.get("premium_features", {}),
        "consent": user.get("consent", False),
        "verified": user.get("verified", False),
        "created_at": user.get("created_at", int(time.time()))
    }):
        await safe_reply(update, "❌ Error setting mood. Please try again.", context)

@restrict_access
async def vault(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user_with_cache(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        await safe_reply(update, ban_msg, context)
        return
    if not has_premium_feature(user_id, "vaulted_chats"):
        await safe_reply(update, "📜 *Vaulted Chats* is a premium feature. Buy it with /premium!", context)
        return
    if user_id not in user_pairs:
        await safe_reply(update, "❓ You're not in a chat. Use /start to begin.", context)
        return
    if user_id not in chat_histories:
        chat_histories[user_id] = []
    await safe_reply(update, "📜 Your current chat is being saved to the vault!", context)

@restrict_access
async def history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user_with_cache(user_id)
        ban_msg = "🚫 You are permanently banned 🔒. Contact support to appeal 📧." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')} ⏰."
        await safe_reply(update, ban_msg, context)
        return
    if not has_premium_feature(user_id, "vaulted_chats"):
        await safe_reply(update, "📜 *Chat History* is a premium feature. Buy it with /premium! 🌟", context)
        return
    if user_id not in chat_histories or not chat_histories[user_id]:
        await safe_reply(update, "📭 Your chat vault is empty 😔.", context)
        return
    history_text = "📜 *Your Chat History* 📜\n\n"
    for idx, msg in enumerate(chat_histories[user_id], 1):
        history_text += f"{idx}. {msg}\n"
    await safe_reply(update, history_text, context)

@restrict_access
async def rematch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user_with_cache(user_id)
        ban_msg = "🚫 You are permanently banned 🔒. Contact support to appeal 📧." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')} ⏰."
        await safe_reply(update, ban_msg, context)
        return
    if not check_rate_limit(user_id):
        await safe_reply(update, f"⏳ Please wait {COMMAND_COOLDOWN} seconds before trying again ⏰.", context)
        return
    if not has_premium_feature(user_id, "instant_rematch"):
        await safe_reply(update, "🔄 *Rematch* is a premium feature. Buy it with /premium! 🌟", context)
        return
    user = get_user_with_cache(user_id)
    partners = user.get("profile", {}).get("past_partners", [])
    if not partners:
        await safe_reply(update, "❌ No past partners to rematch with 😔.", context)
        return
    keyboard = []
    for partner_id in partners[-5:]:
        partner_data = get_user_with_cache(partner_id)
        if partner_data:
            partner_name = partner_data.get("profile", {}).get("name", "Anonymous")
            keyboard.append([InlineKeyboardButton(f"Reconnect with {partner_name}", callback_data=f"rematch_request_{partner_id}")])
    if not keyboard:
        await safe_reply(update, "❌ No available past partners to rematch with 😔.", context)
        return
    reply_markup = InlineKeyboardMarkup(keyboard)
    await safe_reply(update, "🔄 *Choose a Past Partner to Rematch* 🔄", context, reply_markup=reply_markup)

@restrict_access
async def flare(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user_with_cache(user_id)
        ban_msg = (
            "🚫 You are permanently banned 🔒. Contact support to appeal 📧."
            if user["ban_type"] == "permanent"
            else f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')} ⏰."
        )
        await safe_reply(update, ban_msg, context)
        return
    if not has_premium_feature(user_id, "flare"):
        await safe_reply(update, "❌ You need Premium to use Flare messages. Use /premium to upgrade!", context)
        return
    await safe_reply(update, "✨ Your messages now have Flare enabled! They'll appear with a special effect.", context)

async def personal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user_with_cache(user_id)
        ban_msg = (
            "🚫 You are permanently banned 🔒. Contact support to appeal 📧."
            if user["ban_type"] == "permanent"
            else f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')} ⏰."
        )
        await safe_reply(update, ban_msg, context)
        return
    if user_id not in user_pairs:
        await safe_reply(update, "❓ You're not in a chat 😔. Use /start to find a partner.", context)
        return
    partner_id = user_pairs[user_id]
    if partner_id not in user_pairs or user_pairs[partner_id] != user_id:
        await safe_reply(update, "❓ Your partner is no longer in the chat 😔.", context)
        return
    if not has_premium_feature(user_id, "personal_chat") or not has_premium_feature(partner_id, "personal_chat"):
        await safe_reply(update, "❌ Both you and your partner need Ultra Premium (6-month or 12-month plan) to use Personal Chat. Use /premium to upgrade!", context)
        return
    chat_key = tuple(sorted([user_id, partner_id]))
    start_time = context.bot_data.get("chat_start_times", {}).get(chat_key, 0)
    if int(time.time()) - start_time < 5 * 60:
        await safe_reply(update, "⏳ You need to chat for at least 5 minutes before starting a personal chat!", context)
        return
    if user_id in context.bot_data.get("personal_chat_requests", {}):
        await safe_reply(update, "⏳ You've already sent a personal chat request to this partner!", context)
        return
    keyboard = [
        [InlineKeyboardButton("✅ Accept", callback_data=f"personal_accept_{user_id}"),
         InlineKeyboardButton("❌ Decline", callback_data="personal_decline")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    user_profile = get_user_with_cache(user_id).get("profile", {})
    request_message = (
        f"🌟 *Personal Chat Request* 🌟\n\n"
        f"Your chat partner wants to continue chatting privately on Telegram!\n"
        f"🧑 *Name*: {user_profile.get('name', 'Anonymous')}\n"
        f"🎂 *Age*: {user_profile.get('age', 'Not set')}\n"
        f"👤 *Gender*: {user_profile.get('gender', 'Not set')}\n"
        f"📍 *Location*: {user_profile.get('location', 'Not set')}\n\n"
        f"Would you like to accept and share your Telegram chat link?"
    )
    try:
        message = await context.bot.send_message(
            chat_id=partner_id,
            text=escape_markdown_v2(request_message),
            parse_mode="MarkdownV2",
            reply_markup=reply_markup
        )
        await safe_reply(update, "📩 Personal chat request sent. Waiting for their response...", context)
        context.bot_data["personal_chat_requests"] = context.bot_data.get("personal_chat_requests", {})
        context.bot_data["personal_chat_requests"][partner_id] = {
            "requester_id": user_id,
            "timestamp": int(time.time()),
            "message_id": message.message_id
        }
        try:
            await schedule_message_deletion(context, partner_id, message.message_id, delay=60)
        except NameError:
            try:
                await asyncio.sleep(10)
                await context.bot.delete_message(chat_id=partner_id, message_id=message.message_id)
                logger.info(f"Deleted personal request message {message.message_id} in chat {partner_id} after timeout")
            except TelegramError as e:
                logger.error(f"Failed to delete personal request message {message.message_id}: {e}")
    except TelegramError as e:
        await safe_reply(update, "❌ Unable to send personal chat request. Your partner may be offline.", context)
        logger.error(f"Failed to send personal chat request from {user_id} to {partner_id}: {e}")

async def button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle button callbacks."""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data

    # Check ban status for all callbacks except consent and verification
    if not data.startswith(("consent_", "emoji_")):
        if is_banned(user_id):
            user = get_user_with_cache(user_id)
            ban_msg = (
                "🚫 You are permanently banned 🔒\\. Contact support to appeal 📧\\."
                if user["ban_type"] == "permanent"
                else f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')} ⏰\\."
            )
            await safe_reply(update, ban_msg, context, parse_mode=ParseMode.MARKDOWN_V2)
            return

    try:
        # Handle help menu
        if data == "help_menu":
            help_text = (
                "🆘 *Help Menu* 🆘\n\n"
                "Here’s how to use the bot:\n"
                "• /start \\- Set up or view your profile 🚀\n"
                "• /next \\- Find a new chat partner 🔍\n"
                "• /stop \\- End current chat or stop waiting 🛑\n"
                "• /settings \\- Update your profile ⚙️\n"
                "• /help \\- Show this menu 🆘\n"
                "• /botinfo \\- View bot information 🤖\n"
                "• /deleteprofile \\- Delete your profile 🗑️\n"
                "• /premium \\- Access premium features 💎\n"
                "• /shine \\- Highlight your profile ✨\n"
                "• /instant \\- Start an instant chat ⚡\n"
                "• /mood \\- Set your mood for matching 😊\n"
                "• /vault \\- Access vaulted chats 🔒\n"
                "• /history \\- View chat history 📜\n"
                "• /rematch \\- Reconnect with a past partner 🔄\n"
                "• /flare \\- Add flair to your messages 🌟\n"
                "• /personal \\- Start a personal chat 👤\n"
                "• /report \\- Report a user 🚨\n"
            )
            await safe_reply(update, help_text, context, parse_mode=ParseMode.MARKDOWN_V2)
            return

        # Handle personal chat requests
        if data.startswith("personal_accept_"):
            requester_id = int(data.split("_")[-1])
            if user_id not in user_pairs or user_pairs[user_id] != requester_id:
                await safe_reply(update, "❓ You are not in a chat with this user 😔\\.", context, parse_mode=ParseMode.MARKDOWN_V2)
                return
            user_features = get_user_with_cache(user_id).get("premium_features", {})
            if not user_features.get("premium_expiry", 0) > int(time.time()):
                await safe_reply(update, "❌ You need an active Premium subscription to accept personal chat\\.", context, parse_mode=ParseMode.MARKDOWN_V2)
                return
            # Delete the request message
            try:
                await query.message.delete()
                logger.info(f"Deleted personal chat request message {query.message.message_id} in chat {user_id}")
            except TelegramError as e:
                logger.error(f"Failed to delete personal chat request message {query.message.message_id}: {e}")
            # End the bot chat
            user_pairs.pop(user_id, None)
            user_pairs.pop(requester_id, None)
            chat_key = tuple(sorted([user_id, requester_id]))
            context.bot_data.get("chat_start_times", {}).pop(chat_key, None)
            # Open personal Telegram chat
            personal_message = (
                f"🌟 *Personal Chat Started* 🌟\n\n"
                f"Your bot chat has ended\\. You can now chat personally with your partner\\!\n"
                f"👤 Open their Telegram chat: [Click here](tg://user?id={requester_id})\n"
                f"Use /next to start a new chat in the bot\\."
            )
            await safe_reply(update, personal_message, context, parse_mode=ParseMode.MARKDOWN_V2)
            await safe_bot_send_message(context.bot, requester_id, (
                f"🌟 *Personal Chat Started* 🌟\n\n"
                f"Your bot chat has ended\\. You can now chat personally with your partner\\!\n"
                f"👤 Open their Telegram chat: [Click here](tg://user?id={user_id})\n"
                f"Use /next to start a new chat in the bot\\."
            ), context, parse_mode=ParseMode.MARKDOWN_V2)
            context.bot_data.get("personal_chat_requests", {}).pop(user_id, None)
            logger.info(f"Users {user_id} and {requester_id} started personal chat and ended bot chat")
            return
        elif data == "personal_decline":
            try:
                await query.message.delete()
                logger.info(f"Deleted personal chat request message {query.message.message_id} in chat {user_id}")
            except TelegramError as e:
                logger.error(f"Failed to delete personal chat request message {query.message.message_id}: {e}")
            await safe_reply(update, "❌ Personal chat request declined\\. You can continue chatting here\\.", context, parse_mode=ParseMode.MARKDOWN_V2)
            requester_data = context.bot_dataggggg.get("personal_chat_requests", {}).get(user_id, {})
            requester_id = requester_data.get("requester_id")
            if requester_id:
                await safe_bot_send_message(context.bot, requester_id, "❌ Your personal chat request was declined 😔\\. You can continue chatting here\\.", context, parse_mode=ParseMode.MARKDOWN_V2)
            context.bot_data.get("personal_chat_requests", {}).pop(user_id, None)
            return

        # Handle settings updates
        if data in ["set_name", "set_age", "set_gender", "set_location", "set_tags"]:
            user = get_user_with_cache(user_id)
            if not is_profile_complete(user):
                await safe_reply(update, "⚠️ Please complete your profile setup with /start before using this feature\\.", context, parse_mode=ParseMode.MARKDOWN_V2)
                return
            context.user_data["settings_state"] = data
            if data == "set_name":
                await safe_reply(update, "✨ Please enter your new name:", context)
            elif data == "set_age":
                await safe_reply(update, "🎂 Please enter your new age (e.g., 25):", context)
            elif data == "set_gender":
                keyboard = [
                    [InlineKeyboardButton("Male", callback_data="settings_gender_male"),
                     InlineKeyboardButton("Female", callback_data="settings_gender_female")],
                    [InlineKeyboardButton("Other", callback_data="settings_gender_other")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await safe_reply(update, "👤 Please select your gender:", context, reply_markup=reply_markup)
            elif data == "set_location":
                await safe_reply(update, "📍 Please enter your new location (e.g., New York):", context)
            elif data == "set_tags":
                await safe_reply(update, "🏷️ Please enter your tags, separated by commas (e.g., music, gaming):", context)
            return
        elif data.startswith("settings_gender_"):
            gender = data.split("_")[2].capitalize()
            update_user(user_id, {"profile.gender": gender})
            await safe_reply(update, f"👤 Gender updated to {gender}\\! Use /settings to make more changes\\.", context, parse_mode=ParseMode.MARKDOWN_V2)
            context.user_data.pop("settings_state", None)
            return

        # Handle rematch requests
        if data.startswith("rematch_request_"):
            partner_id = int(data.split("_")[-1])
            if is_banned(user_id):
                await safe_reply(update, "🚫 You are banned and cannot send rematch requests 🔒\\.", context, parse_mode=ParseMode.MARKDOWN_V2)
                return
            user = get_user_with_cache(user_id)
            if user_id in user_pairs:
                await safe_reply(update, "❓ You're already in a chat 😔\\. Use /stop to end it first\\.", context, parse_mode=ParseMode.MARKDOWN_V2)
                return
            partner_data = get_user_with_cache(partner_id)
            if not partner_data:
                await safe_reply(update, "❌ This user is no longer available 😓\\.", context, parse_mode=ParseMode.MARKDOWN_V2)
                return
            if partner_id in user_pairs:
                await safe_reply(update, "❌ This user is currently in another chat 💬\\.", context, parse_mode=ParseMode.MARKDOWN_V2)
                return
            keyboard = [
                [InlineKeyboardButton("✅ Accept", callback_data=f"rematch_accept_{user_id}"),
                 InlineKeyboardButton("❌ Decline", callback_data="rematch_decline")]
            ]
            if not has_premium_feature(partner_id, "partner_details"):
                keyboard.insert(0, [InlineKeyboardButton("💎 Upgrade to Premium", callback_data="premium_menu")])
                request_message = (
                    f"🔄 *Rematch Request* 🔄\n\n"
                    f"A user wants to reconnect with you\\!\n"
                    f"💎 Upgrade to Premium to view their profile details\\.\n\n"
                    f"You can accept to start chatting or decline the request\\."
                )
            else:
                user_profile = user.get("profile", {})
                request_message = (
                    f"🔄 *Rematch Request* 🔄\n\n"
                    f"A user wants to reconnect with you\\!\n"
                    f"🧑 *Name*: {escape_markdown_v2(user_profile.get('name', 'Anonymous'))}\n"
                    f"🎂 *Age*: {escape_markdown_v2(str(user_profile.get('age', 'Not set')))}\n"
                    f"👤 *Gender*: {escape_markdown_v2(user_profile.get('gender', 'Not set'))}\n"
                    f"📍 *Location*: {escape_markdown_v2(user_profile.get('location', 'Not set'))}\n\n"
                    f"Would you like to chat again\\?"
                )
            reply_markup = InlineKeyboardMarkup(keyboard)
            try:
                message = await context.bot.send_message(
                    chat_id=partner_id,
                    text=escape_markdown_v2(request_message),
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=reply_markup
                )
                await safe_reply(update, "📩 Rematch request sent\\. Waiting for their response\\.\\.\\.", context, parse_mode=ParseMode.MARKDOWN_V2)
                context.bot_data["rematch_requests"] = context.bot_data.get("rematch_requests", {})
                context.bot_data["rematch_requests"][partner_id] = {
                    "requester_id": user_id,
                    "timestamp": int(time.time()),
                    "message_id": message.message_id
                }
            except TelegramError as e:
                await safe_reply(update, "❌ Unable to reach this user\\. They may be offline\\.", context, parse_mode=ParseMode.MARKDOWN_V2)
            return
        elif data.startswith("rematch_accept_"):
            requester_id = int(data.split("_")[-1])
            if user_id in user_pairs:
                await safe_reply(update, "❓ You're already in a chat 😔\\. Use /stop to end it first\\.", context, parse_mode=ParseMode.MARKDOWN_V2)
                return
            requester_data = get_user_with_cache(requester_id)
            if not requester_data:
                await safe_reply(update, "❌ This user is no longer available 😓\\.", context, parse_mode=ParseMode.MARKDOWN_V2)
                return
            if requester_id in user_pairs:
                await safe_reply(update, "❌ This user is currently in another chat 💬\\.", context, parse_mode=ParseMode.MARKDOWN_V2)
                return
            user_pairs[user_id] = requester_id
            user_pairs[requester_id] = user_id
            await safe_reply(update, "🔄 *Reconnected\\!* Start chatting\\! 🗣️", context, parse_mode=ParseMode.MARKDOWN_V2)
            await safe_bot_send_message(context.bot, requester_id, "🔄 *Reconnected\\!* Start chatting\\! 🗣️", context, parse_mode=ParseMode.MARKDOWN_V2)
            if has_premium_feature(user_id, "vaulted_chats"):
                chat_histories[user_id] = chat_histories.get(user_id, [])
            if has_premium_feature(requester_id, "vaulted_chats"):
                chat_histories[requester_id] = chat_histories.get(requester_id, [])
            context.bot_data.get("rematch_requests", {}).pop(user_id, None)
            return
        elif data == "rematch_decline":
            await safe_reply(update, "❌ Rematch request declined\\.", context, parse_mode=ParseMode.MARKDOWN_V2)
            requester_data = context.bot_data.get("rematch_requests", {}).get(user_id, {})
            requester_id = requester_data.get("requester_id")
            if requester_id:
                await safe_bot_send_message(context.bot, requester_id, "❌ Your rematch request was declined 😔\\.", context, parse_mode=ParseMode.MARKDOWN_V2)
            context.bot_data.get("rematch_requests", {}).pop(user_id, None)
            return

        # Handle other commands
        if data == "start_chat":
            await start(update, context)
        elif data == "next_chat":
            await next_chat(update, context)
        elif data == "stop_chat":
            await stop(update, context)
        elif data == "settings_menu":
            await settings(update, context)
        elif data == "premium_menu":
            await premium(update, context)
        elif data == "history_menu":
            await history(update, context)
        elif data == "report_user":
            await report(update, context)
        elif data == "rematch_partner":
            await rematch(update, context)
        elif data == "delete_profile":
            await delete_profile(update, context)
        elif data.startswith("buy_"):
            await buy_premium(update, context)
        elif data.startswith("mood_"):
            await set_mood(update, context)
        elif data.startswith("emoji_"):
            await verify_emoji(update, context)
        elif data.startswith("consent_"):
            await consent_handler(update, context)
        elif data.startswith("gender_"):
            await set_gender(update, context)
        else:
            logger.warning(f"Unhandled callback data: {data} from user {user_id}")
            await query.answer("Unknown action\\. Please try again\\.", show_alert=True, parse_mode=ParseMode.MARKDOWN_V2)

    except Exception as e:
        logger.error(f"Error in button handler for user {user_id}, data {data}: {str(e)}")
        await query.answer("⚠️ An error occurred\\. Please try again later\\.", show_alert=True, parse_mode=ParseMode.MARKDOWN_V2)
        
@restrict_access
async def settings(update: Update, context: ContextTypes) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user_with_cache(user_id)
        ban_msg = "🚫 You are permanently banned 🔒. Contact support to appeal 📧." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')} ⏰."
        await safe_reply(update, ban_msg, context)
        return
    user = get_user_with_cache(user_id)
    profile = user.get("profile", {})
    keyboard = [
        [InlineKeyboardButton("🧑 Change Name", callback_data="set_name"),
         InlineKeyboardButton("🎂 Change Age", callback_data="set_age")],
        [InlineKeyboardButton("👤 Change Gender", callback_data="set_gender"),
         InlineKeyboardButton("📍 Change Location", callback_data="set_location")],
        [InlineKeyboardButton("🏷️ Set Tags", callback_data="set_tags"),
         InlineKeyboardButton("🔙 Back to Help", callback_data="help_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    settings_text = (
        "⚙️ *Settings Menu* ⚙️\n\n"
        "Customize your profile to enhance your chat experience:\n\n"
        f"🧑 *Name*: {profile.get('name', 'Not set')}\n"
        f"🎂 *Age*: {profile.get('age', 'Not set')}\n"
        f"👤 *Gender*: {profile.get('gender', 'Not set')}\n"
        f"📍 *Location*: {profile.get('location', 'Not set')}\n"
        f"🏷️ *Tags*: {', '.join(profile.get('tags', []) or ['None'])}\n\n"
        "Use the buttons below to update your profile! 👇"
    )
    await safe_reply(update, settings_text, context, reply_markup=reply_markup)

@restrict_access
async def settings_update_handler(update: Update, context: ContextTypes) -> None:
    user_id = update.effective_user.id
    message = update.message.text.strip()
    settings_state = context.user_data.get("settings_state")
    
    if not settings_state:
        return  # Let message_handler process the message
    
    # Apply restrict_access logic
    if is_banned(user_id):
        user = get_user_with_cache(user_id)
        ban_msg = (
            "🚫 You are permanently banned 🔒. Contact support to appeal 📧."
            if user["ban_type"] == "permanent"
            else f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')} ⏰."
        )
        await safe_reply(update, ban_msg, context)
        return
    user = get_user_with_cache(user_id)
    if not is_profile_complete(user):
        await safe_reply(update, "⚠️ Please complete your profile setup with /start before using this feature.", context)
        return
    
    profile = user.get("profile", {})
    
    try:
        if settings_state == "set_name":
            if len(message) > 50:
                await safe_reply(update, "❌ Name is too long. Please use 50 characters or fewer.", context)
                return
            profile["name"] = message
            await safe_reply(update, f"🧑 Name updated to {message}! Use /settings to make more changes.", context)
        
        elif settings_state == "set_age":
            age = int(message)
            if not 13 <= age <= 120:
                await safe_reply(update, "❌ Please enter a valid age between 13 and 120.", context)
                return
            profile["age"] = age
            await safe_reply(update, f"🎂 Age updated to {age}! Use /settings to make more changes.", context)
        
        elif settings_state == "set_location":
            if len(message) > 100:
                await safe_reply(update, "❌ Location is too long. Please use 100 characters or fewer.", context)
                return
            profile["location"] = message
            await safe_reply(update, f"📍 Location updated to {message}! Use /settings to make more changes.", context)
        
        elif settings_state == "set_tags":
            tags = [tag.strip() for tag in message.split(",") if tag.strip()]
            if len(tags) > 10:
                await safe_reply(update, "❌ Too many tags. Please use up to 10 tags.", context)
                return
            profile["tags"] = tags
            await safe_reply(update, f"🏷️ Tags updated to {', '.join(tags or ['None'])}! Use /settings to make more changes.", context)
        
        # Update the user’s profile in the database
        update_user(user_id, {"profile": profile})
        
        # Clear the settings state
        context.user_data.pop("settings_state", None)
    
    except ValueError:
        await safe_reply(update, "❌ Invalid input. Please try again (e.g., use a number for age).", context)
    except Exception as e:
        logger.error(f"Error updating settings for user {user_id}: {e}")
        await safe_reply(update, "❌ An error occurred. Please try again or contact support.", context)

@restrict_access
async def report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user_with_cache(user_id)
        ban_msg = "🚫 You are permanently banned 🔒. Contact support to appeal 📧." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')} ⏰."
        await safe_reply(update, ban_msg, context)
        return
    if user_id not in user_pairs:
        await safe_reply(update, "❓ You're not in a chat 😔. Use /start to begin.", context)
        return
    partner_id = user_pairs[user_id]
    try:
        reports = get_db_collection("reports")
        existing = reports.find_one({"reporter_id": user_id, "reported_id": partner_id})
        if existing:
            await safe_reply(update, "⚠️ You've already reported this user 😔.", context)
            return
        reports.insert_one({
            "reporter_id": user_id,
            "reported_id": partner_id,
            "timestamp": int(time.time())
        })
        report_count = reports.count_documents({"reported_id": partner_id})
        if report_count >= REPORT_THRESHOLD:
            ban_expiry = int(time.time()) + TEMP_BAN_DURATION
            update_user(partner_id, {
                "ban_type": "temporary",
                "ban_expiry": ban_expiry,
                "profile": get_user_with_cache(partner_id).get("profile", {}),
                "consent": get_user_with_cache(partner_id).get("consent", False),
                "verified": get_user_with_cache(partner_id).get("verified", False),
                "premium_expiry": get_user_with_cache(partner_id).get("premium_expiry"),
                "premium_features": get_user_with_cache(partner_id).get("premium_features", {}),
                "created_at": get_user_with_cache(partner_id).get("created_at", int(time.time()))
            })
            violations = get_db_collection("keyword_violations")
            violations.update_one(
                {"user_id": partner_id},
                {"$set": {
                    "ban_type": "temporary",
                    "ban_expiry": ban_expiry,
                    "last_violation": int(time.time()),
                    "count": 3,
                    "keyword": "reported"
                }},
                upsert=True
            )
            await safe_bot_send_message(context.bot, partner_id,
                "🚫 *Temporary Ban* 🚫\n"
                f"You've been banned for 24 hours due to multiple reports 📢. "
                "Contact support if you believe this is an error.", context)
            notification_message = (
                "🚨 *User Banned* 🚨\n\n"
                f"👤 *User ID*: {partner_id}\n"
                f"📅 *Ban Duration*: 24 hours\n"
                f"🕒 *Ban Expiry*: {datetime.fromtimestamp(ban_expiry).strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"📢 *Reason*: Multiple reports ({report_count})\n"
                f"🕒 *Reported At*: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            await send_channel_notification(context, notification_message)
            await stop(update, context)
        else:
            await safe_reply(update, "🚨 Report submitted. Thank you for keeping the community safe! 🌟", context)
            notification_message = (
                "🚨 *New Report Filed* 🚨\n\n"
                f"👤 *Reporter ID*: {user_id}\n"
                f"👤 *Reported ID*: {partner_id}\n"
                f"📢 *Total Reports*: {report_count}\n"
                f"🕒 *Reported At*: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            await send_channel_notification(context, notification_message)
    except Exception as e:
        await safe_reply(update, "❌ Error submitting report 😔. Please try again.", context)


@restrict_access
async def delete_profile(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    partner_id = None
    
    try:
        if user_id in user_pairs:
            partner_id = user_pairs[user_id]
            partner_data = get_user_with_cache(partner_id)
            partner_chat_id = partner_data.get("chat_id") if partner_data else None
        
        delete_user(user_id, context)
        
        if partner_id and partner_chat_id:
            await safe_bot_send_message(
                context.bot,
                partner_chat_id,
                "👋 Your chat partner has deleted their profile\\. Use /next to find a new partner\\.",
                context,
                parse_mode=ParseMode.MARKDOWN_V2
            )
        
        context.user_data.clear()
        await safe_reply(
            update,
            "🗑️ Your profile and data have been deleted successfully 🌟\\. Use /start to set up a new profile\\.",
            context,
            parse_mode=ParseMode.MARKDOWN_V2
        )
        notification_message = (
            "🗑️ *User Deleted Profile* 🗑️\n\n"
            f"👤 *User ID*: {user_id}\n"
            f"🕒 *Deleted At*: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        await send_channel_notification(context, notification_message)
        logger.info(f"User {user_id} deleted their profile")
        
    except Exception as e:
        logger.error(f"Error deleting profile for user {user_id}: {e}")
        await safe_reply(
            update,
            "❌ Error deleting profile 😔\\. Please try again or contact support\\.",
            context,
            parse_mode=ParseMode.MARKDOWN_V2
        )

async def message_handler(update: Update, context: ContextTypes) -> None:
    user_id = update.effective_user.id
    
    # Check if the user is updating settings
    if context.user_data.get("settings_state"):
        await settings_update_handler(update, context)
        return
    
    # Existing ban check
    if is_banned(user_id):
        user = get_user_with_cache(user_id)
        ban_msg = (
            "🚫 You are permanently banned 🔒. Contact support to appeal 📧."
            if user["ban_type"] == "permanent"
            else f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')} ⏰."
        )
        await safe_reply(update, ban_msg, context)
        return
    
    message = update.message.text.strip()
    is_safe, reason = is_safe_message(message)
    if not is_safe:
        await issue_keyword_violation(user_id, message, reason, context)
        await safe_reply(update, f"⚠️ Your message was flagged: {reason}. Please follow the rules.", context)
        return
    
    if not check_message_rate_limit(user_id):
        await safe_reply(update, f"⏳ You're sending messages too fast! Please wait a moment ⏰.", context)
        return
    
    if user_id not in user_pairs:
        await safe_reply(update, "❓ You're not in a chat 😔. Use /start to find a partner.", context)
        return
    
    partner_id = user_pairs[user_id]
    chat_key = tuple(sorted([user_id, partner_id]))
    
    # Set chat_start_times if not already set
    if chat_key not in context.bot_data.get("chat_start_times", {}):
        context.bot_data["chat_start_times"] = context.bot_data.get("chat_start_times", {})
        context.bot_data["chat_start_times"][chat_key] = int(time.time())
        logger.info(f"Started chat timer for {chat_key}")
    
    user_activities[user_id] = {"last_activity": time.time()}
    user_activities[partner_id] = {"last_activity": time.time()}
    formatted_message = message
    if has_premium_feature(user_id, "flare"):  # Updated to use "flare"
        formatted_message = f"✨ {message} ✨"
    await safe_bot_send_message(context.bot, partner_id, formatted_message, context)
    if user_id in chat_histories:
        chat_histories[user_id].append(f"You: {message}")
    if partner_id in chat_histories:
        chat_histories[partner_id].append(f"Partner: {message}")

def is_safe_message(message: str) -> tuple[bool, str]:
    message_lower = message.lower()
    for word in BANNED_WORDS:
        if word in message_lower:
            return False, f"Contains banned word: {word}"
    for pattern in BLOCKED_KEYWORDS:
        if re.search(pattern, message_lower, re.IGNORECASE):
            return False, "Contains inappropriate content"
    return True, ""

async def cleanup_rematch_requests(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Remove expired rematch requests from bot_data."""
    try:
        current_time = int(time.time())
        rematch_requests = context.bot_data.get("rematch_requests", {})
        expired = [partner_id for partner_id, data in rematch_requests.items()
                   if current_time - data["timestamp"] > 24 * 3600]  # 24 hours
        for partner_id in expired:
            rematch_requests.pop(partner_id, None)
            logger.info(f"Removed expired rematch request for partner {partner_id}")
        context.bot_data["rematch_requests"] = rematch_requests
    except Exception as e:
        logger.error(f"Error cleaning up rematch requests: {e}")

async def issue_keyword_violation(user_id: int, message: str, reason: str, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        violations = get_db_collection("keyword_violations")
        user = get_user_with_cache(user_id)
        violation_count = violations.count_documents({"user_id": user_id}) + 1
        ban_type = None
        ban_expiry = None
        if violation_count >= 3:
            ban_type = "temporary"
            ban_expiry = int(time.time()) + TEMP_BAN_DURATION
        elif violation_count >= 5:
            ban_type = "permanent"
            ban_expiry = None
        violations.insert_one({
            "user_id": user_id,
            "message": message,
            "reason": reason,
            "timestamp": int(time.time()),
            "count": violation_count
        })
        if ban_type:
            update_user(user_id, {
                "ban_type": ban_type,
                "ban_expiry": ban_expiry,
                "profile": user.get("profile", {}),
                "consent": user.get("consent", False),
                "verified": user.get("verified", False),
                "premium_expiry": user.get("premium_expiry"),
                "premium_features": user.get("premium_features", {}),
                "created_at": user.get("created_at", int(time.time()))
            })
            ban_message = (
                f"🚫 *{'Temporary' if ban_type == 'temporary' else 'Permanent'} Ban* 🚫\n"
                f"You've been banned{' for 24 hours' if ban_type == 'temporary' else ''} due to inappropriate content. "
                "Contact support if you believe this is an error."
            )
            await safe_bot_send_message(context.bot, user_id, ban_message, context)
            if user_id in user_pairs:
                partner_id = user_pairs[user_id]
                await safe_bot_send_message(context.bot, partner_id, "👋 Your partner has left the chat. Use /start to find a new one.", context)
                remove_pair(user_id, partner_id)
            if user_id in waiting_users:
                waiting_users.remove(user_id)
            notification_message = (
                f"🚨 *Keyword Violation* 🚨\n\n"
                f"👤 *User ID*: {user_id}\n"
                f"📜 *Message*: {message[:100]}\n"
                f"⚠️ *Reason*: {reason}\n"
                f"📢 *Violation Count*: {violation_count}\n"
                f"📅 *Ban Type*: {ban_type.capitalize() if ban_type else 'None'}\n"
                f"🕒 *Ban Expiry*: {'No expiry' if ban_type == 'permanent' else datetime.fromtimestamp(ban_expiry).strftime('%Y-%m-%d %H:%M:%S') if ban_expiry else 'N/A'}\n"
                f"🕒 *Detected At*: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            await send_channel_notification(context, notification_message)
    except Exception as e:
        logger.error(f"Failed to issue keyword violation for user {user_id}: {e}")
        operation_queue.put(("issue_keyword_violation", (user_id, message, reason, context)))

def is_admin(user_id: int) -> bool:
    """Check if a user is in the admin list."""
    return user_id in ADMIN_IDS

def is_banned(user_id: int) -> bool:
    user = get_user_with_cache(user_id)
    ban_type = user.get("ban_type")
    ban_expiry = user.get("ban_expiry")
    if ban_type == "permanent":
        return True
    if ban_type == "temporary" and ban_expiry and ban_expiry > int(time.time()):
        return True
    if ban_type and ban_expiry and ban_expiry <= int(time.time()):
        update_user(user_id, {
            "ban_type": None,
            "ban_expiry": None,
            "profile": user.get("profile", {}),
            "consent": user.get("consent", False),
            "verified": user.get("verified", False),
            "premium_expiry": user.get("premium_expiry"),
            "premium_features": user.get("premium_features", {}),
            "created_at": user.get("created_at", int(time.time()))
        })
    return False

def check_rate_limit(user_id: int) -> bool:
    current_time = time.time()
    last_command = command_timestamps.get(user_id, 0)
    if current_time - last_command < COMMAND_COOLDOWN:
        return False
    command_timestamps[user_id] = current_time
    return True

def check_message_rate_limit(user_id: int) -> bool:
    current_time = time.time()
    message_timestamps[user_id].append(current_time)
    message_timestamps[user_id] = [t for t in message_timestamps[user_id] if current_time - t < 60]
    return len(message_timestamps[user_id]) <= MAX_MESSAGES_PER_MINUTE

def has_premium_feature(user_id: int, feature: str) -> bool:
    """Check if a user has an active premium feature or is an admin."""
    if is_admin(user_id):
        return True
    user = get_user_with_cache(user_id)
    if not user:
        return False
    features = user.get("premium_features", {})
    current_time = int(time.time())
    # Clean up expired features
    expired_features = [k for k, v in features.items() if isinstance(v, int) and v <= current_time]
    for k in expired_features:
        del features[k]
    if expired_features:
        update_user(user_id, {
            "premium_features": features,
            "premium_expiry": user.get("premium_expiry"),
            "profile": user.get("profile", {}),
            "consent": user.get("consent", False),
            "verified": user.get("verified", False),
            "ban_type": user.get("ban_type"),
            "ban_expiry": user.get("ban_expiry"),
            "created_at": user.get("created_at", current_time)
        })
        logger.info(f"Cleaned up expired features for user {user_id}: {expired_features}")
    expiry = features.get(feature, 0)
    return expiry > current_time

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle errors during update processing."""
    logger.error(f"Update {update} caused error: {context.error}")
    # Reply to user if applicable
    if update and (update.message or update.callback_query):
        await safe_reply(update, "❌ An error occurred 😔. Please try again or contact support.", context)
    # Prepare notification message with escaped error
    error_str = str(context.error)[:100]  # Truncate to avoid length issues
    notification_message = (
        f"⚠️ *Bot Error* ⚠️\n\n"
        f"📜 *Error*: {escape_markdown_v2(error_str)}\n"
        f"🕒 *Occurred At*: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    await send_channel_notification(context, notification_message)

async def cleanup_personal_chat_requests(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Remove expired personal chat requests from bot_data."""
    try:
        current_time = int(time.time())
        personal_chat_requests = context.bot_data.get("personal_chat_requests", {})
        expired = [partner_id for partner_id, data in personal_chat_requests.items()
                   if current_time - data["timestamp"] > 24 * 3600]  # 24 hours
        for partner_id in expired:
            requester_id = personal_chat_requests[partner_id]["requester_id"]
            await safe_bot_send_message(context.bot, requester_id, "⏰ Your personal chat request expired without a response.", context)
            personal_chat_requests.pop(partner_id, None)
            logger.info(f"Removed expired personal chat request for partner {partner_id}")
        context.bot_data["personal_chat_requests"] = personal_chat_requests
    except Exception as e:
        logger.error(f"Error cleaning up personal chat requests: {e}")

async def schedule_message_deletion(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, delay: int = 10) -> None:
    """Schedule a message to be deleted after a delay, storing the job for cancellation."""
    async def delete_message(context: ContextTypes.DEFAULT_TYPE):
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            logger.info(f"Deleted message {message_id} in chat {chat_id} after timeout")
        except TelegramError as e:
            logger.error(f"Failed to delete message {message_id} in chat {chat_id}: {e}")
    job_id = f"delete_{chat_id}_{message_id}"
    context.bot_data["message_deletion_jobs"] = context.bot_data.get("message_deletion_jobs", {})
    job = context.job_queue.run_once(delete_message, delay, data={"chat_id": chat_id, "message_id": message_id}, name=job_id)
    context.bot_data["message_deletion_jobs"][job_id] = job

async def set_tags(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    user = get_user_with_cache(user_id)
    current_state = context.user_data.get("state", user.get("setup_state"))
    
    if current_state != TAGS:
        await safe_reply(update, "⚠️ Please complete the previous steps. Use /start to begin.", context, parse_mode=ParseMode.MARKDOWN_V2)
        return ConversationHandler.END
    
    profile = user.get("profile", {})
    tags_input = update.message.text.strip().lower().split(",")
    tags_input = [tag.strip() for tag in tags_input if tag.strip()]
    
    ALLOWED_TAGS = ["gaming", "music", "movies", "sports", "books", "tech", "food", "travel", "art", "fitness", "nature", "photography", "fashion", "coding", "anime"]
    invalid_tags = [tag for tag in tags_input if tag not in ALLOWED_TAGS]
    if invalid_tags:
        await safe_reply(
            update,
            f"⚠️ Invalid tags: {', '.join(invalid_tags)}. Allowed tags: {', '.join(ALLOWED_TAGS)}",
            context,
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return TAGS
    
    if len(tags_input) > 5:
        await safe_reply(update, "⚠️ You can only set up to 5 tags.", context, parse_mode=ParseMode.MARKDOWN_V2)
        return TAGS
    
    profile["tags"] = tags_input
    updates = {
        "profile": profile,
        "consent": user.get("consent", False),
        "verified": user.get("verified", False),
        "premium_expiry": user.get("premium_expiry"),
        "premium_features": user.get("premium_features", {}),
        "created_at": user.get("created_at", int(time.time())),
        "ban_type": user.get("ban_type"),
        "ban_expiry": user.get("ban_expiry"),
        "setup_state": None
    }
    
    # Update user and clear cache
    try:
        update_user(user_id, updates)
        get_user_cached.cache_clear()
        logger.info(f"Completed setup for user {user_id} with tags {tags_input}")
    except Exception as e:
        logger.error(f"Error completing setup for user {user_id}: {e}")
        await safe_reply(update, "❌ Error saving your profile. Please try again or contact support.", context, parse_mode=ParseMode.MARKDOWN_V2)
        return ConversationHandler.END
    
    # Verify user document
    user = get_db_collection("users").find_one({"user_id": user_id})
    if not (user.get("consent") and user.get("verified") and is_profile_complete(user)):
        logger.error(f"User {user_id} profile incomplete after setup: {user}")
        await safe_reply(update, "❌ Profile setup failed. Please use /start to try again.", context, parse_mode=ParseMode.MARKDOWN_V2)
        return ConversationHandler.END
    
    context.user_data["state"] = None
    await safe_reply(update, f"🏷️ Tags set: {', '.join(tags_input)} 🎉", context, parse_mode=ParseMode.MARKDOWN_V2)
    await safe_reply(
        update,
        (
            "🔍 Your profile is ready! 🎉\n\n"
            "🚀 Use /next to find a chat partner and start connecting!\n"
            "ℹ️ Sending text messages now won’t start a chat. Use /help for more options."
        ),
        context,
        parse_mode=ParseMode.MARKDOWN_V2
    )
    
    notification_message = (
        "🆕 *User Updated Tags* 🆕\n\n"
        f"👤 *User ID*: {user_id}\n"
        f"🧑 *Name*: {profile.get('name', 'Not set')}\n"
        f"🏷️ *Tags*: {', '.join(tags_input)}\n"
        f"🕒 *Updated At*: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    await send_channel_notification(context, notification_message)
    
    return ConversationHandler.END

async def botinfo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display information about the bot."""
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user_with_cache(user_id)
        ban_msg = (
            "🚫 You are permanently banned 🔒\\. Contact support to appeal 📧\\."
            if user["ban_type"] == "permanent"
            else f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M:%S')} ⏰\\."
        )
        await safe_reply(update, ban_msg, context, parse_mode=ParseMode.MARKDOWN_V2)
        return

    try:
        info_message = (
            f"🤖 *{escape_markdown_v2(BOT_INFO['name'])} Info* 🤖\n\n"
            f"🔧 *Tools Used*: {escape_markdown_v2(BOT_INFO['tools'])}\n\n"
            f"👨‍💻 *Created By*: {escape_markdown_v2(BOT_INFO['created_by'])}\n\n"
            f"💻 *Language*: {escape_markdown_v2(BOT_INFO['language'])}\n\n"
            f"👤 *Owner*: {escape_markdown_v2(BOT_INFO['owner'])}\n\n"
            f"📌 *Version*: {escape_markdown_v2(BOT_INFO['version'])}\n\n"
            f"📞 *Help & Support*: {escape_markdown_v2(BOT_INFO['support'])}\n"
        )
        await safe_reply(update, info_message, context, parse_mode=ParseMode.MARKDOWN_V2)
        logger.info(f"Sent /botinfo response to user {user_id}")
    except Exception as e:
        logger.error(f"Failed to send /botinfo response to user {user_id}: {str(e)}")
        await safe_reply(update, "⚠️ Sorry, an error occurred while displaying bot info\\. Please try again later\\.", context, parse_mode=ParseMode.MARKDOWN_V2)

async def admin_access(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Grant admin access and display commands"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await safe_reply(update, "🔒 Unauthorized 🌑.", context)
        logger.info(f"Unauthorized access attempt by user_id={user_id}")
        return
    access_text = (
        "🌟 *Admin Commands* 🌟\n\n"
        "🚀 *User Management*\n"
        "• /admin_userslist - List all users 📋\n"
        "• /admin_premiumuserslist - List premium users 💎\n"
        "• /admin_info <user_id> - View user details 🕵️\n"
        "• /admin_delete <user_id> - Delete a user’s data 🗑️\n"
        "• /admin_premium <user_id> <days> - Grant premium status 🎁\n"
        "• /admin_revoke_premium <user_id> - Revoke premium status ❌\n"
        "━━━━━━━━━━━━━━\n\n"
        "🛡️ *Ban Management*\n"
        "• /admin_ban <user_id> <days/permanent> - Ban a user 🚫\n"
        "• /admin_unban <user_id> - Unban a user 🔓\n"
        "• /admin_violations - List recent keyword violations ⚠️\n"
        "━━━━━━━━━━━━━━\n\n"
        "📊 *Reports & Stats*\n"
        "• /admin_reports - List reported users 🚨\n"
        "• /admin_clear_reports <user_id> - Clear reports 🧹\n"
        "• /admin_stats - View bot statistics 📈\n"
        "━━━━━━━━━━━━━━\n\n"
        "📢 *Broadcast*\n"
        "• /admin_broadcast <message> - Send message to all users 📣\n"
    )
    logger.debug(f"Prepared admin_access text: {access_text}")
    await safe_reply(update, access_text, context)  # No explicit parse_mode

async def admin_delete(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await safe_reply(update, "🔒 Unauthorized 🌑.", context)
        return
    try:
        target_id = int(context.args[0])
        delete_user(target_id, context)
        await safe_reply(update, f"🗑️ User *{target_id}* data deleted successfully 🌟.", context)
        logger.info(f"Admin {user_id} deleted user {target_id}.")
    except (IndexError, ValueError):
        await safe_reply(update, "⚠️ Usage: /admin_delete <user_id> 📋.", context)

async def admin_premium(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Grant premium status to a user"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await safe_reply(update, "🔒 Unauthorized 🌑.", context)
        return
    try:
        target_id = int(context.args[0])
        days = int(context.args[1])
        if days <= 0:
            raise ValueError("Days must be positive")
        expiry = int(time.time()) + days * 24 * 3600
        logger.debug(f"Calculated premium_expiry for user {target_id}: {expiry} ({datetime.fromtimestamp(expiry).strftime('%Y-%m-%d %H:%M:%S')})")
        user = get_user_with_cache(target_id)
        if not user:
            await safe_reply(update, "😕 User not found 🌑.", context)
            return
        logger.debug(f"User {target_id} before admin_premium: premium_expiry={user.get('premium_expiry')}, premium_features={user.get('premium_features')}")
        features = user.get("premium_features", {})
        instant_rematch_count = {1: 3, 7: 5, 30: 10, 180: 30, 365: 60}.get(min(days, 365), 10)
        features.update({
            "flare_messages": expiry,
            "instant_rematch": expiry,
            "instant_rematch_count": features.get("instant_rematch_count", 0) + instant_rematch_count,
            "shine_profile": expiry,
            "mood_match": expiry,
            "partner_details": expiry,
            "vaulted_chats": expiry,
            "flare": expiry
        })
        if days >= 180:
            features["personal_chat"] = expiry
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
        updated_user = get_user_with_cache(target_id)
        expiry_date = datetime.fromtimestamp(updated_user.get('premium_expiry', 0)).strftime('%Y-%m-%d %H:%M:%S') if updated_user.get('premium_expiry') else 'None'
        logger.debug(f"User {target_id} after admin_premium: premium_expiry={updated_user.get('premium_expiry')} ({expiry_date}), premium_features={updated_user.get('premium_features')}")
        await safe_reply(update, f"🎁 Premium granted to user *{target_id}* for *{days}* days 🌟.", context)
        await safe_bot_send_message(context.bot, target_id, f"🎉 You've been granted Premium status for {days} days!", context)
        logger.info(f"Admin {user_id} granted premium to {target_id} for {days} days.")
    except (IndexError, ValueError) as e:
        logger.error(f"Error in admin_premium for user {target_id}: {e}")
        await safe_reply(update, "⚠️ Usage: /admin_premium <user_id> <days> 📋.", context)

async def admin_revoke_premium(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Revoke premium status from a user"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await safe_reply(update, "🔒 Unauthorized 🌑.", context)
        return
    try:
        target_id = int(context.args[0])
        user = get_user_with_cache(target_id)
        if not user:
            await safe_reply(update, "😕 User not found 🌑.", context)
            return
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
        await safe_reply(update, f"❌ Premium status revoked for user *{target_id}* 🌑.", context)
        await safe_bot_send_message(context.bot, target_id, "😔 Your Premium status has been revoked.", context)
        updated_user = get_user_with_cache(target_id)
        logger.info(f"Admin {user_id} revoked premium for {target_id}.")
        logger.debug(f"User {target_id} after revoke_premium: premium_expiry={updated_user.get('premium_expiry')}, premium_features={updated_user.get('premium_features')}")
    except (IndexError, ValueError):
        await safe_reply(update, "⚠️ Usage: /admin_revoke_premium <user_id> 📋.", context)

async def admin_ban(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Ban a user"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await safe_reply(update, "🔒 Unauthorized 🌑.", context)
        return
    try:
        target_id = int(context.args[0])
        ban_type = context.args[1].lower()
        user = get_user_with_cache(target_id)
        if not user:
            await safe_reply(update, "😕 User not found 🌑.", context)
            return
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
        violations = get_db_collection("keyword_violations")
        violations.update_one(
            {"user_id": target_id},
            {"$set": {
                "count": 5 if ban_type == "permanent" else 3,
                "keyword": "admin_ban",
                "last_violation": int(time.time()),
                "ban_type": ban_type,
                "ban_expiry": ban_expiry
            }},
            upsert=True
        )
        if target_id in user_pairs:
            partner_id = user_pairs[target_id]
            del user_pairs[target_id]
            if partner_id in user_pairs:
                del user_pairs[partner_id]
            await safe_bot_send_message(context.bot, partner_id, "😔 Your partner has left the chat.", context)
        if target_id in waiting_users:
            waiting_users.remove(target_id)
        await safe_reply(update, f"🚫 User *{target_id}* has been {ban_type} banned 🌑.", context)
        await safe_bot_send_message(context.bot, target_id, f"🚫 You have been {ban_type} banned from Talk2Anyone.", context)
        updated_user = get_user_with_cache(target_id)
        logger.info(f"Admin {user_id} banned user {target_id} ({ban_type}).")
        logger.debug(f"User {target_id} after admin_ban: premium_expiry={updated_user.get('premium_expiry')}, premium_features={updated_user.get('premium_features')}")
    except (IndexError, ValueError):
        await safe_reply(update, "⚠️ Usage: /admin_ban <user_id> <days/permanent> 📋.", context)

async def admin_unban(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Unban a user"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await safe_reply(update, "🔒 Unauthorized 🌑.", context)
        return
    try:
        target_id = int(context.args[0])
        user = get_user_with_cache(target_id)
        if not user:
            await safe_reply(update, "😕 User not found 🌑.", context)
            return
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
        violations = get_db_collection("keyword_violations")
        violations.delete_one({"user_id": target_id})
        await safe_reply(update, f"🔓 User *{target_id}* has been unbanned 🌟.", context)
        await safe_bot_send_message(context.bot, target_id, "🎉 You have been unbanned. Use /start to begin.", context)
        updated_user = get_user_with_cache(target_id)
        logger.info(f"Admin {user_id} unbanned user {target_id}.")
        logger.debug(f"User {target_id} after admin_unban: premium_expiry={updated_user.get('premium_expiry')}, premium_features={updated_user.get('premium_features')}")
    except (IndexError, ValueError):
        await safe_reply(update, "⚠️ Usage: /admin_unban <user_id> 📋.", context)

async def admin_violations(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """List recent keyword violations"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await safe_reply(update, "🔒 Unauthorized 🌑.", context)
        return
    try:
        violations = get_db_collection("keyword_violations")
        cursor = violations.find().sort("last_violation", -1).limit(10)
        violations_list = list(cursor)
        if not violations_list:
            await safe_reply(update, "✅ No recent keyword violations 🌟.", context)
            return
        violation_text = "⚠️ *Recent Keyword Violations* ⚠️\n\n"
        for v in violations_list:
            user_id = v["user_id"]
            count = v.get("count", 0)
            keyword = escape_markdown_v2(v.get("keyword", "N/A"))
            last_violation = datetime.fromtimestamp(v["last_violation"]).strftime('%Y-%m-%d %H:%M') if v.get("last_violation") else "Unknown"
            ban_type = v.get("ban_type")
            ban_expiry = v.get("ban_expiry")
            ban_status = (
                "Permanent 🔒" if ban_type == "permanent" else
                f"Temporary until {datetime.fromtimestamp(ban_expiry).strftime('%Y-%m-%d %H:%M')} ⏰"
                if ban_type == "temporary" and ban_expiry else "None ✅"
            )
            violation_text += (
                f"👤 User ID: *{user_id}*\n"
                f"📉 Violations: *{count}*\n"
                f"🔍 Keyword: *{keyword}*\n"
                f"🕒 Last: *{last_violation}*\n"
                f"🚫 Ban: *{ban_status}*\n"
                "━━━━━━━━━━━━━━\n"
            )
        logger.debug(f"Prepared admin_violations text: {violation_text}")
        await safe_reply(update, violation_text, context)
    except Exception as e:
        logger.error(f"Error fetching violations: {e}")
        await safe_reply(update, "😔 Error fetching violations 🌑.", context)

async def admin_userslist(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """List all users for authorized admins"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await safe_reply(update, "🔒 Unauthorized 🌑.", context)
        return
    try:
        users = get_db_collection("users")
        users_list = list(users.find().sort("user_id", 1))
        logger.debug(f"Raw database users: {len(users_list)} users")
        if not users_list:
            await safe_reply(update, "😕 No users found 🌑.", context)
            logger.info(f"Admin {user_id} requested users list: no users found.")
            return
        message = "📋 *All Users List* (Sorted by ID) 📋\n\n"
        user_count = 0
        for user in users_list:
            user_id = user["user_id"]
            profile = user.get("profile", {})
            premium_features = user.get("premium_features", {})
            created_at = user.get("created_at", int(time.time()))
            created_date = (
                datetime.fromtimestamp(created_at).strftime("%Y-%m-%d")
                if created_at and isinstance(created_at, (int, float))
                else "Unknown"
            )
            has_active_features = any(
                v is True or (isinstance(v, int) and v > time.time())
                for k, v in premium_features.items()
                if k != "instant_rematch_count"
            )
            premium_status = (
                "Premium 💎" if (user.get("premium_expiry") and user["premium_expiry"] > time.time()) or has_active_features else "Not Premium 🌑"
            )
            ban_status = user.get("ban_type", "None")
            verified_status = "Yes ✅" if user.get("verified", False) else "No ❌"
            name = escape_markdown_v2(profile.get("name", "Not set"))
            created_date = escape_markdown_v2(created_date)
            premium_status = escape_markdown_v2(premium_status)
            ban_status = escape_markdown_v2(ban_status)
            verified_status = escape_markdown_v2(verified_status)
            message += (
                f"👤 *User ID*: {user_id}\n"
                f"🧑 *Name*: {name}\n"
                f"📅 *Created*: {created_date}\n"
                f"💎 *Premium*: {premium_status}\n"
                f"🚫 *Ban*: {ban_status}\n"
                f"✅ *Verified*: {verified_status}\n"
                "━━━━━━━━━━━━━━\n"
            )
            user_count += 1
            if len(message.encode('utf-8')) > 3500:
                await safe_reply(update, message, context, parse_mode=ParseMode.MARKDOWN)
                message = ""
                logger.debug(f"Sent partial users list for admin {user_id}, users so far: {user_count}")
        if message.strip():
            message += f"📊 *Total Users*: {user_count}\n"
        logger.debug(f"Prepared admin_userslist text: {message}")
        await safe_reply(update, message, context, parse_mode=ParseMode.MARKDOWN)
        logger.info(f"Admin {user_id} requested users list with {user_count} users")
    except Exception as e:
        logger.error(f"Error fetching users list for admin {user_id}: {e}", exc_info=True)
        await safe_reply(update, "😔 Error retrieving users list 🌑.", context)

async def admin_premiumuserslist(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """List premium users"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await safe_reply(update, "🔒 Unauthorized 🌑.", context)
        return
    try:
        current_time = int(time.time())
        users = get_db_collection("users")
        premium_users = list(users.find({"premium_expiry": {"$gt": current_time}}).sort("premium_expiry", -1))
        if not premium_users:
            await safe_reply(update, "😕 No premium users found 🌑.", context)
            return
        message = "💎 *Premium Users List* (Sorted by Expiry) 💎\n\n"
        user_count = 0
        for user in premium_users:
            user_id = user["user_id"]
            premium_expiry = user.get("premium_expiry")
            profile = user.get("profile", {})
            expiry_date = (
                datetime.fromtimestamp(premium_expiry).strftime("%Y-%m-%d")
                if premium_expiry and isinstance(premium_expiry, (int, float)) and premium_expiry > current_time
                else "No expiry set"
            )
            active_features = [k for k, v in user.get("premium_features", {}).items() if v is True or (isinstance(v, int) and v > current_time)]
            if "instant_rematch_count" in user.get("premium_features", {}) and user["premium_features"]["instant_rematch_count"] > 0:
                active_features.append(f"instant_rematch_count: {user['premium_features']['instant_rematch_count']}")
            features_str = ", ".join(active_features) or "None"
            name = escape_markdown_v2(profile.get("name", "Not set"))
            expiry_date = escape_markdown_v2(expiry_date)
            features_str = escape_markdown_v2(features_str)
            message += (
                f"👤 *User ID*: {user_id}\n"
                f"🧑 *Name*: {name}\n"
                f"⏰ *Premium Until*: {expiry_date}\n"
                f"✨ *Features*: {features_str}\n"
                "━━━━━━━━━━━━━━\n"
            )
            user_count += 1
            if len(message.encode('utf-8')) > 4000:
                await safe_reply(update, message, context, parse_mode=ParseMode.MARKDOWN)
                message = ""
        if message:
            message += f"📊 *Total Premium Users*: {user_count}\n"
        logger.debug(f"Prepared admin_premiumuserslist text: {message}")
        await safe_reply(update, message, context, parse_mode=ParseMode.MARKDOWN)
        logger.info(f"Admin {user_id} requested premium users list with {user_count} users")
    except Exception as e:
        logger.error(f"Error fetching premium users list: {e}")
        await safe_reply(update, "😔 Error retrieving premium users list 🌑.", context)

async def admin_info(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display detailed user information"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await safe_reply(update, "🔒 Unauthorized 🌑.", context)
        return
    try:
        target_id = int(context.args[0])
        user = get_user_with_cache(target_id)
        if not user:
            await safe_reply(update, "😕 User not found 🌑.", context)
            return
        profile = user.get("profile", {})
        consent = "Yes ✅" if user.get("consent") else "No ❌"
        verified = "Yes ✅" if user.get("verified") else "No ❌"
        premium = user.get("premium_expiry")
        premium_status = f"Until {datetime.fromtimestamp(premium).strftime('%Y-%m-%d %H:%M:%S')} ⏰" if premium and premium > time.time() else "None 🌑"
        ban_status = user.get("ban_type")
        if ban_status == "permanent":
            ban_info = "Permanent 🔒"
        elif ban_status == "temporary" and user.get("ban_expiry") > time.time():
            ban_info = f"Until {datetime.fromtimestamp(user.get('ban_expiry')).strftime('%Y-%m-%d %H:%M:%S')} ⏰"
        else:
            ban_info = "None ✅"
        created_at = datetime.fromtimestamp(user.get("created_at", int(time.time()))).strftime("%Y-%m-%d %H:%M:%S")
        features = ", ".join([k for k, v in user.get("premium_features", {}).items() if v is True or (isinstance(v, int) and v > time.time())]) or "None"
        violations = get_db_collection("keyword_violations").find_one({"user_id": target_id})
        violations_count = violations.get("count", 0) if violations else 0
        violation_status = (
            "Permanent 🔒" if violations and violations.get("ban_type") == "permanent" else
            f"Temporary until {datetime.fromtimestamp(violations['ban_expiry']).strftime('%Y-%m-%d %H:%M')} ⏰"
            if violations and violations.get("ban_type") == "temporary" and violations.get("ban_expiry") else
            f"{violations_count} warnings ⚠️" if violations_count > 0 else "None ✅"
        )
        message = (
            f"🕵️ *User Info: {target_id}* 🕵️\n\n"
            f"🧑 *Name*: {escape_markdown_v2(profile.get('name', 'Not set'))}\n"
            f"🎂 *Age*: {escape_markdown_v2(str(profile.get('age', 'Not set')))}\n"
            f"👤 *Gender*: {escape_markdown_v2(profile.get('gender', 'Not set'))}\n"
            f"📍 *Location*: {escape_markdown_v2(profile.get('location', 'Not set'))}\n"
            f"🏷️ *Tags*: {escape_markdown_v2(', '.join(profile.get('tags', [])) or 'None')}\n"
            f"🤝 *Consent*: {consent}\n"
            f"✅ *Verified*: {verified}\n"
            f"💎 *Premium*: {premium_status}\n"
            f"✨ *Features*: {escape_markdown_v2(features)}\n"
            f"🚫 *Ban*: {ban_info}\n"
            f"⚠️ *Keyword Violations*: {violation_status}\n"
            f"📅 *Joined*: {created_at}"
        )
        logger.debug(f"Prepared admin_info text: {message}")
        await safe_reply(update, message, context)
    except (IndexError, ValueError):
        await safe_reply(update, "⚠️ Usage: /admin_info <user_id> 📋.", context)

async def admin_reports(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """List reported users"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await safe_reply(update, "🔒 Unauthorized 🌑.", context)
        return
    try:
        reports = get_db_collection("reports")
        pipeline = [
            {"$group": {"_id": "$reported_id", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 20}
        ]
        reports_list = list(reports.aggregate(pipeline))
        if not reports_list:
            await safe_reply(update, "✅ No reports found 🌟.", context)
            return
        message = "🚨 *Reported Users* (Top 20) 🚨\n\n"
        for report in reports_list:
            reported_id = report["_id"]
            count = report["count"]
            message += f"👤 {reported_id} | Reports: *{count}*\n"
        logger.debug(f"Prepared admin_reports text: {message}")
        await safe_reply(update, message, context)
    except Exception as e:
        logger.error(f"Failed to list reports: {e}")
        await safe_reply(update, "😔 Error retrieving reports 🌑.", context)

async def admin_clear_reports(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Clear reports for a user"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await safe_reply(update, "🔒 Unauthorized 🌑.", context)
        return
    try:
        target_id = int(context.args[0])
        reports = get_db_collection("reports")
        reports.delete_many({"reported_id": target_id})
        await safe_reply(update, f"🧹 Reports cleared for user *{target_id}* 🌟.", context)
        logger.info(f"Admin {user_id} cleared reports for {target_id}.")
    except (IndexError, ValueError):
        await safe_reply(update, "⚠️ Usage: /admin_clear_reports <user_id> 📋.", context)

async def admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Broadcast a message to all users"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await safe_reply(update, "🔒 Unauthorized 🌑.", context)
        return
    if not context.args:
        await safe_reply(update, "⚠️ Usage: /admin_broadcast <message> 📋.", context)
        return
    message = "📣 *Announcement*: " + escape_markdown_v2(" ".join(context.args))
    try:
        users = get_db_collection("users")
        users_list = users.find({"consent": True})
        sent_count = 0
        for user in users_list:
            try:
                await safe_bot_send_message(context.bot, user["user_id"], message, context)
                sent_count += 1
                time.sleep(0.05)  # Avoid rate limits
            except Exception as e:
                logger.warning(f"Failed to send broadcast to {user['user_id']}: {e}")
        await safe_reply(update, f"📢 Broadcast sent to *{sent_count}* users 🌟.", context)
        logger.info(f"Admin {user_id} sent broadcast to {sent_count} users.")
    except Exception as e:
        logger.error(f"Failed to send broadcast: {e}")
        await safe_reply(update, "😔 Error sending broadcast 🌑.", context)

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display bot statistics"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await safe_reply(update, "🔒 Unauthorized 🌑.", context)
        return
    try:
        users = get_db_collection("users")
        total_users = users.count_documents({})
        current_time = int(time.time())
        premium_users = users.count_documents({"premium_expiry": {"$gt": current_time}})
        banned_users = users.count_documents({
            "ban_type": {"$in": ["permanent", "temporary"]},
            "$or": [{"ban_expiry": {"$gt": current_time}}, {"ban_type": "permanent"}]
        })
        active_users = len(set(user_pairs.keys()).union(waiting_users))
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        stats_message = (
            "📈 *Bot Statistics* 📈\n\n"
            f"👥 *Total Users*: *{total_users}*\n"
            f"💎 *Premium Users*: *{premium_users}*\n"
            f"💬 *Active Users*: *{active_users}* (in chats or waiting)\n"
            f"🚫 *Banned Users*: *{banned_users}*\n"
            "━━━━━━━━━━━━━━\n"
            f"🕒 *Updated*: {timestamp}"
        )
        logger.debug(f"Prepared admin_stats text: {stats_message}")
        await safe_reply(update, stats_message, context)
        logger.info(f"Admin {user_id} requested bot statistics: total={total_users}, premium={premium_users}, active={active_users}, banned={banned_users}")
    except Exception as e:
        logger.error(f"Error fetching bot statistics: {e}", exc_info=True)
        await safe_reply(update, "😔 Error retrieving statistics 🌑.", context)

def main() -> None:
    """Initialize and run the Telegram bot."""
    global db  # Ensure db is accessible globally
    # Initialize MongoDB
    try:
        db = init_mongodb()
        logger.info("MongoDB initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize MongoDB: {e}")
        raise EnvironmentError("Cannot start bot without MongoDB connection")

    # Get Telegram bot token
    token = os.getenv("TOKEN")
    if not token:
        logger.error("TOKEN not set")
        raise EnvironmentError("TOKEN not set")

    # Build Application
    application = Application.builder().token(token).build()

    # Define ConversationHandler for user setup
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            CONSENT: [CallbackQueryHandler(consent_handler, pattern="^consent_")],
            VERIFICATION: [CallbackQueryHandler(verify_emoji, pattern="^emoji_")],
            NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_name)],
            AGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_age)],
            GENDER: [CallbackQueryHandler(set_gender, pattern="^gender_")],
            LOCATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_location)],
            TAGS: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_tags)]
        },
        fallbacks=[CommandHandler("start", start)],
        allow_reentry=True
    )

    # Add handlers
    application.add_handler(conv_handler)
    application.add_handler(CommandHandler("stop", restrict_access(stop)))
    application.add_handler(CommandHandler("next", restrict_access(next_chat)))
    application.add_handler(CommandHandler("help", restrict_access(help_command)))
    application.add_handler(CommandHandler("premium", restrict_access(premium)))
    application.add_handler(CommandHandler("shine", restrict_access(shine)))
    application.add_handler(CommandHandler("instant", restrict_access(instant)))
    application.add_handler(CommandHandler("mood", restrict_access(mood)))
    application.add_handler(CommandHandler("vault", restrict_access(vault)))
    application.add_handler(CommandHandler("history", restrict_access(history)))
    application.add_handler(CommandHandler("rematch", restrict_access(rematch)))
    application.add_handler(CommandHandler("flare", restrict_access(flare)))
    application.add_handler(CommandHandler("personal", restrict_access(personal)))
    application.add_handler(CommandHandler("settings", restrict_access(settings)))
    application.add_handler(CommandHandler("report", restrict_access(report)))
    application.add_handler(CommandHandler("deleteprofile", restrict_access(delete_profile)))
    application.add_handler(CommandHandler("botinfo", botinfo))

    # Add admin command handlers
    application.add_handler(CommandHandler("admin", admin_access))
    application.add_handler(CommandHandler("admin_userslist", admin_userslist))
    application.add_handler(CommandHandler("adminuserslist", admin_userslist))
    application.add_handler(CommandHandler("admin_premiumuserslist", admin_premiumuserslist))
    application.add_handler(CommandHandler("adminpremiumuserslist", admin_premiumuserslist))
    application.add_handler(CommandHandler("admin_info", admin_info))
    application.add_handler(CommandHandler("admininfo", admin_info))
    application.add_handler(CommandHandler("admin_delete", admin_delete))
    application.add_handler(CommandHandler("admindelete", admin_delete))
    application.add_handler(CommandHandler("admin_premium", admin_premium))
    application.add_handler(CommandHandler("adminpremium", admin_premium))
    application.add_handler(CommandHandler("admin_revoke_premium", admin_revoke_premium))
    application.add_handler(CommandHandler("adminrevokepremium", admin_revoke_premium))
    application.add_handler(CommandHandler("admin_ban", admin_ban))
    application.add_handler(CommandHandler("adminban", admin_ban))
    application.add_handler(CommandHandler("admin_unban", admin_unban))
    application.add_handler(CommandHandler("adminunban", admin_unban))
    application.add_handler(CommandHandler("admin_violations", admin_violations))
    application.add_handler(CommandHandler("adminviolations", admin_violations))
    application.add_handler(CommandHandler("admin_reports", admin_reports))
    application.add_handler(CommandHandler("adminreports", admin_reports))
    application.add_handler(CommandHandler("admin_clear_reports", admin_clear_reports))
    application.add_handler(CommandHandler("adminclearreports", admin_clear_reports))
    application.add_handler(CommandHandler("admin_stats", admin_stats))
    application.add_handler(CommandHandler("adminstats", admin_stats))
    application.add_handler(CommandHandler("admin_broadcast", admin_broadcast))
    application.add_handler(CommandHandler("adminbroadcast", admin_broadcast))

    # Handle all callback queries (buttons)
    application.add_handler(CallbackQueryHandler(button))

    # Handle payments
    application.add_handler(PreCheckoutQueryHandler(pre_checkout))
    application.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment))

    # Handle regular messages
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    # Add error handler
    application.add_error_handler(error_handler)

    # Schedule recurring jobs
    if application.job_queue:
        try:
            application.job_queue.run_repeating(cleanup_in_memory, interval=300, first=10)
            application.job_queue.run_repeating(process_queued_operations, interval=60, first=10)
            application.job_queue.run_repeating(match_users, interval=30, first=5)
            logger.info("Scheduled job queue tasks")
        except NameError as e:
            logger.error(f"Job queue function not defined: {e}")
            raise
    else:
        logger.error("Failed to initialize job queue")
        raise RuntimeError("Failed to initialize job queue")

    logger.info("Starting bot...")
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")
        raise

if __name__ == "__main__":
    main()


