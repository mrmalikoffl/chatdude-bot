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
    JobQueue,
)
import logging
import os
import time
from datetime import datetime, timedelta
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure  # Corrected import
from queue import Queue
from urllib.parse import urlparse
import urllib.parse
import json
import re
from collections import defaultdict
import warnings
import telegram.error
import random
import threading

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
NOTIFICATION_CHANNEL_ID = "-1002519617667"  # Your channel ID

# Security configurations
BANNED_WORDS = {
    "spam", "hate", "abuse", "nsfw", "inappropriate", "offensive",
    "harass", "bully", "threat", "sex", "porn", "nude", "violence",
    "scam", "hack", "phish", "malware"
}
BLOCKED_KEYWORDS = [
    r"f[u*]ck|sh[i1][t*]|b[i1]tch|a[s$][s$]h[o0]le",
    r"d[i1]ck|p[u*][s$][s$]y|c[o0]ck|t[i1]t[s$]",
    r"n[i1]gg[e3]r|f[a4]g|r[e3]t[a4]rd",
    r"idi[o0]t|m[o0]r[o0]n|sc[u*]m",
    r"s[e3]x|p[o0]rn|r[a4]p[e3]|h[o0]rny",
    r"h[a4]t[e3]|b[u*]lly|k[i1]ll|di[e3]",
]
COMMAND_COOLDOWN = 5
MAX_MESSAGES_PER_MINUTE = 15
REPORT_THRESHOLD = 3
TEMP_BAN_DURATION = 24 * 3600  # 1 day
MAX_PROFILE_LENGTH = 500
ALLOWED_TAGS = {"music", "gaming", "movies", "tech", "sports", "art", "travel", "food", "books", "fashion"}

# In-memory storage
waiting_users_lock = threading.Lock()
waiting_users = []
user_pairs = {}
user_activities = {}
command_timestamps = {}
message_timestamps = defaultdict(list)
chat_histories = {}

# Conversation states
NAME, AGE, GENDER, LOCATION, CONSENT, VERIFICATION, TAGS = range(7)

# Emoji list for verification
VERIFICATION_EMOJIS = ['😊', '😢', '😡', '😎', '😍', '😉', '😜', '😴']

# MongoDB client and database
mongo_client = None
db = None
operation_queue = Queue()

def init_mongodb():
    uri = os.getenv("MONGODB_URI")
    if not uri:
        logger.error("MONGODB_URI not set")
        raise EnvironmentError("MONGODB_URI not set")
    
    try:
        logger.info(f"Connecting to MongoDB with URI: {uri[:30]}...[redacted]")
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.talk2anyone.command("ping")
        db = client.get_database("talk2anyone")
        db.users.create_index("user_id", unique=True)  # Add index
        logger.info("MongoDB connected successfully")
        return db
    except ConnectionFailure as e:
        logger.error(f"MongoDB connection failed: {e}")
        raise
    except OperationFailure as e:
        logger.error(f"MongoDB authentication failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error initializing MongoDB: {e}")
        raise

def get_db_collection(collection_name):
    """Get a MongoDB collection"""
    return db[collection_name]

def process_queued_operations(context: CallbackContext):
    while not operation_queue.empty():
        op_type, args = operation_queue.get()
        try:
            if op_type == "update_user":
                update_user(*args)
            elif op_type == "delete_user":
                delete_user(*args)
            elif op_type == "issue_keyword_violation":
                issue_keyword_violation(*args)
            logger.info(f"Successfully processed queued operation: {op_type} with args {args}")
        except Exception as e:
            logger.error(f"Failed to process queued operation {op_type} with args {args}: {e}")
            operation_queue.put((op_type, args))  # Re-queue on failure

try:
    db = init_mongodb()
except Exception as e:
    logger.error(f"Failed to set up MongoDB: {e}")
    exit(1)

def cleanup_in_memory(context: CallbackContext) -> None:
    logger.info(f"Cleaning up in-memory data. user_pairs size: {len(user_pairs)}, waiting_users size: {len(waiting_users)}")
    current_time = time.time()
    for user_id in list(user_pairs.keys()):
        last_activity = user_activities.get(user_id, {}).get("last_activity", 0)
        if current_time - last_activity > INACTIVITY_TIMEOUT:
            partner_id = user_pairs.get(user_id)
            if partner_id:
                safe_send_message(user_id, "🛑 Chat ended due to inactivity.")
                safe_send_message(partner_id, "🛑 Chat ended due to inactivity.")
                remove_pair(user_id, partner_id)
    logger.info(f"Cleanup complete. user_pairs size: {len(user_pairs)}, waiting_users size: {len(waiting_users)}")

# Add at the top with other in-memory storage
user_activities = {}
INACTIVITY_TIMEOUT = 1800  # 30 minutes in seconds

def remove_pair(user_id: int, partner_id: int) -> None:
    """Remove a user pair and clean up related data"""
    if user_id in user_pairs:
        del user_pairs[user_id]
    if partner_id in user_pairs:
        del user_pairs[partner_id]
    logger.info(f"Removed pair: {user_id} and {partner_id}")

def cleanup_premium_features(user_id: int) -> bool:
    user = get_user(user_id)
    if not user:
        return False
    features = user.get("premium_features", {})
    current_time = int(time.time())
    updated_features = {}
    for key, value in features.items():
        if key == "instant_rematch_count":
            updated_features[key] = value
        elif value is True or (isinstance(value, int) and value > current_time):
            updated_features[key] = value
    premium_expiry = user.get("premium_expiry")
    if premium_expiry and premium_expiry <= current_time:
        premium_expiry = None
    return update_user(user_id, {
        "premium_features": updated_features,
        "premium_expiry": premium_expiry,
        "profile": user.get("profile", {}),
        "consent": user.get("consent", False),
        "verified": user.get("verified", False),
        "created_at": user.get("created_at", int(time.time()))
    })

def get_user(user_id: int) -> dict:
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
        user = users.find_one({"user_id": user_id})  # Fetch again instead of recursion
    logger.debug(f"Returning user {user_id}: {user}")
    return user

def update_user(user_id: int, data: dict) -> bool:
    retries = 3
    for attempt in range(retries):
        try:
            users = get_db_collection("users")
            existing = users.find_one({"user_id": user_id})
            if existing:
                existing_data = {
                    "profile": existing.get("profile", {}),
                    "consent": existing.get("consent", False),
                    "consent_time": existing.get("consent_time"),
                    "premium_expiry": existing.get("premium_expiry"),
                    "ban_type": existing.get("ban_type"),
                    "ban_expiry": existing.get("ban_expiry"),
                    "verified": existing.get("verified", False),
                    "premium_features": existing.get("premium_features", {}),
                    "created_at": existing.get("created_at", int(time.time()))
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
            updated_data = {
                "user_id": user_id,
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
            users.update_one(
                {"user_id": user_id},
                {"$set": updated_data},
                upsert=True
            )
            logger.debug(f"Updated user {user_id}: {json.dumps(updated_data, default=str)}")
            return True
        except (ConnectionError, OperationFailure) as e:
            logger.error(f"Attempt {attempt + 1}/{retries} failed to update user {user_id}: {e}")
            if attempt < retries - 1:
                time.sleep(1)
        except Exception as e:
            logger.error(f"Unexpected error updating user {user_id}: {e}")
            break
    logger.error(f"Failed to update user {user_id} after {retries} attempts")
    operation_queue.put(("update_user", (user_id, data)))
    return False

def delete_user(user_id: int):
    try:
        reports = get_db_collection("reports")
        violations = get_db_collection("keyword_violations")
        users = get_db_collection("users")
        reports.delete_many({"$or": [{"reporter_id": user_id}, {"reported_id": user_id}]})
        violations.delete_many({"user_id": user_id})
        result = users.delete_one({"user_id": user_id})
        if result.deleted_count > 0:
            logger.info(f"Deleted user {user_id} from database.")
        else:
            logger.warning(f"No user found with user_id {user_id}")
    except (ConnectionError, OperationFailure) as e:
        logger.error(f"Failed to delete user {user_id}: {e}")
        operation_queue.put(("delete_user", (user_id,)))
        raise
    except Exception as e:
        logger.error(f"Unexpected error deleting user {user_id}: {e}")
        raise

def issue_keyword_violation(user_id: int, keyword: str, update: Update, context: CallbackContext) -> str:
    try:
        violations = get_db_collection("keyword_violations")
        violation = violations.find_one({"user_id": user_id})
        current_time = int(time.time())
        if violation and (current_time - violation.get("last_violation", 0) > 48 * 3600):
            violations.delete_one({"user_id": user_id})
            violation = None
        count = (violation.get("count", 0) + 1) if violation else 1
        if count == 1:
            violations.update_one(
                {"user_id": user_id},
                {"$set": {"count": 1, "keyword": keyword, "last_violation": current_time}},
                upsert=True
            )
            safe_reply(update, 
                "🚨 *Warning 1/5* 🚨\n"
                "Your message contained inappropriate content. Please follow the rules to avoid further action."
            )
            logger.warning(f"User {user_id} warned (1/5) for keyword: {keyword}")
            return "warned_1"
        elif count == 2:
            violations.update_one(
                {"user_id": user_id},
                {"$set": {"count": 2, "keyword": keyword, "last_violation": current_time}},
                upsert=True
            )
            safe_reply(update, 
                "🚨 *Warning 2/5* 🚨\n"
                "Another inappropriate message detected. One more violation will result in a 12-hour ban."
            )
            logger.warning(f"User {user_id} warned (2/5) for keyword: {keyword}")
            return "warned_2"
        elif count == 3:
            ban_expiry = current_time + 12 * 3600
            violations.update_one(
                {"user_id": user_id},
                {"$set": {"count": 3, "keyword": keyword, "last_violation": current_time, "ban_type": "temporary", "ban_expiry": ban_expiry}},
                upsert=True
            )
            update_user(user_id, {"ban_type": "temporary", "ban_expiry": ban_expiry})
            safe_reply(update, 
                "🚫 *Temporary Ban* 🚫\n"
                "You’ve been banned for 12 hours due to repeated inappropriate messages. "
                "Contact support if you believe this is an error."
            )
            logger.info(f"User {user_id} banned for 12 hours (3/5) for keyword: {keyword}")
            stop(update, context)
            return "banned_12h"
        elif count == 4:
            ban_expiry = current_time + 24 * 3600
            violations.update_one(
                {"user_id": user_id},
                {"$set": {"count": 4, "keyword": keyword, "last_violation": current_time, "ban_type": "temporary", "ban_expiry": ban_expiry}},
                upsert=True
            )
            update_user(user_id, {"ban_type": "temporary", "ban_expiry": ban_expiry})
            safe_reply(update, 
                "🚫 *Temporary Ban* 🚫\n"
                "You’ve been banned for 24 hours due to continued violations. "
                "Further issues will lead to a permanent ban."
            )
            logger.info(f"User {user_id} banned for 24 hours (4/5) for keyword: {keyword}")
            stop(update, context)
            return "banned_24h"
        else:
            violations.update_one(
                {"user_id": user_id},
                {"$set": {"count": 5, "keyword": keyword, "last_violation": current_time, "ban_type": "permanent", "ban_expiry": None}},
                upsert=True
            )
            update_user(user_id, {"ban_type": "permanent", "ban_expiry": None})
            safe_reply(update, 
                "🚫 *Permanent Ban* 🚫\n"
                "You’ve been permanently banned for repeated violations. "
                "Contact support to appeal."
            )
            logger.info(f"User {user_id} permanently banned (5/5) for keyword: {keyword}")
            stop(update, context)
            return "banned_permanent"
    except (ConnectionError, OperationFailure) as e:
        logger.error(f"Error issuing keyword violation for {user_id}: {e}")
        operation_queue.put(("issue_keyword_violation", (user_id, keyword, update, context)))
        safe_reply(update, "❌ Error processing your message.")
        return "error"
    except Exception as e:
        logger.error(f"Unexpected error issuing violation for {user_id}: {e}")
        safe_reply(update, "❌ Error processing your message.")
        return "error"

def is_banned(user_id: int) -> bool:
    user = get_user(user_id)
    if user.get("ban_type"):
        if user["ban_type"] == "permanent":
            return True
        if user["ban_type"] == "temporary" and user["ban_expiry"] and user["ban_expiry"] > time.time():
            return True
        if user["ban_type"] == "temporary" and user["ban_expiry"] and user["ban_expiry"] <= time.time():
            update_user(user_id, {"ban_type": None, "ban_expiry": None})
    return False

def has_premium_feature(user_id: int, feature: str) -> bool:
    user = get_user(user_id)
    features = user.get("premium_features", {})
    current_time = int(time.time())
    if feature == "instant_rematch":
        return features.get("instant_rematch_count", 0) > 0
    return feature in features and (features[feature] is True or (isinstance(features[feature], int) and features[feature] > current_time))

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
    if any(word in text_lower for word in BANNED_WORDS):
        return False, "banned_word"
    for pattern in BLOCKED_KEYWORDS:
        if re.search(pattern, text_lower):
            return False, pattern
    url_pattern = re.compile(r'http[s]?://|www\.|\.com|\.org|\.net')
    if url_pattern.search(text_lower):
        return False, "url"
    return True, ""

def send_channel_notification(bot, message: str) -> bool:
    if not NOTIFICATION_CHANNEL_ID:
        logger.error("NOTIFICATION_CHANNEL_ID not set.")
        return False
    try:
        safe_bot_send_message(bot, NOTIFICATION_CHANNEL_ID, message)
        logger.info(f"Sent notification to channel {NOTIFICATION_CHANNEL_ID}: {message[:100]}...")
        return True
    except telegram.error.TelegramError as e:
        logger.error(f"Failed to send notification to channel {NOTIFICATION_CHANNEL_ID}: {e}")
        return False

import re

def escape_markdown_v2(text: str) -> str:
    if not isinstance(text, str):
        return str(text) if text is not None else ""
    special_chars = r'_[]()~`>#+-=|{}.!'
    # First, ensure existing backslashes aren't doubled unnecessarily
    text = re.sub(r'\\(?=[{}])'.format(re.escape(special_chars)), '', text)
    # Then escape any unescaped special characters
    return re.sub(r'([{}])'.format(re.escape(special_chars)), r'\\\1', text)

def safe_reply(update: Update, text: str, parse_mode: str = "MarkdownV2", **kwargs) -> None:
    try:
        if parse_mode == "MarkdownV2":
            escaped_text = escape_markdown_v2(text)
            logger.debug(f"Escaped text for MarkdownV2: {escaped_text[:200]}")
            text = escaped_text
        if update.message:
            update.message.reply_text(text, parse_mode=parse_mode, **kwargs)
        elif update.callback_query:
            query = update.callback_query
            query.answer()
            query.message.reply_text(text, parse_mode=parse_mode, **kwargs)
    except telegram.error.BadRequest as bre:
        logger.warning(f"MarkdownV2 parsing failed: {bre}. Text: {text[:200]}")
        clean_text = re.sub(r'([_*[\]()~`>#+-|=}{.!])', '', text)
        if update.callback_query:
            update.callback_query.message.reply_text(clean_text, parse_mode=None, **kwargs)
        elif update.message:
            update.message.reply_text(clean_text, parse_mode=None, **kwargs)
    except Exception as e:
        logger.error(f"Failed to send message: {e}")

def safe_bot_send_message(bot, chat_id: int, text: str, parse_mode: str = "MarkdownV2", **kwargs):
    try:
        if parse_mode == "MarkdownV2":
            escaped_text = escape_markdown_v2(text)
            logger.debug(f"Escaped text for MarkdownV2: {escaped_text[:200]}")
            text = escaped_text
        bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode, **kwargs)
    except telegram.error.BadRequest as e:
        logger.warning(f"MarkdownV2 parsing failed for chat {chat_id}: {e}. Text: {text[:200]}")
        clean_text = re.sub(r'([_*[\]()~`>#+-|=}{.!])', '', text)
        bot.send_message(chat_id=chat_id, text=clean_text, parse_mode=None, **kwargs)
    except Exception as e:
        logger.error(f"Failed to send message to {chat_id}: {e}")
        
def start(update: Update, context: CallbackContext) -> int:
    user_id = update.effective_user.id
    logger.info(f"Received /start command from user {user_id}")
    
    try:
        if is_banned(user_id):
            user = get_user(user_id)
            ban_msg = (
                "🚫 You are permanently banned 🔒. Contact support to appeal 📧."
                if user["ban_type"] == "permanent"
                else f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')} ⏰."
            )
            safe_reply(update, ban_msg)
            return ConversationHandler.END
        
        if not check_rate_limit(user_id):
            logger.info(f"User {user_id} hit rate limit")
            safe_reply(update, f"⏳ Please wait {COMMAND_COOLDOWN} seconds before trying again ⏰.")
            return ConversationHandler.END
        
        # Check if user is already in a chat or waiting
        if user_id in user_pairs:
            logger.info(f"User {user_id} already in a chat")
            safe_reply(update, "💬 You're already in a chat 😔. Use /next to switch or /stop to end.")
            return ConversationHandler.END
        
        if user_id in waiting_users:
            logger.info(f"User {user_id} already in waiting list")
            safe_reply(update, "🔍 You're already waiting for a chat partner... Please wait!")
            return ConversationHandler.END
        
        user = get_user(user_id)
        if not user.get("consent"):
            logger.info(f"User {user_id} needs to consent")
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
            safe_reply(update, welcome_text, reply_markup=reply_markup)
            send_channel_notification(context.bot, (
                "🆕 *New User Accessed* 🆕\n\n"
                f"👤 *User ID*: {user_id}\n"
                f"📅 *Time*: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
                "ℹ️ Awaiting consent"
            ))
            return CONSENT
        
        if not user.get("verified"):
            logger.info(f"User {user_id} needs verification")
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
            logger.info(f"User {user_id} missing profile fields: {missing_fields}")
            safe_reply(update, "✨ Let’s set up your profile\\! Please enter your name:")
            return NAME
        
        # Check if profile was just set up (e.g., from conversation flow)
        # For simplicity, assume if all fields are present and user just started, prompt for /next
        safe_reply(update, "🎉 Your profile is ready! Use `/next` to find a chat partner and start connecting! 🚀")
        return ConversationHandler.END
    
    except Exception as e:
        logger.error(f"Error in start for user {user_id}: {str(e)}", exc_info=True)
        safe_reply(update, "😔 An error occurred 🌑. Please try again later.")
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
        safe_reply(update, "✅ Thank you for agreeing! Let’s verify your profile.")
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
    if not 1 <= len(name) <= 50:
        safe_reply(update, "⚠️ Name must be 1-50 characters.")
        return NAME
    is_safe, _ = is_safe_message(name)
    if not is_safe:
        safe_reply(update, "⚠️ Name contains inappropriate content.")
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
    safe_reply(update, f"🧑 Name set to: *{name}*!")
    safe_reply(update, "🎂 Please enter your age (e.g., 25):")
    return AGE

def set_age(update: Update, context: CallbackContext) -> int:
    user_id = update.effective_user.id
    user = get_user(user_id)
    profile = user.get("profile", {})
    age_text = update.message.text.strip()
    try:
        age = int(age_text)
        if not 13 <= age <= 120:
            safe_reply(update, "⚠️ Age must be between 13 and 120.")
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
        safe_reply(update, f"🎂 Age set to: *{age}*!")
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
        safe_reply(update, "⚠️ Please enter a valid number for your age.")
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
        safe_reply(update, f"👤 Gender set to: *{gender}*!")
        safe_reply(update, "📍 Please enter your location (e.g., New York):")
        return LOCATION
    safe_reply(update, "⚠️ Invalid selection. Please choose a gender.")
    return GENDER

def set_location(update: Update, context: CallbackContext) -> int:
    user_id = update.effective_user.id
    user = get_user(user_id)
    profile = user.get("profile", {})
    location = update.message.text.strip()

    # Validate location
    if not 1 <= len(location) <= 100:
        safe_reply(update, "⚠️ Location must be 1-100 characters.")
        logger.warning(f"User {user_id} provided invalid location: {location}")
        return LOCATION
    is_safe, _ = is_safe_message(location)
    if not is_safe:
        safe_reply(update, "⚠️ Location contains inappropriate content.")
        logger.warning(f"User {user_id} provided unsafe location: {location}")
        return LOCATION

    # Update user's profile with location
    profile["location"] = location
    update_user(user_id, {
        "profile": profile,
        "consent": user.get("consent", False),
        "verified": user.get("verified", False),
        "premium_expiry": user.get("premium_expiry"),
        "premium_features": user.get("premium_features", {}),
        "created_at": user.get("created_at", int(time.time()))
    })
    logger.info(f"User {user_id} set location to: {location}. Full profile: {profile}")

    # Send congratulatory message
    safe_reply(update, "🎉 Congratulations! Profile setup complete! 🎉")

    # Prompt user to use /next to connect
    safe_reply(update, (
        "🔍 Your profile is ready! 🎉\n\n"
        "🚀 Use `/next` to find a chat partner and start connecting!\n"
        "ℹ️ Sending text messages now won’t start a chat. Use /help for more options."
    ))
    logger.info(f"User {user_id} completed profile setup. Prompted /next.")

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

    return ConversationHandler.END

def match_users(context: CallbackContext) -> None:
    with waiting_users_lock:  # Add lock to ensure thread-safe access
        logger.info(f"Starting match_users. Current waiting list: {waiting_users}")
        
        if len(waiting_users) < 2:
            logger.info("Not enough users to match.")
            return
        
        premium_users = [u for u in waiting_users if has_premium_feature(u, "shine_profile")]
        regular_users = [u for u in waiting_users if u not in premium_users]
        
        for user1 in premium_users + regular_users:
            if user1 not in waiting_users:
                continue
            for user2 in waiting_users:
                if user2 == user1 or user2 not in waiting_users:
                    continue
                if can_match(user1, user2):
                    waiting_users.remove(user1)
                    waiting_users.remove(user2)
                    user_pairs[user1] = user2
                    user_pairs[user2] = user1
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
                        "✅ *Connected!* Start chatting now! 🗣️\n\n"
                        f"👤 *Partner Details*:\n"
                        f"🧑 *Name*: {user2_profile.get('name', 'Not set')}\n"
                        f"🎂 *Age*: {user2_profile.get('age', 'Not set')}\n"
                        f"👤 *Gender*: {user2_profile.get('gender', 'Not set')}\n"
                        f"📍 *Location*: {user2_profile.get('location', 'Not set')}\n\n"
                        "Use /help for more options."
                    ) if has_premium_feature(user1, "partner_details") else (
                        "✅ *Connected!* Start chatting now! 🗣️\n\n"
                        "🔒 *Partner Details*: Upgrade to *Partner Details Reveal* to view your partner’s profile!\n"
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
                        "🔒 *Partner Details*: Upgrade to *Partner Details Reveal* to view your partner’s profile!\n"
                        "Unlock with /premium.\n\n"
                        "Use /help for more options."
                    )
                    safe_bot_send_message(context.bot, user1, user1_message)
                    safe_bot_send_message(context.bot, user2, user2_message)
                    logger.info(f"Matched users {user1} and {user2}. Current waiting list: {waiting_users}, Paired users: {list(user_pairs.keys())}")
                    if has_premium_feature(user1, "vaulted_chats"):
                        chat_histories[user1] = []
                    if has_premium_feature(user2, "vaulted_chats"):
                        chat_histories[user2] = []
                    return

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
    
    # Check if user is waiting
    if user_id in waiting_users:
        waiting_users.remove(user_id)
        logger.info(f"User {user_id} removed from waiting list. Current waiting list: {waiting_users}")
        safe_reply(update, "⏹️ You’ve stopped waiting for a chat partner. Use /next to try again.")
        return
    
    # Check if user is in a chat
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        del user_pairs[user_id]
        if partner_id in user_pairs:
            del user_pairs[partner_id]
        safe_bot_send_message(context.bot, partner_id, "👋 Your partner has left the chat. Use /next to find a new one.")
        safe_reply(update, "👋 Chat ended. Use /next to begin a new chat.")
        logger.info(f"User {user_id} stopped chat with {partner_id}.")
        if user_id in chat_histories and not has_premium_feature(user_id, "vaulted_chats"):
            del chat_histories[user_id]
    else:
        user = get_user(user_id)
        profile = user.get("profile", {})
        required_fields = ["name", "age", "gender", "location"]
        if all(field in profile for field in required_fields):
            safe_reply(update, "🎉 Your profile is ready! Use `/next` to find a chat partner and start connecting! 🚀")
        else:
            safe_reply(update, "❓ You're not in a chat or waiting. Use /start to begin.")
        
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
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        del user_pairs[user_id]
        del user_pairs[partner_id]
        safe_bot_send_message(context.bot, partner_id, "🔌 Your chat partner disconnected.")
    if user_id not in waiting_users:
        if has_premium_feature(user_id, "shine_profile"):
            waiting_users.insert(0, user_id)
        else:
            waiting_users.append(user_id)
    safe_reply(update, "🔍 Looking for a new chat partner...")
    match_users(context)
    logger.info(f"User {user_id} requested next chat.")

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
        "buy_premium_pass": ("Premium Pass", 1000, "Unlock all features for 30 days + 5 Instant Rematches!", "premium_pass"),
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
        safe_reply(update, "❌ Error generating payment invoice.")

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
        "flare_messages": (7 * 24 * 3600, "✨ *Flare Messages* activated for 7 days!", "Flare Messages", 100),
        "instant_rematch": (None, "🔄 *Instant Rematch* unlocked! Use /instant to reconnect.", "Instant Rematch", 100),
        "shine_profile": (24 * 3600, "🌟 *Shine Profile* activated for 24 hours!", "Shine Profile", 250),
        "mood_match": (30 * 24 * 3600, "😊 *Mood Match* activated for 30 days!", "Mood Match", 250),
        "partner_details": (30 * 24 * 3600, "👤 *Partner Details* unlocked for 30 days!", "Partner Details", 500),
        "vaulted_chats": (None, "📜 *Vaulted Chats* unlocked forever!", "Vaulted Chats", 500),
        "premium_pass": (30 * 24 * 3600, "🎉 *Premium Pass* activated! Enjoy all features for 30 days + 5 Instant Rematches!", "Premium Pass", 1000),
    }
    user = get_user(user_id)
    features = user.get("premium_features", {})
    premium_expiry = user.get("premium_expiry")
    profile = user.get("profile", {})
    for feature, (duration, message, feature_name, stars) in feature_map.items():
        if payload.startswith(feature):
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
    if not has_premium_feature(user_id, "shine_profile"):
        safe_reply(update, "🌟 *Shine Profile* is a premium feature. Buy it with /premium!")
        return
    if user_id not in waiting_users and user_id not in user_pairs:
        waiting_users.insert(0, user_id)
        safe_reply(update, "✨ Your profile is now shining! You’re first in line for matches!")
        match_users(context)
    else:
        safe_reply(update, "❓ You're already in a chat or waiting list.")

def instant(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        safe_reply(update, ban_msg)
        return
    if not has_premium_feature(user_id, "instant_rematch"):
        safe_reply(update, "🔄 *Instant Rematch* is a premium feature. Buy it with /premium!")
        return
    user = get_user(user_id)
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
    if user_id in user_pairs:
        safe_reply(update, "❓ You're already in a chat. Use /stop to end it first.")
        return
    if partner_id in user_pairs:
        safe_reply(update, "❌ Your previous partner is currently in another chat.")
        return
    if partner_id in waiting_users:
        waiting_users.remove(partner_id)
        user_pairs[user_id] = partner_id
        user_pairs[partner_id] = user_id
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
        logger.info(f"User {user_id} used Instant Rematch with {partner_id}")
        if has_premium_feature(user_id, "vaulted_chats"):
            chat_histories[user_id] = chat_histories.get(user_id, [])
        if has_premium_feature(partner_id, "vaulted_chats"):
            chat_histories[partner_id] = chat_histories.get(partner_id, [])
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
        safe_reply(update, "📩 Rematch request sent to your previous partner. Waiting for their response...")
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

def mood(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        safe_reply(update, ban_msg)
        return
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

def set_mood(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
    if not has_premium_feature(user_id, "mood_match"):
        safe_reply(update, "😊 *Mood Match* is a premium feature. Buy it with /premium!")
        return
    choice = query.data
    user = get_user(user_id)
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

def vault(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        safe_reply(update, ban_msg)
        return
    if not has_premium_feature(user_id, "vaulted_chats"):
        safe_reply(update, "📜 *Vaulted Chats* is a premium feature. Buy it with /premium!")
        return
    if user_id not in user_pairs:
        safe_reply(update, "❓ You're not in a chat. Use /start to begin.")
        return
    if user_id not in chat_histories:
        chat_histories[user_id] = []
    safe_reply(update, "📜 Your current chat is being saved to the vault!")

def history(update: Update, context: CallbackContext) -> None:
    """Display chat history for premium users"""
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned 🔒. Contact support to appeal 📧." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')} ⏰."
        safe_reply(update, ban_msg)
        return
    user = get_user(user_id)
    logger.debug(f"User {user_id} before history: premium_expiry={user.get('premium_expiry')}, premium_features={user.get('premium_features')}")
    if not has_premium_feature(user_id, "vaulted_chats"):
        safe_reply(update, "📜 *Chat History* is a premium feature. Buy it with /premium! 🌟")
        return
    if user_id not in chat_histories or not chat_histories[user_id]:
        safe_reply(update, "📭 Your chat vault is empty 😔.")
        return
    history_text = "📜 *Your Chat History* 📜\n\n"
    for idx, msg in enumerate(chat_histories[user_id], 1):
        history_text += f"{idx}. {msg}\n"
    safe_reply(update, history_text)
    updated_user = get_user(user_id)
    logger.debug(f"User {user_id} after history: premium_expiry={updated_user.get('premium_expiry')}, premium_features={updated_user.get('premium_features')}")

def rematch(update: Update, context: CallbackContext) -> None:
    """Initiate a rematch with past partners"""
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned 🔒. Contact support to appeal 📧." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')} ⏰."
        safe_reply(update, ban_msg)
        return
    if not check_rate_limit(user_id):
        safe_reply(update, f"⏳ Please wait {COMMAND_COOLDOWN} seconds before trying again ⏰.")
        return
    user = get_user(user_id)
    logger.debug(f"User {user_id} before rematch: premium_expiry={user.get('premium_expiry')}, premium_features={user.get('premium_features')}")
    if not has_premium_feature(user_id, "instant_rematch"):
        safe_reply(update, "🔄 *Rematch* is a premium feature. Buy it with /premium! 🌟")
        return
    partners = user.get("profile", {}).get("past_partners", [])
    if not partners:
        safe_reply(update, "❌ No past partners to rematch with 😔.")
        return
    keyboard = []
    for partner_id in partners[-5:]:  # Limit to last 5 partners
        partner_data = get_user(partner_id)
        if partner_data:
            partner_name = partner_data.get("profile", {}).get("name", "Anonymous")
            keyboard.append([InlineKeyboardButton(f"Reconnect with {partner_name}", callback_data=f"rematch_request_{partner_id}")])
    if not keyboard:
        safe_reply(update, "❌ No available past partners to rematch with 😔.")
        return
    reply_markup = InlineKeyboardMarkup(keyboard)
    safe_reply(update, "🔄 *Choose a Past Partner to Rematch* 🔄", reply_markup=reply_markup)
    updated_user = get_user(user_id)
    logger.debug(f"User {user_id} after rematch: premium_expiry={updated_user.get('premium_expiry')}, premium_features={updated_user.get('premium_features')}")

def flare(update: Update, context: CallbackContext) -> None:
    """Activate flare messages for premium users"""
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned 🔒. Contact support to appeal 📧." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')} ⏰."
        safe_reply(update, ban_msg)
        return
    user = get_user(user_id)
    logger.debug(f"User {user_id} before flare: premium_expiry={user.get('premium_expiry')}, premium_features={user.get('premium_features')}")
    if not has_premium_feature(user_id, "flare_messages"):
        safe_reply(update, "🌟 *Flare Messages* is a premium feature. Buy it with /premium! 🌟")
        return
    safe_reply(update, "✨ Your messages are sparkling with *Flare*! Keep chatting to show it off! 🌟")
    logger.info(f"User {user_id} activated Flare Messages 🎉")
    updated_user = get_user(user_id)
    logger.debug(f"User {user_id} after flare: premium_expiry={updated_user.get('premium_expiry')}, premium_features={updated_user.get('premium_features')}")

def button(update: Update, context: CallbackContext) -> None:
    """Handle callback queries from inline buttons"""
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
            safe_reply(update, "🚫 You are banned and cannot send rematch requests 🔒.")
            return
        user = get_user(user_id)
        if user_id in user_pairs:
            safe_reply(update, "❓ You're already in a chat 😔. Use /stop to end it first.")
            return
        partner_data = get_user(partner_id)
        if not partner_data:
            safe_reply(update, "❌ This user is no longer available 😓.")
            return
        if partner_id in user_pairs:
            safe_reply(update, "❌ This user is currently in another chat 💬.")
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
        if user_id in user_pairs:
            safe_reply(update, "❓ You're already in a chat 😔. Use /stop to end it first.")
            return
        requester_data = get_user(requester_id)
        if not requester_data:
            safe_reply(update, "❌ This user is no longer available 😓.")
            return
        if requester_id in user_pairs:
            safe_reply(update, "❌ This user is currently in another chat 💬.")
            return
        user_pairs[user_id] = requester_id
        user_pairs[requester_id] = user_id
        safe_reply(update, "🔄 *Reconnected!* Start chatting! 🗣️")
        safe_bot_send_message(context.bot, requester_id, "🔄 *Reconnected!* Start chatting! 🗣️")
        if has_premium_feature(user_id, "vaulted_chats"):
            chat_histories[user_id] = chat_histories.get(user_id, [])
        if has_premium_feature(requester_id, "vaulted_chats"):
            chat_histories[requester_id] = chat_histories.get(requester_id, [])
        context.bot_data.get("rematch_requests", {}).pop(user_id, None)
        logger.info(f"User {user_id} accepted rematch with {requester_id}")
    elif data == "rematch_decline":
        safe_reply(update, "❌ Rematch request declined.")
        requester_data = context.bot_data.get("rematch_requests", {}).get(user_id, {})
        requester_id = requester_data.get("requester_id")
        if requester_id:
            safe_bot_send_message(context.bot, requester_id, "❌ Your rematch request was declined 😔.")
            logger.info(f"User {user_id} declined rematch with {requester_id}")
        context.bot_data.get("rematch_requests", {}).pop(user_id, None)
    elif data.startswith("emoji_"):
        verify_emoji(update, context)
    elif data.startswith("consent_"):
        consent_handler(update, context)
    elif data.startswith("gender_"):
        set_gender(update, context)

def settings(update: Update, context: CallbackContext) -> None:
    """Display settings menu for profile customization"""
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned 🔒. Contact support to appeal 📧." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')} ⏰."
        safe_reply(update, ban_msg)
        return
    user = get_user(user_id)
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
    safe_reply(update, settings_text, reply_markup=reply_markup)

def report(update: Update, context: CallbackContext) -> None:
    """Report a user for inappropriate behavior"""
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned 🔒. Contact support to appeal 📧." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')} ⏰."
        safe_reply(update, ban_msg)
        return
    if user_id not in user_pairs:
        safe_reply(update, "❓ You're not in a chat 😔. Use /start to begin.")
        return
    partner_id = user_pairs[user_id]
    try:
        reports = get_db_collection("reports")
        existing = reports.find_one({"reporter_id": user_id, "reported_id": partner_id})
        if existing:
            safe_reply(update, "⚠️ You've already reported this user 😔.")
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
                "profile": get_user(partner_id).get("profile", {}),
                "consent": get_user(partner_id).get("consent", False),
                "verified": get_user(partner_id).get("verified", False),
                "premium_expiry": get_user(partner_id).get("premium_expiry"),
                "premium_features": get_user(partner_id).get("premium_features", {}),
                "created_at": get_user(partner_id).get("created_at", int(time.time()))
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
            safe_bot_send_message(context.bot, partner_id,
                "🚫 *Temporary Ban* 🚫\n"
                f"You've been banned for 24 hours due to multiple reports 📢. "
                "Contact support if you believe this is an error."
            )
            notification_message = (
                "🚨 *User Banned* 🚨\n\n"
                f"👤 *User ID*: {partner_id}\n"
                f"📅 *Ban Duration*: 24 hours\n"
                f"🕒 *Ban Expiry*: {datetime.fromtimestamp(ban_expiry).strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"📢 *Reason*: Multiple reports ({report_count})\n"
                f"🕒 *Reported At*: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            send_channel_notification(context.bot, notification_message)
            logger.info(f"User {partner_id} banned for 24 hours due to {report_count} reports")
            stop(update, context)
        else:
            safe_reply(update, "🚨 Report submitted. Thank you for keeping the community safe! 🌟")
            notification_message = (
                "🚨 *New Report Filed* 🚨\n\n"
                f"👤 *Reporter ID*: {user_id}\n"
                f"👤 *Reported ID*: {partner_id}\n"
                f"📢 *Total Reports*: {report_count}\n"
                f"🕒 *Reported At*: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            send_channel_notification(context.bot, notification_message)
            logger.info(f"User {user_id} reported user {partner_id}. Total reports: {report_count}")
    except Exception as e:
        logger.error(f"Error processing report from {user_id} against {partner_id}: {e}")
        safe_reply(update, "❌ Error submitting report 😔. Please try again.")

def delete_profile(update: Update, context: CallbackContext) -> None:
    """Delete user profile and all associated data"""
    user_id = update.effective_user.id
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        del user_pairs[user_id]
        if partner_id in user_pairs:
            del user_pairs[partner_id]
        safe_bot_send_message(context.bot, partner_id, "👋 Your partner has left the chat. Use /start to find a new one.")
    if user_id in waiting_users:
        waiting_users.remove(user_id)
    try:
        delete_user(user_id)
        safe_reply(update, "🗑️ Your profile and data have been deleted successfully 🌟.")
        notification_message = (
            "🗑️ *User Deleted Profile* 🗑️\n\n"
            f"👤 *User ID*: {user_id}\n"
            f"🕒 *Deleted At*: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        send_channel_notification(context.bot, notification_message)
        logger.info(f"User {user_id} deleted their profile")
        if user_id in chat_histories:
            del chat_histories[user_id]
    except Exception as e:
        logger.error(f"Error deleting profile for user {user_id}: {e}")
        safe_reply(update, "❌ Error deleting your profile 😔. Please try again.")

def admin_access(update: Update, context: CallbackContext) -> None:
    """Grant admin access and display commands"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, " 🔒 Unauthorized 🌑 .")
        logger.info(f"Unauthorized access attempt by user_id={user_id}")
        return
    access_text = (
        " 🌟 *Admin Commands* 🌟 \n\n"
        " 🚀 *User Management*\n"
        "• /admin_userslist - List all users 📋 \n"
        "• /admin_premiumuserslist - List premium users 💎 \n"
        "• /admin_info <user_id> - View user details 🕵️ \n"
        "• /admin_delete <user_id> - Delete a user’s data 🗑️ \n"
        "• /admin_premium <user_id> <days> - Grant premium status 🎁 \n"
        "• /admin_revoke_premium <user_id> - Revoke premium status ❌ \n"
        "━━━━━━━━━━━━━━\n\n"
        " 🛡️ *Ban Management*\n"
        "• /admin_ban <user_id> <days/permanent> - Ban a user 🚫 \n"
        "• /admin_unban <user_id> - Unban a user 🔓 \n"
        "• /admin_violations - List recent keyword violations ⚠️ \n"
        "━━━━━━━━━━━━━━\n\n"
        " 📊 *Reports & Stats*\n"
        "• /admin_reports - List reported users 🚨 \n"
        "• /admin_clear_reports <user_id> - Clear reports 🧹 \n"
        "• /admin_stats - View bot statistics 📈 \n"
        "━━━━━━━━━━━━━━\n\n"
        " 📢 *Broadcast*\n"
        "• /admin_broadcast <message> - Send message to all users 📣 \n"
    )
    safe_reply(update, access_text)

def admin_delete(update: Update, context: CallbackContext) -> None:
    """Delete a user's data"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, " 🔒 Unauthorized 🌑 .")
        return
    try:
        target_id = int(context.args[0])
        delete_user(target_id)
        safe_reply(update, f" 🗑️ User *{target_id}* data deleted successfully 🌟 .")
        logger.info(f"Admin {user_id} deleted user {target_id}.")
    except (IndexError, ValueError):
        safe_reply(update, " ⚠️ Usage: /admin_delete <user_id> 📋 .")

def admin_premium(update: Update, context: CallbackContext) -> None:
    """Grant premium status to a user"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, " 🔒 Unauthorized 🌑 .")
        return
    try:
        target_id = int(context.args[0])
        days = int(context.args[1])
        if days <= 0:
            raise ValueError("Days must be positive")
        expiry = int(time.time()) + days * 24 * 3600
        logger.debug(f"Calculated premium_expiry for user {target_id}: {expiry} ({datetime.fromtimestamp(expiry).strftime('%Y-%m-%d %H:%M:%S')})")
        user = get_user(target_id)
        if not user:
            safe_reply(update, " 😕 User not found 🌑 .")
            return
        logger.debug(f"User {target_id} before admin_premium: premium_expiry={user.get('premium_expiry')}, premium_features={user.get('premium_features')}")
        features = user.get("premium_features", {})
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
        safe_reply(update, f" 🎁 Premium granted to user *{target_id}* for *{days}* days 🌟 .")
        safe_bot_send_message(context.bot, target_id, f" 🎉 You've been granted Premium status for {days} days !")
        logger.info(f"Admin {user_id} granted premium to {target_id} for {days} days.")
    except (IndexError, ValueError) as e:
        logger.error(f"Error in admin_premium for user {target_id}: {e}")
        safe_reply(update, " ⚠️ Usage: /admin_premium <user_id> <days> 📋 .")

def admin_revoke_premium(update: Update, context: CallbackContext) -> None:
    """Revoke premium status from a user"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, " 🔒 Unauthorized 🌑 .")
        return
    try:
        target_id = int(context.args[0])
        user = get_user(target_id)
        if not user:
            safe_reply(update, " 😕 User not found 🌑 .")
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
        safe_reply(update, f" ❌ Premium status revoked for user *{target_id}* 🌑 .")
        safe_bot_send_message(context.bot, target_id, " 😔 Your Premium status has been revoked .")
        updated_user = get_user(target_id)
        logger.info(f"Admin {user_id} revoked premium for {target_id}.")
        logger.debug(f"User {target_id} after revoke_premium: premium_expiry={updated_user.get('premium_expiry')}, premium_features={updated_user.get('premium_features')}")
    except (IndexError, ValueError):
        safe_reply(update, " ⚠️ Usage: /admin_revoke_premium <user_id> 📋 .")

def admin_ban(update: Update, context: CallbackContext) -> None:
    """Ban a user"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, " 🔒 Unauthorized 🌑 .")
        return
    try:
        target_id = int(context.args[0])
        ban_type = context.args[1].lower()
        user = get_user(target_id)
        if not user:
            safe_reply(update, " 😕 User not found 🌑 .")
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
            safe_bot_send_message(context.bot, partner_id, " 😔 Your partner has left the chat .")
        if target_id in waiting_users:
            waiting_users.remove(target_id)
        safe_reply(update, f" 🚫 User *{target_id}* has been {ban_type} banned 🌑 .")
        safe_bot_send_message(context.bot, target_id, f" 🚫 You have been {ban_type} banned from Talk2Anyone .")
        updated_user = get_user(target_id)
        logger.info(f"Admin {user_id} banned user {target_id} ({ban_type}).")
        logger.debug(f"User {target_id} after admin_ban: premium_expiry={updated_user.get('premium_expiry')}, premium_features={updated_user.get('premium_features')}")
    except (IndexError, ValueError):
        safe_reply(update, " ⚠️ Usage: /admin_ban <user_id> <days/permanent> 📋 .")

def admin_unban(update: Update, context: CallbackContext) -> None:
    """Unban a user"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, " 🔒 Unauthorized 🌑 .")
        return
    try:
        target_id = int(context.args[0])
        user = get_user(target_id)
        if not user:
            safe_reply(update, " 😕 User not found 🌑 .")
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
        safe_reply(update, f" 🔓 User *{target_id}* has been unbanned 🌟 .")
        safe_bot_send_message(context.bot, target_id, " 🎉 You have been unbanned . Use /start to begin.")
        updated_user = get_user(target_id)
        logger.info(f"Admin {user_id} unbanned user {target_id}.")
        logger.debug(f"User {target_id} after admin_unban: premium_expiry={updated_user.get('premium_expiry')}, premium_features={updated_user.get('premium_features')}")
    except (IndexError, ValueError):
        safe_reply(update, " ⚠️ Usage: /admin_unban <user_id> 📋 .")

def admin_violations(update: Update, context: CallbackContext) -> None:
    """List recent keyword violations"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, " 🔒 Unauthorized 🌑 .")
        return
    try:
        violations = get_db_collection("keyword_violations")
        cursor = violations.find().sort("last_violation", -1).limit(10)
        violations_list = list(cursor)
        if not violations_list:
            safe_reply(update, " ✅ No recent keyword violations 🌟 .")
            return
        violation_text = " ⚠️ *Recent Keyword Violations* ⚠️ \n\n"
        for v in violations_list:
            user_id = v["user_id"]
            count = v.get("count", 0)
            keyword = v.get("keyword", "N/A")
            last_violation = datetime.fromtimestamp(v["last_violation"]).strftime('%Y-%m-%d %H:%M') if v.get("last_violation") else "Unknown"
            ban_type = v.get("ban_type")
            ban_expiry = v.get("ban_expiry")
            ban_status = (
                "Permanent 🔒" if ban_type == "permanent" else
                f"Temporary until {datetime.fromtimestamp(ban_expiry).strftime('%Y-%m-%d %H:%M')} ⏰"
                if ban_type == "temporary" and ban_expiry else "None ✅"
            )
            violation_text += (
                f" 👤 User ID: *{user_id}*\n"
                f" 📉 Violations: *{count}*\n"
                f" 🔍 Keyword: *{keyword}*\n"
                f" 🕒 Last: *{last_violation}*\n"
                f" 🚫 Ban: *{ban_status}*\n"
                "━━━━━━━━━━━━━━\n"
            )
        safe_reply(update, violation_text)
    except Exception as e:
        logger.error(f"Error fetching violations: {e}")
        safe_reply(update, " 😔 Error fetching violations 🌑 .")

def admin_userslist(update: Update, context: CallbackContext) -> None:
    """List all users for authorized admins"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, " 🔒 Unauthorized 🌑 .")
        return
    try:
        users = get_db_collection("users")
        users_list = list(users.find().sort("user_id", 1))
        logger.debug(f"Raw database users: {len(users_list)} users")
        if not users_list:
            safe_reply(update, " 😕 No users found 🌑 .")
            logger.info(f"Admin {user_id} requested users list: no users found.")
            return
        message = " 📋 *All Users List* \\(Sorted by ID\\) 📋 \n\n"
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
            ban_status = user.get("ban_type", "None")  # Default to "None" if not set
            verified_status = "Yes ✅" if user.get("verified", False) else "No ❌"
            # Escape all dynamic content
            name = escape_markdown_v2(profile.get("name", "Not set"))
            user_id_str = escape_markdown_v2(str(user_id))
            created_date = escape_markdown_v2(created_date)
            premium_status = escape_markdown_v2(premium_status)
            ban_status = escape_markdown_v2(ban_status)  # Escape ban_status
            verified_status = escape_markdown_v2(verified_status)
            logger.debug(f"User {user_id}: name={name}, premium={premium_status}, ban={ban_status}, verified={verified_status}")
            message += (
                f" 👤 *User ID*: {user_id_str}\n"
                f" 🧑 *Name*: {name}\n"
                f" 📅 *Created*: {created_date}\n"
                f" 💎 *Premium*: {premium_status}\n"
                f" 🚫 *Ban*: {ban_status}\n"  # No .capitalize() needed due to default
                f" ✅ *Verified*: {verified_status}\n"
                "━━━━━━━━━━━━━━\n"
            )
            user_count += 1
            if len(message.encode('utf-8')) > 3500:
                logger.debug(f"Sending partial message: {message[:200]}...")
                safe_reply(update, message, parse_mode="MarkdownV2")
                message = ""
                logger.debug(f"Sent partial users list for admin {user_id}, users so far: {user_count}")
        if message.strip():
            message += f" 📊 *Total Users*: {user_count}\n"
            logger.debug(f"Sending final message: {message[:200]}...")
            safe_reply(update, message, parse_mode="MarkdownV2")
        logger.info(f"Admin {user_id} requested users list with {user_count} users ")
        logger.debug(f"Users list sent with {user_count} users.")
    except Exception as e:
        logger.error(f"Error fetching users list for admin {user_id}: {e}", exc_info=True)
        safe_reply(update, " 😔 Error retrieving users list 🌑 .")

def admin_premiumuserslist(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, " 🔒 Unauthorized 🌑 .")
        return
    try:
        current_time = int(time.time())
        users = get_db_collection("users")
        premium_users = list(users.find({"premium_expiry": {"$gt": current_time}}).sort("premium_expiry", -1))
        if not premium_users:
            safe_reply(update, " 😕 No premium users found 🌑 .")
            return
        # Escape the static header text
        header = escape_markdown_v2("💎 *Premium Users List* (Sorted by Expiry) 💎\n\n")
        message = header
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
            name = escape_markdown_v2(profile.get("name", "Not set"))
            expiry_date = escape_markdown_v2(expiry_date)
            active_features = [k for k, v in user.get("premium_features", {}).items() if v is True or (isinstance(v, int) and v > current_time)]
            if "instant_rematch_count" in user.get("premium_features", {}) and user["premium_features"]["instant_rematch_count"] > 0:
                active_features.append(f"instant_rematch_count: {user['premium_features']['instant_rematch_count']}")
            features_str = escape_markdown_v2(", ".join(active_features) or "None")
            message += (
                f" 👤 *User ID*: {escape_markdown_v2(str(user_id))}\n"
                f" 🧑 *Name*: {name}\n"
                f" ⏰ *Premium Until*: {expiry_date}\n"
                f" ✨ *Features*: {features_str}\n"
                "━━━━━━━━━━━━━━\n"
            )
            user_count += 1
            if len(message.encode('utf-8')) > 4000:
                safe_reply(update, message, parse_mode="MarkdownV2")
                message = ""
        if message:
            message += f" 📊 *Total Premium Users*: {user_count}\n"
            safe_reply(update, message, parse_mode="MarkdownV2")
    except Exception as e:
        logger.error(f"Error fetching premium users list: {e}")
        safe_reply(update, " 😔 Error retrieving premium users list 🌑 .")

def admin_info(update: Update, context: CallbackContext) -> None:
    """Display detailed user information"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, " 🔒 Unauthorized 🌑 .")
        return
    try:
        target_id = int(context.args[0])
        user = get_user(target_id)
        if not user:
            safe_reply(update, " 😕 User not found 🌑 .")
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
            f" 🕵️ *User Info: {target_id}* 🕵️ \n\n"
            f" 🧑 *Name*: {profile.get('name', 'Not set')}\n"
            f" 🎂 *Age*: {profile.get('age', 'Not set')}\n"
            f" 👤 *Gender*: {profile.get('gender', 'Not set')}\n"
            f" 📍 *Location*: {profile.get('location', 'Not set')}\n"
            f" 🏷️ *Tags*: {', '.join(profile.get('tags', [])) or 'None'}\n"
            f" 🤝 *Consent*: {consent}\n"
            f" ✅ *Verified*: {verified}\n"
            f" 💎 *Premium*: {premium_status}\n"
            f" ✨ *Features*: {features}\n"
            f" 🚫 *Ban*: {ban_info}\n"
            f" ⚠️ *Keyword Violations*: {violation_status}\n"
            f" 📅 *Joined*: {created_at}"
        )
        safe_reply(update, message)
    except (IndexError, ValueError):
        safe_reply(update, " ⚠️ Usage: /admin_info <user_id> 📋 .")

def admin_reports(update: Update, context: CallbackContext) -> None:
    """List reported users"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, " 🔒 Unauthorized 🌑 .")
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
            safe_reply(update, " ✅ No reports found 🌟 .")
            return
        message = " 🚨 *Reported Users* \\(Top 20\\) 🚨 \n\n"
        for report in reports_list:
            reported_id = report["_id"]
            count = report["count"]
            message += f" 👤 {reported_id} | Reports: *{count}*\n"
        safe_reply(update, message)
    except Exception as e:
        logger.error(f"Failed to list reports: {e}")
        safe_reply(update, " 😔 Error retrieving reports 🌑 .")

def admin_clear_reports(update: Update, context: CallbackContext) -> None:
    """Clear reports for a user"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, " 🔒 Unauthorized 🌑 .")
        return
    try:
        target_id = int(context.args[0])
        reports = get_db_collection("reports")
        reports.delete_many({"reported_id": target_id})
        safe_reply(update, f" 🧹 Reports cleared for user *{target_id}* 🌟 .")
        logger.info(f"Admin {user_id} cleared reports for {target_id}.")
    except (IndexError, ValueError):
        safe_reply(update, " ⚠️ Usage: /admin_clear_reports <user_id> 📋 .")

def admin_broadcast(update: Update, context: CallbackContext) -> None:
    """Broadcast a message to all users"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, " 🔒 Unauthorized 🌑 .")
        return
    if not context.args:
        safe_reply(update, " ⚠️ Usage: /admin_broadcast <message> 📋 .")
        return
    message = " 📣 *Announcement*: " + " ".join(context.args)
    try:
        users = get_db_collection("users")
        users_list = users.find({"consent": True})
        sent_count = 0
        for user in users_list:
            try:
                safe_bot_send_message(context.bot, user["user_id"], message)
                sent_count += 1
            except Exception as e:
                logger.warning(f"Failed to send broadcast to {user['user_id']}: {e}")
        safe_reply(update, f" 📢 Broadcast sent to *{sent_count}* users 🌟 .")
        logger.info(f"Admin {user_id} sent broadcast to {sent_count} users.")
    except Exception as e:
        logger.error(f"Failed to send broadcast: {e}")
        safe_reply(update, " 😔 Error sending broadcast 🌑 .")

def admin_stats(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        safe_reply(update, "🔒 Unauthorized 🌑.")
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
        timestamp = datetime.now().strftime("%Y\\-%m\\-%d %H\\:%M\\:%S")
        stats_message = (
            "📈 *Bot Statistics* 📈\n\n"
            f"👥 *Total Users*: *{total_users}*\n"
            f"💎 *Premium Users*: *{premium_users}*\n"
            f"💬 *Active Users*: *{active_users}* \\(in chats or waiting\\)\n"
            f"🚫 *Banned Users*: *{banned_users}*\n"
            "━━━━━━━━━━━━━━\n"
            f"🕒 *Updated*: {timestamp}"
        )
        safe_reply(update, stats_message)
        logger.info(f"Admin {user_id} requested bot statistics: total={total_users}, premium={premium_users}, active={active_users}, banned={banned_users}")
    except Exception as e:
        logger.error(f"Error fetching bot statistics: {e}", exc_info=True)
        safe_reply(update, "😔 Error retrieving statistics 🌑.")

def set_tags(update: Update, context: CallbackContext) -> int:
    """Set user tags for matching"""
    user_id = update.effective_user.id
    user = get_user(user_id)
    profile = user.get("profile", {})
    tags_input = update.message.text.strip().lower().split(",")
    tags = [tag.strip() for tag in tags_input if tag.strip() in ALLOWED_TAGS]
    if not tags:
        safe_reply(update, " ⚠️ Invalid Tags 😕 \nEnter valid tags (e.g., music,gaming). Allowed: " + ", ".join(ALLOWED_TAGS))
        return TAGS
    if len(tags) > 5:
        safe_reply(update, " ⚠️ Too Many Tags 😕 \nYou can set up to 5 tags.")
        return TAGS
    profile["tags"] = tags
    if not update_user(user_id, {
        "profile": profile,
        "consent": user.get("consent", False),
        "verified": user.get("verified", False),
        "premium_expiry": user.get("premium_expiry"),
        "premium_features": user.get("premium_features", {}),
        "ban_type": user.get("ban_type"),
        "ban_expiry": user.get("ban_expiry"),
        "created_at": user.get("created_at", int(time.time()))
    }):
        safe_reply(update, " 😔 Error saving tags 🌑 .")
        return TAGS
    safe_reply(update, f" 🏷️ Tags set: *{', '.join(tags)}* 🌟 .")
    if user_id not in waiting_users:
        if has_premium_feature(user_id, "shine_profile"):
            waiting_users.insert(0, user_id)
        else:
            waiting_users.append(user_id)
        safe_reply(update, " 🔍 Looking for a chat partner... 🚀")
    match_users(context)
    logger.info(f"User {user_id} set tags: {tags}")
    return ConversationHandler.END

def cancel(update: Update, context: CallbackContext) -> int:
    """Cancel an operation"""
    user_id = update.effective_user.id
    safe_reply(update, " 🛑 Operation cancelled 🌟 . Use /start to begin again.")
    logger.info(f"User {user_id} cancelled the operation.")
    return ConversationHandler.END

def message_handler(update: Update, context: CallbackContext) -> None:
    user_id = update.effective_user.id
    message_text = update.message.text.strip()

    # Log the incoming message
    logger.info(f"Processing message from user {user_id}: '{message_text}'")

    # Update last activity
    user_activities[user_id] = {"last_activity": time.time()}

    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = (
            "🚫 You are permanently banned 🔒. Contact support to appeal 📧."
            if user["ban_type"] == "permanent"
            else f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')} ⏰."
        )
        safe_reply(update, ban_msg)
        logger.info(f"User {user_id} is banned. Response: {ban_msg}")
        return

    # Check if user is waiting or in a chat
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        if not partner_id or partner_id not in user_pairs:
            safe_reply(update, "❌ Your partner is no longer available 😔. Use /next to find a new one.")
            logger.info(f"User {user_id} has no valid partner. Prompted /next.")
            del user_pairs[user_id]
            return
        # Relay message to partner (existing logic)
        is_safe, reason = is_safe_message(message_text)
        if not is_safe:
            violation_result = issue_keyword_violation(user_id, reason, update, context)
            logger.warning(f"User {user_id} sent unsafe message: '{message_text}' (reason: {reason})")
            return
        display_text = message_text
        if has_premium_feature(user_id, "flare_messages"):
            display_text = f"✨ {message_text} ✨"
        try:
            safe_bot_send_message(context.bot, partner_id, display_text)
            logger.debug(f"Message relayed from {user_id} to {partner_id}: '{message_text}'")
        except telegram.error.TelegramError as e:
            safe_reply(update, "❌ Failed to send message 😔. Your partner may be offline.")
            logger.error(f"Failed to send message from {user_id} to {partner_id}: {e}")
            return
        # Save to chat history if applicable
        if has_premium_feature(user_id, "vaulted_chats"):
            chat_histories[user_id] = chat_histories.get(user_id, []) + [f"You: {message_text}"]
        if has_premium_feature(partner_id, "vaulted_chats"):
            partner_name = get_user(user_id).get("profile", {}).get("name", "Anonymous")
            chat_histories[partner_id] = chat_histories.get(partner_id, []) + [f"{partner_name}: {message_text}"]
    elif user_id in waiting_users:
        safe_reply(update, "🔍 You're currently waiting for a chat partner... Please wait! Use /next to refresh or /stop to cancel.")
        logger.info(f"User {user_id} is waiting. Prompted to wait or use /next.")
        return
    else:
        # Check if user has a complete profile
        user = get_user(user_id)
        profile = user.get("profile", {})
        required_fields = ["name", "age", "gender", "location"]
        if all(field in profile for field in required_fields):
            safe_reply(update, (
                "🎉 Your profile is ready! 🎉\n\n"
                "🔍 Use `/next` to find a chat partner and start connecting! 🚀\n"
                "ℹ️ Sending text messages now won’t start a chat. Use /help for more options."
            ))
            logger.info(f"User {user_id} has complete profile: {profile}. Prompted /next.")
        else:
            safe_reply(update, "❓ You're not in a chat or waiting 😔. Use /start to begin.")
            logger.info(f"User {user_id} has incomplete profile: {profile}. Prompted /start.")
        return
        
def cleanup_rematch_requests(context: CallbackContext) -> None:
    """Clean up expired rematch requests"""
    current_time = int(time.time())
    rematch_requests = context.bot_data.get("rematch_requests", {})
    expired_requests = [
        user_id for user_id, data in rematch_requests.items()
        if current_time - data.get("timestamp", 0) > 3600  # 1 hour expiry
    ]
    for user_id in expired_requests:
        try:
            request_data = rematch_requests.pop(user_id)
            requester_id = request_data.get("requester_id")
            message_id = request_data.get("message_id")
            if message_id:
                context.bot.delete_message(chat_id=user_id, message_id=message_id)
                logger.debug(f"Deleted expired rematch request message for user {user_id}")
            if requester_id:
                safe_bot_send_message(
                    context.bot, requester_id,
                    "⏳ Your rematch request expired as the user did not respond 😔."
                )
            logger.info(f"Cleaned up expired rematch request for user {user_id} from requester {requester_id}")
        except telegram.error.TelegramError as e:
            logger.warning(f"Failed to clean up rematch request for user {user_id}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error cleaning up rematch request for user {user_id}: {e}")

def error_handler(update: Update, context: CallbackContext) -> None:
    """Handle bot errors"""
    logger.error(f"Update {update} caused error: {context.error}")
    try:
        if update and (update.message or update.callback_query):
            safe_reply(update, " 😔 An error occurred 🌑 . Please try again later.")
    except Exception as e:
        logger.error(f"Failed to send error message: {e}")

def main() -> None:
    """Initialize and start the Telegram bot"""
    token = os.getenv("TOKEN")
    if not token:
        logger.error("TOKEN not set")
        exit(1)
    
    try:
        logger.info("Starting bot initialization")
        updater = Updater(token, use_context=True)
        dp = updater.dispatcher

        # Conversation handler and other handlers (unchanged)
        conv_handler = ConversationHandler(
            entry_points=[
                CommandHandler("start", start),
                CommandHandler("settings", settings),
            ],
            states={
                NAME: [
                    MessageHandler(Filters.text & ~Filters.command, set_name),
                    CallbackQueryHandler(button),
                ],
                AGE: [
                    MessageHandler(Filters.text & ~Filters.command, set_age),
                    CallbackQueryHandler(button),
                ],
                GENDER: [
                    CallbackQueryHandler(button),
                ],
                LOCATION: [
                    MessageHandler(Filters.text & ~Filters.command, set_location),
                    CallbackQueryHandler(button),
                ],
                CONSENT: [
                    CallbackQueryHandler(consent_handler),
                ],
                VERIFICATION: [
                    CallbackQueryHandler(verify_emoji),
                ],
                TAGS: [
                    MessageHandler(Filters.text & ~Filters.command, set_tags),
                    CallbackQueryHandler(button),
                ],
            },
            fallbacks=[
                CommandHandler("cancel", cancel),
                CallbackQueryHandler(button),
            ],
        )

        dp.add_handler(conv_handler)
        dp.add_handler(CommandHandler("stop", stop))
        dp.add_handler(CommandHandler("next", next_chat))
        dp.add_handler(CommandHandler("help", help_command))
        dp.add_handler(CommandHandler("premium", premium))
        dp.add_handler(CommandHandler("shine", shine))
        dp.add_handler(CommandHandler("instant", instant))
        dp.add_handler(CommandHandler("flare", flare))
        dp.add_handler(CommandHandler("mood", mood))
        dp.add_handler(CommandHandler("vault", vault))
        dp.add_handler(CommandHandler("history", history))
        dp.add_handler(CommandHandler("report", report))
        dp.add_handler(CommandHandler("deleteprofile", delete_profile))
        dp.add_handler(CallbackQueryHandler(button))
        dp.add_handler(PreCheckoutQueryHandler(pre_checkout))
        dp.add_handler(MessageHandler(Filters.successful_payment, successful_payment))
        dp.add_handler(MessageHandler(Filters.text & ~Filters.command, message_handler))

        # Admin commands (unchanged)
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

        dp.add_error_handler(error_handler)

        # Periodic cleanup
        logger.info("Scheduling cleanup jobs")
        updater.job_queue.run_repeating(cleanup_in_memory, interval=300, first=10)
        updater.job_queue.run_repeating(cleanup_rematch_requests, interval=60, first=10)
        logger.info("Cleanup jobs scheduled successfully")

        # Start the bot
        logger.info("Starting polling")
        updater.start_polling(allowed_updates=Update.ALL_TYPES)
        logger.info("🚀 Bot started polling 🎉")
        updater.idle()

    except Exception as e:
        logger.error(f"Failed to start bot: {str(e)}", exc_info=True)
        raise
if __name__ == "__main__":
    main()
