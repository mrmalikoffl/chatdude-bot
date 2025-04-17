from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    PreCheckoutQueryHandler,
    ConversationHandler,
    ContextTypes,
    filters,  # New import for filters
)
import logging
import os
import time
from datetime import datetime, timedelta
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from queue import Queue
from telegram.constants import ParseMode
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
INACTIVITY_TIMEOUT = 1800  # 30 minutes

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
        db.users.create_index("user_id", unique=True)
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
    return db[collection_name]

async def process_queued_operations(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Process queued database or notification operations."""
    max_retries = 3  # Maximum retries per operation
    operation_retries = {}  # Track retries per operation

    while not operation_queue.empty():
        op_type, args = operation_queue.get()
        operation_key = (op_type, str(args))  # Unique key for the operation
        retries = operation_retries.get(operation_key, 0)

        if retries >= max_retries:
            logger.error(f"Operation {op_type} with args {args} failed after {max_retries} attempts. Discarding.")
            continue

        try:
            if op_type == "update_user":
                update_user(*args)
            elif op_type == "delete_user":
                delete_user(*args)
            elif op_type == "issue_keyword_violation":
                issue_keyword_violation(*args)
            logger.info(f"Successfully processed queued operation: {op_type}")
            operation_retries.pop(operation_key, None)  # Clear retry count on success
        except Exception as e:
            logger.error(f"Failed to process queued operation {op_type}: {e}")
            operation_retries[operation_key] = retries + 1
            if operation_retries[operation_key] < max_retries:
                logger.info(f"Requeuing operation {op_type} (attempt {retries + 1}/{max_retries})")
                operation_queue.put((op_type, args))
            else:
                logger.error(f"Operation {op_type} discarded after {max_retries} attempts.")

async def cleanup_in_memory(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Clean up in-memory data like inactive user pairs."""
    logger.info(f"Cleaning up in-memory data. user_pairs: {len(user_pairs)}, waiting_users: {len(waiting_users)}")
    current_time = time.time()
    for user_id in list(user_pairs.keys()):
        last_activity = user_activities.get(user_id, {}).get("last_activity", 0)
        if current_time - last_activity > INACTIVITY_TIMEOUT:
            partner_id = user_pairs.get(user_id)
            if partner_id:
                await safe_send_message(user_id, "🛑 Chat ended due to inactivity.", context, parse_mode=ParseMode.MARKDOWN_V2)
                await safe_send_message(partner_id, "🛑 Chat ended due to inactivity.", context, parse_mode=ParseMode.MARKDOWN_V2)
                remove_pair(user_id, partner_id)
    logger.info(f"Cleanup complete. user_pairs: {len(user_pairs)}, waiting_users: {len(waiting_users)}")

def remove_pair(user_id: int, partner_id: int) -> None:
    if user_id in user_pairs:
        del user_pairs[user_id]
    if partner_id in user_pairs:
        del user_pairs[partner_id]
    logger.info(f"Removed pair: {user_id} and {partner_id}")

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
        user = users.find_one({"user_id": user_id})
    logger.debug(f"Returning user {user_id}: {user}")
    return user

def update_user(user_id: int, data: dict) -> bool:
def update_user(user_id: int, data: dict) -> bool:
    retries = 3
    for attempt in range(retries):
        try:
            users = get_db_collection("users")
            existing = users.find_one({"user_id": user_id})
            existing_data = existing or {
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
                "profile": data.get("profile", existing_data.get("profile", {})),
                "consent": data.get("consent", existing_data.get("consent", False)),
                "consent_time": data.get("consent_time", existing_data.get("consent_time", None)),
                "premium_expiry": data.get("premium_expiry", existing_data.get("premium_expiry", None)),
                "ban_type": data.get("ban_type", existing_data.get("ban_type", None)),
                "ban_expiry": data.get("ban_expiry", existing_data.get("ban_expiry", None)),
                "verified": data.get("verified", existing_data.get("verified", False)),
                "premium_features": data.get("premium_features", existing_data.get("premium_features", {})),
                "created_at": data.get("created_at", existing_data.get("created_at", int(time.time())))
            }
            users.update_one(
                {"user_id": user_id},
                {"$set": updated_data},
                upsert=True
            )
            logger.debug(f"Updated user {user_id}")
            return True
        except (ConnectionError, OperationFailure) as e:
            logger.error(f"Attempt {attempt + 1}/{retries} failed to update user {user_id}: {e}")
            if attempt < retries - 1:
                time.sleep(1)
        except Exception as e:
            logger.error(f"Unexpected error updating user {user_id}: {e}")
            break
    logger.error(f"Failed to update user {user_id} after {retries} attempts")
    return False  # Do not requeue here

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

def escape_markdown_v2(text: str) -> str:
    if not isinstance(text, str):
        return str(text) if text is not None else ""
    special_chars = r'_[]()~`>#+-=|{}.!'
    text = re.sub(r'\\(?=[{}])'.format(re.escape(special_chars)), '', text)
    return re.sub(r'([{}])'.format(re.escape(special_chars)), r'\\\1', text)

async def safe_reply(update: Update, text: str, context: ContextTypes.DEFAULT_TYPE, parse_mode: str = "MarkdownV2", **kwargs) -> None:
    try:
        if parse_mode == "MarkdownV2":
            text = escape_markdown_v2(text)
        if update.message:
            await update.message.reply_text(text, parse_mode=parse_mode, **kwargs)
        elif update.callback_query:
            await update.callback_query.message.reply_text(text, parse_mode=parse_mode, **kwargs)
    except telegram.error.BadRequest as bre:
        logger.warning(f"MarkdownV2 parsing failed: {bre}. Text: {text[:200]}")
        clean_text = re.sub(r'([_*[\]()~`>#+-|=}{.!])', '', text)
        if update.callback_query:
            await update.callback_query.message.reply_text(clean_text, parse_mode=None, **kwargs)
        elif update.message:
            await update.message.reply_text(clean_text, parse_mode=None, **kwargs)
    except Exception as e:
        logger.error(f"Failed to send message: {e}")

async def safe_send_message(chat_id: int, text: str, context: ContextTypes.DEFAULT_TYPE, parse_mode: str = "MarkdownV2", **kwargs):
    try:
        if parse_mode == "MarkdownV2":
            text = escape_markdown_v2(text)
        await context.bot.send_message(chat_id=chat_id, text=text, parse_mode=parse_mode, **kwargs)
    except telegram.error.BadRequest as e:
        logger.warning(f"MarkdownV2 parsing failed for chat {chat_id}: {e}")
        clean_text = re.sub(r'([_*[\]()~`>#+-|=}{.!])', '', text)
        await context.bot.send_message(chat_id=chat_id, text=clean_text, parse_mode=None, **kwargs)
    except Exception as e:
        logger.error(f"Failed to send message to {chat_id}: {e}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    logger.info(f"Received /start command from user {user_id}")
    
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = (
            "🚫 You are permanently banned 🔒. Contact support to appeal 📧."
            if user["ban_type"] == "permanent"
            else f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')} ⏰."
        )
        await safe_reply(update, ban_msg, context)
        return ConversationHandler.END
    
    if not check_rate_limit(user_id):
        await safe_reply(update, f"⏳ Please wait {COMMAND_COOLDOWN} seconds before trying again ⏰.", context)
        return ConversationHandler.END
    
    if user_id in user_pairs:
        await safe_reply(update, "💬 You're already in a chat 😔. Use /next to switch or /stop to end.", context)
        return ConversationHandler.END
    
    if user_id in waiting_users:
        await safe_reply(update, "🔍 You're already waiting for a chat partner... Please wait!", context)
        return ConversationHandler.END
    
    user = get_user(user_id)
    current_state = user.get("setup_state")
    if current_state is not None:
        context.user_data["state"] = current_state
        await safe_reply(update, f"Continuing setup. Please provide the requested information for {current_state}.", context)
        return current_state
    
    if not user.get("consent"):
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
        await safe_reply(update, welcome_text, context, reply_markup=reply_markup)
        await send_channel_notification(context, (
            "🆕 *New User Accessed* 🆕\n\n"
            f"👤 *User ID*: {user_id}\n"
            f"📅 *Time*: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
            "ℹ️ Awaiting consent"
        ))
        update_user(user_id, {"setup_state": CONSENT})
        context.user_data["state"] = CONSENT
        return CONSENT
    
    if not user.get("verified"):
        correct_emoji = random.choice(VERIFICATION_EMOJIS)
        context.user_data["correct_emoji"] = correct_emoji
        other_emojis = random.sample([e for e in VERIFICATION_EMOJIS if e != correct_emoji], 3)
        all_emojis = [correct_emoji] + other_emojis
        random.shuffle(all_emojis)
        keyboard = [[InlineKeyboardButton(emoji, callback_data=f"emoji_{emoji}") for emoji in all_emojis]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await safe_reply(update, f"🔐 *Verify Your Profile* 🔐\n\nPlease select this emoji: *{correct_emoji}*", context, reply_markup=reply_markup)
        update_user(user_id, {"setup_state": VERIFICATION})
        context.user_data["state"] = VERIFICATION
        return VERIFICATION
    
    profile = user.get("profile", {})
    required_fields = ["name", "age", "gender", "location"]
    missing_fields = [field for field in required_fields if not profile.get(field)]
    if missing_fields:
        await safe_reply(update, "✨ Let’s set up your profile\\! Please enter your name:", context)
        update_user(user_id, {"setup_state": NAME})
        context.user_data["state"] = NAME
        return NAME
    
    await safe_reply(update, "🎉 Your profile is ready! Use `/next` to find a chat partner and start connecting! 🚀", context)
    update_user(user_id, {"setup_state": None})
    context.user_data["state"] = None
    return ConversationHandler.END

async def consent_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    choice = query.data
    if choice == "consent_agree":
        try:
            user = get_user(user_id)
            success = update_user(user_id, {
                "consent": True,
                "consent_time": int(time.time()),
                "profile": user.get("profile", {}),
                "premium_expiry": user.get("premium_expiry"),
                "premium_features": user.get("premium_features", {}),
                "created_at": user.get("created_at", int(time.time()))
            })
            if not success:
                await safe_reply(update, "⚠️ Failed to update consent. Please try again.", context)
                return ConversationHandler.END
            await safe_reply(update, "✅ Thank you for agreeing! Let’s verify your profile.", context)
            correct_emoji = random.choice(VERIFICATION_EMOJIS)
            context.user_data["correct_emoji"] = correct_emoji
            other_emojis = random.sample([e for e in VERIFICATION_EMOJIS if e != correct_emoji], 3)
            all_emojis = [correct_emoji] + other_emojis
            random.shuffle(all_emojis)
            keyboard = [[InlineKeyboardButton(emoji, callback_data=f"emoji_{emoji}") for emoji in all_emojis]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await safe_reply(update, f"🔐 *Verify Your Profile* 🔐\n\nPlease select this emoji: *{correct_emoji}*".replace(".", "\\."), context, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2)
            return VERIFICATION
        except Exception as e:
            logger.error(f"Error in consent_handler for user {user_id}: {e}")
            await safe_reply(update, "⚠️ An error occurred. Please try again with /start.", context)
            return ConversationHandler.END
    else:
        await safe_reply(update, "❌ You must agree to the rules to use this bot. Use /start to try again.", context)
        return ConversationHandler.END

async def verify_emoji(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
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
        await safe_reply(update, "🎉 Profile verified successfully! Let’s set up your profile.", context)
        await safe_reply(update, "✨ Please enter your name:", context)
        return NAME
    else:
        correct_emoji = random.choice(VERIFICATION_EMOJIS)
        context.user_data["correct_emoji"] = correct_emoji
        other_emojis = random.sample([e for e in VERIFICATION_EMOJIS if e != correct_emoji], 3)
        all_emojis = [correct_emoji] + other_emojis
        random.shuffle(all_emojis)
        keyboard = [[InlineKeyboardButton(emoji, callback_data=f"emoji_{emoji}") for emoji in all_emojis]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await safe_reply(update, f"❌ Incorrect emoji. Try again!\nPlease select this emoji: *{correct_emoji}*", context, reply_markup=reply_markup)
        return VERIFICATION

async def set_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    user = get_user(user_id)
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
    update_user(user_id, {
        "profile": profile,
        "consent": user.get("consent", False),
        "verified": user.get("verified", False),
        "premium_expiry": user.get("premium_expiry"),
        "premium_features": user.get("premium_features", {}),
        "created_at": user.get("created_at", int(time.time())),
        "setup_state": AGE
    })
    context.user_data["state"] = AGE
    await safe_reply(update, f"🧑 Name set to: *{name}*!", context)
    await safe_reply(update, "🎂 Please enter your age (e.g., 25):", context)
    return AGE

async def set_age(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    user = get_user(user_id)
    profile = user.get("profile", {})
    age_text = update.message.text.strip()
    try:
        age = int(age_text)
        if not 13 <= age <= 120:
            await safe_reply(update, "⚠️ Age must be between 13 and 120.", context)
            return AGE
        profile["age"] = age
        update_user(user_id, {
            "profile": profile,
            "consent": user.get("consent", False),
            "verified": user.get("verified", False),
            "premium_expiry": user.get("premium_expiry"),
            "premium_features": user.get("premium_features", {}),
            "created_at": user.get("created_at", int(time.time())),
            "setup_state": GENDER
        })
        context.user_data["state"] = GENDER
        await safe_reply(update, f"🎂 Age set to: *{age}*!", context)
        keyboard = [
            [
                InlineKeyboardButton("👨 Male", callback_data="gender_male"),
                InlineKeyboardButton("👩 Female", callback_data="gender_female"),
                InlineKeyboardButton("🌈 Other", callback_data="gender_other")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await safe_reply(update, "👤 *Set Your Gender* 👤\n\nChoose your gender below:", context, reply_markup=reply_markup)
        return GENDER
    except ValueError:
        await safe_reply(update, "⚠️ Please enter a valid number for your age.", context)
        return AGE

async def set_gender(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    user = get_user(user_id)
    profile = user.get("profile", {})
    if data.startswith("gender_"):
        gender = data.split("_")[1].capitalize()
        if gender not in ["Male", "Female", "Other"]:
            await safe_reply(update, "⚠️ Invalid gender selection.", context)
            return GENDER
        profile["gender"] = gender
        update_user(user_id, {
            "profile": profile,
            "consent": user.get("consent", False),
            "verified": user.get("verified", False),
            "premium_expiry": user.get("premium_expiry"),
            "premium_features": user.get("premium_features", {}),
            "created_at": user.get("created_at", int(time.time())),
            "setup_state": LOCATION
        })
        context.user_data["state"] = LOCATION
        await safe_reply(update, f"👤 Gender set to: *{gender}*!", context)
        await safe_reply(update, "📍 Please enter your location (e.g., New York):", context)
        return LOCATION
    await safe_reply(update, "⚠️ Invalid selection. Please choose a gender.", context)
    return GENDER

async def set_location(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = update.effective_user.id
    user = get_user(user_id)
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
    update_user(user_id, {
        "profile": profile,
        "consent": user.get("consent", False),
        "verified": user.get("verified", False),
        "premium_expiry": user.get("premium_expiry"),
        "premium_features": user.get("premium_features", {}),
        "created_at": user.get("created_at", int(time.time())),
        "setup_state": None
    })
    context.user_data["state"] = None
    await safe_reply(update, "🎉 Congratulations! Profile setup complete! 🎉", context)
    await safe_reply(update, (
        "🔍 Your profile is ready! 🎉\n\n"
        "🚀 Use `/next` to find a chat partner and start connecting!\n"
        "ℹ️ Sending text messages now won’t start a chat. Use /help for more options."
    ), context)
    notification_message = (
        "🆕 *New User Registered* 🆕\n\n"
        f"👤 *User ID*: {user_id}\n"
        f"🧑 *Name*: {profile.get('name', 'Not set')}\n"
        f"🎂 *Age*: {profile.get('age', 'Not set')}\n"
        f"👤 *Gender*: {profile.get('gender', 'Not set')}\n"
        f"📍 *Location*: {location}\n"
        f"📅 *Joined*: {datetime.fromtimestamp(user.get('created_at', int(time.time()))).strftime('%Y-%m-%d %H:%M:%S')}"
    )
    await send_channel_notification(context, notification_message)
    return ConversationHandler.END

async def match_users(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Match waiting users for chats."""
    with waiting_users_lock:
        if len(waiting_users) < 2:
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
                    await safe_send_message(user1, user1_message, context)
                    await safe_send_message(user2, user2_message, context)
                    if has_premium_feature(user1, "vaulted_chats"):
                        chat_histories[user1] = []
                    if has_premium_feature(user2, "vaulted_chats"):
                        chat_histories[user2] = []
                    return

# Initialize MongoDB
try:
    db = init_mongodb()
except Exception as e:
    logger.error(f"Failed to set up MongoDB: {e}")
    exit(1)

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

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        await safe_reply(update, ban_msg, context)
        return
    if user_id in waiting_users:
        waiting_users.remove(user_id)
        await safe_reply(update, "⏹️ You’ve stopped waiting for a chat partner. Use /start to begin again.", context)
        return
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        del user_pairs[user_id]
        if partner_id in user_pairs:
            del user_pairs[partner_id]
        await safe_send_message(partner_id, "👋 Your partner has left the chat. Use /start to find a new one.", context)
        await safe_reply(update, "👋 Chat ended. Use /start to begin a new chat.", context)
        if user_id in chat_histories and not has_premium_feature(user_id, "vaulted_chats"):
            del chat_histories[user_id]
    else:
        await safe_reply(update, "❓ You're not in a chat or waiting. Use /start to find a partner.", context)

async def next_chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        await safe_reply(update, ban_msg, context)
        return
    if not check_rate_limit(user_id):
        await safe_reply(update, f"⏳ Please wait {COMMAND_COOLDOWN} seconds before trying again.", context)
        return
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        del user_pairs[user_id]
        del user_pairs[partner_id]
        await safe_send_message(partner_id, "🔌 Your chat partner disconnected.", context)
    if user_id not in waiting_users:
        if has_premium_feature(user_id, "shine_profile"):
            waiting_users.insert(0, user_id)
        else:
            waiting_users.append(user_id)
    await safe_reply(update, "🔍 Looking for a new chat partner...", context)
    await match_users(context)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        await safe_reply(update, ban_msg, context)
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
    await safe_reply(update, help_text, context, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2)

async def premium(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        await safe_reply(update, ban_msg, context)
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
    await safe_reply(update, message_text, context, reply_markup=reply_markup)

async def buy_premium(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        await safe_reply(update, ban_msg, context)
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
        await safe_reply(update, "❌ Invalid feature selected.", context)
        return
    title, stars, desc, feature_key = feature_map[choice]
    try:
        await context.bot.send_invoice(
            chat_id=user_id,
            title=title,
            description=desc,
            payload=f"{feature_key}_{user_id}",
            currency="XTR",
            prices=[LabeledPrice(title, stars)],
            start_parameter=f"buy-{feature_key}",
            provider_token=None
        )
    except Exception as e:
        await safe_reply(update, "❌ Error generating payment invoice.", context)

async def pre_checkout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.pre_checkout_query
    if query.currency != "XTR":
        await context.bot.answer_pre_checkout_query(
            query.id, ok=False, error_message="Only Telegram Stars payments are supported."
        )
        return
    valid_payloads = [f"{key}_{query.from_user.id}" for key in ["flare_messages", "instant_rematch", "shine_profile", "mood_match", "partner_details", "vaulted_chats", "premium_pass"]]
    if query.invoice_payload in valid_payloads:
        await context.bot.answer_pre_checkout_query(query.id, ok=True)
    else:
        await context.bot.answer_pre_checkout_query(
            query.id, ok=False, error_message="Invalid purchase payload."
        )

async def successful_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    payment = update.message.successful_payment
    if payment.currency != "XTR":
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
                await safe_reply(update, "❌ Error processing your purchase. Please contact support.", context)
                return
            await safe_reply(update, message, context)
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
            await send_channel_notification(context, notification_message)
            break

async def shine(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
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

async def instant(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Attempt an instant rematch with a previous partner using premium feature."""
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = (
            "🚫 You are permanently banned. Contact support to appeal."
            if user["ban_type"] == "permanent"
            else f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        )
        await safe_reply(update, ban_msg, context)
        return
    if not has_premium_feature(user_id, "instant_rematch"):
        await safe_reply(update, "🔄 *Instant Rematch* is a premium feature. Buy it with /premium!", context, parse_mode=ParseMode.MARKDOWN_V2)
        return
    user = get_user(user_id)
    features = user.get("premium_features", {})
    rematch_count = features.get("instant_rematch_count", 0)
    if rematch_count <= 0:
        await safe_reply(update, "🔄 You need an *Instant Rematch*! Buy one with /premium!", context, parse_mode=ParseMode.MARKDOWN_V2)
        return
    partners = user.get("profile", {}).get("past_partners", [])
    if not partners:
        await safe_reply(update, "❌ No past partners to rematch with.", context, parse_mode=ParseMode.MARKDOWN_V2)
        return
    partner_id = partners[-1]
    partner_data = get_user(partner_id)
    if not partner_data:
        await safe_reply(update, "❌ Your previous partner is no longer available.", context, parse_mode=ParseMode.MARKDOWN_V2)
        return
    if user_id in user_pairs:
        await safe_reply(update, "❓ You're already in a chat. Use /stop to end it first.", context, parse_mode=ParseMode.MARKDOWN_V2)
        return
    if partner_id in user_pairs:
        await safe_reply(update, "❌ Your previous partner is currently in another chat.", context, parse_mode=ParseMode.MARKDOWN_V2)
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
            "created_at": user.get("created_at", int(time.time())),
            "ban_type": user.get("ban_type"),
            "ban_expiry": user.get("ban_expiry")
        })
        await safe_reply(update, "🔄 *Instantly reconnected!* Start chatting! 🗣️", context, parse_mode=ParseMode.MARKDOWN_V2)
        await safe_send_message(partner_id, "🔄 *Instantly reconnected!* Start chatting! 🗣️", context)
        if has_premium_feature(user_id, "vaulted_chats"):
            chat_histories[user_id] = chat_histories.get(user_id, [])
        if has_premium_feature(partner_id, "vaulted_chats"):
            chat_histories[partner_id] = chat_histories.get(partner_id, [])
        logger.info(f"Instant rematch: user {user_id} with {partner_id}")
        notification_message = (
            f"🔄 *Instant Rematch* 🔄\n\n"
            f"👤 *User ID*: {user_id}\n"
            f"🤝 *Partner ID*: {partner_id}\n"
            f"🕒 *Rematched At*: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        await send_channel_notification(context, notification_message)
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
        message = await context.bot.send_message(
            chat_id=partner_id,
            text=request_message,  # Removed escape_markdown_v2 since it's applied in safe_reply
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_markup=reply_markup
        )
        await safe_reply(update, "📩 Rematch request sent to your previous partner. Waiting for their response...", context, parse_mode=ParseMode.MARKDOWN_V2)
        context.bot_data["rematch_requests"] = context.bot_data.get("rematch_requests", {})
        context.bot_data["rematch_requests"][partner_id] = {
            "requester_id": user_id,
            "timestamp": int(time.time()),
            "message_id": message.message_id
        }
    except telegram.error.TelegramError as e:
        await safe_reply(update, "❌ Unable to reach your previous partner. They may be offline.", context, parse_mode=ParseMode.MARKDOWN_V2)
        logger.warning(f"Failed to send rematch request to {partner_id}: {e}")

async def mood(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned. Contact support to appeal." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')}."
        await safe_reply(update, ban_msg, context)
        return
    if not has_premium_feature(user_id, "mood_match"):
        await safe_reply(update, "😊 *Mood Match* is a premium feature. Buy it with /premium!", context)
        return
    keyboard = [
        [InlineKeyboardButton("😎 Chill", callback_data="mood_chill"),
         InlineKeyboardButton("🤔 Deep", callback_data="mood_deep")],
        [InlineKeyboardButton("😂 Fun", callback_data="mood_fun"),
         InlineKeyboardButton("❌ Clear Mood", callback_data="mood_clear")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await safe_reply(update, "🎭 Choose your chat mood:", context, reply_markup=reply_markup)

async def set_mood(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    if not has_premium_feature(user_id, "mood_match"):
        await safe_reply(update, "😊 *Mood Match* is a premium feature. Buy it with /premium!", context)
        return
    choice = query.data
    user = get_user(user_id)
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

async def vault(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
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

async def history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
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

async def rematch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
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
    user = get_user(user_id)
    partners = user.get("profile", {}).get("past_partners", [])
    if not partners:
        await safe_reply(update, "❌ No past partners to rematch with 😔.", context)
        return
    keyboard = []
    for partner_id in partners[-5:]:
        partner_data = get_user(partner_id)
        if partner_data:
            partner_name = partner_data.get("profile", {}).get("name", "Anonymous")
            keyboard.append([InlineKeyboardButton(f"Reconnect with {partner_name}", callback_data=f"rematch_request_{partner_id}")])
    if not keyboard:
        await safe_reply(update, "❌ No available past partners to rematch with 😔.", context)
        return
    reply_markup = InlineKeyboardMarkup(keyboard)
    await safe_reply(update, "🔄 *Choose a Past Partner to Rematch* 🔄", context, reply_markup=reply_markup)

async def flare(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned 🔒. Contact support to appeal 📧." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')} ⏰."
        await safe_reply(update, ban_msg, context)
        return
    if not has_premium_feature(user_id, "flare_messages"):
        await safe_reply(update, "🌟 *Flare Messages* is a premium feature. Buy it with /premium! 🌟", context)
        return
    await safe_reply(update, "✨ Your messages are sparkling with *Flare*! Keep chatting to show it off! 🌟", context)

async def button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
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
    elif data.startswith("rematch_request_"):
        partner_id = int(data.split("_")[-1])
        if is_banned(user_id):
            await safe_reply(update, "🚫 You are banned and cannot send rematch requests 🔒.", context)
            return
        user = get_user(user_id)
        if user_id in user_pairs:
            await safe_reply(update, "❓ You're already in a chat 😔. Use /stop to end it first.", context)
            return
        partner_data = get_user(partner_id)
        if not partner_data:
            await safe_reply(update, "❌ This user is no longer available 😓.", context)
            return
        if partner_id in user_pairs:
            await safe_reply(update, "❌ This user is currently in another chat 💬.", context)
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
            message = await context.bot.send_message(
                chat_id=partner_id,
                text=escape_markdown_v2(request_message),
                parse_mode="MarkdownV2",
                reply_markup=reply_markup
            )
            await safe_reply(update, "📩 Rematch request sent. Waiting for their response...", context)
            context.bot_data["rematch_requests"] = context.bot_data.get("rematch_requests", {})
            context.bot_data["rematch_requests"][partner_id] = {
                "requester_id": user_id,
                "timestamp": int(time.time()),
                "message_id": message.message_id
            }
        except telegram.error.TelegramError as e:
            await safe_reply(update, "❌ Unable to reach this user. They may be offline.", context)
    elif data.startswith("rematch_accept_"):
        requester_id = int(data.split("_")[-1])
        if user_id in user_pairs:
            await safe_reply(update, "❓ You're already in a chat 😔. Use /stop to end it first.", context)
            return
        requester_data = get_user(requester_id)
        if not requester_data:
            await safe_reply(update, "❌ This user is no longer available 😓.", context)
            return
        if requester_id in user_pairs:
            await safe_reply(update, "❌ This user is currently in another chat 💬.", context)
            return
        user_pairs[user_id] = requester_id
        user_pairs[requester_id] = user_id
        await safe_reply(update, "🔄 *Reconnected!* Start chatting! 🗣️", context)
        await safe_send_message(requester_id, "🔄 *Reconnected!* Start chatting! 🗣️", context)
        if has_premium_feature(user_id, "vaulted_chats"):
            chat_histories[user_id] = chat_histories.get(user_id, [])
        if has_premium_feature(requester_id, "vaulted_chats"):
            chat_histories[requester_id] = chat_histories.get(requester_id, [])
        context.bot_data.get("rematch_requests", {}).pop(user_id, None)
    elif data == "rematch_decline":
        await safe_reply(update, "❌ Rematch request declined.", context)
        requester_data = context.bot_data.get("rematch_requests", {}).get(user_id, {})
        requester_id = requester_data.get("requester_id")
        if requester_id:
            await safe_send_message(requester_id, "❌ Your rematch request was declined 😔.", context)
        context.bot_data.get("rematch_requests", {}).pop(user_id, None)
    elif data.startswith("emoji_"):
        await verify_emoji(update, context)
    elif data.startswith("consent_"):
        await consent_handler(update, context)
    elif data.startswith("gender_"):
        await set_gender(update, context)

async def settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
        ban_msg = "🚫 You are permanently banned 🔒. Contact support to appeal 📧." if user["ban_type"] == "permanent" else \
                  f"🚫 You are banned until {datetime.fromtimestamp(user['ban_expiry']).strftime('%Y-%m-%d %H:%M')} ⏰."
        await safe_reply(update, ban_msg, context)
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
    await safe_reply(update, settings_text, context, reply_markup=reply_markup)

async def report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
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
            await safe_send_message(partner_id,
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

async def delete_profile(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        del user_pairs[user_id]
        if partner_id in user_pairs:
            del user_pairs[partner_id]
        await safe_send_message(partner_id, "👋 Your partner has left the chat. Use /start to find a new one.", context)
    if user_id in waiting_users:
        waiting_users.remove(user_id)
    try:
        delete_user(user_id)
        await safe_reply(update, "🗑️ Your profile and data have been deleted successfully 🌟.", context)
        notification_message = (
            "🗑️ *User Deleted Profile* 🗑️\n\n"
            f"👤 *User ID*: {user_id}\n"
            f"🕒 *Deleted At*: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        await send_channel_notification(context, notification_message)
        if user_id in chat_histories:
            del chat_histories[user_id]
    except Exception as e:
        logger.error(f"Error deleting profile for user {user_id}: {e}")
        await safe_reply(update, "❌ Error deleting profile 😔. Please try again or contact support.", context)

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if is_banned(user_id):
        user = get_user(user_id)
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
    user_activities[user_id] = {"last_activity": time.time()}
    user_activities[partner_id] = {"last_activity": time.time()}
    formatted_message = message
    if has_premium_feature(user_id, "flare_messages"):
        formatted_message = f"✨ {message} ✨"
    await safe_send_message(partner_id, formatted_message, context)
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

async def issue_keyword_violation(user_id: int, message: str, reason: str, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        violations = get_db_collection("keyword_violations")
        user = get_user(user_id)
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
            await safe_send_message(user_id, ban_message, context)
            if user_id in user_pairs:
                partner_id = user_pairs[user_id]
                await safe_send_message(partner_id, "👋 Your partner has left the chat. Use /start to find a new one.", context)
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

def is_banned(user_id: int) -> bool:
    user = get_user(user_id)
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
    user = get_user(user_id)
    features = user.get("premium_features", {})
    current_time = int(time.time())
    if feature == "instant_rematch":
        return features.get("instant_rematch_count", 0) > 0
    if feature == "vaulted_chats":
        return features.get("vaulted_chats", False)
    expiry = features.get(feature)
    return expiry and expiry > current_time

async def send_channel_notification(context: ContextTypes.DEFAULT_TYPE, message: str) -> None:
    try:
        await safe_send_message(NOTIFICATION_CHANNEL_ID, message, context)
    except Exception as e:
        logger.error(f"Failed to send notification to channel {NOTIFICATION_CHANNEL_ID}: {e}")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Update {update} caused error: {context.error}")
    if update and (update.message or update.callback_query):
        await safe_reply(update, "❌ An error occurred 😔. Please try again or contact support.", context)
    notification_message = (
        f"⚠️ *Bot Error* ⚠️\n\n"
        f"📜 *Error*: {str(context.error)[:100]}\n"
        f"🕒 *Occurred At*: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    await send_channel_notification(context, notification_message)

async def set_tags(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle user input for setting profile tags."""
    user_id = update.effective_user.id
    user = get_user(user_id)
    current_state = context.user_data.get("state", user.get("setup_state"))
    
    if current_state != TAGS:
        await safe_reply(update, "⚠️ Please complete the previous steps. Use /start to begin.", context, parse_mode=ParseMode.MARKDOWN_V2)
        return ConversationHandler.END
    
    profile = user.get("profile", {})
    tags_input = update.message.text.strip().lower().split(",")
    tags_input = [tag.strip() for tag in tags_input if tag.strip()]
    
    # Define allowed tags (adjust as needed)
    ALLOWED_TAGS = [
        "gaming", "music", "movies", "sports", "books", "tech", "food", "travel",
        "art", "fitness", "nature", "photography", "fashion", "coding", "anime"
    ]
    
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
    update_user(user_id, {
        "profile": profile,
        "consent": user.get("consent", False),
        "verified": user.get("verified", False),
        "premium_expiry": user.get("premium_expiry"),
        "premium_features": user.get("premium_features", {}),
        "created_at": user.get("created_at", int(time.time())),
        "ban_type": user.get("ban_type"),
        "ban_expiry": user.get("ban_expiry"),
        "setup_state": None
    })
    
    context.user_data["state"] = None
    await safe_reply(update, f"🏷️ Tags set: {', '.join(tags_input)} 🎉", context, parse_mode=ParseMode.MARKDOWN_V2)
    await safe_reply(
        update,
        (
            "🔍 Your profile is ready! 🎉\n\n"
            "🚀 Use `/next` to find a chat partner and start connecting!\n"
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
    await safe_reply(update, access_text, context, parse_mode=ParseMode.MARKDOWN_V2)

async def admin_delete(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Delete a user's data"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await safe_reply(update, "🔒 Unauthorized 🌑.", context)
        return
    try:
        target_id = int(context.args[0])
        delete_user(target_id)
        await safe_reply(update, f"🗑️ User *{target_id}* data deleted successfully 🌟.", context, parse_mode=ParseMode.MARKDOWN_V2)
        logger.info(f"Admin {user_id} deleted user {target_id}.")
        notification_message = (
            f"🗑️ *User Deleted* 🗑️\n\n"
            f"👤 *User ID*: {target_id}\n"
            f"🕒 *Deleted At*: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        await send_channel_notification(context, notification_message)
    except (IndexError, ValueError):
        await safe_reply(update, "⚠️ Usage: /admin_delete <user_id> 📋.", context, parse_mode=ParseMode.MARKDOWN_V2)

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
        user = get_user(target_id)
        if not user:
            await safe_reply(update, "😕 User not found 🌑.", context, parse_mode=ParseMode.MARKDOWN_V2)
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
        await safe_reply(update, f"🎁 Premium granted to user *{target_id}* for *{days}* days 🌟.", context, parse_mode=ParseMode.MARKDOWN_V2)
        await safe_send_message(target_id, f"🎉 You've been granted Premium status for {days} days!", context)
        logger.info(f"Admin {user_id} granted premium to {target_id} for {days} days.")
        notification_message = (
            f"🎁 *Premium Granted* 🎁\n\n"
            f"👤 *User ID*: {target_id}\n"
            f"📅 *Days*: {days}\n"
            f"🕒 *Expiry*: {expiry_date}\n"
            f"🕒 *Granted At*: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        await send_channel_notification(context, notification_message)
    except (IndexError, ValueError) as e:
        logger.error(f"Error in admin_premium for user {target_id}: {e}")
        await safe_reply(update, "⚠️ Usage: /admin_premium <user_id> <days> 📋.", context, parse_mode=ParseMode.MARKDOWN_V2)

async def admin_revoke_premium(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Revoke premium status from a user"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await safe_reply(update, "🔒 Unauthorized 🌑.", context)
        return
    try:
        target_id = int(context.args[0])
        user = get_user(target_id)
        if not user:
            await safe_reply(update, "😕 User not found 🌑.", context, parse_mode=ParseMode.MARKDOWN_V2)
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
        await safe_reply(update, f"❌ Premium status revoked for user *{target_id}* 🌑.", context, parse_mode=ParseMode.MARKDOWN_V2)
        await safe_send_message(target_id, "😔 Your Premium status has been revoked.", context)
        updated_user = get_user(target_id)
        logger.info(f"Admin {user_id} revoked premium for {target_id}.")
        logger.debug(f"User {target_id} after revoke_premium: premium_expiry={updated_user.get('premium_expiry')}, premium_features={updated_user.get('premium_features')}")
        notification_message = (
            f"❌ *Premium Revoked* ❌\n\n"
            f"👤 *User ID*: {target_id}\n"
            f"🕒 *Revoked At*: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        await send_channel_notification(context, notification_message)
    except (IndexError, ValueError):
        await safe_reply(update, "⚠️ Usage: /admin_revoke_premium <user_id> 📋.", context, parse_mode=ParseMode.MARKDOWN_V2)

async def admin_ban(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Ban a user"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await safe_reply(update, "🔒 Unauthorized 🌑.", context)
        return
    try:
        target_id = int(context.args[0])
        ban_type = context.args[1].lower()
        user = get_user(target_id)
        if not user:
            await safe_reply(update, "😕 User not found 🌑.", context, parse_mode=ParseMode.MARKDOWN_V2)
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
            await safe_send_message(partner_id, "😔 Your partner has left the chat.", context)
        if target_id in waiting_users:
            waiting_users.remove(target_id)
        await safe_reply(update, f"🚫 User *{target_id}* has been {ban_type} banned 🌑.", context, parse_mode=ParseMode.MARKDOWN_V2)
        await safe_send_message(target_id, f"🚫 You have been {ban_type} banned from Talk2Anyone.", context)
        updated_user = get_user(target_id)
        logger.info(f"Admin {user_id} banned user {target_id} ({ban_type}).")
        logger.debug(f"User {target_id} after admin_ban: premium_expiry={updated_user.get('premium_expiry')}, premium_features={updated_user.get('premium_features')}")
        notification_message = (
            f"🚨 *User Banned* 🚨\n\n"
            f"👤 *User ID*: {target_id}\n"
            f"📅 *Ban Type*: {ban_type.capitalize()}\n"
            f"🕒 *Ban Expiry*: {'No expiry' if ban_type == 'permanent' else datetime.fromtimestamp(ban_expiry).strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"🕒 *Banned At*: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        await send_channel_notification(context, notification_message)
    except (IndexError, ValueError):
        await safe_reply(update, "⚠️ Usage: /admin_ban <user_id> <days/permanent> 📋.", context, parse_mode=ParseMode.MARKDOWN_V2)

async def admin_unban(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Unban a user"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await safe_reply(update, "🔒 Unauthorized 🌑.", context)
        return
    try:
        target_id = int(context.args[0])
        user = get_user(target_id)
        if not user:
            await safe_reply(update, "😕 User not found 🌑.", context, parse_mode=ParseMode.MARKDOWN_V2)
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
        await safe_reply(update, f"🔓 User *{target_id}* has been unbanned 🌟.", context, parse_mode=ParseMode.MARKDOWN_V2)
        await safe_send_message(target_id, "🎉 You have been unbanned. Use /start to begin.", context)
        updated_user = get_user(target_id)
        logger.info(f"Admin {user_id} unbanned user {target_id}.")
        logger.debug(f"User {target_id} after admin_unban: premium_expiry={updated_user.get('premium_expiry')}, premium_features={updated_user.get('premium_features')}")
        notification_message = (
            f"✅ *User Unbanned* ✅\n\n"
            f"👤 *User ID*: {target_id}\n"
            f"🕒 *Unbanned At*: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        await send_channel_notification(context, notification_message)
    except (IndexError, ValueError):
        await safe_reply(update, "⚠️ Usage: /admin_unban <user_id> 📋.", context, parse_mode=ParseMode.MARKDOWN_V2)

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
            await safe_reply(update, "✅ No recent keyword violations 🌟.", context, parse_mode=ParseMode.MARKDOWN_V2)
            return
        violation_text = "⚠️ *Recent Keyword Violations* ⚠️\n\n"
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
                f"👤 User ID: *{user_id}*\n"
                f"📉 Violations: *{count}*\n"
                f"🔍 Keyword: *{keyword}*\n"
                f"🕒 Last: *{last_violation}*\n"
                f"🚫 Ban: *{ban_status}*\n"
                "━━━━━━━━━━━━━━\n"
            )
        await safe_reply(update, violation_text, context, parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Error fetching violations: {e}")
        await safe_reply(update, "😔 Error fetching violations 🌑.", context, parse_mode=ParseMode.MARKDOWN_V2)

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
            await safe_reply(update, "😕 No users found 🌑.", context, parse_mode=ParseMode.MARKDOWN_V2)
            logger.info(f"Admin {user_id} requested users list: no users found.")
            return
        message = "📋 *All Users List* \\(Sorted by ID\\) 📋\n\n"
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
            name = profile.get("name", "Not set")
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
                await safe_reply(update, message, context, parse_mode=ParseMode.MARKDOWN_V2)
                message = ""
                logger.debug(f"Sent partial users list for admin {user_id}, users so far: {user_count}")
        if message.strip():
            message += f"📊 *Total Users*: {user_count}\n"
            await safe_reply(update, message, context, parse_mode=ParseMode.MARKDOWN_V2)
        logger.info(f"Admin {user_id} requested users list with {user_count} users")
        logger.debug(f"Users list sent with {user_count} users.")
    except Exception as e:
        logger.error(f"Error fetching users list for admin {user_id}: {e}", exc_info=True)
        await safe_reply(update, "😔 Error retrieving users list 🌑.", context, parse_mode=ParseMode.MARKDOWN_V2)

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
            await safe_reply(update, "😕 No premium users found 🌑.", context, parse_mode=ParseMode.MARKDOWN_V2)
            return
        message = "💎 *Premium Users List* \\(Sorted by Expiry\\) 💎\n\n"
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
            message += (
                f"👤 *User ID*: {user_id}\n"
                f"🧑 *Name*: {profile.get('name', 'Not set')}\n"
                f"⏰ *Premium Until*: {expiry_date}\n"
                f"✨ *Features*: {features_str}\n"
                "━━━━━━━━━━━━━━\n"
            )
            user_count += 1
            if len(message.encode('utf-8')) > 4000:
                await safe_reply(update, message, context, parse_mode=ParseMode.MARKDOWN_V2)
                message = ""
        if message:
            message += f"📊 *Total Premium Users*: {user_count}\n"
            await safe_reply(update, message, context, parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Error fetching premium users list: {e}")
        await safe_reply(update, "😔 Error retrieving premium users list 🌑.", context, parse_mode=ParseMode.MARKDOWN_V2)

async def admin_info(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display detailed user information"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await safe_reply(update, "🔒 Unauthorized 🌑.", context)
        return
    try:
        target_id = int(context.args[0])
        user = get_user(target_id)
        if not user:
            await safe_reply(update, "😕 User not found 🌑.", context, parse_mode=ParseMode.MARKDOWN_V2)
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
            f"🧑 *Name*: {profile.get('name', 'Not set')}\n"
            f"🎂 *Age*: {profile.get('age', 'Not set')}\n"
            f"👤 *Gender*: {profile.get('gender', 'Not set')}\n"
            f"📍 *Location*: {profile.get('location', 'Not set')}\n"
            f"🏷️ *Tags*: {', '.join(profile.get('tags', [])) or 'None'}\n"
            f"🤝 *Consent*: {consent}\n"
            f"✅ *Verified*: {verified}\n"
            f"💎 *Premium*: {premium_status}\n"
            f"✨ *Features*: {features}\n"
            f"🚫 *Ban*: {ban_info}\n"
            f"⚠️ *Keyword Violations*: {violation_status}\n"
            f"📅 *Joined*: {created_at}"
        )
        await safe_reply(update, message, context, parse_mode=ParseMode.MARKDOWN_V2)
    except (IndexError, ValueError):
        await safe_reply(update, "⚠️ Usage: /admin_info <user_id> 📋.", context, parse_mode=ParseMode.MARKDOWN_V2)

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
            await safe_reply(update, "✅ No reports found 🌟.", context, parse_mode=ParseMode.MARKDOWN_V2)
            return
        message = "🚨 *Reported Users* \\(Top 20\\) 🚨\n\n"
        for report in reports_list:
            reported_id = report["_id"]
            count = report["count"]
            message += f"👤 {reported_id} | Reports: *{count}*\n"
        await safe_reply(update, message, context, parse_mode=ParseMode.MARKDOWN_V2)
    except Exception as e:
        logger.error(f"Failed to list reports: {e}")
        await safe_reply(update, "😔 Error retrieving reports 🌑.", context, parse_mode=ParseMode.MARKDOWN_V2)

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
        await safe_reply(update, f"🧹 Reports cleared for user *{target_id}* 🌟.", context, parse_mode=ParseMode.MARKDOWN_V2)
        logger.info(f"Admin {user_id} cleared reports for {target_id}.")
        notification_message = (
            f"🧹 *Reports Cleared* 🧹\n\n"
            f"👤 *User ID*: {target_id}\n"
            f"🕒 *Cleared At*: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        await send_channel_notification(context, notification_message)
    except (IndexError, ValueError):
        await safe_reply(update, "⚠️ Usage: /admin_clear_reports <user_id> 📋.", context, parse_mode=ParseMode.MARKDOWN_V2)

async def admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Broadcast a message to all users"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await safe_reply(update, "🔒 Unauthorized 🌑.", context)
        return
    if not context.args:
        await safe_reply(update, "⚠️ Usage: /admin_broadcast <message> 📋.", context, parse_mode=ParseMode.MARKDOWN_V2)
        return
    message = "📣 *Announcement*: " + " ".join(context.args)
    try:
        users = get_db_collection("users")
        users_list = users.find({"consent": True})
        sent_count = 0
        for user in users_list:
            try:
                await safe_send_message(user["user_id"], message, context)
                sent_count += 1
                time.sleep(0.05)  # Avoid rate limits
            except Exception as e:
                logger.warning(f"Failed to send broadcast to {user['user_id']}: {e}")
        await safe_reply(update, f"📢 Broadcast sent to *{sent_count}* users 🌟.", context, parse_mode=ParseMode.MARKDOWN_V2)
        logger.info(f"Admin {user_id} sent broadcast to {sent_count} users.")
        notification_message = (
            f"📢 *Broadcast Sent* 📢\n\n"
            f"📩 *Message*: {message}\n"
            f"👥 *Sent to*: {sent_count} users\n"
            f"🕒 *Sent At*: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        await send_channel_notification(context, notification_message)
    except Exception as e:
        logger.error(f"Failed to send broadcast: {e}")
        await safe_reply(update, "😔 Error sending broadcast 🌑.", context, parse_mode=ParseMode.MARKDOWN_V2)

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
            f"💬 *Active Users*: *{active_users}* \\(in chats or waiting\\)\n"
            f"🚫 *Banned Users*: *{banned_users}*\n"
            "━━━━━━━━━━━━━━\n"
            f"🕒 *Updated*: {timestamp}"
        )
        await safe_reply(update, stats_message, context, parse_mode=ParseMode.MARKDOWN_V2)
        logger.info(f"Admin {user_id} requested bot statistics: total={total_users}, premium={premium_users}, active={active_users}, banned={banned_users}")
    except Exception as e:
        logger.error(f"Error fetching bot statistics: {e}", exc_info=True)
        await safe_reply(update, "😔 Error retrieving statistics 🌑.", context, parse_mode=ParseMode.MARKDOWN_V2)

def main() -> None:
    """Initialize and run the Telegram bot."""
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
    
    # Add all handlers
    application.add_handler(conv_handler)
    application.add_handler(CommandHandler("stop", stop))
    application.add_handler(CommandHandler("next", next_chat))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("premium", premium))
    application.add_handler(CommandHandler("shine", shine))
    application.add_handler(CommandHandler("instant", instant))
    application.add_handler(CommandHandler("mood", mood))
    application.add_handler(CommandHandler("vault", vault))
    application.add_handler(CommandHandler("history", history))
    application.add_handler(CommandHandler("rematch", rematch))
    application.add_handler(CommandHandler("flare", flare))
    application.add_handler(CommandHandler("settings", settings))
    application.add_handler(CommandHandler("report", report))
    application.add_handler(CommandHandler("deleteprofile", delete_profile))
    
    # Add admin command handlers
    application.add_handler(CommandHandler("admin", admin_access))
    application.add_handler(CommandHandler("admin_userslist", admin_userslist))
    application.add_handler(CommandHandler("admin_premiumuserslist", admin_premiumuserslist))
    application.add_handler(CommandHandler("admin_info", admin_info))
    application.add_handler(CommandHandler("admin_delete", admin_delete))
    application.add_handler(CommandHandler("admin_premium", admin_premium))
    application.add_handler(CommandHandler("admin_revoke_premium", admin_revoke_premium))
    application.add_handler(CommandHandler("admin_ban", admin_ban))
    application.add_handler(CommandHandler("admin_unban", admin_unban))
    application.add_handler(CommandHandler("admin_violations", admin_violations))
    application.add_handler(CommandHandler("admin_reports", admin_reports))
    application.add_handler(CommandHandler("admin_clear_reports", admin_clear_reports))
    application.add_handler(CommandHandler("admin_stats", admin_stats))
    application.add_handler(CommandHandler("admin_broadcast", admin_broadcast))
    
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
            application.job_queue.run_repeating(match_users, interval=10, first=5)
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
