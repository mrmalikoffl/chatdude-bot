from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Updater,
    CommandHandler,
    MessageHandler,
    Filters,
    CallbackContext,
    ConversationHandler,
    CallbackQueryHandler,
)
import logging
import os
import time
from datetime import datetime
import psycopg2
from urllib.parse import urlparse

# Set up logging for Heroku
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO if os.getenv("DEBUG_MODE") != "true" else logging.DEBUG,
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Admin configuration
ADMIN_IDS = {5975525252}  # Replace with your Telegram user ID(s)

# Security configurations
BANNED_WORDS = {
    "spam", "hate", "abuse", "nsfw", "inappropriate", "offensive",
    "harass", "bully", "threat", "sex", "porn", "nude", "violence"
}
COMMAND_COOLDOWN = 10
MAX_MESSAGES_PER_MINUTE = 10
REPORT_THRESHOLD = 3
TEMP_BAN_DURATION = 24 * 3600

# In-memory storage for runtime (non-persistent)
waiting_users = []
user_pairs = {}
previous_partners = {}
reported_chats = []
command_timestamps = {}
message_timestamps = {}

# Conversation states
GENDER, AGE, TAGS, LOCATION, CONSENT = range(5)

# Database connection
def get_db_connection():
    try:
        url = urlparse(os.getenv("DATABASE_URL"))
        conn = psycopg2.connect(
            database=url.path[1:],
            user=url.username,
            password=url.password,
            host=url.hostname,
            port=url.port
        )
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return None

# Initialize database tables
def init_db():
    conn = get_db_connection()
    if conn:
        try:
            c = conn.cursor()
            c.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    profile JSONB,
                    consent BOOLEAN,
                    consent_time BIGINT,
                    premium_expiry BIGINT,
                    ban_type TEXT,
                    ban_expiry BIGINT
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS reports (
                    report_id SERIAL PRIMARY KEY,
                    reporter_id BIGINT,
                    reported_id BIGINT,
                    timestamp BIGINT,
                    reporter_profile JSONB,
                    reported_profile JSONB
                )
            """)
            conn.commit()
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
        finally:
            conn.close()

init_db()

# Helper functions for database operations
def get_user(user_id: int) -> dict:
    conn = get_db_connection()
    if conn:
        try:
            c = conn.cursor()
            c.execute("SELECT profile, consent, consent_time, premium_expiry, ban_type, ban_expiry FROM users WHERE user_id = %s", (user_id,))
            result = c.fetchone()
            if result:
                return {
                    "profile": result[0] or {},
                    "consent": result[1],
                    "consent_time": result[2],
                    "premium_expiry": result[3],
                    "ban_type": result[4],
                    "ban_expiry": result[5]
                }
            return {}
        except Exception as e:
            logger.error(f"Failed to get user {user_id}: {e}")
        finally:
            conn.close()
    return {}

def update_user(user_id: int, data: dict):
    conn = get_db_connection()
    if conn:
        try:
            c = conn.cursor()
            c.execute("""
                INSERT INTO users (user_id, profile, consent, consent_time, premium_expiry, ban_type, ban_expiry)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE
                SET profile = %s, consent = %s, consent_time = %s, premium_expiry = %s, ban_type = %s, ban_expiry = %s
            """, (
                user_id, data.get("profile", {}), data.get("consent", False), data.get("consent_time"),
                data.get("premium_expiry"), data.get("ban_type"), data.get("ban_expiry"),
                data.get("profile", {}), data.get("consent", False), data.get("consent_time"),
                data.get("premium_expiry"), data.get("ban_type"), data.get("ban_expiry")
            ))
            conn.commit()
        except Exception as e:
            logger.error(f"Failed to update user {user_id}: {e}")
        finally:
            conn.close()

def delete_user_data(user_id: int):
    conn = get_db_connection()
    if conn:
        try:
            c = conn.cursor()
            c.execute("DELETE FROM users WHERE user_id = %s", (user_id,))
            c.execute("DELETE FROM reports WHERE reporter_id = %s OR reported_id = %s", (user_id, user_id))
            conn.commit()
            logger.info(f"Deleted data for user {user_id}")
        except Exception as e:
            logger.error(f"Failed to delete user {user_id}: {e}")
        finally:
            conn.close()

def is_banned(user_id: int) -> bool:
    user = get_user(user_id)
    if user.get("ban_type"):
        if user["ban_type"] == "permanent" or (user["ban_type"] == "temporary" and user["ban_expiry"] > time.time()):
            return True
        if user["ban_type"] == "temporary" and user["ban_expiry"] <= time.time():
            update_user(user_id, {"ban_type": None, "ban_expiry": None})
    return False

def is_premium(user_id: int) -> bool:
    user = get_user(user_id)
    return user.get("premium_expiry") and user["premium_expiry"] > time.time()

def check_rate_limit(user_id: int, cooldown: int = COMMAND_COOLDOWN) -> bool:
    current_time = time.time()
    if user_id in command_timestamps and current_time - command_timestamps[user_id] < cooldown:
        return False
    command_timestamps[user_id] = current_time
    return True

def check_message_rate_limit(user_id: int) -> bool:
    current_time = time.time()
    if user_id not in message_timestamps:
        message_timestamps[user_id] = []
    message_timestamps[user_id] = [t for t in message_timestamps[user_id] if current_time - t < 60]
    if len(message_timestamps[user_id]) >= MAX_MESSAGES_PER_MINUTE:
        return False
    message_timestamps[user_id].append(current_time)
    return True

# Admin helper functions
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def format_user_info(user_id: int) -> str:
    user = get_user(user_id)
    if not user:
        return f"User {user_id} not found."
    info = f"User ID: {user_id}\n"
    info += f"Profile: {user.get('profile', {})}\n"
    info += f"Consent: {user.get('consent', False)}"
    if user.get("consent_time"):
        info += f" (given at {datetime.fromtimestamp(user['consent_time'])})\n"
    else:
        info += "\n"
    info += f"Premium: {is_premium(user_id)}"
    if user.get("premium_expiry"):
        info += f" (expires at {datetime.fromtimestamp(user['premium_expiry'])})\n"
    else:
        info += "\n"
    info += f"Banned: {is_banned(user_id)}"
    if user.get("ban_type"):
        info += f" ({user['ban_type']}"
        if user["ban_expiry"]:
            info += f", expires at {datetime.fromtimestamp(user['ban_expiry'])}"
        info += ")\n"
    else:
        info += "\n"
    conn = get_db_connection()
    if conn:
        try:
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM reports WHERE reported_id = %s", (user_id,))
            report_count = c.fetchone()[0]
            info += f"Reports: {report_count}"
        finally:
            conn.close()
    return info

# Bot commands
def start(update: Update, context: CallbackContext) -> int:
    user_id = update.message.from_user.id
    if is_banned(user_id):
        update.message.reply_text("You are currently banned. Contact support if you believe this is an error.")
        logger.info(f"Banned user {user_id} attempted to start a chat.")
        return ConversationHandler.END
    if not check_rate_limit(user_id):
        update.message.reply_text(f"Please wait {COMMAND_COOLDOWN} seconds before trying again.")
        return ConversationHandler.END
    if user_id in user_pairs:
        update.message.reply_text("You're already in a chat. Use /next to switch or /stop to end.")
        return ConversationHandler.END
    user = get_user(user_id)
    if not user.get("consent"):
        keyboard = [
            [InlineKeyboardButton("I Agree", callback_data="consent_agree")],
            [InlineKeyboardButton("I Disagree", callback_data="consent_disagree")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        update.message.reply_text(
            "Welcome to Talk2Anyone! 🌟\n\n"
            "I connect you with random people for anonymous chats. Rules:\n"
            "- No harassment, spam, or inappropriate content.\n"
            "- Respect other users at all times.\n"
            "- Use /report to report issues.\n"
            "- Violations may result in a ban.\n\n"
            "Privacy: We store only your user ID, profile settings, and consent timestamp. Use /deleteprofile to remove your data.\n\n"
            "Do you agree to these rules?",
            reply_markup=reply_markup
        )
        return CONSENT
    if user_id not in waiting_users:
        if is_premium(user_id):
            waiting_users.insert(0, user_id)
        else:
            waiting_users.append(user_id)
        update.message.reply_text("Looking for a partner... 🔍")
    match_users(context)
    return ConversationHandler.END

def consent_handler(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
    choice = query.data
    if choice == "consent_agree":
        update_user(user_id, {
            "consent": True,
            "consent_time": int(time.time()),
            "profile": get_user(user_id).get("profile", {})
        })
        query.message.reply_text("Thank you for agreeing! Let’s get started.")
        if user_id not in waiting_users:
            if is_premium(user_id):
                waiting_users.insert(0, user_id)
            else:
                waiting_users.append(user_id)
            query.message.reply_text("Looking for a partner... 🔍")
        match_users(context)
    else:
        query.message.reply_text("You must agree to the rules to use this bot. Use /start to try again.")
        logger.info(f"User {user_id} declined rules.")
    return ConversationHandler.END

def match_users(context: CallbackContext) -> None:
    if len(waiting_users) < 2:
        return
    for i in range(len(waiting_users)):
        user1 = waiting_users[i]
        for j in range(i + 1, len(waiting_users)):
            user2 = waiting_users[j]
            if can_match(user1, user2):
                waiting_users.remove(user1)
                waiting_users.remove(user2)
                user_pairs[user1] = user2
                user_pairs[user2] = user1
                previous_partners[user1] = user2
                previous_partners[user2] = user1
                context.bot.send_message(user1, "Connected! Start chatting. 🗣️ Use /help for commands.")
                context.bot.send_message(user2, "Connected! Start chatting. 🗣️ Use /help for commands.")
                logger.info(f"Matched users {user1} and {user2}.")
                return

def can_match(user1: int, user2: int) -> bool:
    profile1 = get_user(user1).get("profile", {})
    profile2 = get_user(user2).get("profile", {})
    if not profile1 or not profile2:
        return True
    tags1 = profile1.get("tags", [])
    tags2 = profile2.get("tags", [])
    if tags1 and tags2:
        if not any(tag in tags2 for tag in tags1):
            return False
    age1 = profile1.get("age")
    age2 = profile2.get("age")
    if age1 and age2:
        if abs(age1 - age2) > 10:
            return False
    return True

def stop(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if is_banned(user_id):
        update.message.reply_text("You are currently banned.")
        return
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        del user_pairs[user_id]
        del user_pairs[partner_id]
        context.bot.send_message(partner_id, "Your partner left the chat. Use /start to find a new one.")
        update.message.reply_text("Chat ended. Use /start to begin a new chat.")
        logger.info(f"User {user_id} stopped chat with {partner_id}.")
    else:
        update.message.reply_text("You're not in a chat. Use /start to find a partner.")

def next_chat(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if is_banned(user_id):
        update.message.reply_text("You are currently banned.")
        return
    if not check_rate_limit(user_id):
        update.message.reply_text(f"Please wait {COMMAND_COOLDOWN} seconds before trying again.")
        return
    stop(update, context)
    if get_user(user_id).get("consent"):
        if user_id not in waiting_users:
            if is_premium(user_id):
                waiting_users.insert(0, user_id)
            else:
                waiting_users.append(user_id)
            update.message.reply_text("Looking for a partner... 🔍")
        match_users(context)
    else:
        update.message.reply_text("Please agree to the rules first by using /start.")

def help_command(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if is_banned(user_id):
        update.message.reply_text("You are currently banned.")
        return
    help_text = (
        "📋 *Talk2Anyone Commands*\n\n"
        "/start - Start a new anonymous chat\n"
        "/next - Find a new chat partner\n"
        "/stop - End the current chat\n"
        "/help - Show this help message\n"
        "/report - Report inappropriate behavior\n"
        "/settings - Customize your profile and matching filters\n"
        "/rematch - Reconnect with your previous partner (Premium)\n"
        "/deleteprofile - Delete your profile and data\n\n"
        "Stay respectful and enjoy chatting! 🗣️"
    )
    if is_admin(user_id):
        help_text += (
            "\n*Admin Commands*\n"
            "/admin_delete <user_id> - Delete a user’s data\n"
            "/admin_premium <user_id> <days> - Grant premium status\n"
            "/admin_revoke_premium <user_id> - Revoke premium status\n"
            "/admin_ban <user_id> <days/permanent> - Ban a user\n"
            "/admin_unban <user_id> - Unban a user\n"
            "/admin_info <user_id> - View user details\n"
            "/admin_reports - List reported users\n"
            "/admin_clear_reports <user_id> - Clear a user’s reports\n"
        )
    update.message.reply_text(help_text, parse_mode="Markdown")

def report(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if is_banned(user_id):
        update.message.reply_text("You are currently banned.")
        return
    if not check_rate_limit(user_id):
        update.message.reply_text(f"Please wait {COMMAND_COOLDOWN} seconds before trying again.")
        return
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        conn = get_db_connection()
        if conn:
            try:
                c = conn.cursor()
                c.execute("""
                    INSERT INTO reports (reporter_id, reported_id, timestamp, reporter_profile, reported_profile)
                    VALUES (%s, %s, %s, %s, %s)
                """, (user_id, partner_id, int(time.time()), get_user(user_id).get("profile", {}), get_user(partner_id).get("profile", {})))
                conn.commit()
                c.execute("SELECT COUNT(*) FROM reports WHERE reported_id = %s", (partner_id,))
                report_count = c.fetchone()[0]
                if report_count >= REPORT_THRESHOLD:
                    update_user(partner_id, {
                        "ban_type": "temporary",
                        "ban_expiry": int(time.time()) + TEMP_BAN_DURATION,
                        "profile": get_user(partner_id).get("profile", {})
                    })
                    context.bot.send_message(partner_id, "You’ve been temporarily banned due to multiple reports.")
                    logger.warning(f"User {partner_id} banned temporarily due to {report_count} reports.")
                    stop(update, context)
                update.message.reply_text("Thank you for reporting. We’ll review the issue. Use /next to find a new partner.")
                logger.info(f"User {user_id} reported user {partner_id}. Total reports: {report_count}.")
            except Exception as e:
                logger.error(f"Failed to log report: {e}")
            finally:
                conn.close()
    else:
        update.message.reply_text("You're not in a chat. Use /start to begin.")

def handle_message(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if is_banned(user_id):
        update.message.reply_text("You are currently banned.")
        return
    if not check_message_rate_limit(user_id):
        update.message.reply_text("You're sending messages too fast. Please slow down.")
        logger.info(f"User {user_id} hit message rate limit.")
        return
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        message_text = update.message.text.lower()
        if any(word in message_text for word in BANNED_WORDS):
            update.message.reply_text("Inappropriate content detected. Please keep the chat respectful.")
            logger.info(f"User {user_id} sent message with banned word: {message_text}")
            return
        context.bot.send_message(partner_id, update.message.text)
        logger.info(f"Message from {user_id} to {partner_id}: {update.message.text}")
    else:
        update.message.reply_text("You're not connected. Use /start to find a partner.")

def settings(update: Update, context: CallbackContext) -> int:
    user_id = update.message.from_user.id
    if is_banned(user_id):
        update.message.reply_text("You are currently banned.")
        return ConversationHandler.END
    if not check_rate_limit(user_id):
        update.message.reply_text(f"Please wait {COMMAND_COOLDOWN} seconds before trying again.")
        return ConversationHandler.END
    user = get_user(user_id)
    if not user.get("profile"):
        update_user(user_id, {"profile": {}, "consent": user.get("consent", False)})
    keyboard = [
        [InlineKeyboardButton("Gender", callback_data="gender")],
        [InlineKeyboardButton("Age", callback_data="age")],
        [InlineKeyboardButton("Tags", callback_data="tags")],
        [InlineKeyboardButton("Location", callback_data="location")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    update.message.reply_text(
        "Customize your profile and add matching filters "
        "(note: filters may increase matching time)",
        reply_markup=reply_markup
    )
    return GENDER

def button(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
    choice = query.data
    if choice == "gender":
        keyboard = [
            [InlineKeyboardButton("Male", callback_data="gender_male")],
            [InlineKeyboardButton("Female", callback_data="gender_female")],
            [InlineKeyboardButton("Other", callback_data="gender_other")],
            [InlineKeyboardButton("Skip", callback_data="gender_skip")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        query.message.reply_text("Select your gender:", reply_markup=reply_markup)
        return GENDER
    elif choice == "age":
        query.message.reply_text("Please enter your age (e.g., 25):")
        return AGE
    elif choice == "tags":
        query.message.reply_text("Enter tags (e.g., music, gaming, movies) separated by commas:")
        return TAGS
    elif choice == "location":
        query.message.reply_text("Enter your location (e.g., New York):")
        return LOCATION

def set_gender(update: Update, context: CallbackContext) -> int:
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
    choice = query.data
    user = get_user(user_id)
    profile = user.get("profile", {})
    if choice.startswith("gender_"):
        gender = choice.split("_")[1]
        if gender != "skip":
            profile["gender"] = gender
        else:
            profile.pop("gender", None)
        update_user(user_id, {"profile": profile, "consent": user.get("consent", False)})
        query.message.reply_text(f"Gender set to: {gender if gender != 'skip' else 'Not specified'}")
    return settings(update, context)

def set_age(update: Update, context: CallbackContext) -> int:
    user_id = update.message.from_user.id
    user = get_user(user_id)
    profile = user.get("profile", {})
    try:
        age = int(update.message.text)
        if 13 <= age <= 120:
            profile["age"] = age
            update_user(user_id, {"profile": profile, "consent": user.get("consent", False)})
            update.message.reply_text(f"Age set to: {age}")
        else:
            update.message.reply_text("Please enter a valid age between 13 and 120.")
            return AGE
    except ValueError:
        update.message.reply_text("Please enter a valid number for age.")
        return AGE
    return settings(update, context)

def set_tags(update: Update, context: CallbackContext) -> int:
    user_id = update.message.from_user.id
    user = get_user(user_id)
    profile = user.get("profile", {})
    tags = [tag.strip().lower() for tag in update.message.text.split(",") if tag.strip()]
    profile["tags"] = tags
    update_user(user_id, {"profile": profile, "consent": user.get("consent", False)})
    update.message.reply_text(f"Tags set to: {', '.join(tags) if tags else 'None'}")
    return settings(update, context)

def set_location(update: Update, context: CallbackContext) -> int:
    user_id = update.message.from_user.id
    user = get_user(user_id)
    profile = user.get("profile", {})
    location = update.message.text.strip()
    profile["location"] = location
    update_user(user_id, {"profile": profile, "consent": user.get("consent", False)})
    update.message.reply_text(f"Location set to: {location}")
    return settings(update, context)

def cancel(update: Update, context: CallbackContext) -> int:
    update.message.reply_text("Operation cancelled.")
    return ConversationHandler.END

def rematch(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if is_banned(user_id):
        update.message.reply_text("You are currently banned.")
        return
    if not check_rate_limit(user_id):
        update.message.reply_text(f"Please wait {COMMAND_COOLDOWN} seconds before trying again.")
        return
    if not is_premium(user_id):
        update.message.reply_text(
            "Re-matching is a PREMIUM feature. Subscribe to use it.\n"
            "100 ⭐ / $1.99 Week\n"
            "350 ⭐ / $3.99 Month\n"
            "1000 ⭐ / $19.99 Year"
        )
        return
    if user_id in user_pairs:
        update.message.reply_text("You're already in a chat. Use /stop to end it first.")
        return
    previous_partner = previous_partners.get(user_id)
    if not previous_partner:
        update.message.reply_text("You don't have a previous partner to re-match with.")
        return
    if previous_partner in user_pairs:
        update.message.reply_text("Your previous partner is currently in another chat.")
        return
    if previous_partner in waiting_users:
        waiting_users.remove(previous_partner)
    user_pairs[user_id] = previous_partner
    user_pairs[previous_partner] = user_id
    context.bot.send_message(user_id, "Re-connected with your previous partner! Start chatting. 🗣️")
    context.bot.send_message(previous_partner, "Your previous partner re-connected with you! Start chatting. 🗣️")
    logger.info(f"User {user_id} rematched with {previous_partner}.")

def delete_profile(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if is_banned(user_id):
        update.message.reply_text("You are currently banned.")
        return
    if not check_rate_limit(user_id):
        update.message.reply_text(f"Please wait {COMMAND_COOLDOWN} seconds before trying again.")
        return
    delete_user_data(user_id)
    update.message.reply_text("Your profile and data have been deleted.")
    logger.info(f"User {user_id} deleted their profile via /deleteprofile.")

# Admin commands
def admin_delete(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if not is_admin(user_id):
        update.message.reply_text("You are not authorized to use this command.")
        return
    try:
        target_id = int(context.args[0])
        delete_user_data(target_id)
        update.message.reply_text(f"User {target_id} data deleted.")
        logger.info(f"Admin {user_id} deleted user {target_id}.")
    except (IndexError, ValueError):
        update.message.reply_text("Usage: /admin_delete <user_id>")

def admin_premium(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if not is_admin(user_id):
        update.message.reply_text("You are not authorized to use this command.")
        return
    try:
        target_id = int(context.args[0])
        days = int(context.args[1])
        expiry = int(time.time()) + days * 24 * 3600
        user = get_user(target_id)
        update_user(target_id, {
            "premium_expiry": expiry,
            "profile": user.get("profile", {}),
            "consent": user.get("consent", False)
        })
        update.message.reply_text(f"User {target_id} granted premium for {days} days.")
        logger.info(f"Admin {user_id} granted premium to {target_id} for {days} days.")
    except (IndexError, ValueError):
        update.message.reply_text("Usage: /admin_premium <user_id> <days>")

def admin_revoke_premium(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if not is_admin(user_id):
        update.message.reply_text("You are not authorized to use this command.")
        return
    try:
        target_id = int(context.args[0])
        user = get_user(target_id)
        update_user(target_id, {
            "premium_expiry": None,
            "profile": user.get("profile", {}),
            "consent": user.get("consent", False)
        })
        update.message.reply_text(f"Premium status revoked for user {target_id}.")
        logger.info(f"Admin {user_id} revoked premium for {target_id}.")
    except (IndexError, ValueError):
        update.message.reply_text("Usage: /admin_revoke_premium <user_id>")

def admin_ban(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if not is_admin(user_id):
        update.message.reply_text("You are not authorized to use this command.")
        return
    try:
        target_id = int(context.args[0])
        ban_type = context.args[1].lower()
        user = get_user(target_id)
        if ban_type == "permanent":
            update_user(target_id, {
                "ban_type": "permanent",
                "ban_expiry": None,
                "profile": user.get("profile", {}),
                "consent": user.get("consent", False)
            })
            update.message.reply_text(f"User {target_id} permanently banned.")
            logger.info(f"Admin {user_id} permanently banned {target_id}.")
        elif ban_type.isdigit():
            days = int(ban_type)
            expiry = int(time.time()) + days * 24 * 3600
            update_user(target_id, {
                "ban_type": "temporary",
                "ban_expiry": expiry,
                "profile": user.get("profile", {}),
                "consent": user.get("consent", False)
            })
            update.message.reply_text(f"User {target_id} banned for {days} days.")
            logger.info(f"Admin {user_id} banned {target_id} for {days} days.")
        else:
            update.message.reply_text("Usage: /admin_ban <user_id> <days/permanent>")
        if target_id in user_pairs:
            stop(update, context)
    except (IndexError, ValueError):
        update.message.reply_text("Usage: /admin_ban <user_id> <days/permanent>")

def admin_unban(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if not is_admin(user_id):
        update.message.reply_text("You are not authorized to use this command.")
        return
    try:
        target_id = int(context.args[0])
        user = get_user(target_id)
        update_user(target_id, {
            "ban_type": None,
            "ban_expiry": None,
            "profile": user.get("profile", {}),
            "consent": user.get("consent", False)
        })
        update.message.reply_text(f"User {target_id} unbanned.")
        logger.info(f"Admin {user_id} unbanned {target_id}.")
    except (IndexError, ValueError):
        update.message.reply_text("Usage: /admin_unban <user_id>")

def admin_info(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if not is_admin(user_id):
        update.message.reply_text("You are not authorized to use this command.")
        return
    try:
        target_id = int(context.args[0])
        info = format_user_info(target_id)
        update.message.reply_text(info)
        logger.info(f"Admin {user_id} viewed info for {target_id}.")
    except (IndexError, ValueError):
        update.message.reply_text("Usage: /admin_info <user_id>")

def admin_reports(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if not is_admin(user_id):
        update.message.reply_text("You are not authorized to use this command.")
        return
    conn = get_db_connection()
    if conn:
        try:
            c = conn.cursor()
            c.execute("""
                SELECT reported_id, COUNT(*) as report_count
                FROM reports
                GROUP BY reported_id
                ORDER BY report_count DESC
            """)
            results = c.fetchall()
            if not results:
                update.message.reply_text("No reported users.")
                return
            report_text = "Reported Users:\n"
            for reported_id, count in results:
                report_text += f"User {reported_id}: {count} reports\n"
            update.message.reply_text(report_text)
            logger.info(f"Admin {user_id} viewed reported users.")
        except Exception as e:
            logger.error(f"Failed to fetch reports: {e}")
            update.message.reply_text("Error fetching reports.")
        finally:
            conn.close()

def admin_clear_reports(update: Update, context: CallbackContext) -> None:
    user_id = update.message.from_user.id
    if not is_admin(user_id):
        update.message.reply_text("You are not authorized to use this command.")
        return
    try:
        target_id = int(context.args[0])
        conn = get_db_connection()
        if conn:
            try:
                c = conn.cursor()
                c.execute("DELETE FROM reports WHERE reported_id = %s", (target_id,))
                conn.commit()
                update.message.reply_text(f"Reports cleared for user {target_id}.")
                logger.info(f"Admin {user_id} cleared reports for {target_id}.")
            except Exception as e:
                logger.error(f"Failed to clear reports: {e}")
                update.message.reply_text("Error clearing reports.")
            finally:
                conn.close()
    except (IndexError, ValueError):
        update.message.reply_text("Usage: /admin_clear_reports <user_id>")

def error_handler(update: Update, context: CallbackContext) -> None:
    logger.error(f"Update {update} caused error {context.error}")
    if update and update.message:
        update.message.reply_text("An error occurred. Please try again or use /help for assistance.")

def main() -> None:
    TOKEN = os.getenv("TOKEN")
    if not TOKEN:
        logger.error("No TOKEN found in environment variables. Exiting.")
        exit(1)
    updater = Updater(TOKEN, use_context=True)
    dispatcher = updater.dispatcher
    start_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            CONSENT: [CallbackQueryHandler(consent_handler, pattern="^consent_")],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    settings_handler = ConversationHandler(
        entry_points=[CommandHandler("settings", settings)],
        states={
            GENDER: [CallbackQueryHandler(button), CallbackQueryHandler(set_gender, pattern="^gender_")],
            AGE: [MessageHandler(Filters.text & ~Filters.command, set_age)],
            TAGS: [MessageHandler(Filters.text & ~Filters.command, set_tags)],
            LOCATION: [MessageHandler(Filters.text & ~Filters.command, set_location)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    dispatcher.add_handler(start_handler)
    dispatcher.add_handler(settings_handler)
    dispatcher.add_handler(CommandHandler("stop", stop))
    dispatcher.add_handler(CommandHandler("next", next_chat))
    dispatcher.add_handler(CommandHandler("help", help_command))
    dispatcher.add_handler(CommandHandler("report", report))
    dispatcher.add_handler(CommandHandler("rematch", rematch))
    dispatcher.add_handler(CommandHandler("deleteprofile", delete_profile))
    # Admin commands
    dispatcher.add_handler(CommandHandler("admin_delete", admin_delete))
    dispatcher.add_handler(CommandHandler("admin_premium", admin_premium))
    dispatcher.add_handler(CommandHandler("admin_revoke_premium", admin_revoke_premium))
    dispatcher.add_handler(CommandHandler("admin_ban", admin_ban))
    dispatcher.add_handler(CommandHandler("admin_unban", admin_unban))
    dispatcher.add_handler(CommandHandler("admin_info", admin_info))
    dispatcher.add_handler(CommandHandler("admin_reports", admin_reports))
    dispatcher.add_handler(CommandHandler("admin_clear_reports", admin_clear_reports))
    dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))
    dispatcher.add_error_handler(error_handler)
    logger.info("Starting Talk2Anyone bot...")
    updater.start_polling(drop_pending_updates=True)
    updater.idle()

if __name__ == "__main__":
    main()
