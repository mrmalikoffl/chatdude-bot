from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,  # Updated import
)
import logging
import os

# Set up logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Store active users and their chat partners
waiting_users = []
user_pairs = {}
reported_chats = []

# Get the bot token from environment variables
TOKEN = os.getenv("TOKEN")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.message.from_user.id
    if user_id in user_pairs:
        await update.message.reply_text("You're already in a chat. Use /next to switch or /stop to end.")
        return
    if user_id not in waiting_users:
        waiting_users.append(user_id)
        await update.message.reply_text("Looking for a partner... 🔍")
    if len(waiting_users) >= 2:
        user1 = waiting_users.pop(0)
        user2 = waiting_users.pop(0)
        user_pairs[user1] = user2
        user_pairs[user2] = user1
        await context.bot.send_message(user1, "Connected! Start chatting. 🗣️ Use /help for commands.")
        await context.bot.send_message(user2, "Connected! Start chatting. 🗣️ Use /help for commands.")

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.message.from_user.id
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        del user_pairs[user_id]
        del user_pairs[partner_id]
        await context.bot.send_message(partner_id, "Your]- partner left the chat. Use /start to find a new one.")
        await update.message.reply_text("Chat ended. Use /start to begin a new chat.")
    else:
        await update.message.reply_text("You're not in a chat. Use /start to find a partner.")

async def next_chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await stop(update, context)
    await start(update, context)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    help_text = (
        "📋 *Chat Dude Commands*\n\n"
        "/start - Start a new anonymous chat\n"
        "/next - Find a new chat partner\n"
        "/stop - End the current chat\n"
        "/help - Show this help message\n"
        "/report - Report inappropriate behavior\n\n"
        "Stay respectful and enjoy chatting! 🗣️"
    )
    await update.message.reply_text(help_text, parse_mode="Markdown")

async def report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.message.from_user.id
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        reported_chats.append({"reporter": user_id, "reported": partner_id})
        await update.message.reply_text("Thank you for reporting. We’ll review the issue. Use /next to find a new partner.")
        logger.info(f"User {user_id} reported user {partner_id}")
    else:
        await update.message.reply_text("You're not in a chat. Use /start to begin.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.message.from_user.id
    if user_id in user_pairs:
        partner_id = user_pairs[user_id]
        await context.bot.send_message(partner_id, update.message.text)
    else:
        await update.message.reply_text("You're not connected. Use /start to find a partner.")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Update {update} caused error {context.error}")
    if update:
        await update.message.reply_text("An error occurred. Please try again or use /help for assistance.")

def main() -> None:
    if not TOKEN:
        logger.error("No TOKEN found in environment variables. Please set the TOKEN variable.")
        return

    # Simplified Application initialization
    application = Application.builder().token(TOKEN).build()

    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("stop", stop))
    application.add_handler(CommandHandler("next", next_chat))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("report", report))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_error_handler(error_handler)

    logger.info("Starting Chat Dude bot...")
    # Use run_polling with simplified parameters
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
