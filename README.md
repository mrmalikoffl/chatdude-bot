# Talk2Anyone Bot

**Talk2Anyone Bot** (@Talk2Anyone_Bot) is a feature-rich anonymous chat bot for Telegram, designed to connect users worldwide in safe, engaging, and personalized text-only conversations. Inspired by platforms like Omegle, it offers a modern twist with premium features, robust moderation, and a user-friendly experience.

## Features

- **Anonymous Chatting**: Connect with random users globally using simple commands like `/start`, `/next`, and `/stop`.
- **Customizable Profiles**: Set your name, age, gender, location, bio, and interest tags (e.g., music, gaming) with `/settings`.
- **Premium Features**:
  - ✨ **Flare Messages**: Add sparkling effects to messages (7 days, 100 ⭐).
  - 🔄 **Instant Rematch**: Reconnect with past partners instantly (100 ⭐).
  - 🌟 **Shine Profile**: Priority matching for 24 hours (250 ⭐).
  - 😊 **Mood Match**: Find users with similar vibes (30 days, 250 ⭐).
  - 👤 **Partner Details**: Reveal partner’s name, age, gender, and location (30 days, 500 ⭐).
  - 📜 **Vaulted Chats**: Save chats forever (500 ⭐).
  - 🎉 **Premium Pass**: Unlock all features + 5 rematches (30 days, 1000 ⭐).
  - Access premium features via `/premium`.
- **Chat History & Rematching**: View past chats with `/history` or reconnect with `/rematch` and `/instant` (premium).
- **Safety & Moderation**:
  - Emoji-based user verification to ensure authenticity.
  - Keyword-based content filtering to block inappropriate messages.
  - Report system (`/report`) with automatic temporary bans after multiple reports.
  - Temporary and permanent ban management for rule-breakers.
- **Privacy Controls**: Delete all user data with `/deleteprofile`.
- **Admin Tools**:
  - Manage users, bans, and reports with commands like `/admin_ban`, `/admin_unban`, `/admin_info`, and `/admin_broadcast`.
  - View user lists (`/admin_userslist`, `/admin_premiumuserslist`) and violations (`/admin_violations`).
- **Rate Limiting & Security**:
  - Cooldowns for commands and message limits to prevent spam.
  - Secure database storage with PostgreSQL for user profiles and reports.
- **Payment Integration**: Purchase premium features using Telegram Stars.
- **Error Handling & Logging**: Comprehensive logging for debugging and monitoring via Heroku-compatible setup.

## Prerequisites

- **Python 3.8 or higher**
- A **Telegram account** and a bot token from `@BotFather`
- A **PostgreSQL database** (local or hosted, e.g., Heroku Postgres)
- A **Heroku account** (optional, for cloud deployment)
- Required Python packages (listed in `requirements.txt`)

## Setup Instructions

### 1. Create a Telegram Bot
1. Open Telegram and search for `@BotFather`.
2. Send `/newbot` and follow the prompts to create a bot.
3. Set the bot's name (e.g., "Talk2Anyone") and username (e.g., `@Talk2Anyone_Bot`).
4. Copy the bot token provided by BotFather (e.g., `123456789:ABCDEF...`).

### 2. Clone the Repository
```bash
git clone https://github.com/yourusername/talk2anyone-bot.git
cd talk2anyone-bot
