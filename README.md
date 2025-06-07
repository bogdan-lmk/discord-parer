# Discord Announcement Parser with Telegram Integration

A tool to parse announcement channels from Discord and forward them to Telegram with a user-friendly interface.

## Features

- Supports multiple Discord accounts
- Discovers announcement channels automatically
- Formats messages with server/channel info
- Telegram bot with interactive commands
- Tracks processed messages to avoid duplicates

## Installation

1. Clone this repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Create a `.env` file with your credentials:
   ```
   DISCORD_AUTH_TOKENS=your_token1,your_token2
   TELEGRAM_BOT_TOKEN=your_telegram_bot_token
   TELEGRAM_CHAT_ID=your_chat_id
   ```

## Usage

Run the main application:
```bash
python -m discord_telegram_parser.app
```

### Telegram Bot Commands

- `/start` or `/help` - Show help message
- `/servers` - List all subscribed servers
- `/channels [server]` - List channels for a server
- `/latest [server] [channel]` - Show latest messages
- `/discover` - Discover new servers/channels

## Configuration

Edit `discord_telegram_parser/config/settings.py` to customize:
- Telegram UI preferences
- Message parsing parameters
- Server/channel mappings

## Requirements

- Python 3.7+
- Discord account(s) with access to announcement channels
- Telegram bot token
