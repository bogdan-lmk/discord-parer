import json
import os
from .services.telegram_bot import TelegramBotService
from .config.settings import config
from .main import DiscordParser
import threading
import time
from loguru import logger
from datetime import datetime

class DiscordTelegramParser:
    def __init__(self):
        self.discord_parser = DiscordParser()
        self.telegram_bot = TelegramBotService(config.TELEGRAM_BOT_TOKEN)
        self.telegram_bot.discord_parser = self.discord_parser
        
    def discover_channels(self):
        """Discover announcement channels using channel_id_parser"""
        from discord_telegram_parser.utils.channel_id_parser import parse_discord_servers
        
        mappings = parse_discord_servers()
        if mappings:
            config.SERVER_CHANNEL_MAPPINGS = mappings
            # Save discovered channels to config file
            with open('discord_telegram_parser/config/settings.py', 'a') as f:
                f.write(f"\n# Auto-discovered channels\nconfig.SERVER_CHANNEL_MAPPINGS = {json.dumps(mappings, indent=2)}\n")
        else:
            print("Failed to discover channels")
        
    def parse_and_forward(self):
        """Main parsing and forwarding loop"""
        while True:
            try:
                # Discover channels if not already configured
                if not config.SERVER_CHANNEL_MAPPINGS:
                    self.discover_channels()
                
                # Sync servers between Discord and Telegram
                self.sync_servers()
                
                # Parse messages from all configured channels
                messages = []
                for server, channels in config.SERVER_CHANNEL_MAPPINGS.items():
                    for channel_id, channel_name in channels.items():
                        # Always parse the last 10 messages
                        messages.extend(
                            self.discord_parser.parse_announcement_channel(
                                channel_id, 
                                server,
                                channel_name,
                                limit=10
                            )
                        )
                
                # Forward messages to Telegram in chronological order
                if messages:
                    # Sort by timestamp (oldest first)
                    messages.sort(key=lambda x: x.timestamp)
                    self.telegram_bot.send_messages(messages)
                
                # Wait 60 seconds before next check for better responsiveness
                time.sleep(60)
                
            except Exception as e:
                # Sanitize error message to fix encoding issues
                error_msg = str(e).encode('utf-8', 'replace').decode('utf-8')
                logger.error(f"Error in main loop: {error_msg}")
                time.sleep(60)

    def run(self):
        """Run all components"""
        # Start Telegram bot in separate thread
        bot_thread = threading.Thread(
            target=self.telegram_bot.start_bot,
            daemon=True
        )
        bot_thread.start()
        
        # Start main parsing loop
        self.parse_and_forward()

    def sync_servers(self):
        """Sync Discord servers with Telegram topics"""
        # Get current Discord servers
        current_servers = set(config.SERVER_CHANNEL_MAPPINGS.keys())
        
        # Get Telegram topics
        telegram_topics = set(self.telegram_bot.server_topics.keys())
        
        # Find new servers to add
        new_servers = current_servers - telegram_topics
        for server in new_servers:
            # Create topic for new server
            self.telegram_bot._create_or_get_topic(server)
        
        # Find removed servers to delete
        removed_servers = telegram_topics - current_servers
        for server in removed_servers:
            # Remove topic mapping
            if server in self.telegram_bot.server_topics:
                del self.telegram_bot.server_topics[server]
                self.telegram_bot._save_data()

def main():
    """Main entry point for the application"""
    app = DiscordTelegramParser()
    app.run()

if __name__ == '__main__':
    main()
