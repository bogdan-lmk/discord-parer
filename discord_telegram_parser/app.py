import json
import os
import asyncio
import threading
import time
from loguru import logger
from datetime import datetime

from .services.telegram_bot import TelegramBotService
from .services.discord_websocket import DiscordWebSocketService
from .config.settings import config
from .main import DiscordParser

class DiscordTelegramParser:
    def __init__(self):
        self.discord_parser = DiscordParser()
        self.telegram_bot = TelegramBotService(config.TELEGRAM_BOT_TOKEN)
        self.websocket_service = DiscordWebSocketService(self.telegram_bot)
        
        # Cross-reference services
        self.telegram_bot.discord_parser = self.discord_parser
        self.telegram_bot.websocket_service = self.websocket_service
        
        self.running = False
        self.websocket_task = None
        
    def discover_channels(self):
        """Discover announcement channels using channel_id_parser"""
        from discord_telegram_parser.utils.channel_id_parser import parse_discord_servers
        
        mappings = parse_discord_servers()
        if mappings:
            config.SERVER_CHANNEL_MAPPINGS = mappings
            
            # Add discovered channels to WebSocket subscriptions
            for server, channels in mappings.items():
                for channel_id in channels.keys():
                    self.websocket_service.add_channel_subscription(channel_id)
            
            # Save discovered channels to config file
            with open('discord_telegram_parser/config/settings.py', 'a') as f:
                f.write(f"\n# Auto-discovered channels\nconfig.SERVER_CHANNEL_MAPPINGS = {json.dumps(mappings, indent=2)}\n")
        else:
            print("Failed to discover channels")
    
    async def websocket_main_loop(self):
        """Main async loop for WebSocket service"""
        while self.running:
            try:
                logger.info("Starting WebSocket connections...")
                await self.websocket_service.start()
            except Exception as e:
                error_msg = str(e).encode('utf-8', 'replace').decode('utf-8')
                logger.error(f"WebSocket error: {error_msg}")
                logger.info("Restarting WebSocket in 30 seconds...")
                await asyncio.sleep(30)
    
    def run_websocket_in_thread(self):
        """Run WebSocket service in separate thread with async loop"""
        def websocket_thread():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self.websocket_main_loop())
            except Exception as e:
                logger.error(f"WebSocket thread error: {e}")
            finally:
                loop.close()
        
        thread = threading.Thread(target=websocket_thread, daemon=True)
        thread.start()
        return thread
    
    def safe_encode_string(self, text):
        """Safely encode string to handle Unicode issues"""
        if not text:
            return ""
        try:
            # Handle surrogates and problematic characters
            if isinstance(text, str):
                # Remove surrogates and invalid characters
                text = text.encode('utf-8', 'surrogatepass').decode('utf-8', 'replace')
                # Filter out characters that might cause issues
                text = ''.join(char for char in text if ord(char) < 0x110000)
            return text
        except (UnicodeEncodeError, UnicodeDecodeError):
            return "[Encoding Error]"
    
    def test_channel_http_access(self, channel_id):
        """Quick test if channel is accessible via HTTP"""
        try:
            session = self.discord_parser.sessions[0]  # Use first session
            r = session.get(f'https://discord.com/api/v9/channels/{channel_id}/messages?limit=1')
            return r.status_code == 200
        except:
            return False
    
    def initial_sync(self):
        """Perform initial sync of recent messages with smart channel filtering"""
        try:
            # Discover channels if not already configured
            if not config.SERVER_CHANNEL_MAPPINGS:
                self.discover_channels()
            
            # Sync servers between Discord and Telegram
            self.sync_servers()
            
            # Get recent messages from HTTP-accessible channels only
            logger.info("🔍 Performing smart initial sync (HTTP-accessible channels only)...")
            messages = []
            http_channels = []
            websocket_only_channels = []
            
            for server, channels in config.SERVER_CHANNEL_MAPPINGS.items():
                if not channels:
                    continue
                
                safe_server = self.safe_encode_string(server)
                    
                for channel_id, channel_name in channels.items():
                    safe_channel = self.safe_encode_string(channel_name)
                    
                    # Быстрая проверка HTTP доступа
                    if self.test_channel_http_access(channel_id):
                        # HTTP доступен - синхронизируем
                        try:
                            recent_messages = self.discord_parser.parse_announcement_channel(
                                channel_id, 
                                safe_server,
                                safe_channel,
                                limit=5
                            )
                            
                            # Clean message content for encoding issues
                            for msg in recent_messages:
                                msg.content = self.safe_encode_string(msg.content)
                                msg.author = self.safe_encode_string(msg.author)
                                msg.server_name = self.safe_encode_string(msg.server_name)
                                msg.channel_name = self.safe_encode_string(msg.channel_name)
                            
                            messages.extend(recent_messages)
                            http_channels.append((safe_server, safe_channel))
                            logger.info(f"✅ HTTP sync: {safe_server}#{safe_channel} - {len(recent_messages)} messages")
                            
                        except Exception as channel_error:
                            safe_error = str(channel_error).encode('utf-8', 'replace').decode('utf-8')
                            logger.warning(f"❌ HTTP sync failed: {safe_server}#{safe_channel}: {safe_error}")
                            websocket_only_channels.append((safe_server, safe_channel))
                    else:
                        # HTTP недоступен - оставляем для WebSocket
                        websocket_only_channels.append((safe_server, safe_channel))
                        logger.info(f"🔌 WebSocket only: {safe_server}#{safe_channel} - will monitor via WebSocket")
            
            # Summary
            logger.info(f"📊 Initial sync summary:")
            logger.info(f"   ✅ HTTP synced: {len(http_channels)} channels")
            logger.info(f"   🔌 WebSocket only: {len(websocket_only_channels)} channels")
            logger.info(f"   📨 Total messages: {len(messages)}")
            
            if websocket_only_channels:
                logger.info(f"🔌 These channels will be monitored via WebSocket only:")
                for server, channel in websocket_only_channels:
                    logger.info(f"   • {server}#{channel}")
            
            # Forward HTTP messages to Telegram in chronological order
            if messages:
                messages.sort(key=lambda x: x.timestamp)
                self.telegram_bot.send_messages(messages)
                logger.success(f"✅ Initial HTTP sync completed: {len(messages)} messages")
            else:
                logger.info("No HTTP messages found during initial sync")
            
            logger.success(f"🎉 Smart initial sync complete! WebSocket will handle real-time monitoring.")
            
        except Exception as e:
            try:
                error_msg = str(e).encode('utf-8', 'replace').decode('utf-8')
            except:
                error_msg = "Initial sync error (encoding issue)"
            logger.error(f"❌ Error in initial sync: {error_msg}")
    
    def fallback_polling_loop(self):
        """Fallback polling loop for HTTP-accessible channels only"""
        while self.running:
            try:
                time.sleep(300)  # Check every 5 minutes as fallback
                
                if not config.SERVER_CHANNEL_MAPPINGS:
                    continue
                
                logger.debug("🔄 Fallback polling check (HTTP channels only)...")
                
                messages = []
                recent_threshold = datetime.now().timestamp() - 120  # 2 minutes ago
                
                for server, channels in config.SERVER_CHANNEL_MAPPINGS.items():
                    safe_server = self.safe_encode_string(server)
                    
                    for channel_id, channel_name in channels.items():
                        # Only poll HTTP-accessible channels
                        if not self.test_channel_http_access(channel_id):
                            continue  # Skip WebSocket-only channels
                            
                        try:
                            safe_channel = self.safe_encode_string(channel_name)
                            
                            recent_messages = self.discord_parser.parse_announcement_channel(
                                channel_id, 
                                safe_server,
                                safe_channel,
                                limit=3
                            )
                            
                            # Clean message content for encoding issues
                            for msg in recent_messages:
                                msg.content = self.safe_encode_string(msg.content)
                                msg.author = self.safe_encode_string(msg.author)
                                msg.server_name = self.safe_encode_string(msg.server_name)
                                msg.channel_name = self.safe_encode_string(msg.channel_name)
                            
                            # Filter for very recent messages
                            new_messages = [
                                msg for msg in recent_messages
                                if msg.timestamp.timestamp() > recent_threshold
                            ]
                            messages.extend(new_messages)
                            
                        except Exception as e:
                            logger.debug(f"Fallback polling error for {safe_server}#{safe_channel}: {e}")
                            continue
                
                # Send any found messages
                if messages:
                    messages.sort(key=lambda x: x.timestamp)
                    logger.info(f"🔄 Fallback polling found {len(messages)} new messages")
                    self.telegram_bot.send_messages(messages)
                
            except Exception as e:
                error_msg = str(e).encode('utf-8', 'replace').decode('utf-8')
                logger.error(f"Error in fallback polling: {error_msg}")
                time.sleep(60)
    
    def run(self):
        """Run all components with WebSocket priority and smart initial sync"""
        self.running = True
        
        try:
            # Perform smart initial sync (HTTP channels only)
            logger.info("🚀 Starting smart initial sync...")
            self.initial_sync()
            
            # Start Telegram bot in separate thread
            bot_thread = threading.Thread(
                target=self.telegram_bot.start_bot,
                daemon=True
            )
            bot_thread.start()
            logger.success("✅ Telegram bot started")
            
            # Start WebSocket service in separate thread
            websocket_thread = self.run_websocket_in_thread()
            logger.success("✅ WebSocket service started")
            
            # Start fallback polling in separate thread (HTTP channels only)
            fallback_thread = threading.Thread(
                target=self.fallback_polling_loop,
                daemon=True
            )
            fallback_thread.start()
            logger.success("✅ Fallback polling started (HTTP channels only)")
            
            # Keep main thread alive
            logger.success("🎉 Discord Telegram Parser running with smart sync!")
            logger.info("📊 HTTP channels: Initial sync + fallback polling")
            logger.info("🔌 WebSocket channels: Real-time monitoring only")
            logger.info("Press Ctrl+C to stop")
            
            while self.running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            self.running = False
            
            # Stop WebSocket service
            if self.websocket_service:
                asyncio.run(self.websocket_service.stop())
                
        except Exception as e:
            error_msg = str(e).encode('utf-8', 'replace').decode('utf-8')
            logger.error(f"Error in main run loop: {error_msg}")
            self.running = False

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
            safe_server = self.safe_encode_string(server)
            self.telegram_bot._create_or_get_topic(safe_server)
        
        # Find removed servers to delete
        removed_servers = telegram_topics - current_servers
        for server in removed_servers:
            # Remove topic mapping
            if server in self.telegram_bot.server_topics:
                del self.telegram_bot.server_topics[server]
                self.telegram_bot._save_data()

def main():
    """Main entry point for the application"""
    logger.info("Starting Discord Telegram Parser with WebSocket support...")
    app = DiscordTelegramParser()
    app.run()

if __name__ == '__main__':
    main()