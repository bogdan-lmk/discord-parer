import telebot
from typing import List, Dict
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from datetime import datetime
from discord_telegram_parser.models.message import Message
from discord_telegram_parser.config.settings import config
import json
import os
import time
import threading
import asyncio

class TelegramBotService:
    def __init__(self, bot_token: str):
        self.bot = telebot.TeleBot(bot_token)
        self.bot.skip_pending = True  # Skip old messages
        self.bot.threaded = True  # Enable threading
        self.network_timeout = 30  # Store timeout separately
        self.bot._net_helper = self._net_helper_wrapper
        self.message_store = 'telegram_messages.json'
        self.user_states = {}  # Track user navigation states
        self.server_topics = {}  # Store server -> topic_id mapping
        self.websocket_service = None  # Will be set by main app
        self.topic_creation_lock = threading.Lock()  # Prevent concurrent topic creation
        
        # Load existing message mappings if file exists
        if os.path.exists(self.message_store):
            with open(self.message_store, 'r') as f:
                data = json.load(f)
                self.message_mappings = data.get('messages', {})
                self.server_topics = data.get('topics', {})
        else:
            self.message_mappings = {}
            self.server_topics = {}

    def _save_data(self):
        """Save message mappings and topic mappings"""
        with open(self.message_store, 'w') as f:
            json.dump({
                'messages': self.message_mappings,
                'topics': self.server_topics
            }, f)

    def _check_if_supergroup_with_topics(self, chat_id):
        """Check if the chat supports topics"""
        try:
            chat = self.bot.get_chat(chat_id)
            return chat.type == 'supergroup' and getattr(chat, 'is_forum', False)
        except Exception as e:
            print(f"Error checking chat type: {e}")
            return False

    def _topic_exists(self, chat_id, topic_id):
        """Check if a specific topic exists using Telegram API"""
        if not topic_id:
            return False
            
        try:
            # Get forum topic to verify existence
            topic_info = self.bot.get_forum_topic(
                chat_id=chat_id,
                message_thread_id=topic_id
            )
            return topic_info is not None
        except telebot.apihelper.ApiException as e:
            if "not found" in str(e).lower():
                return False
            # For other errors, assume topic exists and let send_message handle it
            return True
        except Exception:
            return False

    def _get_or_create_topic_safe(self, server_name: str, chat_id=None):
        """Thread-safe method to get or create topic for server"""
        chat_id = chat_id or config.TELEGRAM_CHAT_ID
        
        # Use lock to prevent concurrent topic creation
        with self.topic_creation_lock:
            # Double-check if topic exists after acquiring lock
            if server_name in self.server_topics:
                topic_id = self.server_topics[server_name]
                
                # Verify topic still exists
                if self._topic_exists(chat_id, topic_id):
                    print(f"✅ Using existing topic {topic_id} for server '{server_name}'")
                    return topic_id
                else:
                    print(f"⚠️ Topic {topic_id} not found, removing from cache")
                    del self.server_topics[server_name]
                    self._save_data()
            
            # Check if we need to create topics (only for supergroups with topics)
            if not self._check_if_supergroup_with_topics(chat_id):
                print(f"ℹ️ Chat doesn't support topics, using regular messages")
                return None
            
            # Create a new forum topic
            print(f"🔨 Creating new topic for server '{server_name}'")
            
            try:
                topic = self.bot.create_forum_topic(
                    chat_id=chat_id,
                    name=f"🏰 {server_name}",
                    icon_color=0x6FB9F0,  # Blue color
                    icon_custom_emoji_id=None
                )
                
                topic_id = topic.message_thread_id
                self.server_topics[server_name] = topic_id
                self._save_data()
                
                print(f"✅ Created new topic for server '{server_name}' with ID: {topic_id}")
                return topic_id
                
            except Exception as e:
                print(f"❌ Error creating topic for server '{server_name}': {e}")
                return None

    def _create_or_get_topic(self, server_name: str, chat_id=None):
        """Legacy method - redirects to safe version"""
        return self._get_or_create_topic_safe(server_name, chat_id)

    def _recreate_topic_if_missing(self, server_name: str, chat_id=None):
        """Recreate a topic if the current one is missing"""
        chat_id = chat_id or config.TELEGRAM_CHAT_ID
        
        # Remove the old topic ID from our mapping
        if server_name in self.server_topics:
            old_topic_id = self.server_topics[server_name]
            print(f"🗑️ Removing invalid topic {old_topic_id} for server '{server_name}'")
            del self.server_topics[server_name]
            self._save_data()
        
        # Create a new topic using safe method
        return self._get_or_create_topic_safe(server_name, chat_id)

    def format_message(self, message: Message) -> str:
        """Format message for topic replies"""
        formatted = []
        
        # Add channel info if available
        if message.channel_name:
            formatted.append(f"📢 #{message.channel_name}")
        
        if config.TELEGRAM_UI_PREFERENCES['show_timestamps']:
            formatted.append(f"📅 {message.timestamp.strftime('%H:%M:%S')}")
        
        formatted.append(f"👤 {message.author}")
        formatted.append(f"💬 {message.content}")
        
        return "\n".join(formatted)

    def send_messages(self, messages: List[Message]):
        """Send formatted messages to Telegram with proper topic management"""
        if not messages:
            return
        
        server_groups = {}
        
        # Group messages by server
        for message in messages:
            server_name = message.server_name or "Unknown Server"
            if server_name not in server_groups:
                server_groups[server_name] = []
            server_groups[server_name].append(message)
        
        # Send messages with server topics
        for server_name, server_messages in server_groups.items():
            print(f"📤 Sending {len(server_messages)} messages for server: {server_name}")
            
            # Get or create topic for this server (thread-safe)
            topic_id = self._get_or_create_topic_safe(server_name)
            
            # Sort messages chronologically (oldest first)
            server_messages.sort(key=lambda x: x.timestamp)
            
            # Send messages in order
            for message in server_messages:
                formatted = self.format_message(message)
                sent_msg = self._send_message(
                    formatted,
                    message_thread_id=topic_id,
                    server_name=server_name
                )
                
                if sent_msg:
                    # Store mapping between Discord and Telegram message IDs
                    self.message_mappings[str(message.timestamp)] = sent_msg.message_id
                else:
                    print(f"❌ Failed to send message: {formatted[:50]}...")
            
            # Save mappings after each server
            self._save_data()
            
        print(f"✅ Completed sending messages for {len(server_groups)} servers")

    def _net_helper_wrapper(self, method, url, **kwargs):
        """Wrapper for network requests with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return method(url, **kwargs)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                print(f"Retry {attempt + 1} for {url}: {e}")
                time.sleep(1)
                
    def _send_message(self, text: str, chat_id=None, message_thread_id=None, server_name=None):
        """Send message to topic or regular chat with error recovery"""
        chat_id = chat_id or config.TELEGRAM_CHAT_ID
        max_retries = 3
        retry_delay = 5  # seconds
        
        print(f"📤 Sending message to chat {chat_id}")
        if message_thread_id:
            print(f"📍 Topic: {message_thread_id}")
            
        for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
            for attempt in range(max_retries):
                try:
                    # Use message_thread_id for topics
                    result = self.bot.send_message(
                        chat_id, 
                        chunk,
                        message_thread_id=message_thread_id
                    )
                    print(f"✅ Message sent successfully: {result.message_id}")
                    return result
                    
                except Exception as e:
                    error_str = str(e)
                    print(f"❌ Error sending message (attempt {attempt + 1}): {e}")
                    
                    # Handle specific error cases
                    if "message thread not found" in error_str and server_name and message_thread_id:
                        print(f"🔍 Topic {message_thread_id} not found for server '{server_name}'")
                        
                        # Try to recreate the topic
                        new_topic_id = self._recreate_topic_if_missing(server_name, chat_id)
                        
                        if new_topic_id:
                            print(f"🔨 Created new topic {new_topic_id}. Retrying...")
                            message_thread_id = new_topic_id
                            continue  # Retry with new topic ID
                        else:
                            print("⚠️ Failed to recreate topic. Sending as regular message.")
                            message_thread_id = None  # Fall back to regular message
                            continue
                            
                    elif "message thread not found" in error_str and message_thread_id:
                        print("⚠️ Topic not found and no server name provided. Falling back to regular message.")
                        message_thread_id = None  # Fall back to regular message
                        continue
                        
                    elif "Too Many Requests" in error_str:
                        wait_time = 60  # Default wait time if no retry-after
                        if "retry after" in error_str:
                            try:
                                wait_time = int(error_str.split("retry after")[1].strip())
                            except:
                                pass
                        print(f"⏳ Rate limited. Waiting {wait_time} seconds...")
                        time.sleep(wait_time)
                        continue
                        
                    elif attempt == max_retries - 1:
                        print(f"💥 Failed to send message after {max_retries} attempts: {e}")
                        return None
                        
                    time.sleep(retry_delay)
            
        return None

    def get_server_topic_id(self, server_name: str):
        """Get topic ID for server (for external use)"""
        return self.server_topics.get(server_name)

    def list_server_topics(self):
        """List all server topics"""
        return dict(self.server_topics)

    def cleanup_invalid_topics(self, chat_id=None):
        """Clean up invalid topic mappings"""
        chat_id = chat_id or config.TELEGRAM_CHAT_ID
        invalid_topics = []
        
        for server_name, topic_id in self.server_topics.items():
            if not self._topic_exists(chat_id, topic_id):
                invalid_topics.append(server_name)
        
        # Remove invalid topics
        for server_name in invalid_topics:
            print(f"🗑️ Removing invalid topic for server: {server_name}")
            del self.server_topics[server_name]
        
        if invalid_topics:
            self._save_data()
            print(f"🧹 Cleaned up {len(invalid_topics)} invalid topics")
        
        return len(invalid_topics)

    def start_bot(self):
        """Start bot with improved topic management"""
        @self.bot.message_handler(commands=['start', 'help'])
        def send_welcome(message):
            # Check if chat supports topics
            supports_topics = self._check_if_supergroup_with_topics(message.chat.id)
            
            text = (
                "🤖 Welcome to Discord Announcement Parser!\n\n"
                "🔥 **Real-time WebSocket Mode** - Instant message delivery!\n"
                "📡 Messages are received via WebSocket for immediate forwarding\n"
                "🔄 Improved topic management: One server = One topic\n\n"
            )
            
            if supports_topics:
                text += (
                    "🔹 Forum Topics Mode (Enabled):\n"
                    "• Each Discord server gets ONE topic\n"
                    "• Messages from all channels in server go to same topic\n"
                    "• Auto-recovery for missing topics\n"
                    "• No duplicate topics created\n"
                    "• Messages displayed chronologically\n\n"
                )
            else:
                text += (
                    "🔹 Regular Messages Mode:\n"
                    "• Messages sent as regular chat messages\n"
                    "• To enable topics, convert this chat to a supergroup with topics enabled\n\n"
                )
            
            text += "Choose an action below:"
            
            markup = InlineKeyboardMarkup(row_width=2)
            markup.add(
                InlineKeyboardButton("📋 Server List", callback_data="action_servers"),
                InlineKeyboardButton("🔄 Manual Sync", callback_data="action_refresh"),
                InlineKeyboardButton("⚡ WebSocket Status", callback_data="action_websocket"),
                InlineKeyboardButton("🧹 Clean Topics", callback_data="action_cleanup"),
                InlineKeyboardButton("📊 Bot Status", callback_data="action_status"),
                InlineKeyboardButton("ℹ️ Help", callback_data="action_help")
            )
            
            self.bot.send_message(message.chat.id, text, reply_markup=markup)

        @self.bot.callback_query_handler(func=lambda call: call.data.startswith('action_'))
        def handle_action(call):
            action = call.data.replace('action_', '')
            
            if action == 'cleanup':
                cleaned = self.cleanup_invalid_topics(call.message.chat.id)
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="action_start"))
                self.bot.edit_message_text(
                    f"🧹 Topic cleanup completed!\n\n"
                    f"Removed {cleaned} invalid topics.\n"
                    f"Current topics: {len(self.server_topics)}",
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup
                )
            elif action == 'servers':
                list_servers(call.message)
            elif action == 'websocket':
                show_websocket_status(call.message)
            elif action == 'refresh':
                markup = InlineKeyboardMarkup()
                if not self.user_states.get(call.from_user.id):
                    markup.add(InlineKeyboardButton("📋 Select Server", callback_data="action_servers"))
                    markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="action_start"))
                    self.bot.edit_message_text(
                        "Please select a server first to check for new messages.",
                        call.message.chat.id,
                        call.message.message_id,
                        reply_markup=markup
                    )
                else:
                    state = self.user_states[call.from_user.id]
                    markup.add(
                        InlineKeyboardButton("🔄 Check Now", callback_data="refresh_check"),
                        InlineKeyboardButton("📋 Change Server", callback_data="action_servers")
                    )
                    markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="action_start"))
                    self.bot.edit_message_text(
                        f"Currently watching:\n"
                        f"🏰 Server: {state['server']}\n\n"
                        f"Choose an action:",
                        call.message.chat.id,
                        call.message.message_id,
                        reply_markup=markup
                    )
            elif action == 'help':
                supports_topics = self._check_if_supergroup_with_topics(call.message.chat.id)
                
                help_text = (
                    "📖 Bot Commands:\n\n"
                    "🔹 /servers - Browse Discord servers\n"
                    "🔹 /refresh - Manual message sync\n"
                    "🔹 /websocket - WebSocket status\n"
                    "🔹 /help - Show this help\n"
                    "🔹 /reset_topics - Reset all topic mappings\n"
                    "🔹 /cleanup_topics - Clean invalid topics\n\n"
                    "⚙️ Real-time Features:\n"
                    "• WebSocket connections for instant delivery\n"
                    "• Multiple Discord token support\n"
                    "• Auto-discovery of announcement channels\n"
                    "• Messages in chronological order (oldest first)\n"
                    "• Fallback polling for reliability\n"
                    "• One server = One topic (no duplicates)\n"
                )
                
                if supports_topics:
                    help_text += (
                        "• Topic-based organization ✅\n"
                        "• Auto-created server topics\n"
                        "• Auto-recovery for missing topics\n"
                        "• Thread-safe topic management\n"
                    )
                else:
                    help_text += (
                        "• Regular message organization\n"
                        "• Convert to supergroup for topics\n"
                    )
                
                help_text += (
                    "\n💡 To enable topics:\n"
                    "1. Convert this chat to a supergroup\n"
                    "2. Enable 'Topics' in group settings\n"
                    "3. Restart the bot"
                )
                
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="action_start"))
                self.bot.edit_message_text(
                    help_text,
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup
                )
            elif action == 'status':
                supports_topics = self._check_if_supergroup_with_topics(call.message.chat.id)
                
                status_text = (
                    "📊 Bot Status\n\n"
                    f"🔹 Topics Support: {'✅ Enabled' if supports_topics else '❌ Disabled'}\n"
                    f"🔹 Active Topics: {len(self.server_topics)}\n"
                    f"🔹 Configured Servers: {len(config.SERVER_CHANNEL_MAPPINGS) if hasattr(config, 'SERVER_CHANNEL_MAPPINGS') else 0}\n"
                    f"🔹 Total Channels: {sum(len(channels) for channels in config.SERVER_CHANNEL_MAPPINGS.values()) if hasattr(config, 'SERVER_CHANNEL_MAPPINGS') else 0}\n"
                    f"🔹 Message Cache: {len(self.message_mappings)} messages\n"
                    f"🔹 WebSocket Channels: {len(self.websocket_service.subscribed_channels) if self.websocket_service else 0}\n"
                    "🔹 Topic Logic: One server = One topic ✅\n\n"
                    "📋 Current Topics:\n"
                )
                
                if self.server_topics:
                    for server, topic_id in list(self.server_topics.items())[:10]:  # Show first 10
                        # Check if topic still exists
                        exists = self._topic_exists(call.message.chat.id, topic_id)
                        status_icon = "✅" if exists else "❌"
                        status_text += f"• {server}: Topic {topic_id} {status_icon}\n"
                    
                    if len(self.server_topics) > 10:
                        status_text += f"• ... and {len(self.server_topics) - 10} more topics\n"
                else:
                    status_text += "• No topics created yet\n"
                
                markup = InlineKeyboardMarkup()
                markup.add(
                    InlineKeyboardButton("🧹 Clean Invalid", callback_data="action_cleanup"),
                    InlineKeyboardButton("🔙 Back to Menu", callback_data="action_start")
                )
                self.bot.edit_message_text(
                    status_text,
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup
                )
            elif action == 'start':
                send_welcome(call.message)
            
            self.bot.answer_callback_query(call.id)

        @self.bot.message_handler(commands=['servers'])
        def list_servers(message):
            """Show interactive server list with topic info"""
            if not hasattr(config, 'SERVER_CHANNEL_MAPPINGS') or not config.SERVER_CHANNEL_MAPPINGS:
                self.bot.reply_to(message, "❌ No servers found. Please configure servers first.")
                return
                
            markup = InlineKeyboardMarkup()
            for server in config.SERVER_CHANNEL_MAPPINGS.keys():
                # Add topic indicator
                topic_indicator = ""
                if server in self.server_topics:
                    topic_id = self.server_topics[server]
                    if self._topic_exists(message.chat.id, topic_id):
                        topic_indicator = " 📋"
                    else:
                        topic_indicator = " ❌"
                
                markup.add(InlineKeyboardButton(
                    f"🏰 {server}{topic_indicator}",
                    callback_data=f"server_{server}"
                ))
            markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="action_start"))
            
            server_count = len(config.SERVER_CHANNEL_MAPPINGS)
            topic_count = len(self.server_topics)
            
            self.bot.reply_to(
                message, 
                f"📋 Select a server to view announcements:\n\n"
                f"📊 {server_count} servers configured, {topic_count} topics created\n"
                f"📋 = Has topic, ❌ = Invalid topic",
                reply_markup=markup
            )

        @self.bot.callback_query_handler(func=lambda call: call.data == "refresh_check")
        def refresh_check(call):
            """Handle refresh check button with improved topic management"""
            user_id = call.from_user.id
            if user_id not in self.user_states:
                self.bot.answer_callback_query(call.id, "Please select a server first")
                return
                
            state = self.user_states[user_id]
            messages = self.discord_parser.parse_announcement_channel(
                state['channel_id'],
                state['server'],
                state['channel_name'],
                limit=10
            )
            
            # Initialize last_message if not set
            if 'last_message' not in state:
                state['last_message'] = datetime.min
                
            # Filter for new messages and sort chronologically
            new_messages = [
                msg for msg in messages
                if msg.timestamp > state['last_message']
            ][:10]
            
            new_messages.sort(key=lambda x: x.timestamp)
            
            if not new_messages:
                self.bot.answer_callback_query(call.id, "No new messages found")
                return
            
            # Send messages using improved topic logic
            self.send_messages(new_messages)
            
            self.bot.answer_callback_query(
                call.id,
                f"Sent {len(new_messages)} new messages to server topic!"
            )
            
            # Update last message timestamp
            if new_messages:
                self.user_states[user_id]['last_message'] = new_messages[-1].timestamp

        @self.bot.callback_query_handler(func=lambda call: call.data.startswith('server_'))
        def server_selected(call):
            """Handle server selection with improved topic management"""
            server_name = call.data.replace('server_', '')
            if not hasattr(config, 'SERVER_CHANNEL_MAPPINGS') or server_name not in config.SERVER_CHANNEL_MAPPINGS:
                self.bot.answer_callback_query(call.id, "Server not found")
                return
                
            # Get first announcement channel
            channels = [
                (cid, name) for cid, name in config.SERVER_CHANNEL_MAPPINGS[server_name].items()
                if not cid.startswith('telegram_')
            ]
            
            if not channels:
                self.bot.answer_callback_query(call.id, "No announcement channels found for this server")
                return
                
            channel_id, channel_name = channels[0]
            
            # Get last 10 messages and sort chronologically
            messages = self.discord_parser.parse_announcement_channel(
                channel_id,
                server_name,
                channel_name,
                limit=10
            )
            
            messages.sort(key=lambda x: x.timestamp)
            
            print(f"📥 Fetched {len(messages)} messages from Discord for {server_name}")
            
            if not messages:
                self.bot.answer_callback_query(call.id, "No messages found")
                return
            
            # Show topic status
            topic_status = ""
            if server_name in self.server_topics:
                topic_id = self.server_topics[server_name]
                if self._topic_exists(call.message.chat.id, topic_id):
                    topic_status = f" to existing topic {topic_id}"
                else:
                    topic_status = " (will create new topic - old one invalid)"
            else:
                topic_status = " (will create new topic)"
            
            # Send messages using improved topic logic
            self.send_messages(messages)
            
            self.bot.answer_callback_query(
                call.id,
                f"Sent {len(messages)} messages{topic_status}"
            )
            
            # Store user state
            self.user_states[call.from_user.id] = {
                'server': server_name,
                'channel_id': channel_id,
                'channel_name': channel_name,
                'last_message': messages[-1].timestamp if messages else datetime.min
            }

        @self.bot.message_handler(commands=['websocket'])
        def show_websocket_status(message):
            """Show WebSocket connection status with topic info"""
            if not self.websocket_service:
                self.bot.reply_to(message, "❌ WebSocket service not initialized")
                return
            
            status_text = (
                "⚡ WebSocket Status\n\n"
                f"🔹 Service Running: {'✅ Yes' if self.websocket_service.running else '❌ No'}\n"
                f"🔹 Active Connections: {len([ws for ws in self.websocket_service.websockets if ws.get('websocket')])}\n"
                f"🔹 Subscribed Channels: {len(self.websocket_service.subscribed_channels)}\n"
                f"🔹 HTTP Accessible: {len(self.websocket_service.http_accessible_channels)}\n"
                f"🔹 WebSocket Only: {len(self.websocket_service.websocket_accessible_channels)}\n"
                f"🔹 Session ID: {self.websocket_service.session_id or 'Not connected'}\n"
                f"🔹 Topics Created: {len(self.server_topics)}\n"
                f"🔹 Topic Logic: One server = One topic ✅\n\n"
                "📡 Channel Access Types:\n"
            )
            
            if self.websocket_service.subscribed_channels:
                channel_info = {}
                for channel_id in list(self.websocket_service.subscribed_channels)[:10]:
                    # Find server and channel name
                    for server, channels in config.SERVER_CHANNEL_MAPPINGS.items():
                        if channel_id in channels:
                            access_type = "📡"  # Default
                            if channel_id in self.websocket_service.http_accessible_channels:
                                if channel_id in self.websocket_service.websocket_accessible_channels:
                                    access_type = "🌐+📡"  # Both
                                else:
                                    access_type = "🌐"  # HTTP only
                            elif channel_id in self.websocket_service.websocket_accessible_channels:
                                access_type = "📡"  # WebSocket only
                            
                            if server not in channel_info:
                                channel_info[server] = []
                            channel_info[server].append(f"#{channels[channel_id]} {access_type}")
                            break
                
                for server, channels_list in list(channel_info.items())[:5]:  # Show first 5 servers
                    status_text += f"• {server}:\n"
                    for ch in channels_list[:3]:  # Show first 3 channels per server
                        status_text += f"  {ch}\n"
                    if len(channels_list) > 3:
                        status_text += f"  ... and {len(channels_list) - 3} more\n"
                
                if len(channel_info) > 5:
                    status_text += f"• ... and {len(channel_info) - 5} more servers\n"
                    
                status_text += f"\n🔤 Legend: 🌐 HTTP, 📡 WebSocket, 🌐+📡 Both"
            else:
                status_text += "• No channels subscribed\n"
            
            markup = InlineKeyboardMarkup()
            markup.add(
                InlineKeyboardButton("🔄 Refresh Status", callback_data="action_websocket"),
                InlineKeyboardButton("🔙 Back to Menu", callback_data="action_start")
            )
            
            self.bot.reply_to(message, status_text, reply_markup=markup)

        @self.bot.message_handler(commands=['reset_topics'])
        def reset_topics(message):
            """Reset all topic mappings - useful when topics are deleted"""
            with self.topic_creation_lock:
                self.server_topics.clear()
                self._save_data()
            self.bot.reply_to(message, "✅ All topic mappings have been reset. New topics will be created when needed.")

        @self.bot.message_handler(commands=['cleanup_topics'])
        def cleanup_topics_command(message):
            """Clean up invalid topic mappings"""
            cleaned = self.cleanup_invalid_topics(message.chat.id)
            self.bot.reply_to(
                message, 
                f"🧹 Cleaned up {cleaned} invalid topics.\n"
                f"Current active topics: {len(self.server_topics)}"
            )

        @self.bot.message_handler(commands=['refresh'])
        def refresh_messages(message):
            """Check for new messages with improved topic management"""
            user_id = message.from_user.id
            if user_id not in self.user_states:
                self.bot.reply_to(message, "Please select a server first using /servers")
                return
                
            state = self.user_states[user_id]
            messages = self.discord_parser.parse_announcement_channel(
                state['channel_id'],
                state['server'],
                state['channel_name'],
                limit=10
            )
            
            # Initialize last_message if not set
            if 'last_message' not in state:
                state['last_message'] = datetime.min
                
            # Filter for new messages and sort chronologically
            new_messages = [
                msg for msg in messages
                if msg.timestamp > state['last_message']
            ][:10]
            
            new_messages.sort(key=lambda x: x.timestamp)
            
            if not new_messages:
                self.bot.reply_to(message, "No new messages found")
                return
            
            # Send messages using improved topic logic
            self.send_messages(new_messages)
            
            # Show result with topic info
            topic_info = ""
            if state['server'] in self.server_topics:
                topic_id = self.server_topics[state['server']]
                topic_info = f" to topic {topic_id}"
            
            self.bot.reply_to(
                message,
                f"✅ Sent {len(new_messages)} new messages{topic_info}"
            )
            
            # Update last message timestamp
            if new_messages:
                self.user_states[user_id]['last_message'] = new_messages[-1].timestamp

        @self.bot.message_handler(func=lambda message: True)
        def handle_text_message(message):
            """Handle regular text messages"""
            pass

        print("🤖 Telegram Bot started with improved topic management:")
        print("   ✅ One server = One topic")
        print("   ✅ Thread-safe topic creation")
        print("   ✅ Auto-cleanup of invalid topics")
        print("   ✅ No duplicate topics")
        print("   ✅ Topic status indicators")
        print("   ✅ Enhanced status reporting")
        self.bot.polling(none_stop=True)