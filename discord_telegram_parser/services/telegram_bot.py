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

    def _create_or_get_topic(self, server_name: str, chat_id=None):
        """Create a new topic for server only when needed"""
        chat_id = chat_id or config.TELEGRAM_CHAT_ID
        
        # Check if we already have a topic for this server
        if server_name in self.server_topics:
            topic_id = self.server_topics[server_name]
            
            # Verify topic still exists
            if self._topic_exists(chat_id, topic_id):
                print(f"Using existing topic {topic_id} for server '{server_name}'")
                return topic_id
            else:
                print(f"Topic {topic_id} not found, will create new one")
                del self.server_topics[server_name]
        
        # Only create topic when server is selected by user
        print(f"Creating new topic for server '{server_name}'")
        
        try:
            # Create a new forum topic
            topic = self.bot.create_forum_topic(
                chat_id=chat_id,
                name=f"🏰 {server_name}",
                icon_color=0x6FB9F0,  # Blue color
                icon_custom_emoji_id=None
            )
            
            topic_id = topic.message_thread_id
            self.server_topics[server_name] = topic_id
            self._save_data()
            
            print(f"Created new topic for server '{server_name}' with ID: {topic_id}")
            return topic_id
            
        except Exception as e:
            print(f"Error creating topic for server '{server_name}': {e}")
            return None

    def _recreate_topic_if_missing(self, server_name: str, chat_id=None):
        """Recreate a topic if the current one is missing"""
        chat_id = chat_id or config.TELEGRAM_CHAT_ID
        
        # Remove the old topic ID from our mapping
        if server_name in self.server_topics:
            old_topic_id = self.server_topics[server_name]
            print(f"Removing invalid topic {old_topic_id} for server '{server_name}'")
            del self.server_topics[server_name]
            self._save_data()
        
        # Create a new topic
        return self._create_or_get_topic(server_name, chat_id)

    def format_message(self, message: Message) -> str:
        """Format message for topic replies"""
        formatted = []
        if config.TELEGRAM_UI_PREFERENCES['show_timestamps']:
            formatted.append(f"📅 {message.timestamp}")
        formatted.append(f"👤 {message.author}")
        formatted.append(f"💬 {message.content}")
        
        return "\n".join(formatted)

    def send_messages(self, messages: List[Message]):
        """Send formatted messages to Telegram with server topics in reverse order"""
        server_groups = {}
        
        # Group messages by server
        for message in messages:
            if message.server_name not in server_groups:
                server_groups[message.server_name] = []
            server_groups[message.server_name].append(message)
        
        # Send messages with server topics
        for server_name, server_messages in server_groups.items():
            # Get or create topic for this server
            topic_id = self._create_or_get_topic(server_name)
            
            # REVERSE ORDER: Send messages from oldest to newest
            for message in reversed(server_messages):
                formatted = self.format_message(message)
                sent_msg = self._send_message(
                    formatted,
                    message_thread_id=topic_id,
                    server_name=server_name  # Pass server name for recovery
                )
                
                if sent_msg:
                    # Store mapping between Discord and Telegram message IDs
                    self.message_mappings[str(message.timestamp)] = sent_msg.message_id
            
            # Save mappings after each server
            self._save_data()

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
        
        print(f"Attempting to send message to chat {chat_id}")
        if message_thread_id:
            print(f"Sending to topic {message_thread_id}")
            
        for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
            for attempt in range(max_retries):
                try:
                    # Use message_thread_id for topics
                    result = self.bot.send_message(
                        chat_id, 
                        chunk,
                        message_thread_id=message_thread_id
                    )
                    print(f"Message sent successfully: {result.message_id}")
                    return result
                    
                except Exception as e:
                    error_str = str(e)
                    print(f"Error sending message (attempt {attempt + 1}): {e}")
                    
                    # Handle specific error cases
                    if "message thread not found" in error_str and server_name and message_thread_id:
                        print(f"Topic {message_thread_id} not found. Attempting to recreate topic for server '{server_name}'")
                        
                        # Try to recreate the topic
                        new_topic_id = self._recreate_topic_if_missing(server_name, chat_id)
                        
                        if new_topic_id:
                            print(f"Created new topic {new_topic_id}. Retrying message send...")
                            message_thread_id = new_topic_id
                            continue  # Retry with new topic ID
                        else:
                            print("Failed to recreate topic. Sending as regular message.")
                            message_thread_id = None  # Fall back to regular message
                            continue
                            
                    elif "message thread not found" in error_str and message_thread_id:
                        print("Topic not found and no server name provided. Falling back to regular message.")
                        message_thread_id = None  # Fall back to regular message
                        continue
                        
                    elif "Too Many Requests" in error_str:
                        wait_time = 60  # Default wait time if no retry-after
                        if "retry after" in error_str:
                            try:
                                wait_time = int(error_str.split("retry after")[1].strip())
                            except:
                                pass
                        print(f"Rate limited. Waiting {wait_time} seconds before retry...")
                        time.sleep(wait_time)
                        continue
                        
                    elif attempt == max_retries - 1:
                        print(f"Failed to send message after {max_retries} attempts: {e}")
                        return None
                        
                    time.sleep(retry_delay)
            
        return None

    def start_bot(self):
        """Start bot with interactive server/channel selection"""
        @self.bot.message_handler(commands=['start', 'help'])
        def send_welcome(message):
            # Check if chat supports topics
            supports_topics = self._check_if_supergroup_with_topics(message.chat.id)
            
            text = (
                "👋 Welcome to Discord Announcement Parser!\n\n"
                "This bot forwards Discord announcements to Telegram.\n\n"
            )
            
            if supports_topics:
                text += (
                    "🔹 Forum Topics Mode (Enabled):\n"
                    "• Each Discord server gets its own topic\n"
                    "• Messages are organized by server\n"
                    "• Auto-recovery for missing topics\n"
                    "• Messages displayed in chronological order (oldest first)\n\n"
                )
            else:
                text += (
                    "🔹 Regular Messages Mode:\n"
                    "• Messages sent as regular chat messages\n"
                    "• Messages displayed in chronological order (oldest first)\n"
                    "• To enable topics, convert this chat to a supergroup with topics enabled\n\n"
                )
            
            text += "Choose an action below:"
            
            markup = InlineKeyboardMarkup(row_width=2)
            markup.add(
                InlineKeyboardButton("📋 Server List", callback_data="action_servers"),
                InlineKeyboardButton("🔄 Refresh", callback_data="action_refresh"),
                InlineKeyboardButton("ℹ️ Help", callback_data="action_help"),
                InlineKeyboardButton("📊 Status", callback_data="action_status")
            )
            
            self.bot.send_message(message.chat.id, text, reply_markup=markup)

        @self.bot.callback_query_handler(func=lambda call: call.data.startswith('action_'))
        def handle_action(call):
            action = call.data.replace('action_', '')
            
            if action == 'servers':
                list_servers(call.message)
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
                    "🔹 /refresh - Check for new messages\n"
                    "🔹 /help - Show this help\n"
                    "🔹 /reset_topics - Reset all topic mappings\n\n"
                    "⚙️ Features:\n"
                    "• Multiple Discord servers\n"
                    "• Messages in chronological order (oldest first)\n"
                )
                
                if supports_topics:
                    help_text += (
                        "• Topic-based organization\n"
                        "• Auto-created server topics\n"
                        "• Auto-recovery for missing topics\n"
                    )
                else:
                    help_text += (
                        "• Regular message organization\n"
                        "• Convert to supergroup for topics\n"
                    )
                
                help_text += (
                    "• Message formatting\n"
                    "• Auto-updates\n\n"
                    "💡 To enable topics:\n"
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
                    f"🔹 Configured Channels: {sum(len(channels) for channels in config.SERVER_CHANNEL_MAPPINGS.values()) if hasattr(config, 'SERVER_CHANNEL_MAPPINGS') else 0}\n"
                    f"🔹 Message Cache: {len(self.message_mappings)} messages\n"
                    "🔹 Update Interval: Manual refresh\n"
                    "🔹 Message Order: Chronological (oldest first)\n\n"
                    "📋 Current Topics:\n"
                )
                
                if self.server_topics:
                    for server, topic_id in self.server_topics.items():
                        status_text += f"• {server}: Topic {topic_id}\n"
                else:
                    status_text += "• No topics created yet\n"
                
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="action_start"))
                self.bot.edit_message_text(
                    status_text,
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup
                )
            elif action == 'start':
                send_welcome(call.message)
            
            self.bot.answer_callback_query(call.id)

        @self.bot.message_handler(commands=['reset_topics'])
        def reset_topics(message):
            """Reset all topic mappings - useful when topics are deleted"""
            self.server_topics.clear()
            self._save_data()
            self.bot.reply_to(message, "✅ All topic mappings have been reset. New topics will be created when needed.")

        @self.bot.message_handler(commands=['servers'])
        def list_servers(message):
            """Show interactive server list"""
            if not hasattr(config, 'SERVER_CHANNEL_MAPPINGS') or not config.SERVER_CHANNEL_MAPPINGS:
                self.bot.reply_to(message, "No servers found. Please configure servers first.")
                return
                
            markup = InlineKeyboardMarkup()
            for server in config.SERVER_CHANNEL_MAPPINGS.keys():
                markup.add(InlineKeyboardButton(
                    f"🏰 {server}",
                    callback_data=f"server_{server}"
                ))
            markup.add(InlineKeyboardButton("🔙 Back to Menu", callback_data="action_start"))
            self.bot.reply_to(message, "📋 Select a server to view announcements:", reply_markup=markup)

        @self.bot.callback_query_handler(func=lambda call: call.data == "refresh_check")
        def refresh_check(call):
            """Handle refresh check button"""
            user_id = call.from_user.id
            if user_id not in self.user_states:
                self.bot.answer_callback_query(call.id, "Please select a server first")
                return
                
            state = self.user_states[user_id]
            messages = self.discord_parser.parse_announcement_channel(
                state['channel_id'],
                state['server'],
                state['channel_name'],
                since_timestamp=state.get('last_message')
            )
            
            # Initialize last_message if not set
            if 'last_message' not in state:
                state['last_message'] = datetime.min
                
            # Filter for new messages and reverse order (oldest first)
            new_messages = [
                msg for msg in messages
                if msg.timestamp > state['last_message']
            ][:10]
            
            # REVERSE ORDER: Sort by timestamp to ensure oldest first
            new_messages.sort(key=lambda x: x.timestamp)
            
            if not new_messages:
                self.bot.answer_callback_query(call.id, "No new messages found")
                return
                
            # Get or create topic for this server
            topic_id = self._create_or_get_topic(state['server'])
            
            # Send new messages to the topic in chronological order
            sent_count = 0
            for msg in new_messages:
                try:
                    formatted = self.format_message(msg)
                    sent_msg = self._send_message(
                        formatted,
                        message_thread_id=topic_id,
                        server_name=state['server']
                    )
                    if sent_msg:
                        # Store mapping between Discord and Telegram message IDs
                        self.message_mappings[str(msg.timestamp)] = sent_msg.message_id
                        sent_count += 1
                    else:
                        print(f"Failed to send message: {formatted}")
                except Exception as e:
                    print(f"Error sending message: {e}")
                    continue
                    
            # Save mappings after sending messages
            self._save_data()
                    
            self.bot.answer_callback_query(
                call.id,
                f"Sent {sent_count} new messages to server topic!"
            )
            
            # Update last message timestamp
            if new_messages:
                self.user_states[user_id]['last_message'] = new_messages[-1].timestamp

        @self.bot.callback_query_handler(func=lambda call: call.data.startswith('server_'))
        def server_selected(call):
            """Handle server selection and show latest messages"""
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
            
            # Get last 10 messages and reverse order (oldest first)
            messages = self.discord_parser.parse_announcement_channel(
                channel_id,
                server_name,
                channel_name
            )[:10]
            
            # REVERSE ORDER: Sort by timestamp to ensure oldest first
            messages.sort(key=lambda x: x.timestamp)
            
            print(f"Fetched {len(messages)} messages from Discord (chronological order)")
            
            if not messages:
                self.bot.answer_callback_query(call.id, "No messages found")
                return
                
            # Get or create topic for this server
            topic_id = self._create_or_get_topic(server_name)
            
            # Send messages to the topic in chronological order
            sent_count = 0
            for msg in messages:
                try:
                    formatted = self.format_message(msg)
                    sent_msg = self._send_message(
                        formatted,
                        message_thread_id=topic_id,
                        server_name=server_name
                    )
                    if sent_msg:
                        # Store mapping between Discord and Telegram message IDs
                        self.message_mappings[str(msg.timestamp)] = sent_msg.message_id
                        sent_count += 1
                    else:
                        print(f"Failed to send message: {formatted}")
                except Exception as e:
                    print(f"Error sending message: {e}")
                    continue
                    
            # Save mappings after sending messages
            self._save_data()
                    
            self.bot.answer_callback_query(
                call.id,
                f"Sent {sent_count} messages to server topic"
            )
            
            # Store user state
            self.user_states[call.from_user.id] = {
                'server': server_name,
                'channel_id': channel_id,
                'channel_name': channel_name,
                'last_message': messages[-1].timestamp if messages else datetime.min
            }

        @self.bot.message_handler(func=lambda message: True)
        def handle_text_message(message):
            """Handle regular text messages"""
            pass

        @self.bot.message_handler(commands=['refresh'])
        def refresh_messages(message):
            """Check for new messages in selected channel"""
            user_id = message.from_user.id
            if user_id not in self.user_states:
                self.bot.reply_to(message, "Please select a server first using /servers")
                return
                
            state = self.user_states[user_id]
            messages = self.discord_parser.parse_announcement_channel(
                state['channel_id'],
                state['server'],
                state['channel_name'],
                since_timestamp=state.get('last_message')
            )
            
            # Initialize last_message if not set
            if 'last_message' not in state:
                state['last_message'] = datetime.min
                
            # Filter for new messages and sort in chronological order
            new_messages = [
                msg for msg in messages
                if msg.timestamp > state['last_message']
            ][:10]  # Limit to 10 new messages
            
            # REVERSE ORDER: Sort by timestamp to ensure oldest first
            new_messages.sort(key=lambda x: x.timestamp)
            
            if not new_messages:
                self.bot.reply_to(message, "No new messages found")
                return
                
            # Get or create topic for this server
            topic_id = self._create_or_get_topic(state['server'])
            
            # Send new messages to the topic in chronological order
            sent_count = 0
            for msg in new_messages:
                try:
                    formatted = self.format_message(msg)
                    sent_msg = self._send_message(
                        formatted,
                        message_thread_id=topic_id,
                        server_name=state['server']
                    )
                    if sent_msg:
                        # Store mapping between Discord and Telegram message IDs
                        self.message_mappings[str(msg.timestamp)] = sent_msg.message_id
                        sent_count += 1
                    else:
                        print(f"Failed to send message: {formatted}")
                except Exception as e:
                    print(f"Error sending message: {e}")
                    continue
                    
            # Save mappings after sending messages
            self._save_data()
                    
            self.bot.reply_to(
                message,
                f"Sent {sent_count} new messages to server topic"
            )
            
            # Update last message timestamp
            if new_messages:
                self.user_states[user_id]['last_message'] = new_messages[-1].timestamp

        print("Telegram Bot started with Topics support, error recovery, and chronological message order")
        self.bot.polling(none_stop=True)
