import asyncio
import json
import aiohttp
import time
from datetime import datetime
from loguru import logger
from discord_telegram_parser.models.message import Message
from discord_telegram_parser.config.settings import config

class DiscordWebSocketService:
    def __init__(self, telegram_bot=None):
        self.telegram_bot = telegram_bot
        self.websockets = []
        self.heartbeat_interval = 41250  # Default heartbeat interval
        self.session_id = None
        self.last_sequence = None
        self.subscribed_channels = set()
        self.running = False
        
        # Initialize WebSocket sessions for each token
        for token in config.DISCORD_TOKENS:
            ws_session = {
                'token': token,
                'websocket': None,
                'session': None,
                'heartbeat_task': None,
                'user_id': None
            }
            self.websockets.append(ws_session)
    
    async def identify(self, websocket, token):
        """Send IDENTIFY payload to establish connection"""
        identify_payload = {
            "op": 2,
            "d": {
                "token": token,
                "properties": {
                    "$os": "linux",
                    "$browser": "discord_parser",
                    "$device": "discord_parser"
                },
                "compress": False,
                "large_threshold": 50,
                "intents": 513  # GUILD_MESSAGES + MESSAGE_CONTENT
            }
        }
        await websocket.send_str(json.dumps(identify_payload))
        logger.info("Sent IDENTIFY payload")
    
    async def send_heartbeat(self, websocket, interval):
        """Send periodic heartbeat to maintain connection"""
        try:
            while self.running:
                heartbeat_payload = {
                    "op": 1,
                    "d": self.last_sequence
                }
                await websocket.send_str(json.dumps(heartbeat_payload))
                logger.debug("Sent heartbeat")
                await asyncio.sleep(interval / 1000)
        except asyncio.CancelledError:
            logger.info("Heartbeat task cancelled")
        except Exception as e:
            logger.error(f"Error in heartbeat: {e}")
    
    async def handle_gateway_message(self, data, ws_session):
        """Handle incoming WebSocket messages from Discord Gateway"""
        try:
            if data['op'] == 10:  # HELLO
                self.heartbeat_interval = data['d']['heartbeat_interval']
                logger.info(f"Received HELLO, heartbeat interval: {self.heartbeat_interval}ms")
                
                # Start heartbeat
                ws_session['heartbeat_task'] = asyncio.create_task(
                    self.send_heartbeat(ws_session['websocket'], self.heartbeat_interval)
                )
                
                # Send IDENTIFY
                await self.identify(ws_session['websocket'], ws_session['token'])
                
            elif data['op'] == 11:  # HEARTBEAT_ACK
                logger.debug("Received heartbeat ACK")
                
            elif data['op'] == 0:  # DISPATCH
                self.last_sequence = data['s']
                event_type = data['t']
                
                if event_type == 'READY':
                    self.session_id = data['d']['session_id']
                    ws_session['user_id'] = data['d']['user']['id']
                    logger.success(f"WebSocket ready for user: {data['d']['user']['username']}")
                    
                    # Load existing channel subscriptions from config
                    await self.load_channel_subscriptions()
                    
                    # Subscribe to channels via HTTP API (more reliable)
                    await self.subscribe_to_configured_channels(ws_session)
                    
                elif event_type == 'MESSAGE_CREATE':
                    await self.handle_new_message(data['d'])
                    
                elif event_type == 'GUILD_CREATE':
                    # Auto-discover announcement channels in new guilds
                    await self.discover_guild_channels(data['d'], ws_session)
                    
        except Exception as e:
            logger.error(f"Error handling gateway message: {e}")
    
    async def load_channel_subscriptions(self):
        """Load channel subscriptions from config"""
        try:
            for server, channels in config.SERVER_CHANNEL_MAPPINGS.items():
                for channel_id in channels.keys():
                    self.subscribed_channels.add(channel_id)
                    logger.info(f"Loaded subscription for channel {channel_id} in {server}")
            
            logger.info(f"Loaded {len(self.subscribed_channels)} channel subscriptions from config")
        except Exception as e:
            logger.error(f"Error loading channel subscriptions: {e}")
    
    async def subscribe_to_configured_channels(self, ws_session):
        """Subscribe to configured announcement channels using HTTP API"""
        try:
            logger.info("Subscribing to configured announcement channels...")
            
            # Use HTTP API to verify channel access
            async with aiohttp.ClientSession() as session:
                headers = {'Authorization': ws_session['token']}
                
                for server, channels in config.SERVER_CHANNEL_MAPPINGS.items():
                    for channel_id, channel_name in channels.items():
                        try:
                            # Test channel access
                            async with session.get(
                                f'https://discord.com/api/v9/channels/{channel_id}',
                                headers=headers
                            ) as resp:
                                if resp.status == 200:
                                    self.subscribed_channels.add(channel_id)
                                    logger.success(f"✅ Subscribed to {server}#{channel_name} ({channel_id})")
                                else:
                                    logger.warning(f"❌ Cannot access {server}#{channel_name} ({channel_id}) - Status: {resp.status}")
                        except Exception as e:
                            logger.warning(f"Error checking channel {channel_id}: {e}")
                            continue
                
                logger.info(f"Active subscriptions: {len(self.subscribed_channels)} channels")
                
        except Exception as e:
            logger.error(f"Error subscribing to channels: {e}")
    
    async def discover_guild_channels(self, guild_data, ws_session):
        """Discover announcement channels in a guild"""
        try:
            guild_name = guild_data['name']
            guild_id = guild_data['id']
            
            # Find announcement channels
            announcement_channels = {}
            for channel in guild_data.get('channels', []):
                if (channel['type'] == 0 and  # Text channel
                    'announcement' in channel['name'].lower()):
                    announcement_channels[channel['id']] = channel['name']
                    self.subscribed_channels.add(channel['id'])
            
            if announcement_channels:
                # Update config with discovered channels
                if guild_name not in config.SERVER_CHANNEL_MAPPINGS:
                    config.SERVER_CHANNEL_MAPPINGS[guild_name] = {}
                
                config.SERVER_CHANNEL_MAPPINGS[guild_name].update(announcement_channels)
                logger.info(f"Discovered {len(announcement_channels)} announcement channels in {guild_name}")
                
        except Exception as e:
            logger.error(f"Error discovering guild channels: {e}")
    
    async def handle_new_message(self, message_data):
        """Process new message from WebSocket"""
        try:
            channel_id = message_data['channel_id']
            
            # Only process messages from subscribed announcement channels
            if channel_id not in self.subscribed_channels:
                logger.debug(f"Ignoring message from unsubscribed channel {channel_id}")
                return
            
            # Find server and channel info
            server_name = None
            channel_name = None
            
            for server, channels in config.SERVER_CHANNEL_MAPPINGS.items():
                if channel_id in channels:
                    server_name = server
                    channel_name = channels[channel_id]
                    break
            
            if not server_name:
                logger.warning(f"Received message from subscribed channel {channel_id} but no server mapping found")
                return
            
            # Safely extract message content with Unicode handling
            try:
                content = message_data.get('content', '')
                # Handle potential surrogate characters and encoding issues
                if content:
                    # Convert surrogates to replacement characters
                    content = content.encode('utf-8', 'surrogatepass').decode('utf-8', 'replace')
                    # Remove any remaining problematic characters
                    content = ''.join(char for char in content if ord(char) < 0x110000)
            except (UnicodeEncodeError, UnicodeDecodeError):
                content = '[Message content encoding error]'
                logger.warning(f"Unicode encoding issue in message content from {server_name}")
            
            try:
                author = message_data['author']['username']
                author = author.encode('utf-8', 'surrogatepass').decode('utf-8', 'replace')
                author = ''.join(char for char in author if ord(char) < 0x110000)
            except (UnicodeEncodeError, UnicodeDecodeError, KeyError):
                author = 'Unknown User'
                logger.warning(f"Unicode encoding issue in author name from {server_name}")
            
            # Skip empty messages
            if not content.strip():
                logger.debug(f"Skipping empty message from {server_name}#{channel_name}")
                return
            
            # Create Message object
            message = Message(
                content=content,
                timestamp=datetime.fromisoformat(message_data['timestamp'].replace('Z', '+00:00')),
                server_name=server_name,
                channel_name=channel_name,
                author=author
            )
            
            logger.info(f"📨 New message in {server_name}#{channel_name}: {author} - {content[:50]}...")
            
            # Forward to Telegram if bot is available
            if self.telegram_bot:
                await asyncio.create_task(
                    self.forward_to_telegram(message)
                )
            else:
                logger.warning("Telegram bot not available for message forwarding")
                
        except Exception as e:
            # Handle any Unicode errors in exception reporting
            try:
                error_msg = str(e).encode('utf-8', 'replace').decode('utf-8')
            except:
                error_msg = "Message processing error (encoding issue)"
            logger.error(f"Error handling new message: {error_msg}")
    
    async def forward_to_telegram(self, message):
        """Forward message to Telegram bot asynchronously"""
        try:
            logger.info(f"Forwarding message to Telegram: {message.server_name}#{message.channel_name}")
            
            # Run Telegram bot methods in executor since they're synchronous
            loop = asyncio.get_event_loop()
            
            # Get or create topic for server
            topic_id = await loop.run_in_executor(
                None,
                self.telegram_bot._create_or_get_topic,
                message.server_name
            )
            
            if not topic_id:
                logger.error(f"Failed to get/create topic for server {message.server_name}")
                return
            
            # Format and send message
            formatted = self.telegram_bot.format_message(message)
            sent_msg = await loop.run_in_executor(
                None,
                self.telegram_bot._send_message,
                formatted,
                None,  # chat_id (use default)
                topic_id,
                message.server_name
            )
            
            if sent_msg:
                # Store mapping
                self.telegram_bot.message_mappings[str(message.timestamp)] = sent_msg.message_id
                self.telegram_bot._save_data()
                logger.success(f"✅ Forwarded message to Telegram topic {topic_id}")
            else:
                logger.error(f"Failed to send message to Telegram")
            
        except Exception as e:
            logger.error(f"Error forwarding to Telegram: {e}")
    
    async def connect_websocket(self, ws_session):
        """Connect to Discord Gateway WebSocket"""
        try:
            # Get gateway URL
            async with aiohttp.ClientSession() as session:
                async with session.get('https://discord.com/api/v9/gateway') as resp:
                    gateway_data = await resp.json()
                    gateway_url = gateway_data['url']
            
            # Connect to WebSocket
            ws_session['session'] = aiohttp.ClientSession()
            ws_session['websocket'] = await ws_session['session'].ws_connect(
                f"{gateway_url}/?v=9&encoding=json"
            )
            
            logger.info(f"Connected to Discord Gateway: {gateway_url}")
            
            # Listen for messages
            async for msg in ws_session['websocket']:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    await self.handle_gateway_message(data, ws_session)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"WebSocket error: {ws_session['websocket'].exception()}")
                    break
                elif msg.type == aiohttp.WSMsgType.CLOSE:
                    logger.warning("WebSocket connection closed")
                    break
                    
        except Exception as e:
            logger.error(f"WebSocket connection error: {e}")
        finally:
            await self.cleanup_websocket(ws_session)
    
    async def cleanup_websocket(self, ws_session):
        """Clean up WebSocket connection"""
        try:
            if ws_session['heartbeat_task']:
                ws_session['heartbeat_task'].cancel()
                try:
                    await ws_session['heartbeat_task']
                except asyncio.CancelledError:
                    pass
            
            if ws_session['websocket'] and not ws_session['websocket'].closed:
                await ws_session['websocket'].close()
            
            if ws_session['session'] and not ws_session['session'].closed:
                await ws_session['session'].close()
                
        except Exception as e:
            logger.error(f"Error cleaning up WebSocket: {e}")
    
    async def start(self):
        """Start WebSocket connections for all tokens"""
        self.running = True
        logger.info("Starting Discord WebSocket service...")
        
        tasks = []
        for ws_session in self.websockets:
            task = asyncio.create_task(self.connect_websocket(ws_session))
            tasks.append(task)
        
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            logger.error(f"Error in WebSocket service: {e}")
        finally:
            self.running = False
    
    async def stop(self):
        """Stop WebSocket connections"""
        self.running = False
        logger.info("Stopping Discord WebSocket service...")
        
        for ws_session in self.websockets:
            await self.cleanup_websocket(ws_session)
    
    def add_channel_subscription(self, channel_id):
        """Add a channel to subscription list"""
        self.subscribed_channels.add(channel_id)
        logger.info(f"Added channel {channel_id} to subscriptions")
    
    def remove_channel_subscription(self, channel_id):
        """Remove a channel from subscription list"""
        self.subscribed_channels.discard(channel_id)
        logger.info(f"Removed channel {channel_id} from subscriptions")