import asyncio
import json
import aiohttp
import time
import threading
from datetime import datetime, timedelta
from loguru import logger
from discord_telegram_parser.models.message import Message
from discord_telegram_parser.config.settings import config

class DiscordWebSocketService:
    def __init__(self, telegram_bot=None):
        self.telegram_bot = telegram_bot
        self.websockets = []
        self.heartbeat_interval = 41250
        self.session_id = None
        self.last_sequence = None
        self.subscribed_channels = set()
        self.http_accessible_channels = set()
        self.websocket_accessible_channels = set()
        self.running = False
        
        # Новые атрибуты для автообнаружения
        self.last_server_scan = 0
        self.server_scan_interval = 300  # 5 минут
        self.known_servers = set()
        self.auto_discovery_enabled = True
        self.discovery_lock = threading.Lock()
        
        # Инициализируем WebSocket сессии для каждого токена
        for token in config.DISCORD_TOKENS:
            ws_session = {
                'token': token,
                'websocket': None,
                'session': None,
                'heartbeat_task': None,
                'user_id': None,
                'connected_guilds': set(),
                'last_guild_sync': 0
            }
            self.websockets.append(ws_session)
    
    async def identify(self, websocket, token):
        """Send IDENTIFY payload с расширенными intents"""
        identify_payload = {
            "op": 2,
            "d": {
                "token": token,
                "properties": {
                    "$os": "linux",
                    "$browser": "discord_parser_enhanced",
                    "$device": "discord_parser_enhanced"
                },
                "compress": False,
                "large_threshold": 50,
                "intents": 33281  # GUILDS (1) + GUILD_MESSAGES (512) + MESSAGE_CONTENT (32768)
            }
        }
        await websocket.send_str(json.dumps(identify_payload))
        logger.info(f"🔑 Sent IDENTIFY with enhanced intents for auto-discovery")
    
    async def send_heartbeat(self, websocket, interval):
        """Send periodic heartbeat to maintain connection"""
        try:
            while self.running:
                heartbeat_payload = {
                    "op": 1,
                    "d": self.last_sequence
                }
                await websocket.send_str(json.dumps(heartbeat_payload))
                logger.debug("💓 Sent heartbeat")
                await asyncio.sleep(interval / 1000)
        except asyncio.CancelledError:
            logger.info("Heartbeat task cancelled")
        except Exception as e:
            logger.error(f"Error in heartbeat: {e}")

    async def periodic_server_discovery(self):
        """Периодическое обнаружение новых серверов"""
        while self.running:
            try:
                await asyncio.sleep(self.server_scan_interval)
                
                if not self.auto_discovery_enabled:
                    continue
                
                current_time = time.time()
                if current_time - self.last_server_scan > self.server_scan_interval:
                    logger.info("🔍 Запуск периодического сканирования новых серверов...")
                    await self.discover_and_add_new_servers()
                    self.last_server_scan = current_time
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ Ошибка в периодическом сканировании: {e}")
                await asyncio.sleep(60)

    async def discover_and_add_new_servers(self):
        """Обнаруживаем новые серверы и добавляем их в конфиг"""
        try:
            # Импортируем обнаружитель
            from discord_telegram_parser.utils.channel_id_parser import discover_new_servers_only
            
            # Выполняем в отдельном потоке
            loop = asyncio.get_event_loop()
            new_servers = await loop.run_in_executor(None, discover_new_servers_only)
            
            if new_servers:
                with self.discovery_lock:
                    # Добавляем новые серверы в конфиг
                    for server_name, channels in new_servers.items():
                        if server_name not in config.SERVER_CHANNEL_MAPPINGS:
                            config.SERVER_CHANNEL_MAPPINGS[server_name] = channels
                            self.known_servers.add(server_name)
                            
                            # ИСПРАВЛЕНИЕ: Добавляем каналы в подписки
                            for channel_id in channels.keys():
                                self.subscribed_channels.add(channel_id)
                                logger.info(f"📡 Канал добавлен в подписку: {channel_id}")
                            
                            logger.success(f"🆕 Автоматически добавлен сервер: {server_name} ({len(channels)} каналов)")
                
                # Создаем топики в Telegram для новых серверов
                if self.telegram_bot:
                    for server_name in new_servers.keys():
                        try:
                            topic_id = await loop.run_in_executor(
                        None,
                        self.telegram_bot._get_or_create_topic_safe,
                        message.server_name
                    )
                    
                    if topic_id is None:
                        logger.error(f"❌ Failed to get/create topic for {message.server_name}")
                        return
                else:
                    logger.error("❌ Cannot create topic - chat doesn't support topics")
                    return
            else:
                logger.info(f"✅ Using existing topic {topic_id} for {message.server_name}")
            
            # Форматируем и отправляем сообщение
            formatted = self.telegram_bot.format_message(message)
            sent_msg = await loop.run_in_executor(
                None,
                self.telegram_bot._send_message,
                formatted,
                None,
                topic_id,
                message.server_name
            )
            
            if sent_msg:
                self.telegram_bot.message_mappings[str(message.timestamp)] = sent_msg.message_id
                self.telegram_bot._save_data()
                
                topic_info = f" to topic {topic_id}" if topic_id else " as regular message"
                logger.success(f"✅ Successfully forwarded{topic_info}")
            else:
                logger.error("❌ Failed to send to Telegram")
            
        except Exception as e:
            logger.error(f"❌ Error forwarding to Telegram: {e}")

    async def connect_websocket(self, ws_session):
        """Подключение к Discord Gateway WebSocket"""
        try:
            # Получаем URL Gateway
            async with aiohttp.ClientSession() as session:
                async with session.get('https://discord.com/api/v9/gateway') as resp:
                    gateway_data = await resp.json()
                    gateway_url = gateway_data['url']
            
            # Подключаемся к WebSocket
            ws_session['session'] = aiohttp.ClientSession()
            ws_session['websocket'] = await ws_session['session'].ws_connect(
                f"{gateway_url}/?v=9&encoding=json"
            )
            
            logger.info(f"🔗 Connected to Discord Gateway: {gateway_url}")
            
            # Слушаем сообщения
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
        """Очистка WebSocket соединения"""
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
        """Запуск WebSocket сервиса с автообнаружением"""
        self.running = True
        logger.info("🚀 Starting Enhanced Discord WebSocket service with auto-discovery...")
        
        # Инициализируем известные серверы
        self.known_servers = set(config.SERVER_CHANNEL_MAPPINGS.keys())
        
        # Запускаем периодическое обнаружение
        discovery_task = asyncio.create_task(self.periodic_server_discovery())
        
        # Запускаем WebSocket подключения
        websocket_tasks = []
        for ws_session in self.websockets:
            task = asyncio.create_task(self.connect_websocket(ws_session))
            websocket_tasks.append(task)
        
        try:
            await asyncio.gather(discovery_task, *websocket_tasks)
        except Exception as e:
            logger.error(f"Error in WebSocket service: {e}")
        finally:
            self.running = False

    async def stop(self):
        """Остановка WebSocket сервиса"""
        self.running = False
        logger.info("Stopping Enhanced Discord WebSocket service...")
        
        for ws_session in self.websockets:
            await self.cleanup_websocket(ws_session)

    def add_channel_subscription(self, channel_id):
        """Добавление канала в подписки"""
        self.subscribed_channels.add(channel_id)
        logger.info(f"Added channel {channel_id} to subscriptions")

    def remove_channel_subscription(self, channel_id):
        """Удаление канала из подписок"""
        self.subscribed_channels.discard(channel_id)
        self.http_accessible_channels.discard(channel_id)
        self.websocket_accessible_channels.discard(channel_id)
        logger.info(f"Removed channel {channel_id} from subscriptions")

    def enable_auto_discovery(self):
        """Включить автообнаружение"""
        self.auto_discovery_enabled = True
        logger.info("🔍 Auto-discovery ENABLED")

    def disable_auto_discovery(self):
        """Отключить автообнаружение"""
        self.auto_discovery_enabled = False
        logger.info("🔍 Auto-discovery DISABLED")

    def get_discovery_stats(self):
        """Получить статистику автообнаружения"""
        return {
            'auto_discovery_enabled': self.auto_discovery_enabled,
            'known_servers': len(self.known_servers),
            'subscribed_channels': len(self.subscribed_channels),
            'last_server_scan': self.last_server_scan,
            'scan_interval': self.server_scan_interval
        }d = await loop.run_in_executor(
                                None,
                                self.telegram_bot._get_or_create_topic_safe,
                                server_name
                            )
                            if topic_id:
                                logger.success(f"📋 Создан топик для нового сервера {server_name}: {topic_id}")
                        except Exception as e:
                            logger.error(f"❌ Ошибка создания топика для {server_name}: {e}")
                
                # Сохраняем обновленный конфиг
                self._save_updated_config()
                
                logger.success(f"🎉 Автообнаружение завершено: добавлено {len(new_servers)} новых серверов")
            else:
                logger.debug("ℹ️ Новых серверов не обнаружено")
                
        except Exception as e:
            logger.error(f"❌ Ошибка автообнаружения серверов: {e}")

    def _save_updated_config(self):
        """Сохраняем обновленный конфиг в файл"""
        try:
            config_file_path = 'discord_telegram_parser/config/settings.py'
            
            # Читаем текущий файл
            with open(config_file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Создаем новую секцию конфигурации
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            new_mappings = f"\n\n# Auto-updated server mappings - {timestamp}\nconfig.SERVER_CHANNEL_MAPPINGS = {json.dumps(config.SERVER_CHANNEL_MAPPINGS, indent=2, ensure_ascii=False)}\n"
            
            # Добавляем в конец файла
            content += new_mappings
            
            # Записываем обратно
            with open(config_file_path, 'w', encoding='utf-8') as f:
                f.write(content)
                
            logger.info("💾 Конфигурация обновлена и сохранена")
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения конфига: {e}")

    async def handle_gateway_message(self, data, ws_session):
        """Обработка сообщений от Discord Gateway с поддержкой автообнаружения"""
        try:
            if data['op'] == 10:  # HELLO
                self.heartbeat_interval = data['d']['heartbeat_interval']
                logger.info(f"👋 Received HELLO, heartbeat interval: {self.heartbeat_interval}ms")
                
                # Start heartbeat
                ws_session['heartbeat_task'] = asyncio.create_task(
                    self.send_heartbeat(ws_session['websocket'], self.heartbeat_interval)
                )
                
                # Send IDENTIFY
                await self.identify(ws_session['websocket'], ws_session['token'])
                
            elif data['op'] == 11:  # HEARTBEAT_ACK
                logger.debug("💚 Received heartbeat ACK")
                
            elif data['op'] == 0:  # DISPATCH
                self.last_sequence = data['s']
                event_type = data['t']
                
                if event_type == 'READY':
                    await self._handle_ready_event(data['d'], ws_session)
                    
                elif event_type == 'MESSAGE_CREATE':
                    await self.handle_new_message(data['d'])
                    
                elif event_type == 'GUILD_CREATE':
                    await self._handle_guild_create_event(data['d'], ws_session)
                    
                elif event_type == 'GUILD_DELETE':
                    await self._handle_guild_delete_event(data['d'], ws_session)
                    
        except Exception as e:
            logger.error(f"Error handling gateway message: {e}")

    async def _handle_ready_event(self, ready_data, ws_session):
        """Обработка READY события"""
        self.session_id = ready_data['session_id']
        ws_session['user_id'] = ready_data['user']['id']
        user = ready_data['user']
        guilds = ready_data['guilds']
        
        logger.success(f"🚀 WebSocket ready for user: {user['username']}")
        logger.info(f"🏰 Connected to {len(guilds)} guilds")
        
        # Сохраняем подключенные серверы
        ws_session['connected_guilds'] = {g['id'] for g in guilds}
        
        # Инициализируем известные серверы
        if not self.known_servers:
            self.known_servers = set(config.SERVER_CHANNEL_MAPPINGS.keys())
        
        # Выполняем гибридную верификацию каналов
        await self.hybrid_channel_verification(ws_session, guilds)

    async def _handle_guild_create_event(self, guild_data, ws_session):
        """Обработка GUILD_CREATE - новый сервер присоединился"""
        guild_id = guild_data['id']
        guild_name = guild_data['name']
        
        # Проверяем, является ли это действительно новым сервером
        if guild_id not in ws_session['connected_guilds']:
            logger.info(f"🆕 НОВЫЙ СЕРВЕР ОБНАРУЖЕН: {guild_name} (ID: {guild_id})")
            
            # Добавляем в подключенные серверы
            ws_session['connected_guilds'].add(guild_id)
            
            # Если автообнаружение включено, обрабатываем новый сервер
            if self.auto_discovery_enabled and guild_name not in self.known_servers:
                await self._process_new_guild_real_time(guild_data, ws_session)
        else:
            # Это существующий сервер, просто обновляем каналы
            await self.process_guild_channels(guild_data, ws_session)

    async def _handle_guild_delete_event(self, guild_data, ws_session):
        """Обработка GUILD_DELETE - сервер покинут или недоступен"""
        guild_id = guild_data['id']
        unavailable = guild_data.get('unavailable', False)
        
        if unavailable:
            logger.warning(f"⚠️ Сервер {guild_id} временно недоступен")
        else:
            logger.info(f"👋 Покинули сервер {guild_id}")
            ws_session['connected_guilds'].discard(guild_id)

    async def _process_new_guild_real_time(self, guild_data, ws_session):
        """Обработка нового сервера в реальном времени с ПОЛНОЙ синхронизацией"""
        guild_name = guild_data['name']
        guild_id = guild_data['id']
        
        logger.info(f"🔍 Обрабатываем новый сервер в реальном времени: {guild_name}")
        
        try:
            # Получаем announcement каналы
            announcement_channels = await self._extract_announcement_channels_from_guild(guild_data)
            
            if announcement_channels:
                with self.discovery_lock:
                    # Добавляем в конфиг
                    config.SERVER_CHANNEL_MAPPINGS[guild_name] = {}
                    for channel_id, channel_info in announcement_channels.items():
                        config.SERVER_CHANNEL_MAPPINGS[guild_name][channel_id] = channel_info['name']
                        # ВАЖНО: Добавляем в подписки WebSocket
                        self.subscribed_channels.add(channel_id)
                        self.websocket_accessible_channels.add(channel_id)
                        logger.info(f"  📡 Добавлен в WebSocket подписку: {channel_info['name']} ({channel_id})")
                    
                    self.known_servers.add(guild_name)
                
                logger.success(f"✅ Автоматически добавлен новый сервер: {guild_name} ({len(announcement_channels)} каналов)")
                
                # Создаем топик в Telegram
                if self.telegram_bot:
                    try:
                        loop = asyncio.get_event_loop()
                        topic_id = await loop.run_in_executor(
                            None,
                            self.telegram_bot._get_or_create_topic_safe,
                            guild_name
                        )
                        if topic_id:
                            logger.success(f"📋 Создан топик для нового сервера {guild_name}: {topic_id}")
                    except Exception as e:
                        logger.error(f"❌ Ошибка создания топика для {guild_name}: {e}")
                
                # НОВОЕ: Выполняем начальную синхронизацию сообщений
                await self._sync_new_server_messages(guild_name, announcement_channels, ws_session)
                
                # Сохраняем конфиг
                self._save_updated_config()
                
                # Отправляем уведомление в Telegram
                if self.telegram_bot:
                    await self._send_new_server_notification(guild_name, len(announcement_channels))
            else:
                logger.info(f"ℹ️ В новом сервере {guild_name} не найдены announcement каналы")
                
        except Exception as e:
            logger.error(f"❌ Ошибка обработки нового сервера {guild_name}: {e}")

    async def _sync_new_server_messages(self, server_name, announcement_channels, ws_session):
        """Синхронизация сообщений из новых каналов"""
        logger.info(f"📥 Синхронизация сообщений для нового сервера: {server_name}")
        
        try:
            # Импортируем парсер для HTTP запросов
            from discord_telegram_parser.main import DiscordParser
            
            # Создаем временный парсер
            parser = DiscordParser()
            all_messages = []
            
            for channel_id, channel_info in announcement_channels.items():
                channel_name = channel_info['name']
                
                try:
                    # Получаем последние 5 сообщений из канала
                    messages = parser.parse_announcement_channel(
                        channel_id, 
                        server_name,
                        channel_name,
                        limit=5
                    )
                    
                    if messages:
                        # Очищаем содержимое от проблем с кодировкой
                        for msg in messages:
                            msg.content = self.safe_encode_string(msg.content)
                            msg.author = self.safe_encode_string(msg.author)
                            msg.server_name = self.safe_encode_string(msg.server_name)
                            msg.channel_name = self.safe_encode_string(msg.channel_name)
                        
                        all_messages.extend(messages)
                        logger.success(f"  📥 {channel_name}: получено {len(messages)} сообщений")
                    else:
                        logger.info(f"  ℹ️ {channel_name}: сообщения не найдены")
                        
                except Exception as e:
                    logger.warning(f"  ❌ Ошибка синхронизации {channel_name}: {e}")
                    continue
            
            # Отправляем сообщения в Telegram если есть
            if all_messages:
                # Сортируем по времени (старые сначала)
                all_messages.sort(key=lambda x: x.timestamp)
                
                # Отправляем через Telegram бота
                if self.telegram_bot:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(
                        None,
                        self.telegram_bot.send_messages,
                        all_messages
                    )
                    
                    logger.success(f"✅ Отправлено {len(all_messages)} сообщений для {server_name}")
                else:
                    logger.warning("❌ Telegram bot недоступен для отправки сообщений")
            else:
                logger.info(f"ℹ️ Нет сообщений для синхронизации в {server_name}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка синхронизации сообщений для {server_name}: {e}")

    def safe_encode_string(self, text):
        """Безопасное кодирование строк"""
        if not text:
            return ""
        try:
            if isinstance(text, str):
                text = text.encode('utf-8', 'surrogatepass').decode('utf-8', 'replace')
                text = ''.join(char for char in text if ord(char) < 0x110000)
            return text
        except (UnicodeEncodeError, UnicodeDecodeError):
            return "[Encoding Error]"

    async def _extract_announcement_channels_from_guild(self, guild_data):
        """Извлекаем announcement каналы из данных сервера"""
        channels = guild_data.get('channels', [])
        announcement_channels = {}
        
        # Приоритетные keywords для поиска
        priority_keywords = [
            'announcements',     # Высший приоритет
            'announcement', 
            'news',
            'updates',
            'важное',
            'объявления'
        ]
        
        for channel in channels:
            if channel['type'] not in [0, 5]:  # Только текстовые и announcement каналы
                continue
                
            channel_name = channel['name'].lower()
            
            # 1. Официальный announcement тип (приоритет 1)
            if channel.get('type') == 5:
                announcement_channels[channel['id']] = {
                    'name': channel['name'],
                    'type': channel['type'],
                    'priority': 1
                }
                continue
            
            # 2. Точное совпадение "announcements" (приоритет 2)
            if channel_name == 'announcements':
                announcement_channels[channel['id']] = {
                    'name': channel['name'],
                    'type': channel['type'],
                    'priority': 2
                }
                continue
            
            # 3. Содержит приоритетные keywords (приоритет 3-10)
            for i, keyword in enumerate(priority_keywords):
                if keyword in channel_name:
                    announcement_channels[channel['id']] = {
                        'name': channel['name'],
                        'type': channel['type'],
                        'priority': 3 + i
                    }
                    break
        
        # Возвращаем топ-3 канала по приоритету
        sorted_channels = dict(sorted(
            announcement_channels.items(),
            key=lambda x: x[1]['priority']
        )[:3])
        
        return sorted_channels

    async def _send_new_server_notification(self, server_name, channels_count):
        """Отправляем уведомление о новом сервере в Telegram"""
        try:
            notification_text = (
                f"🆕 АВТОМАТИЧЕСКИ ДОБАВЛЕН НОВЫЙ СЕРВЕР!\n\n"
                f"🏰 Сервер: {server_name}\n"
                f"📢 Announcement каналов: {channels_count}\n"
                f"⚡ Обнаружен через WebSocket в реальном времени\n"
                f"📋 Топик создан автоматически\n"
                f"📥 Последние сообщения синхронизированы\n\n"
                f"Мониторинг уже активен! 🚀"
            )
            
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                self.telegram_bot._send_message,
                notification_text,
                None,  # chat_id
                None,  # message_thread_id
                None   # server_name
            )
            
        except Exception as e:
            logger.error(f"❌ Ошибка отправки уведомления о новом сервере: {e}")

    async def test_http_access(self, channel_id, server_name, channel_name, token):
        """Test HTTP API access"""
        try:
            async with aiohttp.ClientSession() as session:
                headers = {'Authorization': token}
                
                async with session.get(
                    f'https://discord.com/api/v9/channels/{channel_id}/messages?limit=1',
                    headers=headers
                ) as resp:
                    return resp.status == 200
                        
        except Exception:
            return False

    def check_websocket_channel_access(self, channel_id, guilds_data):
        """Check if channel is accessible via WebSocket guild data"""
        for guild in guilds_data:
            channels = guild.get('channels', [])
            for channel in channels:
                if channel['id'] == channel_id:
                    # Проверяем в порядке приоритета
                    if channel['type'] == 0 and channel['name'].lower() == 'announcements':
                        return True
                    if channel.get('type') == 5:
                        return True
                    if (channel['type'] == 0 and 
                        any(keyword in channel['name'].lower() 
                            for keyword in ['announcements', 'announcement'])):
                        return True
        return False

    async def hybrid_channel_verification(self, ws_session, guilds_data):
        """Гибридная верификация: HTTP + WebSocket каналов"""
        logger.info("🔍 Starting hybrid channel verification...")
        
        http_working = []
        websocket_only = []
        total_monitoring = []
        failed_completely = []
        
        # Проверяем все каналы из конфига
        for server, channels in config.SERVER_CHANNEL_MAPPINGS.items():
            if not channels:
                continue
                
            for channel_id, channel_name in channels.items():
                logger.debug(f"🧪 Testing {server}#{channel_name}...")
                
                # Тест 1: HTTP API
                http_works = await self.test_http_access(
                    channel_id, server, channel_name, ws_session['token']
                )
                
                # Тест 2: WebSocket
                websocket_works = self.check_websocket_channel_access(channel_id, guilds_data)
                
                if http_works and websocket_works:
                    self.http_accessible_channels.add(channel_id)
                    self.websocket_accessible_channels.add(channel_id)
                    self.subscribed_channels.add(channel_id)
                    http_working.append((server, channel_name, channel_id))
                    total_monitoring.append((server, channel_name, channel_id, "HTTP+WS"))
                    
                elif not http_works and websocket_works:
                    self.websocket_accessible_channels.add(channel_id)
                    self.subscribed_channels.add(channel_id)
                    websocket_only.append((server, channel_name, channel_id))
                    total_monitoring.append((server, channel_name, channel_id, "WS only"))
                    
                elif http_works and not websocket_works:
                    self.http_accessible_channels.add(channel_id)
                    logger.warning(f"   ⚠️ {server}#{channel_name} - HTTP only")
                    
                else:
                    failed_completely.append((server, channel_name, channel_id))

        # Выводим статистику
        logger.info(f"\n📊 Channel Verification Results:")
        logger.info(f"   🎉 Full access (HTTP+WS): {len(http_working)} channels")
        logger.info(f"   🔌 WebSocket only: {len(websocket_only)} channels")
        logger.info(f"   📡 Total monitoring: {len(total_monitoring)} channels")
        logger.info(f"   ❌ Failed: {len(failed_completely)} channels")
        
        return len(total_monitoring)

    async def process_guild_channels(self, guild_data, ws_session):
        """Обработка каналов сервера"""
        try:
            guild_name = guild_data['name']
            
            # Если это новый сервер, обрабатываем его
            if guild_name not in self.known_servers and self.auto_discovery_enabled:
                await self._process_new_guild_real_time(guild_data, ws_session)
                        
        except Exception as e:
            logger.error(f"Error processing guild channels: {e}")

    async def handle_new_message(self, message_data):
        """Обработка нового сообщения с проверкой подписок"""
        try:
            channel_id = message_data['channel_id']
            
            # ВАЖНО: Проверяем подписку на канал
            if channel_id not in self.subscribed_channels:
                logger.debug(f"🔇 Сообщение из неподписанного канала {channel_id} - игнорируем")
                return
            
            # Логируем для отладки
            logger.debug(f"📨 Сообщение из подписанного канала {channel_id}")
            
            # Находим информацию о канале
            server_name = None
            channel_name = None
            
            for server, channels in config.SERVER_CHANNEL_MAPPINGS.items():
                if channel_id in channels:
                    server_name = server
                    channel_name = channels[channel_id]
                    break
            
            if not server_name:
                logger.warning(f"⚠️ Сообщение из подписанного но немапленного канала {channel_id}")
                return
            
            # Безопасная обработка контента
            try:
                content = message_data.get('content', '')
                if content:
                    content = content.encode('utf-8', 'surrogatepass').decode('utf-8', 'replace')
                    content = ''.join(char for char in content if ord(char) < 0x110000)
                else:
                    return
            except:
                content = '[Message content encoding error]'
            
            try:
                author = message_data['author']['username']
                author = author.encode('utf-8', 'surrogatepass').decode('utf-8', 'replace')
                author = ''.join(char for char in author if ord(char) < 0x110000)
            except:
                author = 'Unknown User'
            
            if not content.strip():
                return
            
            # Создаем объект сообщения
            message = Message(
                content=content,
                timestamp=datetime.fromisoformat(message_data['timestamp'].replace('Z', '+00:00')),
                server_name=server_name,
                channel_name=channel_name,
                author=author
            )
            
            # Определяем тип доступа для логирования
            access_type = ""
            if channel_id in self.http_accessible_channels and channel_id in self.websocket_accessible_channels:
                access_type = " (HTTP+WS)"
            elif channel_id in self.websocket_accessible_channels:
                access_type = " (WS only)"
            
            logger.info(f"🎉 NEW MESSAGE RECEIVED{access_type}!")
            logger.info(f"   📍 {server_name}#{channel_name}")
            logger.info(f"   👤 {author}")
            logger.info(f"   💬 {content[:100]}...")
            
            # Пересылаем в Telegram
            if self.telegram_bot:
                await self.forward_to_telegram(message)
                
        except Exception as e:
            logger.error(f"❌ Error handling new message: {e}")

    async def forward_to_telegram(self, message):
        """Пересылка сообщения в Telegram с правильным управлением топиками"""
        try:
            logger.info(f"🚀 Forwarding to Telegram: {message.server_name}#{message.channel_name}")
            
            loop = asyncio.get_event_loop()
            
            # Получаем существующий топик
            topic_id = await loop.run_in_executor(
                None,
                self.telegram_bot.get_server_topic_id,
                message.server_name
            )
            
            # Создаем новый топик только если нужно
            if topic_id is None:
                if self.telegram_bot._check_if_supergroup_with_topics(config.TELEGRAM_CHAT_ID):
                    logger.info(f"🔍 No existing topic found for {message.server_name}, creating new one...")
                    topic_i