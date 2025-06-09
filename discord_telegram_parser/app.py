import json
import os
import asyncio
import threading
import time
from loguru import logger
from datetime import datetime

from .services.telegram_bot import TelegramBotService
from .services.discord_websocket import EnhancedDiscordWebSocketService
from .config.settings import config
from .main import DiscordParser

class EnhancedDiscordTelegramParser:
    def __init__(self):
        # Перезагружаем переменные окружения
        from dotenv import load_dotenv
        load_dotenv(override=True)
        
        self.discord_parser = DiscordParser()
        self.telegram_bot = TelegramBotService(config.TELEGRAM_BOT_TOKEN)
        
        # Используем улучшенный WebSocket сервис
        self.websocket_service = EnhancedDiscordWebSocketService(self.telegram_bot)
        
        # Перекрестные ссылки
        self.telegram_bot.discord_parser = self.discord_parser
        self.telegram_bot.websocket_service = self.websocket_service
        
        self.running = False
        self.websocket_task = None
        
    def discover_all_servers(self):
        """Полное обнаружение всех серверов с улучшенным алгоритмом"""
        try:
            from discord_telegram_parser.utils.channel_id_parser import parse_discord_servers
            
            logger.info("🔍 Запускаем полное обнаружение серверов...")
            mappings = parse_discord_servers()
            
            if mappings:
                # Сохраняем количество найденных серверов
                old_count = len(config.SERVER_CHANNEL_MAPPINGS)
                config.SERVER_CHANNEL_MAPPINGS = mappings
                new_count = len(mappings)
                
                logger.success(f"✅ Обнаружение завершено:")
                logger.info(f"   📊 Было серверов: {old_count}")
                logger.info(f"   📊 Стало серверов: {new_count}")
                logger.info(f"   📊 Прирост: +{new_count - old_count}")
                
                # Добавляем обнаруженные каналы в WebSocket подписки
                for server, channels in mappings.items():
                    for channel_id in channels.keys():
                        self.websocket_service.add_channel_subscription(channel_id)
                
                # Сохраняем конфиг
                self._save_config_to_file(mappings)
                
                return mappings
            else:
                logger.error("❌ Обнаружение серверов не дало результатов")
                return {}
                
        except Exception as e:
            logger.error(f"❌ Ошибка при обнаружении серверов: {e}")
            return {}

    def _save_config_to_file(self, mappings):
        """Сохраняем обновленную конфигурацию в файл"""
        try:
            config_file = 'discord_telegram_parser/config/settings.py'
            
            # Читаем существующий файл
            with open(config_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Подготавливаем новую секцию конфигурации
            new_config_section = f"\n# Auto-discovered servers - Updated {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\nconfig.SERVER_CHANNEL_MAPPINGS = {json.dumps(mappings, indent=2, ensure_ascii=False)}\n"
            
            # Добавляем в конец файла
            content += new_config_section
            
            # Записываем обратно
            with open(config_file, 'w', encoding='utf-8') as f:
                f.write(content)
                
            logger.info(f"💾 Конфигурация сохранена: {len(mappings)} серверов")
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения конфигурации: {e}")

    async def websocket_main_loop(self):
        """Главный async цикл для WebSocket сервиса с автообнаружением"""
        while self.running:
            try:
                logger.info("🚀 Starting WebSocket connections with auto-discovery...")
                await self.websocket_service.start()
            except Exception as e:
                error_msg = str(e).encode('utf-8', 'replace').decode('utf-8')
                logger.error(f"WebSocket error: {error_msg}")
                logger.info("Restarting WebSocket in 30 seconds...")
                await asyncio.sleep(30)
    
    def run_websocket_in_thread(self):
        """Запуск WebSocket сервиса в отдельном потоке с async loop"""
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
        """Безопасное кодирование строк для обработки Unicode"""
        if not text:
            return ""
        try:
            if isinstance(text, str):
                text = text.encode('utf-8', 'surrogatepass').decode('utf-8', 'replace')
                text = ''.join(char for char in text if ord(char) < 0x110000)
            return text
        except (UnicodeEncodeError, UnicodeDecodeError):
            return "[Encoding Error]"
    
    def test_channel_http_access(self, channel_id):
        """Быстрый тест доступности канала через HTTP"""
        try:
            session = self.discord_parser.sessions[0]
            r = session.get(f'https://discord.com/api/v9/channels/{channel_id}/messages?limit=1')
            return r.status_code == 200
        except:
            return False
    
    def sync_servers_enhanced(self):
        """Улучшенная синхронизация серверов между Discord и Telegram"""
        try:
            # Получаем текущие серверы Discord
            current_servers = set(config.SERVER_CHANNEL_MAPPINGS.keys())
            
            # Получаем топики Telegram
            telegram_topics = set(self.telegram_bot.server_topics.keys())
            
            logger.info(f"🔄 Enhanced server sync...")
            logger.info(f"   📊 Discord servers: {len(current_servers)}")
            logger.info(f"   📊 Telegram topics: {len(telegram_topics)}")
            logger.info(f"   🔍 Auto-discovery: {'ENABLED' if self.websocket_service.auto_discovery_enabled else 'DISABLED'}")
            
            # Очищаем недействительные топики
            cleaned_topics = self.telegram_bot.cleanup_invalid_topics()
            if cleaned_topics > 0:
                logger.info(f"   🧹 Cleaned {cleaned_topics} invalid topics")
                telegram_topics = set(self.telegram_bot.server_topics.keys())
            
            # Находим новые серверы (топики будут созданы при необходимости)
            new_servers = current_servers - telegram_topics
            if new_servers:
                logger.info(f"   🆕 New servers found: {len(new_servers)}")
                for server in new_servers:
                    logger.info(f"      • {server} (topic will be created when needed)")
            
            # Находим удаленные серверы
            removed_servers = telegram_topics - current_servers
            if removed_servers:
                logger.info(f"   🗑️ Removing topics for deleted servers: {len(removed_servers)}")
                for server in removed_servers:
                    if server in self.telegram_bot.server_topics:
                        old_topic_id = self.telegram_bot.server_topics[server]
                        del self.telegram_bot.server_topics[server]
                        logger.info(f"      • Removed {server} (topic {old_topic_id})")
                
                if removed_servers:
                    self.telegram_bot._save_data()
            
            logger.success(f"✅ Enhanced server sync completed")
            
        except Exception as e:
            error_msg = str(e).encode('utf-8', 'replace').decode('utf-8')
            logger.error(f"❌ Error in enhanced server sync: {error_msg}")

    def initial_sync_enhanced(self):
        """Улучшенная начальная синхронизация с полным обнаружением"""
        try:
            # Шаг 1: Обнаруживаем ВСЕ серверы (не только настроенные)
            logger.info("🔍 Step 1: Discovering ALL servers...")
            discovered_servers = self.discover_all_servers()
            
            if not discovered_servers:
                logger.warning("⚠️ No servers discovered, using existing config")
                discovered_servers = config.SERVER_CHANNEL_MAPPINGS
            
            # Шаг 2: Синхронизируем серверы между Discord и Telegram
            logger.info("🔄 Step 2: Enhanced server synchronization...")
            self.sync_servers_enhanced()
            
            # Шаг 3: Получаем последние сообщения из HTTP-доступных каналов
            logger.info("📥 Step 3: Smart initial sync (HTTP-accessible channels)...")
            messages = []
            http_channels = []
            websocket_only_channels = []
            
            for server, channels in discovered_servers.items():
                if not channels:
                    continue
                
                safe_server = self.safe_encode_string(server)
                    
                for channel_id, channel_name in channels.items():
                    safe_channel = self.safe_encode_string(channel_name)
                    
                    # Тест HTTP доступности
                    if self.test_channel_http_access(channel_id):
                        try:
                            recent_messages = self.discord_parser.parse_announcement_channel(
                                channel_id, 
                                safe_server,
                                safe_channel,
                                limit=3  # Меньше сообщений для быстрого старта
                            )
                            
                            # Очистка контента от проблем с кодировкой
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
                        websocket_only_channels.append((safe_server, safe_channel))
                        logger.info(f"🔌 WebSocket only: {safe_server}#{safe_channel} - will monitor via WebSocket")
            
            # Шаг 4: Статистика и отправка сообщений
            logger.info(f"📊 Enhanced initial sync summary:")
            logger.info(f"   📁 Total servers discovered: {len(discovered_servers)}")
            logger.info(f"   ✅ HTTP synced: {len(http_channels)} channels")
            logger.info(f"   🔌 WebSocket only: {len(websocket_only_channels)} channels")
            logger.info(f"   📨 Total messages: {len(messages)}")
            logger.info(f"   🔍 Auto-discovery: ENABLED for real-time detection")
            
            if websocket_only_channels:
                logger.info(f"🔌 These channels will be monitored via WebSocket only:")
                for server, channel in websocket_only_channels[:10]:  # Показываем первые 10
                    logger.info(f"   • {server}#{channel}")
                if len(websocket_only_channels) > 10:
                    logger.info(f"   • ... and {len(websocket_only_channels) - 10} more")
            
            # Группируем сообщения по серверам и отправляем в Telegram
            if messages:
                messages.sort(key=lambda x: x.timestamp)
                
                server_messages = {}
                for msg in messages:
                    server = msg.server_name
                    if server not in server_messages:
                        server_messages[server] = []
                    server_messages[server].append(msg)
                
                logger.info(f"📤 Sending messages for {len(server_messages)} servers...")
                
                for server, msgs in server_messages.items():
                    logger.info(f"   📍 {server}: {len(msgs)} messages")
                    self.telegram_bot.send_messages(msgs)
                
                logger.success(f"✅ Enhanced initial sync completed: {len(messages)} messages sent")
            else:
                logger.info("ℹ️ No HTTP messages found during initial sync")
            
            logger.success(f"🎉 Enhanced smart initial sync complete! WebSocket will handle real-time monitoring with auto-discovery.")
            
        except Exception as e:
            try:
                error_msg = str(e).encode('utf-8', 'replace').decode('utf-8')
            except:
                error_msg = "Enhanced initial sync error (encoding issue)"
            logger.error(f"❌ Error in enhanced initial sync: {error_msg}")
    
    def enhanced_fallback_polling_loop(self):
        """Улучшенный резервный поллинг с управлением нагрузкой"""
        while self.running:
            try:
                time.sleep(600)  # Проверяем каждые 10 минут (меньше нагрузки)
                
                if not config.SERVER_CHANNEL_MAPPINGS:
                    continue
                
                logger.debug("🔄 Enhanced fallback polling check...")
                
                server_messages = {}
                recent_threshold = datetime.now().timestamp() - 300  # 5 минут назад
                
                # Проверяем только HTTP-доступные каналы для экономии ресурсов
                http_channels_checked = 0
                for server, channels in config.SERVER_CHANNEL_MAPPINGS.items():
                    safe_server = self.safe_encode_string(server)
                    
                    for channel_id, channel_name in channels.items():
                        # Пропускаем каналы, доступные только через WebSocket
                        if not self.test_channel_http_access(channel_id):
                            continue
                            
                        http_channels_checked += 1
                        if http_channels_checked > 20:  # Ограничиваем нагрузку
                            break
                            
                        try:
                            safe_channel = self.safe_encode_string(channel_name)
                            
                            recent_messages = self.discord_parser.parse_announcement_channel(
                                channel_id, 
                                safe_server,
                                safe_channel,
                                limit=2  # Еще меньше сообщений для резервного поллинга
                            )
                            
                            # Очистка контента
                            for msg in recent_messages:
                                msg.content = self.safe_encode_string(msg.content)
                                msg.author = self.safe_encode_string(msg.author)
                                msg.server_name = self.safe_encode_string(msg.server_name)
                                msg.channel_name = self.safe_encode_string(msg.channel_name)
                            
                            # Фильтруем очень свежие сообщения
                            new_messages = [
                                msg for msg in recent_messages
                                if msg.timestamp.timestamp() > recent_threshold
                            ]
                            
                            if new_messages:
                                if safe_server not in server_messages:
                                    server_messages[safe_server] = []
                                server_messages[safe_server].extend(new_messages)
                            
                        except Exception as e:
                            logger.debug(f"Fallback polling error for {safe_server}#{safe_channel}: {e}")
                            continue
                    
                    if http_channels_checked > 20:
                        break
                
                # Отправляем найденные сообщения
                if server_messages:
                    total_messages = sum(len(msgs) for msgs in server_messages.values())
                    logger.info(f"🔄 Fallback polling found {total_messages} new messages in {len(server_messages)} servers")
                    
                    for server, msgs in server_messages.items():
                        msgs.sort(key=lambda x: x.timestamp)
                        logger.info(f"   📍 {server}: {len(msgs)} messages")
                        self.telegram_bot.send_messages(msgs)
                
            except Exception as e:
                error_msg = str(e).encode('utf-8', 'replace').decode('utf-8')
                logger.error(f"Error in enhanced fallback polling: {error_msg}")
                time.sleep(120)  # Больше ждем при ошибке
    
    def run(self):
        """Запуск всех компонентов с улучшенными возможностями"""
        self.running = True
        
        try:
            # Выполняем улучшенную начальную синхронизацию
            logger.info("🚀 Starting enhanced initial sync with full server discovery...")
            self.initial_sync_enhanced()
            
            # Запускаем Telegram bot в отдельном потоке
            bot_thread = threading.Thread(
                target=self.telegram_bot.start_bot,
                daemon=True
            )
            bot_thread.start()
            logger.success("✅ Telegram bot started with enhanced features")
            
            # Запускаем улучшенный WebSocket сервис в отдельном потоке
            websocket_thread = self.run_websocket_in_thread()
            logger.success("✅ Enhanced WebSocket service started with auto-discovery")
            
            # Запускаем улучшенный резервный поллинг в отдельном потоке
            fallback_thread = threading.Thread(
                target=self.enhanced_fallback_polling_loop,
                daemon=True
            )
            fallback_thread.start()
            logger.success("✅ Enhanced fallback polling started")
            
            # Сохраняем статистику автообнаружения
            discovery_stats = self.websocket_service.get_discovery_stats()
            
            # Основной цикл
            logger.success("🎉 Enhanced Discord Telegram Parser running!")
            logger.info("📊 Enhanced Features:")
            logger.info("   🔍 FULL server auto-discovery (finds ALL 15+ servers)")
            logger.info("   ⚡ Real-time new server detection via WebSocket")
            logger.info("   📋 One server = One topic (no duplicates)")
            logger.info("   🧵 Thread-safe topic creation")
            logger.info("   🧹 Auto-cleanup of invalid topics")
            logger.info("   🌐 HTTP channels: Initial sync + smart fallback polling")
            logger.info("   📡 WebSocket channels: Real-time monitoring")
            logger.info("   📁 Messages grouped by server")
            logger.info("   💾 Auto-save updated configuration")
            logger.info(f"   📊 Current stats: {discovery_stats['known_servers']} servers, {discovery_stats['subscribed_channels']} channels")
            logger.info("   🚨 Automatic notifications for new servers")
            logger.info("Press Ctrl+C to stop")
            
            while self.running:
                time.sleep(5)
                
                # Периодически выводим статистику (каждые 5 минут)
                if int(time.time()) % 300 == 0:
                    stats = self.websocket_service.get_discovery_stats()
                    logger.info(f"📊 Stats: {stats['known_servers']} servers, {stats['subscribed_channels']} channels, auto-discovery: {stats['auto_discovery_enabled']}")
                
        except KeyboardInterrupt:
            logger.info("Shutting down enhanced parser...")
            self.running = False
            
            # Останавливаем WebSocket сервис
            if self.websocket_service:
                asyncio.run(self.websocket_service.stop())
                
        except Exception as e:
            error_msg = str(e).encode('utf-8', 'replace').decode('utf-8')
            logger.error(f"Error in enhanced main run loop: {error_msg}")
            self.running = False

def main():
    """Главная точка входа для улучшенного приложения"""
    logger.info("Starting Enhanced Discord Telegram Parser with full auto-discovery...")
    app = EnhancedDiscordTelegramParser()
    app.run()

if __name__ == '__main__':
    main()