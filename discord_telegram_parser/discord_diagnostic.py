#!/usr/bin/env python3
"""
Диагностические инструменты для Discord Telegram Parser
Проверка токенов, серверов, каналов и конфигурации
"""

import requests
import json
import os
import sys
from datetime import datetime
from loguru import logger
from dotenv import load_dotenv
from discord_telegram_parser.config.settings import config

class EnhancedDiagnosticTool:
    def __init__(self):
        load_dotenv()
        self.tokens = [t.strip() for t in os.getenv('DISCORD_AUTH_TOKENS', '').split(',') if t.strip()]
        self.sessions = []
        self.guild_data = {}
        
        # Инициализируем сессии
        self._init_sessions()
    
    def _init_sessions(self):
        """Инициализация сессий для каждого токена"""
        for token in self.tokens:
            session = requests.Session()
            session.headers = {'Authorization': token}
            
            try:
                r = session.get('https://discord.com/api/v9/users/@me')
                if r.status_code == 200:
                    user_info = r.json()
                    self.sessions.append({
                        'session': session,
                        'token': token,
                        'user_info': user_info,
                        'valid': True
                    })
                    logger.info(f"✅ Токен валидный: {user_info['username']}#{user_info['discriminator']}")
                else:
                    logger.error(f"❌ Невалидный токен: {token[:20]}...")
                    self.sessions.append({
                        'session': session,
                        'token': token,
                        'user_info': None,
                        'valid': False
                    })
            except Exception as e:
                logger.error(f"❌ Ошибка проверки токена: {e}")
                self.sessions.append({
                    'session': session,
                    'token': token,
                    'user_info': None,
                    'valid': False
                })
    
    def check_tokens(self):
        """Проверка всех токенов"""
        logger.info("🔑 Checking Discord tokens...")
        
        valid_tokens = [s for s in self.sessions if s['valid']]
        invalid_tokens = [s for s in self.sessions if not s['valid']]
        
        logger.info(f"📊 Token Summary:")
        logger.info(f"   ✅ Valid tokens: {len(valid_tokens)}")
        logger.info(f"   ❌ Invalid tokens: {len(invalid_tokens)}")
        
        if valid_tokens:
            logger.info(f"✅ Valid accounts:")
            for session_data in valid_tokens:
                user = session_data['user_info']
                logger.info(f"   • {user['username']}#{user['discriminator']} (ID: {user['id']})")
        
        if invalid_tokens:
            logger.warning(f"❌ Invalid tokens found:")
            for session_data in invalid_tokens:
                logger.warning(f"   • {session_data['token'][:20]}...")
        
        return len(valid_tokens) > 0
    
    def discover_all_guilds(self):
        """Обнаружение всех серверов со всех аккаунтов"""
        logger.info("🏰 Discovering all guilds across all accounts...")
        
        all_guilds = {}
        total_unique_guilds = 0
        
        for session_data in self.sessions:
            if not session_data['valid']:
                continue
                
            session = session_data['session']
            username = session_data['user_info']['username']
            
            try:
                guilds = self._get_all_guilds_paginated(session)
                logger.info(f"🏰 {username}: found {len(guilds)} guilds")
                
                for guild in guilds:
                    guild_id = guild['id']
                    if guild_id not in all_guilds:
                        all_guilds[guild_id] = {
                            'guild_info': guild,
                            'accessible_via': [],
                            'announcement_channels': {}
                        }
                        total_unique_guilds += 1
                    
                    all_guilds[guild_id]['accessible_via'].append(username)
                    
            except Exception as e:
                logger.error(f"❌ Error getting guilds for {username}: {e}")
        
        self.guild_data = all_guilds
        logger.success(f"🎉 Discovery complete: {total_unique_guilds} unique guilds found")
        
        return all_guilds
    
    def _get_all_guilds_paginated(self, session):
        """Получение всех серверов с пагинацией"""
        guilds = []
        url = 'https://discord.com/api/v9/users/@me/guilds?limit=200'
        
        page = 1
        while url:
            try:
                r = session.get(url)
                if r.status_code == 200:
                    batch = r.json()
                    guilds.extend(batch)
                    logger.debug(f"   Page {page}: {len(batch)} guilds")
                    
                    # Проверяем пагинацию
                    if 'Link' in r.headers and len(batch) == 200:
                        links = r.headers['Link'].split(',')
                        next_url = None
                        for link in links:
                            if 'rel="next"' in link:
                                next_url = link[link.find('<')+1:link.find('>')]
                                break
                        url = next_url
                        page += 1
                    else:
                        url = None
                        
                elif r.status_code == 429:
                    retry_after = float(r.json().get('retry_after', 1))
                    logger.warning(f"   Rate limited, waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue
                else:
                    logger.error(f"   Error: HTTP {r.status_code}")
                    break
                    
            except Exception as e:
                logger.error(f"   Exception: {e}")
                break
        
        return guilds
    
    def analyze_announcement_channels(self):
        """Анализ announcement каналов во всех серверах"""
        if not self.guild_data:
            self.discover_all_guilds()
        
        logger.info("📢 Analyzing announcement channels...")
        
        servers_with_announcements = 0
        total_announcement_channels = 0
        
        for guild_id, guild_data in self.guild_data.items():
            guild_info = guild_data['guild_info']
            guild_name = guild_info['name']
            accessible_via = guild_data['accessible_via']
            
            # Пробуем получить каналы через доступные аккаунты
            announcement_channels = {}
            
            for username in accessible_via:
                session_data = next(s for s in self.sessions if s['user_info']['username'] == username)
                session = session_data['session']
                
                try:
                    channels = self._get_guild_channels(session, guild_id)
                    if channels:
                        announcement_channels = self._filter_announcement_channels(channels)
                        break
                except Exception as e:
                    logger.debug(f"   Failed to get channels via {username}: {e}")
                    continue
            
            if announcement_channels:
                servers_with_announcements += 1
                total_announcement_channels += len(announcement_channels)
                
                logger.info(f"✅ {guild_name}:")
                logger.info(f"   📊 Accessible via: {', '.join(accessible_via)}")
                logger.info(f"   📢 Announcement channels: {len(announcement_channels)}")
                
                for channel_id, channel_info in announcement_channels.items():
                    priority = channel_info.get('priority', 'unknown')
                    channel_type = 'Announcement' if channel_info['type'] == 5 else 'Text'
                    logger.info(f"      • {channel_info['name']} (ID: {channel_id}, Type: {channel_type}, Priority: {priority})")
                
                # Сохраняем в guild_data
                guild_data['announcement_channels'] = announcement_channels
            else:
                logger.warning(f"⚠️ {guild_name}: No announcement channels found")
        
        logger.info(f"📊 Analysis Summary:")
        logger.info(f"   🏰 Total servers: {len(self.guild_data)}")
        logger.info(f"   📢 Servers with announcements: {servers_with_announcements}")
        logger.info(f"   📈 Total announcement channels: {total_announcement_channels}")
        logger.info(f"   📊 Coverage: {servers_with_announcements/len(self.guild_data)*100:.1f}%")
        
        return servers_with_announcements, total_announcement_channels
    
    def _get_guild_channels(self, session, guild_id):
        """Получение каналов сервера"""
        try:
            r = session.get(f'https://discord.com/api/v9/guilds/{guild_id}/channels')
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 403:
                return None  # Нет доступа
            else:
                logger.debug(f"Error getting channels for {guild_id}: HTTP {r.status_code}")
                return None
        except Exception as e:
            logger.debug(f"Exception getting channels for {guild_id}: {e}")
            return None
    
    def _filter_announcement_channels(self, channels):
        """Фильтрация announcement каналов"""
        announcement_channels = {}
        
        priority_keywords = [
            'announcements',
            'announcement', 
            'news',
            'updates',
            'important',
            'официальные',
            'объявления',
            'важное'
        ]
        
        for channel in channels:
            if channel['type'] not in [0, 5]:  # Только текстовые и announcement каналы
                continue
                
            channel_name = channel['name'].lower()
            
            # Официальный announcement тип (приоритет 1)
            if channel.get('type') == 5:
                announcement_channels[channel['id']] = {
                    'name': channel['name'],
                    'type': channel['type'],
                    'priority': 1
                }
                continue
            
            # Точное совпадение "announcements" (приоритет 2)
            if channel_name == 'announcements':
                announcement_channels[channel['id']] = {
                    'name': channel['name'],
                    'type': channel['type'],
                    'priority': 2
                }
                continue
            
            # Содержит keywords (приоритет 3+)
            for i, keyword in enumerate(priority_keywords):
                if keyword in channel_name:
                    announcement_channels[channel['id']] = {
                        'name': channel['name'],
                        'type': channel['type'],
                        'priority': 3 + i
                    }
                    break
        
        # Возвращаем топ-5 каналов по приоритету
        sorted_channels = dict(sorted(
            announcement_channels.items(),
            key=lambda x: x[1]['priority']
        )[:5])
        
        return sorted_channels
    
    def compare_with_current_config(self):
        """Сравнение обнаруженных серверов с текущей конфигурацией"""
        logger.info("🔍 Comparing discovered servers with current configuration...")
        
        if not self.guild_data:
            self.discover_all_guilds()
            self.analyze_announcement_channels()
        
        # Получаем обнаруженные серверы с announcement каналами
        discovered_servers = set()
        for guild_data in self.guild_data.values():
            if guild_data['announcement_channels']:
                discovered_servers.add(guild_data['guild_info']['name'])
        
        # Получаем серверы из текущей конфигурации
        config_servers = set(config.SERVER_CHANNEL_MAPPINGS.keys())
        
        # Анализируем различия
        missing_from_config = discovered_servers - config_servers
        not_discovered = config_servers - discovered_servers
        in_both = discovered_servers & config_servers
        
        logger.info(f"📊 Configuration Comparison:")
        logger.info(f"   🔍 Discovered servers: {len(discovered_servers)}")
        logger.info(f"   ⚙️ Config servers: {len(config_servers)}")
        logger.info(f"   ✅ In both: {len(in_both)}")
        logger.info(f"   🆕 Missing from config: {len(missing_from_config)}")
        logger.info(f"   ❓ In config but not discovered: {len(not_discovered)}")
        
        if missing_from_config:
            logger.warning(f"🆕 Servers missing from config:")
            for server in sorted(missing_from_config):
                guild_data = next(g for g in self.guild_data.values() if g['guild_info']['name'] == server)
                channels_count = len(guild_data['announcement_channels'])
                logger.warning(f"   • {server} ({channels_count} announcement channels)")
        
        if not_discovered:
            logger.info(f"❓ Servers in config but not discovered:")
            for server in sorted(not_discovered):
                logger.info(f"   • {server} (may be inaccessible or renamed)")
        
        return missing_from_config, not_discovered
    
    def test_channel_access(self):
        """Тестирование доступа к каналам из текущей конфигурации"""
        logger.info("🧪 Testing access to configured channels...")
        
        accessible_channels = []
        inaccessible_channels = []
        
        for server_name, channels in config.SERVER_CHANNEL_MAPPINGS.items():
            if not channels:
                continue
                
            logger.info(f"🏰 Testing {server_name}:")
            
            for channel_id, channel_name in channels.items():
                # Тестируем доступ через все доступные токены
                access_results = []
                
                for session_data in self.sessions:
                    if not session_data['valid']:
                        continue
                        
                    session = session_data['session']
                    username = session_data['user_info']['username']
                    
                    try:
                        r = session.get(f'https://discord.com/api/v9/channels/{channel_id}/messages?limit=1')
                        if r.status_code == 200:
                            access_results.append(f"✅ {username}")
                        elif r.status_code == 403:
                            access_results.append(f"🔒 {username}")
                        else:
                            access_results.append(f"❌ {username} (HTTP {r.status_code})")
                    except Exception as e:
                        access_results.append(f"❌ {username} (Error)")
                
                # Определяем общий статус доступа
                has_access = any("✅" in result for result in access_results)
                
                if has_access:
                    accessible_channels.append((server_name, channel_name, channel_id))
                    logger.info(f"   ✅ {channel_name}: {', '.join(access_results)}")
                else:
                    inaccessible_channels.append((server_name, channel_name, channel_id))
                    logger.warning(f"   ❌ {channel_name}: {', '.join(access_results)}")
        
        logger.info(f"📊 Channel Access Summary:")
        logger.info(f"   ✅ Accessible: {len(accessible_channels)}")
        logger.info(f"   ❌ Inaccessible: {len(inaccessible_channels)}")
        
        if inaccessible_channels:
            logger.warning(f"❌ Inaccessible channels:")
            for server, channel, channel_id in inaccessible_channels:
                logger.warning(f"   • {server}#{channel} (ID: {channel_id})")
        
        return accessible_channels, inaccessible_channels
    
    def generate_new_config(self):
        """Генерация новой конфигурации на основе обнаружения"""
        logger.info("⚙️ Generating new configuration...")
        
        if not self.guild_data:
            self.discover_all_guilds()
            self.analyze_announcement_channels()
        
        new_config = {}
        
        for guild_data in self.guild_data.values():
            if not guild_data['announcement_channels']:
                continue
                
            guild_name = guild_data['guild_info']['name']
            channels = {}
            
            for channel_id, channel_info in guild_data['announcement_channels'].items():
                channels[channel_id] = channel_info['name']
            
            if channels:
                new_config[guild_name] = channels
        
        logger.success(f"✅ New configuration generated:")
        logger.info(f"   🏰 Servers: {len(new_config)}")
        logger.info(f"   📢 Total channels: {sum(len(channels) for channels in new_config.values())}")
        
        # Сохраняем в файл
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'discovered_config_{timestamp}.json'
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(new_config, f, indent=2, ensure_ascii=False)
        
        logger.info(f"💾 Configuration saved to: {filename}")
        
        return new_config
    
    def run_full_diagnostic(self):
        """Полная диагностика"""
        logger.info("🚀 Running full diagnostic...")
        logger.info("=" * 80)
        
        # Шаг 1: Проверка токенов
        if not self.check_tokens():
            logger.error("❌ No valid tokens found. Cannot proceed.")
            return False
        
        print()
        
        # Шаг 2: Обнаружение серверов
        self.discover_all_guilds()
        print()
        
        # Шаг 3: Анализ announcement каналов
        servers_with_announcements, total_channels = self.analyze_announcement_channels()
        print()
        
        # Шаг 4: Сравнение с текущей конфигурацией
        missing, not_discovered = self.compare_with_current_config()
        print()
        
        # Шаг 5: Тестирование доступа к каналам
        accessible, inaccessible = self.test_channel_access()
        print()
        
        # Шаг 6: Генерация новой конфигурации
        new_config = self.generate_new_config()
        print()
        
        # Итоговый отчет
        logger.info("📋 DIAGNOSTIC SUMMARY")
        logger.info("=" * 50)
        logger.info(f"✅ Valid tokens: {len([s for s in self.sessions if s['valid']])}")
        logger.info(f"🏰 Total servers discovered: {len(self.guild_data)}")
        logger.info(f"📢 Servers with announcements: {servers_with_announcements}")
        logger.info(f"📈 Total announcement channels: {total_channels}")
        logger.info(f"🆕 Missing from current config: {len(missing)}")
        logger.info(f"❓ In config but not found: {len(not_discovered)}")
        logger.info(f"✅ Accessible channels: {len(accessible)}")
        logger.info(f"❌ Inaccessible channels: {len(inaccessible)}")
        logger.info(f"⚙️ New config servers: {len(new_config)}")
        
        if missing:
            logger.warning(f"\n🔧 RECOMMENDED ACTIONS:")
            logger.warning(f"1. Add {len(missing)} missing servers to your configuration")
            logger.warning(f"2. Review {len(inaccessible)} inaccessible channels")
            logger.warning(f"3. Use the generated config file to update your settings")
        
        logger.success(f"\n🎉 Diagnostic complete! Your Discord account can access {servers_with_announcements} servers with {total_channels} announcement channels.")
        
        return True

def main():
    """CLI для запуска диагностики"""
    if len(sys.argv) > 1:
        if sys.argv[1] == '--tokens':
            diagnostic = EnhancedDiagnosticTool()
            diagnostic.check_tokens()
        elif sys.argv[1] == '--discovery':
            diagnostic = EnhancedDiagnosticTool()
            diagnostic.discover_all_guilds()
            diagnostic.analyze_announcement_channels()
        elif sys.argv[1] == '--config':
            diagnostic = EnhancedDiagnosticTool()
            diagnostic.compare_with_current_config()
        elif sys.argv[1] == '--channels':
            diagnostic = EnhancedDiagnosticTool()
            diagnostic.test_channel_access()
        elif sys.argv[1] == '--generate':
            diagnostic = EnhancedDiagnosticTool()
            diagnostic.generate_new_config()
        elif sys.argv[1] == '--help':
            print("🔧 Enhanced Diagnostic Tool")
            print("Usage:")
            print("  python diagnostic_tools.py [option]")
            print("\nOptions:")
            print("  --tokens     Check Discord token validity")
            print("  --discovery  Discover all servers and announcement channels")
            print("  --config     Compare discovered servers with current config")
            print("  --channels   Test access to configured channels")
            print("  --generate   Generate new configuration file")
            print("  --help       Show this help")
            print("\nRun without arguments for full diagnostic")
        else:
            print(f"Unknown option: {sys.argv[1]}")
            print("Use --help for available options")
    else:
        # Полная диагностика
        diagnostic = EnhancedDiagnosticTool()
        diagnostic.run_full_diagnostic()

if __name__ == '__main__':
    main()