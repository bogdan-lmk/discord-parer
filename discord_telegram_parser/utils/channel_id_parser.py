import requests
import json
import os
import time
from typing import Dict, List, Tuple, Optional
from loguru import logger
from dotenv import load_dotenv

class DiscordServerDiscovery:
    def __init__(self, tokens: List[str]):
        """Инициализация с поддержкой нескольких токенов"""
        self.tokens = tokens if isinstance(tokens, list) else [tokens]
        self.sessions = []
        self.servers_data = {}
        self.last_scan_timestamp = 0
        
        # Создаем сессии для каждого токена
        for token in self.tokens:
            session = requests.Session()
            session.headers = {'Authorization': token}
            self.sessions.append({
                'session': session,
                'token': token,
                'user_info': None
            })
            
        # Получаем информацию о пользователях
        self._validate_tokens()

    def _validate_tokens(self):
        """Проверяем валидность токенов и получаем информацию о пользователях"""
        valid_sessions = []
        
        for session_data in self.sessions:
            try:
                r = session_data['session'].get('https://discord.com/api/v9/users/@me')
                if r.status_code == 200:
                    user_info = r.json()
                    session_data['user_info'] = user_info
                    valid_sessions.append(session_data)
                    logger.info(f"✅ Валидный токен для: {user_info['username']}#{user_info['discriminator']}")
                else:
                    logger.error(f"❌ Невалидный токен: HTTP {r.status_code}")
            except Exception as e:
                logger.error(f"❌ Ошибка проверки токена: {e}")
        
        self.sessions = valid_sessions
        logger.info(f"📊 Активных токенов: {len(self.sessions)}")

    def get_all_guilds(self) -> Dict[str, dict]:
        """Получаем все серверы со всех аккаунтов"""
        all_guilds = {}
        
        for session_data in self.sessions:
            session = session_data['session']
            username = session_data['user_info']['username']
            
            try:
                guilds = self._get_guilds_paginated(session)
                logger.info(f"🏰 {username}: найдено {len(guilds)} серверов")
                
                for guild in guilds:
                    guild_id = guild['id']
                    # Если сервер уже есть от другого аккаунта, объединяем данные
                    if guild_id not in all_guilds:
                        all_guilds[guild_id] = {
                            'guild_data': guild,
                            'accessible_tokens': [],
                            'channels': {}
                        }
                    
                    all_guilds[guild_id]['accessible_tokens'].append({
                        'username': username,
                        'session': session
                    })
                    
            except Exception as e:
                logger.error(f"❌ Ошибка получения серверов для {username}: {e}")
        
        logger.success(f"🎉 Всего уникальных серверов: {len(all_guilds)}")
        return all_guilds

    def _get_guilds_paginated(self, session: requests.Session) -> List[dict]:
        """Получаем серверы с пагинацией"""
        guilds = []
        url = 'https://discord.com/api/v9/users/@me/guilds?limit=200'  # Увеличиваем лимит
        
        while url:
            try:
                r = session.get(url)
                if r.status_code == 200:
                    batch = r.json()
                    guilds.extend(batch)
                    
                    # Проверяем пагинацию через Link header
                    if 'Link' in r.headers:
                        links = r.headers['Link'].split(',')
                        next_url = None
                        for link in links:
                            if 'rel="next"' in link:
                                next_url = link[link.find('<')+1:link.find('>')]
                                break
                        url = next_url
                    else:
                        url = None
                        
                    # Если получили меньше чем лимит, значит это последняя страница
                    if len(batch) < 200:
                        url = None
                        
                elif r.status_code == 429:
                    retry_after = float(r.json().get('retry_after', 1))
                    logger.warning(f"⏳ Rate limit, ждем {retry_after}с...")
                    time.sleep(retry_after)
                    continue
                else:
                    logger.error(f"❌ Ошибка получения серверов: HTTP {r.status_code}")
                    break
                    
            except Exception as e:
                logger.error(f"❌ Ошибка запроса серверов: {e}")
                break
                
        return guilds

    def get_guild_channels_multi_token(self, guild_id: str, guild_name: str, token_sessions: List[dict]) -> Dict[str, dict]:
        """Получаем каналы сервера, пробуя разные токены"""
        for token_data in token_sessions:
            session = token_data['session']
            username = token_data['username']
            
            try:
                channels = self._get_guild_channels_paginated(session, guild_id)
                if channels:
                    logger.debug(f"✅ {guild_name}: получены каналы через {username}")
                    return self._filter_announcement_channels(channels)
                    
            except Exception as e:
                logger.debug(f"⚠️ {guild_name}: ошибка для {username}: {e}")
                continue
        
        logger.warning(f"❌ {guild_name}: каналы недоступны ни через один токен")
        return {}

    def _get_guild_channels_paginated(self, session: requests.Session, guild_id: str) -> List[dict]:
        """Получаем каналы сервера с пагинацией"""
        channels = []
        url = f'https://discord.com/api/v9/guilds/{guild_id}/channels'
        
        while url:
            try:
                r = session.get(url)
                if r.status_code == 200:
                    batch = r.json()
                    channels.extend(batch)
                    
                    # Проверяем пагинацию
                    if 'Link' in r.headers:
                        links = r.headers['Link'].split(',')
                        next_url = None
                        for link in links:
                            if 'rel="next"' in link:
                                next_url = link[link.find('<')+1:link.find('>')]
                                break
                        url = next_url
                    else:
                        url = None
                        
                elif r.status_code == 429:
                    retry_after = float(r.json().get('retry_after', 1))
                    logger.warning(f"⏳ Rate limit каналов, ждем {retry_after}с...")
                    time.sleep(retry_after)
                    continue
                elif r.status_code == 403:
                    logger.debug(f"🔒 Нет доступа к каналам сервера {guild_id}")
                    return []
                else:
                    logger.warning(f"⚠️ Ошибка получения каналов: HTTP {r.status_code}")
                    return []
                    
            except Exception as e:
                logger.error(f"❌ Ошибка запроса каналов: {e}")
                return []
                
        return channels

    def _filter_announcement_channels(self, channels: List[dict]) -> Dict[str, dict]:
        """Фильтруем announcement каналы с приоритетом"""
        announcement_channels = {}
        
        # Приоритетные keywords для поиска
        priority_keywords = [
            'announcements',     # Высший приоритет
            'announcement',
            'news',
            'updates',
            'важное',
            'объявления',
            'анонсы'
        ]
        
        # Сначала ищем точные совпадения
        for channel in channels:
            if channel['type'] not in [0, 5]:  # Только текстовые и announcement каналы
                continue
                
            channel_name = channel['name'].lower()
            
            # 1. Официальный announcement тип (приоритет 1)
            if channel.get('type') == 5:
                announcement_channels[channel['id']] = {
                    'name': channel['name'],
                    'type': channel['type'],
                    'priority': 1,
                    'raw_data': channel
                }
                continue
            
            # 2. Точное совпадение "announcements" (приоритет 2)
            if channel_name == 'announcements':
                announcement_channels[channel['id']] = {
                    'name': channel['name'],
                    'type': channel['type'],
                    'priority': 2,
                    'raw_data': channel
                }
                continue
            
            # 3. Содержит приоритетные keywords (приоритет 3-10)
            for i, keyword in enumerate(priority_keywords):
                if keyword in channel_name:
                    announcement_channels[channel['id']] = {
                        'name': channel['name'],
                        'type': channel['type'],
                        'priority': 3 + i,
                        'raw_data': channel
                    }
                    break
        
        # Сортируем по приоритету и возвращаем топ-5
        sorted_channels = dict(sorted(
            announcement_channels.items(),
            key=lambda x: x[1]['priority']
        )[:5])
        
        return sorted_channels

    def discover_all_servers(self, save_to_file: bool = True) -> Dict[str, Dict[str, str]]:
        """Полное обнаружение всех серверов и их announcement каналов"""
        logger.info("🔍 Начинаем полное обнаружение серверов...")
        
        # Получаем все серверы
        all_guilds = self.get_all_guilds()
        
        # Обрабатываем каждый сервер
        server_mappings = {}
        processed_count = 0
        
        for guild_id, guild_data in all_guilds.items():
            guild_info = guild_data['guild_data']
            guild_name = guild_info['name']
            token_sessions = guild_data['accessible_tokens']
            
            processed_count += 1
            logger.info(f"🏰 [{processed_count}/{len(all_guilds)}] Обрабатываем: {guild_name}")
            
            # Получаем announcement каналы
            announcement_channels = self.get_guild_channels_multi_token(
                guild_id, guild_name, token_sessions
            )
            
            if announcement_channels:
                # Преобразуем в формат для конфига
                server_mappings[guild_name] = {}
                for channel_id, channel_info in announcement_channels.items():
                    server_mappings[guild_name][channel_id] = channel_info['name']
                    
                logger.success(f"  ✅ Найдено {len(announcement_channels)} announcement каналов")
                for channel_id, info in announcement_channels.items():
                    logger.info(f"    📢 {info['name']} (приоритет: {info['priority']})")
            else:
                logger.warning(f"  ⚠️ Announcement каналы не найдены")
            
            # Задержка между серверами
            time.sleep(0.5)
        
        # Сохраняем результаты
        if save_to_file:
            self._save_discovery_results(server_mappings, all_guilds)
        
        self.last_scan_timestamp = time.time()
        logger.success(f"🎉 Обнаружение завершено! Найдено {len(server_mappings)} серверов с announcement каналами")
        
        return server_mappings

    def get_new_servers_since_last_scan(self) -> Dict[str, Dict[str, str]]:
        """Получаем только новые серверы с последнего сканирования"""
        if not hasattr(self, 'last_known_servers'):
            # Если это первый запуск, сохраняем текущее состояние
            current_servers = self.discover_all_servers(save_to_file=False)
            self.last_known_servers = set(current_servers.keys())
            return current_servers
        
        # Получаем текущие серверы
        current_servers = self.discover_all_servers(save_to_file=False)
        current_server_names = set(current_servers.keys())
        
        # Находим новые серверы
        new_server_names = current_server_names - self.last_known_servers
        
        if new_server_names:
            new_servers = {name: current_servers[name] for name in new_server_names}
            logger.info(f"🆕 Найдено {len(new_servers)} новых серверов:")
            for server_name in new_server_names:
                logger.info(f"  • {server_name}")
            
            # Обновляем известные серверы
            self.last_known_servers = current_server_names
            return new_servers
        else:
            logger.info("ℹ️ Новых серверов не найдено")
            return {}

    def _save_discovery_results(self, server_mappings: Dict, all_guilds_data: Dict):
        """Сохраняем результаты обнаружения"""
        # Основной конфиг
        with open('discovered_servers.json', 'w', encoding='utf-8') as f:
            json.dump(server_mappings, f, indent=2, ensure_ascii=False)
        
        # Детальная информация для отладки
        detailed_data = {
            'discovery_timestamp': time.time(),
            'total_servers_found': len(server_mappings),
            'total_guilds_accessible': len(all_guilds_data),
            'server_mappings': server_mappings,
            'tokens_used': len(self.sessions),
            'detailed_guild_info': {
                guild_data['guild_data']['name']: {
                    'guild_id': guild_id,
                    'owner_id': guild_data['guild_data'].get('owner_id'),
                    'member_count': guild_data['guild_data'].get('approximate_member_count'),
                    'accessible_via_tokens': [t['username'] for t in guild_data['accessible_tokens']]
                }
                for guild_id, guild_data in all_guilds_data.items()
            }
        }
        
        with open('discovery_detailed.json', 'w', encoding='utf-8') as f:
            json.dump(detailed_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"💾 Результаты сохранены в discovered_servers.json и discovery_detailed.json")

def parse_discord_servers() -> Dict[str, Dict[str, str]]:
    """Основная функция для получения серверов (совместимость с существующим кодом)"""
    load_dotenv()
    
    # Получаем токены
    tokens_str = os.getenv('DISCORD_AUTH_TOKENS', '')
    tokens = [t.strip() for t in tokens_str.split(',') if t.strip()]
    
    if not tokens:
        logger.error("❌ Токены Discord не найдены в .env")
        return {}
    
    logger.info(f"🔑 Используем {len(tokens)} токенов")
    
    # Создаем обнаружитель и запускаем полное сканирование
    discovery = DiscordServerDiscovery(tokens)
    server_mappings = discovery.discover_all_servers()
    
    return server_mappings

def discover_new_servers_only() -> Dict[str, Dict[str, str]]:
    """Функция для получения только новых серверов"""
    load_dotenv()
    
    tokens_str = os.getenv('DISCORD_AUTH_TOKENS', '')
    tokens = [t.strip() for t in tokens_str.split(',') if t.strip()]
    
    if not tokens:
        logger.error("❌ Токены Discord не найдены в .env")
        return {}
    
    discovery = DiscordServerDiscovery(tokens)
    return discovery.get_new_servers_since_last_scan()

# CLI для тестирования
if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == '--new-only':
        print("🔍 Сканирование только новых серверов...")
        new_servers = discover_new_servers_only()
        print(f"📊 Результат: {len(new_servers)} новых серверов")
    else:
        print("🔍 Полное сканирование всех серверов...")
        all_servers = parse_discord_servers()
        print(f"📊 Результат: {len(all_servers)} серверов найдено")