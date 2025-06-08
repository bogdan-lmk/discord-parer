#!/usr/bin/env python3
"""
WebSocket vs HTTP API Comparison Tool
Tests the same channels with both methods to find the difference
"""

import requests
import asyncio
import aiohttp
import json
import os
from datetime import datetime
from dotenv import load_dotenv
from discord_telegram_parser.config.settings import config

load_dotenv()

class WebSocketVsHTTPTest:
    def __init__(self, token):
        self.token = token
        self.session = requests.Session()
        self.session.headers = {'Authorization': token}
    
    def test_http_access(self, channel_id, server_name, channel_name):
        """Test HTTP API access (like your working parser)"""
        print(f"\n🌐 Testing HTTP API: {server_name}#{channel_name}")
        try:
            # Exactly like your working parser
            r = self.session.get(f'https://discord.com/api/v9/channels/{channel_id}/messages?limit=1')
            
            if r.status_code == 200:
                messages = r.json()
                print(f"   ✅ HTTP API: Got {len(messages)} messages")
                if messages:
                    msg = messages[0]
                    print(f"   📝 Latest: {msg['author']['username']}: {msg['content'][:50]}...")
                return True, messages
            elif r.status_code == 403:
                print(f"   ❌ HTTP API: 403 Forbidden")
                return False, None
            else:
                print(f"   ❌ HTTP API: Status {r.status_code}")
                print(f"   📄 Response: {r.text}")
                return False, None
                
        except Exception as e:
            print(f"   ❌ HTTP API Error: {e}")
            return False, None
    
    async def test_websocket_access(self, channel_id, server_name, channel_name):
        """Test WebSocket API access"""
        print(f"\n🔌 Testing WebSocket API: {server_name}#{channel_name}")
        
        try:
            # Get gateway URL
            async with aiohttp.ClientSession() as session:
                async with session.get('https://discord.com/api/v9/gateway') as resp:
                    gateway_data = await resp.json()
                    gateway_url = gateway_data['url']
                
                print(f"   🔗 Connecting to: {gateway_url}")
                
                # Connect to WebSocket
                websocket = await session.ws_connect(f"{gateway_url}/?v=9&encoding=json")
                
                print(f"   ✅ WebSocket connected")
                
                # Wait for HELLO message
                async for msg in websocket:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        
                        if data['op'] == 10:  # HELLO
                            print(f"   👋 Received HELLO")
                            
                            # Send IDENTIFY with different intents
                            identify_payload = {
                                "op": 2,
                                "d": {
                                    "token": self.token,
                                    "properties": {
                                        "$os": "linux",
                                        "$browser": "discord_test",
                                        "$device": "discord_test"
                                    },
                                    "compress": False,
                                    "large_threshold": 50,
                                    "intents": 33281  # More comprehensive intents
                                }
                            }
                            await websocket.send_str(json.dumps(identify_payload))
                            print(f"   🔑 Sent IDENTIFY with intents 33281")
                            
                        elif data['op'] == 0 and data['t'] == 'READY':
                            print(f"   🚀 WebSocket READY")
                            user = data['d']['user']
                            guilds = data['d']['guilds']
                            print(f"   👤 User: {user['username']}")
                            print(f"   🏰 Guilds: {len(guilds)}")
                            
                            # Check if we can see the specific channel in guild data
                            channel_found = False
                            for guild in guilds:
                                if guild.get('channels'):
                                    for ch in guild['channels']:
                                        if ch['id'] == channel_id:
                                            channel_found = True
                                            print(f"   ✅ Channel found in guild data: {ch['name']}")
                                            break
                            
                            if not channel_found:
                                print(f"   ❌ Channel {channel_id} not found in guild data")
                            
                            await websocket.close()
                            return channel_found
                            
                        elif data['op'] == 11:  # HEARTBEAT_ACK
                            print(f"   💓 Heartbeat ACK")
                            
                await websocket.close()
                return False
                
        except Exception as e:
            print(f"   ❌ WebSocket Error: {e}")
            return False
    
    def test_different_intents(self):
        """Show different intent values and their meanings"""
        print(f"\n🎯 Discord Intents Explanation:")
        
        intents_info = {
            513: "GUILD_MESSAGES (1) + MESSAGE_CONTENT (512) = Basic messages",
            1024: "DIRECT_MESSAGES = DMs only", 
            32768: "MESSAGE_CONTENT = Message content access",
            33281: "GUILDS (1) + GUILD_MESSAGES (512) + MESSAGE_CONTENT (32768) = Full access",
            3276800: "All privileged intents",
            131071: "All non-privileged intents"
        }
        
        for intent_value, description in intents_info.items():
            print(f"   {intent_value}: {description}")
    
    async def comprehensive_test(self, channel_id, server_name, channel_name):
        """Run both tests and compare results"""
        print(f"\n🧪 Comprehensive Test: {server_name}#{channel_name}")
        print(f"   Channel ID: {channel_id}")
        
        # Test HTTP API
        http_success, http_data = self.test_http_access(channel_id, server_name, channel_name)
        
        # Test WebSocket API
        websocket_success = await self.test_websocket_access(channel_id, server_name, channel_name)
        
        # Compare results
        print(f"\n📊 Results Comparison:")
        print(f"   HTTP API: {'✅ Works' if http_success else '❌ Failed'}")
        print(f"   WebSocket: {'✅ Works' if websocket_success else '❌ Failed'}")
        
        if http_success and not websocket_success:
            print(f"   🔍 Issue: HTTP works but WebSocket doesn't")
            print(f"   💡 Possible causes:")
            print(f"      - Different intent requirements")
            print(f"      - WebSocket needs guild membership verification")
            print(f"      - Channel not included in READY guild data")
        elif not http_success and not websocket_success:
            print(f"   🚫 Both methods failed - permission issue")
        elif http_success and websocket_success:
            print(f"   🎉 Both methods work!")
        
        return http_success, websocket_success

async def main():
    """Test all configured channels"""
    token = os.getenv('DISCORD_AUTH_TOKENS', '').split(',')[0].strip()
    
    if not token:
        print("❌ No Discord token found")
        return
    
    print(f"🔑 Using token: {token[:20]}...")
    tester = WebSocketVsHTTPTest(token)
    
    # Show intents info
    tester.test_different_intents()
    
    # Test a few key channels
    test_channels = [
        ("1014577787039924226", "Hugging Face", "announcements"),
        ("1289179986879840348", "GenLayer", "announcements"),  # This one failed
        ("1036885738182168597", "Monad", "announcements"),
    ]
    
    print(f"\n🧪 Testing {len(test_channels)} channels...")
    
    results = []
    for channel_id, server_name, channel_name in test_channels:
        http_ok, ws_ok = await tester.comprehensive_test(channel_id, server_name, channel_name)
        results.append((server_name, channel_name, http_ok, ws_ok))
        
        # Small delay between tests
        await asyncio.sleep(1)
    
    # Summary
    print(f"\n📋 Final Summary:")
    for server, channel, http_ok, ws_ok in results:
        status = "🎉 Both work" if http_ok and ws_ok else \
                "🔍 HTTP only" if http_ok and not ws_ok else \
                "❌ Both fail" if not http_ok and not ws_ok else \
                "🤔 WS only"
        print(f"   {server}#{channel}: {status}")

if __name__ == '__main__':
    asyncio.run(main())