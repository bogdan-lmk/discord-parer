import os
from dotenv import load_dotenv

class Config:
    def __init__(self):
        load_dotenv()
        
        # Discord Configuration
        self.DISCORD_TOKENS = [
            t.strip() for t in 
            os.getenv('DISCORD_AUTH_TOKENS', '').split(',') 
            if t.strip()
        ]
        
        # Parsing Configuration
        self.PARSE_TYPE_ALL_CHAT = 1
        self.PARSE_TYPE_ONE_USER = 2
        self.PARSE_TYPE_DELETE_DUPLICATES = 3
        
        # Message Parsing Parameters
        self.MIN_WORDS = 0
        self.TRANSLATE_MESSAGES = False
        self.TRANSLATION_LANGUAGE = 'en'
        
        # Telegram Configuration
        self.TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
        self.TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')
        
        # Server/Channel Mappings
        self.SERVER_CHANNEL_MAPPINGS = {}
        
        # Telegram UI Preferences
        self.TELEGRAM_UI_PREFERENCES = {
            'use_topics': True,
            'show_timestamps': True
        }

config = Config()



config.TELEGRAM_CHAT_ID = -1002541501551



# Auto-discovered channels
config.SERVER_CHANNEL_MAPPINGS = {
  "shpp": {},
  "Allora": {},
  "Enso": {},
  "Burnt (XION)": {},
  "Galxe": {},
  "Hugging Face": {
    "1014577787039924226": "announcements"
  },
  "Huddle01": {},
  "Fusionist": {},
  "Hyperlane": {
    "975909531710283806": "announcements",
    "1339345834872934501": "Announcements"
  },
  "Prom": {},
  "\u2609 \u211d \u2200": {},
  "Monad": {
    "1036885738182168597": "announcements"
  },
  "Stream": {},
  "CodeWithAntonio": {},
  "GPUNET": {},
  "Flipsuite": {},
  "Atleta": {},
  "Morkie": {},
  "Mint Blockchain \ud83c\udf40": {},
  "Gaia \ud83c\udf31": {},
  "Lagrange": {},
  "GenLayer": {
    "1289179986879840348": "announcements"
  },
  "13B": {},
  "Shape \u26ab": {
    "1259842667995594794": "announcements"
  },
  "\u0421\u0435\u0440\u0432\u0435\u0440 bogdanlameko": {
    "1338784680601849858": "announcements"
  }
}
