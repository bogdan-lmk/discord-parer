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



config.TELEGRAM_CHAT_ID = -1002890737800








# Auto-discovered channels


# Auto-discovered channels
config.SERVER_CHANNEL_MAPPINGS = {
  "Enso": {
    "817180334743093269": "\ud83d\udce3\u30fbannouncements",
    "1338585905228812368": "\ud83d\udc25\u30fbx-posts",
    "1357425502872404269": "\ud83d\ude3c\u30fbcommunity-updates"
  },
  "Linera": {
    "1098613827886649404": "\ud83d\udce2\u2502big-announcements",
    "1251050156263084093": "\ud83d\udce3\u2502community-updates",
    "1303016936992411669": "\ud83d\udce2\u2502validator-announcements",
    "1326508918142140437": "\ud83d\udce1\u2502dev-info"
  },
  "XRPL EVM": {
    "1150919510421938268": "\ud83d\udce3\u30fbannouncements",
    "1354884037118005502": "\ud83c\udf10\u30fbvalidator-announcements"
  },
  "Plume": {
    "1177252221830836264": "\u300e\ud83d\udd0a\u300fannouncements",
    "1266142517917913138": "\u300e\ud83e\udd16\u300fannouncement-logs",
    "1325991893674491914": "\u300e\ud83d\udca3\u300fbuilder-announcements",
    "1331013909766996069": "\u300e\ud83d\udd0a\u300fecosystem-announcements",
    "1361727081989537903": "\u300e\ud83c\udf89\u300fevents"
  },
  "Mezo": {
    "1220069685911883876": "\ud83d\udce2\u2502announcements",
    "1226947580710097067": "\ud83d\udd17\u2502links",
    "1228396285107769344": "\ud83d\udc24\u2502twitter-feed",
    "1319326473991098440": "\ud83d\udd10\u2502validator-alerts"
  },
  "Shape \u26ab": {
    "1259842667995594794": "announcements",
    "1309495021321322526": "twitter"
  }
}
