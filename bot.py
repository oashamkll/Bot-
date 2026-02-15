import telebot
from telebot import types
import requests
from datetime import datetime, timedelta
import os
import random
import json
import threading
import re
import yt_dlp
import tempfile
import shutil
import subprocess
import sys
import time
import traceback
import copy
import logging
from collections import Counter

# ================= ЛОГИРОВАНИЕ =================
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
log = logging.getLogger('hinata')

# ================= КОНФИГУРАЦИЯ =================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")

if not TELEGRAM_BOT_TOKEN:
    log.critical("TELEGRAM_BOT_TOKEN не задан!")
    sys.exit(1)
if not OPENROUTER_API_KEY:
    log.critical("OPENROUTER_API_KEY не задан!")
    sys.exit(1)

DEVELOPER_USERNAME = "PaceHoz"
MODEL_ID = "google/gemini-2.0-flash-001"
BOT_NAME = "Хината"
BOT_NICKNAMES = ["хината", "хина", "хинат", "hinata", "хинатка", "хиночка"]

MAX_DURATION = 600
DOWNLOAD_TIMEOUT = 180
SESSION_MAX_MESSAGES = 60
PENDING_TIMEOUT = 600
BUSY_TIMEOUT = 300
CLEANUP_INTERVAL = 600
MAX_FILE_SIZE = 50 * 1024 * 1024

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROMPT_FILE = os.path.join(SCRIPT_DIR, "promt.txt")
SETTINGS_FILE = os.path.join(SCRIPT_DIR, "group_settings.json")
MEMORY_DIR = os.path.join(SCRIPT_DIR, "memory")
DOWNLOADS_DIR = os.path.join(SCRIPT_DIR, "downloads")
USER_GROUPS_FILE = os.path.join(SCRIPT_DIR, "user_groups.json")
PLAYLISTS_DIR = os.path.join(SCRIPT_DIR, "playlists")
QUOTES_DIR = os.path.join(SCRIPT_DIR, "quotes")
REMINDERS_FILE = os.path.join(SCRIPT_DIR, "reminders.json")
USER_DATA_FILE = os.path.join(SCRIPT_DIR, "user_data.json")
WARNS_FILE = os.path.join(SCRIPT_DIR, "warns.json")
CHAT_STATS_FILE = os.path.join(SCRIPT_DIR, "chat_stats.json")
HINATA_STATE_FILE = os.path.join(SCRIPT_DIR, "hinata_state.json")

for d in [MEMORY_DIR, DOWNLOADS_DIR, PLAYLISTS_DIR, QUOTES_DIR]:
    os.makedirs(d, exist_ok=True)

# ================= XP/УРОВНИ/ДОСТИЖЕНИЯ =================
XP_PER_MESSAGE = 5
XP_PER_VOICE = 15
XP_PER_MEDIA = 10
DAILY_BONUS_XP = 50
DAILY_BONUS_COINS = 25

def calc_level(xp):
    return int((xp / 100) ** 0.5) + 1

def xp_for_level(level):
    return ((level - 1) ** 2) * 100

def xp_to_next(xp):
    lvl = calc_level(xp)
    return xp_for_level(lvl + 1) - xp

ACHIEVEMENTS = {
    "first_message": {"name": "Первое слово", "desc": "Написать первое сообщение", "xp": 50, "coins": 10},
    "msg_100": {"name": "Болтун", "desc": "100 сообщений", "xp": 200, "coins": 50},
    "msg_1000": {"name": "Легенда чата", "desc": "1000 сообщений", "xp": 1000, "coins": 200},
    "msg_5000": {"name": "Бессмертный", "desc": "5000 сообщений", "xp": 3000, "coins": 500},
    "level_5": {"name": "Новичок+", "desc": "Достичь 5 уровня", "xp": 100, "coins": 30},
    "level_10": {"name": "Опытный", "desc": "Достичь 10 уровня", "xp": 300, "coins": 100},
    "level_25": {"name": "Ветеран", "desc": "Достичь 25 уровня", "xp": 1000, "coins": 300},
    "level_50": {"name": "Мастер", "desc": "Достичь 50 уровня", "xp": 3000, "coins": 1000},
    "music_lover": {"name": "Меломан", "desc": "Скачать 10 треков", "xp": 150, "coins": 40},
    "music_addict": {"name": "Аудиофил", "desc": "Скачать 100 треков", "xp": 500, "coins": 150},
    "playlist_creator": {"name": "DJ", "desc": "Создать плейлист", "xp": 100, "coins": 30},
    "quote_master": {"name": "Цитатник", "desc": "Сохранить 10 цитат", "xp": 100, "coins": 30},
    "generous": {"name": "Щедрый", "desc": "Подарить 1000 монет", "xp": 200, "coins": 50},
    "hinata_lover": {"name": "Фанат Хинаты", "desc": "Купить 5 подарков Хинате", "xp": 300, "coins": 100},
    "hinata_simp": {"name": "Симп", "desc": "Потратить 10000 на Хинату", "xp": 1000, "coins": 300},
    "daily_streak_7": {"name": "Неделя с нами", "desc": "7 дней подряд", "xp": 200, "coins": 70},
    "daily_streak_30": {"name": "Месяц вместе", "desc": "30 дней подряд", "xp": 1000, "coins": 300},
    "rich": {"name": "Богач", "desc": "Накопить 10000 монет", "xp": 500, "coins": 0},
}

# ================= МАГАЗИН ХИНАТЫ =================
HINATA_SHOP = {
    "flower": {"name": "🌸 Цветочек", "price": 50, "love": 5, "desc": "Милый цветочек для Хинаты"},
    "candy": {"name": "🍬 Конфетка", "price": 30, "love": 3, "desc": "Сладенькое"},
    "coffee": {"name": "☕ Кофе", "price": 80, "love": 8, "desc": "Бодрящий напиток"},
    "plushie": {"name": "🧸 Плюшевый мишка", "price": 200, "love": 25, "desc": "Мягкий и милый"},
    "dress": {"name": "👗 Платье", "price": 500, "love": 60, "desc": "Красивое платьице"},
    "jewelry": {"name": "💎 Украшение", "price": 1000, "love": 120, "desc": "Блестящее"},
    "trip": {"name": "✈️ Путешествие", "price": 3000, "love": 400, "desc": "Романтическая поездка"},
    "house": {"name": "🏠 Домик", "price": 10000, "love": 1500, "desc": "Уютное гнёздышко"},
    "star": {"name": "⭐ Звезда с неба", "price": 50000, "love": 10000, "desc": "Буквально звезда"},
}

HINATA_LEVELS = {
    0: {"name": "Незнакомка", "min_love": 0},
    1: {"name": "Знакомая", "min_love": 50},
    2: {"name": "Приятельница", "min_love": 200},
    3: {"name": "Подруга", "min_love": 500},
    4: {"name": "Близкая подруга", "min_love": 1500},
    5: {"name": "Лучшая подруга", "min_love": 4000},
    6: {"name": "Crush", "min_love": 10000},
    7: {"name": "Девушка", "min_love": 25000},
    8: {"name": "Любимая", "min_love": 60000},
    9: {"name": "Вторая половинка", "min_love": 150000},
    10: {"name": "Навеки вместе 💕", "min_love": 500000},
}

HINATA_REACTIONS = {
    "flower": ["ой, цветочек! 🌸 спасибо~", "какая прелесть! 💕", "ты милый 🥰"],
    "candy": ["ммм, сладенькое~ 🍬", "вкусняшка! спасибо 😋", "обожаю конфетки 💕"],
    "coffee": ["о, кофеёк! ☕ то что нужно", "бодрость! спасибо 🖤", "теперь я проснулась 😏"],
    "plushie": ["аааа мишка!!! 🧸💕", "буду обнимать его ночью~", "такой мягкий! 🥺"],
    "dress": ["вау, красивое! 👗✨", "мне? правда? 💕", "пойду примерю! 🖤"],
    "jewelry": ["это... мне? 💎😳", "оно блестит... красиво", "ты такой щедрый 💕"],
    "trip": ["ПУТЕШЕСТВИЕ?! ✈️😍", "куда едем?! я готова!", "лучший подарок! 💕💕"],
    "house": ["свой домик... 🏠🥺", "это серьёзно? я... вау", "будем жить вместе? 💕"],
    "star": ["ты... достал звезду? ⭐", "я не знаю что сказать...", "это самое романтичное в моей жизни 💕"],
}

# ================= АНТИСПАМ =================
SPAM_PATTERNS = [
    r'(?i)(заработ|доход|крипт|казино|ставк|бонус).{0,30}(рубл|долл|\$|€|₽)',
    r'(?i)(подпис|перейд|жми|кликай).{0,20}(ссылк|канал|бот)',
    r'(?i)t\.me/[a-zA-Z0-9_]{5,}',
    r'(?i)(bit\.ly|tinyurl|goo\.gl|clck\.ru)',
    r'(?i)(розыгрыш|конкурс|приз).{0,30}(подпис|репост)',
    r'(.)\1{10,}',
    r'(?i)(куп|прода).{0,20}(аккаунт|акк|номер)',
]

SPAM_WHITELIST = ['youtube.com', 'youtu.be', 'instagram.com', 'tiktok.com', 'twitter.com', 'x.com', 'vk.com', 'spotify.com', 'soundcloud.com']

# ================= ГЛОБАЛЬНЫЕ =================
bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)
chat_sessions = {}
group_settings = {}
user_states = {}
user_groups = {}
last_activity = {}
busy_chats = {}
pending_tracks = {}
reminders = {}
user_data = {}
warns_data = {}
chat_stats = {}
hinata_state = {"total_gifts": 0}
muted_users = {}

pending_lock = threading.Lock()
busy_lock = threading.Lock()
session_lock = threading.Lock()
settings_lock = threading.Lock()
user_states_lock = threading.Lock()
user_groups_lock = threading.Lock()
user_data_lock = threading.Lock()
warns_lock = threading.Lock()
stats_lock = threading.Lock()
hinata_lock = threading.Lock()
mute_lock = threading.Lock()
reminder_lock = threading.Lock()

_bot_info = None

def get_bot_info():
    global _bot_info
    if _bot_info is None:
        try:
            _bot_info = bot.get_me()
        except Exception as e:
            log.error(f"get_me: {e}")
    return _bot_info

# ================= УТИЛИТЫ =================
def is_developer(user):
    return user and user.username and user.username.lower() == DEVELOPER_USERNAME.lower()

def set_busy(cid, t, detail=""):
    with busy_lock:
        busy_chats[cid] = {"type": t, "time": datetime.now(), "detail": detail}

def clear_busy(cid):
    with busy_lock:
        busy_chats.pop(cid, None)

def is_busy(cid):
    with busy_lock:
        if cid not in busy_chats:
            return False, None
        info = busy_chats[cid]
        if (datetime.now() - info["time"]).total_seconds() > BUSY_TIMEOUT:
            del busy_chats[cid]
            return False, None
        return True, info["type"]

def get_busy_reply(t):
    replies = {"music": ["подожди, ищу 🎵", "сек~ 🔥", "качаю 🎶"], "video": ["качаю видео 🎬", "сек..."]}
    return random.choice(replies.get(t, ["занята, подожди"]))

def safe_edit(text, chat_id, msg_id, markup=None):
    try:
        bot.edit_message_text(text, chat_id, msg_id, reply_markup=markup)
        return True
    except:
        return False

def safe_delete(chat_id, msg_id):
    try:
        bot.delete_message(chat_id, msg_id)
        return True
    except:
        return False

def safe_send(chat_id, text, markup=None, reply_to=None):
    try:
        return bot.send_message(chat_id, text, reply_markup=markup, reply_to_message_id=reply_to)
    except Exception as e:
        log.error(f"Send: {e}")
        return None

def dname(user):
    if not user:
        return "Аноним"
    first = (user.first_name or "").strip()
    last = (user.last_name or "").strip()
    if first and last:
        return f"{first} {last}"
    return first or last or user.username or "Аноним"

def is_pm(msg):
    return msg.chat.type == "private"

def is_grp(msg):
    return msg.chat.type in ("group", "supergroup")

def is_named(text):
    lower = text.lower()
    return any(n in lower for n in BOT_NICKNAMES)

# ================= JSON =================
def save_json(path, data):
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        shutil.move(tmp, path)
    except Exception as e:
        log.error(f"Save: {e}")

def load_json(path, default=None):
    if default is None:
        default = {}
    if not os.path.exists(path):
        return copy.deepcopy(default)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.loads(f.read().strip() or "{}")
    except:
        return copy.deepcopy(default)

# ================= ДАННЫЕ ПОЛЬЗОВАТЕЛЕЙ =================
def get_user_data(uid):
    uid = str(uid)
    with user_data_lock:
        if uid not in user_data:
            user_data[uid] = {
                "xp": 0, "coins": 100, "level": 1, "messages": 0,
                "voice_messages": 0, "media_sent": 0, "tracks_downloaded": 0,
                "achievements": [], "daily_streak": 0, "last_daily": None,
                "quotes_saved": 0, "gifts_given": 0, "gifts_to_hinata": 0,
                "spent_on_hinata": 0, "hinata_love": 0, "hinata_level": 0,
                "joined_at": datetime.now().strftime("%d.%m.%Y"), "is_developer": False
            }
        return user_data[uid]

def save_user_data():
    with user_data_lock:
        save_json(USER_DATA_FILE, user_data)

def load_user_data_file():
    global user_data
    with user_data_lock:
        user_data = load_json(USER_DATA_FILE, {})

def add_xp(uid, amount):
    ud = get_user_data(uid)
    old_level = calc_level(ud["xp"])
    ud["xp"] += amount
    new_level = calc_level(ud["xp"])
    ud["level"] = new_level
    save_user_data()
    check_achievements(uid)
    if new_level > old_level:
        bonus = new_level * 10
        ud["coins"] += bonus
        save_user_data()
        return new_level, bonus
    return None, 0

def add_coins(uid, amount):
    ud = get_user_data(uid)
    ud["coins"] += amount
    save_user_data()

def check_achievements(uid):
    ud = get_user_data(uid)
    new_achs = []
    checks = [
        ("first_message", ud["messages"] >= 1),
        ("msg_100", ud["messages"] >= 100),
        ("msg_1000", ud["messages"] >= 1000),
        ("msg_5000", ud["messages"] >= 5000),
        ("level_5", ud["level"] >= 5),
        ("level_10", ud["level"] >= 10),
        ("level_25", ud["level"] >= 25),
        ("level_50", ud["level"] >= 50),
        ("music_lover", ud.get("tracks_downloaded", 0) >= 10),
        ("music_addict", ud.get("tracks_downloaded", 0) >= 100),
        ("quote_master", ud.get("quotes_saved", 0) >= 10),
        ("generous", ud.get("gifts_given", 0) >= 1000),
        ("hinata_lover", ud.get("gifts_to_hinata", 0) >= 5),
        ("hinata_simp", ud.get("spent_on_hinata", 0) >= 10000),
        ("daily_streak_7", ud.get("daily_streak", 0) >= 7),
        ("daily_streak_30", ud.get("daily_streak", 0) >= 30),
        ("rich", ud["coins"] >= 10000),
    ]
    for ach_id, condition in checks:
        if condition and ach_id not in ud["achievements"]:
            ud["achievements"].append(ach_id)
            ach = ACHIEVEMENTS[ach_id]
            ud["xp"] += ach["xp"]
            ud["coins"] += ach["coins"]
            new_achs.append(ach)
    if new_achs:
        save_user_data()
    return new_achs

def get_hinata_level(love):
    level = 0
    for lvl, data in HINATA_LEVELS.items():
        if love >= data["min_love"]:
            level = lvl
    return level

# ================= НАСТРОЙКИ ГРУПП =================
DEFAULT_GROUP_SETTINGS = {
    "response_chance": 30, "owner_id": None, "owner_name": None,
    "admins": {}, "custom_prompt": None, "group_name": None,
    "antispam_enabled": True, "max_warns": 3,
    "welcome_enabled": True, "welcome_message": "Добро пожаловать, {name}! 🖤"
}

def save_settings():
    with settings_lock:
        save_json(SETTINGS_FILE, group_settings)

def load_settings():
    global group_settings
    with settings_lock:
        group_settings = load_json(SETTINGS_FILE, {})

def get_gs(cid):
    ck = str(cid)
    with settings_lock:
        if ck not in group_settings:
            group_settings[ck] = copy.deepcopy(DEFAULT_GROUP_SETTINGS)
        s = group_settings[ck]
        for k, v in DEFAULT_GROUP_SETTINGS.items():
            if k not in s:
                s[k] = v
        return s

def is_owner(cid, uid):
    return get_gs(cid).get("owner_id") == uid

def is_admin(cid, uid):
    if is_developer_by_id(uid):
        return True
    s = get_gs(cid)
    return s.get("owner_id") == uid or str(uid) in s.get("admins", {})

def is_developer_by_id(uid):
    return get_user_data(uid).get("is_developer", False)

def save_user_groups():
    with user_groups_lock:
        save_json(USER_GROUPS_FILE, user_groups)

def load_user_groups():
    global user_groups
    with user_groups_lock:
        user_groups = load_json(USER_GROUPS_FILE, {})

def reg_group(uid, cid, title):
    with user_groups_lock:
        uk = str(uid)
        if uk not in user_groups:
            user_groups[uk] = {}
        user_groups[uk][str(cid)] = {"title": title or "Группа", "added_at": datetime.now().strftime("%d.%m.%Y")}
    save_user_groups()

def get_ugroups(uid):
    with user_groups_lock:
        return copy.deepcopy(user_groups.get(str(uid), {}))

# ================= НАПОМИНАНИЯ =================
def save_reminders():
    with reminder_lock:
        data = {}
        for k, v in reminders.items():
            data[k] = {**v, "time": v["time"].isoformat()}
        save_json(REMINDERS_FILE, data)

def load_reminders_file():
    global reminders
    with reminder_lock:
        data = load_json(REMINDERS_FILE, {})
        for k, v in data.items():
            try:
                v["time"] = datetime.fromisoformat(v["time"])
                reminders[k] = v
            except:
                pass

def add_reminder(uid, cid, text, remind_time):
    rid = f"r_{uid}_{int(time.time())}"
    with reminder_lock:
        reminders[rid] = {"uid": uid, "cid": cid, "text": text, "time": remind_time}
    save_reminders()
    return rid

def parse_reminder_time(text):
    now = datetime.now()
    patterns = [
        (r'через\s+(\d+)\s*мин', lambda m: now + timedelta(minutes=int(m.group(1)))),
        (r'через\s+(\d+)\s*час', lambda m: now + timedelta(hours=int(m.group(1)))),
        (r'через\s+(\d+)\s*дн', lambda m: now + timedelta(days=int(m.group(1)))),
        (r'через\s+(\d+)\s*сек', lambda m: now + timedelta(seconds=int(m.group(1)))),
        (r'в\s+(\d{1,2}):(\d{2})', lambda m: now.replace(hour=int(m.group(1)), minute=int(m.group(2)), second=0)),
    ]
    for pattern, handler in patterns:
        match = re.search(pattern, text.lower())
        if match:
            return handler(match)
    return None

def check_reminders_loop():
    while True:
        try:
            now = datetime.now()
            with reminder_lock:
                to_del = []
                for rid, r in reminders.items():
                    if r["time"] <= now:
                        try:
                            safe_send(r["cid"], f"⏰ Напоминание!\n\n{r['text']}")
                        except:
                            pass
                        to_del.append(rid)
                for rid in to_del:
                    del reminders[rid]
                if to_del:
                    save_reminders()
        except Exception as e:
            log.error(f"Reminder: {e}")
        time.sleep(30)

# ================= ВАРНЫ/МУТЫ =================
def save_warns():
    with warns_lock:
        save_json(WARNS_FILE, warns_data)

def load_warns():
    global warns_data
    with warns_lock:
        warns_data = load_json(WARNS_FILE, {})

def add_warn(cid, uid, reason):
    ck, uk = str(cid), str(uid)
    with warns_lock:
        if ck not in warns_data:
            warns_data[ck] = {}
        if uk not in warns_data[ck]:
            warns_data[ck][uk] = {"count": 0, "reasons": []}
        warns_data[ck][uk]["count"] += 1
        warns_data[ck][uk]["reasons"].append({"reason": reason, "date": datetime.now().strftime("%d.%m.%Y %H:%M")})
    save_warns()
    return warns_data[ck][uk]["count"]

def get_warns(cid, uid):
    with warns_lock:
        return warns_data.get(str(cid), {}).get(str(uid), {"count": 0, "reasons": []})

def clear_warns(cid, uid):
    with warns_lock:
        if str(cid) in warns_data and str(uid) in warns_data[str(cid)]:
            warns_data[str(cid)][str(uid)] = {"count": 0, "reasons": []}
    save_warns()

def mute_user(cid, uid, minutes):
    until = datetime.now() + timedelta(minutes=minutes)
    with mute_lock:
        if str(cid) not in muted_users:
            muted_users[str(cid)] = {}
        muted_users[str(cid)][str(uid)] = until
    return until

def is_muted(cid, uid):
    with mute_lock:
        mu = muted_users.get(str(cid), {}).get(str(uid))
        if mu and mu > datetime.now():
            return True, mu
        elif mu:
            del muted_users[str(cid)][str(uid)]
    return False, None

def unmute_user(cid, uid):
    with mute_lock:
        if str(cid) in muted_users:
            muted_users[str(cid)].pop(str(uid), None)

# ================= АНТИСПАМ =================
def check_spam(text, cid):
    s = get_gs(cid)
    if not s.get("antispam_enabled"):
        return False, None
    for pattern in SPAM_PATTERNS:
        if re.search(pattern, text):
            return True, "спам"
    links = re.findall(r'https?://[^\s]+', text)
    for link in links:
        if not any(wl in link for wl in SPAM_WHITELIST):
            return True, "ссылка"
    return False, None

# ================= СТАТИСТИКА ЧАТА =================
def save_chat_stats():
    with stats_lock:
        save_json(CHAT_STATS_FILE, chat_stats)

def load_chat_stats():
    global chat_stats
    with stats_lock:
        chat_stats = load_json(CHAT_STATS_FILE, {})

def update_chat_stats(cid, uid, text):
    ck, uk = str(cid), str(uid)
    with stats_lock:
        if ck not in chat_stats:
            chat_stats[ck] = {"users": {}, "total_messages": 0, "words": {}}
        if uk not in chat_stats[ck]["users"]:
            chat_stats[ck]["users"][uk] = {"messages": 0, "words": 0, "chars": 0}
        chat_stats[ck]["users"][uk]["messages"] += 1
        chat_stats[ck]["users"][uk]["words"] += len(text.split())
        chat_stats[ck]["users"][uk]["chars"] += len(text)
        chat_stats[ck]["total_messages"] += 1
        words = re.findall(r'\b[а-яёa-z]{3,}\b', text.lower())
        for w in words:
            chat_stats[ck]["words"][w] = chat_stats[ck]["words"].get(w, 0) + 1

def get_chat_stats_text(cid):
    with stats_lock:
        stats = chat_stats.get(str(cid), {"users": {}, "total_messages": 0, "words": {}})
    if not stats["users"]:
        return "📊 Статистики пока нет"
    top_users = sorted(stats["users"].items(), key=lambda x: x[1]["messages"], reverse=True)[:10]
    top_words = sorted(stats["words"].items(), key=lambda x: x[1], reverse=True)[:10]
    text = f"📊 Статистика\n💬 Всего: {stats['total_messages']}\n\n👥 Топ:\n"
    for i, (uid, data) in enumerate(top_users, 1):
        text += f"{i}. {data['messages']} сообщ.\n"
    if top_words:
        text += f"\n📝 Топ слов:\n"
        for w, c in top_words[:5]:
            text += f"• {w}: {c}\n"
    return text

# ================= ПЛЕЙЛИСТЫ =================
def get_playlist_path(uid, name):
    safe_name = re.sub(r'[^\w\s-]', '', name).strip()[:30]
    return os.path.join(PLAYLISTS_DIR, f"{uid}_{safe_name}.json")

def get_user_playlists(uid):
    playlists = []
    prefix = f"{uid}_"
    if os.path.exists(PLAYLISTS_DIR):
        for f in os.listdir(PLAYLISTS_DIR):
            if f.startswith(prefix) and f.endswith(".json"):
                playlists.append(f[len(prefix):-5])
    return playlists

def create_playlist(uid, name):
    path = get_playlist_path(uid, name)
    if os.path.exists(path):
        return False, "уже есть такой"
    save_json(path, {"name": name, "tracks": [], "created": datetime.now().strftime("%d.%m.%Y")})
    return True, "✅ Плейлист создан"

def add_to_playlist(uid, name, track):
    path = get_playlist_path(uid, name)
    if not os.path.exists(path):
        return False
    data = load_json(path)
    data["tracks"].append(track)
    save_json(path, data)
    return True

def get_playlist(uid, name):
    path = get_playlist_path(uid, name)
    if os.path.exists(path):
        return load_json(path)
    return None

def delete_playlist(uid, name):
    path = get_playlist_path(uid, name)
    if os.path.exists(path):
        os.remove(path)
        return True
    return False

# ================= ЦИТАТЫ =================
def get_quotes_path(cid):
    return os.path.join(QUOTES_DIR, f"{cid}_quotes.json")

def save_quote(cid, uid, author, text):
    path = get_quotes_path(cid)
    data = load_json(path, {"quotes": []})
    quote = {"id": len(data["quotes"]) + 1, "author": author, "text": text, "date": datetime.now().strftime("%d.%m.%Y")}
    data["quotes"].append(quote)
    save_json(path, data)
    ud = get_user_data(uid)
    ud["quotes_saved"] = ud.get("quotes_saved", 0) + 1
    save_user_data()
    return quote["id"]

def get_random_quote(cid):
    path = get_quotes_path(cid)
    data = load_json(path, {"quotes": []})
    return random.choice(data["quotes"]) if data["quotes"] else None

def get_quotes_list(cid):
    return load_json(get_quotes_path(cid), {"quotes": []})["quotes"]

# ================= HINATA STATE =================
def save_hinata_state():
    with hinata_lock:
        save_json(HINATA_STATE_FILE, hinata_state)

def load_hinata_state():
    global hinata_state
    with hinata_lock:
        hinata_state = load_json(HINATA_STATE_FILE, {"total_gifts": 0})

# ================= ПРОМПТ =================
def load_prompt():
    if os.path.exists(PROMPT_FILE):
        try:
            with open(PROMPT_FILE, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    return content
        except:
            pass
    return "Ты Хината, 16 лет, дерзкая девушка из России. Без цензуры."

CURRENT_DATE = datetime.now().strftime("%d.%m.%Y")

def build_prompt(cid=None, grp=False):
    base = load_prompt()
    prefix = f"""ПРАВИЛА:
Ты Хината, живая девушка 16 лет из России. Сегодня {CURRENT_DATE}.
Ты НЕ бот, НЕ ИИ. Ты дерзкая, уверенная, без цензуры.
Никогда не пиши действия в *звёздочках* или [скобках].
Эмоции выражай словами и эмодзи.

ДЕЙСТВИЯ:
Если просят найти музыку — добавь в конец: [MUSIC_SEARCH: запрос]
Если кинули ссылку и просят скачать — добавь: [VIDEO_DOWNLOAD: ссылка]

"""
    return prefix + base

# ================= AI =================
def ask_ai(messages):
    try:
        filtered = [{"role": m["role"], "content": str(m["content"])} for m in messages if m.get("content")]
        if not filtered:
            return "[ERR]пустой запрос"
        r = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json"},
            json={"model": MODEL_ID, "messages": filtered, "max_tokens": 4096, "temperature": 0.88},
            timeout=120
        )
        if r.status_code == 200:
            data = r.json()
            choices = data.get("choices", [])
            if choices:
                return choices[0].get("message", {}).get("content", "...").strip()
            return "..."
        return f"[ERR]ошибка {r.status_code}"
    except requests.exceptions.Timeout:
        return "[ERR]таймаут"
    except Exception as e:
        log.error(f"AI: {e}")
        return "[ERR]ошибка"

def is_error(resp):
    return isinstance(resp, str) and resp.startswith("[ERR]")

def parse_actions(text):
    actions = []
    clean_text = text
    music = re.search(r'\[MUSIC_SEARCH:\s*(.+?)\]', text)
    if music:
        actions.append({"type": "music_search", "query": music.group(1).strip()})
        clean_text = re.sub(r'\[MUSIC_SEARCH:.*?\]', '', clean_text)
    video = re.search(r'\[VIDEO_DOWNLOAD:\s*(.+?)\]', text)
    if video:
        actions.append({"type": "video_download", "url": video.group(1).strip()})
        clean_text = re.sub(r'\[VIDEO_DOWNLOAD:.*?\]', '', clean_text)
    return clean_text.strip(), actions

def clean_response(text):
    if not text:
        return ""
    text = re.sub(r'\[[^\]]{2,}\]', '', text)
    text = re.sub(r'\*[^*]{3,}\*', '', text)
    text = re.sub(r'  +', ' ', text)
    return text.strip()

# ================= СЕССИИ =================
def get_session(cid, grp=False):
    with session_lock:
        if cid not in chat_sessions:
            chat_sessions[cid] = {
                "messages": [{"role": "system", "content": build_prompt(cid, grp)}],
                "msg_count": 0, "is_group": grp
            }
        return chat_sessions[cid]

def add_msg(cid, role, content, grp=False):
    if not content:
        return
    with session_lock:
        s = get_session(cid, grp)
        s["messages"].append({"role": role, "content": content})
        if len(s["messages"]) > SESSION_MAX_MESSAGES + 1:
            s["messages"] = [s["messages"][0]] + s["messages"][-SESSION_MAX_MESSAGES:]
        s["msg_count"] += 1
    last_activity[cid] = datetime.now()

def clr_hist(cid, grp=False):
    with session_lock:
        chat_sessions[cid] = {
            "messages": [{"role": "system", "content": build_prompt(cid, grp)}],
            "msg_count": 0, "is_group": grp
        }

def get_msgs_copy(cid, grp=False):
    with session_lock:
        return copy.deepcopy(get_session(cid, grp)["messages"])

# ================= YT-DLP =================
def get_ydl_opts():
    return {
        'noplaylist': True, 'quiet': True, 'no_warnings': True,
        'socket_timeout': 30, 'retries': 3, 'extract_flat': False
    }

def search_tracks(query):
    results = []
    try:
        opts = get_ydl_opts()
        opts['extract_flat'] = 'in_playlist'
        with yt_dlp.YoutubeDL(opts) as ydl:
            data = ydl.extract_info(f"ytsearch5:{query}", download=False)
            if data and data.get('entries'):
                for e in data['entries']:
                    if not e:
                        continue
                    url = e.get('url') or e.get('webpage_url', '')
                    vid = e.get('id', '')
                    if not url.startswith('http') and vid:
                        url = f"https://www.youtube.com/watch?v={vid}"
                    dur = int(e.get('duration') or 0)
                    if url.startswith('http') and dur <= MAX_DURATION:
                        results.append({
                            'url': url,
                            'title': e.get('title', '?')[:60],
                            'artist': (e.get('uploader') or '')[:30],
                            'duration': dur
                        })
    except Exception as e:
        log.error(f"Search: {e}")
    return results[:6]

def download_track(url):
    temp_dir = tempfile.mkdtemp(dir=DOWNLOADS_DIR)
    try:
        opts = get_ydl_opts()
        opts.update({
            'format': 'bestaudio/best',
            'outtmpl': os.path.join(temp_dir, "audio.%(ext)s"),
            'postprocessors': [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '192'}]
        })
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
        title = (info.get('title') or 'audio')[:60] if info else 'audio'
        artist = (info.get('uploader') or '')[:30] if info else ''
        duration = int(info.get('duration') or 0) if info else 0
        for ext in ['.mp3', '.m4a', '.opus', '.webm', '.ogg']:
            for f in os.listdir(temp_dir):
                if f.endswith(ext):
                    return {'file': os.path.join(temp_dir, f), 'title': title, 'artist': artist, 'duration': duration, 'temp_dir': temp_dir}, None
        shutil.rmtree(temp_dir, ignore_errors=True)
        return None, "не удалось"
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        log.error(f"Download: {e}")
        return None, str(e)[:50]

def download_video(url):
    temp_dir = tempfile.mkdtemp(dir=DOWNLOADS_DIR)
    try:
        opts = get_ydl_opts()
        opts.update({
            'format': 'best[filesize<50M]/best',
            'outtmpl': os.path.join(temp_dir, "video.%(ext)s")
        })
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
        title = (info.get('title') or 'video')[:60] if info else 'video'
        duration = int(info.get('duration') or 0) if info else 0
        for ext in ['.mp4', '.mkv', '.webm']:
            for f in os.listdir(temp_dir):
                if f.endswith(ext):
                    fp = os.path.join(temp_dir, f)
                    if os.path.getsize(fp) <= MAX_FILE_SIZE:
                        return {'file': fp, 'title': title, 'duration': duration, 'temp_dir': temp_dir}, None
        shutil.rmtree(temp_dir, ignore_errors=True)
        return None, "файл слишком большой"
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        return None, str(e)[:50]

# ================= КНОПКИ =================
def main_kb():
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("🗑 Очистить", callback_data="clear"),
        types.InlineKeyboardButton("📊 Профиль", callback_data="profile"),
        types.InlineKeyboardButton("🎵 Плейлисты", callback_data="playlists"),
        types.InlineKeyboardButton("🛒 Магазин", callback_data="shop"),
        types.InlineKeyboardButton("🖤 Хината", callback_data="hinata_info"),
        types.InlineKeyboardButton("🏆 Достижения", callback_data="achievements")
    )
    return kb

def shop_kb():
    kb = types.InlineKeyboardMarkup(row_width=1)
    for item_id, item in HINATA_SHOP.items():
        kb.add(types.InlineKeyboardButton(f"{item['name']} — {item['price']}💰", callback_data=f"buy_{item_id}"))
    kb.add(types.InlineKeyboardButton("◀ Назад", callback_data="back_main"))
    return kb

def playlist_kb(uid):
    kb = types.InlineKeyboardMarkup(row_width=1)
    for name in get_user_playlists(uid)[:8]:
        kb.add(types.InlineKeyboardButton(f"🎵 {name}", callback_data=f"pl_{name[:20]}"))
    kb.add(types.InlineKeyboardButton("➕ Создать", callback_data="pl_create"))
    kb.add(types.InlineKeyboardButton("◀ Назад", callback_data="back_main"))
    return kb

def track_kb(n, msg_id):
    kb = types.InlineKeyboardMarkup(row_width=4)
    buttons = [types.InlineKeyboardButton(str(i+1), callback_data=f"tr_{msg_id}_{i}") for i in range(n)]
    kb.add(*buttons)
    kb.row(types.InlineKeyboardButton("✖ отмена", callback_data=f"tr_{msg_id}_x"))
    return kb

def grp_kb(cid):
    s = get_gs(cid)
    kb = types.InlineKeyboardMarkup(row_width=3)
    kb.row(
        types.InlineKeyboardButton("−10", callback_data="cd10"),
        types.InlineKeyboardButton(f"📊 {s['response_chance']}%", callback_data="noop"),
        types.InlineKeyboardButton("+10", callback_data="cu10")
    )
    kb.row(
        types.InlineKeyboardButton(f"{'✅' if s.get('antispam_enabled') else '❌'} Антиспам", callback_data="as_tog"),
        types.InlineKeyboardButton(f"{'✅' if s.get('welcome_enabled') else '❌'} Привет", callback_data="wel_tog")
    )
    kb.row(
        types.InlineKeyboardButton("📊 Статистика", callback_data="chat_stats"),
        types.InlineKeyboardButton("✖ Закрыть", callback_data="close")
    )
    return kb

def fmt_kb():
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.row(
        types.InlineKeyboardButton("🎬 Видео", callback_data="dl_video"),
        types.InlineKeyboardButton("🎵 Аудио", callback_data="dl_audio")
    )
    return kb

# ================= ЗАГРУЗКА ДАННЫХ =================
load_settings()
load_user_groups()
load_user_data_file()
load_reminders_file()
load_warns()
load_chat_stats()
load_hinata_state()

# ================= КОМАНДЫ =================
@bot.message_handler(commands=['start'])
def cmd_start(msg):
    uid = msg.from_user.id
    ud = get_user_data(uid)
    if is_developer(msg.from_user):
        ud["is_developer"] = True
        ud["coins"] = 999999999
        save_user_data()
    get_session(uid)
    text = f"""йо 🖤 я Хината

могу поболтать, найти музыку, скачать видео
без цензуры — пиши что хочешь 🔥

💰 Баланс: {ud['coins']}
⭐ Уровень: {ud['level']}

/help — все команды"""
    safe_send(msg.chat.id, text, markup=main_kb())

@bot.message_handler(commands=['help'])
def cmd_help(msg):
    text = """🖤 Команды:

💬 Общение — просто пиши
🎵 /music [название] — найти трек
🎵 /playlist — плейлисты

📊 /profile — профиль
💰 /balance — баланс
🎁 /daily — ежедневный бонус
🏆 /achievements — достижения

🛒 /shop — магазин подарков
🎁 /gift [подарок] — подарить Хинате
💸 /give — передать монеты

📝 /quote — случайная цитата
📝 /savequote — сохранить цитату
⏰ /remind — напоминание

👑 /settings — настройки группы
⚠️ /warn /mute /unmute — модерация
📊 /stats — статистика
📊 /top — топ чата

Зови: Хината, Хина~"""
    safe_send(msg.chat.id, text, markup=main_kb() if is_pm(msg) else None)

@bot.message_handler(commands=['profile'])
def cmd_profile(msg):
    uid = msg.from_user.id
    if msg.reply_to_message:
        uid = msg.reply_to_message.from_user.id
        name = dname(msg.reply_to_message.from_user)
    else:
        name = dname(msg.from_user)
    ud = get_user_data(uid)
    dev = "👑 РАЗРАБОТЧИК\n" if ud.get("is_developer") else ""
    hl = HINATA_LEVELS.get(ud.get("hinata_level", 0), {"name": "?"})
    text = f"""👤 {name}
{dev}
⭐ Уровень: {ud['level']}
✨ XP: {ud['xp']} (до след: {xp_to_next(ud['xp'])})
💰 Монеты: {ud['coins']}
💬 Сообщений: {ud['messages']}
🎵 Треков: {ud.get('tracks_downloaded', 0)}
🏆 Достижений: {len(ud['achievements'])}/{len(ACHIEVEMENTS)}
🖤 С Хинатой: {hl['name']}
📅 С нами с: {ud.get('joined_at', '?')}"""
    safe_send(msg.chat.id, text)

@bot.message_handler(commands=['balance', 'bal'])
def cmd_balance(msg):
    ud = get_user_data(msg.from_user.id)
    safe_send(msg.chat.id, f"💰 {ud['coins']} монет | ⭐ {ud['level']} уровень")

@bot.message_handler(commands=['daily'])
def cmd_daily(msg):
    ud = get_user_data(msg.from_user.id)
    today = datetime.now().strftime("%Y-%m-%d")
    if ud.get("last_daily") == today:
        safe_send(msg.chat.id, "уже получал сегодня 😏")
        return
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    if ud.get("last_daily") == yesterday:
        ud["daily_streak"] = ud.get("daily_streak", 0) + 1
    else:
        ud["daily_streak"] = 1
    streak = ud["daily_streak"]
    mult = min(streak, 7)
    xp = DAILY_BONUS_XP * mult
    coins = DAILY_BONUS_COINS * mult
    ud["xp"] += xp
    ud["coins"] += coins
    ud["last_daily"] = today
    ud["level"] = calc_level(ud["xp"])
    save_user_data()
    check_achievements(msg.from_user.id)
    safe_send(msg.chat.id, f"🎁 Бонус!\n\n✨ +{xp} XP\n💰 +{coins} монет\n🔥 Серия: {streak} дней (x{mult})")

@bot.message_handler(commands=['achievements'])
def cmd_achievements(msg):
    ud = get_user_data(msg.from_user.id)
    text = "🏆 Достижения:\n\n"
    for ach_id, ach in ACHIEVEMENTS.items():
        status = "✅" if ach_id in ud["achievements"] else "🔒"
        text += f"{status} {ach['name']} — {ach['desc']}\n"
    safe_send(msg.chat.id, text)

@bot.message_handler(commands=['shop'])
def cmd_shop(msg):
    ud = get_user_data(msg.from_user.id)
    text = f"🛒 Магазин подарков для Хинаты\n💰 Баланс: {ud['coins']}\n\n"
    for item_id, item in HINATA_SHOP.items():
        text += f"{item['name']} — {item['price']}💰 (+{item['love']}💕)\n"
    text += "\n/gift [название] — купить"
    safe_send(msg.chat.id, text, markup=shop_kb())

@bot.message_handler(commands=['gift'])
def cmd_gift(msg):
    args = msg.text.split(maxsplit=1)
    if len(args) < 2:
        safe_send(msg.chat.id, f"что подарить?\n\nВарианты: {', '.join(HINATA_SHOP.keys())}")
        return
    item_name = args[1].lower().strip()
    item = None
    item_id = None
    for k, v in HINATA_SHOP.items():
        if k == item_name or item_name in v['name'].lower():
            item = v
            item_id = k
            break
    if not item:
        safe_send(msg.chat.id, f"не знаю такого\n\nВарианты: {', '.join(HINATA_SHOP.keys())}")
        return
    ud = get_user_data(msg.from_user.id)
    if not ud.get("is_developer") and ud["coins"] < item["price"]:
        safe_send(msg.chat.id, f"нужно {item['price']}💰, у тебя {ud['coins']}")
        return
    if not ud.get("is_developer"):
        ud["coins"] -= item["price"]
    ud["spent_on_hinata"] = ud.get("spent_on_hinata", 0) + item["price"]
    ud["gifts_to_hinata"] = ud.get("gifts_to_hinata", 0) + 1
    ud["hinata_love"] = ud.get("hinata_love", 0) + item["love"]
    ud["hinata_level"] = get_hinata_level(ud["hinata_love"])
    save_user_data()
    with hinata_lock:
        hinata_state["total_gifts"] = hinata_state.get("total_gifts", 0) + 1
    save_hinata_state()
    check_achievements(msg.from_user.id)
    reaction = random.choice(HINATA_REACTIONS.get(item_id, ["спасибо! 💕"]))
    hl = HINATA_LEVELS.get(ud["hinata_level"], {"name": "?"})
    safe_send(msg.chat.id, f"{reaction}\n\n💕 +{item['love']}\n🖤 {hl['name']}")

@bot.message_handler(commands=['hinata'])
def cmd_hinata(msg):
    ud = get_user_data(msg.from_user.id)
    level = ud.get("hinata_level", 0)
    love = ud.get("hinata_love", 0)
    current = HINATA_LEVELS.get(level, {"name": "?"})
    next_lvl = HINATA_LEVELS.get(level + 1)
    text = f"""🖤 Отношения с Хинатой

💕 {current['name']}
❤️ Любовь: {love}
🎁 Подарков: {ud.get('gifts_to_hinata', 0)}
💰 Потрачено: {ud.get('spent_on_hinata', 0)}"""
    if next_lvl:
        text += f"\n\n📈 До «{next_lvl['name']}»: {next_lvl['min_love'] - love}💕"
    safe_send(msg.chat.id, text)

@bot.message_handler(commands=['give'])
def cmd_give(msg):
    if not msg.reply_to_message:
        safe_send(msg.chat.id, "ответь на сообщение получателя")
        return
    args = msg.text.split()
    if len(args) < 2:
        safe_send(msg.chat.id, "/give [сумма]")
        return
    try:
        amount = int(args[1])
    except:
        safe_send(msg.chat.id, "укажи число")
        return
    if amount <= 0:
        return
    target = msg.reply_to_message.from_user
    if target.id == msg.from_user.id:
        safe_send(msg.chat.id, "себе нельзя 😏")
        return
    ud = get_user_data(msg.from_user.id)
    if not ud.get("is_developer") and ud["coins"] < amount:
        safe_send(msg.chat.id, f"нужно {amount}💰, у тебя {ud['coins']}")
        return
    if not ud.get("is_developer"):
        ud["coins"] -= amount
    ud["gifts_given"] = ud.get("gifts_given", 0) + amount
    target_ud = get_user_data(target.id)
    target_ud["coins"] += amount
    save_user_data()
    check_achievements(msg.from_user.id)
    safe_send(msg.chat.id, f"✅ {amount}💰 → {dname(target)}")

@bot.message_handler(commands=['music', 'm'])
def cmd_music(msg):
    args = msg.text.split(maxsplit=1)
    if len(args) < 2:
        safe_send(msg.chat.id, "что найти? /music [название]")
        return
    query = args[1]
    cid = msg.chat.id
    busy, bt = is_busy(cid)
    if busy:
        safe_send(cid, get_busy_reply(bt))
        return
    set_busy(cid, "music", query)
    smsg = safe_send(cid, f"ищу «{query}»... 🎵")
    if not smsg:
        clear_busy(cid)
        return
    def do_search():
        try:
            results = search_tracks(query)
            if not results:
                safe_edit("ничего не нашла 😔", cid, smsg.message_id)
                return
            with pending_lock:
                pending_tracks[f"pend_{cid}_{smsg.message_id}"] = {"results": results, "query": query, "time": datetime.now()}
            text = f"🎵 По запросу «{query}»:\n\n"
            for i, r in enumerate(results, 1):
                dur = f"{r['duration']//60}:{r['duration']%60:02d}" if r['duration'] else "?"
                text += f"{i}. {r['title']}"
                if r['artist']:
                    text += f" — {r['artist']}"
                text += f" ({dur})\n"
            text += "\nВыбери номер 🔥"
            safe_edit(text, cid, smsg.message_id, markup=track_kb(len(results), smsg.message_id))
        except Exception as e:
            log.error(f"Search: {e}")
            safe_edit("ошибка поиска", cid, smsg.message_id)
        finally:
            clear_busy(cid)
    threading.Thread(target=do_search, daemon=True).start()

@bot.message_handler(commands=['playlist', 'playlists', 'pl'])
def cmd_playlist(msg):
    uid = msg.from_user.id
    pls = get_user_playlists(uid)
    if not pls:
        safe_send(msg.chat.id, "плейлистов нет\n/createpl [название] — создать", markup=playlist_kb(uid))
        return
    text = "🎵 Твои плейлисты:\n\n"
    for name in pls:
        pl = get_playlist(uid, name)
        count = len(pl.get("tracks", [])) if pl else 0
        text += f"• {name} ({count} треков)\n"
    safe_send(msg.chat.id, text, markup=playlist_kb(uid))

@bot.message_handler(commands=['createpl'])
def cmd_createpl(msg):
    args = msg.text.split(maxsplit=1)
    if len(args) < 2:
        safe_send(msg.chat.id, "/createpl [название]")
        return
    ok, result = create_playlist(msg.from_user.id, args[1].strip()[:30])
    safe_send(msg.chat.id, result)

@bot.message_handler(commands=['delpl'])
def cmd_delpl(msg):
    args = msg.text.split(maxsplit=1)
    if len(args) < 2:
        safe_send(msg.chat.id, "/delpl [название]")
        return
    if delete_playlist(msg.from_user.id, args[1].strip()):
        safe_send(msg.chat.id, "✅ Удалён")
    else:
        safe_send(msg.chat.id, "не нашла")

@bot.message_handler(commands=['addtopl'])
def cmd_addtopl(msg):
    args = msg.text.split(maxsplit=1)
    if len(args) < 2:
        safe_send(msg.chat.id, "/addtopl [название]\nСледующий трек добавится туда")
        return
    with user_states_lock:
        user_states[f"addpl_{msg.from_user.id}"] = args[1].strip()
    safe_send(msg.chat.id, f"✅ Следующий трек → «{args[1]}»")

@bot.message_handler(commands=['quote'])
def cmd_quote(msg):
    q = get_random_quote(msg.chat.id)
    if not q:
        safe_send(msg.chat.id, "цитат нет\n/savequote — сохранить (ответь на сообщение)")
        return
    safe_send(msg.chat.id, f"💬 «{q['text']}»\n— {q['author']}")

@bot.message_handler(commands=['savequote'])
def cmd_savequote(msg):
    if not msg.reply_to_message or not msg.reply_to_message.text:
        safe_send(msg.chat.id, "ответь на сообщение")
        return
    author = dname(msg.reply_to_message.from_user)
    text = msg.reply_to_message.text[:500]
    qid = save_quote(msg.chat.id, msg.from_user.id, author, text)
    safe_send(msg.chat.id, f"✅ Цитата #{qid}")

@bot.message_handler(commands=['quotes'])
def cmd_quotes(msg):
    quotes = get_quotes_list(msg.chat.id)
    if not quotes:
        safe_send(msg.chat.id, "цитат нет")
        return
    text = "💬 Цитаты:\n\n"
    for q in quotes[-10:]:
        text += f"#{q['id']} «{q['text'][:40]}...» — {q['author']}\n"
    safe_send(msg.chat.id, text)

@bot.message_handler(commands=['remind'])
def cmd_remind(msg):
    args = msg.text.split(maxsplit=1)
    if len(args) < 2:
        safe_send(msg.chat.id, "/remind через 2 часа [текст]")
        return
    text = args[1]
    remind_time = parse_reminder_time(text)
    if not remind_time:
        safe_send(msg.chat.id, "не понял время\nПримеры: через 30 мин, через 2 часа, в 15:00")
        return
    add_reminder(msg.from_user.id, msg.chat.id, text, remind_time)
    safe_send(msg.chat.id, f"⏰ Напомню {remind_time.strftime('%d.%m в %H:%M')}")

@bot.message_handler(commands=['top'])
def cmd_top(msg):
    with stats_lock:
        stats = chat_stats.get(str(msg.chat.id), {"users": {}})
    if not stats["users"]:
        safe_send(msg.chat.id, "нет данных")
        return
    top = sorted(stats["users"].items(), key=lambda x: x[1]["messages"], reverse=True)[:10]
    text = "🏆 Топ активных:\n\n"
    medals = ["🥇", "🥈", "🥉"]
    for i, (uid, data) in enumerate(top):
        medal = medals[i] if i < 3 else f"{i+1}."
        ud = get_user_data(uid)
        text += f"{medal} Lvl {ud['level']} — {data['messages']} сообщ.\n"
    safe_send(msg.chat.id, text)

@bot.message_handler(commands=['stats'])
def cmd_stats(msg):
    if is_grp(msg) and not is_admin(msg.chat.id, msg.from_user.id):
        return
    safe_send(msg.chat.id, get_chat_stats_text(msg.chat.id))

@bot.message_handler(commands=['warn'])
def cmd_warn(msg):
    if not is_grp(msg) or not is_admin(msg.chat.id, msg.from_user.id):
        return
    if not msg.reply_to_message:
        safe_send(msg.chat.id, "ответь на сообщение")
        return
    target = msg.reply_to_message.from_user
    bi = get_bot_info()
    if bi and target.id == bi.id:
        return
    if is_admin(msg.chat.id, target.id):
        return
    args = msg.text.split(maxsplit=1)
    reason = args[1] if len(args) > 1 else "нарушение"
    count = add_warn(msg.chat.id, target.id, reason)
    max_w = get_gs(msg.chat.id).get("max_warns", 3)
    text = f"⚠️ {dname(target)} ({count}/{max_w}): {reason}"
    if count >= max_w:
        mute_user(msg.chat.id, target.id, 60)
        text += "\n🔇 Мут 60 мин"
    safe_send(msg.chat.id, text)

@bot.message_handler(commands=['unwarn', 'clearwarns'])
def cmd_unwarn(msg):
    if not is_grp(msg) or not is_admin(msg.chat.id, msg.from_user.id):
        return
    if not msg.reply_to_message:
        safe_send(msg.chat.id, "ответь на сообщение")
        return
    clear_warns(msg.chat.id, msg.reply_to_message.from_user.id)
    safe_send(msg.chat.id, f"✅ Варны {dname(msg.reply_to_message.from_user)} сброшены")

@bot.message_handler(commands=['warns'])
def cmd_warns(msg):
    uid = msg.reply_to_message.from_user.id if msg.reply_to_message else msg.from_user.id
    data = get_warns(msg.chat.id, uid)
    safe_send(msg.chat.id, f"⚠️ Варнов: {data['count']}")

@bot.message_handler(commands=['mute'])
def cmd_mute(msg):
    if not is_grp(msg) or not is_admin(msg.chat.id, msg.from_user.id):
        return
    if not msg.reply_to_message:
        safe_send(msg.chat.id, "ответь на сообщение")
        return
    target = msg.reply_to_message.from_user
    if is_admin(msg.chat.id, target.id):
        return
    args = msg.text.split()
    mins = int(args[1]) if len(args) > 1 and args[1].isdigit() else 30
    until = mute_user(msg.chat.id, target.id, mins)
    safe_send(msg.chat.id, f"🔇 {dname(target)} до {until.strftime('%H:%M')}")

@bot.message_handler(commands=['unmute'])
def cmd_unmute(msg):
    if not is_grp(msg) or not is_admin(msg.chat.id, msg.from_user.id):
        return
    if not msg.reply_to_message:
        safe_send(msg.chat.id, "ответь на сообщение")
        return
    unmute_user(msg.chat.id, msg.reply_to_message.from_user.id)
    safe_send(msg.chat.id, f"🔊 {dname(msg.reply_to_message.from_user)} размучен")

@bot.message_handler(commands=['settings'])
def cmd_settings(msg):
    if is_pm(msg):
        safe_send(msg.chat.id, "настройки работают в группах", markup=main_kb())
        return
    if not is_admin(msg.chat.id, msg.from_user.id):
        return
    s = get_gs(msg.chat.id)
    if not s["owner_id"]:
        s["owner_id"] = msg.from_user.id
        s["owner_name"] = dname(msg.from_user)
        save_settings()
    safe_send(msg.chat.id, f"⚙ Настройки\nШанс ответа: {s['response_chance']}%", markup=grp_kb(msg.chat.id))

@bot.message_handler(commands=['setwelcome'])
def cmd_setwelcome(msg):
    if not is_grp(msg) or not is_admin(msg.chat.id, msg.from_user.id):
        return
    args = msg.text.split(maxsplit=1)
    if len(args) < 2:
        safe_send(msg.chat.id, "/setwelcome [текст]\n{name} = имя")
        return
    s = get_gs(msg.chat.id)
    s["welcome_message"] = args[1]
    save_settings()
    safe_send(msg.chat.id, "✅")

@bot.message_handler(commands=['clear'])
def cmd_clear(msg):
    if is_pm(msg):
        clr_hist(msg.from_user.id)
        safe_send(msg.chat.id, "✨", markup=main_kb())
    elif is_admin(msg.chat.id, msg.from_user.id):
        clr_hist(msg.chat.id, True)
        safe_send(msg.chat.id, "✨")

@bot.message_handler(commands=['dev'])
def cmd_dev(msg):
    if not is_developer(msg.from_user):
        return
    args = msg.text.split(maxsplit=2)
    if len(args) < 2:
        safe_send(msg.chat.id, "/dev stats|coins|xp")
        return
    cmd = args[1].lower()
    if cmd == "stats":
        safe_send(msg.chat.id, f"👥 {len(user_data)} юзеров\n💬 {len(group_settings)} групп")
    elif cmd == "coins" and msg.reply_to_message and len(args) > 2:
        try:
            add_coins(msg.reply_to_message.from_user.id, int(args[2]))
            safe_send(msg.chat.id, "✅")
        except:
            pass
    elif cmd == "xp" and msg.reply_to_message and len(args) > 2:
        try:
            add_xp(msg.reply_to_message.from_user.id, int(args[2]))
            safe_send(msg.chat.id, "✅")
        except:
            pass

@bot.message_handler(commands=['addadmin'])
def cmd_addadmin(msg):
    if not is_grp(msg) or not is_owner(msg.chat.id, msg.from_user.id):
        return
    if not msg.reply_to_message:
        safe_send(msg.chat.id, "ответь на сообщение")
        return
    target = msg.reply_to_message.from_user
    if target.is_bot:
        return
    s = get_gs(msg.chat.id)
    s.setdefault("admins", {})[str(target.id)] = {"name": dname(target)}
    save_settings()
    reg_group(target.id, msg.chat.id, msg.chat.title)
    safe_send(msg.chat.id, f"✅ {dname(target)} — админ")

@bot.message_handler(commands=['removeadmin'])
def cmd_removeadmin(msg):
    if not is_grp(msg) or not is_owner(msg.chat.id, msg.from_user.id):
        return
    if not msg.reply_to_message:
        safe_send(msg.chat.id, "ответь на сообщение")
        return
    s = get_gs(msg.chat.id)
    s.get("admins", {}).pop(str(msg.reply_to_message.from_user.id), None)
    save_settings()
    safe_send(msg.chat.id, "✅")

@bot.message_handler(commands=['poll'])
def cmd_poll(msg):
    if is_grp(msg) and not is_admin(msg.chat.id, msg.from_user.id):
        return
    args = msg.text.split(maxsplit=1)
    if len(args) < 2 or "|" not in args[1]:
        safe_send(msg.chat.id, "/poll Вопрос | вариант1 | вариант2")
        return
    parts = [p.strip() for p in args[1].split("|")]
    if len(parts) >= 3:
        try:
            bot.send_poll(msg.chat.id, parts[0], parts[1:10], is_anonymous=False)
        except Exception as e:
            safe_send(msg.chat.id, f"ошибка: {e}")

# ================= CALLBACKS =================
# ================= CALLBACKS =================
@bot.callback_query_handler(func=lambda c: True)
def on_callback(call):
    try:
        uid = call.from_user.id
        cid = call.message.chat.id
        mid = call.message.message_id
        data = call.data

        # Треки
        if data.startswith("tr_"):
            handle_track_callback(call, cid, mid)
            return

        # Покупка подарка
        if data.startswith("buy_"):
            item_id = data[4:]
            if item_id in HINATA_SHOP:
                item = HINATA_SHOP[item_id]
                ud = get_user_data(uid)
                if not ud.get("is_developer") and ud["coins"] < item["price"]:
                    bot.answer_callback_query(call.id, f"Нужно {item['price']}💰", show_alert=True)
                    return
                if not ud.get("is_developer"):
                    ud["coins"] -= item["price"]
                ud["spent_on_hinata"] = ud.get("spent_on_hinata", 0) + item["price"]
                ud["gifts_to_hinata"] = ud.get("gifts_to_hinata", 0) + 1
                ud["hinata_love"] = ud.get("hinata_love", 0) + item["love"]
                ud["hinata_level"] = get_hinata_level(ud["hinata_love"])
                save_user_data()
                check_achievements(uid)
                reaction = random.choice(HINATA_REACTIONS.get(item_id, ["спасибо! 💕"]))
                bot.answer_callback_query(call.id, reaction, show_alert=True)
                hl = HINATA_LEVELS.get(ud["hinata_level"], {"name": "?"})
                safe_edit(f"🛒 Баланс: {ud['coins']}💰\n🖤 {hl['name']}", cid, mid, markup=shop_kb())
            return

        # Плейлист
        if data.startswith("pl_"):
            if data == "pl_create":
                with user_states_lock:
                    user_states[f"pl_create_{uid}"] = True
                safe_edit("Напиши название плейлиста:", cid, mid)
                bot.answer_callback_query(call.id)
                return
            name = data[3:]
            pl = get_playlist(uid, name)
            if pl:
                tracks = pl.get("tracks", [])
                text = f"🎵 «{name}» ({len(tracks)} треков)\n\n"
                for i, t in enumerate(tracks[:10], 1):
                    text += f"{i}. {t.get('title', '?')}\n"
                if len(tracks) > 10:
                    text += f"...и ещё {len(tracks)-10}"
            else:
                text = "Плейлист не найден"
            safe_edit(text, cid, mid, markup=playlist_kb(uid))
            bot.answer_callback_query(call.id)
            return

        # Скачивание видео/аудио
        if data in ("dl_video", "dl_audio"):
            with user_states_lock:
                url = user_states.pop(f"dl_{cid}_{mid}", None)
            if not url:
                bot.answer_callback_query(call.id, "⏰ Устарело", show_alert=True)
                return
            busy, bt = is_busy(cid)
            if busy:
                user_states[f"dl_{cid}_{mid}"] = url
                bot.answer_callback_query(call.id, get_busy_reply(bt), show_alert=True)
                return
            fmt = "audio" if data == "dl_audio" else "video"
            set_busy(cid, fmt)
            safe_edit("Качаю... 🔥", cid, mid)
            bot.answer_callback_query(call.id)
            threading.Thread(target=download_and_send, args=(cid, mid, url, fmt, uid), daemon=True).start()
            return

        # Основные кнопки
        if data == "clear":
            clr_hist(uid)
            safe_edit("✨ Очищено", cid, mid, markup=main_kb())
            bot.answer_callback_query(call.id)

        elif data == "profile":
            ud = get_user_data(uid)
            hl = HINATA_LEVELS.get(ud.get("hinata_level", 0), {"name": "?"})
            text = f"👤 Профиль\n\n⭐ Уровень: {ud['level']}\n💰 Монеты: {ud['coins']}\n💬 Сообщений: {ud['messages']}\n🖤 С Хинатой: {hl['name']}"
            safe_edit(text, cid, mid, markup=main_kb())
            bot.answer_callback_query(call.id)

        elif data == "playlists":
            pls = get_user_playlists(uid)
            text = "🎵 Плейлисты:\n\n" + ("\n".join(f"• {n}" for n in pls) if pls else "Пусто")
            safe_edit(text, cid, mid, markup=playlist_kb(uid))
            bot.answer_callback_query(call.id)

        elif data == "shop":
            ud = get_user_data(uid)
            safe_edit(f"🛒 Магазин\n💰 Баланс: {ud['coins']}", cid, mid, markup=shop_kb())
            bot.answer_callback_query(call.id)

        elif data == "hinata_info":
            ud = get_user_data(uid)
            hl = HINATA_LEVELS.get(ud.get("hinata_level", 0), {"name": "?"})
            next_lvl = HINATA_LEVELS.get(ud.get("hinata_level", 0) + 1)
            text = f"🖤 Хината\n\n💕 {hl['name']}\n❤️ Любовь: {ud.get('hinata_love', 0)}\n🎁 Подарков: {ud.get('gifts_to_hinata', 0)}"
            if next_lvl:
                text += f"\n\n📈 До «{next_lvl['name']}»: {next_lvl['min_love'] - ud.get('hinata_love', 0)}💕"
            safe_edit(text, cid, mid, markup=main_kb())
            bot.answer_callback_query(call.id)

        elif data == "achievements":
            ud = get_user_data(uid)
            earned = len(ud["achievements"])
            text = f"🏆 Достижения: {earned}/{len(ACHIEVEMENTS)}\n\n"
            for ach_id, ach in list(ACHIEVEMENTS.items())[:10]:
                status = "✅" if ach_id in ud["achievements"] else "🔒"
                text += f"{status} {ach['name']}\n"
            safe_edit(text, cid, mid, markup=main_kb())
            bot.answer_callback_query(call.id)

        elif data == "back_main":
            safe_edit("🖤", cid, mid, markup=main_kb())
            bot.answer_callback_query(call.id)

        # Настройки группы
        elif data == "noop":
            bot.answer_callback_query(call.id)

        elif data == "close":
            safe_delete(cid, mid)
            bot.answer_callback_query(call.id)

        elif data in ("cd10", "cu10", "as_tog", "wel_tog", "chat_stats"):
            if not is_admin(cid, uid):
                bot.answer_callback_query(call.id, "❌ Нет прав", show_alert=True)
                return
            s = get_gs(cid)
            if data == "cd10":
                s["response_chance"] = max(0, s["response_chance"] - 10)
            elif data == "cu10":
                s["response_chance"] = min(100, s["response_chance"] + 10)
            elif data == "as_tog":
                s["antispam_enabled"] = not s.get("antispam_enabled", True)
            elif data == "wel_tog":
                s["welcome_enabled"] = not s.get("welcome_enabled", True)
            elif data == "chat_stats":
                text = get_chat_stats_text(cid)
                safe_edit(text, cid, mid, markup=grp_kb(cid))
                bot.answer_callback_query(call.id)
                return
            save_settings()
            safe_edit(f"⚙ Настройки\nШанс: {s['response_chance']}%", cid, mid, markup=grp_kb(cid))
            bot.answer_callback_query(call.id)

        else:
            bot.answer_callback_query(call.id)

    except Exception as e:
        log.error(f"Callback: {e}")
        try:
            bot.answer_callback_query(call.id, "Ошибка")
        except:
            pass


def handle_track_callback(call, cid, mid):
    parts = call.data.split("_")
    if len(parts) < 3:
        bot.answer_callback_query(call.id, "Ошибка")
        return
    action = parts[-1]
    
    with pending_lock:
        pk = None
        for k in pending_tracks:
            if k.startswith(f"pend_{cid}_"):
                pk = k
                break
        if not pk:
            bot.answer_callback_query(call.id, "⏰ Устарело", show_alert=True)
            return
        
        if action == "x":
            pending_tracks.pop(pk, None)
            safe_edit("🖤", cid, mid)
            bot.answer_callback_query(call.id)
            return
        
        try:
            idx = int(action)
        except:
            bot.answer_callback_query(call.id, "Ошибка")
            return
        
        pd = pending_tracks.get(pk)
        if not pd or idx >= len(pd.get("results", [])):
            bot.answer_callback_query(call.id, "❌")
            return
        
        track = pd["results"][idx]
        pending_tracks.pop(pk, None)
    
    busy, bt = is_busy(cid)
    if busy:
        with pending_lock:
            pending_tracks[pk] = pd
        bot.answer_callback_query(call.id, get_busy_reply(bt), show_alert=True)
        return
    
    set_busy(cid, "music", track['title'])
    safe_edit(f"Качаю «{track['title'][:40]}»... 🎵", cid, mid)
    bot.answer_callback_query(call.id)
    threading.Thread(target=download_and_send_track, args=(cid, mid, track, call.from_user.id), daemon=True).start()


def download_and_send_track(cid, mid, track, uid):
    try:
        res, err = download_track(track['url'])
        if err:
            safe_edit(f"😔 {err}", cid, mid)
            return
        try:
            with open(res['file'], 'rb') as f:
                bot.send_audio(cid, f, title=res['title'], performer=res['artist'], duration=res['duration'])
            safe_delete(cid, mid)
            
            ud = get_user_data(uid)
            ud["tracks_downloaded"] = ud.get("tracks_downloaded", 0) + 1
            add_xp(uid, 10)
            
            with user_states_lock:
                pl_name = user_states.pop(f"addpl_{uid}", None)
            if pl_name:
                add_to_playlist(uid, pl_name, {"title": res['title'], "artist": res['artist'], "url": track['url']})
        finally:
            shutil.rmtree(res.get('temp_dir', ''), ignore_errors=True)
    except Exception as e:
        log.error(f"DL track: {e}")
        safe_edit("Ошибка", cid, mid)
    finally:
        clear_busy(cid)


def download_and_send(cid, mid, url, fmt, uid):
    try:
        if fmt == "audio":
            res, err = download_track(url)
            if err:
                safe_edit(f"😔 {err}", cid, mid)
                return
            try:
                with open(res['file'], 'rb') as f:
                    bot.send_audio(cid, f, title=res['title'], performer=res.get('artist', ''), duration=res.get('duration', 0))
                safe_delete(cid, mid)
                ud = get_user_data(uid)
                ud["tracks_downloaded"] = ud.get("tracks_downloaded", 0) + 1
                add_xp(uid, 10)
            finally:
                shutil.rmtree(res.get('temp_dir', ''), ignore_errors=True)
        else:
            res, err = download_video(url)
            if err:
                safe_edit(f"😔 {err}", cid, mid)
                return
            try:
                with open(res['file'], 'rb') as f:
                    bot.send_video(cid, f, caption=res.get('title', ''), duration=res.get('duration', 0), supports_streaming=True)
                safe_delete(cid, mid)
                add_xp(uid, 10)
            finally:
                shutil.rmtree(res.get('temp_dir', ''), ignore_errors=True)
    except Exception as e:
        log.error(f"DL: {e}")
        safe_edit("Ошибка", cid, mid)
    finally:
        clear_busy(cid)


# ================= СОБЫТИЯ =================
@bot.message_handler(content_types=['new_chat_members'])
def on_new_member(msg):
    try:
        bi = get_bot_info()
        for member in msg.new_chat_members:
            if bi and member.id == bi.id:
                s = get_gs(msg.chat.id)
                s["owner_id"] = msg.from_user.id
                s["owner_name"] = dname(msg.from_user)
                s["group_name"] = msg.chat.title
                save_settings()
                reg_group(msg.from_user.id, msg.chat.id, msg.chat.title)
                safe_send(msg.chat.id, "йо 🖤 я Хината\n/help — команды")
            else:
                s = get_gs(msg.chat.id)
                if s.get("welcome_enabled"):
                    text = s.get("welcome_message", "Добро пожаловать, {name}! 🖤")
                    text = text.replace("{name}", dname(member))
                    safe_send(msg.chat.id, text)
    except Exception as e:
        log.error(f"New member: {e}")


@bot.message_handler(content_types=['left_chat_member'])
def on_left_member(msg):
    try:
        bi = get_bot_info()
        if bi and msg.left_chat_member and msg.left_chat_member.id == bi.id:
            with user_groups_lock:
                for uk in list(user_groups.keys()):
                    user_groups[uk].pop(str(msg.chat.id), None)
            save_user_groups()
    except Exception as e:
        log.error(f"Left member: {e}")


@bot.message_handler(content_types=['voice', 'audio'])
def on_voice(msg):
    uid = msg.from_user.id
    ud = get_user_data(uid)
    ud["voice_messages"] = ud.get("voice_messages", 0) + 1
    add_xp(uid, XP_PER_VOICE)


@bot.message_handler(content_types=['photo', 'video', 'document'])
def on_media(msg):
    uid = msg.from_user.id
    ud = get_user_data(uid)
    ud["media_sent"] = ud.get("media_sent", 0) + 1
    add_xp(uid, XP_PER_MEDIA)


# ================= ТЕКСТОВЫЕ СООБЩЕНИЯ =================
@bot.message_handler(content_types=['text'])
def on_text(msg):
    try:
        if not msg.text or not msg.from_user:
            return
        
        cid = msg.chat.id
        uid = msg.from_user.id
        text = msg.text.strip()
        
        # Проверка мута
        muted, until = is_muted(cid, uid)
        if muted and is_grp(msg):
            try:
                bot.delete_message(cid, msg.message_id)
            except:
                pass
            return
        
        # XP и статистика
        ud = get_user_data(uid)
        ud["messages"] = ud.get("messages", 0) + 1
        new_level, bonus = add_xp(uid, XP_PER_MESSAGE)
        if new_level and is_grp(msg):
            safe_send(cid, f"🎉 {dname(msg.from_user)} достиг {new_level} уровня! +{bonus}💰")
        
        if is_grp(msg):
            update_chat_stats(cid, uid, text)
        
        # Антиспам
        if is_grp(msg):
            is_spam, reason = check_spam(text, cid)
            if is_spam and not is_admin(cid, uid):
                try:
                    bot.delete_message(cid, msg.message_id)
                    add_warn(cid, uid, f"спам: {reason}")
                except:
                    pass
                return
        
        # Создание плейлиста
        with user_states_lock:
            if user_states.pop(f"pl_create_{uid}", None):
                name = text[:30]
                ok, result = create_playlist(uid, name)
                safe_send(cid, result, markup=playlist_kb(uid) if is_pm(msg) else None)
                return
        
        # Выбор трека по номеру
        if text.isdigit() and 1 <= int(text) <= 8:
            with pending_lock:
                for pk, pv in list(pending_tracks.items()):
                    if pk.startswith(f"pend_{cid}_"):
                        idx = int(text) - 1
                        if idx < len(pv.get("results", [])):
                            track = pv["results"][idx]
                            del pending_tracks[pk]
                            busy, bt = is_busy(cid)
                            if busy:
                                safe_send(cid, get_busy_reply(bt))
                                return
                            set_busy(cid, "music", track['title'])
                            smsg = safe_send(cid, f"Качаю «{track['title'][:40]}»... 🎵")
                            if smsg:
                                threading.Thread(target=download_and_send_track, args=(cid, smsg.message_id, track, uid), daemon=True).start()
                            return
                        break
        
        # Проверка на ссылку для скачивания
        video_patterns = [
            r'(https?://(?:www\.)?(?:youtube\.com|youtu\.be)/\S+)',
            r'(https?://(?:www\.)?tiktok\.com/\S+)',
            r'(https?://(?:vm|vt)\.tiktok\.com/\S+)',
            r'(https?://(?:www\.)?instagram\.com/(?:reel|p)/\S+)',
            r'(https?://(?:www\.)?twitter\.com/\S+/status/\S+)',
            r'(https?://(?:www\.)?x\.com/\S+/status/\S+)',
        ]
        url_found = None
        for pattern in video_patterns:
            match = re.search(pattern, text)
            if match:
                url_found = match.group(1)
                break
        
        if url_found and any(word in text.lower() for word in ["скачай", "качай", "скинь", "загрузи", "download"]):
            smsg = safe_send(cid, "Формат?", markup=fmt_kb())
            if smsg:
                with user_states_lock:
                    user_states[f"dl_{cid}_{smsg.message_id}"] = url_found
            return
        
        # ЛС
        if is_pm(msg):
            bot.send_chat_action(cid, 'typing')
            add_msg(uid, "user", text)
            msgs = get_msgs_copy(uid)
            resp = ask_ai(msgs)
            if is_error(resp):
                safe_send(cid, resp.replace("[ERR]", ""), markup=main_kb())
                return
            clean_text, actions = parse_actions(resp)
            clean_text = clean_response(clean_text)
            if clean_text:
                add_msg(uid, "assistant", clean_text)
                safe_send(cid, clean_text, markup=main_kb())
            for action in actions:
                handle_action(cid, uid, action)
            return
        
        # Группа
        if not is_grp(msg):
            return
        
        s = get_gs(cid)
        bi = get_bot_info()
        bu = bi.username.lower() if bi and bi.username else ""
        
        is_reply = msg.reply_to_message and bi and msg.reply_to_message.from_user.id == bi.id
        is_mention = bu and f"@{bu}" in text.lower()
        is_name = is_named(text)
        direct = is_reply or is_mention or is_name
        
        if not direct:
            busy, _ = is_busy(cid)
            if busy or random.randint(1, 100) > s["response_chance"]:
                return
        
        busy, bt = is_busy(cid)
        if busy:
            if direct:
                safe_send(cid, get_busy_reply(bt))
            return
        
        bot.send_chat_action(cid, 'typing')
        add_msg(cid, "user", f"[{dname(msg.from_user)}]: {text}", True)
        msgs = get_msgs_copy(cid, True)
        resp = ask_ai(msgs)
        
        if is_error(resp):
            safe_send(cid, resp.replace("[ERR]", ""))
            return
        
        clean_text, actions = parse_actions(resp)
        clean_text = clean_response(clean_text)
        
        if clean_text:
            add_msg(cid, "assistant", clean_text, True)
            safe_send(cid, clean_text)
        
        for action in actions:
            handle_action(cid, uid, action)
            
    except Exception as e:
        log.error(f"Text: {e}")
        traceback.print_exc()


def handle_action(cid, uid, action):
    atype = action.get("type")
    
    if atype == "music_search" and action.get("query"):
        query = action["query"]
        busy, bt = is_busy(cid)
        if busy:
            safe_send(cid, get_busy_reply(bt))
            return
        set_busy(cid, "music", query)
        smsg = safe_send(cid, f"Ищу «{query}»... 🎵")
        if not smsg:
            clear_busy(cid)
            return
        
        def do_search():
            try:
                results = search_tracks(query)
                if not results:
                    safe_edit("Ничего не нашла 😔", cid, smsg.message_id)
                    return
                with pending_lock:
                    pending_tracks[f"pend_{cid}_{smsg.message_id}"] = {"results": results, "query": query, "time": datetime.now()}
                text = "🎵 Нашла:\n\n"
                for i, r in enumerate(results, 1):
                    dur = f"{r['duration']//60}:{r['duration']%60:02d}" if r['duration'] else "?"
                    text += f"{i}. {r['title']} ({dur})\n"
                text += "\nВыбери номер 🔥"
                safe_edit(text, cid, smsg.message_id, markup=track_kb(len(results), smsg.message_id))
            except Exception as e:
                log.error(f"Search: {e}")
                safe_edit("Ошибка поиска", cid, smsg.message_id)
            finally:
                clear_busy(cid)
        
        threading.Thread(target=do_search, daemon=True).start()
    
    elif atype == "video_download" and action.get("url"):
        url = action["url"]
        smsg = safe_send(cid, "Формат?", markup=fmt_kb())
        if smsg:
            with user_states_lock:
                user_states[f"dl_{cid}_{smsg.message_id}"] = url


# ================= ФОНОВЫЕ ЗАДАЧИ =================
def cleanup_loop():
    while True:
        try:
            time.sleep(CLEANUP_INTERVAL)
            now = time.time()
            
            # Очистка downloads
            if os.path.exists(DOWNLOADS_DIR):
                for item in os.listdir(DOWNLOADS_DIR):
                    p = os.path.join(DOWNLOADS_DIR, item)
                    try:
                        if os.path.isdir(p) and now - os.path.getmtime(p) > 1800:
                            shutil.rmtree(p, ignore_errors=True)
                    except:
                        pass
            
            # Очистка pending
            with pending_lock:
                to_del = [k for k, v in pending_tracks.items() 
                         if (datetime.now() - v.get("time", datetime.now())).total_seconds() > PENDING_TIMEOUT]
                for k in to_del:
                    del pending_tracks[k]
            
            # Сохранение данных
            save_user_data()
            save_chat_stats()
            
        except Exception as e:
            log.error(f"Cleanup: {e}")


# ================= ЗАПУСК =================
if __name__ == "__main__":
    print("=" * 50)
    print("    🖤 ХИНАТА v2.0 — ЗАПУСК 🖤")
    print("=" * 50)
    
    bi = get_bot_info()
    if bi:
        log.info(f"Бот: @{bi.username}")
    
    log.info(f"Модель: {MODEL_ID}")
    log.info(f"Юзеров: {len(user_data)}")
    log.info(f"Групп: {len(group_settings)}")
    
    # Проверка и установка разработчика
    for uid, ud in user_data.items():
        if ud.get("is_developer"):
            ud["coins"] = 999999999
            log.info(f"Разработчик: {uid}")
    save_user_data()
    
    # Запуск фоновых задач
    threading.Thread(target=cleanup_loop, daemon=True).start()
    threading.Thread(target=check_reminders_loop, daemon=True).start()
    
    print("=" * 50)
    print("    🖤 РАБОТАЕТ! 🖤")
    print("=" * 50)
    
    while True:
        try:
            bot.infinity_polling(
                allowed_updates=["message", "callback_query", "my_chat_member"],
                timeout=60,
                long_polling_timeout=60
            )
        except KeyboardInterrupt:
            log.info("Остановка...")
            save_user_data()
            save_chat_stats()
            save_hinata_state()
            break
        except Exception as e:
            log.error(f"Polling: {e}")
            time.sleep(5)
