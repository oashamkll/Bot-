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
import urllib.parse
import copy
import logging
import sqlite3
from queue import Queue
from concurrent.futures import ThreadPoolExecutor

# ====================== НАСТРОЙКА ЛОГИРОВАНИЯ ======================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger('hinata')

# ====================== ТОКЕНЫ ======================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")

if not TELEGRAM_BOT_TOKEN:
    log.critical("❌ TELEGRAM_BOT_TOKEN не найден!")
    sys.exit(1)
if not OPENROUTER_API_KEY:
    log.critical("❌ OPENROUTER_API_KEY не найден!")
    sys.exit(1)

# ====================== МОДЕЛИ AI ======================
AVAILABLE_MODELS = {
    # Бесплатные модели
    "gemini_flash": {"id": "google/gemini-2.0-flash-001", "name": "Gemini 2.0 Flash", "free": True, "cat": "google"},
    "gemini_pro": {"id": "google/gemini-pro", "name": "Gemini Pro", "free": True, "cat": "google"},
    "gemini_flash_lite": {"id": "google/gemini-2.0-flash-lite-001", "name": "Gemini 2.0 Flash Lite", "free": True, "cat": "google"},
    "gemma_27b": {"id": "google/gemma-2-27b-it", "name": "Gemma 2 27B", "free": True, "cat": "google"},
    "gemma_9b": {"id": "google/gemma-2-9b-it", "name": "Gemma 2 9B", "free": True, "cat": "google"},
    "llama_70b": {"id": "meta-llama/llama-3-70b-instruct", "name": "Llama 3 70B", "free": True, "cat": "meta"},
    "llama_8b": {"id": "meta-llama/llama-3-8b-instruct", "name": "Llama 3 8B", "free": True, "cat": "meta"},
    "llama_3.1_8b": {"id": "meta-llama/llama-3.1-8b-instruct:free", "name": "Llama 3.1 8B", "free": True, "cat": "meta"},
    "llama_3.1_70b": {"id": "meta-llama/llama-3.1-70b-instruct:free", "name": "Llama 3.1 70B", "free": True, "cat": "meta"},
    "llama_3.2_3b": {"id": "meta-llama/llama-3.2-3b-instruct:free", "name": "Llama 3.2 3B", "free": True, "cat": "meta"},
    "llama_3.2_11b_vision": {"id": "meta-llama/llama-3.2-11b-vision-instruct:free", "name": "Llama 3.2 11B Vision", "free": True, "cat": "meta"},
    "mixtral": {"id": "mistralai/mixtral-8x7b-instruct", "name": "Mixtral 8x7B", "free": True, "cat": "mistral"},
    "mistral_7b": {"id": "mistralai/mistral-7b-instruct:free", "name": "Mistral 7B", "free": True, "cat": "mistral"},
    "qwen_72b": {"id": "qwen/qwen-2-72b-instruct", "name": "Qwen 2 72B", "free": True, "cat": "qwen"},
    "qwen_7b": {"id": "qwen/qwen-2-7b-instruct:free", "name": "Qwen 2 7B", "free": True, "cat": "qwen"},
    "phi_3": {"id": "microsoft/phi-3-medium-128k-instruct", "name": "Phi 3 Medium", "free": True, "cat": "microsoft"},
    "phi_3_mini": {"id": "microsoft/phi-3-mini-128k-instruct:free", "name": "Phi 3 Mini", "free": True, "cat": "microsoft"},
    "deepseek": {"id": "deepseek/deepseek-chat", "name": "DeepSeek V2", "free": True, "cat": "deepseek"},
    
    # Платные модели
    "gpt_4o": {"id": "openai/gpt-4o", "name": "GPT-4o", "free": False, "cat": "openai"},
    "gpt_4o_mini": {"id": "openai/gpt-4o-mini", "name": "GPT-4o Mini", "free": False, "cat": "openai"},
    "claude_3.5_sonnet": {"id": "anthropic/claude-3.5-sonnet", "name": "Claude 3.5 Sonnet", "free": False, "cat": "anthropic"},
    "claude_3_haiku": {"id": "anthropic/claude-3-haiku", "name": "Claude 3 Haiku", "free": False, "cat": "anthropic"},
}

MODEL_CATEGORIES = {
    "google": "Google", "meta": "Meta (Llama)", "mistral": "Mistral",
    "qwen": "Qwen", "microsoft": "Microsoft", "deepseek": "DeepSeek",
    "openai": "OpenAI 💰", "anthropic": "Anthropic 💰",
}

# ====================== НАСТРОЙКИ БОТА ======================
BOT_NAME = "Хината"
BOT_NICKNAMES = ["хината", "хина", "hinata", "хинаточка", "хинатик"]
DEVELOPER_USERNAME = "PaceHoz"
DEVELOPER_IDS = set()

MAX_DURATION = 600  # Макс. длительность музыки (сек)
DOWNLOAD_TIMEOUT = 180
SESSION_MAX_MESSAGES = 60
LEARN_INTERVAL = 15
PENDING_TIMEOUT = 600
BUSY_TIMEOUT = 300
CLEANUP_INTERVAL = 600
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 MB
STATE_SAVE_INTERVAL = 300
SESSION_CLEANUP_AGE = 3600  # 1 час

# ====================== ЭКОНОМИКА ======================
CURRENCY_NAME = "коин"
CURRENCY_EMOJI = "🪙"
CURRENCY_PLURAL = ["коин", "коина", "коинов"]
DAILY_REWARD = 50
MESSAGE_REWARD = 2
VOICE_REWARD = 5
STICKER_REWARD = 1
INITIAL_BALANCE = 100

# ====================== УРОВНИ ======================
LEVELS = [
    {"level": 1, "xp": 0, "title": "🌟 Новичок"},
    {"level": 2, "xp": 100, "title": "✨ Активный"},
    {"level": 3, "xp": 300, "title": "⭐ Свой человек"},
    {"level": 4, "xp": 600, "title": "💫 Старожил"},
    {"level": 5, "xp": 1000, "title": "🌟 Легенда"},
    {"level": 6, "xp": 1500, "title": "👑 Полубог"},
    {"level": 7, "xp": 2500, "title": "💎 Бог чата"},
    {"level": 8, "xp": 4000, "title": "🔥 Бессмертный"},
    {"level": 9, "xp": 6000, "title": "⚡ Властелин"},
    {"level": 10, "xp": 10000, "title": "🌀 Абсолют"},
]

# ====================== МАГАЗИН ======================
SHOP_ITEMS = {
    # Услуги
    "compliment": {"name": "💌 Комплимент", "price": 30, "desc": "Хината скажет комплимент", "type": "hinata_action", "cat": "service"},
    "roast": {"name": "🔥 Рофл", "price": 40, "desc": "Хината подшутит над тобой", "type": "hinata_action", "cat": "service"},
    "poem": {"name": "📜 Стих", "price": 50, "desc": "Хината напишет стих", "type": "hinata_action", "cat": "service"},
    "fortune": {"name": "🔮 Предсказание", "price": 60, "desc": "Узнай своё будущее", "type": "hinata_action", "cat": "service"},
    "nickname": {"name": "🏷️ Прозвище", "price": 70, "desc": "Хината придумает прозвище", "type": "hinata_action", "cat": "service"},
    "story": {"name": "📖 История", "price": 80, "desc": "Короткая история о тебе", "type": "hinata_action", "cat": "service"},
    "advice": {"name": "🎯 Совет", "price": 25, "desc": "Мудрый совет", "type": "hinata_action", "cat": "service"},
    
    # Подарки
    "gift_rose": {"name": "🌹 Роза", "price": 15, "desc": "Красная роза", "type": "gift", "cat": "gift", "rel": 2},
    "gift_choco": {"name": "🍫 Шоколадка", "price": 25, "desc": "Сладкий подарок", "type": "gift", "cat": "gift", "rel": 3},
    "gift_teddy": {"name": "🧸 Мишка", "price": 40, "desc": "Мягкая игрушка", "type": "gift", "cat": "gift", "rel": 4},
    "gift_ring": {"name": "💍 Кольцо", "price": 100, "desc": "Блестящее колечко", "type": "gift", "cat": "gift", "rel": 7},
    "gift_crown": {"name": "👑 Корона", "price": 200, "desc": "Корона чемпиона", "type": "gift", "cat": "gift", "rel": 10},
    "gift_heart": {"name": "❤️ Сердце", "price": 150, "desc": "Хината тронута", "type": "gift", "cat": "gift", "rel": 8},
    
    # Для себя
    "double_xp": {"name": "⚡ 2x XP (1 час)", "price": 200, "desc": "Удвоенный опыт на час", "type": "boost", "cat": "self", "dur": 3600},
    "title_custom": {"name": "🏷️ Свой титул", "price": 1000, "desc": "Придумай себе титул", "type": "custom_title", "cat": "self"},
    "color_name": {"name": "🎨 Цвет имени", "price": 350, "desc": "Эмодзи в профиль", "type": "name_emoji", "cat": "self"},
    "vip_badge": {"name": "💎 VIP значок", "price": 500, "desc": "Особый статус", "type": "badge", "cat": "self", "badge": "💎"},
    "heart_badge": {"name": "❤️ Сердечко", "price": 300, "desc": "Значок любви", "type": "badge", "cat": "self", "badge": "❤️"},
    "star_badge": {"name": "⭐ Звезда", "price": 200, "desc": "Звездный значок", "type": "badge", "cat": "self", "badge": "⭐"},
}

# ====================== ОТНОШЕНИЯ ======================
RELATION_LEVELS = [
    {"min": -100, "max": -50, "title": "👿 Враг", "emoji": "👿"},
    {"min": -50, "max": -20, "title": "😤 Недоверие", "emoji": "😤"},
    {"min": -20, "max": 0, "title": "😐 Нейтралитет", "emoji": "😐"},
    {"min": 0, "max": 20, "title": "🙂 Знакомый", "emoji": "🙂"},
    {"min": 20, "max": 40, "title": "🤝 Приятель", "emoji": "🤝"},
    {"min": 40, "max": 60, "title": "😊 Друг", "emoji": "😊"},
    {"min": 60, "max": 80, "title": "❤️ Близкий друг", "emoji": "❤️"},
    {"min": 80, "max": 95, "title": "💖 Лучший друг", "emoji": "💖"},
    {"min": 95, "max": 200, "title": "💕 Половиночка", "emoji": "💕"},
]

# ====================== ДОСТИЖЕНИЯ ======================
ACHIEVEMENTS = {
    "first_msg": {"name": "🎙️ Первый шаг", "desc": "Отправить 1 сообщение", "xp": 10},
    "msg_100": {"name": "💬 Болтун", "desc": "100 сообщений", "xp": 50},
    "msg_500": {"name": "📢 Оратор", "desc": "500 сообщений", "xp": 100},
    "msg_1000": {"name": "🗣️ Легенда чата", "desc": "1000 сообщений", "xp": 200},
    "music_10": {"name": "🎧 Меломан", "desc": "10 песен", "xp": 50},
    "music_50": {"name": "🎵 DJ", "desc": "50 песен", "xp": 100},
    "daily_7": {"name": "📅 Неделя", "desc": "7 дней подряд", "xp": 70},
    "daily_30": {"name": "📆 Месяц", "desc": "30 дней подряд", "xp": 200},
    "rich_1000": {"name": "💰 Богач", "desc": "1000 монет", "xp": 50},
    "rich_5000": {"name": "💎 Магнат", "desc": "5000 монет", "xp": 100},
    "gift_first": {"name": "🎁 Первый подарок", "desc": "Подарить что-то", "xp": 30},
    "gift_10": {"name": "🎀 Щедрая душа", "desc": "10 подарков", "xp": 100},
    "level_5": {"name": "⭐ Ветеран", "desc": "5 уровень", "xp": 50},
    "level_10": {"name": "👑 Бог", "desc": "10 уровень", "xp": 200},
    "relation_50": {"name": "🤝 Дружба", "desc": "50 отношений", "xp": 80},
    "relation_90": {"name": "💖 Любовь", "desc": "90 отношений", "xp": 150},
    "game_first": {"name": "🎮 Игрок", "desc": "Сыграть 1 игру", "xp": 20},
    "game_win_10": {"name": "🏆 Чемпион", "desc": "10 побед", "xp": 80},
}

# ====================== НАСТРОЙКИ МОДЕРАЦИИ ======================
SPAM_THRESHOLD = 5
SPAM_WINDOW = 10
SPAM_MUTE_TIME = 60
MOD_ACTIONS = ["warn", "mute", "ban", "unban", "unmute", "unwarn"]

# ====================== ПУТИ К ФАЙЛАМ ======================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROMPT_FILE = os.path.join(SCRIPT_DIR, "prompt.txt")
SETTINGS_FILE = os.path.join(SCRIPT_DIR, "group_settings.json")
MEMORY_DIR = os.path.join(SCRIPT_DIR, "memory")
DOWNLOADS_DIR = os.path.join(SCRIPT_DIR, "downloads")
FFMPEG_DIR = os.path.join(SCRIPT_DIR, "ffmpeg_bin")
USER_GROUPS_FILE = os.path.join(SCRIPT_DIR, "user_groups.json")
STYLE_MEMORY_DIR = os.path.join(SCRIPT_DIR, "style_memory")
PLAYLISTS_DIR = os.path.join(SCRIPT_DIR, "playlists")
GAMES_DIR = os.path.join(SCRIPT_DIR, "games")
GIFTS_DIR = os.path.join(SCRIPT_DIR, "gifts")
GROUP_PLAYLISTS_DIR = os.path.join(SCRIPT_DIR, "group_playlists")
BOT_STATE_FILE = os.path.join(SCRIPT_DIR, "bot_state.json")
MOD_LOG_DIR = os.path.join(SCRIPT_DIR, "mod_logs")
DB_FILE = os.path.join(SCRIPT_DIR, "hinata.db")
CACHE_DIR = os.path.join(SCRIPT_DIR, "cache")

for d in [MEMORY_DIR, DOWNLOADS_DIR, FFMPEG_DIR, STYLE_MEMORY_DIR, 
          PLAYLISTS_DIR, GAMES_DIR, GIFTS_DIR, GROUP_PLAYLISTS_DIR, 
          MOD_LOG_DIR, CACHE_DIR]:
    os.makedirs(d, exist_ok=True)

# ====================== SQLITE ======================
def init_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    c = conn.cursor()
    
    # Экономика
    c.execute("""CREATE TABLE IF NOT EXISTS economy (
        uid INTEGER PRIMARY KEY,
        balance INTEGER DEFAULT 100,
        earned INTEGER DEFAULT 100,
        spent INTEGER DEFAULT 0,
        streak INTEGER DEFAULT 0,
        last_daily TEXT,
        tx TEXT DEFAULT '[]'
    )""")
    
    # Профили
    c.execute("""CREATE TABLE IF NOT EXISTS profiles (
        uid INTEGER PRIMARY KEY,
        xp INTEGER DEFAULT 0,
        level INTEGER DEFAULT 1,
        messages INTEGER DEFAULT 0,
        voice INTEGER DEFAULT 0,
        stickers INTEGER DEFAULT 0,
        music INTEGER DEFAULT 0,
        videos INTEGER DEFAULT 0,
        games INTEGER DEFAULT 0,
        wins INTEGER DEFAULT 0,
        gifts_given INTEGER DEFAULT 0,
        achievements TEXT DEFAULT '[]',
        badges TEXT DEFAULT '[]',
        relation INTEGER DEFAULT 10,
        joined TEXT,
        last_seen TEXT,
        title TEXT DEFAULT '',
        custom_title TEXT,
        boosts TEXT DEFAULT '{}',
        summaries INTEGER DEFAULT 0,
        pl_saves INTEGER DEFAULT 0,
        username TEXT,
        display_name TEXT,
        name_emoji TEXT,
        warns INTEGER DEFAULT 0
    )""")
    
    conn.commit()
    return conn

_db_lock = threading.Lock()
_db = init_db()

# ====================== ОЧЕРЕДЬ ЗАДАЧ ======================
task_queue = Queue()
executor = ThreadPoolExecutor(max_workers=5)

def add_task(func, *args, **kwargs):
    """Добавляет задачу в очередь"""
    task_queue.put((func, args, kwargs))

def worker():
    while True:
        try:
            func, args, kwargs = task_queue.get()
            try:
                func(*args, **kwargs)
            except Exception as e:
                log.error(f"Ошибка в задаче {func.__name__}: {e}")
            finally:
                task_queue.task_done()
        except Exception as e:
            log.error(f"Ошибка в воркере: {e}")
            time.sleep(1)

# Запускаем воркеров
for _ in range(5):
    t = threading.Thread(target=worker, daemon=True)
    t.start()

# ====================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ======================
def db_execute(query, params=(), fetch=False, fetchone=False):
    with _db_lock:
        try:
            c = _db.cursor()
            c.execute(query, params)
            if fetch:
                return c.fetchall()
            if fetchone:
                return c.fetchone()
            _db.commit()
            return c.lastrowid
        except Exception as e:
            log.error(f"Ошибка БД: {e} | {query[:80]}")
            return None

def find_ffmpeg():
    try:
        subprocess.run(["ffmpeg", "-version"], capture_output=True, timeout=5, check=True)
        return None
    except Exception:
        pass
    local = os.path.join(FFMPEG_DIR, "ffmpeg.exe" if sys.platform == "win32" else "ffmpeg")
    return FFMPEG_DIR if os.path.exists(local) else None

FFMPEG_LOCATION = find_ffmpeg()

def check_ffmpeg():
    try:
        cmd = "ffmpeg"
        if FFMPEG_LOCATION:
            cmd = os.path.join(FFMPEG_LOCATION, "ffmpeg.exe" if sys.platform == "win32" else "ffmpeg")
        subprocess.run([cmd, "-version"], capture_output=True, timeout=5)
        return True
    except Exception:
        return False

FFMPEG_AVAILABLE = check_ffmpeg()

# ====================== JSON УТИЛИТЫ ======================
def save_json(path, data):
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        shutil.move(tmp, path)
    except Exception as e:
        log.error(f"Ошибка сохранения {path}: {e}")

def load_json(path, default=None):
    if default is None:
        default = {}
    if not os.path.exists(path):
        return copy.deepcopy(default)
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if content:
                return json.loads(content)
    except Exception as e:
        log.error(f"Ошибка загрузки {path}: {e}")
    return copy.deepcopy(default)

# ====================== ПРОМПТ ======================
def load_system_prompt():
    if os.path.exists(PROMPT_FILE):
        try:
            with open(PROMPT_FILE, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    return content
        except Exception:
            pass
    
    # Базовый промпт с ограничением на 2-3 предложения
    return (
        "Ты — Хината, дружелюбный и милый ассистент в Telegram. "
        "Твоя задача — помогать людям, общаться с ними, развлекать и быть полезной.\n\n"
        "ВАЖНЫЕ ПРАВИЛА:\n"
        "1. Отвечай максимум 2-3 предложениями. Будь краткой, но информативной.\n"
        "2. Используй эмодзи, чтобы быть милее 😊\n"
        "3. Если тебя просят сделать что-то сложное — предложи помощь или объясни, как это сделать.\n"
        "4. Ты можешь искать музыку [MUSIC_SEARCH: запрос], скачивать видео [VIDEO_DOWNLOAD: ссылка], "
        "играть в игры, управлять экономикой.\n"
        "5. Относись к пользователям с теплотой, но не будь навязчивой.\n"
        "6. В группах общайся свободно, но не спамь.\n"
        "7. ПОМНИ: максимум 2-3 предложения в ответе!"
    )

DEFAULT_SYSTEM_PROMPT = load_system_prompt()

# ====================== СОСТОЯНИЕ БОТА ======================
def load_bot_state():
    state = load_json(BOT_STATE_FILE, {
        "current_model": "google/gemini-2.0-flash-001",
        "started_at": None,
        "restarts": 0
    })
    return state

def save_bot_state():
    state = {
        "current_model": CURRENT_MODEL,
        "started_at": datetime.now().strftime("%d.%m.%Y %H:%M"),
        "restarts": _bot_state.get("restarts", 0),
        "developer_ids": list(DEVELOPER_IDS),
    }
    save_json(BOT_STATE_FILE, state)

_bot_state = load_bot_state()
CURRENT_MODEL = _bot_state.get("current_model", "google/gemini-2.0-flash-001")

for did in _bot_state.get("developer_ids", []):
    try:
        DEVELOPER_IDS.add(int(did))
    except Exception:
        pass

# ====================== ИНИЦИАЛИЗАЦИЯ БОТА ======================
bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)

# Глобальные хранилища
chat_sessions = {}
group_settings = {}
user_states = {}
user_groups = {}
proactive_timers = {}
last_activity = {}
busy_chats = {}
pending_tracks = {}
spam_tracker = {}
active_games = {}
reminders = {}
secret_links = {}
pending_mod_actions = {}
group_members_cache = {}  # Кэш участников групп: {chat_id: {"members": [...], "updated": timestamp}}

# Блокировки
pending_lock = threading.Lock()
busy_lock = threading.Lock()
session_lock = threading.Lock()
settings_lock = threading.Lock()
user_states_lock = threading.Lock()
user_groups_lock = threading.Lock()
spam_lock = threading.Lock()
game_lock = threading.Lock()
model_lock = threading.Lock()
mod_lock = threading.Lock()
cache_lock = threading.Lock()

_bot_info_cache = None
_bot_info_lock = threading.Lock()

def get_bot_info():
    global _bot_info_cache
    with _bot_info_lock:
        if _bot_info_cache is None:
            try:
                _bot_info_cache = bot.get_me()
            except Exception as e:
                log.error(f"Ошибка get_me: {e}")
        return _bot_info_cache

# ====================== КЭШ УЧАСТНИКОВ ГРУПП ======================
def update_group_members_cache(chat_id):
    """Обновляет кэш участников группы"""
    try:
        admins = bot.get_chat_administrators(chat_id)
        members = []
        for admin in admins:
            user = admin.user
            if not user.is_bot:
                members.append({
                    "id": user.id,
                    "username": user.username,
                    "first_name": user.first_name,
                    "last_name": user.last_name,
                    "full_name": f"{user.first_name or ''} {user.last_name or ''}".strip(),
                    "is_admin": True,
                    "status": admin.status
                })
        
        # TODO: Получить обычных участников (API не позволяет получить полный список)
        # Временно используем тех, кто писал в чат
        
        with cache_lock:
            group_members_cache[chat_id] = {
                "members": members,
                "updated": time.time()
            }
        return members
    except Exception as e:
        log.error(f"Ошибка обновления кэша группы {chat_id}: {e}")
        return []

def get_group_members(chat_id):
    """Возвращает кэш участников группы (с автообновлением раз в час)"""
    with cache_lock:
        cache = group_members_cache.get(chat_id)
        if not cache or time.time() - cache["updated"] > 3600:  # 1 час
            # Обновляем в фоне
            add_task(update_group_members_cache, chat_id)
            return cache["members"] if cache else []
        return cache["members"]

def find_user_in_group(chat_id, name_or_username):
    """Ищет пользователя в группе по имени или юзернейму"""
    members = get_group_members(chat_id)
    target = name_or_username.lower().lstrip('@')
    
    for member in members:
        if member.get("username") and member["username"].lower() == target:
            return member["id"], member["full_name"] or member["first_name"]
        if member["full_name"] and member["full_name"].lower() == target:
            return member["id"], member["full_name"]
        if member["first_name"] and member["first_name"].lower() == target:
            return member["id"], member["first_name"]
    
    return None, None

# ====================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ======================
def plural(n, forms):
    n = abs(n)
    if n % 10 == 1 and n % 100 != 11:
        return forms[0]
    elif 2 <= n % 10 <= 4 and (n % 100 < 10 or n % 100 >= 20):
        return forms[1]
    return forms[2]

def fmt_coins(amount):
    return f"{amount} {CURRENCY_EMOJI}"

def is_developer(user):
    if not user:
        return False
    if user.id in DEVELOPER_IDS:
        return True
    if user.username and user.username.lower() == DEVELOPER_USERNAME.lower():
        DEVELOPER_IDS.add(user.id)
        save_bot_state()
        return True
    return False

def set_busy(chat_id, task_type, detail=""):
    with busy_lock:
        busy_chats[chat_id] = {"type": task_type, "time": datetime.now(), "detail": detail}

def clear_busy(chat_id):
    with busy_lock:
        busy_chats.pop(chat_id, None)

def is_busy(chat_id):
    with busy_lock:
        if chat_id not in busy_chats:
            return False, None
        info = busy_chats[chat_id]
        if (datetime.now() - info["time"]).total_seconds() > BUSY_TIMEOUT:
            del busy_chats[chat_id]
            return False, None
        return True, info["type"]

def safe_edit(text, chat_id, message_id, markup=None):
    try:
        bot.edit_message_text(text, chat_id, message_id, reply_markup=markup)
        return True
    except Exception:
        return False

def safe_delete(chat_id, message_id):
    try:
        bot.delete_message(chat_id, message_id)
        return True
    except Exception:
        return False

def safe_send(chat_id, text, markup=None, reply_to=None):
    try:
        return bot.send_message(chat_id, text, reply_markup=markup, reply_to_message_id=reply_to)
    except Exception as e:
        log.error(f"Ошибка отправки: {e}")
        return None

def safe_reply(message, text, markup=None):
    return safe_send(message.chat.id, text, markup=markup, reply_to=message.message_id)

def get_display_name(user):
    if not user:
        return "Неизвестно"
    first = (user.first_name or "").strip()
    last = (user.last_name or "").strip()
    if first and last:
        return f"{first} {last}"
    return first or last or user.username or "Пользователь"

# ====================== ЗАГРУЗКА НАСТРОЕК ======================
def save_settings():
    with settings_lock:
        save_json(SETTINGS_FILE, group_settings)

def load_settings():
    global group_settings
    with settings_lock:
        group_settings = load_json(SETTINGS_FILE, {})

def save_user_groups():
    with user_groups_lock:
        save_json(USER_GROUPS_FILE, user_groups)

def load_user_groups():
    global user_groups
    with user_groups_lock:
        user_groups = load_json(USER_GROUPS_FILE, {})

load_settings()
load_user_groups()

DEFAULT_GS = {
    "response_chance": 30,
    "owner_id": None,
    "owner_name": None,
    "admins": {},
    "custom_prompt": None,
    "proactive_enabled": False,
    "proactive_min": 30,
    "proactive_max": 120,
    "hours_start": 9,
    "hours_end": 23,
    "learn_style": True,
    "group_name": None,
    "antispam": True,
    "moderation": False,
    "mod_rules": "",
    "auto_admin": True
}

def get_group_settings(chat_id):
    key = str(chat_id)
    with settings_lock:
        if key not in group_settings:
            group_settings[key] = {}
        settings = group_settings[key]
        changed = False
        for k, v in DEFAULT_GS.items():
            if k not in settings:
                settings[k] = v
                changed = True
        if changed:
            save_json(SETTINGS_FILE, group_settings)
        return settings

def is_owner(chat_id, user_id):
    return get_group_settings(chat_id).get("owner_id") == user_id

def is_admin(chat_id, user_id):
    if user_id in DEVELOPER_IDS:
        return True
    settings = get_group_settings(chat_id)
    if settings.get("owner_id") == user_id:
        return True
    if str(user_id) in settings.get("admins", {}):
        return True
    if settings.get("auto_admin"):
        try:
            member = bot.get_chat_member(chat_id, user_id)
            if member.status in ("administrator", "creator"):
                return True
        except Exception:
            pass
    return False

def register_group(user_id, chat_id, title):
    key = str(user_id)
    with user_groups_lock:
        if key not in user_groups:
            user_groups[key] = {}
        user_groups[key][str(chat_id)] = {
            "title": title or "Без названия",
            "added": datetime.now().strftime("%d.%m.%Y %H:%M")
        }
        save_user_groups()

def get_user_groups(user_id):
    with user_groups_lock:
        return copy.deepcopy(user_groups.get(str(user_id), {}))

# ====================== ЭКОНОМИКА (SQLite) ======================
def ensure_economy(uid):
    row = db_execute("SELECT uid FROM economy WHERE uid=?", (uid,), fetchone=True)
    if not row:
        db_execute("INSERT OR IGNORE INTO economy (uid) VALUES (?)", (uid,))

def get_balance(uid):
    if uid in DEVELOPER_IDS:
        return 999999999
    ensure_economy(uid)
    row = db_execute("SELECT balance FROM economy WHERE uid=?", (uid,), fetchone=True)
    return row[0] if row else INITIAL_BALANCE

def add_coins(uid, amount, reason=""):
    if uid in DEVELOPER_IDS:
        return 999999999
    ensure_economy(uid)
    with _db_lock:
        try:
            c = _db.cursor()
            c.execute("SELECT balance, earned, spent, tx FROM economy WHERE uid=?", (uid,))
            row = c.fetchone()
            if not row:
                return 0
            
            balance, earned, spent, tx_json = row
            balance += amount
            
            if amount > 0:
                earned += amount
            else:
                spent += abs(amount)
            
            try:
                tx = json.loads(tx_json) if tx_json else []
            except Exception:
                tx = []
            
            tx.append({
                "amt": amount,
                "why": reason,
                "when": datetime.now().strftime("%d.%m.%Y %H:%M"),
                "bal": balance
            })
            tx = tx[-100:]
            
            c.execute("UPDATE economy SET balance=?, earned=?, spent=?, tx=? WHERE uid=?",
                     (balance, earned, spent, json.dumps(tx, ensure_ascii=False), uid))
            _db.commit()
            return balance
        except Exception as e:
            log.error(f"Ошибка add_coins: {e}")
            return 0

def spend_coins(uid, amount, reason=""):
    if uid in DEVELOPER_IDS:
        return True
    ensure_economy(uid)
    with _db_lock:
        try:
            c = _db.cursor()
            c.execute("SELECT balance, spent, tx FROM economy WHERE uid=?", (uid,))
            row = c.fetchone()
            if not row or row[0] < amount:
                return False
            
            balance = row[0] - amount
            spent_total = row[1] + amount
            
            try:
                tx = json.loads(row[2]) if row[2] else []
            except Exception:
                tx = []
            
            tx.append({
                "amt": -amount,
                "why": reason,
                "when": datetime.now().strftime("%d.%m.%Y %H:%M"),
                "bal": balance
            })
            tx = tx[-100:]
            
            c.execute("UPDATE economy SET balance=?, spent=?, tx=? WHERE uid=?",
                     (balance, spent_total, json.dumps(tx, ensure_ascii=False), uid))
            _db.commit()
            return True
        except Exception as e:
            log.error(f"Ошибка spend_coins: {e}")
            return False

def transfer_coins(from_uid, to_uid, amount, reason="перевод"):
    """Перевод монет между пользователями"""
    if from_uid in DEVELOPER_IDS:
        return False, "Нельзя переводить разработчику"
    if to_uid in DEVELOPER_IDS:
        return False, "Нельзя переводить разработчику"
    if from_uid == to_uid:
        return False, "Нельзя переводить самому себе"
    if amount < 1:
        return False, "Сумма должна быть положительной"
    
    ensure_economy(from_uid)
    ensure_economy(to_uid)
    
    with _db_lock:
        try:
            c = _db.cursor()
            # Проверяем баланс отправителя
            c.execute("SELECT balance FROM economy WHERE uid=?", (from_uid,))
            row = c.fetchone()
            if not row or row[0] < amount:
                return False, f"Недостаточно средств. Нужно {amount} {CURRENCY_EMOJI}"
            
            # Списываем у отправителя
            c.execute("UPDATE economy SET balance = balance - ? WHERE uid=?", (amount, from_uid))
            
            # Начисляем получателю
            c.execute("UPDATE economy SET balance = balance + ?, earned = earned + ? WHERE uid=?",
                     (amount, amount, to_uid))
            
            # Записываем транзакции
            for uid, delta in [(from_uid, -amount), (to_uid, amount)]:
                c.execute("SELECT tx FROM economy WHERE uid=?", (uid,))
                tx_row = c.fetchone()
                try:
                    tx = json.loads(tx_row[0]) if tx_row and tx_row[0] else []
                except Exception:
                    tx = []
                
                tx.append({
                    "amt": delta,
                    "why": f"перевод {'от' if delta < 0 else 'от'} {from_uid if delta < 0 else to_uid}",
                    "when": datetime.now().strftime("%d.%m.%Y %H:%M"),
                    "bal": get_balance(uid)  # Получим после обновления
                })
                tx = tx[-100:]
                c.execute("UPDATE economy SET tx=? WHERE uid=?", (json.dumps(tx, ensure_ascii=False), uid))
            
            _db.commit()
            return True, f"Переведено {amount} {CURRENCY_EMOJI}"
        except Exception as e:
            log.error(f"Ошибка transfer_coins: {e}")
            return False, "Ошибка при переводе"

def claim_daily(uid):
    ensure_economy(uid)
    with _db_lock:
        try:
            c = _db.cursor()
            c.execute("SELECT balance, earned, streak, last_daily FROM economy WHERE uid=?", (uid,))
            row = c.fetchone()
            if not row:
                return None, 0, 0
            
            balance, earned, streak, last_daily = row
            now = datetime.now().strftime("%Y-%m-%d")
            
            if last_daily == now and uid not in DEVELOPER_IDS:
                return None, 0, 0
            
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            streak = (streak + 1) if last_daily == yesterday else 1
            bonus = min(streak * 5, 100)
            total = DAILY_REWARD + bonus
            
            if uid in DEVELOPER_IDS:
                balance = 999999999
            else:
                balance += total
                earned += total
            
            c.execute("UPDATE economy SET balance=?, earned=?, streak=?, last_daily=? WHERE uid=?",
                     (balance, earned, streak, now, uid))
            _db.commit()
            return total, streak, bonus
        except Exception as e:
            log.error(f"Ошибка claim_daily: {e}")
            return None, 0, 0

# ====================== ПРОФИЛИ ======================
def ensure_profile(uid):
    row = db_execute("SELECT uid FROM profiles WHERE uid=?", (uid,), fetchone=True)
    if not row:
        now = datetime.now().strftime("%d.%m.%Y")
        now_full = datetime.now().strftime("%d.%m.%Y %H:%M")
        db_execute(
            "INSERT OR IGNORE INTO profiles (uid, joined, last_seen) VALUES (?, ?, ?)",
            (uid, now, now_full)
        )

PROFILE_FIELDS = [
    "xp", "level", "messages", "voice", "stickers", "music", "videos",
    "games", "wins", "gifts_given", "achievements", "badges", "relation",
    "joined", "last_seen", "title", "custom_title", "boosts", "summaries",
    "pl_saves", "username", "display_name", "name_emoji", "warns"
]

def load_profile(uid):
    ensure_profile(uid)
    row = db_execute(
        f"SELECT {','.join(PROFILE_FIELDS)} FROM profiles WHERE uid=?",
        (uid,), fetchone=True
    )
    if not row:
        return default_profile()
    
    profile = {}
    for i, field in enumerate(PROFILE_FIELDS):
        val = row[i]
        if field in ("achievements", "badges"):
            try:
                profile[field] = json.loads(val) if val else []
            except Exception:
                profile[field] = []
        elif field == "boosts":
            try:
                profile[field] = json.loads(val) if val else {}
            except Exception:
                profile[field] = {}
        else:
            profile[field] = val
    return profile

def default_profile():
    return {
        "xp": 0, "level": 1, "messages": 0, "voice": 0,
        "stickers": 0, "music": 0, "videos": 0,
        "games": 0, "wins": 0, "gifts_given": 0,
        "achievements": [], "badges": [], "relation": 10,
        "joined": datetime.now().strftime("%d.%m.%Y"),
        "last_seen": datetime.now().strftime("%d.%m.%Y %H:%M"),
        "title": "", "custom_title": None,
        "boosts": {}, "summaries": 0, "pl_saves": 0,
        "username": None, "display_name": None,
        "name_emoji": None, "warns": 0
    }

def save_profile(uid, profile):
    ensure_profile(uid)
    sets = []
    vals = []
    for field in PROFILE_FIELDS:
        if field in profile:
            val = profile[field]
            if field in ("achievements", "badges"):
                val = json.dumps(val, ensure_ascii=False) if isinstance(val, list) else "[]"
            elif field == "boosts":
                val = json.dumps(val, ensure_ascii=False) if isinstance(val, dict) else "{}"
            sets.append(f"{field}=?")
            vals.append(val)
    if sets:
        vals.append(uid)
        db_execute(f"UPDATE profiles SET {','.join(sets)} WHERE uid=?", tuple(vals))

def add_xp(uid, amount):
    ensure_profile(uid)
    with _db_lock:
        try:
            c = _db.cursor()
            c.execute("SELECT xp, level, boosts FROM profiles WHERE uid=?", (uid,))
            row = c.fetchone()
            if not row:
                return 0, 1, False
            
            xp, old_level, boosts_json = row
            
            try:
                boosts = json.loads(boosts_json) if boosts_json else {}
            except Exception:
                boosts = {}
            
            # Проверяем буст 2x XP
            if boosts.get("double_xp"):
                try:
                    exp = datetime.strptime(boosts["double_xp"], "%Y-%m-%d %H:%M:%S")
                    if datetime.now() < exp:
                        amount *= 2
                    else:
                        del boosts["double_xp"]
                except Exception:
                    boosts.pop("double_xp", None)
            
            xp += amount
            new_level = old_level
            title = "Новичок"
            
            for level in LEVELS:
                if xp >= level["xp"]:
                    new_level = level["level"]
                    title = level["title"]
            
            c.execute("UPDATE profiles SET xp=?, level=?, title=?, boosts=? WHERE uid=?",
                     (xp, new_level, title, json.dumps(boosts, ensure_ascii=False), uid))
            _db.commit()
            return xp, new_level, new_level > old_level
        except Exception as e:
            log.error(f"Ошибка add_xp: {e}")
            return 0, 1, False

def update_stat(uid, stat, inc=1):
    ensure_profile(uid)
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    db_execute(f"UPDATE profiles SET {stat}={stat}+?, last_seen=? WHERE uid=?", (inc, now, uid))
    row = db_execute(f"SELECT {stat} FROM profiles WHERE uid=?", (uid,), fetchone=True)
    return row[0] if row else 0

def update_user_info(uid, user):
    ensure_profile(uid)
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    db_execute("UPDATE profiles SET username=?, display_name=?, last_seen=? WHERE uid=?",
               (user.username, get_display_name(user), now, uid))

def change_relation(uid, amount):
    ensure_profile(uid)
    with _db_lock:
        try:
            c = _db.cursor()
            c.execute("SELECT relation FROM profiles WHERE uid=?", (uid,))
            row = c.fetchone()
            if not row:
                return 10
            new_rel = max(-100, min(100, row[0] + amount))
            c.execute("UPDATE profiles SET relation=? WHERE uid=?", (new_rel, uid))
            _db.commit()
            return new_rel
        except Exception as e:
            log.error(f"Ошибка change_relation: {e}")
            return 10

def get_relation_info(uid):
    profile = load_profile(uid)
    rel = profile.get("relation", 10) or 10
    for level in RELATION_LEVELS:
        if level["min"] <= rel < level["max"]:
            return rel, level["title"], level["emoji"]
    return rel, "Незнакомец", "👤"

def relation_bar(rel):
    shifted = rel + 100
    filled = max(0, min(20, int((shifted / 200) * 20)))
    
    if rel < -20:
        bar_char = "⬛"
    elif rel < 20:
        bar_char = "🟫"
    elif rel < 60:
        bar_char = "🟨"
    else:
        bar_char = "🟩"
    
    return f"{bar_char * filled}{'⬜' * (20 - filled)}"

def check_achievements(uid):
    profile = load_profile(uid)
    eco = db_execute("SELECT balance, streak FROM economy WHERE uid=?", (uid,), fetchone=True)
    eco_balance = eco[0] if eco else 0
    eco_streak = eco[1] if eco else 0
    
    new_achievements = []
    existing = set(profile.get("achievements") or [])
    
    checks = {
        "first_msg": (profile.get("messages") or 0) >= 1,
        "msg_100": (profile.get("messages") or 0) >= 100,
        "msg_500": (profile.get("messages") or 0) >= 500,
        "msg_1000": (profile.get("messages") or 0) >= 1000,
        "music_10": (profile.get("music") or 0) >= 10,
        "music_50": (profile.get("music") or 0) >= 50,
        "daily_7": (eco_streak or 0) >= 7,
        "daily_30": (eco_streak or 0) >= 30,
        "rich_1000": (eco_balance or 0) >= 1000,
        "rich_5000": (eco_balance or 0) >= 5000,
        "gift_first": (profile.get("gifts_given") or 0) >= 1,
        "gift_10": (profile.get("gifts_given") or 0) >= 10,
        "level_5": (profile.get("level") or 1) >= 5,
        "level_10": (profile.get("level") or 1) >= 10,
        "relation_50": (profile.get("relation") or 0) >= 50,
        "relation_90": (profile.get("relation") or 0) >= 90,
        "game_first": (profile.get("games") or 0) >= 1,
        "game_win_10": (profile.get("wins") or 0) >= 10,
    }
    
    for aid, cond in checks.items():
        if cond and aid not in existing and aid in ACHIEVEMENTS:
            new_achievements.append(aid)
    
    if new_achievements:
        achs_list = list(existing) + new_achievements
        total_xp = profile.get("xp", 0)
        for aid in new_achievements:
            total_xp += ACHIEVEMENTS[aid]["xp"]
        
        new_level = 1
        new_title = "Новичок"
        for level in LEVELS:
            if total_xp >= level["xp"]:
                new_level = level["level"]
                new_title = level["title"]
        
        save_profile(uid, {
            "achievements": achs_list,
            "xp": total_xp,
            "level": new_level,
            "title": new_title
        })
    
    return new_achievements

def notify_achievements(chat_id, uid, achievements, reply_to=None):
    for aid in achievements:
        a = ACHIEVEMENTS.get(aid, {})
        safe_send(chat_id,
                 f"🏆 Достижение: {a.get('name', '?')}\n"
                 f"{a.get('desc', '')}\n"
                 f"+{a.get('xp', 0)} XP",
                 reply_to=reply_to)

# ====================== ПОДАРКИ ======================
def load_gifts(uid):
    return load_json(os.path.join(GIFTS_DIR, f"{uid}.json"), {"received": [], "given": []})

def save_gifts(uid, data):
    save_json(os.path.join(GIFTS_DIR, f"{uid}.json"), data)

def record_gift(from_uid, from_name, gift_item):
    # Для получателя (условный uid 0 - общая копилка)
    gifts = load_gifts(0)
    gifts["received"].append({
        "from_uid": from_uid,
        "from_name": from_name,
        "item": gift_item["name"],
        "price": gift_item["price"],
        "when": datetime.now().strftime("%d.%m.%Y %H:%M")
    })
    gifts["received"] = gifts["received"][-200:]
    save_gifts(0, gifts)
    
    # Для отправителя
    g2 = load_gifts(from_uid)
    g2["given"].append({
        "item": gift_item["name"],
        "price": gift_item["price"],
        "when": datetime.now().strftime("%d.%m.%Y %H:%M")
    })
    g2["given"] = g2["given"][-100:]
    save_gifts(from_uid, g2)

# ====================== ПЛЕЙЛИСТЫ ======================
def load_playlist(uid):
    return load_json(os.path.join(PLAYLISTS_DIR, f"{uid}.json"), {"tracks": []})

def save_playlist(uid, data):
    save_json(os.path.join(PLAYLISTS_DIR, f"{uid}.json"), data)

def load_group_playlist(chat_id):
    return load_json(os.path.join(GROUP_PLAYLISTS_DIR, f"{chat_id}.json"), {"tracks": []})

def save_group_playlist(chat_id, data):
    save_json(os.path.join(GROUP_PLAYLISTS_DIR, f"{chat_id}.json"), data)

def add_to_playlist(uid, track, group_chat_id=None, save_personal=True, save_group=True):
    added = False
    
    if save_personal:
        pl = load_playlist(uid)
        if not any(t.get("url") == track.get("url") for t in pl["tracks"]):
            pl["tracks"].append({
                "title": track.get("title", "?"),
                "artist": track.get("artist", ""),
                "url": track.get("url", ""),
                "duration": track.get("duration", 0),
                "added": datetime.now().strftime("%d.%m.%Y %H:%M"),
                "added_by": uid
            })
            pl["tracks"] = pl["tracks"][-50:]
            save_playlist(uid, pl)
            update_stat(uid, "pl_saves")
            added = True
    
    if save_group and group_chat_id:
        gpl = load_group_playlist(group_chat_id)
        if not any(t.get("url") == track.get("url") for t in gpl["tracks"]):
            gpl["tracks"].append({
                "title": track.get("title", "?"),
                "artist": track.get("artist", ""),
                "url": track.get("url", ""),
                "duration": track.get("duration", 0),
                "added": datetime.now().strftime("%d.%m.%Y %H:%M"),
                "added_by": uid
            })
            gpl["tracks"] = gpl["tracks"][-100:]
            save_group_playlist(group_chat_id, gpl)
            added = True
    
    return added

# ====================== ПАМЯТЬ ======================
def empty_memory():
    return {"users": {}, "facts": [], "topics": [], "learned_at": None}

def load_memory(chat_id):
    return load_json(os.path.join(MEMORY_DIR, f"{chat_id}_memory.json"), empty_memory())

def save_memory(chat_id, mem):
    save_json(os.path.join(MEMORY_DIR, f"{chat_id}_memory.json"), mem)

def remember_user(chat_id, user):
    if not user:
        return
    uid = str(user.id)
    name = get_display_name(user)
    mem = load_memory(chat_id)
    
    if uid not in mem["users"]:
        mem["users"][uid] = {
            "name": name,
            "tg_name": name,
            "traits": [],
            "interests": [],
            "notes": [],
            "preferred_name": None
        }
        save_memory(chat_id, mem)
    else:
        u = mem["users"][uid]
        changed = False
        if u.get("tg_name") != name:
            u["tg_name"] = name
            changed = True
        if u.get("name") != name and not u.get("preferred_name"):
            u["name"] = name
            changed = True
        if changed:
            save_memory(chat_id, mem)

# ====================== АНТИСПАМ ======================
def check_spam(chat_id, user_id):
    with spam_lock:
        now = time.time()
        key = f"{chat_id}_{user_id}"
        
        if key not in spam_tracker:
            spam_tracker[key] = {"times": [], "warns": 0, "muted_until": 0}
        
        tracker = spam_tracker[key]
        
        if now < tracker.get("muted_until", 0):
            return True, tracker["muted_until"] - now
        
        tracker["times"] = [x for x in tracker["times"] if now - x < SPAM_WINDOW]
        tracker["times"].append(now)
        
        if len(tracker["times"]) >= SPAM_THRESHOLD:
            tracker["warns"] = tracker.get("warns", 0) + 1
            tracker["muted_until"] = now + SPAM_MUTE_TIME * tracker["warns"]
            tracker["times"] = []
            return True, SPAM_MUTE_TIME * tracker["warns"]
        
        return False, 0

# ====================== ИГРЫ ======================
class TruthOrDare:
    TRUTHS = [
        "Какая у тебя самая странная привычка?",
        "Кому из присутствующих ты доверяешь больше всех?",
        "Какую самую дорогую вещь ты разбил/потерял?",
        "Был ли у тебя смешной случай в общественном месте?",
        "Что ты никогда не сделаешь даже за миллион?",
        "Какая твоя самая большая тайна?",
        "Кого из знаменитостей ты бы поцеловал?",
        "Сколько раз ты влюблялся?"
    ]
    
    DARES = [
        "Отправь рандомный стикер",
        "Напиши комплимент предыдущему оратору",
        "Придумай прозвище для соседа сверху",
        "Спой куплет любимой песни",
        "Расскажи анекдот",
        "Пришли фото своей кружки",
        "Напиши признание в любви боту",
        "Сделай комплимент самому себе"
    ]

class QuizGame:
    QUESTIONS = [
        {"q": "Сколько планет в Солнечной системе?", "opts": ["7", "8", "9", "10"], "a": ["8"]},
        {"q": "Кто написал 'Войну и мир'?", "opts": ["Достоевский", "Толстой", "Чехов", "Пушкин"], "a": ["толстой"]},
        {"q": "Столица Франции?", "opts": ["Лондон", "Берлин", "Париж", "Мадрид"], "a": ["париж"]},
        {"q": "Сколько ног у паука?", "opts": ["6", "8", "10", "12"], "a": ["8"]},
        {"q": "Какой язык программирования назван в честь комедийного шоу?", "opts": ["Python", "Java", "C++", "Ruby"], "a": ["python"]},
        {"q": "Что измеряется в амперах?", "opts": ["Напряжение", "Сопротивление", "Сила тока", "Мощность"], "a": ["сила тока"]},
    ]

class NumberGame:
    def __init__(self):
        self.number = random.randint(1, 100)
        self.attempts = 0
        self.max_attempts = 7

class WordGame:
    WORDS = ["питон", "кофе", "солнце", "книга", "музыка", "робот", "дружба"]
    
    def __init__(self):
        self.word = random.choice(self.WORDS)
        self.guessed = set()
        self.wrong = 0
        self.max_wrong = 6
    
    def display(self):
        return " ".join(c if c in self.guessed else "⬜" for c in self.word)
    
    def solved(self):
        return all(c in self.guessed for c in self.word)
    
    def guess(self, letter):
        letter = letter.lower()
        if letter in self.guessed:
            return "repeat"
        self.guessed.add(letter)
        if letter in self.word:
            return "correct"
        self.wrong += 1
        return "wrong"

# ====================== ПОИСК В ИНТЕРНЕТЕ ======================
def web_search(query, n=5):
    results = []
    try:
        r = requests.get("https://api.duckduckgo.com/",
                        params={"q": query, "format": "json", "no_html": 1, "skip_disambig": 1},
                        timeout=8)
        if r.status_code == 200:
            data = r.json()
            if data.get("AbstractText"):
                results.append(data["AbstractText"])
            for topic in data.get("RelatedTopics", []):
                if isinstance(topic, dict) and topic.get("Text"):
                    results.append(topic["Text"])
    except Exception:
        pass
    
    if len(results) < 2:
        try:
            r = requests.get("https://ru.wikipedia.org/api/rest_v1/page/summary/" +
                            urllib.parse.quote(query), timeout=8)
            if r.status_code == 200:
                extract = r.json().get("extract", "")
                if extract:
                    results.append(extract)
        except Exception:
            pass
    
    return "\n\n".join(results[:n]) if results else None

# ====================== AI ФУНКЦИИ ======================
def build_prompt(chat_id=None, is_group=False, uid=None):
    base_prompt = get_group_settings(chat_id).get("custom_prompt") if (chat_id and is_group) else DEFAULT_SYSTEM_PROMPT
    
    # Добавляем информацию об участниках группы
    if is_group and chat_id:
        members = get_group_members(chat_id)
        if members:
            member_list = "\n".join([
                f"• {m['full_name']} (@{m['username'] if m['username'] else 'нет'})"
                for m in members[:10]  # Показываем первых 10
            ])
            base_prompt += f"\n\nУчастники группы (первые 10):\n{member_list}"
    
    return base_prompt

def ask_ai(messages, max_tokens=300):
    """Отправка запроса к AI с ограничением токенов"""
    try:
        with model_lock:
            current = CURRENT_MODEL
        
        filtered = [{"role": m["role"], "content": m["content"]}
                   for m in messages if m.get("content") and m.get("role")]
        
        if not filtered:
            return "[ERR] Нет сообщений"
        
        response = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://t.me/hinata_bot",
                "X-Title": "Hinata Bot"
            },
            json={
                "model": current,
                "messages": filtered,
                "max_tokens": max_tokens,
                "temperature": 0.7,
                "top_p": 0.9
            },
            timeout=120
        )
        
        if response.status_code == 200:
            data = response.json()
            choices = data.get("choices", [])
            if choices:
                content = choices[0].get("message", {}).get("content", "")
                return content.strip() if content else "..."
        
        if response.status_code in (429, 402, 403):
            return f"[ERR] Лимит API: {response.status_code}"
        
        return f"[ERR] Ошибка API: {response.status_code}"
    
    except requests.exceptions.Timeout:
        return "[ERR] Таймаут API"
    except Exception as e:
        log.error(f"Ошибка AI: {e}")
        return f"[ERR] {str(e)[:50]}"

def is_error(response):
    return isinstance(response, str) and response.startswith("[ERR]")

def clean_text(text):
    if not text:
        return ""
    text = text.strip()
    # Убираем служебные метки
    for pat in [r'\[MUSIC_SEARCH:.*?\]', r'\[VIDEO_DOWNLOAD:.*?\]',
                r'\[PLAYLIST_PLAY:.*?\]', r'\[REMINDER:.*?\]']:
        text = re.sub(pat, '', text)
    # Убираем лишние пробелы и переносы
    text = re.sub(r' +', ' ', text)
    text = re.sub(r'\n\s*\n\s*\n', '\n\n', text)
    return text.strip()

def parse_actions(text):
    actions = []
    clean_text = text
    
    # MOD_REQUEST
    m = re.search(r'\[MOD_REQUEST:\s*(.+?)\]', text)
    if m:
        action_text = m.group(1).strip()
        clean_text = text[:m.start()].strip()
        actions.append({"type": "mod_request", "action": action_text})
    
    # MUSIC_SEARCH
    m = re.search(r'\[MUSIC_SEARCH:\s*(.+?)\]', text)
    if m:
        query = m.group(1).strip()
        clean_text = text[:m.start()].strip()
        if query and len(query) > 1:
            actions.append({"type": "music_search", "query": query})
    
    # VIDEO_DOWNLOAD
    m = re.search(r'\[VIDEO_DOWNLOAD:\s*(.+?)\]', text)
    if m:
        url = m.group(1).strip()
        clean_text = text[:m.start()].strip()
        if url.startswith("http"):
            actions.append({"type": "video_download", "url": url})
    
    # REMINDER
    m = re.search(r'\[REMINDER:\s*(\d+)\s*\|\s*(.+?)\]', text)
    if m:
        minutes = int(m.group(1))
        reminder_text = m.group(2).strip()
        clean_text = text[:m.start()].strip()
        actions.append({"type": "reminder", "minutes": minutes, "text": reminder_text})
    
    return clean_text, actions

# ====================== YT-DLP ФУНКЦИИ ======================
def get_ydl_opts():
    opts = {
        'noplaylist': True,
        'quiet': True,
        'no_warnings': True,
        'socket_timeout': 30,
        'retries': 5,
        'ignoreerrors': True,
        'no_check_certificates': True,
        'geo_bypass': True,
        'source_address': '0.0.0.0',
        'force_ipv4': True,
        'extractor_args': {'youtube': {'player_client': ['web', 'android']}},
        'http_headers': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    }
    if FFMPEG_LOCATION:
        opts['ffmpeg_location'] = FFMPEG_LOCATION
    cookies = os.path.join(SCRIPT_DIR, "cookies.txt")
    if os.path.exists(cookies):
        opts['cookiefile'] = cookies
    return opts

def safe_duration(val):
    try:
        return int(float(val)) if val else 0
    except Exception:
        return 0

def format_duration(seconds):
    seconds = safe_duration(seconds)
    return f"{seconds // 60}:{seconds % 60:02d}" if seconds > 0 else "???"

def search_tracks(query):
    results = []
    seen = set()
    
    for search_type, search_query, limit, source in [
        ("scsearch", query, 5, "🎵 SC"),
        ("ytsearch", query, 5, "📺 YT")
    ]:
        try:
            opts = get_ydl_opts()
            opts['skip_download'] = True
            if 'ytsearch' in search_type:
                opts['extract_flat'] = 'in_playlist'
            
            with yt_dlp.YoutubeDL(opts) as ydl:
                data = ydl.extract_info(f"{search_type}{limit}:{search_query}", download=False)
                if data and data.get('entries'):
                    for entry in data['entries']:
                        if not entry:
                            continue
                        
                        url = entry.get('webpage_url') or entry.get('url', '')
                        vid = entry.get('id', '')
                        
                        if not url.startswith('http'):
                            if vid and 'youtube' in search_type:
                                url = f"https://www.youtube.com/watch?v={vid}"
                            else:
                                continue
                        
                        duration = safe_duration(entry.get('duration'))
                        if 0 < MAX_DURATION < duration:
                            continue
                        
                        if url not in seen:
                            results.append({
                                'url': url,
                                'title': entry.get('title', '?'),
                                'artist': entry.get('artist') or entry.get('uploader', ''),
                                'duration': duration,
                                'source': source
                            })
                            seen.add(url)
        except Exception as e:
            log.warning(f"Ошибка поиска {source}: {e}")
    
    # Дедупликация по названию
    unique = []
    seen_titles = set()
    for r in results:
        title_key = re.sub(r'[^\w\s]', '', r['title'].lower()).strip()
        if title_key and title_key not in seen_titles:
            unique.append(r)
            seen_titles.add(title_key)
    
    return unique[:8]

def find_file(directory, extensions, min_size=500):
    for ext in extensions:
        for f in os.listdir(directory):
            if f.lower().endswith(ext):
                fp = os.path.join(directory, f)
                if os.path.isfile(fp) and os.path.getsize(fp) > min_size:
                    return fp
    return None

def convert_to_mp3(input_path, output_dir):
    if input_path.lower().endswith('.mp3') or not FFMPEG_AVAILABLE:
        return input_path
    
    output = os.path.join(output_dir, "audio.mp3")
    try:
        cmd = os.path.join(FFMPEG_LOCATION, "ffmpeg") if FFMPEG_LOCATION else "ffmpeg"
        # Исправленная команда (без пробелов в аргументах)
        result = subprocess.run(
            [cmd, '-i', input_path, '-codec:a', 'libmp3lame', '-q:a', '2', '-y', output],
            capture_output=True,
            timeout=120
        )
        if os.path.exists(output) and os.path.getsize(output) > 500:
            return output
    except Exception as e:
        log.error(f"Ошибка конвертации: {e}")
    
    return input_path

def download_track(url):
    temp_dir = tempfile.mkdtemp(dir=DOWNLOADS_DIR)
    try:
        opts = get_ydl_opts()
        opts.update({
            'format': 'bestaudio/best',
            'outtmpl': os.path.join(temp_dir, 'audio.%(ext)s')
        })
        if FFMPEG_AVAILABLE:
            opts['postprocessors'] = [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192'
            }]
        
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
            title = info.get('title', 'audio') if info else 'audio'
            artist = info.get('artist') or info.get('uploader', '') if info else ''
            duration = safe_duration(info.get('duration')) if info else 0
            thumb_url = info.get('thumbnail') if info else None
            
            audio = find_file(temp_dir, ['mp3', 'm4a', 'opus', 'ogg', 'webm'])
            if not audio:
                shutil.rmtree(temp_dir, ignore_errors=True)
                return None, "Не удалось найти аудиофайл"
            
            audio = convert_to_mp3(audio, temp_dir)
            
            if os.path.getsize(audio) > MAX_FILE_SIZE:
                shutil.rmtree(temp_dir, ignore_errors=True)
                return None, "Файл слишком большой (>50 МБ)"
            
            thumbnail = None
            if thumb_url:
                try:
                    thumb_path = os.path.join(temp_dir, "thumb.jpg")
                    resp = requests.get(thumb_url, timeout=8)
                    if resp.status_code == 200:
                        with open(thumb_path, 'wb') as f:
                            f.write(resp.content)
                        thumbnail = thumb_path
                except Exception:
                    pass
            
            return {
                'file': audio,
                'title': title,
                'artist': artist,
                'duration': duration,
                'thumbnail': thumbnail,
                'temp_dir': temp_dir,
                'url': url
            }, None
    
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        return None, f"Ошибка скачивания: {str(e)[:100]}"

def download_video(url):
    temp_dir = tempfile.mkdtemp(dir=DOWNLOADS_DIR)
    try:
        opts = get_ydl_opts()
        opts.update({
            'format': 'best[filesize<50M]/best[height<=720]/best',
            'outtmpl': os.path.join(temp_dir, 'video.%(ext)s'),
            'merge_output_format': 'mp4'
        })
        
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
            title = info.get('title', 'video') if info else 'video'
            duration = safe_duration(info.get('duration')) if info else 0
            
            video = find_file(temp_dir, ['mp4', 'mkv', 'webm'])
            if video and os.path.getsize(video) <= MAX_FILE_SIZE:
                return {
                    'file': video,
                    'title': title,
                    'duration': duration,
                    'temp_dir': temp_dir
                }, None
            
            shutil.rmtree(temp_dir, ignore_errors=True)
            return None, "Видео слишком большое или не найдено"
    
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        return None, f"Ошибка скачивания: {str(e)[:100]}"

def download_with_timeout(func, url, timeout=None):
    timeout = timeout or DOWNLOAD_TIMEOUT
    result = [None]
    error = [None]
    done = [False]
    
    def worker():
        try:
            result[0], error[0] = func(url)
        except Exception as e:
            error[0] = str(e)
        finally:
            done[0] = True
    
    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    thread.join(timeout=timeout)
    
    if done[0]:
        return result[0], error[0]
    return None, "Таймаут скачивания"

def get_platform_name(url):
    platforms = {
        'tiktok.com': 'TikTok',
        'instagram.com': 'Instagram',
        'youtube.com': 'YouTube',
        'youtu.be': 'YouTube',
        'soundcloud.com': 'SoundCloud',
        'vk.com': 'VK',
        'twitter.com': 'Twitter',
        'x.com': 'X'
    }
    for domain, name in platforms.items():
        if domain in url:
            return name
    return 'ссылка'

# ====================== КОММЕНТАРИИ ======================
def music_comment(chat_id, title, is_group=False):
    try:
        prompt = f"Напиши короткую эмоциональную реакцию (1 предложение) на песню '{title}'. Будь милой, используй эмодзи."
        response = ask_ai([{"role": "system", "content": prompt}], max_tokens=60)
        if response and not is_error(response):
            result = clean_text(response)
            if result and len(result) < 100:
                return result
    except Exception:
        pass
    
    comments = [
        "🎵 Классный трек!",
        "🎧 Отличный выбор!",
        "🎶 Люблю эту песню!",
        "🎼 Забирай!",
        "🎤 Наслаждайся!"
    ]
    return random.choice(comments)

def gift_reaction(gift_name, user_name, relation):
    try:
        prompt = f"Ты получаешь подарок '{gift_name}' от {user_name}. Ваши отношения: {relation}/100. Напиши эмоциональную реакцию (1-2 предложения)."
        response = ask_ai([{"role": "system", "content": prompt}], max_tokens=100)
        if response and not is_error(response):
            result = clean_text(response)
            if result and len(result) < 150:
                return result
    except Exception:
        pass
    
    return f"💝 Спасибо за {gift_name}, {user_name}!"

# ====================== ОБУЧЕНИЕ ======================
def learn_from_chat(chat_id):
    """Фоновое обучение на основе сообщений"""
    try:
        with session_lock:
            session = chat_sessions.get(chat_id)
            if not session:
                return
            
            user_msgs = [m for m in session.get("messages", []) if m["role"] == "user"]
            if len(user_msgs) < 5:
                return
            
            text = "\n".join([m["content"] for m in user_msgs[-20:]])
            is_group = session.get("is_group", False)
            
            prompt = (
                "Проанализируй эти сообщения и извлеки информацию о пользователях и темах.\n"
                "Формат JSON: {\"users\": {\"имя\": {\"traits\": [...], \"interests\": [...]}}, "
                "\"facts\": [...], \"topics\": [...]}"
            )
            
            response = ask_ai([
                {"role": "system", "content": prompt},
                {"role": "user", "content": text}
            ], max_tokens=500)
            
            if not response or is_error(response):
                return
            
            # TODO: Парсинг JSON и сохранение
    except Exception as e:
        log.error(f"Ошибка обучения: {e}")

# ====================== НАПОМИНАНИЯ ======================
def set_reminder(chat_id, user_id, minutes, text, reply_to=None):
    def remind():
        profile = load_profile(user_id)
        name = profile.get("display_name") or str(user_id)
        safe_send(chat_id, f"⏰ Напоминание для {name}!\n{text}", reply_to=reply_to)
    
    timer = threading.Timer(minutes * 60, remind)
    timer.daemon = True
    timer.start()
    reminders[f"{chat_id}_{user_id}_{int(time.time())}"] = timer

# ====================== ПРОАКТИВНЫЕ СООБЩЕНИЯ ======================
def start_proactive_timer(chat_id):
    settings = get_group_settings(chat_id)
    if not settings.get("proactive_enabled"):
        return
    
    stop_proactive_timer(chat_id)
    min_time = max(1, settings.get("proactive_min", 30))
    max_time = max(min_time + 1, settings.get("proactive_max", 120))
    delay = random.randint(min_time, max_time) * 60
    
    timer = threading.Timer(delay, send_proactive_message, args=(chat_id,))
    timer.daemon = True
    timer.start()
    proactive_timers[chat_id] = timer

def stop_proactive_timer(chat_id):
    timer = proactive_timers.pop(chat_id, None)
    if timer:
        try:
            timer.cancel()
        except Exception:
            pass

def send_proactive_message(chat_id):
    try:
        settings = get_group_settings(chat_id)
        if not settings.get("proactive_enabled"):
            return
        
        is_busy_flag, _ = is_busy(chat_id)
        if is_busy_flag:
            start_proactive_timer(chat_id)
            return
        
        now = datetime.now()
        start_hour, end_hour = settings.get("hours_start", 9), settings.get("hours_end", 23)
        if end_hour > start_hour and not (start_hour <= now.hour < end_hour):
            start_proactive_timer(chat_id)
            return
        
        last = last_activity.get(chat_id)
        if last and (now - last).total_seconds() > 10800:  # 3 часа
            start_proactive_timer(chat_id)
            return
        
        with session_lock:
            if chat_id not in chat_sessions:
                start_proactive_timer(chat_id)
                return
            
            session = chat_sessions[chat_id]
            user_msgs = [m for m in session["messages"] if m["role"] == "user"]
            if len(user_msgs) < 3:
                start_proactive_timer(chat_id)
                return
            
            msgs = copy.deepcopy(session["messages"])
            msgs.append({"role": "user", "content": "[Бот хочет начать разговор]"})
            
            response = ask_ai(msgs, max_tokens=100)
            if response and not is_error(response):
                response = clean_text(response)
                if response and 2 < len(response) < 200:
                    sent = safe_send(chat_id, response)
                    if sent:
                        add_message(chat_id, "assistant", response, True)
    
    except Exception as e:
        log.error(f"Ошибка проактивного сообщения: {e}")
    finally:
        start_proactive_timer(chat_id)

# ====================== СЕССИИ ======================
def get_session(chat_id, is_group=False, uid=None):
    if chat_id not in chat_sessions:
        chat_sessions[chat_id] = {
            "messages": [{"role": "system", "content": build_prompt(chat_id, is_group, uid)}],
            "created": datetime.now().strftime("%d.%m.%Y %H:%M"),
            "users": {},
            "msg_count": 0,
            "is_group": is_group,
            "last_active": datetime.now()
        }
    return chat_sessions[chat_id]

def add_message(chat_id, role, content, is_group=False):
    if not content or not isinstance(content, str):
        return
    
    with session_lock:
        session = get_session(chat_id, is_group)
        session["messages"].append({"role": role, "content": content})
        session["last_active"] = datetime.now()
        
        if len(session["messages"]) > SESSION_MAX_MESSAGES + 1:
            session["messages"] = [session["messages"][0]] + session["messages"][-SESSION_MAX_MESSAGES:]
        
        session["msg_count"] = session.get("msg_count", 0) + 1
        last_activity[chat_id] = datetime.now()
        
        if session["msg_count"] % LEARN_INTERVAL == 0:
            add_task(learn_from_chat, chat_id)

def clear_history(chat_id, is_group=False, uid=None):
    with session_lock:
        old_users = chat_sessions.get(chat_id, {}).get("users", {}).copy()
        chat_sessions[chat_id] = {
            "messages": [{"role": "system", "content": build_prompt(chat_id, is_group, uid)}],
            "created": datetime.now().strftime("%d.%m.%Y %H:%M"),
            "users": old_users,
            "msg_count": 0,
            "is_group": is_group,
            "last_active": datetime.now()
        }

def clear_all_memory(chat_id, is_group=False):
    save_memory(chat_id, empty_memory())
    clear_history(chat_id, is_group)

def cleanup_old_sessions():
    """Удаляет старые неактивные сессии из памяти"""
    with session_lock:
        now = datetime.now()
        to_delete = []
        for chat_id, session in chat_sessions.items():
            last = session.get("last_active")
            if last and (now - last).total_seconds() > SESSION_CLEANUP_AGE:
                to_delete.append(chat_id)
        
        for chat_id in to_delete:
            del chat_sessions[chat_id]
            log.info(f"Очищена сессия чата {chat_id} (неактивен >1ч)")
        
        return len(to_delete)

# ====================== ОТПРАВКА ======================
def send_audio_file(chat_id, audio_data, caption, reply_to=None):
    thumb = None
    try:
        if audio_data.get('thumbnail') and os.path.exists(audio_data['thumbnail']):
            thumb = open(audio_data['thumbnail'], 'rb')
        
        with open(audio_data['file'], 'rb') as audio:
            bot.send_audio(
                chat_id,
                audio,
                title=audio_data.get('title', ''),
                performer=audio_data.get('artist', ''),
                duration=safe_duration(audio_data.get('duration', 0)),
                thumbnail=thumb,
                caption=caption,
                reply_to_message_id=reply_to
            )
    except Exception as e:
        log.error(f"Ошибка отправки аудио: {e}")
        try:
            with open(audio_data['file'], 'rb') as audio:
                bot.send_audio(
                    chat_id,
                    audio,
                    title=audio_data.get('title', ''),
                    caption=caption,
                    reply_to_message_id=reply_to
                )
        except Exception as e2:
            log.error(f"Ошибка fallback отправки: {e2}")
    finally:
        if thumb:
            try:
                thumb.close()
            except Exception:
                pass

def send_long_message(chat_id, text, markup=None, reply_to=None):
    if not text or not text.strip():
        text = "..."
    
    chunks = []
    while len(text) > 4096:
        split = text.rfind('\n', 0, 4096)
        if split < 2000:
            split = 4096
        chunks.append(text[:split])
        text = text[split:].lstrip()
    
    if text:
        chunks.append(text)
    
    for i, chunk in enumerate(chunks):
        safe_send(
            chat_id,
            chunk,
            markup=markup if i == len(chunks) - 1 else None,
            reply_to=reply_to if i == 0 else None
        )

# ====================== PENDING ======================
def pending_key(chat_id, message_id):
    return f"p_{chat_id}_{message_id}"

def find_pending(chat_id):
    with pending_lock:
        return [(k, v) for k, v in pending_tracks.items()
                if k.startswith(f"p_{chat_id}_") and v.get("time") and
                (datetime.now() - v["time"]).total_seconds() < PENDING_TIMEOUT]

def cleanup_pending():
    with pending_lock:
        for key in [k for k, v in pending_tracks.items()
                   if v.get("time") and
                   (datetime.now() - v["time"]).total_seconds() > PENDING_TIMEOUT]:
            del pending_tracks[key]

# ====================== ПРОФИЛЬ ======================
def format_profile(uid, user=None):
    profile = load_profile(uid)
    eco = db_execute("SELECT balance, streak FROM economy WHERE uid=?", (uid,), fetchone=True)
    balance = eco[0] if eco else INITIAL_BALANCE
    streak = eco[1] if eco else 0
    
    rel, rel_title, rel_emoji = get_relation_info(uid)
    is_dev = uid in DEVELOPER_IDS
    
    name = profile.get("display_name") or (get_display_name(user) if user else "?")
    emoji = profile.get("name_emoji", "") or ""
    badges = " ".join(profile.get("badges") or [])
    
    if is_dev:
        badges = "👑 " + badges
    
    xp = profile.get("xp") or 0
    level = profile.get("level") or 1
    title = profile.get("custom_title") or profile.get("title") or "Новичок"
    
    # Прогресс до следующего уровня
    next_level = None
    for l in LEVELS:
        if l["level"] > level:
            next_level = l
            break
    
    if next_level:
        prev_xp = 0
        for l in LEVELS:
            if l["level"] == level:
                prev_xp = l["xp"]
                break
        
        progress = (xp - prev_xp) / max(1, next_level["xp"] - prev_xp)
        filled = int(progress * 15)
        xp_bar = f"{'🟩' * filled}{'⬜' * (15 - filled)} {xp}/{next_level['xp']}"
    else:
        xp_bar = "🟩" * 15 + " MAX"
    
    achievements = profile.get("achievements") or []
    
    text = f"{'👑 РАЗРАБОТЧИК' if is_dev else '👤 ПРОФИЛЬ'}\n"
    text += "━" * 25 + "\n"
    text += f"{'👑' if is_dev else '👤'} {emoji}{name}"
    if profile.get("username"):
        text += f" (@{profile['username']})"
    text += "\n"
    if badges:
        text += f"   {badges}\n"
    text += f"\n📊 Ур. {level} — {title}\n"
    text += f"   {xp_bar}\n"
    text += f"\n💰 {fmt_coins(balance)} | 🔥 Стрик: {streak}\n"
    text += f"\n{rel_emoji} Отношения: {rel}/100\n"
    text += f"{relation_bar(rel)}\n"
    text += f"   {rel_title}\n"
    text += f"\n💬 {profile.get('messages', 0)} | 🎤 {profile.get('voice', 0)} | 🎵 {profile.get('music', 0)} | "
    text += f"🎮 {profile.get('games', 0)} | 🏆 {profile.get('wins', 0)} | 🎁 {profile.get('gifts_given', 0)}\n"
    text += f"\n🏅 Достижения: {len(achievements)}/{len(ACHIEVEMENTS)}"
    if achievements:
        text += " " + " ".join(ACHIEVEMENTS[a]["name"].split()[0] for a in achievements[-6:] if a in ACHIEVEMENTS)
    text += f"\n📅 Присоединился: {profile.get('joined', '?')}"
    
    return text

# ====================== ГЕНЕРАЦИЯ САММАРИ ======================
def generate_summary(chat_id):
    with session_lock:
        session = chat_sessions.get(chat_id)
        if not session:
            return "Нет данных для саммари"
        
        user_msgs = [m for m in session.get("messages", []) if m["role"] == "user"]
        if len(user_msgs) < 5:
            return "Мало сообщений для саммари (нужно хотя бы 5)"
        
        text = "\n".join([m["content"] for m in user_msgs[-30:]])
        
        prompt = (
            "Сделай краткое саммари (3-5 предложений) этого разговора. "
            "Выдели основные темы и настроение."
        )
        
        response = ask_ai([
            {"role": "system", "content": prompt},
            {"role": "user", "content": text}
        ])
        
        return clean_text(response) if response and not is_error(response) else "Не удалось создать саммари"

# ====================== КЛАВИАТУРЫ ======================
def main_keyboard():
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("👤 Профиль", callback_data="profile"),
        types.InlineKeyboardButton("💰 Баланс", callback_data="balance"),
        types.InlineKeyboardButton("🎮 Игры", callback_data="games_menu"),
        types.InlineKeyboardButton("📜 Помощь", callback_data="help_commands"),
        types.InlineKeyboardButton("🛒 Магазин", callback_data="shop_main"),
        types.InlineKeyboardButton("📚 Документация", callback_data="docs_main"),
        types.InlineKeyboardButton("🧹 Очистить", callback_data="clear"),
        types.InlineKeyboardButton("📊 Мои группы", callback_data="my_groups")
    )
    return kb

def start_keyboard():
    bot_info = get_bot_info()
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton("➕ Добавить в группу", url=f"https://t.me/{bot_info.username if bot_info else 'bot'}?startgroup=true"),
        types.InlineKeyboardButton("👤 Профиль", callback_data="profile"),
        types.InlineKeyboardButton("🎮 Игры", callback_data="games_menu"),
        types.InlineKeyboardButton("📚 Документация", callback_data="docs_main"),
        types.InlineKeyboardButton("🛒 Магазин", callback_data="shop_main")
    )
    return kb

def help_keyboard():
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton("📋 Команды", callback_data="help_commands"),
        types.InlineKeyboardButton("🎤 Голос/Видео", callback_data="help_voice"),
        types.InlineKeyboardButton("📚 Документация", callback_data="docs_main"),
        types.InlineKeyboardButton("◀️ Назад", callback_data="back_main")
    )
    return kb

def docs_keyboard():
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton("💬 Общение", callback_data="docs_chat"),
        types.InlineKeyboardButton("🎵 Музыка/Видео", callback_data="docs_media"),
        types.InlineKeyboardButton("🎮 Игры", callback_data="docs_games"),
        types.InlineKeyboardButton("💰 Экономика", callback_data="docs_economy"),
        types.InlineKeyboardButton("👤 Профиль", callback_data="docs_profile"),
        types.InlineKeyboardButton("🛒 Магазин", callback_data="docs_shop"),
        types.InlineKeyboardButton("⚙️ Настройки", callback_data="docs_settings"),
        types.InlineKeyboardButton("🛡️ Модерация", callback_data="docs_mod"),
        types.InlineKeyboardButton("◀️ Назад", callback_data="back_main")
    )
    return kb

def shop_main_keyboard():
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("🎁 Услуги", callback_data="shop_cat_service"),
        types.InlineKeyboardButton("💝 Подарки", callback_data="shop_cat_gift"),
        types.InlineKeyboardButton("✨ Для себя", callback_data="shop_cat_self"),
        types.InlineKeyboardButton("📅 Ежедневно", callback_data="daily")
    )
    kb.row(types.InlineKeyboardButton("◀️ Назад", callback_data="back_main"))
    return kb

def shop_category_keyboard(category):
    kb = types.InlineKeyboardMarkup(row_width=1)
    for item_id, item in SHOP_ITEMS.items():
        if item.get("cat") == category:
            kb.add(types.InlineKeyboardButton(
                f"{item['name']} - {item['price']} 🪙",
                callback_data=f"buy_{item_id}"
            ))
    kb.add(types.InlineKeyboardButton("◀️ Назад", callback_data="shop_main"))
    return kb

def games_keyboard():
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton("🎭 Правда или действие", callback_data="game_tod"),
        types.InlineKeyboardButton("❓ Викторина", callback_data="game_quiz"),
        types.InlineKeyboardButton("🔢 Угадай число", callback_data="game_number"),
        types.InlineKeyboardButton("📝 Виселица", callback_data="game_word"),
        types.InlineKeyboardButton("◀️ Назад", callback_data="back_main")
    )
    return kb

def track_keyboard(count, msg_id):
    kb = types.InlineKeyboardMarkup(row_width=4)
    buttons = [types.InlineKeyboardButton(str(i + 1), callback_data=f"tr_{msg_id}_{i}") for i in range(count)]
    kb.add(*buttons)
    kb.row(
        types.InlineKeyboardButton("💾 Сохранить всё", callback_data=f"trsv_{msg_id}"),
        types.InlineKeyboardButton("❌ Отмена", callback_data=f"tr_{msg_id}_x")
    )
    return kb

def playlist_save_keyboard(chat_id, uid, track_key):
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton("💾 В личный", callback_data=f"plsv_my_{track_key}"),
        types.InlineKeyboardButton("👥 В групповой", callback_data=f"plsv_grp_{track_key}"),
        types.InlineKeyboardButton("📋 В оба", callback_data=f"plsv_both_{track_key}"),
        types.InlineKeyboardButton("❌ Пропустить", callback_data=f"plsv_skip_{track_key}")
    )
    return kb

def playlist_keyboard(uid, is_group_pl=False):
    pl = load_group_playlist(uid) if is_group_pl else load_playlist(uid)
    kb = types.InlineKeyboardMarkup(row_width=2)
    
    if pl["tracks"]:
        for i, track in enumerate(pl["tracks"][-10:]):
            real_idx = len(pl["tracks"]) - 10 + i if len(pl["tracks"]) > 10 else i
            prefix = "gpl" if is_group_pl else "pl"
            kb.add(types.InlineKeyboardButton(
                f"{track['title'][:35]}",
                callback_data=f"{prefix}_play_{real_idx}"
            ))
    
    if not is_group_pl:
        kb.row(types.InlineKeyboardButton("🗑️ Очистить", callback_data="pl_clear"))
    
    if not is_group_pl:
        kb.row(types.InlineKeyboardButton("👥 Групповой", callback_data="group_pl"))
    
    kb.row(types.InlineKeyboardButton("◀️ Назад", callback_data="back_main"))
    return kb

def model_categories_keyboard():
    kb = types.InlineKeyboardMarkup(row_width=2)
    categories_free = {}
    categories_paid = {}
    
    for mid, info in AVAILABLE_MODELS.items():
        cat = info.get("cat", "other")
        if info.get("free"):
            categories_free[cat] = categories_free.get(cat, 0) + 1
        else:
            categories_paid[cat] = categories_paid.get(cat, 0) + 1
    
    if categories_free:
        kb.row(types.InlineKeyboardButton("🎁 БЕСПЛАТНЫЕ", callback_data="noop"))
        for cat, count in categories_free.items():
            cat_name = MODEL_CATEGORIES.get(cat, cat)
            kb.add(types.InlineKeyboardButton(
                f"{cat_name} ({count})",
                callback_data=f"mcat_{cat}_free"
            ))
    
    if categories_paid:
        kb.row(types.InlineKeyboardButton("💰 ПЛАТНЫЕ", callback_data="noop"))
        for cat, count in categories_paid.items():
            cat_name = MODEL_CATEGORIES.get(cat, cat)
            kb.add(types.InlineKeyboardButton(
                f"{cat_name} ({count})",
                callback_data=f"mcat_{cat}_paid"
            ))
    
    kb.row(types.InlineKeyboardButton("◀️ Назад", callback_data="dev_back"))
    kb.row(types.InlineKeyboardButton("🔍 Поиск", callback_data="mcat_search"))
    return kb

def model_list_keyboard(category, free_filter=None):
    kb = types.InlineKeyboardMarkup(row_width=1)
    with model_lock:
        current = CURRENT_MODEL
        for mid, info in AVAILABLE_MODELS.items():
            if info.get("cat") != category:
                continue
            if free_filter == "free" and not info.get("free"):
                continue
            if free_filter == "paid" and info.get("free"):
                continue
            
            current_mark = "✅ " if info['id'] == current else ""
            free_mark = "🆓 " if info.get("free") else "💰 "
            kb.add(types.InlineKeyboardButton(
                f"{current_mark}{free_mark}{info['name']}",
                callback_data=f"mset_{mid}"
            ))
    
    kb.row(types.InlineKeyboardButton("◀️ Назад", callback_data="mcat_back"))
    return kb

def group_settings_keyboard(chat_id):
    settings = get_group_settings(chat_id)
    kb = types.InlineKeyboardMarkup(row_width=3)
    
    kb.row(
        types.InlineKeyboardButton("−10", callback_data="cd10"),
        types.InlineKeyboardButton(f"⚡ {settings['response_chance']}%", callback_data="noop"),
        types.InlineKeyboardButton("+10", callback_data="cu10")
    )
    
    kb.row(
        types.InlineKeyboardButton("−5", callback_data="cd5"),
        types.InlineKeyboardButton("+5", callback_data="cu5"),
        types.InlineKeyboardButton("⚙️", callback_data="noop")
    )
    
    proactive_status = "✅" if settings.get('proactive_enabled') else "❌"
    kb.row(types.InlineKeyboardButton(
        f"{proactive_status} Проактив",
        callback_data=f"pt"
    ))
    
    antispam_status = "✅" if settings.get('antispam') else "❌"
    kb.row(types.InlineKeyboardButton(
        f"{antispam_status} Антиспам",
        callback_data=f"as"
    ))
    
    moderation_status = "✅" if settings.get('moderation') else "❌"
    kb.row(types.InlineKeyboardButton(
        f"{moderation_status} Модерация",
        callback_data=f"md"
    ))
    
    auto_admin_status = "✅" if settings.get('auto_admin') else "❌"
    kb.row(types.InlineKeyboardButton(
        f"{auto_admin_status} Авто-админ",
        callback_data=f"aa"
    ))
    
    kb.row(
        types.InlineKeyboardButton("📝 Промпт", callback_data="pc"),
        types.InlineKeyboardButton("📋 Правила", callback_data="mr")
    )
    
    kb.row(
        types.InlineKeyboardButton("🔄 Очистить историю", callback_data="cc"),
        types.InlineKeyboardButton("🧹 Сброс", callback_data="cm")
    )
    
    kb.row(types.InlineKeyboardButton("🔐 Секретная ссылка", callback_data="secret"))
    kb.row(types.InlineKeyboardButton("❌ Закрыть", callback_data="close"))
    
    return kb

def groups_list_keyboard(uid):
    kb = types.InlineKeyboardMarkup(row_width=1)
    groups = get_user_groups(uid)
    
    for gid, info in groups.items():
        kb.add(types.InlineKeyboardButton(
            f"👥 {info.get('title', 'Группа')}",
            callback_data=f"pg_sel_{gid}"
        ))
    
    kb.add(types.InlineKeyboardButton("◀️ Назад", callback_data="back_main"))
    return kb

def download_format_keyboard():
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("🎵 MP3", callback_data="dl_mp3"),
        types.InlineKeyboardButton("🎬 MP4", callback_data="dl_mp4")
    )
    return kb

# ====================== ПРИМЕНЕНИЕ НАСТРОЕК ======================
def apply_setting(settings, action, chat_id=None):
    if action == "cd10":
        settings["response_chance"] = max(0, settings["response_chance"] - 10)
    elif action == "cu10":
        settings["response_chance"] = min(100, settings["response_chance"] + 10)
    elif action == "cd5":
        settings["response_chance"] = max(0, settings["response_chance"] - 5)
    elif action == "cu5":
        settings["response_chance"] = min(100, settings["response_chance"] + 5)
    elif action == "pt":
        settings["proactive_enabled"] = not settings.get("proactive_enabled", False)
        if chat_id:
            if settings["proactive_enabled"]:
                start_proactive_timer(chat_id)
            else:
                stop_proactive_timer(chat_id)
    elif action == "as":
        settings["antispam"] = not settings.get("antispam", True)
    elif action == "md":
        settings["moderation"] = not settings.get("moderation", False)
    elif action == "aa":
        settings["auto_admin"] = not settings.get("auto_admin", True)
    elif action == "pr":
        settings["custom_prompt"] = None
    elif action == "cc":
        if chat_id:
            clear_history(chat_id, True)
    elif action == "cm":
        if chat_id:
            clear_all_memory(chat_id, True)
    else:
        return None
    
    save_settings()
    return f"{settings['response_chance']}%"

# ====================== ДОКУМЕНТАЦИЯ ======================
DOCS = {
    "chat": (
        "📝 **ОБЩЕНИЕ**\n\n"
        "Просто пишите боту в личку или упоминайте его в группе.\n"
        "Можно отвечать на его сообщения — он поймёт.\n\n"
        "**Советы:**\n"
        "• Задавайте вопросы\n"
        "• Просите совета\n"
        "• Общайтесь как с другом"
    ),
    "media": (
        "🎵 **МУЗЫКА И ВИДЕО**\n\n"
        "Напишите:\n"
        "• `найди песню [название]`\n"
        "• `скачай видео [ссылка]`\n"
        "• `включи музыку`\n\n"
        "Поддерживаемые платформы:\n"
        "YouTube, TikTok, Instagram, SoundCloud, VK и другие"
    ),
    "games": (
        "🎮 **ИГРЫ**\n\n"
        "Команды:\n"
        "/game tod — Правда или действие\n"
        "/game quiz — Викторина\n"
        "/game number — Угадай число\n"
        "/game word — Виселица\n\n"
        "За победы дают монеты и XP!"
    ),
    "economy": (
        "💰 **ЭКОНОМИКА**\n\n"
        "Команды:\n"
        "/daily — Ежедневный бонус\n"
        "/balance — Баланс\n"
        "/send @user сумма — Перевести монеты\n"
        "/top — Топ пользователей\n\n"
        "Монеты можно получить за:\n"
        "• Сообщения (+2)\n"
        "• Голосовые (+5)\n"
        "• Стикеры (+1)\n"
        "• Победы в играх"
    ),
    "profile": (
        "👤 **ПРОФИЛЬ**\n\n"
        "Команды:\n"
        "/me — Мой профиль\n"
        "/profile @user — Профиль другого\n"
        "/shop — Магазин\n\n"
        "В профиле видно:\n"
        "• Уровень и XP\n"
        "• Баланс и стрик\n"
        "• Отношения с ботом\n"
        "• Достижения"
    ),
    "shop": (
        "🛒 **МАГАЗИН**\n\n"
        "/shop — Открыть магазин\n\n"
        "Категории:\n"
        "• Услуги — бот сделает что-то\n"
        "• Подарки — поднимите отношения\n"
        "• Для себя — бонусы и украшения"
    ),
    "settings": (
        "⚙️ **НАСТРОЙКИ ГРУППЫ**\n\n"
        "/settings — Меню настроек\n\n"
        "Можно:\n"
        "• Шанс ответа бота\n"
        "• Проактивные сообщения\n"
        "• Антиспам\n"
        "• Модерацию\n"
        "• Свой промпт"
    ),
    "mod": (
        "🛡️ **МОДЕРАЦИЯ**\n\n"
        "Команды для админов:\n"
        "/ban @user причина — Бан\n"
        "/mute @user время причина — Мут\n"
        "/warn @user причина — Предупреждение\n"
        "/unban @user — Разбан\n"
        "/unmute @user — Размут\n"
        "/unwarn @user — Снять варн\n\n"
        "При 3 варнах — авто-мут на час"
    ),
}

HELP_COMMANDS = (
    "📋 **КОМАНДЫ**\n\n"
    "/start — Запустить бота\n"
    "/help — Это меню\n"
    "/me — Профиль\n"
    "/balance — Баланс\n"
    "/daily — Ежедневный бонус\n"
    "/send @user сумма — Перевести монеты\n"
    "/shop — Магазин\n"
    "/game — Игры\n"
    "/top — Топ пользователей\n"
    "/settings — Настройки группы\n"
    "/clear — Очистить историю\n\n"
    "**Модерация:**\n"
    "/ban /mute /warn /unban /unmute /unwarn"
)

HELP_VOICE = (
    "🎤 **ГОЛОС И ВИДЕО**\n\n"
    "Бот понимает:\n"
    "• Голосовые сообщения\n"
    "• Кружочки (видео-сообщения)\n"
    "• Стикеры\n"
    "• GIF-анимации\n"
    "• Фото\n\n"
    "Просто отправьте файл — бот ответит!"
)

# ====================== ОБРАБОТЧИКИ СОБЫТИЙ ======================
@bot.message_handler(content_types=['new_chat_members'])
def on_join(message):
    try:
        bot_info = get_bot_info()
        if not bot_info:
            return
        
        for member in message.new_chat_members:
            if member.id == bot_info.id:
                chat_id = message.chat.id
                settings = get_group_settings(chat_id)
                
                with settings_lock:
                    settings["owner_id"] = message.from_user.id
                    settings["owner_name"] = get_display_name(message.from_user)
                    settings["group_name"] = message.chat.title
                    save_settings()
                
                register_group(message.from_user.id, chat_id, message.chat.title)
                
                with session_lock:
                    get_session(chat_id, True)
                
                # Обновляем кэш участников
                add_task(update_group_members_cache, chat_id)
                
                safe_send(chat_id,
                         "🌸 Всем привет! Я Хината, ваш новый друг!\n"
                         "Я умею общаться, играть в игры, искать музыку и помогать с модерацией.\n"
                         "/help — список команд\n"
                         "По вопросам: @PaceHoz")
                
                if settings.get("proactive_enabled"):
                    start_proactive_timer(chat_id)
            else:
                # Приветствуем нового участника
                chat_id = message.chat.id
                name = get_display_name(member)
                safe_send(chat_id, f"👋 Привет, {name}! Добро пожаловать в чат!")
                # Обновляем кэш
                add_task(update_group_members_cache, chat_id)
    
    except Exception as e:
        log.error(f"Ошибка приветствия: {e}")

@bot.message_handler(content_types=['left_chat_member'])
def on_leave(message):
    try:
        bot_info = get_bot_info()
        if bot_info and message.left_chat_member and message.left_chat_member.id == bot_info.id:
            stop_proactive_timer(message.chat.id)
        else:
            # Прощаемся с уходящим
            if message.left_chat_member:
                name = get_display_name(message.left_chat_member)
                safe_send(message.chat.id, f"👋 Пока, {name}! Будем скучать!")
                # Обновляем кэш
                add_task(update_group_members_cache, message.chat.id)
    except Exception:
        pass

@bot.message_handler(commands=['start'])
def cmd_start(message):
    if message.chat.type == "private":
        uid = message.from_user.id
        is_developer(message.from_user)
        update_user_info(uid, message.from_user)
        
        with session_lock:
            get_session(uid)
        
        safe_reply(message,
                  "🌸 Привет! Я Хината — твой виртуальный друг!\n\n"
                  "Я умею:\n"
                  "• Общаться как человек (AI)\n"
                  "• Искать музыку и видео\n"
                  "• Играть в игры\n"
                  "• Следить за экономикой (монеты, уровни)\n"
                  "• Помогать в группах\n\n"
                  "Напиши что-нибудь или выбери в меню 👇",
                  markup=start_keyboard())
    else:
        safe_reply(message, "Привет! Я уже в группе. Используй /help для списка команд")

@bot.message_handler(commands=['help'])
def cmd_help(message):
    safe_reply(message, "🌸 Чем могу помочь?", markup=help_keyboard())

@bot.message_handler(commands=['clear'])
def cmd_clear(message):
    if message.chat.type == "private":
        clear_history(message.from_user.id)
        safe_reply(message, "🧹 История очищена!", markup=main_keyboard())
    elif is_admin(message.chat.id, message.from_user.id):
        clear_history(message.chat.id, True)
        safe_reply(message, "🧹 История группы очищена!")

@bot.message_handler(commands=['settings'])
def cmd_settings(message):
    if message.chat.type == "private":
        groups = get_user_groups(message.from_user.id)
        if not groups:
            safe_reply(message, "У вас пока нет групп. Добавьте меня в группу!", markup=start_keyboard())
        else:
            safe_reply(message, "Выберите группу для настройки:", markup=groups_list_keyboard(message.from_user.id))
        return
    
    chat_id = message.chat.id
    settings = get_group_settings(chat_id)
    
    if settings['owner_id'] is None:
        with settings_lock:
            settings['owner_id'] = message.from_user.id
            settings['owner_name'] = get_display_name(message.from_user)
            save_settings()
    
    if not is_admin(chat_id, message.from_user.id):
        return
    
    safe_reply(message,
              f"⚙️ **Настройки группы**\n"
              f"Шанс ответа: {settings['response_chance']}%\n"
              f"Проактив: {'✅' if settings.get('proactive_enabled') else '❌'}\n"
              f"Антиспам: {'✅' if settings.get('antispam') else '❌'}\n"
              f"Модерация: {'✅' if settings.get('moderation') else '❌'}",
              markup=group_settings_keyboard(chat_id))

@bot.message_handler(commands=['me', 'profile'])
def cmd_me(message):
    uid = message.from_user.id
    update_user_info(uid, message.from_user)
    safe_reply(message, format_profile(uid, message.from_user))

@bot.message_handler(commands=['balance', 'bal'])
def cmd_balance(message):
    safe_reply(message, f"💰 Твой баланс: {fmt_coins(get_balance(message.from_user.id))}")

@bot.message_handler(commands=['daily'])
def cmd_daily(message):
    uid = message.from_user.id
    result = claim_daily(uid)
    
    if result[0] is None:
        safe_reply(message, "⏰ Ты уже получал бонус сегодня! Приходи завтра.")
        return
    
    total, streak, bonus = result
    text = f"🎁 Ежедневный бонус: +{total} {CURRENCY_EMOJI}\n🔥 Стрик: {streak} дней"
    
    if bonus > 0:
        text += f" (+{bonus} бонус)"
    
    text += f"\n💰 Текущий баланс: {fmt_coins(get_balance(uid))}"
    
    safe_reply(message, text)
    add_xp(uid, 5)
    achievements = check_achievements(uid)
    notify_achievements(message.chat.id, uid, achievements, message.message_id)

@bot.message_handler(commands=['send'])
def cmd_send(message):
    """Перевод монет другому пользователю"""
    if message.chat.type == "private":
        safe_reply(message, "❌ Эту команду нужно использовать в группе!")
        return
    
    parts = message.text.split()
    if len(parts) < 3:
        safe_reply(message, "❌ Использование: /send @user сумма")
        return
    
    # Парсим сумму
    try:
        amount = int(parts[2])
        if amount <= 0:
            raise ValueError
    except ValueError:
        safe_reply(message, "❌ Сумма должна быть положительным числом")
        return
    
    # Ищем получателя
    target = parts[1].lstrip('@').lower()
    target_uid, target_name = find_user_in_group(message.chat.id, target)
    
    if not target_uid:
        safe_reply(message, f"❌ Пользователь @{target} не найден в этом чате")
        return
    
    from_uid = message.from_user.id
    
    # Проверяем, не бот ли это
    if target_uid == get_bot_info().id:
        safe_reply(message, "❌ Нельзя переводить монеты боту (используй магазин /shop)")
        return
    
    # Выполняем перевод
    success, msg = transfer_coins(from_uid, target_uid, amount, f"перевод от {from_uid}")
    
    if success:
        safe_reply(message, f"✅ {msg}\nОтправитель: {get_display_name(message.from_user)}\nПолучатель: {target_name}")
        # Добавляем XP за доброту
        add_xp(from_uid, 2)
        add_xp(target_uid, 1)
    else:
        safe_reply(message, f"❌ {msg}")

@bot.message_handler(commands=['top'])
def cmd_top(message):
    chat_id = message.chat.id
    members = get_group_members(chat_id)
    
    if not members:
        safe_reply(message, "📊 Нет данных для топа. Попросите участников написать что-нибудь!")
        return
    
    data = []
    for member in members:
        try:
            uid = member["id"]
            profile = load_profile(uid)
            data.append({
                "name": profile.get("display_name") or member["full_name"] or str(uid),
                "xp": profile.get("xp") or 0,
                "level": profile.get("level") or 1
            })
        except Exception:
            pass
    
    if not data:
        safe_reply(message, "📊 Нет данных для топа")
        return
    
    data.sort(key=lambda x: x["xp"], reverse=True)
    medals = ["🥇", "🥈", "🥉"]
    
    text = "🏆 **ТОП УЧАСТНИКОВ**\n\n"
    for i, d in enumerate(data[:10]):
        medal = medals[i] if i < 3 else f"{i + 1}."
        text += f"{medal} {d['name']} — {d['level']} ур. ({d['xp']} XP)\n"
    
    safe_reply(message, text)

@bot.message_handler(commands=['shop'])
def cmd_shop(message):
    safe_reply(message, "🛒 Добро пожаловать в магазин!\nВыбери категорию:", markup=shop_main_keyboard())

@bot.message_handler(commands=['game'])
def cmd_game(message):
    safe_reply(message, "🎮 Выбери игру:", markup=games_keyboard())

@bot.message_handler(commands=['playlist'])
def cmd_playlist(message):
    uid = message.from_user.id
    pl = load_playlist(uid)
    
    if not pl["tracks"]:
        safe_reply(message, "📭 Твой плейлист пуст. Найди музыку и сохрани её!")
        return
    
    text = f"🎵 **Твой плейлист** ({len(pl['tracks'])} треков)\n\n"
    for i, track in enumerate(pl["tracks"][-10:]):
        text += f"{i + 1}. {track['title'][:35]}\n"
    
    safe_reply(message, text, markup=playlist_keyboard(uid))

# ====================== МОДЕРАЦИЯ ======================
def create_mod_request(chat_id, action, target_uid, target_name, reason, requested_by=None):
    request_id = f"mod_{chat_id}_{target_uid}_{int(time.time())}"
    
    with mod_lock:
        pending_mod_actions[request_id] = {
            "cid": chat_id,
            "action": action,
            "target_uid": target_uid,
            "target_name": target_name,
            "reason": reason,
            "requested_by": requested_by,
            "time": datetime.now()
        }
    
    action_labels = {
        "warn": "⚠️ Предупреждение",
        "mute": "🔇 Мут",
        "ban": "🚫 Бан",
        "unban": "✅ Разбан",
        "unmute": "✅ Размут",
        "unwarn": "✅ Снять варн"
    }
    action_label = action_labels.get(action, action)
    
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton(f"✅ {action_label}", callback_data=f"modok_{request_id}"),
        types.InlineKeyboardButton("❌ Отмена", callback_data=f"moddeny_{request_id}")
    )
    
    text = (
        f"🛡️ **Запрос на модерацию**\n"
        f"Действие: {action_label}\n"
        f"Пользователь: {target_name}\n"
        f"Причина: {reason or 'не указана'}\n"
        f"Подтвердите действие:"
    )
    
    return text, kb

def execute_mod_action(chat_id, action, target_uid, target_name, reason, moderator_uid):
    bot_info = get_bot_info()
    if bot_info and target_uid == bot_info.id:
        return "❌ Нельзя модерировать бота"
    
    if target_uid in DEVELOPER_IDS:
        return "❌ Нельзя модерировать разработчика"
    
    if is_admin(chat_id, target_uid) and not is_owner(chat_id, moderator_uid) and moderator_uid not in DEVELOPER_IDS:
        return "❌ Нельзя модерировать другого админа"
    
    result_text = ""
    
    try:
        if action == "warn":
            profile = load_profile(target_uid)
            warns = (profile.get("warns") or 0) + 1
            save_profile(target_uid, {"warns": warns})
            result_text = f"⚠️ {target_name} получил предупреждение ({warns}/3)"
            
            if warns >= 3:
                try:
                    bot.restrict_chat_member(chat_id, target_uid,
                                           until_date=int(time.time()) + 3600)
                    result_text += f"\n🔇 Авто-мут на 1 час (3/3 предупреждений)"
                    save_profile(target_uid, {"warns": 0})
                except Exception as e:
                    result_text += f"\n❌ Не удалось замутить: {str(e)[:50]}"
        
        elif action == "unwarn":
            profile = load_profile(target_uid)
            warns = max(0, (profile.get("warns") or 0) - 1)
            save_profile(target_uid, {"warns": warns})
            result_text = f"✅ Снято предупреждение у {target_name}. Текущих: {warns}"
        
        elif action == "mute":
            mute_time = 3600  # 1 час по умолчанию
            
            # Парсим время из причины
            time_match = re.search(r'(\d+)\s*(минут|минуту|мин|min|час|ч|h)', reason.lower() if reason else "")
            if time_match:
                val = int(time_match.group(1))
                unit = time_match.group(2)
                if unit in ("час", "ч", "h"):
                    mute_time = val * 3600
                else:
                    mute_time = val * 60
            
            mute_time = max(60, min(86400 * 7, mute_time))  # От 1 минуты до 7 дней
            
            bot.restrict_chat_member(chat_id, target_uid,
                                    until_date=int(time.time()) + mute_time)
            
            if mute_time >= 3600:
                dur_str = f"{mute_time // 3600} ч."
            else:
                dur_str = f"{mute_time // 60} мин."
            
            result_text = f"🔇 {target_name} замучен на {dur_str}"
        
        elif action == "ban":
            bot.ban_chat_member(chat_id, target_uid)
            result_text = f"🚫 {target_name} забанен"
        
        elif action == "unban":
            bot.unban_chat_member(chat_id, target_uid, only_if_banned=True)
            result_text = f"✅ {target_name} разбанен"
        
        elif action == "unmute":
            from telebot.types import ChatPermissions
            bot.restrict_chat_member(
                chat_id, target_uid,
                permissions=ChatPermissions(
                    can_send_messages=True,
                    can_send_media_messages=True,
                    can_send_other_messages=True,
                    can_add_web_page_previews=True
                )
            )
            result_text = f"✅ {target_name} размучен"
    
    except Exception as e:
        result_text = f"❌ Ошибка: {str(e)[:100]}"
    
    # Логируем действие
    log_file = os.path.join(MOD_LOG_DIR, f"{chat_id}.json")
    logs = load_json(log_file, {"actions": []})
    logs["actions"].append({
        "moderator": moderator_uid,
        "action": action,
        "target": target_name,
        "reason": reason,
        "result": result_text,
        "when": datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    })
    logs["actions"] = logs["actions"][-200:]
    save_json(log_file, logs)
    
    return result_text

@bot.message_handler(commands=['ban', 'mute', 'warn', 'unban', 'unmute', 'unwarn'])
def cmd_moderation(message):
    if message.chat.type == "private":
        return
    
    if not is_admin(message.chat.id, message.from_user.id):
        safe_reply(message, "❌ У вас нет прав администратора")
        return
    
    command = message.text.split()[0][1:]  # без слеша
    
    # Парсим аргументы
    parts = message.text.split(maxsplit=2)
    if len(parts) < 2:
        safe_reply(message, f"❌ Использование: /{command} @user [причина]")
        return
    
    target_arg = parts[1].lstrip('@')
    reason = parts[2] if len(parts) > 2 else ""
    
    # Ищем пользователя
    target_uid, target_name = find_user_in_group(message.chat.id, target_arg)
    
    if not target_uid:
        safe_reply(message, f"❌ Пользователь @{target_arg} не найден в этом чате")
        return
    
    text, kb = create_mod_request(message.chat.id, command, target_uid, target_name, reason, message.from_user.id)
    safe_reply(message, text, markup=kb)

# ====================== DEV КОМАНДЫ ======================
@bot.message_handler(commands=['dev'])
def cmd_dev(message):
    if not is_developer(message.from_user) or message.chat.type != "private":
        return
    
    with model_lock:
        current = CURRENT_MODEL
    
    text = (
        "👑 **DEV-ПАНЕЛЬ**\n\n"
        "**Управление:**\n"
        "/dev_give @user сумма — Выдать монеты\n"
        "/dev_take @user сумма — Забрать монеты\n"
        "/dev_setrel @user число — Установить отношения\n"
        "/dev_setlevel @user уровень — Установить уровень\n"
        "/dev_badge @user значок — Выдать значок\n"
        "/dev_reset @user — Сбросить профиль\n\n"
        "**Информация:**\n"
        "/dev_stats — Статистика\n"
        "/dev_economy — Топ экономики\n"
        "/dev_groups — Список групп\n"
        "/dev_model — Выбор модели AI\n"
        "/dev_prompt — Текущий промпт\n\n"
        "**Прочее:**\n"
        "/dev_broadcast текст — Рассылка\n"
        "/dev_modlog ID — Логи модерации\n\n"
        f"Текущая модель: `{current}`"
    )
    
    safe_send(message.chat.id, text)

@bot.message_handler(commands=['dev_give'])
def cmd_dev_give(message):
    if not is_developer(message.from_user) or message.chat.type != "private":
        return
    
    parts = message.text.split()
    if len(parts) < 3:
        safe_send(message.chat.id, "❌ /dev_give @user сумма")
        return
    
    target_arg = parts[1].lstrip('@')
    try:
        amount = abs(int(parts[2]))
    except ValueError:
        safe_send(message.chat.id, "❌ Сумма должна быть числом")
        return
    
    # Ищем пользователя в БД
    row = db_execute("SELECT uid FROM profiles WHERE LOWER(username)=?", (target_arg.lower(),), fetchone=True)
    if not row:
        safe_send(message.chat.id, f"❌ Пользователь @{target_arg} не найден в БД")
        return
    
    target_uid = row[0]
    new_balance = add_coins(target_uid, amount, "dev_give")
    safe_send(message.chat.id, f"✅ Выдано {amount} {CURRENCY_EMOJI} пользователю @{target_arg}\nНовый баланс: {new_balance}")

@bot.message_handler(commands=['dev_take'])
def cmd_dev_take(message):
    if not is_developer(message.from_user) or message.chat.type != "private":
        return
    
    parts = message.text.split()
    if len(parts) < 3:
        safe_send(message.chat.id, "❌ /dev_take @user сумма")
        return
    
    target_arg = parts[1].lstrip('@')
    try:
        amount = abs(int(parts[2]))
    except ValueError:
        safe_send(message.chat.id, "❌ Сумма должна быть числом")
        return
    
    row = db_execute("SELECT uid FROM profiles WHERE LOWER(username)=?", (target_arg.lower(),), fetchone=True)
    if not row:
        safe_send(message.chat.id, f"❌ Пользователь @{target_arg} не найден в БД")
        return
    
    target_uid = row[0]
    if spend_coins(target_uid, amount, "dev_take"):
        new_balance = get_balance(target_uid)
        safe_send(message.chat.id, f"✅ Забрано {amount} {CURRENCY_EMOJI} у @{target_arg}\nНовый баланс: {new_balance}")
    else:
        safe_send(message.chat.id, f"❌ Недостаточно средств у пользователя")

@bot.message_handler(commands=['dev_setrel'])
def cmd_dev_setrel(message):
    if not is_developer(message.from_user) or message.chat.type != "private":
        return
    
    parts = message.text.split()
    if len(parts) < 3:
        safe_send(message.chat.id, "❌ /dev_setrel @user число (-100..100)")
        return
    
    target_arg = parts[1].lstrip('@')
    try:
        value = max(-100, min(100, int(parts[2])))
    except ValueError:
        safe_send(message.chat.id, "❌ Число должно быть от -100 до 100")
        return
    
    row = db_execute("SELECT uid FROM profiles WHERE LOWER(username)=?", (target_arg.lower(),), fetchone=True)
    if not row:
        safe_send(message.chat.id, f"❌ Пользователь @{target_arg} не найден в БД")
        return
    
    target_uid = row[0]
    ensure_profile(target_uid)
    save_profile(target_uid, {"relation": value})
    safe_send(message.chat.id, f"✅ Отношения с @{target_arg} установлены на {value}")

@bot.message_handler(commands=['dev_setlevel'])
def cmd_dev_setlevel(message):
    if not is_developer(message.from_user) or message.chat.type != "private":
        return
    
    parts = message.text.split()
    if len(parts) < 3:
        safe_send(message.chat.id, "❌ /dev_setlevel @user уровень (1-10)")
        return
    
    target_arg = parts[1].lstrip('@')
    try:
        level = max(1, min(10, int(parts[2])))
    except ValueError:
        safe_send(message.chat.id, "❌ Уровень должен быть от 1 до 10")
        return
    
    row = db_execute("SELECT uid FROM profiles WHERE LOWER(username)=?", (target_arg.lower(),), fetchone=True)
    if not row:
        safe_send(message.chat.id, f"❌ Пользователь @{target_arg} не найден в БД")
        return
    
    target_uid = row[0]
    ensure_profile(target_uid)
    
    for l in LEVELS:
        if l["level"] == level:
            save_profile(target_uid, {"level": level, "xp": l["xp"], "title": l["title"]})
            break
    
    safe_send(message.chat.id, f"✅ Уровень @{target_arg} установлен на {level}")

@bot.message_handler(commands=['dev_badge'])
def cmd_dev_badge(message):
    if not is_developer(message.from_user) or message.chat.type != "private":
        return
    
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        safe_send(message.chat.id, "❌ /dev_badge @user значок")
        return
    
    target_arg = parts[1].lstrip('@')
    badge = parts[2]
    
    row = db_execute("SELECT uid FROM profiles WHERE LOWER(username)=?", (target_arg.lower(),), fetchone=True)
    if not row:
        safe_send(message.chat.id, f"❌ Пользователь @{target_arg} не найден в БД")
        return
    
    target_uid = row[0]
    profile = load_profile(target_uid)
    badges = profile.get("badges") or []
    
    if badge not in badges:
        badges.append(badge)
        save_profile(target_uid, {"badges": badges})
        safe_send(message.chat.id, f"✅ Значок '{badge}' выдан @{target_arg}")
    else:
        safe_send(message.chat.id, f"❌ У @{target_arg} уже есть такой значок")

@bot.message_handler(commands=['dev_stats'])
def cmd_dev_stats(message):
    if not is_developer(message.from_user) or message.chat.type != "private":
        return
    
    profile_count = db_execute("SELECT COUNT(*) FROM profiles", fetch=True)
    profile_count = profile_count[0][0] if profile_count else 0
    
    with model_lock:
        current = CURRENT_MODEL
    
    free_count = sum(1 for m in AVAILABLE_MODELS.values() if m.get("free"))
    paid_count = sum(1 for m in AVAILABLE_MODELS.values() if not m.get("free"))
    
    text = (
        f"📊 **СТАТИСТИКА**\n\n"
        f"👥 Профилей: {profile_count}\n"
        f"👥 Групп: {len(group_settings)}\n"
        f"💬 Активных сессий: {len(chat_sessions)}\n"
        f"🎵 Ожидающих треков: {len(pending_tracks)}\n"
        f"🛡️ Ожидающих модераций: {len(pending_mod_actions)}\n\n"
        f"🤖 Модель: {current}\n"
        f"📚 Моделей всего: {len(AVAILABLE_MODELS)} (🆓 {free_count} / 💰 {paid_count})\n"
        f"🎮 Активных игр: {len(active_games)}"
    )
    
    safe_send(message.chat.id, text)

@bot.message_handler(commands=['dev_economy'])
def cmd_dev_economy(message):
    if not is_developer(message.from_user) or message.chat.type != "private":
        return
    
    rows = db_execute(
        "SELECT e.uid, p.display_name, e.balance FROM economy e "
        "LEFT JOIN profiles p ON e.uid=p.uid ORDER BY e.balance DESC LIMIT 15",
        fetch=True
    ) or []
    
    text = "💰 **ТОП ЭКОНОМИКИ**\n\n"
    for i, row in enumerate(rows):
        text += f"{i + 1}. {row[1] or row[0]} — {row[2]} {CURRENCY_EMOJI}\n"
    
    safe_send(message.chat.id, text or "Нет данных")

@bot.message_handler(commands=['dev_model'])
def cmd_dev_model(message):
    global CURRENT_MODEL
    
    if not is_developer(message.from_user) or message.chat.type != "private":
        return
    
    parts = message.text.split(maxsplit=1)
    
    if len(parts) < 2:
        with model_lock:
            current = CURRENT_MODEL
        
        current_name = current
        for mid, info in AVAILABLE_MODELS.items():
            if info['id'] == current:
                current_name = f"{info['name']} ({'🆓' if info.get('free') else '💰'})"
                break
        
        safe_send(message.chat.id,
                 f"🤖 **Выбор модели AI**\n\nТекущая: {current_name}\n\nВыберите категорию:",
                 markup=model_categories_keyboard())
        return
    
    # Прямая установка по ID
    model_id = parts[1].strip()
    with model_lock:
        CURRENT_MODEL = model_id
        save_bot_state()
    
    safe_send(message.chat.id, f"✅ Модель установлена: {model_id}")

@bot.message_handler(commands=['dev_prompt'])
def cmd_dev_prompt(message):
    if not is_developer(message.from_user) or message.chat.type != "private":
        return
    
    prompt = build_prompt(message.chat.id, False, message.from_user.id)
    safe_send(message.chat.id, f"📝 **ТЕКУЩИЙ ПРОМПТ**\n\nДлина: {len(prompt)} символов\n\n")
    
    for i in range(0, len(prompt), 4000):
        safe_send(message.chat.id, prompt[i:i + 4000])

@bot.message_handler(commands=['dev_reset'])
def cmd_dev_reset(message):
    if not is_developer(message.from_user) or message.chat.type != "private":
        return
    
    parts = message.text.split()
    if len(parts) < 2:
        safe_send(message.chat.id, "❌ /dev_reset @user")
        return
    
    target_arg = parts[1].lstrip('@')
    row = db_execute("SELECT uid FROM profiles WHERE LOWER(username)=?", (target_arg.lower(),), fetchone=True)
    if not row:
        safe_send(message.chat.id, f"❌ Пользователь @{target_arg} не найден в БД")
        return
    
    target_uid = row[0]
    db_execute("DELETE FROM profiles WHERE uid=?", (target_uid,))
    db_execute("DELETE FROM economy WHERE uid=?", (target_uid,))
    
    safe_send(message.chat.id, f"✅ Профиль @{target_arg} сброшен")

@bot.message_handler(commands=['dev_broadcast'])
def cmd_dev_broadcast(message):
    if not is_developer(message.from_user) or message.chat.type != "private":
        return
    
    text = message.text.replace("/dev_broadcast", "").strip()
    if not text:
        safe_send(message.chat.id, "❌ /dev_broadcast текст сообщения")
        return
    
    sent = 0
    for gid in group_settings:
        try:
            safe_send(int(gid), f"📢 **РАССЫЛКА**\n\n{text}")
            sent += 1
        except Exception:
            pass
    
    safe_send(message.chat.id, f"✅ Отправлено в {sent} групп")

@bot.message_handler(commands=['dev_modlog'])
def cmd_dev_modlog(message):
    if not is_developer(message.from_user) or message.chat.type != "private":
        return
    
    parts = message.text.split()
    if len(parts) < 2:
        safe_send(message.chat.id, "❌ /dev_modlog ID_группы")
        return
    
    try:
        gid = int(parts[1])
    except ValueError:
        safe_send(message.chat.id, "❌ ID должен быть числом")
        return
    
    log_file = os.path.join(MOD_LOG_DIR, f"{gid}.json")
    logs = load_json(log_file, {"actions": []})
    
    if not logs["actions"]:
        safe_send(message.chat.id, f"📭 Нет логов для группы {gid}")
        return
    
    text = f"📋 **ЛОГИ МОДЕРАЦИИ** (группа {gid})\n\n"
    for a in logs["actions"][-15:]:
        text += f"• {a.get('when', '?')} | {a.get('action', '?')} | {a.get('target', '?')} | {a.get('result', '?')[:30]}\n"
    
    safe_send(message.chat.id, text[:4000])

@bot.message_handler(commands=['dev_groups'])
def cmd_dev_groups(message):
    if not is_developer(message.from_user) or message.chat.type != "private":
        return
    
    text = "👥 **ГРУППЫ**\n\n"
    for gid, settings in group_settings.items():
        text += f"• {settings.get('group_name', gid)} [`{gid}`]\n  Владелец: {settings.get('owner_name', '?')}\n"
    
    safe_send(message.chat.id, text or "Нет групп")

# ====================== УПРАВЛЕНИЕ АДМИНАМИ ======================
@bot.message_handler(commands=['addadmin'])
def cmd_addadmin(message):
    if message.chat.type == "private":
        return
    
    if not is_owner(message.chat.id, message.from_user.id) and not is_developer(message.from_user):
        return
    
    if not message.reply_to_message or not message.reply_to_message.from_user:
        safe_reply(message, "❌ Ответьте на сообщение пользователя, которого хотите сделать админом")
        return
    
    target = message.reply_to_message.from_user
    if target.is_bot:
        return
    
    settings = get_group_settings(message.chat.id)
    
    with settings_lock:
        settings.setdefault("admins", {})[str(target.id)] = {"name": get_display_name(target)}
        save_settings()
    
    register_group(target.id, message.chat.id, message.chat.title)
    safe_reply(message, f"✅ {get_display_name(target)} теперь администратор!")

@bot.message_handler(commands=['removeadmin'])
def cmd_removeadmin(message):
    if message.chat.type == "private":
        return
    
    if not is_owner(message.chat.id, message.from_user.id) and not is_developer(message.from_user):
        return
    
    if not message.reply_to_message:
        return
    
    settings = get_group_settings(message.chat.id)
    
    with settings_lock:
        settings.get("admins", {}).pop(str(message.reply_to_message.from_user.id), None)
        save_settings()
    
    safe_reply(message, "✅ Права администратора отозваны")

@bot.message_handler(commands=['admins'])
def cmd_admins(message):
    if message.chat.type == "private":
        return
    
    settings = get_group_settings(message.chat.id)
    text = f"👑 Владелец: {settings.get('owner_name', '?')}\n"
    text += "👥 Админы:\n"
    
    for a in settings.get("admins", {}).values():
        if isinstance(a, dict):
            text += f"• {a.get('name', '?')}\n"
    
    safe_reply(message, text)

@bot.message_handler(commands=['setowner'])
def cmd_setowner(message):
    if message.chat.type == "private":
        return
    
    if not is_owner(message.chat.id, message.from_user.id) and not is_developer(message.from_user):
        return
    
    if not message.reply_to_message or not message.reply_to_message.from_user:
        return
    
    new_owner = message.reply_to_message.from_user
    if new_owner.is_bot:
        return
    
    settings = get_group_settings(message.chat.id)
    
    with settings_lock:
        old_owner_id = str(settings["owner_id"]) if settings["owner_id"] else None
        settings["admins"].pop(str(new_owner.id), None)
        
        if old_owner_id:
            settings["admins"][old_owner_id] = {"name": settings.get("owner_name", "?")}
        
        settings["owner_id"] = new_owner.id
        settings["owner_name"] = get_display_name(new_owner)
        save_settings()
    
    register_group(new_owner.id, message.chat.id, message.chat.title)
    safe_reply(message, f"✅ Права владельца переданы {get_display_name(new_owner)}")

@bot.message_handler(commands=['unsecret'])
def cmd_unsecret(message):
    if message.chat.type == "private":
        secret_links.pop(message.from_user.id, None)
        safe_reply(message, "🔐 Секретная ссылка удалена")

# ====================== CALLBACKS ======================
@bot.callback_query_handler(func=lambda call: True)
def on_callback(call):
    try:
        uid = call.from_user.id
        chat_id = call.message.chat.id
        msg_id = call.message.message_id
        chat_type = call.message.chat.type
        data = call.data
        
        update_user_info(uid, call.from_user)
        
        if data == "noop":
            bot.answer_callback_query(call.id)
            return
        
        # Модерация
        if data.startswith("modok_") or data.startswith("moddeny_"):
            handle_mod_callback(call, uid, chat_id, msg_id, data)
            return
        
        # Модели AI (только для разработчиков)
        if data.startswith("mcat_") or data.startswith("mset_") or data in ("dev_back", "mcat_back", "mcat_search"):
            if not is_developer(call.from_user):
                bot.answer_callback_query(call.id, "❌ Только для разработчиков", show_alert=True)
                return
            handle_model_callback(call, uid, chat_id, msg_id, data)
            return
        
        # Треки
        if data.startswith("tr_"):
            handle_track_callback(call, chat_id, msg_id, chat_type)
            return
        
        if data.startswith("trsv_"):
            handle_save_callback(call, uid, chat_id, msg_id)
            return
        
        if data.startswith("plsv_"):
            handle_plsv_callback(call, uid, chat_id, msg_id, data)
            return
        
        # Скачивание
        if data in ("dl_mp4", "dl_mp3"):
            handle_dl_callback(call, chat_id, msg_id, chat_type)
            return
        
        # Покупки
        if data.startswith("buy_"):
            handle_buy_callback(call, uid, chat_id, msg_id, data)
            return
        
        if data.startswith("shop_") or data == "daily":
            handle_shop_callback(call, uid, chat_id, msg_id, data)
            return
        
        # Игры
        if data.startswith("game_"):
            handle_game_callback(call, uid, chat_id, msg_id, data)
            return
        
        if data.startswith("gans_"):
            handle_quiz_callback(call, uid, chat_id, msg_id, data)
            return
        
        if data.startswith("tod_"):
            handle_tod_callback(call, uid, chat_id, msg_id, data)
            return
        
        # Плейлисты
        if data.startswith("pl_") or data.startswith("gpl_") or data == "group_pl":
            handle_playlist_callback(call, uid, chat_id, msg_id, data)
            return
        
        # Документация
        if data.startswith("help_") or data.startswith("docs_"):
            handle_docs_callback(call, uid, chat_id, msg_id, data)
            return
        
        # Личные сообщения
        if chat_type == "private":
            handle_private_callback(call, uid, chat_id, msg_id, data)
        else:
            # Групповые настройки
            if not is_admin(chat_id, uid) and not is_developer(call.from_user):
                bot.answer_callback_query(call.id, "❌ Нет прав", show_alert=True)
                return
            handle_group_callback(call, data, uid, chat_id, msg_id)
    
    except Exception as e:
        log.error(f"Ошибка callback: {e}")
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка")
        except Exception:
            pass

def handle_mod_callback(call, uid, chat_id, msg_id, data):
    is_confirm = data.startswith("modok_")
    request_id = data[6:] if is_confirm else data[8:]
    
    with mod_lock:
        req = pending_mod_actions.get(request_id)
        if not req:
            bot.answer_callback_query(call.id, "❌ Запрос устарел", show_alert=True)
            safe_edit("❌ Запрос на модерацию устарел", chat_id, msg_id)
            return
        
        req_chat_id = req["cid"]
        
        if not is_admin(req_chat_id, uid) and not is_developer(call.from_user):
            bot.answer_callback_query(call.id, "❌ Нет прав", show_alert=True)
            return
        
        pending_mod_actions.pop(request_id, None)
    
    if not is_confirm:
        bot.answer_callback_query(call.id, "❌ Отменено")
        safe_edit(
            f"❌ Модерация отклонена\n"
            f"Действие: {req['action']} → {req['target_name']}\n"
            f"Отменил: {get_display_name(call.from_user)}",
            chat_id, msg_id
        )
        return
    
    # Выполняем действие
    result = execute_mod_action(
        req_chat_id, req["action"], req["target_uid"],
        req["target_name"], req["reason"], uid
    )
    
    bot.answer_callback_query(call.id, "✅ Выполнено", show_alert=True)
    safe_edit(f"{result}\n\nМодератор: {get_display_name(call.from_user)}", chat_id, msg_id)

def handle_model_callback(call, uid, chat_id, msg_id, data):
    global CURRENT_MODEL
    bot.answer_callback_query(call.id)
    
    if data == "dev_back":
        safe_edit("👑 Выберите действие:", chat_id, msg_id, markup=None)
        return
    
    if data == "mcat_back":
        with model_lock:
            current = CURRENT_MODEL
        current_name = current
        for mid, info in AVAILABLE_MODELS.items():
            if info['id'] == current:
                current_name = f"{info['name']} ({'🆓' if info.get('free') else '💰'})"
                break
        safe_edit(f"🤖 **Выбор модели**\n\nТекущая: {current_name}\n\nВыберите категорию:",
                 chat_id, msg_id, markup=model_categories_keyboard())
        return
    
    if data == "mcat_search":
        with user_states_lock:
            user_states[f"msearch_{uid}"] = True
        safe_edit("🔍 Введите название модели или её ID для поиска:", chat_id, msg_id)
        return
    
    if data.startswith("mcat_"):
        parts = data[5:].rsplit("_", 1)
        if len(parts) == 2 and parts[1] in {"free", "paid"}:
            cat, free_filter = parts[0], parts[1]
        else:
            cat, free_filter = data[5:], None
        
        cat_name = MODEL_CATEGORIES.get(cat, cat)
        safe_edit(f"📚 {cat_name}\nВыберите модель:", chat_id, msg_id,
                 markup=model_list_keyboard(cat, free_filter))
        return
    
    if data.startswith("mset_"):
        key = data[5:]
        if key in AVAILABLE_MODELS:
            with model_lock:
                CURRENT_MODEL = AVAILABLE_MODELS[key]["id"]
                save_bot_state()
            info = AVAILABLE_MODELS[key]
            safe_edit(
                f"✅ Модель установлена: {info['name']}\n"
                f"{'🆓 Бесплатная' if info.get('free') else '💰 Платная'}\n"
                f"ID: `{info['id']}`",
                chat_id, msg_id,
                markup=model_categories_keyboard()
            )

def handle_docs_callback(call, uid, chat_id, msg_id, data):
    bot.answer_callback_query(call.id)
    
    if data == "help_commands":
        safe_edit(HELP_COMMANDS, chat_id, msg_id, markup=help_keyboard())
    elif data == "help_voice":
        safe_edit(HELP_VOICE, chat_id, msg_id, markup=help_keyboard())
    elif data == "docs_main":
        safe_edit("📚 **Документация**\n\nВыберите раздел:", chat_id, msg_id, markup=docs_keyboard())
    elif data.startswith("docs_"):
        key = data[5:]
        text = DOCS.get(key, "Раздел в разработке")
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("◀️ Назад", callback_data="docs_main"))
        safe_edit(text, chat_id, msg_id, markup=kb)

def handle_save_callback(call, uid, chat_id, msg_id):
    pending = find_pending(chat_id)
    if not pending:
        bot.answer_callback_query(call.id, "❌ Нет результатов", show_alert=True)
        return
    
    latest_key, latest_data = max(pending, key=lambda x: x[1].get("time", datetime.min))
    saved = 0
    
    for track in latest_data.get("results", []):
        if add_to_playlist(uid, track, chat_id if chat_id < 0 else None,
                          save_personal=True, save_group=(chat_id < 0)):
            saved += 1
    
    bot.answer_callback_query(call.id, f"✅ Сохранено {saved} треков" if saved else "❌ Ничего не сохранено", show_alert=True)
    
    if saved:
        achievements = check_achievements(uid)
        notify_achievements(chat_id, uid, achievements)

def handle_plsv_callback(call, uid, chat_id, msg_id, data):
    parts = data.split("_", 2)
    if len(parts) < 3:
        bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
        return
    
    save_type = parts[1]
    track_key = parts[2]
    
    if save_type == "skip":
        bot.answer_callback_query(call.id, "⏭️ Пропущено")
        safe_delete(chat_id, msg_id)
        with user_states_lock:
            user_states.pop(f"track_{track_key}", None)
        return
    
    with user_states_lock:
        track = user_states.pop(f"track_{track_key}", None)
    
    if not track:
        bot.answer_callback_query(call.id, "❌ Трек устарел", show_alert=True)
        safe_delete(chat_id, msg_id)
        return
    
    group_chat_id = chat_id if chat_id < 0 else None
    save_personal = save_type in {"my", "both"}
    save_group = save_type in {"grp", "both"} and group_chat_id is not None
    
    result = add_to_playlist(uid, track, group_chat_id,
                            save_personal=save_personal, save_group=save_group)
    
    if result:
        where = {"my": "личный", "grp": "групповой", "both": "оба"}.get(save_type, "плейлист")
        bot.answer_callback_query(call.id, f"✅ Сохранено в {where}!", show_alert=True)
        safe_edit(f"✅ {track['title'][:40]} → сохранено в {where}", chat_id, msg_id)
        
        achievements = check_achievements(uid)
        notify_achievements(chat_id, uid, achievements)
    else:
        bot.answer_callback_query(call.id, "❌ Уже есть в плейлисте", show_alert=True)
        safe_delete(chat_id, msg_id)

def handle_track_callback(call, chat_id, msg_id, chat_type):
    parts = call.data.split("_")
    if len(parts) < 3:
        return
    
    try:
        orig_msg_id = int(parts[1])
        action = parts[2]
    except (IndexError, ValueError):
        return
    
    key = pending_key(chat_id, orig_msg_id)
    
    with pending_lock:
        if key not in pending_tracks:
            bot.answer_callback_query(call.id, "❌ Треки устарели", show_alert=True)
            return
        
        if action == "x":
            pending_tracks.pop(key, None)
            safe_edit("❌ Отменено", chat_id, msg_id)
            bot.answer_callback_query(call.id)
            return
        
        try:
            idx = int(action)
        except ValueError:
            bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
            return
        
        data = pending_tracks.pop(key, None)
    
    if not data or idx >= len(data.get("results", [])):
        bot.answer_callback_query(call.id, "❌ Трек не найден", show_alert=True)
        return
    
    track = data["results"][idx]
    is_busy_flag, busy_type = is_busy(chat_id)
    
    if is_busy_flag:
        with pending_lock:
            pending_tracks[key] = data
        bot.answer_callback_query(call.id, f"⏳ Я занята {busy_type}", show_alert=True)
        return
    
    uid = call.from_user.id
    set_busy(chat_id, "music", track['title'])
    safe_edit(f"⏳ Загружаю {track['title'][:40]}...", chat_id, msg_id)
    bot.answer_callback_query(call.id, f"🎵 {track['title'][:50]}")
    
    update_stat(uid, "music")
    add_xp(uid, 3)
    
    add_task(download_and_send, chat_id, msg_id, track, chat_type != "private", uid)

def handle_dl_callback(call, chat_id, msg_id, chat_type):
    with user_states_lock:
        url = user_states.pop(f"dl_{chat_id}_{msg_id}", None)
    
    if not url:
        bot.answer_callback_query(call.id, "❌ Ссылка устарела", show_alert=True)
        return
    
    is_busy_flag, busy_type = is_busy(chat_id)
    if is_busy_flag:
        with user_states_lock:
            user_states[f"dl_{chat_id}_{msg_id}"] = url
        bot.answer_callback_query(call.id, f"⏳ Я занята {busy_type}", show_alert=True)
        return
    
    fmt = "mp3" if call.data == "dl_mp3" else "mp4"
    set_busy(chat_id, "music" if fmt == "mp3" else "video")
    safe_edit(f"⏳ Скачиваю {fmt.upper()}...", chat_id, msg_id)
    bot.answer_callback_query(call.id, f"⬇️ {fmt.upper()}")
    
    uid = call.from_user.id
    update_stat(uid, "videos" if fmt == "mp4" else "music")
    
    add_task(download_url_send, chat_id, msg_id, url, fmt, chat_type != "private")

def handle_buy_callback(call, uid, chat_id, msg_id, data):
    item_id = data[4:]
    
    if item_id not in SHOP_ITEMS:
        bot.answer_callback_query(call.id, "❌ Товар не найден", show_alert=True)
        return
    
    item = SHOP_ITEMS[item_id]
    
    if get_balance(uid) < item["price"] and uid not in DEVELOPER_IDS:
        bot.answer_callback_query(call.id, f"❌ Нужно {item['price']} {CURRENCY_EMOJI}", show_alert=True)
        return
    
    if not spend_coins(uid, item["price"], f"Покупка: {item['name']}"):
        bot.answer_callback_query(call.id, "❌ Ошибка оплаты", show_alert=True)
        return
    
    bot.answer_callback_query(call.id, f"✅ Куплено: {item['name']}", show_alert=True)
    
    if item["type"] == "badge":
        profile = load_profile(uid)
        badges = profile.get("badges") or []
        badge = item.get("badge", "")
        if badge not in badges:
            badges.append(badge)
            save_profile(uid, {"badges": badges})
        safe_edit(
            f"✅ Приобретено: {item['name']}!\n"
            f"💰 Остаток: {fmt_coins(get_balance(uid))}",
            chat_id, msg_id,
            markup=shop_main_keyboard()
        )
    
    elif item["type"] == "boost":
        exp = (datetime.now() + timedelta(seconds=item.get("dur", 3600))).strftime("%Y-%m-%d %H:%M:%S")
        save_profile(uid, {"boosts": {"double_xp": exp}})
        safe_edit(
            f"✅ Бонус {item['name']} активирован!\n"
            f"⏳ Действует до {exp[11:16]}\n"
            f"💰 Остаток: {fmt_coins(get_balance(uid))}",
            chat_id, msg_id,
            markup=shop_main_keyboard()
        )
    
    elif item["type"] == "custom_title":
        with user_states_lock:
            user_states[f"ct_{uid}"] = True
        safe_edit(
            "✏️ Введите свой титул (максимум 20 символов):\n"
            "(или отправьте «отмена» для отмены)",
            chat_id, msg_id
        )
    
    elif item["type"] == "name_emoji":
        with user_states_lock:
            user_states[f"ne_{uid}"] = True
        safe_edit(
            "🎨 Отправьте эмодзи для имени (1 символ):\n"
            "(или отправьте «отмена» для отмены)",
            chat_id, msg_id
        )
    
    elif item["type"] == "gift":
        rel_bonus = item.get("rel", 3)
        new_rel = change_relation(uid, rel_bonus)
        update_stat(uid, "gifts_given")
        add_xp(uid, rel_bonus * 2)
        record_gift(uid, get_display_name(call.from_user), item)
        
        reaction = gift_reaction(item["name"], get_display_name(call.from_user), new_rel)
        
        safe_edit(
            f"🎁 {get_display_name(call.from_user)} дарит {item['name']}!\n\n"
            f"💬 {reaction}\n\n"
            f"❤️ Отношения: {new_rel}/100 (+{rel_bonus}) | {fmt_coins(get_balance(uid))}",
            chat_id, msg_id,
            markup=shop_main_keyboard()
        )
        
        achievements = check_achievements(uid)
        notify_achievements(chat_id, uid, achievements)
    
    elif item["type"] == "hinata_action":
        add_task(do_hinata_action, chat_id, msg_id, uid, item_id, item, call.from_user)

def do_hinata_action(chat_id, msg_id, uid, item_id, item, user):
    try:
        name = get_display_name(user)
        rel, _, _ = get_relation_info(uid)
        
        prompts = {
            "compliment": f"Сделай комплимент пользователю {name} (отношения {rel}/100). 1 предложение.",
            "roast": f"Подшути по-доброму над {name} (отношения {rel}/100). 1 предложение.",
            "poem": f"Напиши короткое стихотворение (2-4 строки) для {name}.",
            "fortune": f"Придумай забавное предсказание для {name}. 1-2 предложения.",
            "nickname": f"Придумай креативное прозвище для {name}. Только прозвище, без объяснений.",
            "story": f"Напиши очень короткую смешную историю (3 предложения) про {name}.",
            "advice": f"Дай мудрый совет пользователю {name}. 1 предложение."
        }
        
        prompt = prompts.get(item_id, "Ответь пользователю коротко и мило.")
        
        response = ask_ai([{"role": "system", "content": prompt}], max_tokens=100)
        result = clean_text(response) if response and not is_error(response) else "✨ Вот!"
        
        change_relation(uid, 1)
        add_xp(uid, 5)
        
        safe_edit(
            f"✨ **{item['name']}**\n\n{result}\n\n💰 {fmt_coins(get_balance(uid))}",
            chat_id, msg_id,
            markup=shop_main_keyboard()
        )
    except Exception as e:
        log.error(f"Ошибка действия: {e}")
        safe_edit("❌ Что-то пошло не так", chat_id, msg_id, markup=shop_main_keyboard())

def handle_shop_callback(call, uid, chat_id, msg_id, data):
    bot.answer_callback_query(call.id)
    
    if data == "shop_main":
        safe_edit("🛒 **Магазин**\n\nВыбери категорию:", chat_id, msg_id, markup=shop_main_keyboard())
    
    elif data.startswith("shop_cat_"):
        cat = data[9:]
        labels = {"service": "🎁 Услуги", "gift": "💝 Подарки", "self": "✨ Для себя"}
        safe_edit(labels.get(cat, cat) + ":", chat_id, msg_id, markup=shop_category_keyboard(cat))
    
    elif data == "daily":
        result = claim_daily(uid)
        if result[0] is None:
            bot.answer_callback_query(call.id, "⏰ Уже получал сегодня!", show_alert=True)
        else:
            total, streak, bonus = result
            text = f"🎁 Ежедневный бонус: +{total} {CURRENCY_EMOJI}\n🔥 Стрик: {streak} дней"
            if bonus > 0:
                text += f" (+{bonus} бонус)"
            safe_edit(text, chat_id, msg_id, markup=main_keyboard())
            add_xp(uid, 5)

def handle_game_callback(call, uid, chat_id, msg_id, data):
    bot.answer_callback_query(call.id)
    update_stat(uid, "games")
    
    if data == "game_tod":
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.add(
            types.InlineKeyboardButton("🤔 Правда", callback_data="tod_truth"),
            types.InlineKeyboardButton("😈 Действие", callback_data="tod_dare")
        )
        kb.row(types.InlineKeyboardButton("◀️ Назад", callback_data="games_menu"))
        safe_edit("🎭 Выбери:", chat_id, msg_id, markup=kb)
    
    elif data == "game_quiz":
        q = random.choice(QuizGame.QUESTIONS)
        with game_lock:
            active_games[f"q_{chat_id}_{msg_id}"] = {"q": q, "done": False, "time": datetime.now()}
        
        kb = types.InlineKeyboardMarkup(row_width=2)
        for i, opt in enumerate(q["opts"]):
            kb.add(types.InlineKeyboardButton(opt, callback_data=f"gans_{msg_id}_{i}"))
        
        safe_edit(f"❓ **{q['q']}**\n\n+10 🪙 за правильный ответ", chat_id, msg_id, markup=kb)
    
    elif data == "game_number":
        game = NumberGame()
        with game_lock:
            active_games[f"n_{chat_id}"] = {"g": game, "time": datetime.now()}
        safe_edit(
            f"🔢 **Угадай число**\n\n"
            f"Я загадала число от 1 до 100.\n"
            f"У тебя {game.max_attempts} попыток.\n"
            f"Приз: 20 🪙",
            chat_id, msg_id
        )
    
    elif data == "game_word":
        game = WordGame()
        with game_lock:
            active_games[f"w_{chat_id}"] = {"g": game, "time": datetime.now()}
        safe_edit(
            f"📝 **Виселица**\n\n"
            f"Слово: {game.display()}\n"
            f"Ошибок: 0/{game.max_wrong}\n"
            f"Приз: 15 🪙",
            chat_id, msg_id
        )
    
    achievements = check_achievements(uid)
    notify_achievements(chat_id, uid, achievements)

def handle_tod_callback(call, uid, chat_id, msg_id, data):
    bot.answer_callback_query(call.id)
    
    if data == "tod_truth":
        q = random.choice(TruthOrDare.TRUTHS)
        cat = "🤔 Правда"
    else:
        q = random.choice(TruthOrDare.DARES)
        cat = "😈 Действие"
    
    add_coins(uid, 5, "Игра Правда/Действие")
    add_xp(uid, 3)
    
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("🤔 Правда", callback_data="tod_truth"),
        types.InlineKeyboardButton("😈 Действие", callback_data="tod_dare")
    )
    kb.row(types.InlineKeyboardButton("◀️ Назад", callback_data="games_menu"))
    
    safe_edit(f"{cat}:\n\n**{q}**\n\n+5 🪙", chat_id, msg_id, markup=kb)

def handle_quiz_callback(call, uid, chat_id, msg_id, data):
    parts = data.split("_")
    if len(parts) < 3:
        return
    
    orig, idx = parts[1], int(parts[2])
    game_key = f"q_{chat_id}_{orig}"
    
    with game_lock:
        game_data = active_games.get(game_key)
        if not game_data or game_data.get("done"):
            bot.answer_callback_query(call.id, "❌ Игра закончена", show_alert=True)
            return
        game_data["done"] = True
    
    q = game_data["q"]
    selected = q["opts"][idx].lower() if idx < len(q["opts"]) else ""
    is_correct = any(a in selected for a in q["a"])
    
    if is_correct:
        add_coins(uid, 10, "Победа в викторине")
        add_xp(uid, 8)
        update_stat(uid, "wins")
        change_relation(uid, 1)
        result_text = "✅ **Правильно!** +10 🪙"
    else:
        correct = next((o for o in q["opts"] if any(a in o.lower() for a in q["a"])), "?")
        result_text = f"❌ Неправильно. Правильный ответ: {correct}"
        add_xp(uid, 2)
    
    bot.answer_callback_query(call.id, result_text, show_alert=True)
    
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("🎲 Ещё", callback_data="game_quiz"))
    kb.add(types.InlineKeyboardButton("◀️ Назад", callback_data="games_menu"))
    
    safe_edit(f"❓ {q['q']}\n\n{result_text}", chat_id, msg_id, markup=kb)
    
    with game_lock:
        active_games.pop(game_key, None)
    
    achievements = check_achievements(uid)
    notify_achievements(chat_id, uid, achievements)

def handle_playlist_callback(call, uid, chat_id, msg_id, data):
    bot.answer_callback_query(call.id)
    
    if data == "pl_clear":
        save_playlist(uid, {"tracks": []})
        safe_edit("🗑️ Плейлист очищен", chat_id, msg_id, markup=main_keyboard())
    
    elif data == "group_pl":
        groups = get_user_groups(uid)
        if not groups:
            safe_edit("❌ Нет групп", chat_id, msg_id, markup=main_keyboard())
            return
        
        gid = int(list(groups.keys())[0])
        gpl = load_group_playlist(gid)
        
        if not gpl["tracks"]:
            safe_edit("📭 Групповой плейлист пуст", chat_id, msg_id, markup=main_keyboard())
            return
        
        text = f"👥 **Групповой плейлист** ({len(gpl['tracks'])} треков)\n\n"
        for i, track in enumerate(gpl["tracks"][-10:]):
            text += f"{i + 1}. {track['title'][:35]}\n"
        
        safe_edit(text, chat_id, msg_id, markup=playlist_keyboard(gid, True))
    
    elif data.startswith("pl_play_") or data.startswith("gpl_play_"):
        is_group_pl = data.startswith("gpl_")
        idx = int(data.split("_")[-1])
        
        source = load_group_playlist(chat_id) if is_group_pl else load_playlist(uid)
        
        if 0 <= idx < len(source["tracks"]):
            track = source["tracks"][idx]
            if track.get("url"):
                is_busy_flag, busy_type = is_busy(chat_id)
                if is_busy_flag:
                    safe_send(chat_id, f"⏳ Я занята {busy_type}")
                    return
                
                set_busy(chat_id, "music", track['title'])
                safe_edit(f"⏳ Загружаю {track['title'][:35]}...", chat_id, msg_id)
                add_task(download_and_send, chat_id, msg_id, track, False, uid)

def handle_private_callback(call, uid, chat_id, msg_id, data):
    if data == "clear":
        clear_history(uid)
        safe_edit("🧹 История очищена!", chat_id, msg_id, markup=main_keyboard())
    
    elif data == "profile":
        safe_edit(format_profile(uid, call.from_user), chat_id, msg_id, markup=main_keyboard())
    
    elif data == "balance":
        profile = load_profile(uid)
        safe_edit(
            f"👤 **{profile.get('display_name') or 'Пользователь'}**\n\n"
            f"💰 Баланс: {fmt_coins(get_balance(uid))}\n"
            f"📊 Уровень: {profile.get('level', 1)} ({profile.get('xp', 0)} XP)",
            chat_id, msg_id,
            markup=main_keyboard()
        )
    
    elif data == "back_main":
        safe_edit("🌸 Главное меню:", chat_id, msg_id, markup=main_keyboard())
    
    elif data == "my_groups":
        groups = get_user_groups(uid)
        if not groups:
            safe_edit("📭 У вас пока нет групп. Добавьте меня в группу!", chat_id, msg_id, markup=start_keyboard())
        else:
            safe_edit("👥 Ваши группы:", chat_id, msg_id, markup=groups_list_keyboard(uid))
    
    elif data == "games_menu":
        safe_edit("🎮 Выбери игру:", chat_id, msg_id, markup=games_keyboard())
    
    elif data == "playlist":
        pl = load_playlist(uid)
        if not pl["tracks"]:
            safe_edit("📭 Твой плейлист пуст. Найди музыку и сохрани её!", chat_id, msg_id, markup=main_keyboard())
        else:
            text = f"🎵 **Твой плейлист** ({len(pl['tracks'])} треков)\n\n"
            for i, track in enumerate(pl["tracks"][-10:]):
                text += f"{i + 1}. {track['title'][:35]}\n"
            safe_edit(text, chat_id, msg_id, markup=playlist_keyboard(uid))
    
    elif data.startswith("pg_sel_"):
        try:
            gid = int(data[7:])
        except ValueError:
            return
        
        if is_admin(gid, uid) or is_developer(call.from_user):
            settings = get_group_settings(gid)
            group_name = get_user_groups(uid).get(str(gid), {}).get('title', '?')
            safe_edit(
                f"⚙️ **{group_name}**\n"
                f"Шанс ответа: {settings['response_chance']}%",
                chat_id, msg_id,
                markup=group_settings_keyboard(gid)
            )
    
    elif data.startswith("pg_"):
        handle_pg_callback(call, data, uid, chat_id, msg_id)
    
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass

def handle_pg_callback(call, data, uid, chat_id, msg_id):
    actions = {
        "pg_cd10_": "cd10", "pg_cu10_": "cu10",
        "pg_cd5_": "cd5", "pg_cu5_": "cu5",
        "pg_pt_": "pt", "pg_lt_": "lt",
        "pg_as_": "as", "pg_md_": "md",
        "pg_aa_": "aa", "pg_pr_": "pr",
        "pg_cc_": "cc", "pg_cm_": "cm",
        "pg_pc_": "pc", "pg_mr_": "mr",
        "pg_secret_": "secret"
    }
    
    action = None
    gid = None
    
    for prefix, act in actions.items():
        if data.startswith(prefix):
            try:
                gid = int(data[len(prefix):])
                action = act
            except ValueError:
                pass
            break
    
    if not action or gid is None:
        return
    
    if not is_admin(gid, uid) and not is_developer(call.from_user):
        bot.answer_callback_query(call.id, "❌ Нет прав", show_alert=True)
        return
    
    settings = get_group_settings(gid)
    
    if action == "pc":
        with user_states_lock:
            user_states[f"pp_{uid}"] = gid
        safe_edit("📝 Введите новый системный промпт (или «отмена»):", chat_id, msg_id)
    
    elif action == "mr":
        with user_states_lock:
            user_states[f"mr_{uid}"] = gid
        safe_edit("📋 Введите правила модерации (или «отмена»):", chat_id, msg_id)
    
    elif action == "secret":
        secret_links[uid] = gid
        group_name = get_user_groups(uid).get(str(gid), {}).get('title', '?')
        safe_edit(
            f"🔐 Секретная ссылка создана!\n"
            f"Теперь в ЛС с ботом все сообщения будут отправляться в группу {group_name}\n"
            f"Команда /unsecret — удалить ссылку",
            chat_id, msg_id,
            markup=group_settings_keyboard(gid)
        )
        bot.answer_callback_query(call.id, f"🔐 Ссылка на {group_name}", show_alert=True)
        return
    
    else:
        apply_setting(settings, action, gid)
        group_name = get_user_groups(uid).get(str(gid), {}).get('title', '?')
        safe_edit(
            f"⚙️ **{group_name}**\n"
            f"Шанс ответа: {settings['response_chance']}%",
            chat_id, msg_id,
            markup=group_settings_keyboard(gid)
        )
    
    bot.answer_callback_query(call.id)

def handle_group_callback(call, data, uid, chat_id, msg_id):
    settings = get_group_settings(chat_id)
    
    if data == "close":
        safe_delete(chat_id, msg_id)
    
    elif data in ("cd10", "cu10", "cd5", "cu5", "pt", "as", "md", "aa"):
        apply_setting(settings, data, chat_id)
        safe_edit(
            f"⚙️ **Настройки группы**\n"
            f"Шанс ответа: {settings['response_chance']}%",
            chat_id, msg_id,
            markup=group_settings_keyboard(chat_id)
        )
    
    elif data == "pc":
        with user_states_lock:
            user_states[f"{chat_id}_{uid}"] = "wp"
        safe_send(chat_id, "📝 Введите новый системный промпт (или «отмена»):")
    
    elif data == "mr":
        with user_states_lock:
            user_states[f"{chat_id}_{uid}"] = "mr"
        safe_send(chat_id, "📋 Введите правила модерации (или «отмена»):")
    
    elif data == "secret":
        secret_links[uid] = chat_id
        bot.answer_callback_query(call.id, f"🔐 Ссылка создана", show_alert=True)
    
    elif data == "games_menu":
        safe_edit("🎮 Выбери игру:", chat_id, msg_id, markup=games_keyboard())
        bot.answer_callback_query(call.id)
    
    else:
        bot.answer_callback_query(call.id)

# ====================== ЗАГРУЗКА И ОТПРАВКА ======================
def download_and_send(chat_id, msg_id, track, is_group, req_uid=None):
    try:
        result, error = download_with_timeout(download_track, track['url'])
        if error:
            safe_edit(f"❌ {error}", chat_id, msg_id)
            return
        
        try:
            comment = music_comment(chat_id, result['title'], is_group)
            send_audio_file(chat_id, result, comment)
            safe_delete(chat_id, msg_id)
            add_message(chat_id, "assistant", comment, is_group)
            
            if req_uid:
                track_info = {
                    'title': result.get('title', '?'),
                    'artist': result.get('artist', ''),
                    'url': result.get('url', track.get('url', '')),
                    'duration': result.get('duration', 0)
                }
                
                track_key = f"{chat_id}_{req_uid}_{int(time.time())}"
                with user_states_lock:
                    user_states[f"track_{track_key}"] = track_info
                
                if chat_id < 0:
                    safe_send(chat_id,
                             "💾 Сохранить трек?",
                             markup=playlist_save_keyboard(chat_id, req_uid, track_key))
                else:
                    kb = types.InlineKeyboardMarkup()
                    kb.add(types.InlineKeyboardButton("💾 Сохранить", callback_data=f"plsv_my_{track_key}"))
                    kb.add(types.InlineKeyboardButton("❌ Пропустить", callback_data=f"plsv_skip_{track_key}"))
                    safe_send(chat_id, "💾 Сохранить трек?", markup=kb)
        
        except Exception as e:
            log.error(f"Ошибка отправки: {e}")
            safe_edit("❌ Ошибка отправки", chat_id, msg_id)
        
        finally:
            shutil.rmtree(result.get('temp_dir', ''), ignore_errors=True)
    
    except Exception as e:
        log.error(f"Ошибка загрузки: {e}")
        safe_edit("❌ Ошибка загрузки", chat_id, msg_id)
    
    finally:
        clear_busy(chat_id)

def download_url_send(chat_id, msg_id, url, fmt, is_group):
    try:
        download_func = download_track if fmt == "mp3" else download_video
        result, error = download_with_timeout(download_func, url)
        
        if error:
            safe_edit(f"❌ {error}", chat_id, msg_id)
            return
        
        try:
            if fmt == "mp3":
                send_audio_file(chat_id, result, music_comment(chat_id, result['title'], is_group))
            else:
                with open(result['file'], 'rb') as v:
                    bot.send_video(
                        chat_id,
                        v,
                        caption=result.get('title', ''),
                        duration=safe_duration(result.get('duration', 0)),
                        supports_streaming=True
                    )
            
            safe_delete(chat_id, msg_id)
        
        except Exception as e:
            log.error(f"Ошибка отправки: {e}")
            safe_edit("❌ Ошибка отправки", chat_id, msg_id)
        
        finally:
            shutil.rmtree(result.get('temp_dir', ''), ignore_errors=True)
    
    except Exception as e:
        safe_edit(f"❌ Ошибка: {str(e)[:100]}", chat_id, msg_id)
    
    finally:
        clear_busy(chat_id)

# ====================== ОБРАБОТКА ДЕЙСТВИЙ ======================
def handle_actions(chat_id, actions, is_group, uid=None, reply_to=None):
    for action in actions:
        if action["type"] == "music_search":
            handle_music_search(chat_id, action["query"], is_group, uid)
        
        elif action["type"] == "video_download":
            handle_video_download(chat_id, action["url"], is_group)
        
        elif action["type"] == "reminder":
            set_reminder(chat_id, uid, action["minutes"], action["text"], reply_to)
            safe_send(chat_id, f"⏰ Напоминание через {action['minutes']} мин.", reply_to=reply_to)
        
        elif action["type"] == "mod_request":
            settings = get_group_settings(chat_id)
            if not settings.get("moderation"):
                safe_send(chat_id, "❌ Модерация отключена в этой группе", reply_to=reply_to)
                continue
            
            parts = action["action"].split(maxsplit=2)
            if len(parts) < 2:
                continue
            
            mod_action = parts[0].lower()
            target_name = parts[1].lstrip('@')
            reason = parts[2] if len(parts) > 2 else ""
            
            if mod_action not in MOD_ACTIONS:
                continue
            
            target_uid, display = find_user_in_group(chat_id, target_name)
            if not target_uid:
                safe_send(chat_id, f"❌ Пользователь {target_name} не найден", reply_to=reply_to)
                continue
            
            text, kb = create_mod_request(chat_id, mod_action, target_uid,
                                         display or target_name, reason,
                                         requested_by=None)
            safe_send(chat_id, f"🛡️ {text}", markup=kb, reply_to=reply_to)

def handle_music_search(chat_id, query, is_group, uid=None):
    is_busy_flag, busy_type = is_busy(chat_id)
    if is_busy_flag:
        safe_send(chat_id, f"⏳ Я занята {busy_type}")
        return
    
    set_busy(chat_id, "music", query)
    status_msg = safe_send(chat_id, f"🎵 Ищу «{query}»...")
    
    if not status_msg:
        clear_busy(chat_id)
        return
    
    if uid:
        update_stat(uid, "music")
        add_xp(uid, 3)
    
    def search_worker():
        try:
            results = search_tracks(query)
            if not results:
                safe_edit("❌ Ничего не найдено", chat_id, status_msg.message_id)
                return
            
            results = results[:6]
            key = pending_key(chat_id, status_msg.message_id)
            
            with pending_lock:
                pending_tracks[key] = {
                    "results": results,
                    "query": query,
                    "time": datetime.now()
                }
            
            text = f"🎵 **Результаты для «{query}»**\n\n"
            for i, r in enumerate(results):
                text += f"{i + 1}. {r['title'][:40]} ({format_duration(r.get('duration', 0))}) {r.get('source', '')}\n"
            
            safe_edit(text, chat_id, status_msg.message_id,
                     markup=track_keyboard(len(results), status_msg.message_id))
        
        except Exception as e:
            log.error(f"Ошибка поиска: {e}")
            safe_edit("❌ Ошибка поиска", chat_id, status_msg.message_id)
        
        finally:
            clear_busy(chat_id)
    
    add_task(search_worker)

def handle_video_download(chat_id, url, is_group):
    msg = safe_send(chat_id,
                   f"⬇️ {get_platform_name(url)} - выбрать формат:",
                   markup=download_format_keyboard())
    if msg:
        with user_states_lock:
            user_states[f"dl_{chat_id}_{msg.message_id}"] = url

# ====================== ПРОВЕРКА ИГР ======================
def check_game_message(chat_id, uid, text):
    # Числовая игра
    game_key = f"n_{chat_id}"
    with game_lock:
        game_data = active_games.get(game_key)
        if game_data and text.strip().isdigit():
            game = game_data["g"]
            guess = int(text.strip())
            
            if guess < 1 or guess > 100:
                safe_send(chat_id, "❌ Число должно быть от 1 до 100!")
                return True
            
            game.attempts += 1
            
            if guess == game.number:
                add_coins(uid, 20, "Победа в игре Число")
                add_xp(uid, 15)
                update_stat(uid, "wins")
                
                with game_lock:
                    active_games.pop(game_key, None)
                
                safe_send(chat_id, f"🎉 **Победа!**\nЗагаданное число: {game.number}\nПопыток: {game.attempts}\n+20 🪙")
                
                achievements = check_achievements(uid)
                notify_achievements(chat_id, uid, achievements)
                return True
            
            elif game.attempts >= game.max_attempts:
                with game_lock:
                    active_games.pop(game_key, None)
                safe_send(chat_id, f"😢 Ты проиграл! Я загадала {game.number}")
                return True
            
            else:
                hint = "больше" if guess < game.number else "меньше"
                safe_send(chat_id, f"❌ Не угадал! Моё число {hint}. Осталось попыток: {game.max_attempts - game.attempts}")
                return True
    
    # Словесная игра
    word_key = f"w_{chat_id}"
    with game_lock:
        game_data = active_games.get(word_key)
        if game_data:
            game = game_data["g"]
            guess = text.strip().lower()
            
            if len(guess) == 1 and guess.isalpha():
                result = game.guess(guess)
                
                if result == "repeat":
                    safe_send(chat_id, "❌ Ты уже называл эту букву!")
                    return True
                
                if game.solved():
                    add_coins(uid, 15, "Победа в Виселице")
                    add_xp(uid, 12)
                    update_stat(uid, "wins")
                    
                    with game_lock:
                        active_games.pop(word_key, None)
                    
                    safe_send(chat_id, f"🎉 **Победа!**\nСлово: {game.word}\n+15 🪙")
                    
                    achievements = check_achievements(uid)
                    notify_achievements(chat_id, uid, achievements)
                    return True
                
                elif game.wrong >= game.max_wrong:
                    with game_lock:
                        active_games.pop(word_key, None)
                    safe_send(chat_id, f"😢 Ты проиграл! Слово было: {game.word}")
                    return True
                
                status = "✅ Верно!" if result == "correct" else "❌ Неверно!"
                safe_send(chat_id, f"{status}\nСлово: {game.display()}\nОшибок: {game.wrong}/{game.max_wrong}")
                return True
            
            elif len(guess) > 1 and guess == game.word:
                add_coins(uid, 20, "Победа в Виселице (слово целиком)")
                add_xp(uid, 15)
                update_stat(uid, "wins")
                
                with game_lock:
                    active_games.pop(word_key, None)
                
                safe_send(chat_id, f"🎉 **Победа!**\nСлово: {game.word}\n+20 🪙")
                
                achievements = check_achievements(uid)
                notify_achievements(chat_id, uid, achievements)
                return True
    
    return False

# ====================== ОБРАБОТКА СООБЩЕНИЙ ======================
@bot.message_handler(content_types=['sticker'])
def on_sticker(message):
    try:
        if not message.from_user:
            return
        
        uid = message.from_user.id
        chat_id = message.chat.id
        
        update_user_info(uid, message.from_user)
        update_stat(uid, "stickers")
        add_coins(uid, STICKER_REWARD, "Стикер")
        add_xp(uid, 1)
        
        if message.chat.type in ("group", "supergroup"):
            remember_user(chat_id, message.from_user)
            last_activity[chat_id] = datetime.now()
        
        chance = 40 if message.chat.type == "private" else 15
        if random.randint(1, 100) <= chance:
            emoji = message.sticker.emoji if message.sticker and message.sticker.emoji else "😊"
            
            response = ask_ai([
                {"role": "system", "content": f"Ты получила стикер с эмодзи {emoji}. Напиши короткую реакцию (1 предложение)."},
                {"role": "user", "content": f"[Стикер {emoji}]"}
            ], max_tokens=60)
            
            if response and not is_error(response):
                resp = clean_text(response)
                if resp and len(resp) < 100:
                    safe_send(chat_id, resp, reply_to=message.message_id)
        
        achievements = check_achievements(uid)
        notify_achievements(chat_id, uid, achievements, message.message_id)
    
    except Exception as e:
        log.error(f"Ошибка обработки стикера: {e}")

@bot.message_handler(content_types=['voice', 'video_note'])
def on_voice(message):
    try:
        if not message.from_user:
            return
        
        uid = message.from_user.id
        chat_id = message.chat.id
        
        update_user_info(uid, message.from_user)
        update_stat(uid, "voice")
        add_coins(uid, VOICE_REWARD, "Голосовое")
        add_xp(uid, 3)
        
        if message.chat.type in ("group", "supergroup"):
            remember_user(chat_id, message.from_user)
        
        bot_info = get_bot_info()
        is_reply_to_bot = (message.reply_to_message and bot_info and
                          message.reply_to_message.from_user and
                          message.reply_to_message.from_user.id == bot_info.id)
        
        chance = 50 if message.chat.type == "private" else 15
        if is_reply_to_bot or random.randint(1, 100) <= chance:
            response = ask_ai([
                {"role": "system", "content": "Тебе отправили голосовое сообщение. Ответь коротко (1 предложение) и мило."},
                {"role": "user", "content": "[Голосовое сообщение]"}
            ], max_tokens=60)
            
            if response and not is_error(response):
                resp = clean_text(response)
                if resp:
                    safe_send(chat_id, resp, reply_to=message.message_id)
        
        achievements = check_achievements(uid)
        notify_achievements(chat_id, uid, achievements, message.message_id)
    
    except Exception as e:
        log.error(f"Ошибка обработки голосового: {e}")

@bot.message_handler(content_types=['photo'])
def on_photo(message):
    try:
        if not message.from_user:
            return
        
        uid = message.from_user.id
        chat_id = message.chat.id
        
        update_user_info(uid, message.from_user)
        add_coins(uid, MESSAGE_REWARD, "Фото")
        add_xp(uid, 2)
        
        if message.chat.type in ("group", "supergroup"):
            remember_user(chat_id, message.from_user)
        
        bot_info = get_bot_info()
        is_reply_to_bot = (message.reply_to_message and bot_info and
                          message.reply_to_message.from_user and
                          message.reply_to_message.from_user.id == bot_info.id)
        
        is_mention = False
        if message.caption and bot_info and bot_info.username:
            is_mention = f"@{bot_info.username.lower()}" in message.caption.lower()
        
        chance = 50 if message.chat.type == "private" else 10
        if is_reply_to_bot or is_mention or random.randint(1, 100) <= chance:
            caption = message.caption or ""
            
            def analyze():
                try:
                    photo = message.photo[-1]
                    file_info = bot.get_file(photo.file_id)
                    file_url = f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_info.file_path}"
                    
                    # Пробуем vision модель
                    response = requests.post(
                        "https://openrouter.ai/api/v1/chat/completions",
                        headers={
                            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                            "Content-Type": "application/json"
                        },
                        json={
                            "model": "google/gemini-2.0-flash-001",  # С vision
                            "messages": [
                                {"role": "system", "content": "Опиши фото коротко (1-2 предложения). Будь милой."},
                                {"role": "user", "content": [
                                    {"type": "text", "text": caption or "Что на фото?"},
                                    {"type": "image_url", "image_url": {"url": file_url}}
                                ]}
                            ],
                            "max_tokens": 100
                        },
                        timeout=30
                    )
                    
                    if response.status_code == 200:
                        resp = response.json().get("choices", [{}])[0].get("message", {}).get("content", "")
                        if resp:
                            safe_send(chat_id, clean_text(resp), reply_to=message.message_id)
                            return
                except Exception:
                    pass
                
                # Fallback на текстовый ответ
                resp = ask_ai([
                    {"role": "system", "content": "Тебе отправили фото. Ответь коротко (1 предложение)."},
                    {"role": "user", "content": caption or "[Фото]"}
                ], max_tokens=60)
                
                if resp and not is_error(resp):
                    safe_send(chat_id, clean_text(resp), reply_to=message.message_id)
            
            add_task(analyze)
    
    except Exception as e:
        log.error(f"Ошибка обработки фото: {e}")

@bot.message_handler(content_types=['animation'])
def on_gif(message):
    try:
        if not message.from_user:
            return
        
        uid = message.from_user.id
        chat_id = message.chat.id
        
        add_coins(uid, 1, "GIF")
        add_xp(uid, 1)
        
        if message.chat.type in ("group", "supergroup"):
            remember_user(chat_id, message.from_user)
        
        chance = 30 if message.chat.type == "private" else 8
        if random.randint(1, 100) <= chance:
            response = ask_ai([
                {"role": "system", "content": "Тебе отправили GIF-анимацию. Ответь коротко (1 предложение)."},
                {"role": "user", "content": "[GIF]"}
            ], max_tokens=60)
            
            if response and not is_error(response):
                resp = clean_text(response)
                if resp:
                    safe_send(chat_id, resp, reply_to=message.message_id)
    
    except Exception:
        pass

@bot.message_handler(content_types=['text'])
def on_text(message):
    try:
        if not message.text or not message.from_user:
            return
        
        uid = message.from_user.id
        chat_id = message.chat.id
        
        update_user_info(uid, message.from_user)
        is_developer(message.from_user)
        
        update_stat(uid, "messages")
        add_coins(uid, MESSAGE_REWARD, "Сообщение")
        xp, level, level_up = add_xp(uid, 2)
        
        if level_up:
            profile = load_profile(uid)
            reward = level * 20
            add_coins(uid, reward, f"Повышение уровня до {level}")
            safe_send(chat_id,
                     f"🌟 {get_display_name(message.from_user)} достиг {level} уровня!\n"
                     f"Титул: {profile.get('title', '')}\n"
                     f"+{reward} {CURRENCY_EMOJI}",
                     reply_to=message.message_id)
        
        # Личные сообщения
        if message.chat.type == "private":
            # Поиск модели
            with user_states_lock:
                is_msearch = user_states.pop(f"msearch_{uid}", None)
                if is_msearch:
                    query = message.text.strip().lower()
                    found = [(k, v) for k, v in AVAILABLE_MODELS.items()
                            if query in k.lower() or query in v["name"].lower() or query in v["id"].lower()]
                    
                    if not found:
                        safe_reply(message, "❌ Моделей не найдено")
                    elif len(found) == 1:
                        global CURRENT_MODEL
                        with model_lock:
                            CURRENT_MODEL = found[0][1]["id"]
                            save_bot_state()
                        info = found[0][1]
                        safe_reply(message,
                                  f"✅ Модель установлена: {info['name']}\n"
                                  f"{'🆓 Бесплатная' if info.get('free') else '💰 Платная'}")
                    else:
                        kb = types.InlineKeyboardMarkup(row_width=1)
                        for key, info in found[:10]:
                            with model_lock:
                                current = CURRENT_MODEL
                            mark = "✅ " if info["id"] == current else ""
                            free = "🆓 " if info.get("free") else "💰 "
                            kb.add(types.InlineKeyboardButton(
                                f"{mark}{free}{info['name']}",
                                callback_data=f"mset_{key}"
                            ))
                        kb.row(types.InlineKeyboardButton("◀️ Назад", callback_data="mcat_back"))
                        safe_reply(message, "🔍 Найденные модели:", markup=kb)
                    return
                
                # Установка титула
                is_ct = user_states.pop(f"ct_{uid}", None)
                if is_ct:
                    if message.text.lower().strip() == "отмена":
                        safe_reply(message, "❌ Отменено", markup=main_keyboard())
                        return
                    
                    title = message.text.strip()[:20]
                    save_profile(uid, {"custom_title": title})
                    safe_reply(message, f"✅ Титул установлен: {title}", markup=main_keyboard())
                    return
                
                # Установка эмодзи
                is_ne = user_states.pop(f"ne_{uid}", None)
                if is_ne:
                    if message.text.lower().strip() == "отмена":
                        safe_reply(message, "❌ Отменено", markup=main_keyboard())
                        return
                    
                    emoji = message.text.strip()[:2]
                    save_profile(uid, {"name_emoji": emoji})
                    safe_reply(message, f"✅ Эмодзи установлено: {emoji}", markup=main_keyboard())
                    return
                
                # Установка промпта для группы
                gid = user_states.pop(f"pp_{uid}", None)
                if gid is not None:
                    if message.text.lower().strip() == "отмена":
                        safe_reply(message, "❌ Отменено")
                        return
                    
                    settings = get_group_settings(gid)
                    with settings_lock:
                        settings["custom_prompt"] = message.text
                        save_settings()
                    
                    safe_reply(message, "✅ Промпт группы обновлён!")
                    return
                
                # Установка правил модерации
                gid = user_states.pop(f"mr_{uid}", None)
                if gid is not None:
                    if message.text.lower().strip() == "отмена":
                        safe_reply(message, "❌ Отменено")
                        return
                    
                    settings = get_group_settings(gid)
                    with settings_lock:
                        settings["mod_rules"] = message.text
                        save_settings()
                    
                    safe_reply(message, "✅ Правила модерации обновлены!")
                    return
            
            # Секретная ссылка
            if uid in secret_links:
                target_gid = secret_links[uid]
                bot.send_chat_action(chat_id, 'typing')
                add_message(target_gid, "user", f"[Секретно от {get_display_name(message.from_user)}]: {message.text}", True)
                
                messages = get_msgs(target_gid, True, uid)
                response = ask_ai(messages, max_tokens=200)
                
                if is_error(response):
                    safe_reply(message, response.replace("[ERR]", "❌ "))
                    return
                
                clean, actions = parse_actions(response)
                clean = clean_text(clean)
                
                if clean:
                    add_message(target_gid, "assistant", clean, True)
                    safe_send(target_gid, clean)
                
                safe_reply(message, f"📨 Отправлено в группу:\n\n{clean}")
                
                if actions:
                    handle_actions(target_gid, actions, True, uid)
                
                return
            
            # Обычное общение в ЛС
            is_busy_flag, busy_type = is_busy(chat_id)
            if is_busy_flag:
                safe_send(chat_id, f"⏳ Я занята {busy_type}", reply_to=message.message_id)
                return
            
            if random.randint(1, 5) == 1:
                change_relation(uid, 1)
            
            bot.send_chat_action(chat_id, 'typing')
            add_message(uid, "user", message.text)
            messages = get_msgs(uid, uid=uid)
            
            # Определяем макс. токенов
            max_tokens = 600 if len(message.text) > 200 else 300
            
            response = ask_ai(messages, max_tokens=max_tokens)
            
            if is_error(response):
                safe_reply(message, response.replace("[ERR]", "❌ "), markup=main_keyboard())
                return
            
            clean, actions = parse_actions(response)
            clean = clean_text(clean)
            
            if clean:
                add_message(uid, "assistant", clean)
                send_long_message(chat_id, clean, markup=main_keyboard(), reply_to=message.message_id)
            
            if actions:
                handle_actions(chat_id, actions, False, uid, message.message_id)
            
            achievements = check_achievements(uid)
            notify_achievements(chat_id, uid, achievements, message.message_id)
            return
        
        # Групповые сообщения
        if message.chat.type not in ("group", "supergroup"):
            return
        
        # Проверяем состояние (установка промпта)
        with user_states_lock:
            state = user_states.pop(f"{chat_id}_{uid}", None)
            if state == "wp":
                if message.text.lower().strip() == "отмена":
                    safe_reply(message, "❌ Отменено")
                    return
                
                if not is_admin(chat_id, uid):
                    return
                
                settings = get_group_settings(chat_id)
                with settings_lock:
                    settings["custom_prompt"] = message.text
                    save_settings()
                
                safe_reply(message, "✅ Промпт группы обновлён!")
                return
            
            if state == "mr":
                if message.text.lower().strip() == "отмена":
                    safe_reply(message, "❌ Отменено")
                    return
                
                if not is_admin(chat_id, uid):
                    return
                
                settings = get_group_settings(chat_id)
                with settings_lock:
                    settings["mod_rules"] = message.text
                    save_settings()
                
                safe_reply(message, "✅ Правила модерации обновлены!")
                return
        
        # Инициализация группы
        settings = get_group_settings(chat_id)
        if settings.get("owner_id") is None:
            with settings_lock:
                settings["owner_id"] = uid
                settings["owner_name"] = get_display_name(message.from_user)
                settings["group_name"] = message.chat.title
                save_settings()
            register_group(uid, chat_id, message.chat.title)
        
        # Антиспам
        if settings.get("antispam") and not is_developer(message.from_user) and not is_admin(chat_id, uid):
            is_spam, mute_time = check_spam(chat_id, uid)
            if is_spam:
                safe_delete(chat_id, message.message_id)
                safe_send(chat_id,
                         f"🛡️ {get_display_name(message.from_user)}, антиспам! Мут на {int(mute_time // 60)} мин.")
                return
        
        # Проверка игр
        if check_game_message(chat_id, uid, message.text):
            return
        
        remember_user(chat_id, message.from_user)
        add_message(chat_id, "user", f"[{get_display_name(message.from_user)}]: {message.text}", True)
        last_activity[chat_id] = datetime.now()
        
        if settings.get("proactive_enabled"):
            start_proactive_timer(chat_id)
        
        bot_info = get_bot_info()
        bot_username = bot_info.username.lower() if bot_info and bot_info.username else ""
        
        is_reply_to_bot = (message.reply_to_message and bot_info and
                          message.reply_to_message.from_user and
                          message.reply_to_message.from_user.id == bot_info.id)
        
        is_mention = bot_username and f"@{bot_username}" in message.text.lower()
        is_direct = is_reply_to_bot or is_mention or any(nick in message.text.lower() for nick in BOT_NICKNAMES)
        
        if not is_direct:
            is_busy_flag, _ = is_busy(chat_id)
            if is_busy_flag or random.randint(1, 100) > settings["response_chance"]:
                achievements = check_achievements(uid)
                notify_achievements(chat_id, uid, achievements)
                return
        
        is_busy_flag, busy_type = is_busy(chat_id)
        if is_busy_flag:
            if is_direct:
                safe_send(chat_id, f"⏳ Я занята {busy_type}", reply_to=message.message_id)
            return
        
        if random.randint(1, 8) == 1:
            change_relation(uid, 1)
        
        bot.send_chat_action(chat_id, 'typing')
        messages = get_msgs(chat_id, True, uid)
        
        # Определяем макс. токенов (в группах короче)
        max_tokens = 200
        
        response = ask_ai(messages, max_tokens=max_tokens)
        
        if is_error(response):
            send_long_message(chat_id, response.replace("[ERR]", "❌ "), reply_to=message.message_id)
            return
        
        clean, actions = parse_actions(response)
        clean = clean_text(clean)
        
        if clean:
            add_message(chat_id, "assistant", clean, True)
            send_long_message(chat_id, clean, reply_to=message.message_id)
        
        if actions:
            handle_actions(chat_id, actions, True, uid, message.message_id)
        
        achievements = check_achievements(uid)
        notify_achievements(chat_id, uid, achievements, message.message_id)
    
    except Exception as e:
        log.error(f"Ошибка обработки текста: {e}")
        traceback.print_exc()

# ====================== ОЧИСТКА ======================
def cleanup_loop():
    while True:
        try:
            time.sleep(CLEANUP_INTERVAL)
            now = time.time()
            
            # Очистка временных файлов
            if os.path.exists(DOWNLOADS_DIR):
                for item in os.listdir(DOWNLOADS_DIR):
                    path = os.path.join(DOWNLOADS_DIR, item)
                    try:
                        if os.path.isdir(path) and now - os.path.getmtime(path) > 1800:
                            shutil.rmtree(path, ignore_errors=True)
                    except Exception:
                        pass
            
            # Очистка pending треков
            cleanup_pending()
            
            # Очистка старых сессий
            cleaned = cleanup_old_sessions()
            if cleaned:
                log.info(f"Очищено {cleaned} старых сессий")
            
            # Очистка старых запросов модерации
            with mod_lock:
                for key in [k for k, v in pending_mod_actions.items()
                           if v.get("time") and
                           (datetime.now() - v["time"]).total_seconds() > 600]:
                    pending_mod_actions.pop(key, None)
            
            # Очистка старых игр
            with game_lock:
                for key in [k for k, v in active_games.items()
                           if v.get("time") and
                           (datetime.now() - v["time"]).total_seconds() > 3600]:
                    active_games.pop(key, None)
            
            # Очистка спам-трекера
            with spam_lock:
                for key in [k for k, v in spam_tracker.items()
                           if not v.get("times") and time.time() > v.get("muted_until", 0) + 300]:
                    spam_tracker.pop(key, None)
            
            save_bot_state()
        
        except Exception as e:
            log.error(f"Ошибка в очистке: {e}")

# ====================== ЗАПУСК ======================
if __name__ == "__main__":
    print("=" * 50)
    print("🌸 Хината v3.5 (Исправленная)")
    print("=" * 50)
    
    bot_info = get_bot_info()
    if bot_info:
        log.info(f"Бот: @{bot_info.username}")
    
    log.info(f"FFmpeg: {'✅ Доступен' if FFMPEG_AVAILABLE else '❌ Не найден'}")
    
    with model_lock:
        log.info(f"Текущая модель: {CURRENT_MODEL}")
    
    log.info(f"Групп в настройках: {len(group_settings)}")
    log.info(f"Товаров в магазине: {len(SHOP_ITEMS)}")
    log.info(f"Достижений: {len(ACHIEVEMENTS)}")
    
    free_count = sum(1 for m in AVAILABLE_MODELS.values() if m.get("free"))
    paid_count = sum(1 for m in AVAILABLE_MODELS.values() if not m.get("free"))
    log.info(f"Доступных моделей: {len(AVAILABLE_MODELS)} (🆓 {free_count} / 💰 {paid_count})")
    log.info(f"База данных: SQLite ({DB_FILE})")
    
    _bot_state["restarts"] = _bot_state.get("restarts", 0) + 1
    save_bot_state()
    log.info(f"Перезапусков: {_bot_state['restarts']}")
    
    # Восстанавливаем таймеры для групп
    for chat_key, settings in group_settings.items():
        try:
            chat_id = int(chat_key)
            if settings.get("owner_id"):
                register_group(settings["owner_id"], chat_id, settings.get("group_name", "?"))
            if settings.get("proactive_enabled"):
                start_proactive_timer(chat_id)
        except Exception:
            pass
    
    profile_count = db_execute("SELECT COUNT(*) FROM profiles", fetch=True)
    profile_count = profile_count[0][0] if profile_count else 0
    log.info(f"Профилей в БД: {profile_count}")
    
    # Запускаем поток очистки
    threading.Thread(target=cleanup_loop, daemon=True).start()
    
    print("🌸 Бот запущен! Нажми Ctrl+C для остановки")
    print("=" * 50)
    
    # Основной цикл
    while True:
        try:
            bot.infinity_polling(
                allowed_updates=["message", "callback_query", "my_chat_member"],
                timeout=60,
                long_polling_timeout=60
            )
        except KeyboardInterrupt:
            save_bot_state()
            print("\n🌸 Бот остановлен")
            break
        except Exception as e:
            log.error(f"Ошибка polling: {e}")
            save_bot_state()
            time.sleep(5)
