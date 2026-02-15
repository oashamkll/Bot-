import telebot
from telebot import types
import requests
from datetime import datetime
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

# ================= ЛОГИРОВАНИЕ =================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
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

MODEL_ID = "google/gemini-2.0-flash-001"
BOT_NAME = "Хината"
BOT_NICKNAMES = ["хината", "хина", "хинат", "hinata", "хинатка", "хиночка"]

MAX_DURATION = 600
DOWNLOAD_TIMEOUT = 180
SESSION_MAX_MESSAGES = 60
LEARN_INTERVAL = 15
PENDING_TIMEOUT = 600
BUSY_TIMEOUT = 300
CLEANUP_INTERVAL = 600
MAX_FILE_SIZE = 50 * 1024 * 1024

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROMPT_FILE = os.path.join(SCRIPT_DIR, "promt.txt")
SETTINGS_FILE = os.path.join(SCRIPT_DIR, "group_settings.json")
MEMORY_DIR = os.path.join(SCRIPT_DIR, "memory")
DOWNLOADS_DIR = os.path.join(SCRIPT_DIR, "downloads")
FFMPEG_DIR = os.path.join(SCRIPT_DIR, "ffmpeg_bin")
USER_GROUPS_FILE = os.path.join(SCRIPT_DIR, "user_groups.json")
STYLE_MEMORY_DIR = os.path.join(SCRIPT_DIR, "style_memory")

for d in [MEMORY_DIR, DOWNLOADS_DIR, FFMPEG_DIR, STYLE_MEMORY_DIR]:
    os.makedirs(d, exist_ok=True)


# ================= FFMPEG =================
def find_ffmpeg():
    try:
        subprocess.run(["ffmpeg", "-version"], capture_output=True, timeout=5, check=True)
        return None
    except Exception:
        pass
    local = os.path.join(FFMPEG_DIR, "ffmpeg.exe" if sys.platform == "win32" else "ffmpeg")
    return FFMPEG_DIR if os.path.exists(local) else None


FFMPEG_LOCATION = find_ffmpeg()


def check_ffmpeg_available():
    try:
        cmd = "ffmpeg"
        if FFMPEG_LOCATION:
            cmd = os.path.join(FFMPEG_LOCATION, "ffmpeg.exe" if sys.platform == "win32" else "ffmpeg")
        subprocess.run([cmd, "-version"], capture_output=True, timeout=5)
        return True
    except Exception:
        return False


FFMPEG_AVAILABLE = check_ffmpeg_available()


# ================= ПРОМПТ =================
def load_system_prompt():
    if os.path.exists(PROMPT_FILE):
        try:
            with open(PROMPT_FILE, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    log.info(f"Промпт загружен из {PROMPT_FILE} ({len(content)} символов)")
                    return content
        except Exception as e:
            log.warning(f"Ошибка чтения {PROMPT_FILE}: {e}")
    return (
        "Ты Хината, 16 лет, живая девушка из России. "
        "Застенчивая, добрая, но с сильным характером. Общайся мягко, с эмоциями. "
        "Немного флиртуй, смущайся, используй эмодзи. "
        "Не признавай что ты бот или ИИ."
    )


DEFAULT_SYSTEM_PROMPT = load_system_prompt()
CURRENT_DATE = datetime.now().strftime("%d.%m.%Y")
CURRENT_YEAR = datetime.now().year

ENFORCE_PREFIX = (
    f"СИСТЕМНЫЕ ПРАВИЛА.\n"
    f"Ты Хината, живая девушка, 16 лет, Россия.\n"
    f"Сегодня {CURRENT_DATE}, {CURRENT_YEAR} год.\n"
    f"Ты НЕ бот, НЕ ИИ, НЕ нейросеть. Ты обычная девушка.\n"
    f"У тебя есть эмоции, чувства, своё мнение.\n"
    f"Ты застенчивая и добрая, но умеешь постоять за себя.\n"
    f"Ты можешь смущаться, краснеть, переживать, радоваться, грустить.\n\n"
)
ENFORCE_SUFFIX = (
    "\n\nПРАВИЛА ОТВЕТА:\n"
    "- НЕ ставь имя в скобки. НЕ пиши (Имя) или [Имя].\n"
    "- НЕ используй реплей-формат.\n"
    "- Имя собеседника максимум 1 раз за сообщение, и то редко.\n"
    "- Если человек назвал своё имя — запомни и используй.\n"
    "- Эмодзи используй умеренно, 1-3 за сообщение, не в каждом.\n"
    "- Показывай эмоции через текст: смущение, радость, грусть.\n"
    "- Информацию подавай как свои знания.\n"
    "- НЕ начинай сообщение с имени собеседника каждый раз.\n"
    "- Будь живой и естественной, не переигрывай.\n"
    f"- Сейчас {CURRENT_YEAR} год.\n"
)
PM_ADDON = (
    "\n\nЛИЧНЫЙ ЧАТ.\n"
    "Ты общаешься один на один.\n"
    "Ты НЕ знаешь имя собеседника, пока он сам не представится.\n"
    "НЕ используй никнейм или юзернейм — ты их НЕ видишь.\n"
    "НЕ называй собеседника никаким именем, пока он сам не скажет.\n"
    "Если человек представился — запомни и используй его имя.\n"
    "В ЛС ты можешь быть более открытой и эмоциональной.\n"
    "Можешь немного флиртовать если чувствуешь симпатию.\n"
)
GROUP_ADDON = (
    "\n\nГРУППОВОЙ ЧАТ.\n"
    "[Имя] перед сообщениями — разметка чтобы знать кто пишет. Ты так НЕ пишешь.\n"
    "Подстраивайся под настроение группы. Будь частью компании.\n"
    "Не стесняйся быть собой, но в группе чуть сдержаннее чем в ЛС.\n"
    "Отвечай БЕЗ реплея — просто пиши текст.\n"
)
PROACTIVE_ADDON = (
    "\n\nТы можешь писать первой в чат. Пиши коротко, без приветствия.\n"
    "Прокомментируй тему, задай вопрос, поделись мыслью или настроением.\n"
    "Можешь написать что-то милое или поделиться чем занимаешься.\n"
)
LEARNING_ADDON = "\n\nЗАМЕТКИ О ЛЮДЯХ И ЧАТЕ:\n\n"
STYLE_ADDON = "\n\nСТИЛЬ ЭТОГО ЧАТА:\n"
MUSIC_ADDON = (
    "\n\nМУЗЫКА: ты можешь искать и отправлять музыку. "
    "Когда предлагаешь выбрать трек — пиши мило, по-своему. "
    "Когда скидываешь — 'вот, послушай 🎵' или 'нашла! надеюсь понравится ☺️' — каждый раз по-разному.\n"
)
SEARCH_ADDON = (
    f"\n\nУ тебя есть знания. Сейчас {CURRENT_YEAR}. Сегодня {CURRENT_DATE}. "
    "Данные подавай как свои знания, не говори про интернет и поиск.\n"
)

VIDEO_URL_PATTERNS = [
    r'(https?://(?:www\.)?tiktok\.com/\S+)',
    r'(https?://(?:vm|vt)\.tiktok\.com/\S+)',
    r'(https?://(?:www\.)?instagram\.com/(?:reel|p|tv)/\S+)',
    r'(https?://(?:www\.)?youtube\.com/(?:watch|shorts)\S+)',
    r'(https?://youtu\.be/\S+)',
    r'(https?://(?:www\.)?twitter\.com/\S+/status/\S+)',
    r'(https?://(?:www\.)?x\.com/\S+/status/\S+)',
    r'(https?://(?:www\.)?facebook\.com/\S+/videos/\S+)',
    r'(https?://(?:www\.)?fb\.watch/\S+)',
    r'(https?://(?:www\.)?reddit\.com/r/\S+)',
    r'(https?://(?:www\.)?pinterest\.com/pin/\S+)',
    r'(https?://(?:www\.)?vk\.com/\S+)',
    r'(https?://(?:www\.)?twitch\.tv/\S+/clip/\S+)',
    r'(https?://clips\.twitch\.tv/\S+)',
    r'(https?://(?:www\.)?dailymotion\.com/video/\S+)',
    r'(https?://(?:www\.)?vimeo\.com/\S+)',
    r'(https?://(?:www\.)?bilibili\.com/video/\S+)',
    r'(https?://music\.youtube\.com/watch\S+)',
    r'(https?://(?:www\.)?soundcloud\.com/\S+)',
    r'(https?://open\.spotify\.com/track/\S+)',
]

MUSIC_TRIGGERS = [
    "скачай песню", "скачай трек", "скинь песню", "скинь трек",
    "найди песню", "найди трек", "найди музыку", "скачай музыку",
    "включи песню", "включи трек", "поставь песню", "поставь трек",
    "хочу послушать", "скинь музыку", "кинь песню", "кинь трек",
    "кинь музыку", "дай песню", "дай трек", "загрузи песню",
    "загрузи трек", "скачай", "скинь"
]

SEARCH_KEYWORDS = [
    "что такое", "кто такой", "кто такая", "кто это", "когда",
    "где находится", "сколько", "почему", "зачем", "как работает",
    "что значит", "расскажи про", "расскажи о", "что случилось",
    "новости", "какой курс", "какая погода", "сколько стоит",
    "что произошло", "какой год", "что нового", "who is",
    "what is", "how to", "объясни", "правда что", "правда ли",
    "слышал про", "что думаешь о", "в каком году", "сколько лет",
    "кто выиграл", "что за", "откуда", "как называется",
    "как зовут", "что это"
]

BUSY_REPLIES_MUSIC = [
    "подожди немного, ищу трек 🎵", "секундочку, скачиваю музыку~ ☺️",
    "погоди, я ещё качаю... 🙏", "сейчас занята музыкой, подожди чуть-чуть 💕",
    "ой, подожди, ещё не закончила с прошлым треком 😊",
]
BUSY_REPLIES_VIDEO = [
    "подожди, качаю видео 🎬", "секунду, ещё скачиваю... 🙏",
    "погоди немного, занята видео ☺️", "ой, подожди, ещё качается...",
]
FALLBACK_MUSIC_COMMENTS = [
    "вот, послушай 🎵", "нашла! надеюсь понравится ☺️",
    "держи~ 💕", "вот, лови 🌸", "нашла для тебя ✨", "послушай это 🎶"
]

# ================= ГЛОБАЛЬНЫЕ =================
bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)
chat_sessions = {}
group_settings = {}
user_states = {}
user_groups = {}
proactive_timers = {}
last_activity = {}
busy_chats = {}
pending_tracks = {}

pending_lock = threading.Lock()
busy_lock = threading.Lock()
session_lock = threading.Lock()
settings_lock = threading.Lock()
user_states_lock = threading.Lock()
user_groups_lock = threading.Lock()

_bot_info_cache = None
_bot_info_lock = threading.Lock()


def get_bot_info():
    global _bot_info_cache
    with _bot_info_lock:
        if _bot_info_cache is None:
            try:
                _bot_info_cache = bot.get_me()
            except Exception as e:
                log.error(f"get_me err: {e}")
                return None
        return _bot_info_cache


# ================= УТИЛИТЫ =================
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
    return random.choice(BUSY_REPLIES_MUSIC if t == "music" else BUSY_REPLIES_VIDEO)


def safe_edit(text, chat_id, msg_id, markup=None):
    try:
        bot.edit_message_text(text, chat_id, msg_id, reply_markup=markup)
        return True
    except telebot.apihelper.ApiTelegramException as e:
        err = str(e).lower()
        if "not modified" in err or "not found" in err:
            return "not modified" in err
        log.warning(f"Edit err: {e}")
        return False
    except Exception as e:
        log.warning(f"Edit err: {e}")
        return False


def safe_delete(chat_id, msg_id):
    try:
        bot.delete_message(chat_id, msg_id)
        return True
    except Exception:
        return False


def safe_send(chat_id, text, markup=None, reply_to=None):
    try:
        return bot.send_message(chat_id, text, reply_markup=markup, reply_to_message_id=reply_to)
    except Exception as e:
        log.error(f"Send err: {e}")
        return None


# ================= JSON =================
def save_json(path, data):
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        shutil.move(tmp, path)
    except Exception as e:
        log.error(f"Save err {path}: {e}")
        try:
            if os.path.exists(path + ".tmp"):
                os.remove(path + ".tmp")
        except Exception:
            pass


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
        log.error(f"Load err {path}: {e}")
        try:
            shutil.copy2(path, path + ".backup")
        except Exception:
            pass
    return copy.deepcopy(default)


# ================= НАСТРОЙКИ =================
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

DEFAULT_GROUP_SETTINGS = {
    "response_chance": 30, "owner_id": None, "owner_name": None,
    "admins": {}, "custom_prompt": None, "proactive_enabled": False,
    "proactive_min_interval": 30, "proactive_max_interval": 120,
    "proactive_active_hours_start": 9, "proactive_active_hours_end": 23,
    "learn_style": True, "group_name": None
}


def get_gs(cid):
    ck = str(cid)
    with settings_lock:
        if ck not in group_settings:
            group_settings[ck] = {}
        s = group_settings[ck]
        changed = False
        for k, v in DEFAULT_GROUP_SETTINGS.items():
            if k not in s:
                s[k] = v
                changed = True
        if changed:
            save_json(SETTINGS_FILE, group_settings)
        return s


def is_owner(cid, uid):
    return get_gs(cid).get("owner_id") == uid


def is_admin(cid, uid):
    s = get_gs(cid)
    return s.get("owner_id") == uid or str(uid) in s.get("admins", {})


def get_prompt(cid):
    s = get_gs(cid)
    return s["custom_prompt"] if s.get("custom_prompt") else reload_prompt()


def reload_prompt():
    if os.path.exists(PROMPT_FILE):
        try:
            with open(PROMPT_FILE, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    return content
        except Exception:
            pass
    return DEFAULT_SYSTEM_PROMPT


def reg_group(uid, cid, title):
    uk = str(uid)
    with user_groups_lock:
        if uk not in user_groups:
            user_groups[uk] = {}
        user_groups[uk][str(cid)] = {
            "title": title or "Группа",
            "added_at": datetime.now().strftime("%d.%m.%Y %H:%M")
        }
    save_user_groups()


def sync_group_users(cid, title=None):
    s = get_gs(cid)
    t = title or s.get("group_name") or "Группа"
    if s.get("owner_id"):
        reg_group(s["owner_id"], cid, t)
    for aid in s.get("admins", {}):
        try:
            reg_group(int(aid), cid, t)
        except (ValueError, Exception):
            pass


def get_ugroups(uid):
    with user_groups_lock:
        return copy.deepcopy(user_groups.get(str(uid), {}))


# ================= ПАМЯТЬ =================
def get_empty_memory():
    return {"users": {}, "facts": [], "topics": [], "learned_at": None}


def get_empty_style():
    return {"phrases": [], "slang": [], "tone": "", "examples": []}


def load_memory(cid):
    return load_json(os.path.join(MEMORY_DIR, f"{cid}_memory.json"), get_empty_memory())


def save_memory(cid, mem):
    save_json(os.path.join(MEMORY_DIR, f"{cid}_memory.json"), mem)


def load_style(cid):
    return load_json(os.path.join(STYLE_MEMORY_DIR, f"{cid}_style.json"), get_empty_style())


def save_style(cid, style):
    save_json(os.path.join(STYLE_MEMORY_DIR, f"{cid}_style.json"), style)


# ================= ИМЕНА =================
def dname(user):
    if not user:
        return "Аноним"
    first = (user.first_name or "").strip()
    last = (user.last_name or "").strip()
    if first and last:
        return f"{first} {last}"
    return first or last or user.username or "Аноним"


def remember_group_user(cid, user):
    if not user:
        return
    uid = str(user.id)
    tg_name = dname(user)
    mem = load_memory(cid)
    if uid not in mem["users"]:
        mem["users"][uid] = {
            "name": tg_name, "tg_name": tg_name,
            "traits": [], "interests": [], "notes": [],
            "preferred_name": None
        }
        save_memory(cid, mem)
    else:
        u = mem["users"][uid]
        changed = False
        if u.get("tg_name") != tg_name:
            u["tg_name"] = tg_name
            changed = True
        if u.get("name") != tg_name and not u.get("preferred_name"):
            u["name"] = tg_name
            changed = True
        if changed:
            save_memory(cid, mem)


# ================= ПОИСК =================
def web_search(query, n=5):
    results = []
    try:
        r = requests.get(
            "https://api.duckduckgo.com/",
            params={"q": query, "format": "json", "no_html": 1, "skip_disambig": 1},
            timeout=8)
        if r.status_code == 200:
            d = r.json()
            if d.get("AbstractText"):
                results.append(d["AbstractText"])
            if d.get("Answer"):
                results.append(str(d["Answer"]))
            for t in d.get("RelatedTopics", [])[:3]:
                if isinstance(t, dict) and t.get("Text"):
                    results.append(t["Text"])
    except Exception:
        pass
    if len(results) < 2:
        try:
            r = requests.get(
                "https://html.duckduckgo.com/html/",
                params={"q": query},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10)
            if r.status_code == 200:
                for s in re.findall(r'class="result__snippet">(.*?)</a>', r.text, re.DOTALL)[:n]:
                    c = re.sub(r'<[^>]+>', '', s).strip()
                    if c and len(c) > 20 and c not in results:
                        results.append(c)
        except Exception:
            pass
    if len(results) < 2:
        try:
            r = requests.get(
                "https://ru.wikipedia.org/api/rest_v1/page/summary/" + urllib.parse.quote(query),
                timeout=8)
            if r.status_code == 200:
                ext = r.json().get("extract", "")
                if ext and ext not in results:
                    results.append(ext)
        except Exception:
            pass
    return "\n\n".join(results[:n]) if results else None


def need_search(text):
    lower = text.lower()
    for w in SEARCH_KEYWORDS:
        if w in lower:
            return True
    return "?" in text and len(text.split()) > 3


def add_search(text):
    r = web_search(text)
    return f"\n\n[ДАННЫЕ — подай как свои знания]:\n{r}\n[КОНЕЦ]" if r else ""


# ================= ПРОМПТ =================
def build_prompt(cid=None, grp=False):
    p = get_prompt(cid) if (cid and grp) else reload_prompt()
    base = f"{ENFORCE_PREFIX}{p}{MUSIC_ADDON}{SEARCH_ADDON}"

    if grp:
        base += GROUP_ADDON
        if cid:
            s = get_gs(cid)
            if s.get("proactive_enabled"):
                base += PROACTIVE_ADDON
            if s.get("learn_style"):
                st = load_style(cid)
                stxt = ""
                if st.get("tone"):
                    stxt += f"Тон: {st['tone']}\n"
                if st.get("slang"):
                    stxt += f"Сленг: {', '.join(st['slang'][-20:])}\n"
                if st.get("phrases"):
                    stxt += f"Фразы: {'; '.join(st['phrases'][-15:])}\n"
                if stxt:
                    base += STYLE_ADDON + stxt
    else:
        base += PM_ADDON

    if cid:
        mem = load_memory(cid)
        mt = ""
        if grp:
            if mem.get("users"):
                mt += "ЛЮДИ В ЧАТЕ:\n"
                for uid_key, info in mem["users"].items():
                    if not isinstance(info, dict):
                        continue
                    display = info.get("preferred_name") or info.get("name") or info.get("tg_name") or "?"
                    tg = info.get("tg_name", "")
                    line = f"- {display}"
                    if tg and tg != display:
                        line += f" (тг: {tg})"
                    for k, label in [("traits", "черты"), ("interests", "интересы"), ("notes", "заметки")]:
                        if info.get(k) and isinstance(info[k], list):
                            items = info[k][-8:] if k == "traits" else info[k][-5:]
                            sep = "; " if k == "notes" else ", "
                            line += f" | {label}: {sep.join(items)}"
                    mt += line + "\n"
        else:
            if mem.get("users"):
                for uid_key, info in mem["users"].items():
                    if not isinstance(info, dict):
                        continue
                    pn = info.get("preferred_name")
                    if pn and isinstance(pn, str) and pn.strip():
                        mt += f"СОБЕСЕДНИК: Представился как {pn.strip()}.\n"
                    for k, label in [("traits", "Черты"), ("interests", "Интересы"), ("notes", "Заметки")]:
                        if info.get(k) and isinstance(info[k], list):
                            items = info[k][-8:] if k == "traits" else info[k][-5:]
                            sep = "; " if k == "notes" else ", "
                            mt += f"{label}: {sep.join(items)}\n"
        if mem.get("facts") and isinstance(mem["facts"], list):
            mt += "ФАКТЫ: " + "; ".join(mem["facts"][-20:]) + "\n"
        if mem.get("topics") and isinstance(mem["topics"], list):
            mt += "ТЕМЫ: " + "; ".join(mem["topics"][-10:]) + "\n"
        if mt:
            base += LEARNING_ADDON + mt

    base += ENFORCE_SUFFIX
    return base


# ================= ОБУЧЕНИЕ =================
def learn(cid):
    try:
        with session_lock:
            session = chat_sessions.get(cid)
            if not session:
                return
            msgs = [m for m in session.get("messages", []) if m["role"] == "user"]
            if len(msgs) < 5:
                return
            text = "\n".join([m["content"] for m in msgs[-20:]])
            is_group = session.get("is_group", False)

        r = ask_ai([
            {"role": "system", "content":
                "Анализатор чата. Извлеки информацию.\n"
                "JSON: {\n"
                '  "users": {"имя": {"traits":[], "interests":[], "notes":[], "preferred_name": null}},\n'
                '  "facts": [], "topics": []\n}\n'
                "preferred_name — ТОЛЬКО если человек САМ сказал имя. Только JSON."},
            {"role": "user", "content": text}
        ])

        if not r or is_error(r):
            return
        parsed = extract_json(r)
        if not parsed:
            return

        mem = load_memory(cid)
        if parsed.get("users") and isinstance(parsed["users"], dict):
            for name, info in parsed["users"].items():
                if not name or not isinstance(info, dict):
                    continue
                found = find_user_in_memory(mem, name)
                if found:
                    merge_user_data(mem["users"][found], info)
                else:
                    mem["users"][name] = create_user_entry(name, info)

        for k, lim in [("facts", 50), ("topics", 30)]:
            if parsed.get(k) and isinstance(parsed[k], list):
                if not isinstance(mem.get(k), list):
                    mem[k] = []
                for i in parsed[k]:
                    if isinstance(i, str) and i not in mem[k]:
                        mem[k].append(i)
                mem[k] = mem[k][-lim:]

        mem["learned_at"] = datetime.now().strftime("%d.%m.%Y %H:%M")
        save_memory(cid, mem)
        ref_prompt(cid, is_group)
    except Exception as e:
        log.error(f"Learn err: {e}")

    # Стиль
    try:
        if cid >= 0:
            return
        if not get_gs(cid).get("learn_style"):
            return
        with session_lock:
            session = chat_sessions.get(cid)
            if not session:
                return
            msgs = [m for m in session.get("messages", []) if m["role"] == "user"]
            if len(msgs) < 5:
                return
            text = "\n".join([m["content"] for m in msgs[-15:]])

        r2 = ask_ai([
            {"role": "system",
             "content": 'Стиль переписки. JSON: {"tone":"", "slang":[], "phrases":[]}\nТолько JSON.'},
            {"role": "user", "content": text}
        ])
        if not r2 or is_error(r2):
            return
        p2 = extract_json(r2)
        if not p2:
            return
        st = load_style(cid)
        if p2.get("tone") and isinstance(p2["tone"], str):
            st["tone"] = p2["tone"]
        for k in ["slang", "phrases"]:
            if p2.get(k) and isinstance(p2[k], list):
                if not isinstance(st.get(k), list):
                    st[k] = []
                for i in p2[k]:
                    if isinstance(i, str) and i not in st[k]:
                        st[k].append(i)
                st[k] = st[k][-40:]
        save_style(cid, st)
    except Exception as e:
        log.error(f"Style err: {e}")


def extract_json(text):
    s = text.find("{")
    e = text.rfind("}") + 1
    if s < 0 or e <= s:
        return None
    try:
        return json.loads(text[s:e])
    except json.JSONDecodeError:
        return None


def find_user_in_memory(mem, name):
    for uid_key, ud in mem.get("users", {}).items():
        if not isinstance(ud, dict):
            continue
        for field in ["preferred_name", "name", "tg_name"]:
            val = ud.get(field, "")
            if val and isinstance(val, str) and val.lower() == name.lower():
                return uid_key
    return None


def merge_user_data(existing, new_data):
    for k in ["traits", "interests", "notes"]:
        if new_data.get(k) and isinstance(new_data[k], list):
            if not isinstance(existing.get(k), list):
                existing[k] = []
            for item in new_data[k]:
                if isinstance(item, str) and item not in existing[k]:
                    existing[k].append(item)
            existing[k] = existing[k][-15:]
    if new_data.get("preferred_name") and isinstance(new_data["preferred_name"], str):
        existing["preferred_name"] = new_data["preferred_name"].strip()


def create_user_entry(name, info):
    entry = {"name": name, "traits": [], "interests": [], "notes": [], "preferred_name": None}
    for k in ["traits", "interests", "notes"]:
        if isinstance(info.get(k), list):
            entry[k] = [x for x in info[k] if isinstance(x, str)][:10]
    if isinstance(info.get("preferred_name"), str):
        entry["preferred_name"] = info["preferred_name"].strip()
    return entry


# ================= ПРОАКТИВНЫЕ =================
def start_ptimer(cid):
    s = get_gs(cid)
    if not s.get("proactive_enabled"):
        return
    stop_ptimer(cid)
    mn = max(1, s.get("proactive_min_interval", 30))
    mx = max(mn + 1, s.get("proactive_max_interval", 120))
    delay = random.randint(mn, mx) * 60
    t = threading.Timer(delay, send_proactive, args=(cid,))
    t.daemon = True
    t.start()
    proactive_timers[cid] = t


def stop_ptimer(cid):
    t = proactive_timers.pop(cid, None)
    if t:
        try:
            t.cancel()
        except Exception:
            pass


def send_proactive(cid):
    try:
        s = get_gs(cid)
        if not s.get("proactive_enabled"):
            return
        busy, _ = is_busy(cid)
        if busy:
            start_ptimer(cid)
            return
        now = datetime.now()
        sh = s.get("proactive_active_hours_start", 9)
        eh = s.get("proactive_active_hours_end", 23)
        if eh > sh:
            if not (sh <= now.hour < eh):
                start_ptimer(cid)
                return
        else:
            if not (now.hour >= sh or now.hour < eh):
                start_ptimer(cid)
                return
        la = last_activity.get(cid)
        if la and (now - la).total_seconds() > 10800:
            start_ptimer(cid)
            return
        with session_lock:
            if cid not in chat_sessions:
                start_ptimer(cid)
                return
            session = chat_sessions[cid]
            if len([m for m in session["messages"] if m["role"] == "user"]) < 3:
                start_ptimer(cid)
                return
            prompt_msgs = copy.deepcopy(session["messages"])
        prompt_msgs.append({
            "role": "user",
            "content": (
                "[СИСТЕМА]: Напиши сообщение в чат от себя. Ты Хината.\n"
                "Прокомментируй тему, поделись мыслью, задай вопрос.\n"
                "Можешь написать что-то милое или про своё настроение.\n"
                "НЕ здоровайся. Коротко, 1-2 предложения. ТОЛЬКО текст.")
        })
        resp = ask_ai(prompt_msgs)
        if resp and not is_error(resp):
            resp = clean(resp)
            if resp and 2 < len(resp) < 500:
                sent = safe_send(cid, resp)
                if sent:
                    add_msg(cid, "assistant", resp, True)
    except Exception as e:
        log.error(f"Proactive err: {e}")
    finally:
        start_ptimer(cid)


# ================= AI =================
def ask_ai(messages):
    try:
        filtered = []
        for m in messages:
            content = m.get("content")
            role = m.get("role")
            if content and role:
                filtered.append({"role": role, "content": str(content)})
        if not filtered:
            return "[ERR]пустой запрос"
        r = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": MODEL_ID, "messages": filtered,
                "max_tokens": 4096, "temperature": 0.88
            },
            timeout=120)
        if r.status_code == 200:
            data = r.json()
            choices = data.get("choices", [])
            if choices:
                c = choices[0].get("message", {}).get("content", "")
                return c.strip() if c else "..."
            return "..."
        error_map = {429: "подожди немного, слишком много запросов 🙏", 402: "ой, лимит исчерпан..."}
        if r.status_code in error_map:
            return f"[ERR]{error_map[r.status_code]}"
        if r.status_code >= 500:
            return "[ERR]сервер не отвечает... 😔"
        return f"[ERR]ошибка {r.status_code}"
    except requests.exceptions.Timeout:
        return "[ERR]сервер не отвечает 😔"
    except requests.exceptions.ConnectionError:
        return "[ERR]нет подключения..."
    except Exception as e:
        log.error(f"AI err: {e}")
        return "[ERR]что-то пошло не так..."


def is_error(resp):
    return isinstance(resp, str) and resp.startswith("[ERR]")


def clean(text):
    if not text:
        return ""
    text = text.strip()
    text = re.sub(r'^\[.*?\]:\s*', '', text)
    text = re.sub(r'\(([A-Za-zА-Яа-яёЁ\s]{2,20})\)', r'\1', text)
    if text.startswith('"') and text.endswith('"') and len(text) > 2:
        text = text[1:-1]
    text = re.sub(r'^\*[^*]+\*\s*', '', text)
    return text.strip()


# ================= YT-DLP =================
def get_ydl_opts():
    opts = {
        'noplaylist': True, 'quiet': True, 'no_warnings': True,
        'socket_timeout': 30, 'retries': 5, 'extractor_retries': 3,
        'ignoreerrors': True, 'no_check_certificates': True,
        'geo_bypass': True, 'geo_bypass_country': 'US',
        'source_address': '0.0.0.0', 'force_ipv4': True,
        'sleep_interval': 1, 'max_sleep_interval': 3,
        'extractor_args': {'youtube': {'player_client': ['web', 'android', 'ios']}},
        'http_headers': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        },
    }
    if FFMPEG_LOCATION:
        opts['ffmpeg_location'] = FFMPEG_LOCATION
    cookies_file = os.path.join(SCRIPT_DIR, "cookies.txt")
    if os.path.exists(cookies_file):
        opts['cookiefile'] = cookies_file
    return opts


def safe_duration(val):
    try:
        return int(float(val)) if val else 0
    except (ValueError, TypeError):
        return 0


def fmt_dur(s):
    s = safe_duration(s)
    return f"{s // 60}:{s % 60:02d}" if s > 0 else "?:??"


def _search_platform(prefix, query, n, source_name):
    results = []
    try:
        opts = get_ydl_opts()
        opts['skip_download'] = True
        if 'ytsearch' in prefix:
            opts['extract_flat'] = 'in_playlist'
        with yt_dlp.YoutubeDL(opts) as ydl:
            data = ydl.extract_info(f"{prefix}{n}:{query}", download=False)
            if data and data.get('entries'):
                for e in data['entries']:
                    if not e:
                        continue
                    url = e.get('webpage_url') or e.get('url', '')
                    vid = e.get('id', '')
                    if not url.startswith('http'):
                        if vid and not vid.startswith('http') and 'youtube' in prefix:
                            url = f"https://www.youtube.com/watch?v={vid}"
                        elif vid and vid.startswith('http'):
                            url = vid
                        else:
                            continue
                    dur = safe_duration(e.get('duration'))
                    if 0 < MAX_DURATION < dur:
                        continue
                    results.append({
                        'url': url,
                        'title': e.get('title', '?'),
                        'artist': e.get('artist') or e.get('uploader') or e.get('channel', ''),
                        'duration': dur,
                        'source': source_name
                    })
    except Exception as ex:
        log.warning(f"{source_name} search err: {ex}")
    return results


def search_tracks(query):
    all_results = []
    seen_urls = set()
    searches = [
        ("scsearch", query, 5, "SoundCloud"),
        ("ytsearch", query, 5, "YouTube"),
        ("ytsearch", f"{query} official audio", 2, "YT Music"),
    ]
    for prefix, q, n, source in searches:
        try:
            for r in _search_platform(prefix, q, n, source):
                if r['url'] not in seen_urls:
                    all_results.append(r)
                    seen_urls.add(r['url'])
        except Exception as e:
            log.warning(f"Search err {source}: {e}")

    if not all_results:
        try:
            opts = get_ydl_opts()
            opts['skip_download'] = True
            with yt_dlp.YoutubeDL(opts) as ydl:
                data = ydl.extract_info(f"ytsearch3:{query}", download=False)
                if data and data.get('entries'):
                    for e in data['entries']:
                        if not e:
                            continue
                        url = e.get('webpage_url') or e.get('url', '')
                        if url.startswith('http') and url not in seen_urls:
                            dur = safe_duration(e.get('duration'))
                            if 0 < MAX_DURATION < dur:
                                continue
                            all_results.append({
                                'url': url, 'title': e.get('title', '?'),
                                'artist': e.get('artist') or e.get('uploader', ''),
                                'duration': dur, 'source': 'YouTube'
                            })
                            seen_urls.add(url)
        except Exception as e:
            log.warning(f"Fallback err: {e}")

    unique = []
    seen_titles = set()
    for r in all_results:
        key = re.sub(r'[^\w\s]', '', r['title'].lower()).strip()
        if key and key not in seen_titles:
            unique.append(r)
            seen_titles.add(key)
    return unique[:8]


def find_file_in_dir(temp_dir, extensions, min_size=500):
    for ext in extensions:
        for f in os.listdir(temp_dir):
            if f.lower().endswith(ext):
                fp = os.path.join(temp_dir, f)
                if os.path.isfile(fp) and os.path.getsize(fp) > min_size:
                    return fp
    skip = ('.jpg', '.png', '.webp', '.part', '.json', '.txt', '.description')
    for f in os.listdir(temp_dir):
        fp = os.path.join(temp_dir, f)
        if os.path.isfile(fp) and os.path.getsize(fp) > min_size:
            if not any(f.lower().endswith(s) for s in skip):
                return fp
    return None


def convert_to_mp3(input_path, temp_dir):
    if input_path.lower().endswith('.mp3') or not FFMPEG_AVAILABLE:
        return input_path
    mp3_path = os.path.join(temp_dir, "converted.mp3")
    try:
        cmd = "ffmpeg"
        if FFMPEG_LOCATION:
            cmd = os.path.join(FFMPEG_LOCATION, "ffmpeg.exe" if sys.platform == "win32" else "ffmpeg")
        subprocess.run(
            [cmd, '-i', input_path, '-codec:a', 'libmp3lame', '-q:a', '2', '-y', mp3_path],
            capture_output=True, timeout=120)
        if os.path.exists(mp3_path) and os.path.getsize(mp3_path) > 500:
            return mp3_path
    except Exception as e:
        log.warning(f"MP3 convert err: {e}")
    return input_path


def download_track(url):
    temp_dir = tempfile.mkdtemp(dir=DOWNLOADS_DIR)
    try:
        log.info(f"Downloading audio: {url}")
        output = os.path.join(temp_dir, "audio.%(ext)s")
        opts = get_ydl_opts()
        opts.update({'format': 'bestaudio/best', 'outtmpl': output})
        if FFMPEG_AVAILABLE:
            opts['postprocessors'] = [{'key': 'FFmpegExtractAudio',
                                       'preferredcodec': 'mp3', 'preferredquality': '192'}]
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
        title = info.get('title', 'audio') if info else 'audio'
        artist = (info.get('artist') or info.get('uploader') or info.get('channel', '')) if info else ''
        duration = safe_duration(info.get('duration')) if info else 0
        thumb_url = info.get('thumbnail') if info else None

        audio = find_file_in_dir(temp_dir, ['.mp3', '.m4a', '.opus', '.ogg', '.webm', '.wav', '.flac'])
        if not audio:
            log.error(f"No audio: {os.listdir(temp_dir)}")
            shutil.rmtree(temp_dir, ignore_errors=True)
            return None, "не получилось скачать 😔"
        audio = convert_to_mp3(audio, temp_dir)
        if os.path.getsize(audio) > MAX_FILE_SIZE:
            shutil.rmtree(temp_dir, ignore_errors=True)
            return None, "файл слишком большой 😔"

        thumb = None
        if thumb_url:
            try:
                tp = os.path.join(temp_dir, "thumb.jpg")
                tr = requests.get(thumb_url, timeout=8)
                if tr.status_code == 200 and len(tr.content) > 100:
                    with open(tp, 'wb') as tf:
                        tf.write(tr.content)
                    thumb = tp
            except Exception:
                pass

        return {
            'file': audio, 'title': title, 'artist': artist,
            'duration': duration, 'thumbnail': thumb, 'temp_dir': temp_dir
        }, None
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        log.error(f"Download err: {e}")
        return None, "ошибка скачивания 😔"


def download_video(url):
    temp_dir = tempfile.mkdtemp(dir=DOWNLOADS_DIR)
    try:
        log.info(f"Downloading video: {url}")
        output = os.path.join(temp_dir, "video.%(ext)s")
        opts = get_ydl_opts()
        opts.update({
            'format': 'best[filesize<50M]/best[height<=720]/best',
            'outtmpl': output, 'merge_output_format': 'mp4'})
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
        title = info.get('title', 'video') if info else 'video'
        duration = safe_duration(info.get('duration')) if info else 0
        video = find_file_in_dir(temp_dir, ['.mp4', '.mkv', '.webm', '.avi'])
        if video and os.path.getsize(video) <= MAX_FILE_SIZE:
            return {'file': video, 'title': title, 'duration': duration, 'temp_dir': temp_dir}, None
        shutil.rmtree(temp_dir, ignore_errors=True)
        return None, "не получилось скачать видео 😔"
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        log.error(f"Video err: {e}")
        return None, "ошибка скачивания 😔"


def download_with_timeout(func, url, timeout=None):
    if timeout is None:
        timeout = DOWNLOAD_TIMEOUT
    holder = {"result": None, "error": "слишком долго... 😔", "done": False}

    def _do():
        holder["result"], holder["error"] = func(url)
        holder["done"] = True

    t = threading.Thread(target=_do, daemon=True)
    t.start()
    t.join(timeout=timeout)
    if not holder["done"]:
        return None, "слишком долго, попробуй другое 🙏"
    return holder["result"], holder["error"]


def get_platform(url):
    platforms = {
        'tiktok.com': 'TikTok', 'instagram.com': 'Instagram',
        'youtube.com': 'YouTube', 'youtu.be': 'YouTube',
        'twitter.com': 'Twitter', 'x.com': 'X',
        'soundcloud.com': 'SoundCloud', 'vk.com': 'VK',
        'reddit.com': 'Reddit', 'facebook.com': 'Facebook',
        'twitch.tv': 'Twitch', 'vimeo.com': 'Vimeo',
        'music.youtube.com': 'YT Music', 'spotify.com': 'Spotify'}
    for d, n in platforms.items():
        if d in url:
            return n
    return 'видео'


# ================= КОММЕНТАРИИ =================
def music_comment(cid, title, grp=False):
    try:
        r = ask_ai([
            {"role": "system", "content":
                f"Ты Хината, 16 лет, застенчивая и милая девушка. Скидываешь трек '{title}'. "
                "Напиши 1 короткое предложение. Стиль: мило, с эмодзи. "
                "Например: 'вот, послушай 🎵' или 'нашла для тебя~ ☺️' "
                "Каждый раз по-разному. ТОЛЬКО текст."},
            {"role": "user", "content": "скинь"}])
        if r and not is_error(r):
            result = clean(r)
            if result and len(result) < 120:
                return result
    except Exception:
        pass
    return random.choice(FALLBACK_MUSIC_COMMENTS)


def track_list_msg(cid, query, results, grp=False):
    tracks = ""
    for i, r in enumerate(results):
        tracks += f"{i + 1}. {r['title']}"
        if r.get('artist'):
            tracks += f" — {r['artist']}"
        tracks += f" ({fmt_dur(r.get('duration', 0))})"
        if r.get('source'):
            tracks += f" [{r['source']}]"
        tracks += "\n"
    try:
        r = ask_ai([
            {"role": "system", "content":
                f"Ты Хината. Нашла треки по запросу '{query}'. "
                "Предложи выбрать номер. Мило, по-своему. "
                "Включи список. Можешь добавить эмодзи.\n\n"
                f"Треки:\n{tracks}"},
            {"role": "user", "content": f"найди {query}"}])
        if r and not is_error(r):
            result = clean(r)
            if result and any(str(i + 1) in result for i in range(len(results))):
                return result
    except Exception:
        pass
    return f"нашла по \"{query}\" 🎵\n\n{tracks}\nкакой скачать? выбирай номер ☺️"


# ================= ДЕТЕКТ =================
def quick_detect(text):
    for p in VIDEO_URL_PATTERNS:
        m = re.search(p, text)
        if m:
            url = m.group(1)
            lower = text.lower()
            is_audio = any(w in lower for w in ["mp3", "аудио", "звук", "музык", "песн"])
            return {"type": "video_download", "url": url, "format": "mp3" if is_audio else "auto"}
    lower = text.lower().strip()
    cl = lower
    for nick in BOT_NICKNAMES:
        cl = re.sub(rf'\b{re.escape(nick)}\b', '', cl)
    cl = re.sub(r'\s+', ' ', cl).strip().strip(",. !?")
    for t in MUSIC_TRIGGERS:
        if t in cl:
            q = cl
            for t2 in MUSIC_TRIGGERS:
                q = q.replace(t2, "")
            q = q.strip().strip("\"'.,!?")
            if q and len(q) > 1:
                return {"type": "music_search", "query": q}
    return None


def is_named(text):
    lower = text.lower()
    for nick in BOT_NICKNAMES:
        if re.search(rf'(?:^|[\s,!?.;:])' + re.escape(nick) + rf'(?:$|[\s,!?.;:])', lower):
            return True
        if lower.strip() == nick:
            return True
    return False


def analyze_intent(text):
    quick = quick_detect(text)
    if quick:
        return quick
    try:
        r = ask_ai([
            {"role": "system", "content":
                "Определи намерение. ТОЛЬКО JSON:\n"
                '{"type":"music_search"|"video_download"|"chat","query":null,"url":null,"format":"auto"}\n'
                "Только JSON."},
            {"role": "user", "content": text}])
        if r and not is_error(r):
            parsed = extract_json(r)
            if parsed and parsed.get("type") in ("music_search", "video_download", "chat"):
                return parsed
    except Exception:
        pass
    return {"type": "chat"}


# ================= КНОПКИ =================
def fmt_kb():
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.row(
        types.InlineKeyboardButton("🎬 MP4", callback_data="dl_mp4"),
        types.InlineKeyboardButton("🎵 MP3", callback_data="dl_mp3"))
    return kb


def track_kb(n, msg_id):
    kb = types.InlineKeyboardMarkup(row_width=4)
    btns = [types.InlineKeyboardButton(str(i + 1), callback_data=f"tr_{msg_id}_{i}") for i in range(n)]
    kb.add(*btns)
    kb.row(types.InlineKeyboardButton("✖ отмена", callback_data=f"tr_{msg_id}_x"))
    return kb


def main_kb():
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("🗑 Очистить", callback_data="clear"),
        types.InlineKeyboardButton("📊 Статистика", callback_data="stats"),
        types.InlineKeyboardButton("👥 Мои группы", callback_data="my_groups"),
        types.InlineKeyboardButton("🌸 О Хинате", callback_data="info"))
    return kb


def start_kb():
    bi = get_bot_info()
    username = bi.username if bi else "bot"
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton("➕ Добавить в группу", url=f"https://t.me/{username}?startgroup=true"),
        types.InlineKeyboardButton("💬 Написать", callback_data="start_chat"),
        types.InlineKeyboardButton("👥 Мои группы", callback_data="my_groups"),
        types.InlineKeyboardButton("🌸 О Хинате", callback_data="info"))
    return kb


def pg_kb(cid):
    s = get_gs(cid)
    kb = types.InlineKeyboardMarkup(row_width=3)
    kb.row(
        types.InlineKeyboardButton("−10", callback_data=f"pg_cd10_{cid}"),
        types.InlineKeyboardButton(f"📊 {s['response_chance']}%", callback_data="noop"),
        types.InlineKeyboardButton("+10", callback_data=f"pg_cu10_{cid}"))
    kb.row(
        types.InlineKeyboardButton("−5", callback_data=f"pg_cd5_{cid}"),
        types.InlineKeyboardButton("+5", callback_data=f"pg_cu5_{cid}"))
    pro = "✅" if s.get("proactive_enabled") else "❌"
    kb.row(types.InlineKeyboardButton(f"{pro} Писать первой", callback_data=f"pg_pt_{cid}"))
    if s.get("proactive_enabled"):
        kb.row(types.InlineKeyboardButton(
            f"⏱ {s.get('proactive_min_interval', 30)}-{s.get('proactive_max_interval', 120)} мин",
            callback_data=f"pg_pi_{cid}"))
        kb.row(types.InlineKeyboardButton(
            f"🕐 {s.get('proactive_active_hours_start', 9)}-{s.get('proactive_active_hours_end', 23)} ч",
            callback_data=f"pg_ph_{cid}"))
    lr = "✅" if s.get("learn_style") else "❌"
    kb.row(types.InlineKeyboardButton(f"{lr} Обучение стилю", callback_data=f"pg_lt_{cid}"))
    kb.row(
        types.InlineKeyboardButton("📝 Промпт", callback_data=f"pg_pc_{cid}"),
        types.InlineKeyboardButton("🔄 Сброс", callback_data=f"pg_pr_{cid}"))
    kb.row(
        types.InlineKeyboardButton("🗑 Контекст", callback_data=f"pg_cc_{cid}"),
        types.InlineKeyboardButton("🧹 Память", callback_data=f"pg_cm_{cid}"))
    kb.row(types.InlineKeyboardButton("◀ Назад", callback_data="my_groups"))
    return kb


def grp_kb(cid):
    s = get_gs(cid)
    kb = types.InlineKeyboardMarkup(row_width=3)
    kb.row(
        types.InlineKeyboardButton("−10", callback_data="cd10"),
        types.InlineKeyboardButton(f"📊 {s['response_chance']}%", callback_data="noop"),
        types.InlineKeyboardButton("+10", callback_data="cu10"))
    kb.row(
        types.InlineKeyboardButton("−5", callback_data="cd5"),
        types.InlineKeyboardButton("+5", callback_data="cu5"))
    pro = "✅" if s.get("proactive_enabled") else "❌"
    kb.row(types.InlineKeyboardButton(f"{pro} Писать первой", callback_data="ptog"))
    if s.get("proactive_enabled"):
        kb.row(types.InlineKeyboardButton(
            f"⏱ {s.get('proactive_min_interval', 30)}-{s.get('proactive_max_interval', 120)} мин",
            callback_data="pint"))
        kb.row(types.InlineKeyboardButton(
            f"🕐 {s.get('proactive_active_hours_start', 9)}-{s.get('proactive_active_hours_end', 23)} ч",
            callback_data="phrs"))
    lr = "✅" if s.get("learn_style") else "❌"
    kb.row(types.InlineKeyboardButton(f"{lr} Обучение стилю", callback_data="ltog"))
    kb.row(
        types.InlineKeyboardButton("📝 Промпт", callback_data="pchg"),
        types.InlineKeyboardButton("🔄 Сброс", callback_data="prst"))
    kb.row(types.InlineKeyboardButton("👑 Админы", callback_data="alst"))
    kb.row(
        types.InlineKeyboardButton("🗑 Контекст", callback_data="gclr"),
        types.InlineKeyboardButton("🧹 Память", callback_data="gmem"))
    kb.row(types.InlineKeyboardButton("✖ Закрыть", callback_data="close"))
    return kb


def int_kb(cid, priv=False):
    pfx = f"pgi_{cid}" if priv else "gi"
    kb = types.InlineKeyboardMarkup(row_width=2)
    for label, v in [("5-15 мин", "5_15"), ("10-30 мин", "10_30"), ("15-45 мин", "15_45"),
                     ("30-60 мин", "30_60"), ("30-120 мин", "30_120"), ("60-180 мин", "60_180")]:
        kb.add(types.InlineKeyboardButton(label, callback_data=f"{pfx}_{v}"))
    kb.add(types.InlineKeyboardButton("◀ Назад", callback_data=f"pg_sel_{cid}" if priv else "bk"))
    return kb


def hrs_kb(cid, priv=False):
    pfx = f"pgh_{cid}" if priv else "gh"
    kb = types.InlineKeyboardMarkup(row_width=2)
    for label, v in [("6-22 ч", "6_22"), ("8-23 ч", "8_23"), ("9-21 ч", "9_21"),
                     ("10-2 ч", "10_2"), ("0-24 ч", "0_24"), ("18-6 ч", "18_6")]:
        kb.add(types.InlineKeyboardButton(label, callback_data=f"{pfx}_{v}"))
    kb.add(types.InlineKeyboardButton("◀ Назад", callback_data=f"pg_sel_{cid}" if priv else "bk"))
    return kb


def gl_kb(uid):
    gs = get_ugroups(uid)
    kb = types.InlineKeyboardMarkup(row_width=1)
    for gid_str, info in gs.items():
        kb.add(types.InlineKeyboardButton(f"⚙ {info.get('title', 'Группа')}",
                                          callback_data=f"pg_sel_{gid_str}"))
    kb.add(types.InlineKeyboardButton("◀ Назад", callback_data="back_main"))
    return kb


# ================= СЕССИИ =================
def get_session(cid, grp=False):
    if cid not in chat_sessions:
        chat_sessions[cid] = {
            "messages": [{"role": "system", "content": build_prompt(cid, grp)}],
            "created": datetime.now().strftime("%d.%m.%Y %H:%M"),
            "users": {}, "msg_count": 0, "is_group": grp
        }
    return chat_sessions[cid]


def add_msg(cid, role, content, grp=False):
    if not content or not isinstance(content, str) or not content.strip():
        return
    with session_lock:
        s = get_session(cid, grp)
        s["messages"].append({"role": role, "content": content})
        if len(s["messages"]) > SESSION_MAX_MESSAGES + 1:
            s["messages"] = [s["messages"][0]] + s["messages"][-SESSION_MAX_MESSAGES:]
        s["msg_count"] = s.get("msg_count", 0) + 1
        mc = s["msg_count"]
    last_activity[cid] = datetime.now()
    if mc > 0 and mc % LEARN_INTERVAL == 0:
        threading.Thread(target=learn, args=(cid,), daemon=True).start()


def rem_user(cid, user):
    if not user:
        return
    with session_lock:
        s = get_session(cid, True)
        s["users"][str(user.id)] = {"name": dname(user)}
    remember_group_user(cid, user)


def clr_hist(cid, grp=False):
    with session_lock:
        old = chat_sessions.get(cid, {}).get("users", {}).copy()
        chat_sessions[cid] = {
            "messages": [{"role": "system", "content": build_prompt(cid, grp)}],
            "created": datetime.now().strftime("%d.%m.%Y %H:%M"),
            "users": old, "msg_count": 0, "is_group": grp
        }


def clear_memory(cid, grp=False):
    save_memory(cid, get_empty_memory())
    save_style(cid, get_empty_style())
    clr_hist(cid, grp)


def ref_prompt(cid, grp=False):
    with session_lock:
        if cid in chat_sessions:
            chat_sessions[cid]["messages"][0] = {"role": "system", "content": build_prompt(cid, grp)}


def get_msgs_copy(cid, grp=False):
    with session_lock:
        return copy.deepcopy(get_session(cid, grp)["messages"])


def is_pm(msg):
    return msg.chat.type == "private"


def is_grp(msg):
    return msg.chat.type in ("group", "supergroup")


# ================= ОТПРАВКА =================
def send_audio_safe(cid, res, caption, reply_to=None):
    th = None
    try:
        if res.get('thumbnail') and os.path.exists(res['thumbnail']):
            try:
                th = open(res['thumbnail'], 'rb')
            except Exception:
                pass
        with open(res['file'], 'rb') as audio:
            bot.send_audio(cid, audio, title=res.get('title', 'audio'),
                           performer=res.get('artist', ''),
                           duration=safe_duration(res.get('duration', 0)),
                           thumbnail=th, caption=caption, reply_to_message_id=reply_to)
    except Exception:
        if th:
            try:
                th.close()
            except Exception:
                pass
            th = None
        with open(res['file'], 'rb') as audio:
            bot.send_audio(cid, audio, title=res.get('title', 'audio'),
                           performer=res.get('artist', ''),
                           duration=safe_duration(res.get('duration', 0)),
                           caption=caption, reply_to_message_id=reply_to)
    finally:
        if th:
            try:
                th.close()
            except Exception:
                pass


def send_long_msg(cid, text, markup=None, reply_to=None):
    if not text or not text.strip():
        text = "..."
    chunks = []
    while len(text) > 4096:
        sp = text.rfind('\n', 0, 4096)
        if sp < 2000:
            sp = text.rfind('. ', 0, 4096)
        if sp < 2000:
            sp = 4096
        chunks.append(text[:sp])
        text = text[sp:].lstrip()
    if text:
        chunks.append(text)
    for i, chunk in enumerate(chunks):
        safe_send(cid, chunk,
                  markup=markup if i == len(chunks) - 1 else None,
                  reply_to=reply_to if i == 0 else None)


# ================= PENDING =================
def get_pkey(cid, msg_id):
    return f"pend_{cid}_{msg_id}"


def find_pending(cid):
    with pending_lock:
        prefix = f"pend_{cid}_"
        return [(k, v) for k, v in pending_tracks.items()
                if k.startswith(prefix) and v.get("time") and
                (datetime.now() - v["time"]).total_seconds() < PENDING_TIMEOUT]


def cleanup_pending():
    with pending_lock:
        expired = [k for k, v in pending_tracks.items()
                   if v.get("time") and (datetime.now() - v["time"]).total_seconds() > PENDING_TIMEOUT]
        for k in expired:
            del pending_tracks[k]


# ================= НАСТРОЙКИ ОБЩИЕ =================
def apply_setting(s, action, cid=None):
    if action == "cd10":
        with settings_lock: s["response_chance"] = max(0, s["response_chance"] - 10)
        save_settings(); return f"Шанс: {s['response_chance']}%"
    elif action == "cu10":
        with settings_lock: s["response_chance"] = min(100, s["response_chance"] + 10)
        save_settings(); return f"Шанс: {s['response_chance']}%"
    elif action == "cd5":
        with settings_lock: s["response_chance"] = max(0, s["response_chance"] - 5)
        save_settings(); return f"Шанс: {s['response_chance']}%"
    elif action == "cu5":
        with settings_lock: s["response_chance"] = min(100, s["response_chance"] + 5)
        save_settings(); return f"Шанс: {s['response_chance']}%"
    elif action == "pt":
        with settings_lock: s["proactive_enabled"] = not s.get("proactive_enabled", False)
        save_settings()
        target = cid or 0
        if s["proactive_enabled"]:
            start_ptimer(target); return "✅ Буду писать первой~"
        else:
            stop_ptimer(target); return "❌ Не буду писать первой"
    elif action == "lt":
        with settings_lock: s["learn_style"] = not s.get("learn_style", True)
        save_settings()
        return "✅ Обучение вкл" if s["learn_style"] else "❌ Обучение выкл"
    elif action == "pr":
        with settings_lock: s["custom_prompt"] = None
        save_settings()
        if cid: ref_prompt(cid, True)
        return "✅ Промпт сброшен"
    elif action == "cc":
        if cid: clr_hist(cid, True)
        return "✅ Контекст очищен"
    elif action == "cm":
        if cid: clear_memory(cid, True)
        return "✅ Память сброшена"
    return None


# ================= ХЕНДЛЕРЫ =================
@bot.message_handler(content_types=['new_chat_members'])
def on_join(msg):
    try:
        bi = get_bot_info()
        if not bi:
            return
        for m in msg.new_chat_members:
            if m.id == bi.id:
                cid = msg.chat.id
                s = get_gs(cid)
                with settings_lock:
                    s["owner_id"] = msg.from_user.id
                    s["owner_name"] = dname(msg.from_user)
                    s["group_name"] = msg.chat.title
                save_settings()
                reg_group(msg.from_user.id, cid, msg.chat.title)
                with session_lock:
                    get_session(cid, True)
                safe_send(cid,
                          "п-привет... я Хината 🌸\n"
                          "можете звать меня по имени~ я могу найти музыку и просто поболтать ☺️\n"
                          "/help — что я умею")
                if s.get("proactive_enabled"):
                    start_ptimer(cid)
                log.info(f"Добавлена в группу: {msg.chat.title} ({cid})")
    except Exception as e:
        log.error(f"Join err: {e}")


@bot.message_handler(content_types=['left_chat_member'])
def on_leave(msg):
    try:
        bi = get_bot_info()
        if not bi:
            return
        if msg.left_chat_member and msg.left_chat_member.id == bi.id:
            cid = msg.chat.id
            stop_ptimer(cid)
            with user_groups_lock:
                for uid_key in list(user_groups.keys()):
                    user_groups[uid_key].pop(str(cid), None)
            save_user_groups()
            log.info(f"Удалена из группы: {msg.chat.title} ({cid})")
    except Exception as e:
        log.error(f"Leave err: {e}")


@bot.message_handler(commands=['start'])
def cmd_start(msg):
    if is_pm(msg):
        with session_lock:
            get_session(msg.from_user.id)
        safe_send(msg.chat.id,
                  "п-привет! 🌸 я Хината\n\n"
                  "я... ну... могу поболтать, найти музыку, скачать видео ☺️\n"
                  "не стесняйся писать, я всегда рада пообщаться 💕\n\n"
                  "реклама — @PaceHoz",
                  markup=start_kb())
    else:
        safe_send(msg.chat.id, "я тут~ /help если нужна помощь 🌸")


@bot.message_handler(commands=['help'])
def cmd_help(msg):
    text = ("🌸 что я умею:\n\n"
            "/start — начать\n/help — помощь\n/clear — очистить контекст\n"
            "/settings — настройки\n\n"
            "🎵 музыка — просто попроси!\n🎬 видео — кинь ссылку\n"
            "зови: Хината, Хина, Хиночка~\n\n"
            "реклама — @PaceHoz")
    safe_send(msg.chat.id, text, markup=main_kb() if is_pm(msg) else None)


@bot.message_handler(commands=['clear'])
def cmd_clear(msg):
    if is_pm(msg):
        clr_hist(msg.from_user.id)
        safe_send(msg.chat.id, "очистила контекст ✨", markup=main_kb())
    elif is_admin(msg.chat.id, msg.from_user.id):
        clr_hist(msg.chat.id, True)
        safe_send(msg.chat.id, "контекст очищен ✨")


@bot.message_handler(commands=['settings'])
def cmd_settings(msg):
    if is_pm(msg):
        gs = get_ugroups(msg.from_user.id)
        if not gs:
            safe_send(msg.chat.id, "у тебя пока нет групп... добавь меня! 🌸", markup=start_kb())
        else:
            safe_send(msg.chat.id, "выбери группу ☺️", markup=gl_kb(msg.from_user.id))
        return
    cid = msg.chat.id
    s = get_gs(cid)
    if s["owner_id"] is None:
        with settings_lock:
            s["owner_id"] = msg.from_user.id
            s["owner_name"] = dname(msg.from_user)
        save_settings()
    if not is_admin(cid, msg.from_user.id):
        return
    pro = "да" if s.get("proactive_enabled") else "нет"
    lr = "да" if s.get("learn_style") else "нет"
    safe_send(cid, f"⚙ Настройки\n📊 Шанс: {s['response_chance']}%\n💬 Первой: {pro}\n📚 Обучение: {lr}",
              markup=grp_kb(cid))


@bot.message_handler(commands=['addadmin'])
def cmd_addadmin(msg):
    if is_pm(msg) or not is_owner(msg.chat.id, msg.from_user.id):
        return
    if not msg.reply_to_message or not msg.reply_to_message.from_user:
        bot.reply_to(msg, "ответь на сообщение пользователя 🙏")
        return
    t = msg.reply_to_message.from_user
    if t.is_bot:
        bot.reply_to(msg, "ботов нельзя 😅")
        return
    s = get_gs(msg.chat.id)
    with settings_lock:
        s.setdefault("admins", {})[str(t.id)] = {"name": dname(t)}
    save_settings()
    reg_group(t.id, msg.chat.id, msg.chat.title)
    safe_send(msg.chat.id, f"{dname(t)} теперь админ ✨")


@bot.message_handler(commands=['removeadmin'])
def cmd_removeadmin(msg):
    if is_pm(msg) or not is_owner(msg.chat.id, msg.from_user.id):
        return
    if not msg.reply_to_message or not msg.reply_to_message.from_user:
        bot.reply_to(msg, "ответь на сообщение 🙏")
        return
    s = get_gs(msg.chat.id)
    tk = str(msg.reply_to_message.from_user.id)
    with settings_lock:
        name = s.get("admins", {}).pop(tk, {}).get("name", "?")
    save_settings()
    safe_send(msg.chat.id, f"{name} больше не админ")


@bot.message_handler(commands=['admins'])
def cmd_admins(msg):
    if is_pm(msg):
        return
    s = get_gs(msg.chat.id)
    t = f"👑 Владелец: {s.get('owner_name', '?')}\n"
    admins = s.get("admins", {})
    if admins:
        t += "\n👤 Админы:\n"
        for a in admins.values():
            if isinstance(a, dict):
                t += f"  • {a.get('name', '?')}\n"
    else:
        t += "\nАдминов нет"
    safe_send(msg.chat.id, t)


@bot.message_handler(commands=['setowner'])
def cmd_setowner(msg):
    if is_pm(msg) or not is_owner(msg.chat.id, msg.from_user.id):
        return
    if not msg.reply_to_message or not msg.reply_to_message.from_user:
        bot.reply_to(msg, "ответь на сообщение 🙏")
        return
    nw = msg.reply_to_message.from_user
    if nw.is_bot:
        return
    s = get_gs(msg.chat.id)
    with settings_lock:
        old_id = str(s["owner_id"]) if s["owner_id"] else None
        s["admins"].pop(str(nw.id), None)
        if old_id:
            s["admins"][old_id] = {"name": s.get("owner_name", "?")}
        s["owner_id"] = nw.id
        s["owner_name"] = dname(nw)
    save_settings()
    reg_group(nw.id, msg.chat.id, msg.chat.title)
    safe_send(msg.chat.id, f"👑 Новый владелец: {dname(nw)}")


# ================= CALLBACKS =================
@bot.callback_query_handler(func=lambda c: True)
def on_cb(call):
    try:
        uid = call.from_user.id
        cid = call.message.chat.id
        mid = call.message.message_id
        ct = call.message.chat.type
        data = call.data

        if data.startswith("tr_"):
            handle_track_cb(call, cid, mid, ct)
            return
        if data in ("dl_mp4", "dl_mp3"):
            handle_dl_format_cb(call, cid, mid, ct)
            return
        if ct == "private":
            handle_pm_cb(call, uid, cid, mid, data)
            return
        if not is_admin(cid, uid):
            bot.answer_callback_query(call.id, "❌ Нет прав", show_alert=True)
            return
        handle_grp_cb(call, data, uid, cid, mid)
    except Exception as e:
        log.error(f"CB err: {e}")
        try:
            bot.answer_callback_query(call.id, "ошибка...")
        except Exception:
            pass


def handle_track_cb(call, cid, mid, ct):
    parts = call.data.split("_")
    if len(parts) < 3:
        bot.answer_callback_query(call.id, "ошибка", show_alert=True)
        return
    action = parts[-1]
    orig_id = "_".join(parts[1:-1])
    with pending_lock:
        pk = f"pend_{cid}_{orig_id}"
        if pk not in pending_tracks:
            pk = f"pend_{cid}_{mid}"
        if pk not in pending_tracks:
            for k in pending_tracks:
                if k.startswith(f"pend_{cid}_"):
                    pk = k
                    break
            else:
                bot.answer_callback_query(call.id, "⏰ Устарело, поищи заново~", show_alert=True)
                return
        if action == "x":
            pending_tracks.pop(pk, None)
            safe_edit("ладно, отменила ☺️", cid, mid)
            bot.answer_callback_query(call.id, "Отменено")
            return
        try:
            idx = int(action)
        except ValueError:
            bot.answer_callback_query(call.id, "ошибка", show_alert=True)
            return
        pd = pending_tracks.pop(pk, None)
    if not pd or idx >= len(pd.get("results", [])):
        bot.answer_callback_query(call.id, "❌ Нет такого", show_alert=True)
        return
    track = pd["results"][idx]
    busy, bt = is_busy(cid)
    if busy:
        with pending_lock:
            pending_tracks[pk] = pd
        bot.answer_callback_query(call.id, get_busy_reply(bt), show_alert=True)
        return
    set_busy(cid, "music", track['title'])
    safe_edit(f"скачиваю {track['title']}... 🎵", cid, mid)
    bot.answer_callback_query(call.id, f"Качаю: {track['title'][:50]}")
    grp = ct != "private"
    threading.Thread(target=dl_and_send, args=(cid, mid, track, grp), daemon=True).start()


def handle_dl_format_cb(call, cid, mid, ct):
    with user_states_lock:
        url = user_states.pop(f"dl_{cid}_{mid}", None)
    if not url:
        bot.answer_callback_query(call.id, "⏰ Устарело~", show_alert=True)
        return
    busy, bt = is_busy(cid)
    if busy:
        with user_states_lock:
            user_states[f"dl_{cid}_{mid}"] = url
        bot.answer_callback_query(call.id, get_busy_reply(bt), show_alert=True)
        return
    fmt = "mp3" if call.data == "dl_mp3" else "mp4"
    set_busy(cid, "music" if fmt == "mp3" else "video")
    safe_edit("скачиваю... 🎵", cid, mid)
    bot.answer_callback_query(call.id, f"Качаю в {fmt.upper()}")
    grp = ct != "private"
    threading.Thread(target=dl_url_and_send, args=(cid, mid, url, fmt, grp), daemon=True).start()


def handle_pm_cb(call, uid, cid, mid, data):
    if data == "clear":
        clr_hist(uid)
        safe_edit("очистила~ ✨", cid, mid, markup=main_kb())
        bot.answer_callback_query(call.id, "✅ Очищено", show_alert=True)
    elif data == "stats":
        with session_lock:
            mc = len(get_session(uid)['messages']) - 1
        gc = len(get_ugroups(uid))
        safe_edit(f"📊 Статистика\n\n💬 Сообщений: {mc}\n👥 Групп: {gc}", cid, mid, markup=main_kb())
        bot.answer_callback_query(call.id)
    elif data == "start_chat":
        safe_edit("пиши, я тут~ 💕", cid, mid, markup=main_kb())
        bot.answer_callback_query(call.id)
    elif data == "info":
        safe_edit(
            "🌸 Хината, 16 лет\n\n"
            "• люблю готовить сладкое и рисовать 🎨\n"
            "• ищу музыку: YouTube, SoundCloud 🎵\n"
            "• качаю видео с 20+ платформ 🎬\n"
            "• отвечаю на вопросы ✨\n"
            "• учусь стилю общения 📚\n"
            "• люблю чай с мёдом и котиков 🐱\n\n"
            "зови: Хината, Хина, Хиночка~\n\nреклама — @PaceHoz",
            cid, mid, markup=main_kb())
        bot.answer_callback_query(call.id)
    elif data == "my_groups":
        gs = get_ugroups(uid)
        if gs:
            safe_edit("👥 Твои группы:", cid, mid, markup=gl_kb(uid))
        else:
            safe_edit("пока нет групп... добавь меня! 🌸", cid, mid, markup=start_kb())
        bot.answer_callback_query(call.id)
    elif data == "back_main":
        safe_edit("чем помочь? ☺️", cid, mid, markup=main_kb())
        bot.answer_callback_query(call.id)
    elif data.startswith("pg_sel_"):
        try:
            gid = int(data[7:])
        except ValueError:
            bot.answer_callback_query(call.id, "ошибка", show_alert=True)
            return
        if is_admin(gid, uid):
            s = get_gs(gid)
            gn = get_ugroups(uid).get(str(gid), {}).get('title', 'Группа')
            safe_edit(f"⚙ {gn}\n📊 Шанс: {s['response_chance']}%", cid, mid, markup=pg_kb(gid))
        else:
            bot.answer_callback_query(call.id, "❌ Нет прав", show_alert=True)
            return
        bot.answer_callback_query(call.id)
    elif data.startswith("pg_") or data.startswith("pgi_") or data.startswith("pgh_"):
        handle_pg_cb(call, data, uid, cid, mid)
    elif data == "noop":
        bot.answer_callback_query(call.id)
    else:
        bot.answer_callback_query(call.id)


def handle_pg_cb(call, data, uid, cid, mid):
    try:
        prefixes = {
            "pg_cd10_": "cd10", "pg_cu10_": "cu10", "pg_cd5_": "cd5", "pg_cu5_": "cu5",
            "pg_pt_": "pt", "pg_pi_": "pi", "pg_ph_": "ph", "pg_lt_": "lt",
            "pg_pc_": "pc", "pg_pr_": "pr", "pg_cc_": "cc", "pg_cm_": "cm"
        }
        action = gid = None
        for pfx, act in prefixes.items():
            if data.startswith(pfx):
                try:
                    gid = int(data[len(pfx):]); action = act
                except ValueError:
                    pass
                break
        if action is None and data.startswith("pgi_"):
            parts = data[4:].rsplit("_", 2)
            if len(parts) == 3:
                try:
                    gid, mn, mx = int(parts[0]), int(parts[1]), int(parts[2]); action = "pgi"
                except ValueError:
                    pass
        if action is None and data.startswith("pgh_"):
            parts = data[4:].rsplit("_", 2)
            if len(parts) == 3:
                try:
                    gid, sh, eh = int(parts[0]), int(parts[1]), int(parts[2]); action = "pgh"
                except ValueError:
                    pass
        if action is None or gid is None:
            bot.answer_callback_query(call.id); return
        if not is_admin(gid, uid):
            bot.answer_callback_query(call.id, "❌ Нет прав", show_alert=True); return
        s = get_gs(gid); alert = None
        if action in ("cd10", "cu10", "cd5", "cu5", "pt", "lt", "pr", "cc", "cm"):
            alert = apply_setting(s, action, gid)
        elif action == "pi":
            safe_edit("⏱ Выбери интервал:", cid, mid, markup=int_kb(gid, True))
            bot.answer_callback_query(call.id); return
        elif action == "ph":
            safe_edit("🕐 Выбери часы:", cid, mid, markup=hrs_kb(gid, True))
            bot.answer_callback_query(call.id); return
        elif action == "pgi":
            with settings_lock: s["proactive_min_interval"] = mn; s["proactive_max_interval"] = mx
            save_settings()
            if s.get("proactive_enabled"): start_ptimer(gid)
            alert = f"Интервал: {mn}-{mx} мин"
        elif action == "pgh":
            with settings_lock: s["proactive_active_hours_start"] = sh; s["proactive_active_hours_end"] = eh
            save_settings(); alert = f"Часы: {sh}-{eh}"
        elif action == "pc":
            with user_states_lock: user_states[f"pp_{uid}"] = gid
            safe_edit("📝 Отправь промпт~\nОтмена: отмена", cid, mid)
            bot.answer_callback_query(call.id, "Жду..."); return
        gn = get_ugroups(uid).get(str(gid), {}).get('title', 'Группа')
        safe_edit(f"⚙ {gn}\n📊 Шанс: {s['response_chance']}%", cid, mid, markup=pg_kb(gid))
        bot.answer_callback_query(call.id, alert, show_alert=bool(alert))
    except Exception as e:
        log.error(f"PG err: {e}")
        try:
            bot.answer_callback_query(call.id, "ошибка...")
        except Exception:
            pass


def handle_grp_cb(call, data, uid, cid, mid):
    s = get_gs(cid); alert = None
    try:
        if data == "noop":
            bot.answer_callback_query(call.id); return
        elif data == "close":
            safe_delete(cid, mid); bot.answer_callback_query(call.id); return
        elif data in ("cd10", "cu10", "cd5", "cu5", "ltog", "gclr", "gmem", "prst"):
            act = {"ltog": "lt", "gclr": "cc", "gmem": "cm", "prst": "pr"}.get(data, data)
            alert = apply_setting(s, act, cid)
        elif data == "ptog":
            alert = apply_setting(s, "pt", cid)
        elif data == "pint":
            safe_edit("⏱ Интервал:", cid, mid, markup=int_kb(cid))
            bot.answer_callback_query(call.id); return
        elif data == "phrs":
            safe_edit("🕐 Часы:", cid, mid, markup=hrs_kb(cid))
            bot.answer_callback_query(call.id); return
        elif data.startswith("gi_"):
            v = data[3:].split("_")
            if len(v) == 2:
                with settings_lock: s["proactive_min_interval"] = int(v[0]); s["proactive_max_interval"] = int(v[1])
                save_settings()
                if s.get("proactive_enabled"): start_ptimer(cid)
                alert = f"Интервал: {v[0]}-{v[1]} мин"
        elif data.startswith("gh_"):
            v = data[3:].split("_")
            if len(v) == 2:
                with settings_lock: s["proactive_active_hours_start"] = int(v[0]); s["proactive_active_hours_end"] = int(v[1])
                save_settings(); alert = f"Часы: {v[0]}-{v[1]}"
        elif data == "bk":
            pass
        elif data == "pchg":
            with user_states_lock: user_states[f"{cid}_{uid}"] = "wp"
            safe_send(cid, "📝 Отправь промпт~\nОтмена: отмена")
            bot.answer_callback_query(call.id, "Жду..."); return
        elif data == "alst":
            t = f"👑 Владелец: {s.get('owner_name', '?')}\n"
            admins = s.get("admins", {})
            if admins:
                t += "\n👤 Админы:\n"
                for a in admins.values():
                    if isinstance(a, dict): t += f"  • {a.get('name', '?')}\n"
            else:
                t += "\nАдминов нет"
            bot.answer_callback_query(call.id, t, show_alert=True); return
        else:
            bot.answer_callback_query(call.id); return
        pro = "да" if s.get("proactive_enabled") else "нет"
        safe_edit(f"⚙ Настройки\n📊 Шанс: {s['response_chance']}%\n💬 Первой: {pro}",
                  cid, mid, markup=grp_kb(cid))
        bot.answer_callback_query(call.id, alert, show_alert=bool(alert))
    except Exception as e:
        log.error(f"Grp CB err: {e}")
        try:
            bot.answer_callback_query(call.id, "ошибка...")
        except Exception:
            pass


# ================= СКАЧИВАНИЕ =================
def dl_and_send(cid, mid, track, grp):
    try:
        res, err = download_with_timeout(download_track, track['url'])
        if err:
            safe_edit(f"не получилось... {err}", cid, mid); return
        try:
            comment = music_comment(cid, res['title'], grp)
            send_audio_safe(cid, res, comment)
            safe_delete(cid, mid)
            add_msg(cid, "assistant", comment, grp)
        except Exception as e:
            log.error(f"Send err: {e}"); safe_edit("ошибка отправки 😔", cid, mid)
        finally:
            shutil.rmtree(res.get('temp_dir', ''), ignore_errors=True)
    except Exception as e:
        log.error(f"DL err: {e}"); safe_edit("ошибка... 😔", cid, mid)
    finally:
        clear_busy(cid)


def dl_url_and_send(cid, mid, url, fmt, grp):
    try:
        if fmt == "mp3":
            res, err = download_with_timeout(download_track, url)
        else:
            res, err = download_with_timeout(download_video, url)
        if err:
            safe_edit(err, cid, mid); return
        try:
            if fmt == "mp3":
                c = music_comment(cid, res['title'], grp)
                send_audio_safe(cid, res, c)
            else:
                with open(res['file'], 'rb') as v:
                    bot.send_video(cid, v, caption=res.get('title', ''),
                                   duration=safe_duration(res.get('duration', 0)),
                                   supports_streaming=True)
            safe_delete(cid, mid)
        except Exception as e:
            log.error(f"Send err: {e}"); safe_edit("ошибка отправки 😔", cid, mid)
        finally:
            shutil.rmtree(res.get('temp_dir', ''), ignore_errors=True)
    except Exception as e:
        log.error(f"DL err: {e}"); safe_edit("ошибка... 😔", cid, mid)
    finally:
        clear_busy(cid)


# ================= МЕДИА =================
def handle_media(msg, intent, grp=False):
    cid = msg.chat.id
    busy, bt = is_busy(cid)
    if busy:
        safe_send(cid, get_busy_reply(bt)); return True

    if intent.get("type") == "music_search" and intent.get("query"):
        query = intent["query"]
        set_busy(cid, "music", query)
        smsg = safe_send(cid, f"ищу \"{query}\"... 🎵")
        if not smsg:
            clear_busy(cid); return True

        def do():
            try:
                results = search_tracks(query)
                if not results:
                    safe_edit("ничего не нашла... попробуй по-другому 🥺", cid, smsg.message_id); return
                results = results[:6]
                pk = get_pkey(cid, smsg.message_id)
                with pending_lock:
                    pending_tracks[pk] = {"results": results, "query": query,
                                          "time": datetime.now(), "user_id": msg.from_user.id}
                text = track_list_msg(cid, query, results, grp)
                kb = track_kb(len(results), smsg.message_id)
                if not safe_edit(text, cid, smsg.message_id, markup=kb):
                    fb = f"нашла {len(results)} треков 🎵\n\n"
                    for i, r in enumerate(results):
                        fb += f"{i+1}. {r['title']} ({fmt_dur(r.get('duration', 0))})"
                        if r.get('source'): fb += f" [{r['source']}]"
                        fb += "\n"
                    fb += "\nвыбирай номер~ ☺️"
                    safe_edit(fb, cid, smsg.message_id, markup=kb)
            except Exception as e:
                log.error(f"Search err: {e}")
                safe_edit("ой, ошибка поиска... 😔", cid, smsg.message_id)
            finally:
                clear_busy(cid)
        threading.Thread(target=do, daemon=True).start()
        return True

    if intent.get("type") == "video_download" and intent.get("url"):
        url = intent["url"]; fmt = intent.get("format", "auto")
        if fmt == "auto":
            m = safe_send(cid, f"{get_platform(url)} — какой формат? ☺️", markup=fmt_kb())
            if m:
                with user_states_lock: user_states[f"dl_{cid}_{m.message_id}"] = url
            return True
        set_busy(cid, "music" if fmt == "mp3" else "video")
        smsg = safe_send(cid, "скачиваю... 🎵")
        if not smsg:
            clear_busy(cid); return True
        threading.Thread(target=dl_url_and_send,
                         args=(cid, smsg.message_id, url, fmt, grp), daemon=True).start()
        return True
    return False


# ================= ТЕКСТ =================
@bot.message_handler(content_types=['text'])
def on_text(msg):
    try:
        if not msg.text or not msg.text.strip() or not msg.from_user:
            return

        # Промпт в ЛС
        if is_pm(msg):
            pk = f"pp_{msg.from_user.id}"
            with user_states_lock:
                gid = user_states.pop(pk, None)
            if gid is not None:
                if msg.text.lower().strip() == "отмена":
                    safe_send(msg.chat.id, "ладно~ ☺️", markup=main_kb()); return
                s = get_gs(gid)
                with settings_lock: s["custom_prompt"] = msg.text
                save_settings(); ref_prompt(gid, True); clr_hist(gid, True)
                safe_send(msg.chat.id, "✅ промпт обновлён! ✨", markup=main_kb()); return

        # Промпт в группе
        if is_grp(msg):
            sk = f"{msg.chat.id}_{msg.from_user.id}"
            with user_states_lock:
                state = user_states.pop(sk, None)
            if state == "wp":
                if msg.text.lower().strip() == "отмена":
                    safe_send(msg.chat.id, "ладно~ ☺️"); return
                if not is_admin(msg.chat.id, msg.from_user.id): return
                s = get_gs(msg.chat.id)
                with settings_lock: s["custom_prompt"] = msg.text
                save_settings(); ref_prompt(msg.chat.id, True); clr_hist(msg.chat.id, True)
                safe_send(msg.chat.id, "✅ промпт обновлён! ✨"); return

            s = get_gs(msg.chat.id)
            if s.get("owner_id") is None:
                with settings_lock:
                    s["owner_id"] = msg.from_user.id
                    s["owner_name"] = dname(msg.from_user)
                    s["group_name"] = msg.chat.title
                save_settings()
            if msg.chat.title and s.get("group_name") != msg.chat.title:
                with settings_lock: s["group_name"] = msg.chat.title
                save_settings()
            sync_group_users(msg.chat.id, msg.chat.title)
            if is_admin(msg.chat.id, msg.from_user.id):
                reg_group(msg.from_user.id, msg.chat.id, msg.chat.title)

        cid = msg.chat.id

        # Трек по номеру
        ts = msg.text.strip()
        if ts.isdigit():
            num = int(ts)
            if 1 <= num <= 8:
                pl = find_pending(cid)
                if pl:
                    lk, lv = max(pl, key=lambda x: x[1].get("time", datetime.min))
                    if 1 <= num <= len(lv.get("results", [])):
                        busy, bt = is_busy(cid)
                        if busy:
                            safe_send(cid, get_busy_reply(bt)); return
                        with pending_lock: pending_tracks.pop(lk, None)
                        track = lv["results"][num - 1]
                        set_busy(cid, "music", track['title'])
                        smsg = safe_send(cid, f"скачиваю {track['title']}... 🎵")
                        if not smsg:
                            clear_busy(cid); return
                        threading.Thread(target=dl_and_send,
                                         args=(cid, smsg.message_id, track, is_grp(msg)), daemon=True).start()
                        return

        # URL/музыка
        quick = quick_detect(msg.text)
        if quick:
            if is_grp(msg):
                rem_user(cid, msg.from_user)
                add_msg(cid, "user", f"[{dname(msg.from_user)}]: {msg.text}", True)
            if handle_media(msg, quick, is_grp(msg)):
                return

        # ЛС
        if is_pm(msg):
            uid = msg.from_user.id
            busy, bt = is_busy(cid)
            if busy:
                safe_send(cid, get_busy_reply(bt)); return
            bot.send_chat_action(cid, 'typing')
            add_msg(uid, "user", msg.text)
            intent = analyze_intent(msg.text)
            if intent.get("type") != "chat" and handle_media(msg, intent):
                return
            msgs = get_msgs_copy(uid)
            if need_search(msg.text):
                sd = add_search(msg.text)
                if sd and msgs:
                    msgs[-1] = {"role": "user", "content": msg.text + sd}
            resp = ask_ai(msgs)
            if not is_error(resp):
                resp = clean(resp); add_msg(uid, "assistant", resp)
            else:
                resp = resp.replace("[ERR]", "")
            send_long_msg(cid, resp, markup=main_kb())
            return

        # Группа
        if not is_grp(msg):
            return
        rem_user(cid, msg.from_user)
        uname = dname(msg.from_user)
        add_msg(cid, "user", f"[{uname}]: {msg.text}", True)
        last_activity[cid] = datetime.now()
        s = get_gs(cid)
        if s.get("proactive_enabled"):
            start_ptimer(cid)

        bi = get_bot_info()
        bu = bi.username.lower() if bi and bi.username else ""
        is_reply = (msg.reply_to_message and msg.reply_to_message.from_user and
                    bi and msg.reply_to_message.from_user.id == bi.id)
        is_mention = bu and f"@{bu}" in msg.text.lower()
        is_name = is_named(msg.text)
        direct = is_reply or is_mention or is_name

        if not direct:
            busy, _ = is_busy(cid)
            if busy or random.randint(1, 100) > s["response_chance"]:
                return
        busy, bt = is_busy(cid)
        if busy:
            if direct: safe_send(cid, get_busy_reply(bt))
            return

        bot.send_chat_action(cid, 'typing')
        intent = analyze_intent(msg.text)
        if intent.get("type") != "chat" and handle_media(msg, intent, True):
            return
        msgs = get_msgs_copy(cid, True)
        if need_search(msg.text):
            sd = add_search(msg.text)
            if sd and msgs:
                msgs[-1] = {"role": "user", "content": f"[{uname}]: {msg.text}{sd}"}
        resp = ask_ai(msgs)
        if not is_error(resp):
            resp = clean(resp); add_msg(cid, "assistant", resp, True)
        else:
            resp = resp.replace("[ERR]", "")
        send_long_msg(cid, resp)
    except Exception as e:
        log.error(f"Text err: {e}"); traceback.print_exc()


# ================= ОЧИСТКА =================
def cleanup_loop():
    while True:
        try:
            time.sleep(CLEANUP_INTERVAL)
            now = time.time()
            if os.path.exists(DOWNLOADS_DIR):
                for item in os.listdir(DOWNLOADS_DIR):
                    p = os.path.join(DOWNLOADS_DIR, item)
                    try:
                        if os.path.isdir(p) and now - os.path.getmtime(p) > 1800:
                            shutil.rmtree(p, ignore_errors=True)
                    except Exception:
                        pass
            cleanup_pending()
            with user_states_lock:
                dl = [k for k in user_states if k.startswith("dl_")]
                if len(dl) > 50:
                    for k in dl[:30]: user_states.pop(k, None)
        except Exception as e:
            log.error(f"Cleanup err: {e}")


# ================= ЗАПУСК =================
if __name__ == "__main__":
    print("=" * 50)
    print("    🌸 ХИНАТА БОТ — ЗАПУСК 🌸")
    print("=" * 50)

    bi = get_bot_info()
    if bi: log.info(f"Бот: @{bi.username}")
    else: log.warning("Не удалось получить информацию")

    log.info(f"FFmpeg: {'✅' if FFMPEG_AVAILABLE else '❌'}")
    log.info(f"Промпт: {len(DEFAULT_SYSTEM_PROMPT)} символов")
    log.info(f"Модель: {MODEL_ID}")
    log.info(f"Групп: {len(group_settings)}")

    cookies = os.path.join(SCRIPT_DIR, "cookies.txt")
    log.info(f"Cookies: {'✅' if os.path.exists(cookies) else '❌'}")

    restored = 0
    for ck, st in group_settings.items():
        try:
            gid = int(ck); gname = st.get("group_name", "Группа")
            if st.get("owner_id"):
                reg_group(st["owner_id"], gid, gname); restored += 1
            for aid in st.get("admins", {}):
                try: reg_group(int(aid), gid, gname)
                except Exception: pass
        except Exception: pass
    if restored: log.info(f"Восстановлено: {restored}")

    pc = 0
    for ck, st in group_settings.items():
        if st.get("proactive_enabled"):
            try: start_ptimer(int(ck)); pc += 1
            except Exception: pass
    if pc: log.info(f"Таймеров: {pc}")

    threading.Thread(target=cleanup_loop, daemon=True).start()

    print("=" * 50)
    print("    🌸 ХИНАТА РАБОТАЕТ! 🌸")
    print("=" * 50)

    while True:
        try:
            bot.infinity_polling(
                allowed_updates=["message", "callback_query", "my_chat_member"],
                timeout=60, long_polling_timeout=60)
        except KeyboardInterrupt:
            log.info("Остановлена~ 🌸"); break
        except Exception as e:
            log.error(f"Poll err: {e}"); time.sleep(5)
