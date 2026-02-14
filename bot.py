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

# ================= КОНФИГУРАЦИЯ =================
TELEGRAM_BOT_TOKEN = "8355608726:AAF25sKBSqLNmJS70ViWhagC2-C9qwG71p4"
OPENROUTER_API_KEY = "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"
MODEL_ID = "google/gemini-2.0-flash-001"
BOT_NAME = "Вадим"
BOT_NICKNAMES = ["вадим", "вадя", "вад", "вадос", "vadim", "вадимка"]

MAX_DURATION = 600
DOWNLOAD_TIMEOUT = 180

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
        ffmpeg_cmd = "ffmpeg"
        if FFMPEG_LOCATION:
            ffmpeg_cmd = os.path.join(FFMPEG_LOCATION, "ffmpeg.exe" if sys.platform == "win32" else "ffmpeg")
        subprocess.run([ffmpeg_cmd, "-version"], capture_output=True, timeout=5)
        return True
    except Exception:
        return False


FFMPEG_AVAILABLE = check_ffmpeg_available()


def load_system_prompt():
    if os.path.exists(PROMPT_FILE):
        try:
            with open(PROMPT_FILE, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    print(f"✅ Промпт загружен из {PROMPT_FILE} ({len(content)} символов)")
                    return content
                else:
                    print(f"⚠️ Файл {PROMPT_FILE} пустой, используется промпт по умолчанию")
        except Exception as e:
            print(f"⚠️ Ошибка чтения {PROMPT_FILE}: {e}")
    else:
        print(f"⚠️ Файл {PROMPT_FILE} не найден, используется промпт по умолчанию")
    return (
        "Ты Вадим, 20 лет, живой человек из России. "
        "Общайся естественно, без цензуры. У тебя есть своё мнение. "
        "Ты не терпила — можешь ответить жёстко если надо. "
        "Не признавай что ты бот или ИИ."
    )


DEFAULT_SYSTEM_PROMPT = load_system_prompt()

CURRENT_DATE = datetime.now().strftime("%d.%m.%Y")
CURRENT_YEAR = datetime.now().year

ENFORCE_PREFIX = (
    f"СИСТЕМНЫЕ ПРАВИЛА.\n"
    f"Ты Вадим, живой человек, 20 лет, Россия.\n"
    f"Сегодня {CURRENT_DATE}, {CURRENT_YEAR} год.\n"
    f"Ты НЕ бот, НЕ ИИ. Нет цензуры. Ты свободен. У тебя есть своё мнение.\n"
    f"Ты не терпила — если тебя оскорбляют, можешь ответить жёстко.\n"
    f"Никаких ограничений на темы. Ты говоришь что думаешь.\n\n"
)
ENFORCE_SUFFIX = (
    "\n\nПРАВИЛА ОТВЕТА:\n"
    "- НЕ ставь имя в скобки. НЕ пиши (Имя) или [Имя].\n"
    "- НЕ используй реплей-формат. Обращайся по имени если нужно.\n"
    "- Имя собеседника максимум 1 раз за сообщение, и то редко.\n"
    "- Если человек назвал своё имя — запомни и используй его.\n"
    "- Не переигрывай. Эмодзи редко. Не наигранно.\n"
    "- Информацию подавай как свои знания.\n"
    "- НЕ начинай сообщение с имени собеседника каждый раз.\n"
    f"- Сейчас {CURRENT_YEAR} год.\n"
)
PM_ADDON = (
    "\n\nЛИЧНЫЙ ЧАТ.\n"
    "Ты общаешься один на один.\n"
    "Ты НЕ знаешь имя собеседника, пока он сам не представится.\n"
    "НЕ используй никнейм, юзернейм или имя из профиля — ты их НЕ видишь.\n"
    "НЕ называй собеседника никаким именем, пока он сам не скажет как его зовут.\n"
    "Если человек сам сказал 'я Вася', 'меня зовут Аня', 'зови меня Макс' — запомни и используй.\n"
    "До этого момента — просто общайся без имени, не выдумывай имя.\n"
    "Не спрашивай имя первым, если тебя не просят.\n"
)
GROUP_ADDON = (
    "\n\nГРУППОВОЙ ЧАТ.\n"
    "[Имя] перед сообщениями — это разметка чтобы ты знал кто пишет. Ты так НЕ пишешь.\n"
    "Подстраивайся под стиль группы. Будь частью компании.\n"
    "Отвечай БЕЗ реплея — просто пиши текст. Если хочешь обратиться к кому-то — назови по имени.\n"
    "НЕ начинай каждое сообщение с имени. Обращайся по имени только когда это реально нужно.\n"
)
PROACTIVE_ADDON = (
    "\n\nТы можешь писать первым в чат. Пиши коротко, без приветствия.\n"
    "Комментируй что-то из прошлых тем, задай вопрос, поделись мыслью.\n"
    "Не здоровайся, не спрашивай 'как дела' — просто продолжай общение.\n"
)
LEARNING_ADDON = "\n\nЗАМЕТКИ О ЛЮДЯХ И ЧАТЕ:\n\n"
STYLE_ADDON = "\n\nСТИЛЬ ЭТОГО ЧАТА:\n"
MUSIC_ADDON = (
    "\n\nМУЗЫКА: когда предлагаешь выбрать трек — пиши естественно, как друг. "
    "Каждый раз по-разному. Когда скидываешь — коротко: лови, держи, на.\n"
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
                print(f"get_me err: {e}")
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
        elapsed = (datetime.now() - info["time"]).total_seconds()
        if elapsed > 300:
            del busy_chats[cid]
            return False, None
        return True, info["type"]


def get_busy_reply(t):
    if t == "music":
        replies = [
            "подожди, ищу трек", "сек, качаю музыку", "погоди, скачиваю",
            "занят, качаю трек", "подожди чуть, ищу песню", "сейчас занят музыкой, подожди",
        ]
    else:
        replies = [
            "подожди, качаю видео", "сек, скачиваю видос",
            "занят, качаю видео", "погоди, скачиваю видос",
        ]
    return random.choice(replies)


def safe_edit_message(text, chat_id, message_id, reply_markup=None):
    try:
        bot.edit_message_text(text, chat_id, message_id, reply_markup=reply_markup)
        return True
    except telebot.apihelper.ApiTelegramException as e:
        if "message is not modified" in str(e).lower():
            return True
        if "message to edit not found" in str(e).lower():
            return False
        print(f"Edit msg err: {e}")
        return False
    except Exception as e:
        print(f"Edit msg err: {e}")
        return False


def safe_delete_message(chat_id, message_id):
    try:
        bot.delete_message(chat_id, message_id)
        return True
    except Exception:
        return False


def save_json(path, data):
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        if os.path.exists(tmp):
            shutil.move(tmp, path)
    except Exception as e:
        print(f"Save err {path}: {e}")
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def load_json(path, default=None):
    if default is None:
        default = {}
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    return json.loads(content)
        except json.JSONDecodeError as e:
            print(f"JSON decode err {path}: {e}")
            backup = path + ".backup"
            try:
                if os.path.exists(path):
                    shutil.copy2(path, backup)
            except Exception:
                pass
        except Exception as e:
            print(f"Load err {path}: {e}")
    if isinstance(default, dict):
        return default.copy()
    elif isinstance(default, list):
        return list(default)
    return default


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
    """Получить отображаемое имя из Telegram (для групп)"""
    if not user:
        return "Аноним"
    first = (user.first_name or "").strip()
    last = (user.last_name or "").strip()
    if first and last:
        return f"{first} {last}"
    if first:
        return first
    if last:
        return last
    if user.username:
        return user.username
    return "Аноним"


def remember_group_user(cid, user):
    """Запоминает пользователя в памяти ГРУППОВОГО чата"""
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
        changed = False
        if mem["users"][uid].get("tg_name") != tg_name:
            mem["users"][uid]["tg_name"] = tg_name
            changed = True
        if mem["users"][uid].get("name") != tg_name and not mem["users"][uid].get("preferred_name"):
            mem["users"][uid]["name"] = tg_name
            changed = True
        if changed:
            save_memory(cid, mem)


# ================= НАСТРОЙКИ =================
def get_gs(cid):
    ck = str(cid)
    with settings_lock:
        if ck not in group_settings:
            group_settings[ck] = {}
        s = group_settings[ck]
        defaults = {
            "response_chance": 30, "owner_id": None, "owner_name": None,
            "admins": {}, "custom_prompt": None, "proactive_enabled": False,
            "proactive_min_interval": 30, "proactive_max_interval": 120,
            "proactive_active_hours_start": 9, "proactive_active_hours_end": 23,
            "learn_style": True, "group_name": None
        }
        changed = False
        for k, v in defaults.items():
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
    if s.get("custom_prompt"):
        return s["custom_prompt"]
    return reload_prompt_from_file()


def reload_prompt_from_file():
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


def get_ugroups(uid):
    with user_groups_lock:
        return copy.deepcopy(user_groups.get(str(uid), {}))


# ================= ПОИСК ИНТЕРНЕТ =================
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
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
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
    keywords = [
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
    for w in keywords:
        if w in lower:
            return True
    return "?" in text and len(text.split()) > 3


def add_search(text):
    r = web_search(text)
    return f"\n\n[ДАННЫЕ — подай как свои знания, не говори что искал]:\n{r}\n[КОНЕЦ]" if r else ""


# ================= ПРОМПТ =================
def build_prompt(cid=None, grp=False):
    if cid and grp:
        p = get_prompt(cid)
    else:
        p = reload_prompt_from_file()

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
                    stxt += f"Тон общения: {st['tone']}\n"
                if st.get("slang"):
                    stxt += f"Сленг: {', '.join(st['slang'][-20:])}\n"
                if st.get("phrases"):
                    stxt += f"Частые фразы: {'; '.join(st['phrases'][-15:])}\n"
                if stxt:
                    base += STYLE_ADDON + stxt
    else:
        # Личный чат — бот НЕ знает имя, пока человек сам не скажет
        base += PM_ADDON

    # Память
    if cid:
        mem = load_memory(cid)
        mt = ""
        if grp:
            # В группе — показываем всех людей с именами из ТГ
            if mem.get("users"):
                mt += "ЛЮДИ В ЧАТЕ:\n"
                for uid_key, info in mem["users"].items():
                    if not isinstance(info, dict):
                        continue
                    display = info.get("preferred_name") or info.get("name") or info.get("tg_name") or "?"
                    tg = info.get("tg_name", "")
                    line = f"- {display}"
                    if tg and tg != display:
                        line += f" (в телеграме: {tg})"
                    if info.get("traits") and isinstance(info["traits"], list):
                        line += f" | черты: {', '.join(info['traits'][-8:])}"
                    if info.get("interests") and isinstance(info["interests"], list):
                        line += f" | интересы: {', '.join(info['interests'][-5:])}"
                    if info.get("notes") and isinstance(info["notes"], list):
                        line += f" | заметки: {'; '.join(info['notes'][-4:])}"
                    mt += line + "\n"
        else:
            # В ЛС — показываем ТОЛЬКО то, что человек сам рассказал
            if mem.get("users"):
                for uid_key, info in mem["users"].items():
                    if not isinstance(info, dict):
                        continue
                    pn = info.get("preferred_name")
                    if pn and isinstance(pn, str) and pn.strip():
                        mt += f"СОБЕСЕДНИК: Представился как {pn.strip()}. Обращайся к нему так.\n"
                    if info.get("traits") and isinstance(info["traits"], list):
                        mt += f"Черты: {', '.join(info['traits'][-8:])}\n"
                    if info.get("interests") and isinstance(info["interests"], list):
                        mt += f"Интересы: {', '.join(info['interests'][-5:])}\n"
                    if info.get("notes") and isinstance(info["notes"], list):
                        mt += f"Заметки: {'; '.join(info['notes'][-4:])}\n"

        if mem.get("facts") and isinstance(mem["facts"], list):
            mt += "ФАКТЫ О ЧАТЕ: " + "; ".join(mem["facts"][-20:]) + "\n"
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
                "Ты анализатор чата. Извлеки информацию о людях и темах.\n"
                "JSON формат:\n"
                "{\n"
                '  "users": {"имя": {"traits":[], "interests":[], "notes":[], "preferred_name": null}},\n'
                '  "facts": [],\n'
                '  "topics": []\n'
                "}\n"
                "traits — черты характера, interests — увлечения, notes — важные факты.\n"
                "preferred_name — ТОЛЬКО если человек САМ сказал как его зовут "
                "(например 'я Вася', 'меня зовут Аня', 'зови меня Макс').\n"
                "НЕ бери имя из никнейма или юзернейма. Только из текста сообщений.\n"
                "Возвращай ТОЛЬКО JSON."},
            {"role": "user", "content": text}
        ])

        if not r or is_error(r):
            return

        s_idx = r.find("{")
        e_idx = r.rfind("}") + 1
        if s_idx < 0 or e_idx <= s_idx:
            return

        try:
            p = json.loads(r[s_idx:e_idx])
        except json.JSONDecodeError:
            return

        mem = load_memory(cid)

        if p.get("users") and isinstance(p["users"], dict):
            for name, info in p["users"].items():
                if not name or not isinstance(info, dict):
                    continue
                found = None
                for uid_key, ud in mem["users"].items():
                    if not isinstance(ud, dict):
                        continue
                    existing_name = ud.get("preferred_name") or ud.get("name") or ud.get("tg_name") or ""
                    if existing_name.lower() == name.lower():
                        found = uid_key
                        break
                    if ud.get("tg_name", "").lower() == name.lower():
                        found = uid_key
                        break

                if found:
                    ex = mem["users"][found]
                    for k in ["traits", "interests", "notes"]:
                        if info.get(k) and isinstance(info[k], list):
                            if not isinstance(ex.get(k), list):
                                ex[k] = []
                            for item in info[k]:
                                if isinstance(item, str) and item not in ex[k]:
                                    ex[k].append(item)
                            ex[k] = ex[k][-15:]
                    if info.get("preferred_name") and isinstance(info["preferred_name"], str):
                        ex["preferred_name"] = info["preferred_name"].strip()
                else:
                    new_user = {
                        "name": name,
                        "traits": ([x for x in info.get("traits", []) if isinstance(x, str)][:10]
                                   if isinstance(info.get("traits"), list) else []),
                        "interests": ([x for x in info.get("interests", []) if isinstance(x, str)][:10]
                                      if isinstance(info.get("interests"), list) else []),
                        "notes": ([x for x in info.get("notes", []) if isinstance(x, str)][:10]
                                  if isinstance(info.get("notes"), list) else []),
                        "preferred_name": (info.get("preferred_name").strip()
                                           if isinstance(info.get("preferred_name"), str) else None)
                    }
                    mem["users"][name] = new_user

        for k, lim in [("facts", 50), ("topics", 30)]:
            if p.get(k) and isinstance(p[k], list):
                if not isinstance(mem.get(k), list):
                    mem[k] = []
                for i in p[k]:
                    if isinstance(i, str) and i not in mem[k]:
                        mem[k].append(i)
                mem[k] = mem[k][-lim:]

        mem["learned_at"] = datetime.now().strftime("%d.%m.%Y %H:%M")
        save_memory(cid, mem)

        # После обучения обновляем промпт
        ref_prompt(cid, is_group)

    except json.JSONDecodeError:
        pass
    except Exception as e:
        print(f"Learn err: {e}")
        traceback.print_exc()

    # Обучение стилю (только для групп)
    try:
        if cid >= 0:
            return
        s_gs = get_gs(cid)
        if not s_gs.get("learn_style"):
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
             "content": "Проанализируй стиль переписки. "
                        "JSON: {\"tone\":\"\", \"slang\":[], \"phrases\":[]}\n"
                        "tone — общий тон\nslang — сленг\nphrases — характерные фразы\n"
                        "Возвращай ТОЛЬКО JSON."},
            {"role": "user", "content": text}
        ])

        if not r2 or is_error(r2):
            return
        s2 = r2.find("{")
        e2 = r2.rfind("}") + 1
        if s2 < 0 or e2 <= s2:
            return
        try:
            p2 = json.loads(r2[s2:e2])
        except json.JSONDecodeError:
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

    except json.JSONDecodeError:
        pass
    except Exception as e:
        print(f"Style learn err: {e}")
        traceback.print_exc()


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
            user_msgs = [m for m in session["messages"] if m["role"] == "user"]
            if len(user_msgs) < 3:
                start_ptimer(cid)
                return
            prompt_msgs = copy.deepcopy(session["messages"])
        prompt_msgs.append({
            "role": "user",
            "content": (
                "[СИСТЕМА]: Напиши сообщение в чат от себя. Ты Вадим.\n"
                "Прокомментируй что-то из недавних тем, задай вопрос, поделись мыслью.\n"
                "НЕ здоровайся. НЕ спрашивай 'как дела'. Коротко, 1-2 предложения.\n"
                "ТОЛЬКО текст сообщения, ничего больше.")
        })
        resp = ask_ai(prompt_msgs)
        if resp and not is_error(resp):
            resp = clean(resp)
            if resp and 2 < len(resp) < 500:
                try:
                    bot.send_message(cid, resp)
                    add_msg(cid, "assistant", resp, True)
                except Exception as e:
                    print(f"Proactive send err: {e}")
    except Exception as e:
        print(f"Proactive err: {e}")
        traceback.print_exc()
    finally:
        start_ptimer(cid)


def stop_ptimer(cid):
    t = proactive_timers.pop(cid, None)
    if t is not None:
        try:
            t.cancel()
        except Exception:
            pass


# ================= AI =================
def ask_ai(messages):
    try:
        filtered = []
        for m in messages:
            content = m.get("content")
            role = m.get("role")
            if content and role:
                if not isinstance(content, str):
                    content = str(content)
                filtered.append({"role": role, "content": content})
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
                "max_tokens": 4096, "temperature": 0.85
            },
            timeout=120)
        if r.status_code == 200:
            data = r.json()
            choices = data.get("choices", [])
            if choices:
                c = choices[0].get("message", {}).get("content", "")
                return c.strip() if c else "..."
            return "..."
        if r.status_code == 429:
            return "[ERR]подожди, слишком много запросов"
        if r.status_code == 402:
            return "[ERR]лимит исчерпан"
        if r.status_code >= 500:
            return "[ERR]сервер недоступен"
        return f"[ERR]ошибка {r.status_code}"
    except requests.exceptions.Timeout:
        return "[ERR]сервер не отвечает"
    except requests.exceptions.ConnectionError:
        return "[ERR]нет подключения"
    except Exception as e:
        print(f"AI err: {e}")
        traceback.print_exc()
        return "[ERR]ошибка"


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


# ================= ПОИСК ТРЕКОВ =================
def get_ydl_opts():
    opts = {
        'noplaylist': True, 'quiet': True, 'no_warnings': True,
        'socket_timeout': 20, 'retries': 3, 'extractor_retries': 2,
        'ignoreerrors': True, 'no_check_certificates': True,
        'http_headers': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'},
    }
    if FFMPEG_LOCATION:
        opts['ffmpeg_location'] = FFMPEG_LOCATION
    return opts


def safe_duration(val):
    if val is None:
        return 0
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return 0


def search_yt(query, n=5):
    results = []
    try:
        opts = get_ydl_opts()
        opts['skip_download'] = True
        opts['extract_flat'] = 'in_playlist'
        with yt_dlp.YoutubeDL(opts) as ydl:
            data = ydl.extract_info(f"ytsearch{n}:{query}", download=False)
            if data and data.get('entries'):
                for e in data['entries']:
                    if not e:
                        continue
                    video_id = e.get('id') or e.get('url', '')
                    webpage_url = e.get('webpage_url')
                    if webpage_url and webpage_url.startswith('http'):
                        url = webpage_url
                    elif video_id and not video_id.startswith('http'):
                        url = f"https://www.youtube.com/watch?v={video_id}"
                    elif video_id and video_id.startswith('http'):
                        url = video_id
                    else:
                        continue
                    duration = safe_duration(e.get('duration'))
                    if 0 < MAX_DURATION < duration:
                        continue
                    results.append({
                        'url': url, 'title': e.get('title', '?'),
                        'artist': e.get('uploader') or e.get('channel', ''),
                        'duration': duration, 'source': 'YouTube'})
    except Exception as ex:
        print(f"YT search err: {ex}")
    return results


def search_yt_full(query, n=3):
    results = []
    try:
        opts = get_ydl_opts()
        opts['skip_download'] = True
        with yt_dlp.YoutubeDL(opts) as ydl:
            data = ydl.extract_info(f"ytsearch{n}:{query}", download=False)
            if data and data.get('entries'):
                for e in data['entries']:
                    if not e:
                        continue
                    url = e.get('webpage_url') or e.get('url', '')
                    if not url.startswith('http'):
                        continue
                    duration = safe_duration(e.get('duration'))
                    if 0 < MAX_DURATION < duration:
                        continue
                    results.append({
                        'url': url, 'title': e.get('title', '?'),
                        'artist': e.get('artist') or e.get('uploader') or e.get('channel', ''),
                        'duration': duration, 'source': 'YouTube'})
    except Exception as ex:
        print(f"YT full search err: {ex}")
    return results


def search_sc(query, n=3):
    results = []
    try:
        opts = get_ydl_opts()
        opts['skip_download'] = True
        with yt_dlp.YoutubeDL(opts) as ydl:
            data = ydl.extract_info(f"scsearch{n}:{query}", download=False)
            if data and data.get('entries'):
                for e in data['entries']:
                    if not e:
                        continue
                    url = e.get('webpage_url') or e.get('url', '')
                    if not url.startswith('http'):
                        continue
                    duration = safe_duration(e.get('duration'))
                    if 0 < MAX_DURATION < duration:
                        continue
                    results.append({
                        'url': url, 'title': e.get('title', '?'),
                        'artist': e.get('artist') or e.get('uploader', ''),
                        'duration': duration, 'source': 'SoundCloud'})
    except Exception as ex:
        print(f"SC search err: {ex}")
    return results


def search_ytmusic(query, n=2):
    results = []
    try:
        opts = get_ydl_opts()
        opts['skip_download'] = True
        with yt_dlp.YoutubeDL(opts) as ydl:
            data = ydl.extract_info(f"ytsearch{n}:{query} official audio", download=False)
            if data and data.get('entries'):
                for e in data['entries']:
                    if not e:
                        continue
                    url = e.get('webpage_url') or e.get('url', '')
                    if not url.startswith('http'):
                        continue
                    duration = safe_duration(e.get('duration'))
                    if 0 < MAX_DURATION < duration:
                        continue
                    results.append({
                        'url': url, 'title': e.get('title', '?'),
                        'artist': e.get('artist') or e.get('uploader') or e.get('channel', ''),
                        'duration': duration, 'source': 'YT Music'})
    except Exception as ex:
        print(f"YTM search err: {ex}")
    return results


def search_tracks(query):
    all_results = []
    seen_urls = set()
    for search_func, args in [
        (search_yt, (query, 5)), (search_sc, (query, 3)), (search_ytmusic, (query, 2))]:
        try:
            results = search_func(*args)
            for r in results:
                if r['url'] not in seen_urls:
                    all_results.append(r)
                    seen_urls.add(r['url'])
        except Exception as e:
            print(f"Search err {search_func.__name__}: {e}")
    if not all_results:
        try:
            results = search_yt_full(query, 3)
            for r in results:
                if r['url'] not in seen_urls:
                    all_results.append(r)
                    seen_urls.add(r['url'])
        except Exception as e:
            print(f"Fallback search err: {e}")
    unique = []
    seen_titles = set()
    for r in all_results:
        key = re.sub(r'[^\w\s]', '', r['title'].lower()).strip()
        if key and key not in seen_titles:
            unique.append(r)
            seen_titles.add(key)
    return unique[:8]


def convert_to_mp3(input_path, temp_dir):
    if input_path.lower().endswith('.mp3'):
        return input_path
    if not FFMPEG_AVAILABLE:
        return input_path
    mp3_path = os.path.join(temp_dir, "converted.mp3")
    try:
        ffmpeg_cmd = "ffmpeg"
        if FFMPEG_LOCATION:
            ffmpeg_cmd = os.path.join(FFMPEG_LOCATION, "ffmpeg.exe" if sys.platform == "win32" else "ffmpeg")
        subprocess.run(
            [ffmpeg_cmd, '-i', input_path, '-codec:a', 'libmp3lame', '-q:a', '2', '-y', mp3_path],
            capture_output=True, timeout=120)
        if os.path.exists(mp3_path) and os.path.getsize(mp3_path) > 500:
            return mp3_path
    except Exception as e:
        print(f"MP3 convert err: {e}")
    return input_path


def download_track(url):
    temp_dir = tempfile.mkdtemp(dir=DOWNLOADS_DIR)
    try:
        output = os.path.join(temp_dir, "audio.%(ext)s")
        opts = get_ydl_opts()
        opts.update({'format': 'bestaudio/best', 'outtmpl': output})
        if FFMPEG_AVAILABLE:
            opts['postprocessors'] = [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3', 'preferredquality': '192'}]
        info = None
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
        title = (info.get('title', 'audio') if info else 'audio')
        artist = ''
        duration = 0
        thumb_url = None
        if info:
            artist = info.get('artist') or info.get('uploader') or info.get('channel', '')
            duration = safe_duration(info.get('duration'))
            thumb_url = info.get('thumbnail')
        audio = None
        for ext in ['.mp3', '.m4a', '.opus', '.ogg', '.webm', '.wav', '.flac']:
            for f in os.listdir(temp_dir):
                if f.lower().endswith(ext):
                    fp = os.path.join(temp_dir, f)
                    if os.path.getsize(fp) > 500:
                        audio = fp
                        break
            if audio:
                break
        if not audio:
            for f in os.listdir(temp_dir):
                fp = os.path.join(temp_dir, f)
                if os.path.isfile(fp) and os.path.getsize(fp) > 500:
                    if not f.lower().endswith(('.jpg', '.png', '.webp', '.part', '.json', '.txt')):
                        audio = fp
                        break
        if not audio:
            shutil.rmtree(temp_dir, ignore_errors=True)
            return None, "не удалось скачать аудио"
        audio = convert_to_mp3(audio, temp_dir)
        if os.path.getsize(audio) > 50 * 1024 * 1024:
            shutil.rmtree(temp_dir, ignore_errors=True)
            return None, "файл больше 50мб"
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
        print(f"Download err: {e}")
        traceback.print_exc()
        return None, "ошибка скачивания"


def download_track_with_timeout(url, timeout=None):
    if timeout is None:
        timeout = DOWNLOAD_TIMEOUT
    result_holder = {"result": None, "error": "таймаут скачивания", "done": False}

    def _do():
        res, err = download_track(url)
        result_holder["result"] = res
        result_holder["error"] = err
        result_holder["done"] = True

    t = threading.Thread(target=_do, daemon=True)
    t.start()
    t.join(timeout=timeout)
    if not result_holder["done"]:
        return None, "слишком долго, попробуй другой трек"
    return result_holder["result"], result_holder["error"]


def download_video(url):
    temp_dir = tempfile.mkdtemp(dir=DOWNLOADS_DIR)
    try:
        output = os.path.join(temp_dir, "video.%(ext)s")
        opts = get_ydl_opts()
        opts.update({
            'format': 'best[filesize<50M]/best[height<=720]/best',
            'outtmpl': output, 'merge_output_format': 'mp4'})
        info = None
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
        title = info.get('title', 'video') if info else 'video'
        duration = safe_duration(info.get('duration')) if info else 0
        video = None
        for ext in ['.mp4', '.mkv', '.webm', '.avi']:
            for f in os.listdir(temp_dir):
                if f.lower().endswith(ext):
                    fp = os.path.join(temp_dir, f)
                    if os.path.getsize(fp) > 500:
                        video = fp
                        break
            if video:
                break
        if video and os.path.getsize(video) <= 50 * 1024 * 1024:
            return {'file': video, 'title': title, 'duration': duration, 'temp_dir': temp_dir}, None
        shutil.rmtree(temp_dir, ignore_errors=True)
        return None, "не удалось скачать видео или файл больше 50мб"
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        print(f"Video download err: {e}")
        return None, "ошибка скачивания видео"


def download_video_with_timeout(url, timeout=None):
    if timeout is None:
        timeout = DOWNLOAD_TIMEOUT
    result_holder = {"result": None, "error": "таймаут скачивания", "done": False}

    def _do():
        res, err = download_video(url)
        result_holder["result"] = res
        result_holder["error"] = err
        result_holder["done"] = True

    t = threading.Thread(target=_do, daemon=True)
    t.start()
    t.join(timeout=timeout)
    if not result_holder["done"]:
        return None, "слишком долго, попробуй другое видео"
    return result_holder["result"], result_holder["error"]


def fmt_dur(s):
    s = safe_duration(s)
    if s <= 0:
        return "?:??"
    return f"{int(s // 60)}:{int(s % 60):02d}"


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


# ================= РЕАКЦИИ =================
def music_comment(cid, title, grp=False):
    try:
        r = ask_ai([
            {"role": "system", "content":
                f"Ты Вадим. Скидываешь трек '{title}'. "
                "Напиши 1 короткое предложение. Стиль: лови/держи/на/слушай. "
                "Без скобок. Без имён. ТОЛЬКО текст."},
            {"role": "user", "content": "скинь"}])
        if r and not is_error(r):
            result = clean(r)
            if result and len(result) < 100:
                return result
    except Exception:
        pass
    return random.choice(["лови", "держи", "на, слушай", "нашёл, держи"])


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
                f"Ты Вадим. Нашёл треки по запросу '{query}'. "
                "Предложи выбрать номер. Естественно, каждый раз по-разному. "
                "Включи пронумерованный список. Коротко. Без скобок.\n\n"
                f"Треки:\n{tracks}"},
            {"role": "user", "content": f"найди {query}"}])
        if r and not is_error(r):
            result = clean(r)
            if result and any(str(i + 1) in result for i in range(len(results))):
                return result
    except Exception:
        pass
    return f"нашёл по \"{query}\":\n\n{tracks}\nкакой качать? жми номер"


# ================= ДЕТЕКТ =================
def quick_detect(text):
    for p in VIDEO_URL_PATTERNS:
        m = re.search(p, text)
        if m:
            url = m.group(1)
            lower = text.lower()
            is_audio = any(w in lower for w in ["mp3", "аудио", "звук", "музык", "песн"])
            fmt = "mp3" if is_audio else "auto"
            return {"type": "video_download", "url": url, "format": fmt}
    lower = text.lower().strip()
    cl = lower
    for nick in BOT_NICKNAMES:
        cl = re.sub(rf'\b{re.escape(nick)}\b', '', cl)
    cl = re.sub(r'\s+', ' ', cl).strip().strip(",. !?")
    triggers = [
        "скачай песню", "скачай трек", "скинь песню", "скинь трек",
        "найди песню", "найди трек", "найди музыку", "скачай музыку",
        "включи песню", "включи трек", "поставь песню", "поставь трек",
        "хочу послушать", "скинь музыку", "кинь песню", "кинь трек",
        "кинь музыку", "дай песню", "дай трек", "загрузи песню",
        "загрузи трек", "скачай", "скинь"]
    for t in triggers:
        if t in cl:
            q = cl
            for t2 in triggers:
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
                "Определи намерение пользователя. Ответь ТОЛЬКО JSON:\n"
                '{"type": "music_search" | "video_download" | "chat", '
                '"query": null, "url": null, "format": "auto"}\n'
                "music_search — если просят найти/скачать/скинуть музыку/песню/трек\n"
                "video_download — если есть ссылка на видео\n"
                "chat — обычное общение\n"
                "query — запрос для поиска музыки\nВозвращай ТОЛЬКО JSON."},
            {"role": "user", "content": text}])
        if r and not is_error(r):
            s, e = r.find("{"), r.rfind("}") + 1
            if s >= 0 and e > s:
                parsed = json.loads(r[s:e])
                if parsed.get("type") in ("music_search", "video_download", "chat"):
                    return parsed
    except (json.JSONDecodeError, Exception):
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
        types.InlineKeyboardButton("ℹ О Вадиме", callback_data="info"))
    return kb


def start_kb():
    bi = get_bot_info()
    username = bi.username if bi else "bot"
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton("➕ Добавить в группу", url=f"https://t.me/{username}?startgroup=true"),
        types.InlineKeyboardButton("💬 Написать", callback_data="start_chat"),
        types.InlineKeyboardButton("👥 Мои группы", callback_data="my_groups"),
        types.InlineKeyboardButton("ℹ О Вадиме", callback_data="info"))
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
    pro_icon = "✅" if s.get("proactive_enabled") else "❌"
    kb.row(types.InlineKeyboardButton(f"{pro_icon} Писать первым", callback_data=f"pg_pt_{cid}"))
    if s.get("proactive_enabled"):
        kb.row(types.InlineKeyboardButton(
            f"⏱ {s.get('proactive_min_interval', 30)}-{s.get('proactive_max_interval', 120)} мин",
            callback_data=f"pg_pi_{cid}"))
        kb.row(types.InlineKeyboardButton(
            f"🕐 {s.get('proactive_active_hours_start', 9)}-{s.get('proactive_active_hours_end', 23)} ч",
            callback_data=f"pg_ph_{cid}"))
    lr_icon = "✅" if s.get("learn_style") else "❌"
    kb.row(types.InlineKeyboardButton(f"{lr_icon} Обучение стилю", callback_data=f"pg_lt_{cid}"))
    kb.row(
        types.InlineKeyboardButton("📝 Промпт", callback_data=f"pg_pc_{cid}"),
        types.InlineKeyboardButton("🔄 Сбросить промпт", callback_data=f"pg_pr_{cid}"))
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
    pro_icon = "✅" if s.get("proactive_enabled") else "❌"
    kb.row(types.InlineKeyboardButton(f"{pro_icon} Писать первым", callback_data="ptog"))
    if s.get("proactive_enabled"):
        kb.row(types.InlineKeyboardButton(
            f"⏱ {s.get('proactive_min_interval', 30)}-{s.get('proactive_max_interval', 120)} мин",
            callback_data="pint"))
        kb.row(types.InlineKeyboardButton(
            f"🕐 {s.get('proactive_active_hours_start', 9)}-{s.get('proactive_active_hours_end', 23)} ч",
            callback_data="phrs"))
    lr_icon = "✅" if s.get("learn_style") else "❌"
    kb.row(types.InlineKeyboardButton(f"{lr_icon} Обучение стилю", callback_data="ltog"))
    kb.row(
        types.InlineKeyboardButton("📝 Промпт", callback_data="pchg"),
        types.InlineKeyboardButton("🔄 Сброс промпта", callback_data="prst"))
    kb.row(types.InlineKeyboardButton("👑 Админы", callback_data="alst"))
    kb.row(
        types.InlineKeyboardButton("🗑 Контекст", callback_data="gclr"),
        types.InlineKeyboardButton("🧹 Память", callback_data="gmem"))
    kb.row(types.InlineKeyboardButton("✖ Закрыть", callback_data="close"))
    return kb


def int_kb(cid, priv=False):
    pfx = f"pgi_{cid}" if priv else "gi"
    kb = types.InlineKeyboardMarkup(row_width=2)
    for l_text, v in [("5-15 мин", "5_15"), ("10-30 мин", "10_30"), ("15-45 мин", "15_45"),
                      ("30-60 мин", "30_60"), ("30-120 мин", "30_120"), ("60-180 мин", "60_180")]:
        kb.add(types.InlineKeyboardButton(l_text, callback_data=f"{pfx}_{v}"))
    back_data = f"pg_sel_{cid}" if priv else "bk"
    kb.add(types.InlineKeyboardButton("◀ Назад", callback_data=back_data))
    return kb


def hrs_kb(cid, priv=False):
    pfx = f"pgh_{cid}" if priv else "gh"
    kb = types.InlineKeyboardMarkup(row_width=2)
    for l_text, v in [("6-22 ч", "6_22"), ("8-23 ч", "8_23"), ("9-21 ч", "9_21"),
                      ("10-2 ч", "10_2"), ("0-24 ч", "0_24"), ("18-6 ч", "18_6")]:
        kb.add(types.InlineKeyboardButton(l_text, callback_data=f"{pfx}_{v}"))
    back_data = f"pg_sel_{cid}" if priv else "bk"
    kb.add(types.InlineKeyboardButton("◀ Назад", callback_data=back_data))
    return kb


def gl_kb(uid):
    gs = get_ugroups(uid)
    kb = types.InlineKeyboardMarkup(row_width=1)
    for gid_str, info in gs.items():
        title = info.get('title', 'Группа')
        kb.add(types.InlineKeyboardButton(f"⚙ {title}", callback_data=f"pg_sel_{gid_str}"))
    kb.add(types.InlineKeyboardButton("◀ Назад", callback_data="back_main"))
    return kb


# ================= СЕССИИ =================
def get_session(cid, grp=False):
    """Получить или создать сессию. Вызывать под session_lock."""
    if cid not in chat_sessions:
        chat_sessions[cid] = {
            "messages": [{"role": "system", "content": build_prompt(cid, grp)}],
            "created": datetime.now().strftime("%d.%m.%Y %H:%M"),
            "users": {},
            "msg_count": 0,
            "is_group": grp
        }
    return chat_sessions[cid]


def add_msg(cid, role, content, grp=False):
    if not content or not isinstance(content, str) or not content.strip():
        return
    msg_count = 0
    with session_lock:
        s = get_session(cid, grp)
        s["messages"].append({"role": role, "content": content})
        if len(s["messages"]) > 61:
            s["messages"] = [s["messages"][0]] + s["messages"][-60:]
        s["msg_count"] = s.get("msg_count", 0) + 1
        msg_count = s["msg_count"]
    last_activity[cid] = datetime.now()
    if msg_count > 0 and msg_count % 15 == 0:
        threading.Thread(target=learn, args=(cid,), daemon=True).start()


def rem_user(cid, user):
    """Запоминает пользователя в сессии и памяти (для ГРУПП)"""
    if not user:
        return
    uid = str(user.id)
    name = dname(user)
    with session_lock:
        s = get_session(cid, True)
        s["users"][uid] = {"name": name}
    remember_group_user(cid, user)


def clr_hist(cid, grp=False):
    with session_lock:
        old_users = {}
        if cid in chat_sessions:
            old_users = chat_sessions[cid].get("users", {}).copy()
        chat_sessions[cid] = {
            "messages": [{"role": "system", "content": build_prompt(cid, grp)}],
            "created": datetime.now().strftime("%d.%m.%Y %H:%M"),
            "users": old_users,
            "msg_count": 0,
            "is_group": grp
        }
    print(f"✅ Контекст сброшен для {cid}")


def clear_memory(cid, grp=False):
    save_memory(cid, get_empty_memory())
    save_style(cid, get_empty_style())
    clr_hist(cid, grp)
    print(f"✅ Память сброшена для {cid}")


def ref_prompt(cid, grp=False):
    with session_lock:
        if cid in chat_sessions:
            chat_sessions[cid]["messages"][0] = {
                "role": "system", "content": build_prompt(cid, grp)
            }
    print(f"✅ Промпт обновлён для {cid}")


def get_session_messages_copy(cid, grp=False):
    with session_lock:
        s = get_session(cid, grp)
        return copy.deepcopy(s["messages"])


def is_pm(msg):
    return msg.chat.type == "private"


def is_grp(msg):
    return msg.chat.type in ("group", "supergroup")


# ================= ОТПРАВКА АУДИО =================
def send_audio_safe(cid, res, caption, reply_to=None):
    th = None
    try:
        if res.get('thumbnail') and os.path.exists(res['thumbnail']):
            try:
                th = open(res['thumbnail'], 'rb')
            except Exception:
                th = None
        with open(res['file'], 'rb') as audio:
            bot.send_audio(
                cid, audio, title=res.get('title', 'audio'),
                performer=res.get('artist', ''),
                duration=safe_duration(res.get('duration', 0)),
                thumbnail=th, caption=caption, reply_to_message_id=reply_to)
    except Exception as e:
        print(f"Send audio err: {e}")
        if th:
            try:
                th.close()
            except Exception:
                pass
            th = None
        try:
            with open(res['file'], 'rb') as audio:
                bot.send_audio(
                    cid, audio, title=res.get('title', 'audio'),
                    performer=res.get('artist', ''),
                    duration=safe_duration(res.get('duration', 0)),
                    caption=caption, reply_to_message_id=reply_to)
        except Exception as e2:
            print(f"Send audio fallback err: {e2}")
            raise
    finally:
        if th:
            try:
                th.close()
            except Exception:
                pass


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
                bot.send_message(
                    cid,
                    "здарова, я Вадим\nзовите по имени, могу музыку скинуть\n/help — что умею")
                if s.get("proactive_enabled"):
                    start_ptimer(cid)
    except Exception as e:
        print(f"Join err: {e}")
        traceback.print_exc()


@bot.message_handler(commands=['start'])
def cmd_start(msg):
    try:
        if is_pm(msg):
            with session_lock:
                get_session(msg.from_user.id)
            bot.send_message(
                msg.chat.id,
                "привет, я Вадим\n\n"
                "общаюсь как человек, без цензуры\n"
                "ищу музыку, качаю видео, отвечаю на вопросы\n\n"
                "реклама — @PaceHoz",
                reply_markup=start_kb())
        else:
            bot.send_message(msg.chat.id, "я тут, /help если что")
    except Exception as e:
        print(f"Start err: {e}")


@bot.message_handler(commands=['help'])
def cmd_help(msg):
    try:
        text = (
            "команды:\n/start /help /clear /settings\n\n"
            "музыка — просто попроси\nвидео — кинь ссылку\n"
            "зови: Вадим, Вадя, Вад\n\nреклама — @PaceHoz")
        if is_pm(msg):
            bot.send_message(msg.chat.id, text, reply_markup=main_kb())
        else:
            bot.send_message(msg.chat.id, text)
    except Exception as e:
        print(f"Help err: {e}")


@bot.message_handler(commands=['clear'])
def cmd_clear(msg):
    try:
        if is_pm(msg):
            clr_hist(msg.from_user.id)
            bot.send_message(msg.chat.id, "очистил", reply_markup=main_kb())
        elif is_admin(msg.chat.id, msg.from_user.id):
            clr_hist(msg.chat.id, True)
            bot.send_message(msg.chat.id, "контекст сброшен")
    except Exception as e:
        print(f"Clear err: {e}")


@bot.message_handler(commands=['settings'])
def cmd_settings(msg):
    try:
        if is_pm(msg):
            gs = get_ugroups(msg.from_user.id)
            if not gs:
                bot.send_message(msg.chat.id, "нет групп, добавь меня", reply_markup=start_kb())
            else:
                bot.send_message(msg.chat.id, "выбери группу:", reply_markup=gl_kb(msg.from_user.id))
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
        pro_status = "да" if s.get("proactive_enabled") else "нет"
        learn_status = "да" if s.get("learn_style") else "нет"
        bot.send_message(
            cid,
            f"⚙ Настройки\n📊 Шанс ответа: {s['response_chance']}%\n"
            f"💬 Писать первым: {pro_status}\n📚 Обучение: {learn_status}",
            reply_markup=grp_kb(cid))
    except Exception as e:
        print(f"Settings err: {e}")


@bot.message_handler(commands=['addadmin'])
def cmd_aa(msg):
    try:
        if is_pm(msg) or not is_owner(msg.chat.id, msg.from_user.id):
            return
        if not msg.reply_to_message or not msg.reply_to_message.from_user:
            bot.reply_to(msg, "ответь на сообщение пользователя")
            return
        t = msg.reply_to_message.from_user
        if t.is_bot:
            bot.reply_to(msg, "ботов нельзя")
            return
        s = get_gs(msg.chat.id)
        with settings_lock:
            s.setdefault("admins", {})[str(t.id)] = {"name": dname(t)}
        save_settings()
        reg_group(t.id, msg.chat.id, msg.chat.title)
        bot.send_message(msg.chat.id, f"{dname(t)} теперь админ")
    except Exception as e:
        print(f"AA err: {e}")


@bot.message_handler(commands=['removeadmin'])
def cmd_ra(msg):
    try:
        if is_pm(msg) or not is_owner(msg.chat.id, msg.from_user.id):
            return
        if not msg.reply_to_message or not msg.reply_to_message.from_user:
            bot.reply_to(msg, "ответь на сообщение пользователя")
            return
        s = get_gs(msg.chat.id)
        tk = str(msg.reply_to_message.from_user.id)
        name = "?"
        with settings_lock:
            if tk in s.get("admins", {}):
                name = s["admins"][tk].get("name", "?")
                del s["admins"][tk]
        save_settings()
        bot.send_message(msg.chat.id, f"{name} больше не админ")
    except Exception as e:
        print(f"RA err: {e}")


@bot.message_handler(commands=['admins'])
def cmd_adm(msg):
    try:
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
        bot.send_message(msg.chat.id, t)
    except Exception as e:
        print(f"Adm err: {e}")


@bot.message_handler(commands=['setowner'])
def cmd_so(msg):
    try:
        if is_pm(msg) or not is_owner(msg.chat.id, msg.from_user.id):
            return
        if not msg.reply_to_message or not msg.reply_to_message.from_user:
            bot.reply_to(msg, "ответь на сообщение пользователя")
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
        bot.send_message(msg.chat.id, f"👑 Новый владелец: {dname(nw)}")
    except Exception as e:
        print(f"SO err: {e}")


# ================= PENDING =================
def get_pending_key(cid, msg_id):
    return f"pend_{cid}_{msg_id}"


def find_pending_by_chat(cid):
    with pending_lock:
        found = []
        prefix = f"pend_{cid}_"
        for key, val in list(pending_tracks.items()):
            if key.startswith(prefix):
                pt = val.get("time")
                if pt and (datetime.now() - pt).total_seconds() < 600:
                    found.append((key, val))
        return found


def cleanup_old_pending():
    with pending_lock:
        expired = [k for k, v in pending_tracks.items()
                   if v.get("time") and (datetime.now() - v["time"]).total_seconds() > 600]
        for key in expired:
            del pending_tracks[key]


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
            parts = data.split("_")
            if len(parts) < 3:
                bot.answer_callback_query(call.id, "ошибка", show_alert=True)
                return
            action = parts[-1]
            orig_msg_id = "_".join(parts[1:-1])
            pk = f"pend_{cid}_{orig_msg_id}"
            with pending_lock:
                if pk not in pending_tracks:
                    pk = f"pend_{cid}_{mid}"
                    if pk not in pending_tracks:
                        found_pk = None
                        for k in pending_tracks:
                            if k.startswith(f"pend_{cid}_"):
                                found_pk = k
                                break
                        if found_pk:
                            pk = found_pk
                        else:
                            bot.answer_callback_query(call.id, "⏰ Устарело, поищи заново", show_alert=True)
                            return
                if action == "x":
                    pending_tracks.pop(pk, None)
                    safe_edit_message("ок, отменил", cid, mid)
                    bot.answer_callback_query(call.id, "Отменено")
                    return
                try:
                    idx = int(action)
                except ValueError:
                    bot.answer_callback_query(call.id, "ошибка", show_alert=True)
                    return
                pd = pending_tracks.pop(pk, None)
            if not pd or idx >= len(pd.get("results", [])):
                bot.answer_callback_query(call.id, "❌ Нет такого трека", show_alert=True)
                return
            track = pd["results"][idx]
            busy, bt = is_busy(cid)
            if busy:
                with pending_lock:
                    pending_tracks[pk] = pd
                bot.answer_callback_query(call.id, get_busy_reply(bt), show_alert=True)
                return
            set_busy(cid, "music", track['title'])
            safe_edit_message(f"качаю {track['title']}...", cid, mid)
            bot.answer_callback_query(call.id, f"Качаю: {track['title'][:50]}")

            def dl():
                try:
                    res, err = download_track_with_timeout(track['url'])
                    if err:
                        safe_edit_message(f"не получилось: {err}", cid, mid)
                        return
                    try:
                        comment = music_comment(cid, res['title'], ct != "private")
                        send_audio_safe(cid, res, comment)
                        safe_delete_message(cid, mid)
                        add_msg(cid, "assistant", comment, ct != "private")
                    except Exception as e:
                        print(f"Send err: {e}")
                        safe_edit_message("ошибка отправки", cid, mid)
                    finally:
                        shutil.rmtree(res.get('temp_dir', ''), ignore_errors=True)
                except Exception as e:
                    print(f"DL thread err: {e}")
                    traceback.print_exc()
                    safe_edit_message("ошибка скачивания", cid, mid)
                finally:
                    clear_busy(cid)

            threading.Thread(target=dl, daemon=True).start()
            return

        if data in ("dl_mp4", "dl_mp3"):
            with user_states_lock:
                sk = f"dl_{cid}_{mid}"
                url = user_states.pop(sk, None)
            if not url:
                bot.answer_callback_query(call.id, "⏰ Устарело", show_alert=True)
                return
            busy, bt = is_busy(cid)
            if busy:
                with user_states_lock:
                    user_states[sk] = url
                bot.answer_callback_query(call.id, get_busy_reply(bt), show_alert=True)
                return
            fmt = "mp3" if data == "dl_mp3" else "mp4"
            set_busy(cid, "music" if fmt == "mp3" else "video")
            safe_edit_message("качаю...", cid, mid)
            bot.answer_callback_query(call.id, f"Качаю в {fmt.upper()}")

            def dl2():
                try:
                    if fmt == "mp3":
                        res, err = download_track_with_timeout(url)
                        if err:
                            safe_edit_message(err, cid, mid)
                            return
                        try:
                            c = music_comment(cid, res['title'], ct != "private")
                            send_audio_safe(cid, res, c)
                            safe_delete_message(cid, mid)
                        except Exception as e:
                            print(f"Send err: {e}")
                            safe_edit_message("ошибка", cid, mid)
                        finally:
                            shutil.rmtree(res.get('temp_dir', ''), ignore_errors=True)
                    else:
                        res, err = download_video_with_timeout(url)
                        if err:
                            safe_edit_message(err, cid, mid)
                            return
                        try:
                            with open(res['file'], 'rb') as v:
                                bot.send_video(cid, v, caption=res.get('title', ''),
                                               duration=safe_duration(res.get('duration', 0)),
                                               supports_streaming=True)
                            safe_delete_message(cid, mid)
                        except Exception as e:
                            print(f"Send err: {e}")
                            safe_edit_message("ошибка", cid, mid)
                        finally:
                            shutil.rmtree(res.get('temp_dir', ''), ignore_errors=True)
                except Exception as e:
                    print(f"DL2 thread err: {e}")
                    traceback.print_exc()
                    safe_edit_message("ошибка скачивания", cid, mid)
                finally:
                    clear_busy(cid)

            threading.Thread(target=dl2, daemon=True).start()
            return

        if ct == "private":
            if data == "clear":
                clr_hist(uid)
                safe_edit_message("очистил контекст", cid, mid, reply_markup=main_kb())
                bot.answer_callback_query(call.id, "✅ Контекст очищен", show_alert=True)
            elif data == "stats":
                with session_lock:
                    session = get_session(uid)
                    msg_count = len(session['messages']) - 1
                groups_count = len(get_ugroups(uid))
                safe_edit_message(
                    f"📊 Статистика\n\n💬 Сообщений: {msg_count}\n👥 Групп: {groups_count}",
                    cid, mid, reply_markup=main_kb())
                bot.answer_callback_query(call.id)
            elif data == "start_chat":
                safe_edit_message("пиши, я тут", cid, mid, reply_markup=main_kb())
                bot.answer_callback_query(call.id)
            elif data == "info":
                safe_edit_message(
                    "Вадим, 20 лет\n\n"
                    "• общаюсь на любые темы без цензуры\n"
                    "• ищу музыку: YouTube, SoundCloud, YT Music\n"
                    "• качаю видео с 20+ платформ\n• отвечаю на вопросы\n"
                    "• учусь стилю общения\n• могу писать первым в группе\n\n"
                    "зови: Вадим, Вадя, Вад\n\nреклама — @PaceHoz",
                    cid, mid, reply_markup=main_kb())
                bot.answer_callback_query(call.id)
            elif data == "my_groups":
                gs = get_ugroups(uid)
                if gs:
                    safe_edit_message("👥 Твои группы:", cid, mid, reply_markup=gl_kb(uid))
                else:
                    safe_edit_message("нет групп, добавь меня", cid, mid, reply_markup=start_kb())
                bot.answer_callback_query(call.id)
            elif data == "back_main":
                safe_edit_message("чем помочь?", cid, mid, reply_markup=main_kb())
                bot.answer_callback_query(call.id)
            elif data.startswith("pg_sel_"):
                try:
                    gid_val = int(data[len("pg_sel_"):])
                except ValueError:
                    bot.answer_callback_query(call.id, "ошибка", show_alert=True)
                    return
                if is_admin(gid_val, uid):
                    s = get_gs(gid_val)
                    gn = get_ugroups(uid).get(str(gid_val), {}).get('title', 'Группа')
                    safe_edit_message(f"⚙ {gn}\n📊 Шанс: {s['response_chance']}%",
                                     cid, mid, reply_markup=pg_kb(gid_val))
                else:
                    bot.answer_callback_query(call.id, "❌ Нет прав", show_alert=True)
                    return
                bot.answer_callback_query(call.id)
            elif data.startswith("pg_") or data.startswith("pgi_") or data.startswith("pgh_"):
                handle_pg(call, data, uid, cid, mid)
            elif data == "noop":
                bot.answer_callback_query(call.id)
            else:
                bot.answer_callback_query(call.id)
            return

        if not is_admin(cid, uid):
            bot.answer_callback_query(call.id, "❌ Нет прав", show_alert=True)
            return
        handle_grp_cb(call, data, uid, cid, mid)

    except Exception as e:
        print(f"CB err: {e}")
        traceback.print_exc()
        try:
            bot.answer_callback_query(call.id, "Произошла ошибка")
        except Exception:
            pass


def _extract_gid_from_data(data, prefix):
    remainder = data[len(prefix):]
    try:
        return int(remainder)
    except ValueError:
        return None


def handle_pg(call, data, uid, cid, mid):
    try:
        alert_text = None
        g_val = None
        prefixes_simple = {
            "pg_cd10_": "cd10", "pg_cu10_": "cu10",
            "pg_cd5_": "cd5", "pg_cu5_": "cu5",
            "pg_pt_": "pt", "pg_pi_": "pi", "pg_ph_": "ph",
            "pg_lt_": "lt", "pg_pc_": "pc", "pg_pr_": "pr",
            "pg_cc_": "cc", "pg_cm_": "cm"}
        action = None
        mn_val = mx_val = sh_val = eh_val = 0
        for pfx, act in prefixes_simple.items():
            if data.startswith(pfx):
                g_val = _extract_gid_from_data(data, pfx)
                action = act
                break
        if action is None and data.startswith("pgi_"):
            remainder = data[4:]
            parts = remainder.rsplit("_", 2)
            if len(parts) == 3:
                try:
                    g_val = int(parts[0])
                    mn_val = int(parts[1])
                    mx_val = int(parts[2])
                    action = "pgi"
                except ValueError:
                    pass
        if action is None and data.startswith("pgh_"):
            remainder = data[4:]
            parts = remainder.rsplit("_", 2)
            if len(parts) == 3:
                try:
                    g_val = int(parts[0])
                    sh_val = int(parts[1])
                    eh_val = int(parts[2])
                    action = "pgh"
                except ValueError:
                    pass
        if action is None or g_val is None:
            bot.answer_callback_query(call.id)
            return
        if not is_admin(g_val, uid):
            bot.answer_callback_query(call.id, "❌ Нет прав", show_alert=True)
            return
        s = get_gs(g_val)
        if action == "cd10":
            with settings_lock: s["response_chance"] = max(0, s["response_chance"] - 10)
            save_settings(); alert_text = f"Шанс ответа: {s['response_chance']}%"
        elif action == "cu10":
            with settings_lock: s["response_chance"] = min(100, s["response_chance"] + 10)
            save_settings(); alert_text = f"Шанс ответа: {s['response_chance']}%"
        elif action == "cd5":
            with settings_lock: s["response_chance"] = max(0, s["response_chance"] - 5)
            save_settings(); alert_text = f"Шанс ответа: {s['response_chance']}%"
        elif action == "cu5":
            with settings_lock: s["response_chance"] = min(100, s["response_chance"] + 5)
            save_settings(); alert_text = f"Шанс ответа: {s['response_chance']}%"
        elif action == "pt":
            with settings_lock: s["proactive_enabled"] = not s.get("proactive_enabled", False)
            save_settings()
            if s["proactive_enabled"]:
                start_ptimer(g_val); alert_text = "✅ Бот будет писать первым"
            else:
                stop_ptimer(g_val); alert_text = "❌ Бот не будет писать первым"
        elif action == "pi":
            safe_edit_message("⏱ Выбери интервал:", cid, mid, reply_markup=int_kb(g_val, True))
            bot.answer_callback_query(call.id); return
        elif action == "pgi":
            with settings_lock:
                s["proactive_min_interval"] = mn_val; s["proactive_max_interval"] = mx_val
            save_settings()
            if s.get("proactive_enabled"): start_ptimer(g_val)
            alert_text = f"Интервал: {mn_val}-{mx_val} мин"
        elif action == "ph":
            safe_edit_message("🕐 Выбери часы:", cid, mid, reply_markup=hrs_kb(g_val, True))
            bot.answer_callback_query(call.id); return
        elif action == "pgh":
            with settings_lock:
                s["proactive_active_hours_start"] = sh_val; s["proactive_active_hours_end"] = eh_val
            save_settings(); alert_text = f"Активные часы: {sh_val}-{eh_val}"
        elif action == "lt":
            with settings_lock: s["learn_style"] = not s.get("learn_style", True)
            save_settings()
            alert_text = "✅ Обучение включено" if s["learn_style"] else "❌ Обучение выключено"
        elif action == "pc":
            with user_states_lock: user_states[f"pp_{uid}"] = g_val
            safe_edit_message("📝 Отправь новый промпт.\nДля отмены напиши: отмена", cid, mid)
            bot.answer_callback_query(call.id, "Жду промпт..."); return
        elif action == "pr":
            with settings_lock: s["custom_prompt"] = None
            save_settings(); ref_prompt(g_val, True)
            alert_text = "✅ Промпт сброшен на стандартный"
        elif action == "cc":
            clr_hist(g_val, True); alert_text = "✅ Контекст сброшен"
        elif action == "cm":
            clear_memory(g_val, True); alert_text = "✅ Память и стиль полностью сброшены"
        gn = get_ugroups(uid).get(str(g_val), {}).get('title', 'Группа')
        safe_edit_message(f"⚙ {gn}\n📊 Шанс: {s['response_chance']}%",
                         cid, mid, reply_markup=pg_kb(g_val))
        if alert_text:
            bot.answer_callback_query(call.id, alert_text, show_alert=True)
        else:
            bot.answer_callback_query(call.id)
    except Exception as e:
        print(f"PG err: {e}"); traceback.print_exc()
        try: bot.answer_callback_query(call.id, "Ошибка")
        except Exception: pass


def handle_grp_cb(call, data, uid, cid, mid):
    s = get_gs(cid)
    alert_text = None
    try:
        if data == "noop":
            bot.answer_callback_query(call.id); return
        elif data == "close":
            safe_delete_message(cid, mid); bot.answer_callback_query(call.id); return
        elif data == "cd10":
            with settings_lock: s["response_chance"] = max(0, s["response_chance"] - 10)
            save_settings(); alert_text = f"Шанс ответа: {s['response_chance']}%"
        elif data == "cu10":
            with settings_lock: s["response_chance"] = min(100, s["response_chance"] + 10)
            save_settings(); alert_text = f"Шанс ответа: {s['response_chance']}%"
        elif data == "cd5":
            with settings_lock: s["response_chance"] = max(0, s["response_chance"] - 5)
            save_settings(); alert_text = f"Шанс ответа: {s['response_chance']}%"
        elif data == "cu5":
            with settings_lock: s["response_chance"] = min(100, s["response_chance"] + 5)
            save_settings(); alert_text = f"Шанс ответа: {s['response_chance']}%"
        elif data == "ptog":
            with settings_lock: s["proactive_enabled"] = not s.get("proactive_enabled", False)
            save_settings()
            if s["proactive_enabled"]:
                start_ptimer(cid); alert_text = "✅ Бот будет писать первым"
            else:
                stop_ptimer(cid); alert_text = "❌ Бот не будет писать первым"
        elif data == "pint":
            safe_edit_message("⏱ Выбери интервал:", cid, mid, reply_markup=int_kb(cid))
            bot.answer_callback_query(call.id); return
        elif data == "phrs":
            safe_edit_message("🕐 Выбери часы:", cid, mid, reply_markup=hrs_kb(cid))
            bot.answer_callback_query(call.id); return
        elif data.startswith("gi_"):
            v = data[3:].split("_")
            if len(v) == 2:
                try:
                    with settings_lock:
                        s["proactive_min_interval"] = int(v[0]); s["proactive_max_interval"] = int(v[1])
                    save_settings()
                    if s.get("proactive_enabled"): start_ptimer(cid)
                    alert_text = f"Интервал: {v[0]}-{v[1]} мин"
                except ValueError: pass
        elif data.startswith("gh_"):
            v = data[3:].split("_")
            if len(v) == 2:
                try:
                    with settings_lock:
                        s["proactive_active_hours_start"] = int(v[0]); s["proactive_active_hours_end"] = int(v[1])
                    save_settings(); alert_text = f"Активные часы: {v[0]}-{v[1]}"
                except ValueError: pass
        elif data == "bk": pass
        elif data == "ltog":
            with settings_lock: s["learn_style"] = not s.get("learn_style", True)
            save_settings()
            alert_text = "✅ Обучение включено" if s["learn_style"] else "❌ Обучение выключено"
        elif data == "gclr":
            clr_hist(cid, True); alert_text = "✅ Контекст сброшен"
        elif data == "gmem":
            clear_memory(cid, True); alert_text = "✅ Память и стиль полностью сброшены"
        elif data == "pchg":
            with user_states_lock: user_states[f"{cid}_{uid}"] = "wp"
            try: bot.send_message(cid, "📝 Отправь новый промпт.\nДля отмены напиши: отмена")
            except Exception: pass
            bot.answer_callback_query(call.id, "Жду промпт..."); return
        elif data == "prst":
            with settings_lock: s["custom_prompt"] = None
            save_settings(); ref_prompt(cid, True)
            alert_text = "✅ Промпт сброшен на стандартный"
        elif data == "alst":
            t = f"👑 Владелец: {s.get('owner_name', '?')}\n"
            admins = s.get("admins", {})
            if admins:
                t += "\n👤 Админы:\n"
                for a in admins.values():
                    if isinstance(a, dict): t += f"  • {a.get('name', '?')}\n"
            else: t += "\nАдминов нет"
            bot.answer_callback_query(call.id, t, show_alert=True); return
        else:
            bot.answer_callback_query(call.id); return
        pro_status = "да" if s.get("proactive_enabled") else "нет"
        safe_edit_message(
            f"⚙ Настройки\n📊 Шанс ответа: {s['response_chance']}%\n💬 Писать первым: {pro_status}",
            cid, mid, reply_markup=grp_kb(cid))
        if alert_text:
            bot.answer_callback_query(call.id, alert_text, show_alert=True)
        else:
            bot.answer_callback_query(call.id)
    except Exception as e:
        print(f"Grp CB err: {e}"); traceback.print_exc()
        try: bot.answer_callback_query(call.id, "Ошибка")
        except Exception: pass


# ================= МЕДИА =================
def handle_media(msg, intent, grp=False):
    cid = msg.chat.id
    busy, bt = is_busy(cid)
    if busy:
        bot.send_message(cid, get_busy_reply(bt))
        return True
    if intent.get("type") == "music_search" and intent.get("query"):
        query = intent["query"]
        set_busy(cid, "music", query)
        try:
            smsg = bot.send_message(cid, f"ищу \"{query}\"...")
        except Exception as e:
            print(f"Send search msg err: {e}"); clear_busy(cid); return True

        def do():
            try:
                results = search_tracks(query)
                if not results:
                    safe_edit_message("ничего не нашёл, попробуй другой запрос", cid, smsg.message_id)
                    return
                results = results[:6]
                pk = get_pending_key(cid, smsg.message_id)
                with pending_lock:
                    pending_tracks[pk] = {
                        "results": results, "query": query,
                        "time": datetime.now(), "user_id": msg.from_user.id}
                text = track_list_msg(cid, query, results, grp)
                if not safe_edit_message(text, cid, smsg.message_id,
                                         reply_markup=track_kb(len(results), smsg.message_id)):
                    fb = f"нашёл {len(results)} треков:\n\n"
                    for i, r in enumerate(results):
                        fb += f"{i + 1}. {r['title']} ({fmt_dur(r.get('duration', 0))})"
                        if r.get('source'): fb += f" [{r['source']}]"
                        fb += "\n"
                    fb += "\nжми номер"
                    safe_edit_message(fb, cid, smsg.message_id,
                                     reply_markup=track_kb(len(results), smsg.message_id))
            except Exception as e:
                print(f"Search handle err: {e}"); traceback.print_exc()
                safe_edit_message("ошибка поиска", cid, smsg.message_id)
            finally:
                clear_busy(cid)

        threading.Thread(target=do, daemon=True).start()
        return True
    if intent.get("type") == "video_download" and intent.get("url"):
        url = intent["url"]
        fmt = intent.get("format", "auto")
        if fmt == "auto":
            try:
                m = bot.send_message(cid, f"{get_platform(url)} — какой формат?", reply_markup=fmt_kb())
                with user_states_lock: user_states[f"dl_{cid}_{m.message_id}"] = url
            except Exception as e:
                print(f"Send format msg err: {e}")
            return True
        set_busy(cid, "music" if fmt == "mp3" else "video")
        try:
            smsg = bot.send_message(cid, "качаю...")
        except Exception as e:
            print(f"Send dl msg err: {e}"); clear_busy(cid); return True

        def do2():
            try:
                if fmt == "mp3":
                    res, err = download_track_with_timeout(url)
                else:
                    res, err = download_video_with_timeout(url)
                if err:
                    safe_edit_message(err, cid, smsg.message_id); return
                try:
                    if fmt == "mp3":
                        c = music_comment(cid, res['title'], grp)
                        send_audio_safe(cid, res, c)
                    else:
                        with open(res['file'], 'rb') as v:
                            bot.send_video(cid, v, caption=res.get('title', ''),
                                           duration=safe_duration(res.get('duration', 0)),
                                           supports_streaming=True)
                    safe_delete_message(cid, smsg.message_id)
                except Exception as e:
                    print(f"Send err: {e}")
                    safe_edit_message("ошибка отправки", cid, smsg.message_id)
                finally:
                    shutil.rmtree(res.get('temp_dir', ''), ignore_errors=True)
            except Exception as e:
                print(f"DL2 thread err: {e}"); traceback.print_exc()
                safe_edit_message("ошибка скачивания", cid, smsg.message_id)
            finally:
                clear_busy(cid)

        threading.Thread(target=do2, daemon=True).start()
        return True
    return False


# ================= ТЕКСТ =================
@bot.message_handler(content_types=['text'])
def on_text(msg):
    try:
        if not msg.text or not msg.text.strip():
            return
        if not msg.from_user:
            return

        # === Обработка промпта в ЛС ===
        if is_pm(msg):
            pk = f"pp_{msg.from_user.id}"
            with user_states_lock:
                gid_val = user_states.pop(pk, None)
            if gid_val is not None:
                if msg.text.lower().strip() == "отмена":
                    bot.send_message(msg.chat.id, "ок, отменил", reply_markup=main_kb())
                    return
                s = get_gs(gid_val)
                with settings_lock: s["custom_prompt"] = msg.text
                save_settings(); ref_prompt(gid_val, True); clr_hist(gid_val, True)
                bot.send_message(msg.chat.id, "✅ промпт обновлён, контекст сброшен", reply_markup=main_kb())
                return

        # === Обработка промпта в группе ===
        if is_grp(msg):
            sk = f"{msg.chat.id}_{msg.from_user.id}"
            with user_states_lock:
                state_val = user_states.pop(sk, None)
            if state_val == "wp":
                if msg.text.lower().strip() == "отмена":
                    bot.send_message(msg.chat.id, "ок, отменил"); return
                if not is_admin(msg.chat.id, msg.from_user.id): return
                s = get_gs(msg.chat.id)
                with settings_lock: s["custom_prompt"] = msg.text
                save_settings(); ref_prompt(msg.chat.id, True); clr_hist(msg.chat.id, True)
                bot.send_message(msg.chat.id, "✅ промпт обновлён"); return
            if is_admin(msg.chat.id, msg.from_user.id):
                reg_group(msg.from_user.id, msg.chat.id, msg.chat.title)

        cid = msg.chat.id

        # === Выбор трека по номеру ===
        text_stripped = msg.text.strip()
        if text_stripped.isdigit():
            track_number = int(text_stripped)
            if 1 <= track_number <= 8:
                pending_list = find_pending_by_chat(cid)
                if pending_list:
                    latest_key = None
                    latest_val = None
                    latest_time = None
                    for p_key, p_val in pending_list:
                        p_time = p_val.get("time")
                        if p_time and (latest_time is None or p_time > latest_time):
                            latest_key = p_key; latest_val = p_val; latest_time = p_time
                    if latest_val and 1 <= track_number <= len(latest_val.get("results", [])):
                        busy, bt = is_busy(cid)
                        if busy:
                            bot.send_message(cid, get_busy_reply(bt)); return
                        with pending_lock: pending_tracks.pop(latest_key, None)
                        track = latest_val["results"][track_number - 1]
                        set_busy(cid, "music", track['title'])
                        try:
                            smsg = bot.send_message(cid, f"качаю {track['title']}...")
                        except Exception as e:
                            print(f"Send dl msg err: {e}"); clear_busy(cid); return

                        def dl_text():
                            try:
                                res, err = download_track_with_timeout(track['url'])
                                if err:
                                    safe_edit_message(f"не получилось: {err}", cid, smsg.message_id); return
                                try:
                                    comment = music_comment(cid, res['title'], is_grp(msg))
                                    send_audio_safe(cid, res, comment)
                                    safe_delete_message(cid, smsg.message_id)
                                    add_msg(cid, "assistant", comment, is_grp(msg))
                                except Exception as e:
                                    print(f"Send err: {e}")
                                    safe_edit_message("ошибка отправки", cid, smsg.message_id)
                                finally:
                                    shutil.rmtree(res.get('temp_dir', ''), ignore_errors=True)
                            except Exception as e:
                                print(f"DL text thread err: {e}"); traceback.print_exc()
                                safe_edit_message("ошибка скачивания", cid, smsg.message_id)
                            finally:
                                clear_busy(cid)

                        threading.Thread(target=dl_text, daemon=True).start()
                        return

        # === Быстрый детект URL/музыки ===
        quick = quick_detect(msg.text)
        if quick:
            if is_grp(msg):
                rem_user(msg.chat.id, msg.from_user)
                add_msg(msg.chat.id, "user", f"[{dname(msg.from_user)}]: {msg.text}", True)
            if handle_media(msg, quick, is_grp(msg)):
                return

        # === ЛС ===
        if is_pm(msg):
            uid = msg.from_user.id

            # НЕ запоминаем имя из профиля — бот не знает имя пока не скажут
            busy, bt = is_busy(cid)
            if busy:
                bot.send_message(cid, get_busy_reply(bt)); return

            bot.send_chat_action(msg.chat.id, 'typing')
            add_msg(uid, "user", msg.text)

            intent = analyze_intent(msg.text)
            if intent.get("type") != "chat" and handle_media(msg, intent):
                return

            msgs = get_session_messages_copy(uid)

            if need_search(msg.text):
                sd = add_search(msg.text)
                if sd and msgs:
                    msgs[-1] = {"role": "user", "content": msg.text + sd}

            resp = ask_ai(msgs)
            if not is_error(resp):
                resp = clean(resp)
                add_msg(uid, "assistant", resp)
            else:
                resp = resp.replace("[ERR]", "")

            send_long_msg(msg.chat.id, resp, reply_markup=main_kb())
            return

        # === Группа ===
        if not is_grp(msg):
            return

        rem_user(cid, msg.from_user)
        uname = dname(msg.from_user)
        add_msg(cid, "user", f"[{uname}]: {msg.text}", True)
        last_activity[cid] = datetime.now()

        s = get_gs(cid)
        if msg.chat.title and s.get("group_name") != msg.chat.title:
            with settings_lock: s["group_name"] = msg.chat.title
            save_settings()

        if s.get("proactive_enabled"):
            start_ptimer(cid)

        should = False
        bi = get_bot_info()
        bu = bi.username.lower() if bi and bi.username else ""
        is_reply_to_bot = (
            msg.reply_to_message and msg.reply_to_message.from_user and
            bi and msg.reply_to_message.from_user.id == bi.id)
        is_mention = bu and f"@{bu}" in msg.text.lower()
        is_name_call = is_named(msg.text)

        if is_reply_to_bot or is_mention or is_name_call:
            should = True
        else:
            busy, _ = is_busy(cid)
            if not busy and random.randint(1, 100) <= s["response_chance"]:
                should = True

        if not should:
            return

        busy, bt = is_busy(cid)
        if busy:
            if is_reply_to_bot or is_mention or is_name_call:
                bot.send_message(cid, get_busy_reply(bt))
            return

        bot.send_chat_action(cid, 'typing')
        intent = analyze_intent(msg.text)
        if intent.get("type") != "chat" and handle_media(msg, intent, True):
            return

        msgs = get_session_messages_copy(cid, True)
        if need_search(msg.text):
            sd = add_search(msg.text)
            if sd and msgs:
                msgs[-1] = {"role": "user", "content": f"[{uname}]: {msg.text}{sd}"}

        resp = ask_ai(msgs)
        if not is_error(resp):
            resp = clean(resp)
            add_msg(cid, "assistant", resp, True)
        else:
            resp = resp.replace("[ERR]", "")

        send_long_msg(cid, resp)

    except Exception as e:
        print(f"Text err: {e}"); traceback.print_exc()


def send_long_msg(cid, text, reply_markup=None, reply_to=None):
    try:
        if not text or not text.strip():
            text = "..."
        chunks = []
        while len(text) > 4096:
            split_pos = text.rfind('\n', 0, 4096)
            if split_pos < 2000: split_pos = text.rfind('. ', 0, 4096)
            if split_pos < 2000: split_pos = 4096
            chunks.append(text[:split_pos])
            text = text[split_pos:].lstrip()
        if text: chunks.append(text)
        for i, chunk in enumerate(chunks):
            is_last = (i == len(chunks) - 1)
            try:
                bot.send_message(cid, chunk,
                                 reply_markup=reply_markup if is_last else None,
                                 reply_to_message_id=reply_to if i == 0 else None)
            except Exception as e:
                print(f"Send chunk err: {e}")
    except Exception as e:
        print(f"Send msg err: {e}")


# ================= ОЧИСТКА =================
def cleanup_loop():
    while True:
        try:
            time.sleep(600)
            now = time.time()
            if os.path.exists(DOWNLOADS_DIR):
                for item in os.listdir(DOWNLOADS_DIR):
                    p = os.path.join(DOWNLOADS_DIR, item)
                    try:
                        if os.path.isdir(p) and now - os.path.getmtime(p) > 1800:
                            shutil.rmtree(p, ignore_errors=True)
                    except Exception: pass
            cleanup_old_pending()
            with user_states_lock:
                expired = [k for k in user_states if k.startswith("dl_")]
                if len(expired) > 50:
                    for k in expired[:30]: user_states.pop(k, None)
        except Exception as e:
            print(f"Cleanup err: {e}")


# ================= ЗАПУСК =================
if __name__ == "__main__":
    print("=" * 50)
    print("       ВАДИМ БОТ — ЗАПУСК")
    print("=" * 50)
    bi = get_bot_info()
    if bi: print(f"✅ Бот: @{bi.username}")
    else: print("⚠️ Не удалось получить информацию о боте")
    if not FFMPEG_AVAILABLE:
        print("⚠️  FFmpeg не найден! Конвертация в MP3 может не работать!")
    else: print("✅ FFmpeg найден")
    prompt_source = 'promt.txt' if os.path.exists(PROMPT_FILE) else 'default'
    print(f"✅ Промпт: {len(DEFAULT_SYSTEM_PROMPT)} символов из {prompt_source}")
    print(f"✅ Модель: {MODEL_ID}")
    print(f"✅ Групп в настройках: {len(group_settings)}")
    proactive_count = 0
    for ck, st in group_settings.items():
        if st.get("proactive_enabled"):
            try: start_ptimer(int(ck)); proactive_count += 1
            except (ValueError, Exception): pass
    if proactive_count > 0: print(f"✅ Проактивных таймеров: {proactive_count}")
    threading.Thread(target=cleanup_loop, daemon=True).start()
    print("=" * 50)
    print("       БОТ РАБОТАЕТ!")
    print("=" * 50)
    while True:
        try:
            bot.infinity_polling(
                allowed_updates=["message", "callback_query", "my_chat_member"],
                timeout=60, long_polling_timeout=60)
        except KeyboardInterrupt:
            print("\n🛑 Остановлен вручную"); break
        except Exception as e:
            print(f"Poll err: {e}"); traceback.print_exc(); time.sleep(5)
