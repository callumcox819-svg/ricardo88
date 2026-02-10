import os
import re
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

from ricardo_playwright import POPULAR_CATEGORIES, ricardo_collect_items
import proxy_manager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("ricardo_bot")

PROFILE_DIR = Path("Profile"); PROFILE_DIR.mkdir(exist_ok=True)
RESULTS_DIR = Path("Results"); RESULTS_DIR.mkdir(exist_ok=True)

SETTINGS_FILE = PROFILE_DIR / "settings.json"
BLACKLIST_FILE = PROFILE_DIR / "blacklist.json"
STATE_FILE = PROFILE_DIR / "state.json"
ALLOWED_USERS_FILE = PROFILE_DIR / "allowed_users.json"

# ---------------- Buttons ----------------
BTN_START = "Старт ✅"
BTN_STOP = "Стоп ⛔"
BTN_SETTINGS = "Настройки ⚙️"
BTN_ADMIN = "Админ панель 🛠"
BTN_BACK = "Назад ↩️"

BTN_COUNT = "Кол-во объявлений 📦"
BTN_CATS = "Категории 📂"
BTN_BLACKLIST = "ЧС 🚫"

# Blacklist submenu
BTN_BL_MODE = "Режим ЧС (общий/личный)"
BTN_BL_SHOW = "Показать ЧС"
BTN_BL_ADD = "Добавить в ЧС"
BTN_BL_REMOVE = "Удалить из ЧС"

# Categories submenu
BTN_CATS_ALL = "Все подряд"
BTN_CATS_CLEAR = "Очистить выбор"
BTN_CATS_CONTINUE = "🔥 Продолжить настройку"

# Admin submenu
BTN_ADMIN_USERS_ADD = "Добавить юзера ➕"
BTN_ADMIN_USERS_REMOVE = "Удалить юзера ➖"
BTN_ADMIN_USERS_LIST = "Список юзеров 📋"
BTN_PROXIES = "Прокси 🛡"

# Proxies submenu
BTN_PX_SET = "Задать список прокси"
BTN_PX_SHOW = "Показать прокси"
BTN_PX_CLEAR = "Очистить прокси"

COUNT_CHOICES = ["5", "10", "20", "30"]

# ---------------- Conversation states ----------------
(
    MAIN,
    SET_COUNT,
    BL_MENU,
    BL_ADD_NAME,
    BL_REMOVE_NAME,
    CATS_MENU,
    ADMIN_MENU,
    PX_SET,
    ADMIN_ADD_USER,
    ADMIN_REMOVE_USER,
) = range(10)

DEFAULT_USER_SETTINGS: Dict[str, Any] = {
    "max_items": 30,             # how many items in one JSON
    "cats_mode": "all",          # "all" or "selected"
    "cats_selected": [],         # list[str] of category names
    "edit_blacklist_mode": "personal",  # "personal" or "general"
}

# ---------------- Simple JSON storage ----------------
def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default

def _save_json(path: Path, data: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)

# ---------------- Allowed users (admin) ----------------
def load_allowed_users() -> List[int]:
    data = _load_json(ALLOWED_USERS_FILE, [])
    out: List[int] = []
    if isinstance(data, list):
        for x in data:
            try:
                out.append(int(x))
            except Exception:
                pass
    return sorted(set(out))

def save_allowed_users(users: List[int]) -> None:
    _save_json(ALLOWED_USERS_FILE, sorted(set(int(x) for x in users)))

def is_user_allowed(user_id: int, owner_id: int) -> bool:
    users = load_allowed_users()
    if not users:
        return True
    return user_id == owner_id or user_id in users

# ---------------- Settings per user ----------------
def load_settings() -> Dict[str, Dict[str, Any]]:
    return _load_json(SETTINGS_FILE, {})

def save_settings(data: Dict[str, Dict[str, Any]]) -> None:
    _save_json(SETTINGS_FILE, data)

def get_user_settings(user_id: int) -> Dict[str, Any]:
    all_s = load_settings()
    s = dict(all_s.get(str(user_id), {}) or {})
    for k, v in DEFAULT_USER_SETTINGS.items():
        s.setdefault(k, v)
    return s

def set_user_settings(user_id: int, s: Dict[str, Any]) -> None:
    all_s = load_settings()
    all_s[str(user_id)] = s
    save_settings(all_s)

# ---------------- Blacklists: general + personal ----------------
def load_blacklists() -> Dict[str, Any]:
    return _load_json(BLACKLIST_FILE, {"general": [], "personal": {}})

def save_blacklists(data: Dict[str, Any]) -> None:
    _save_json(BLACKLIST_FILE, data)

def get_blacklist_general() -> List[str]:
    return load_blacklists().get("general", []) or []

def get_blacklist_personal(user_id: int) -> List[str]:
    return (load_blacklists().get("personal", {}) or {}).get(str(user_id), []) or []

def add_to_blacklist(user_id: int, name: str, mode: str) -> None:
    name = (name or "").strip()
    if not name:
        return
    bl = load_blacklists()
    if mode == "general":
        bl.setdefault("general", [])
        if name not in bl["general"]:
            bl["general"].append(name)
    else:
        bl.setdefault("personal", {})
        bl["personal"].setdefault(str(user_id), [])
        if name not in bl["personal"][str(user_id)]:
            bl["personal"][str(user_id)].append(name)
    save_blacklists(bl)

def remove_from_blacklist(user_id: int, name: str, mode: str) -> None:
    name = (name or "").strip()
    bl = load_blacklists()
    if mode == "general":
        if name in bl.get("general", []):
            bl["general"].remove(name)
    else:
        lst = bl.get("personal", {}).get(str(user_id), [])
        if name in lst:
            lst.remove(name)
    save_blacklists(bl)

# ---------------- Runtime state per user ----------------
def load_state() -> Dict[str, Any]:
    return _load_json(STATE_FILE, {})

def save_state(data: Dict[str, Any]) -> None:
    _save_json(STATE_FILE, data)

def get_user_state(user_id: int) -> Dict[str, Any]:
    st = load_state()
    s = dict(st.get(str(user_id), {}) or {})
    s.setdefault("sent_links", [])
    s.setdefault("running", False)
    s.setdefault("buffer", [])   # accumulate items until max_items
    return s

def set_user_state(user_id: int, s: Dict[str, Any]) -> None:
    st = load_state()
    st[str(user_id)] = s
    save_state(st)

# ---------------- Keyboards ----------------
def main_menu_kb(user_id: int, owner_id: int) -> ReplyKeyboardMarkup:
    rows = [[BTN_START, BTN_STOP], [BTN_SETTINGS]]
    if user_id == owner_id:
        rows.append([BTN_ADMIN])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def settings_menu_kb() -> ReplyKeyboardMarkup:
    rows = [[BTN_COUNT], [BTN_CATS], [BTN_BLACKLIST], [BTN_BACK]]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def count_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([COUNT_CHOICES, [BTN_BACK]], resize_keyboard=True)

def blacklist_menu_kb(user_id: int) -> ReplyKeyboardMarkup:
    s = get_user_settings(user_id)
    mode = s.get("edit_blacklist_mode", "personal")
    mode_txt = "личный" if mode == "personal" else "общий"
    return ReplyKeyboardMarkup(
        [[f"{BTN_BL_MODE}: {mode_txt}"], [BTN_BL_SHOW], [BTN_BL_ADD, BTN_BL_REMOVE], [BTN_BACK]],
        resize_keyboard=True,
    )

def _clean_cat_label(t: str) -> str:
    return (t or "").replace("✅", "").strip()

def cats_menu_kb(user_id: int) -> ReplyKeyboardMarkup:
    s = get_user_settings(user_id)
    mode = s.get("cats_mode", "all")
    selected = set(s.get("cats_selected", []))

    names = [k for k in POPULAR_CATEGORIES.keys() if k != "Все подряд"]
    rows: List[List[str]] = []
    for i in range(0, len(names), 2):
        row: List[str] = []
        for name in names[i:i+2]:
            label = f"✅ {name}" if (mode == "selected" and name in selected) else name
            row.append(label)
        rows.append(row)

    all_label = f"✅ {BTN_CATS_ALL}" if mode == "all" else BTN_CATS_ALL
    rows.append([all_label])
    rows.append([BTN_CATS_CLEAR])
    rows.append([BTN_CATS_CONTINUE])
    rows.append([BTN_BACK])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def admin_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[BTN_PROXIES], [BTN_ADMIN_USERS_ADD, BTN_ADMIN_USERS_REMOVE], [BTN_ADMIN_USERS_LIST], [BTN_BACK]],
        resize_keyboard=True,
    )

def proxies_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([[BTN_PX_SET], [BTN_PX_SHOW, BTN_PX_CLEAR], [BTN_BACK]], resize_keyboard=True)

# ---------------- Helpers ----------------
def save_json_result(items: List[Dict[str, Any]], user_id: int) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = RESULTS_DIR / f"ricardo_{user_id}_{ts}.json"
    path.write_text(json.dumps({"items": items}, ensure_ascii=False, indent=2), encoding="utf-8")
    return path

def _format_proxy_list(proxies: List[dict]) -> str:
    if not proxies:
        return "(пусто)"
    out = []
    for p in proxies:
        if not isinstance(p, dict):
            continue
        server = p.get("server", "")
        user = p.get("username")
        pwd = p.get("password")
        if user and pwd:
            out.append(f"{server}  {user}:{pwd}")
        else:
            out.append(server)
    return "\n".join(out) if out else "(пусто)"

def _build_urls_from_user_settings(user_id: int) -> List[str]:
    s = get_user_settings(user_id)
    mode = s.get("cats_mode", "all")
    selected = s.get("cats_selected", []) or []
    if mode == "all":
        return ["__ALL__"]
    urls = [POPULAR_CATEGORIES[n] for n in selected if n in POPULAR_CATEGORIES]
    return urls or ["__ALL__"]

def _apply_blacklists(user_id: int, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    blocked = set(get_blacklist_general()) | set(get_blacklist_personal(user_id))
    out: List[Dict[str, Any]] = []
    for it in items:
        seller = (it.get("item_person_name") or "").strip()
        if seller and seller in blocked:
            continue
        out.append(it)
    return out

def _dedupe_new(user_id: int, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    st = get_user_state(user_id)
    sent = set(st.get("sent_links", []) or [])
    buf = st.get("buffer", []) or []
    in_buf = set([x.get("item_link") for x in buf if isinstance(x, dict)])
    out: List[Dict[str, Any]] = []
    for it in items:
        lk = it.get("item_link")
        if not lk or lk in sent or lk in in_buf:
            continue
        out.append(it)
    return out

async def _search_once(user_id: int) -> List[Dict[str, Any]]:
    s = get_user_settings(user_id)
    max_items = int(s.get("max_items", 30))
    # Fetch more than needed to have шанс найти новые
    fetch_limit = max(60, max_items * 3)

    urls = _build_urls_from_user_settings(user_id)
    items = await ricardo_collect_items(urls=urls, max_items=fetch_limit, fetch_sellers=True)
    items = _apply_blacklists(user_id, items)
    items = _dedupe_new(user_id, items)
    return items

async def run_search_buffer_and_send(app, chat_id: int, user_id: int, one_off: bool) -> None:
    s = get_user_settings(user_id)
    max_items = int(s.get("max_items", 30))

    try:
        new_items = await _search_once(user_id)
        if not new_items:
            if one_off:
                await app.bot.send_message(chat_id, "Новых объявлений нет ✅")
            return

        st = get_user_state(user_id)
        buf: List[Dict[str, Any]] = st.get("buffer", []) or []
        buf.extend(new_items)

        # If buffer reached target, send exactly max_items
        while len(buf) >= max_items:
            to_send = buf[:max_items]
            buf = buf[max_items:]

            # mark as sent
            sent = st.get("sent_links", []) or []
            for it in to_send:
                lk = it.get("item_link")
                if lk:
                    sent.append(lk)
            st["sent_links"] = sent[-5000:]

            path = save_json_result(to_send, user_id)
            await app.bot.send_document(chat_id, document=open(path, "rb"))

        st["buffer"] = buf
        set_user_state(user_id, st)

    except Exception as e:
        logger.exception("Search failed for user %s", user_id)
        if one_off:
            await app.bot.send_message(chat_id, f"Ошибка поиска: {e}")

# ---------------- Jobs ----------------
def _remove_job(context: ContextTypes.DEFAULT_TYPE, name: str) -> None:
    for j in context.job_queue.get_jobs_by_name(name):
        j.schedule_removal()

async def job_tick(context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = context.job.data["chat_id"]
    user_id = context.job.data["user_id"]

    running_guard = context.application.bot_data.setdefault("_running_users", set())
    if user_id in running_guard:
        return
    running_guard.add(user_id)
    try:
        await run_search_buffer_and_send(context.application, chat_id, user_id, one_off=False)
    finally:
        running_guard.discard(user_id)

# ---------------- Handlers ----------------
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    owner_id = int(os.getenv("OWNER_ID", "0") or "0")

    if not is_user_allowed(user_id, owner_id):
        await update.message.reply_text("⛔ Доступ запрещён. Напиши админу.")
        return ConversationHandler.END

    # ensure defaults saved
    set_user_settings(user_id, get_user_settings(user_id))
    await update.message.reply_text("Готов ✅", reply_markup=main_menu_kb(user_id, owner_id))
    return MAIN

async def text_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    owner_id = int(os.getenv("OWNER_ID", "0") or "0")

    if not is_user_allowed(user_id, owner_id):
        await update.message.reply_text("⛔ Доступ запрещён. Напиши админу.")
        return MAIN

    st = get_user_state(user_id)
    st["running"] = True
    set_user_state(user_id, st)

    job_name = f"watch_{user_id}"
    _remove_job(context, job_name)

    interval = int(os.getenv("DEFAULT_INTERVAL_SEC", "30"))
    context.job_queue.run_repeating(
        job_tick,
        interval=interval,
        first=2,
        name=job_name,
        data={"chat_id": chat_id, "user_id": user_id},
    )

    await update.message.reply_text("Мониторинг включен ✅", reply_markup=main_menu_kb(user_id, owner_id))
    # immediate run (one-off message if nothing)
    await run_search_buffer_and_send(context.application, chat_id, user_id, one_off=True)
    return MAIN

async def text_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    owner_id = int(os.getenv("OWNER_ID", "0") or "0")

    if not is_user_allowed(user_id, owner_id):
        await update.message.reply_text("⛔ Доступ запрещён. Напиши админу.")
        return MAIN

    _remove_job(context, f"watch_{user_id}")
    st = get_user_state(user_id)
    st["running"] = False
    set_user_state(user_id, st)

    await update.message.reply_text("Мониторинг остановлен ⛔", reply_markup=main_menu_kb(user_id, owner_id))
    return MAIN

async def text_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    owner_id = int(os.getenv("OWNER_ID", "0") or "0")

    if not is_user_allowed(user_id, owner_id):
        await update.message.reply_text("⛔ Доступ запрещён. Напиши админу.")
        return MAIN

    await update.message.reply_text("Настройки ⚙️", reply_markup=settings_menu_kb())
    return MAIN

async def text_count(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Выбери сколько объявлений в JSON:", reply_markup=count_menu_kb())
    return SET_COUNT

async def set_count(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    t = (update.message.text or "").strip()
    if t not in COUNT_CHOICES:
        await update.message.reply_text("Выбери кнопкой.", reply_markup=count_menu_kb())
        return SET_COUNT
    s = get_user_settings(user_id)
    s["max_items"] = int(t)
    set_user_settings(user_id, s)

    # Clear buffer so the next JSON respects new size strictly
    st = get_user_state(user_id)
    st["buffer"] = []
    set_user_state(user_id, st)

    await update.message.reply_text(f"✅ Теперь в JSON: {t}", reply_markup=settings_menu_kb())
    return MAIN

async def text_blacklist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await update.message.reply_text("ЧС 🚫", reply_markup=blacklist_menu_kb(user_id))
    return BL_MENU

async def bl_toggle_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    s = get_user_settings(user_id)
    cur = s.get("edit_blacklist_mode", "personal")
    s["edit_blacklist_mode"] = "general" if cur == "personal" else "personal"
    set_user_settings(user_id, s)
    await update.message.reply_text("Режим ЧС переключен ✅", reply_markup=blacklist_menu_kb(user_id))
    return BL_MENU

async def bl_show(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    gen = get_blacklist_general()
    per = get_blacklist_personal(user_id)
    txt = "🚫 Общий ЧС:\n" + ("\n".join(f"- {x}" for x in gen) if gen else "(пусто)")
    txt += "\n\n🚫 Твой личный ЧС:\n" + ("\n".join(f"- {x}" for x in per) if per else "(пусто)")
    await update.message.reply_text(txt, reply_markup=blacklist_menu_kb(user_id))
    return BL_MENU

async def bl_add_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    mode = get_user_settings(user_id).get("edit_blacklist_mode", "personal")
    mode_txt = "ОБЩИЙ" if mode == "general" else "ЛИЧНЫЙ"
    await update.message.reply_text(f"Введи имя продавца для добавления в {mode_txt} ЧС:", reply_markup=ReplyKeyboardRemove())
    return BL_ADD_NAME

async def bl_add_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    name = (update.message.text or "").strip()
    mode = get_user_settings(user_id).get("edit_blacklist_mode", "personal")
    add_to_blacklist(user_id, name, mode)
    await update.message.reply_text("✅ Добавлено", reply_markup=blacklist_menu_kb(user_id))
    return BL_MENU

async def bl_remove_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    mode = get_user_settings(user_id).get("edit_blacklist_mode", "personal")
    mode_txt = "ОБЩИЙ" if mode == "general" else "ЛИЧНЫЙ"
    await update.message.reply_text(f"Введи имя продавца для удаления из {mode_txt} ЧС:", reply_markup=ReplyKeyboardRemove())
    return BL_REMOVE_NAME

async def bl_remove_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    name = (update.message.text or "").strip()
    mode = get_user_settings(user_id).get("edit_blacklist_mode", "personal")
    remove_from_blacklist(user_id, name, mode)
    await update.message.reply_text("✅ Удалено", reply_markup=blacklist_menu_kb(user_id))
    return BL_MENU

async def text_cats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await update.message.reply_text("Выбери категории:", reply_markup=cats_menu_kb(user_id))
    return CATS_MENU

async def cats_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    t = (update.message.text or "").strip()

    s = get_user_settings(user_id)
    if t in (BTN_BACK, BTN_CATS_CONTINUE):
        await update.message.reply_text("Ок.", reply_markup=settings_menu_kb())
        return MAIN

    if _clean_cat_label(t) == BTN_CATS_CLEAR:
        s["cats_mode"] = "selected"
        s["cats_selected"] = []
        set_user_settings(user_id, s)
        await update.message.reply_text("✅ Очищено", reply_markup=cats_menu_kb(user_id))
        return CATS_MENU

    if _clean_cat_label(t) == BTN_CATS_ALL:
        s["cats_mode"] = "all"
        s["cats_selected"] = []
        set_user_settings(user_id, s)
        await update.message.reply_text("✅ Теперь: Все подряд", reply_markup=cats_menu_kb(user_id))
        return CATS_MENU

    name = _clean_cat_label(t)
    if name in POPULAR_CATEGORIES and name != "Все подряд":
        s["cats_mode"] = "selected"
        sel = set(s.get("cats_selected", []))
        if name in sel:
            sel.remove(name)
        else:
            sel.add(name)
        s["cats_selected"] = sorted(sel)
        set_user_settings(user_id, s)
        await update.message.reply_text("✅ Обновлено", reply_markup=cats_menu_kb(user_id))
        return CATS_MENU

    await update.message.reply_text("Нажми кнопку.", reply_markup=cats_menu_kb(user_id))
    return CATS_MENU

# ---------------- Admin panel ----------------
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    owner_id = int(os.getenv("OWNER_ID", "0") or "0")
    if user_id != owner_id:
        return MAIN
    await update.message.reply_text("Админ панель 🛠", reply_markup=admin_menu_kb())
    return ADMIN_MENU

async def admin_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    owner_id = int(os.getenv("OWNER_ID", "0") or "0")
    if user_id != owner_id:
        return MAIN

    t = (update.message.text or "").strip()

    if t == BTN_PROXIES:
        await update.message.reply_text("Прокси 🛡", reply_markup=proxies_menu_kb())
        return ADMIN_MENU

    if t == BTN_ADMIN_USERS_LIST:
        users = load_allowed_users()
        txt = "Список юзеров (allowlist):\n" + ("\n".join(str(u) for u in users) if users else "(пусто — доступ всем)")
        await update.message.reply_text(txt, reply_markup=admin_menu_kb())
        return ADMIN_MENU

    if t == BTN_ADMIN_USERS_ADD:
        await update.message.reply_text("Введи Telegram user_id для добавления:", reply_markup=ReplyKeyboardRemove())
        return ADMIN_ADD_USER

    if t == BTN_ADMIN_USERS_REMOVE:
        await update.message.reply_text("Введи Telegram user_id для удаления:", reply_markup=ReplyKeyboardRemove())
        return ADMIN_REMOVE_USER

    if t == BTN_BACK:
        await update.message.reply_text("Ок.", reply_markup=main_menu_kb(user_id, owner_id))
        return MAIN

    await update.message.reply_text("Нажми кнопку.", reply_markup=admin_menu_kb())
    return ADMIN_MENU

async def admin_add_user_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    owner_id = int(os.getenv("OWNER_ID", "0") or "0")
    user_id = update.effective_user.id
    if user_id != owner_id:
        return MAIN

    t = (update.message.text or "").strip()
    try:
        uid = int(re.sub(r"\D+", "", t))
    except Exception:
        await update.message.reply_text("Нужно число (user_id).", reply_markup=admin_menu_kb())
        return ADMIN_MENU

    users = load_allowed_users()
    if uid not in users:
        users.append(uid)
        save_allowed_users(users)

    await update.message.reply_text(f"✅ Добавил: {uid}", reply_markup=admin_menu_kb())
    return ADMIN_MENU

async def admin_remove_user_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    owner_id = int(os.getenv("OWNER_ID", "0") or "0")
    user_id = update.effective_user.id
    if user_id != owner_id:
        return MAIN

    t = (update.message.text or "").strip()
    try:
        uid = int(re.sub(r"\D+", "", t))
    except Exception:
        await update.message.reply_text("Нужно число (user_id).", reply_markup=admin_menu_kb())
        return ADMIN_MENU

    users = load_allowed_users()
    if uid in users:
        users.remove(uid)
        save_allowed_users(users)

    await update.message.reply_text(f"✅ Удалил: {uid}", reply_markup=admin_menu_kb())
    return ADMIN_MENU

# ---------------- Proxies (admin only) ----------------
async def proxy_menu_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    owner_id = int(os.getenv("OWNER_ID", "0") or "0")
    user_id = update.effective_user.id
    if user_id != owner_id:
        return MAIN

    t = (update.message.text or "").strip()

    if t == BTN_PX_SET:
        await update.message.reply_text(
            "Вставь список SOCKS5 прокси (каждый с новой строки).\n"
            "Формат: user:pass@host:port или socks5://user:pass@host:port",
            reply_markup=ReplyKeyboardRemove(),
        )
        return PX_SET

    if t == BTN_PX_SHOW:
        prox = proxy_manager.get_proxies()
        await update.message.reply_text("Текущие прокси:\n" + _format_proxy_list(prox), reply_markup=proxies_menu_kb())
        return ADMIN_MENU

    if t == BTN_PX_CLEAR:
        proxy_manager.clear_proxies()
        await update.message.reply_text("✅ Очищено", reply_markup=proxies_menu_kb())
        return ADMIN_MENU

    if t == BTN_BACK:
        await update.message.reply_text("Ок.", reply_markup=admin_menu_kb())
        return ADMIN_MENU

    return ADMIN_MENU

async def proxy_set_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    owner_id = int(os.getenv("OWNER_ID", "0") or "0")
    user_id = update.effective_user.id
    if user_id != owner_id:
        return MAIN

    txt = update.message.text or ""
    lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
    n = proxy_manager.set_proxies(lines)
    await update.message.reply_text(f"✅ Сохранено прокси: {n}", reply_markup=proxies_menu_kb())
    return ADMIN_MENU

# ---------------- Back ----------------
async def go_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    owner_id = int(os.getenv("OWNER_ID", "0") or "0")
    await update.message.reply_text("Ок.", reply_markup=main_menu_kb(user_id, owner_id))
    return MAIN

# ---------------- Webhook helpers ----------------
def _ensure_webhook_url(webhook_base: str, webhook_path: str) -> str:
    base = webhook_base.strip().rstrip("/")
    if not base.startswith("http"):
        base = "https://" + base
    path = webhook_path.strip()
    if not path.startswith("/"):
        path = "/" + path
    return base + path

def main():
    load_dotenv()

    bot_token = os.getenv("BOT_TOKEN", "").strip()
    if not bot_token:
        raise SystemExit("BOT_TOKEN is missing")

    webhook_base = os.getenv("WEBHOOK_BASE_URL", "").strip()
    webhook_path = os.getenv("WEBHOOK_PATH", "/telegram").strip()
    port = int(os.getenv("PORT", "8080"))

    application = ApplicationBuilder().token(bot_token).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", cmd_start)],
        states={
            MAIN: [
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_START)}$"), text_start),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_STOP)}$"), text_stop),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_SETTINGS)}$"), text_settings),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_COUNT)}$"), text_count),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_CATS)}$"), text_cats),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_BLACKLIST)}$"), text_blacklist),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_ADMIN)}$"), admin_panel),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_BACK)}$"), go_back),
            ],
            SET_COUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_count)],
            BL_MENU: [
                MessageHandler(filters.TEXT & filters.Regex(rf"^{re.escape(BTN_BL_MODE)}"), bl_toggle_mode),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_BL_SHOW)}$"), bl_show),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_BL_ADD)}$"), bl_add_prompt),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_BL_REMOVE)}$"), bl_remove_prompt),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_BACK)}$"), go_back),
            ],
            BL_ADD_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, bl_add_name)],
            BL_REMOVE_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, bl_remove_name)],
            CATS_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, cats_click)],
            ADMIN_MENU: [
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_PROXIES)}$"), admin_click),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_ADMIN_USERS_ADD)}$"), admin_click),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_ADMIN_USERS_REMOVE)}$"), admin_click),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_ADMIN_USERS_LIST)}$"), admin_click),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_PX_SET)}$"), proxy_menu_click),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_PX_SHOW)}$"), proxy_menu_click),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_PX_CLEAR)}$"), proxy_menu_click),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_BACK)}$"), admin_click),
            ],
            PX_SET: [MessageHandler(filters.TEXT & ~filters.COMMAND, proxy_set_text)],
            ADMIN_ADD_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_user_text)],
            ADMIN_REMOVE_USER: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_remove_user_text)],
        },
        fallbacks=[CommandHandler("start", cmd_start)],
    )
    application.add_handler(conv)

    if webhook_base:
        webhook_url = _ensure_webhook_url(webhook_base, webhook_path)
        logger.info("Starting webhook on 0.0.0.0:%s url=%s", port, webhook_url)
        application.run_webhook(
            listen="0.0.0.0",
            port=port,
            url_path=webhook_path.lstrip("/"),
            webhook_url=webhook_url,
            drop_pending_updates=True,
            bootstrap_retries=-1,
        )
    else:
        application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
