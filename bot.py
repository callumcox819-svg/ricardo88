import os
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

from dotenv import load_dotenv
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    ConversationHandler,
    filters,
)

from ricardo_parser import apify_search, POPULAR_CATEGORIES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("ricardo_bot")

PROFILE_DIR = Path("Profile")
PROFILE_DIR.mkdir(exist_ok=True)

RESULTS_DIR = Path("Results")
RESULTS_DIR.mkdir(exist_ok=True)

SETTINGS_FILE = PROFILE_DIR / "settings.json"
BLACKLIST_FILE = PROFILE_DIR / "blacklist.json"
STATE_FILE = PROFILE_DIR / "state.json"

# --------- UI ---------
BTN_START = "Старт ✅"
BTN_STOP = "Стоп ⛔"
BTN_SETTINGS = "Настройки ⚙️"
BTN_ADMIN = "Админ панель 🛠"
BTN_BACK = "Назад ↩️"


BTN_CATEGORIES = "Категории 📂"
BTN_CATS_DONE = "🔥 Продолжить настройку"
BTN_CATS_ALL = "Все подряд"
BTN_COUNT = "Кол-во объявлений 📦"
BTN_BLACKLIST = "ЧС 🚫"

BTN_BL_MODE = "Режим ЧС (общий/личный)"
BTN_BL_SHOW = "Показать ЧС"
BTN_BL_ADD = "Добавить в ЧС"
BTN_BL_REMOVE = "Удалить из ЧС"

BTN_AD_STATUS = "Статус 📊"
BTN_AD_SHOW_GBL = "Показать общий ЧС"
BTN_AD_CLEAR_GBL = "Очистить общий ЧС"

COUNT_CHOICES = ["5", "10", "20", "30"]

# Conversation states
MAIN, SET_COUNT, CATS_MENU, BL_MENU, BL_ADD_NAME, BL_REMOVE_NAME, ADMIN_MENU = range(7)

DEFAULT_USER_SETTINGS = {
    "max_items": 30,
    "pages": 3,
    "interval_sec": 600,              # 10 min
    "edit_blacklist_mode": "personal" # what user edits in ЧС menu
}

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

def load_settings() -> Dict[str, Dict[str, Any]]:
    return _load_json(SETTINGS_FILE, {})

def save_settings(data: Dict[str, Dict[str, Any]]) -> None:
    _save_json(SETTINGS_FILE, data)

def get_user_settings(user_id: int) -> Dict[str, Any]:
    all_s = load_settings()
    s = all_s.get(str(user_id), {}).copy()
    for k, v in DEFAULT_USER_SETTINGS.items():
        s.setdefault(k, v)
    return s

def set_user_settings(user_id: int, new_settings: Dict[str, Any]) -> None:
    all_s = load_settings()
    all_s[str(user_id)] = new_settings
    save_settings(all_s)

def load_blacklists() -> Dict[str, Any]:
    # {"general": [...], "personal": {"user_id": [...]} }
    return _load_json(BLACKLIST_FILE, {"general": [], "personal": {}})

def save_blacklists(data: Dict[str, Any]) -> None:
    _save_json(BLACKLIST_FILE, data)

def get_blacklist_general() -> List[str]:
    bl = load_blacklists()
    return bl.get("general", [])

def get_blacklist_personal(user_id: int) -> List[str]:
    bl = load_blacklists()
    return bl.get("personal", {}).get(str(user_id), [])

def add_to_blacklist(user_id: int, name: str, mode: str) -> None:
    name = (name or "").strip()
    if not name:
        return
    bl = load_blacklists()
    if mode == "general":
        lst = bl.setdefault("general", [])
        if name not in lst:
            lst.append(name)
    else:
        per = bl.setdefault("personal", {})
        lst = per.setdefault(str(user_id), [])
        if name not in lst:
            lst.append(name)
    save_blacklists(bl)

def remove_from_blacklist(user_id: int, name: str, mode: str) -> None:
    name = (name or "").strip()
    bl = load_blacklists()
    if mode == "general":
        lst = bl.get("general", [])
        if name in lst:
            lst.remove(name)
    else:
        per = bl.get("personal", {})
        lst = per.get(str(user_id), [])
        if name in lst:
            lst.remove(name)
    save_blacklists(bl)

def clear_general_blacklist() -> None:
    bl = load_blacklists()
    bl["general"] = []
    save_blacklists(bl)

def load_state() -> Dict[str, Any]:
    return _load_json(STATE_FILE, {})

def save_state(data: Dict[str, Any]) -> None:
    _save_json(STATE_FILE, data)

def get_user_state(user_id: int) -> Dict[str, Any]:
    st = load_state()
    s = st.get(str(user_id), {}).copy()
    s.setdefault("sent_links", [])
    s.setdefault("running", False)
    return s

def set_user_state(user_id: int, new_state: Dict[str, Any]) -> None:
    st = load_state()
    st[str(user_id)] = new_state
    save_state(st)

def safe_filename(s: str) -> str:
    s = (s or "").strip().replace(" ", "_")
    return "".join(ch for ch in s if ch.isalnum() or ch in ("_", "-", "."))[:80] or "all"

def main_menu_kb(is_admin: bool) -> ReplyKeyboardMarkup:
    if is_admin:
        return ReplyKeyboardMarkup([[BTN_START, BTN_STOP], [BTN_SETTINGS, BTN_ADMIN]], resize_keyboard=True)
    return ReplyKeyboardMarkup([[BTN_START, BTN_STOP], [BTN_SETTINGS]], resize_keyboard=True)

def settings_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([[BTN_COUNT], [BTN_BLACKLIST], [BTN_BACK]], resize_keyboard=True)

def count_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([COUNT_CHOICES, [BTN_BACK]], resize_keyboard=True)

def blacklist_menu_kb(user_id: int) -> ReplyKeyboardMarkup:
    s = get_user_settings(user_id)
    mode = s.get("edit_blacklist_mode", "personal")
    mode_txt = "личный" if mode == "personal" else "общий"
    return ReplyKeyboardMarkup(
        [[f"{BTN_BL_MODE}: {mode_txt}"],
         [BTN_BL_SHOW],
         [BTN_BL_ADD, BTN_BL_REMOVE],
         [BTN_BACK]],
        resize_keyboard=True,
    )


def _cat_button_label(label: str, selected: bool) -> str:
    return ("✅ " + label) if selected else label

def categories_menu_kb(user_id: int) -> ReplyKeyboardMarkup:
    s = get_user_settings(user_id)
    mode = s.get("categories_mode", "all")
    selected_urls = set(s.get("categories", []) or [])
    rows = []
    labels = list(POPULAR_CATEGORIES.keys())

    # Make 2-column grid
    for i in range(0, len(labels), 2):
        row = []
        for j in range(2):
            if i + j >= len(labels):
                break
            lab = labels[i + j]
            url = POPULAR_CATEGORIES[lab]
            is_sel = (mode == "selected") and (url in selected_urls)
            row.append(_cat_button_label(lab, is_sel))
        rows.append(row)

    # 'All' row
    all_label = _cat_button_label(BTN_CATS_ALL, mode == "all")
    rows.append([all_label])
    rows.append([BTN_CATS_DONE])
    rows.append([BTN_BACK])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def admin_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([[BTN_AD_STATUS], [BTN_AD_SHOW_GBL, BTN_AD_CLEAR_GBL], [BTN_BACK]], resize_keyboard=True)

def save_json_result(items: List[Dict[str, Any]], user_id: int) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = f"ricardo_{user_id}_all_{ts}.json"
    path = RESULTS_DIR / name
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"items": items}, f, ensure_ascii=False, indent=2)
    return path

def filter_by_blacklists(user_id: int, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    blocked = set(get_blacklist_general()) | set(get_blacklist_personal(user_id))
    out = []
    for it in items:
        seller = (it.get("item_person_name") or "").strip()
        if seller and seller in blocked:
            continue
        out.append(it)
    return out

def filter_new_only(user_id: int, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    st = get_user_state(user_id)
    sent = set(st.get("sent_links", []))
    fresh = []
    for it in items:
        link = it.get("item_link")
        if link and link not in sent:
            fresh.append(it)
    return fresh

async def run_search_and_send(app, chat_id: int, user_id: int, one_off: bool = False) -> None:
    s = get_user_settings(user_id)
    max_items = int(s.get("max_items", 30))
    pages = int(s.get("pages", 3))

    try:
        # IMPORTANT: no query – we scan /de/s/ (all listings) and then apply your TZ filters in parser
        items = ricardo_collect_items(query="", pages=pages, max_items=max_items)
        items = filter_by_blacklists(user_id, items)
        items = filter_new_only(user_id, items)

        # Update sent links state
        st = get_user_state(user_id)
        sent_links = st.get("sent_links", [])
        for it in items:
            link = it.get("item_link")
            if link:
                sent_links.append(link)
        st["sent_links"] = sent_links[-2000:]
        set_user_state(user_id, st)

        if not items:
            if one_off:
                await app.bot.send_message(chat_id, "Новых объявлений нет ✅")
            return

        path = save_json_result(items, user_id)
        await app.bot.send_document(chat_id, document=open(path, "rb"))
    except Exception as e:
        logger.exception("Search failed for user %s: %s", user_id, e)
        if one_off:
            await app.bot.send_message(chat_id, f"Ошибка поиска: {e}")

def _remove_job(context: ContextTypes.DEFAULT_TYPE, name: str) -> None:
    jobs = context.job_queue.get_jobs_by_name(name)
    for j in jobs:
        j.schedule_removal()

async def job_tick(context: ContextTypes.DEFAULT_TYPE) -> None:
    job = context.job
    await run_search_and_send(
        context.application,
        chat_id=job.data["chat_id"],
        user_id=job.data["user_id"],
        one_off=False,
    )

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    s = get_user_settings(user_id)
    set_user_settings(user_id, s)

    is_admin = bool(context.application.bot_data.get("OWNER_ID") == user_id)

    await update.message.reply_text(
        "Ricardo Bot ✅\n"
        "Нажми Старт ✅ чтобы включить мониторинг.\n"
        "Настройки: выбери кол-во объявлений, ЧС.\n",
        reply_markup=main_menu_kb(is_admin),
    )
    return MAIN

async def text_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    is_admin = bool(context.application.bot_data.get("OWNER_ID") == user_id)

    st = get_user_state(user_id)
    st["running"] = True
    set_user_state(user_id, st)

    job_name = f"watch_{user_id}"
    _remove_job(context, job_name)

    s = get_user_settings(user_id)
    interval = int(s.get("interval_sec", 600))
    context.job_queue.run_repeating(
        job_tick,
        interval=interval,
        first=1,
        name=job_name,
        data={"chat_id": chat_id, "user_id": user_id},
    )

    await update.message.reply_text("Мониторинг включен ✅", reply_markup=main_menu_kb(is_admin))
    await run_search_and_send(context.application, chat_id=chat_id, user_id=user_id, one_off=True)
    return MAIN

async def text_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    is_admin = bool(context.application.bot_data.get("OWNER_ID") == user_id)

    job_name = f"watch_{user_id}"
    _remove_job(context, job_name)

    st = get_user_state(user_id)
    st["running"] = False
    set_user_state(user_id, st)

    await update.message.reply_text("Мониторинг остановлен ⛔", reply_markup=main_menu_kb(is_admin))
    return MAIN

async def text_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Настройки ⚙️", reply_markup=settings_menu_kb())
    return MAIN

async def text_count(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Выбери сколько объявлений собирать в один JSON:", reply_markup=count_menu_kb())
    return SET_COUNT

async def set_count(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    t = (update.message.text or "").strip()
    if t not in COUNT_CHOICES:
        await update.message.reply_text("Выбери вариант кнопкой.", reply_markup=count_menu_kb())
        return SET_COUNT
    s = get_user_settings(user_id)
    s["max_items"] = int(t)
    set_user_settings(user_id, s)
    await update.message.reply_text(f"✅ Теперь в JSON: {t} объявлений", reply_markup=settings_menu_kb())
    return MAIN



async def text_categories(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await update.message.reply_text("Категории 📂\nВыбери категории или 'Все подряд':", reply_markup=categories_menu_kb(user_id))
    return CATS_MENU

async def cats_handle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    txt = (update.message.text or "").strip()

    # Continue
    if txt == BTN_CATS_DONE:
        await update.message.reply_text("✅ Категории сохранены.", reply_markup=settings_menu_kb())
        return MAIN

    if txt == BTN_BACK:
        await update.message.reply_text("Ок.", reply_markup=settings_menu_kb())
        return MAIN

    # normalize label
    if txt.startswith("✅ "):
        txt = txt[2:].strip()

    s = get_user_settings(user_id)

    # All mode
    if txt == BTN_CATS_ALL:
        s["categories_mode"] = "all"
        s["categories"] = []
        set_user_settings(user_id, s)
        await update.message.reply_text("✅ Режим: Все подряд", reply_markup=categories_menu_kb(user_id))
        return CATS_MENU

    # Toggle category
    if txt in POPULAR_CATEGORIES:
        url = POPULAR_CATEGORIES[txt]
        sel = set(s.get("categories", []) or [])
        if s.get("categories_mode") != "selected":
            s["categories_mode"] = "selected"
        if url in sel:
            sel.remove(url)
        else:
            sel.add(url)
        s["categories"] = sorted(sel)
        # If none selected, fallback to all
        if not s["categories"]:
            s["categories_mode"] = "all"
        set_user_settings(user_id, s)
        await update.message.reply_text("✅ Обновил выбор.", reply_markup=categories_menu_kb(user_id))
        return CATS_MENU

    await update.message.reply_text("Нажми кнопку категории.", reply_markup=categories_menu_kb(user_id))
    return CATS_MENU
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

    txt = "🚫 Общий ЧС:\n"
    txt += "\n".join(f"- {x}" for x in gen) if gen else "(пусто)"
    txt += "\n\n🚫 Твой личный ЧС:\n"
    txt += "\n".join(f"- {x}" for x in per) if per else "(пусто)"

    await update.message.reply_text(txt, reply_markup=blacklist_menu_kb(user_id))
    return BL_MENU

async def bl_add_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    mode = get_user_settings(user_id).get("edit_blacklist_mode", "personal")
    mode_txt = "ОБЩИЙ" if mode == "general" else "ЛИЧНЫЙ"
    await update.message.reply_text(
        f"Введи имя продавца для добавления в {mode_txt} ЧС:",
        reply_markup=ReplyKeyboardRemove(),
    )
    return BL_ADD_NAME

async def bl_add_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    name = (update.message.text or "").strip()
    mode = get_user_settings(user_id).get("edit_blacklist_mode", "personal")
    add_to_blacklist(user_id, name, mode)
    await update.message.reply_text("✅ Добавлено.", reply_markup=blacklist_menu_kb(user_id))
    return BL_MENU

async def bl_remove_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    mode = get_user_settings(user_id).get("edit_blacklist_mode", "personal")
    mode_txt = "ОБЩИЙ" if mode == "general" else "ЛИЧНЫЙ"
    await update.message.reply_text(
        f"Введи имя продавца для удаления из {mode_txt} ЧС:",
        reply_markup=ReplyKeyboardRemove(),
    )
    return BL_REMOVE_NAME

async def bl_remove_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    name = (update.message.text or "").strip()
    mode = get_user_settings(user_id).get("edit_blacklist_mode", "personal")
    remove_from_blacklist(user_id, name, mode)
    await update.message.reply_text("✅ Удалено.", reply_markup=blacklist_menu_kb(user_id))
    return BL_MENU

async def text_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    owner_id = context.application.bot_data.get("OWNER_ID")
    if owner_id != user_id:
        await update.message.reply_text("Нет доступа.")
        return MAIN
    await update.message.reply_text("Админ панель 🛠", reply_markup=admin_menu_kb())
    return ADMIN_MENU

async def admin_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    st = load_state()
    running = [uid for uid, data in st.items() if isinstance(data, dict) and data.get("running")]
    users_total = len(st.keys())
    await update.message.reply_text(
        f"📊 Статус\nПользователей: {users_total}\nМониторинг активен у: {len(running)}",
        reply_markup=admin_menu_kb(),
    )
    return ADMIN_MENU

async def admin_show_general_bl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    gen = get_blacklist_general()
    txt = "🚫 Общий ЧС:\n" + ("\n".join(f"- {x}" for x in gen) if gen else "(пусто)")
    await update.message.reply_text(txt, reply_markup=admin_menu_kb())
    return ADMIN_MENU

async def admin_clear_general_bl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    clear_general_blacklist()
    await update.message.reply_text("✅ Общий ЧС очищен.", reply_markup=admin_menu_kb())
    return ADMIN_MENU

async def go_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    is_admin = bool(context.application.bot_data.get("OWNER_ID") == user_id)
    await update.message.reply_text("Ок.", reply_markup=main_menu_kb(is_admin))
    return MAIN

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
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise SystemExit("BOT_TOKEN is missing. Set Railway Variable BOT_TOKEN or create .env from .env.example")

    owner_id = int(os.getenv("OWNER_ID", "0") or "0")

    webhook_base = os.getenv("WEBHOOK_BASE_URL", "").strip()
    webhook_path = os.getenv("WEBHOOK_PATH", "/telegram").strip()
    port = int(os.getenv("PORT", "8080"))

    application = ApplicationBuilder().token(token).build()
    application.bot_data["OWNER_ID"] = owner_id

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", cmd_start)],
        states={
            MAIN: [
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_START)}$"), text_start),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_STOP)}$"), text_stop),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_SETTINGS)}$"), text_settings),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_COUNT)}$"), text_count),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_BLACKLIST)}$"), text_blacklist),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_ADMIN)}$"), text_admin),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_BACK)}$"), go_back),
            ],
            SET_COUNT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, set_count),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_BACK)}$"), go_back),
            ],
            CATS_MENU: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, cats_handle),
            ],
            BL_MENU: [
                MessageHandler(filters.TEXT & filters.Regex(rf"^{re.escape(BTN_BL_MODE)}"), bl_toggle_mode),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_BL_SHOW)}$"), bl_show),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_BL_ADD)}$"), bl_add_prompt),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_BL_REMOVE)}$"), bl_remove_prompt),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_BACK)}$"), go_back),
            ],
            BL_ADD_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, bl_add_name),
            ],
            BL_REMOVE_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, bl_remove_name),
            ],
            ADMIN_MENU: [
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_AD_STATUS)}$"), admin_status),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_AD_SHOW_GBL)}$"), admin_show_general_bl),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_AD_CLEAR_GBL)}$"), admin_clear_general_bl),
                MessageHandler(filters.TEXT & filters.Regex(f"^{re.escape(BTN_BACK)}$"), go_back),
            ],
        },
        fallbacks=[CommandHandler("start", cmd_start)],
        name="main_conv",
        persistent=False,
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
        logger.info("Starting polling (WEBHOOK_BASE_URL is empty)")
        application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
