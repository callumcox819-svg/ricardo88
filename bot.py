
import os
import re
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

from dotenv import load_dotenv
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, ConversationHandler,
    ContextTypes, filters
)

from ricardo_parser import POPULAR_CATEGORIES, ricardo_collect_items

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("ricardo_bot")

PROFILE_DIR = Path("Profile"); PROFILE_DIR.mkdir(exist_ok=True)
RESULTS_DIR = Path("Results"); RESULTS_DIR.mkdir(exist_ok=True)

SETTINGS_FILE = PROFILE_DIR / "settings.json"
BLACKLIST_FILE = PROFILE_DIR / "blacklist.json"
STATE_FILE = PROFILE_DIR / "state.json"

# Buttons (keep existing)
BTN_START = "Старт ✅"
BTN_STOP = "Стоп ⛔"
BTN_SETTINGS = "Настройки ⚙️"
BTN_ADMIN = "Админ панель 🛠"
BTN_BACK = "Назад ↩️"

BTN_COUNT = "Кол-во объявлений 📦"
BTN_CATS = "Категории 📂"
BTN_BLACKLIST = "ЧС 🚫"

BTN_BL_MODE = "Режим ЧС (общий/личный)"
BTN_BL_SHOW = "Показать ЧС"
BTN_BL_ADD = "Добавить в ЧС"
BTN_BL_REMOVE = "Удалить из ЧС"

BTN_CATS_ALL = "Все подряд"
BTN_CATS_CONTINUE = "🔥 Продолжить настройку"
BTN_CATS_CLEAR = "Очистить выбор"

COUNT_CHOICES = ["5", "10", "20", "30"]

# States
MAIN, SET_COUNT, BL_MENU, BL_ADD_NAME, BL_REMOVE_NAME, CATS_MENU = range(6)

DEFAULT_USER_SETTINGS = {
    "max_items": 30,
    "interval_sec": 60,  # "по кд" but still safe for Apify
    "cats_mode": "all",  # all | selected
    "cats_selected": [], # list of category names
    "edit_blacklist_mode": "personal",  # personal | general
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

def set_user_settings(user_id: int, s: Dict[str, Any]) -> None:
    all_s = load_settings()
    all_s[str(user_id)] = s
    save_settings(all_s)

def load_blacklists() -> Dict[str, Any]:
    return _load_json(BLACKLIST_FILE, {"general": [], "personal": {}})

def save_blacklists(data: Dict[str, Any]) -> None:
    _save_json(BLACKLIST_FILE, data)

def get_blacklist_general() -> List[str]:
    return load_blacklists().get("general", [])

def get_blacklist_personal(user_id: int) -> List[str]:
    return load_blacklists().get("personal", {}).get(str(user_id), [])

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

def set_user_state(user_id: int, s: Dict[str, Any]) -> None:
    st = load_state()
    st[str(user_id)] = s
    save_state(st)

def main_menu_kb(user_id: int, owner_id: int) -> ReplyKeyboardMarkup:
    rows = [[BTN_START, BTN_STOP], [BTN_SETTINGS]]
    if user_id == owner_id:
        rows.append([BTN_ADMIN])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def settings_menu_kb(user_id: int, owner_id: int) -> ReplyKeyboardMarkup:
    rows = [[BTN_COUNT], [BTN_CATS], [BTN_BLACKLIST], [BTN_BACK]]
    if user_id == owner_id:
        rows.append([BTN_ADMIN])
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

def cats_menu_kb(user_id: int) -> ReplyKeyboardMarkup:
    s = get_user_settings(user_id)
    mode = s.get("cats_mode", "all")
    selected = set(s.get("cats_selected", []))

    # Build 2-column grid like your screenshots (except control buttons at bottom)
    names = [k for k in POPULAR_CATEGORIES.keys() if k != "Все подряд"]
    rows = []
    for i in range(0, len(names), 2):
        row = []
        for name in names[i:i+2]:
            label = f"✅ {name}" if (mode == "selected" and name in selected) else name
            row.append(label)
        rows.append(row)

    # Add "Все подряд" row
    all_label = f"✅ {BTN_CATS_ALL}" if mode == "all" else BTN_CATS_ALL
    rows.append([all_label])

    rows.append([BTN_CATS_CLEAR])
    rows.append([BTN_CATS_CONTINUE])
    rows.append([BTN_BACK])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def safe_filename(s: str) -> str:
    s = (s or "").strip().replace(" ", "_")
    return "".join(ch for ch in s if ch.isalnum() or ch in ("_", "-", "."))[:80] or "items"

def save_json_result(items: List[Dict[str, Any]], user_id: int) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = f"ricardo_{user_id}_{ts}.json"
    path = RESULTS_DIR / name
    path.write_text(json.dumps({"items": items}, ensure_ascii=False, indent=2), encoding="utf-8")
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
    fresh = [it for it in items if it.get("item_link") and it["item_link"] not in sent]
    return fresh

async def run_search_and_send(app, chat_id: int, user_id: int, one_off: bool = False) -> None:
    s = get_user_settings(user_id)
    max_items = int(s.get("max_items", 30))
    mode = s.get("cats_mode", "all")
    selected = s.get("cats_selected", [])

    if mode == "all":
        urls = [POPULAR_CATEGORIES["Все подряд"]]
    else:
        if not selected:
            urls = [POPULAR_CATEGORIES["Все подряд"]]
        else:
            urls = [POPULAR_CATEGORIES[n] for n in selected if n in POPULAR_CATEGORIES]

    try:
        items = ricardo_collect_items(urls=urls, max_items=max_items)
        logger.info("Apify returned %s items before filters for user %s", len(items), user_id)
        items = filter_by_blacklists(user_id, items)
        items = filter_new_only(user_id, items)

        st = get_user_state(user_id)
        sent_links = st.get("sent_links", [])
        for it in items:
            link = it.get("item_link")
            if link:
                sent_links.append(link)
        st["sent_links"] = sent_links[-3000:]
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

RUN_GUARD = "_running_users"

async def job_tick(context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = context.job.data["chat_id"]
    user_id = context.job.data["user_id"]
    running = context.application.bot_data.setdefault(RUN_GUARD, set())
    if user_id in running:
        return
    running.add(user_id)
    try:
        await run_search_and_send(context.application, chat_id=chat_id, user_id=user_id, one_off=False)
    finally:
        running.discard(user_id)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    owner_id = int(os.getenv("OWNER_ID", "0") or "0")
    s = get_user_settings(user_id)
    set_user_settings(user_id, s)
    await update.message.reply_text("Готов ✅", reply_markup=main_menu_kb(user_id, owner_id))
    return MAIN

async def text_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    owner_id = int(os.getenv("OWNER_ID", "0") or "0")

    st = get_user_state(user_id)
    st["running"] = True
    set_user_state(user_id, st)

    job_name = f"watch_{user_id}"
    _remove_job(context, job_name)

    interval = int(get_user_settings(user_id).get("interval_sec", 60))
    context.job_queue.run_repeating(job_tick, interval=interval, first=1, name=job_name, data={"chat_id": chat_id, "user_id": user_id})
    await update.message.reply_text("Мониторинг включен ✅", reply_markup=main_menu_kb(user_id, owner_id))
    await run_search_and_send(context.application, chat_id=chat_id, user_id=user_id, one_off=True)
    return MAIN

async def text_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    owner_id = int(os.getenv("OWNER_ID", "0") or "0")

    _remove_job(context, f"watch_{user_id}")
    st = get_user_state(user_id)
    st["running"] = False
    set_user_state(user_id, st)
    await update.message.reply_text("Мониторинг остановлен ⛔", reply_markup=main_menu_kb(user_id, owner_id))
    return MAIN

async def text_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    owner_id = int(os.getenv("OWNER_ID", "0") or "0")
    await update.message.reply_text("Настройки ⚙️", reply_markup=settings_menu_kb(user_id, owner_id))
    return MAIN

async def text_count(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Выбери сколько объявлений в JSON:", reply_markup=count_menu_kb())
    return SET_COUNT

async def set_count(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    owner_id = int(os.getenv("OWNER_ID", "0") or "0")
    t = (update.message.text or "").strip()
    if t not in COUNT_CHOICES:
        await update.message.reply_text("Выбери кнопкой.", reply_markup=count_menu_kb())
        return SET_COUNT
    s = get_user_settings(user_id)
    s["max_items"] = int(t)
    set_user_settings(user_id, s)
    await update.message.reply_text(f"✅ Теперь в JSON: {t}", reply_markup=settings_menu_kb(user_id, owner_id))
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
    s = get_user_settings(user_id)
    mode = s.get("edit_blacklist_mode", "personal")
    mode_txt = "ОБЩИЙ" if mode == "general" else "ЛИЧНЫЙ"
    await update.message.reply_text(f"Введи имя продавца для добавления в {mode_txt} ЧС:", reply_markup=ReplyKeyboardRemove())
    return BL_ADD_NAME

async def bl_add_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    name = (update.message.text or "").strip()
    s = get_user_settings(user_id)
    mode = s.get("edit_blacklist_mode", "personal")
    add_to_blacklist(user_id, name, mode)
    await update.message.reply_text("✅ Добавлено.", reply_markup=blacklist_menu_kb(user_id))
    return BL_MENU

async def bl_remove_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    s = get_user_settings(user_id)
    mode = s.get("edit_blacklist_mode", "personal")
    mode_txt = "ОБЩИЙ" if mode == "general" else "ЛИЧНЫЙ"
    await update.message.reply_text(f"Введи имя продавца для удаления из {mode_txt} ЧС:", reply_markup=ReplyKeyboardRemove())
    return BL_REMOVE_NAME

async def bl_remove_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    name = (update.message.text or "").strip()
    s = get_user_settings(user_id)
    mode = s.get("edit_blacklist_mode", "personal")
    remove_from_blacklist(user_id, name, mode)
    await update.message.reply_text("✅ Удалено.", reply_markup=blacklist_menu_kb(user_id))
    return BL_MENU

async def text_cats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await update.message.reply_text("Выбери категории ✅", reply_markup=cats_menu_kb(user_id))
    return CATS_MENU

def _clean_label(text: str) -> str:
    return text.replace("✅", "").strip()

async def cats_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    t = (update.message.text or "").strip()

    s = get_user_settings(user_id)
    if _clean_label(t) == BTN_CATS_ALL:
        s["cats_mode"] = "all"
        s["cats_selected"] = []
        set_user_settings(user_id, s)
        await update.message.reply_text("✅ Режим: Все подряд", reply_markup=cats_menu_kb(user_id))
        return CATS_MENU

    if t == BTN_CATS_CLEAR:
        s["cats_mode"] = "selected"
        s["cats_selected"] = []
        set_user_settings(user_id, s)
        await update.message.reply_text("✅ Выбор очищен", reply_markup=cats_menu_kb(user_id))
        return CATS_MENU

    if t == BTN_CATS_CONTINUE:
        owner_id = int(os.getenv("OWNER_ID", "0") or "0")
        await update.message.reply_text("Ок.", reply_markup=settings_menu_kb(user_id, owner_id))
        return MAIN

    name = _clean_label(t)
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

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    owner_id = int(os.getenv("OWNER_ID", "0") or "0")
    if user_id != owner_id:
        return
    st = load_state()
    users = len(st.keys())
    await update.message.reply_text(f"Админ панель 🛠\nUsers: {users}", reply_markup=main_menu_kb(user_id, owner_id))

async def go_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    owner_id = int(os.getenv("OWNER_ID", "0") or "0")
    await update.message.reply_text("Ок.", reply_markup=main_menu_kb(user_id, owner_id))
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
        raise SystemExit("BOT_TOKEN is missing")

    webhook_base = os.getenv("WEBHOOK_BASE_URL", "").strip()
    webhook_path = os.getenv("WEBHOOK_PATH", "/telegram").strip()
    port = int(os.getenv("PORT", "8080"))

    application = ApplicationBuilder().token(token).build()

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
            SET_COUNT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, set_count),
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
            CATS_MENU: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, cats_click),
            ],
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
