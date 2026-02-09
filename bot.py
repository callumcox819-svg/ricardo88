import os
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

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

from ricardo_parser import ricardo_collect_items

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

# ----- UI Text -----
BTN_START = "Старт ✅"
BTN_STOP = "Стоп ⛔"
BTN_SETTINGS = "Настройки ⚙️"
BTN_BACK = "Назад ↩️"

BTN_QUERY = "Запрос 🔎"
BTN_COUNT = "Кол-во объявлений 📦"
BTN_BLACKLIST = "ЧС 🚫"

BTN_BL_MODE = "Режим ЧС (общий/личный)"
BTN_BL_SHOW = "Показать ЧС"
BTN_BL_ADD = "Добавить в ЧС"
BTN_BL_REMOVE = "Удалить из ЧС"

COUNT_CHOICES = ["5", "10", "20", "30"]

# Conversation states
MAIN, SET_QUERY, SET_COUNT, BL_MENU, BL_SET_MODE, BL_ADD_NAME, BL_REMOVE_NAME = range(7)

DEFAULT_USER_SETTINGS = {
    "query": "",
    "max_items": 30,
    "pages": 3,
    "interval_sec": 600,   # 10 min
    "edit_blacklist_mode": "personal",  # which list user edits in UI: personal/general
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
    # {"general": [...], "personal": {"user_id": [...]}}
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

def load_state() -> Dict[str, Any]:
    # {"user_id": {"sent_links": [...], "running": bool}}
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

def main_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([[BTN_START, BTN_STOP], [BTN_SETTINGS]], resize_keyboard=True)

def settings_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([[BTN_QUERY, BTN_COUNT], [BTN_BLACKLIST], [BTN_BACK]], resize_keyboard=True)

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

def safe_filename(s: str) -> str:
    s = (s or "").strip().replace(" ", "_")
    return "".join(ch for ch in s if ch.isalnum() or ch in ("_", "-", "."))[:80] or "query"

def save_json_result(items: List[Dict[str, Any]], user_id: int, query: str) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = f"ricardo_{user_id}_{safe_filename(query)}_{ts}.json"
    path = RESULTS_DIR / name
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"items": items}, f, ensure_ascii=False, indent=2)
    return path

def save_txt_result(items: List[Dict[str, Any]], user_id: int, query: str) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = f"ricardo_{user_id}_{safe_filename(query)}_{ts}.txt"
    path = RESULTS_DIR / name
    lines = []
    for i, it in enumerate(items, 1):
        lines.append(f"{i}. {it.get('item_title','')}")
        lines.append(f"   PRICE: {it.get('item_price','')}")
        lines.append(f"   SELLER: {it.get('item_person_name','')}")
        lines.append(f"   LINK: {it.get('item_link','')}")
        lines.append(f"   PHOTO: {it.get('item_photo','')}")
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")
    return path

def filter_by_blacklists(user_id: int, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    general = set(get_blacklist_general())
    personal = set(get_blacklist_personal(user_id))
    blocked = general.union(personal)
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
    query = (s.get("query") or "").strip()
    if not query:
        if one_off:
            await app.bot.send_message(chat_id, "Сначала задай запрос в Настройках → Запрос 🔎")
        return

    max_items = int(s.get("max_items", 30))
    pages = int(s.get("pages", 3))

    try:
        items = ricardo_collect_items(query=query, pages=pages, max_items=max_items)
        items = filter_by_blacklists(user_id, items)
        items = filter_new_only(user_id, items)

        # Update sent links state (remember what we sent)
        st = get_user_state(user_id)
        sent_links = st.get("sent_links", [])
        for it in items:
            link = it.get("item_link")
            if link:
                sent_links.append(link)
        # keep last 2000 to avoid huge file
        sent_links = sent_links[-2000:]
        st["sent_links"] = sent_links
        set_user_state(user_id, st)

        if not items:
            if one_off:
                await app.bot.send_message(chat_id, "Новых объявлений нет ✅")
            return

        path = save_json_result(items, user_id, query)
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
    chat_id = job.data["chat_id"]
    user_id = job.data["user_id"]
    await run_search_and_send(context.application, chat_id=chat_id, user_id=user_id, one_off=False)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    s = get_user_settings(user_id)
    set_user_settings(user_id, s)  # persist defaults
    await update.message.reply_text(
        "Ricardo Bot ✅\n"
        "Нажми Старт ✅ чтобы включить мониторинг.\n"
        "В Настройках задай запрос и кол-во объявлений.\n",
        reply_markup=main_menu_kb(),
    )
    return MAIN

async def text_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id

    s = get_user_settings(user_id)
    query = (s.get("query") or "").strip()
    if not query:
        await update.message.reply_text("Сначала задай Запрос 🔎 в Настройках.", reply_markup=settings_menu_kb())
        return MAIN

    st = get_user_state(user_id)
    st["running"] = True
    set_user_state(user_id, st)

    job_name = f"watch_{user_id}"
    _remove_job(context, job_name)

    interval = int(s.get("interval_sec", 600))
    context.job_queue.run_repeating(
        job_tick,
        interval=interval,
        first=1,
        name=job_name,
        data={"chat_id": chat_id, "user_id": user_id},
    )

    await update.message.reply_text("Мониторинг включен ✅", reply_markup=main_menu_kb())
    # immediate one-off run (so user sees something right away if exists)
    await run_search_and_send(context.application, chat_id=chat_id, user_id=user_id, one_off=True)
    return MAIN

async def text_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    job_name = f"watch_{user_id}"
    _remove_job(context, job_name)

    st = get_user_state(user_id)
    st["running"] = False
    set_user_state(user_id, st)

    await update.message.reply_text("Мониторинг остановлен ⛔", reply_markup=main_menu_kb())
    return MAIN

async def text_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Настройки ⚙️", reply_markup=settings_menu_kb())
    return MAIN

async def text_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Введи запрос (например: Имя Фамилия):", reply_markup=ReplyKeyboardRemove())
    return SET_QUERY

async def set_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    q = (update.message.text or "").strip()
    s = get_user_settings(user_id)
    s["query"] = q
    set_user_settings(user_id, s)
    await update.message.reply_text(f"✅ Запрос сохранен: {q}", reply_markup=settings_menu_kb())
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

async def go_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Ок.", reply_markup=main_menu_kb())
    return MAIN

async def cmd_ricardo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # one-off search by command, independent of monitoring settings
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    args = context.args
    if not args:
        await update.message.reply_text("Используй: /ricardo Имя Фамилия", reply_markup=main_menu_kb())
        return

    query = " ".join(args).strip()
    s = get_user_settings(user_id)
    # temporarily run with this query, but do not overwrite stored query
    tmp = s.copy()
    tmp["query"] = query
    set_user_settings(user_id, tmp)

    await update.message.reply_text("Ищу объявления...")
    await run_search_and_send(context.application, chat_id=chat_id, user_id=user_id, one_off=True)

    # restore
    tmp["query"] = s.get("query", "")
    set_user_settings(user_id, tmp)

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

    webhook_base = os.getenv("WEBHOOK_BASE_URL", "").strip()
    webhook_path = os.getenv("WEBHOOK_PATH", "/telegram").strip()
    port = int(os.getenv("PORT", "8080"))

    application = ApplicationBuilder().token(token).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", cmd_start)],
        states={
            MAIN: [
                MessageHandler(filters.TEXT & filters.Regex(f"^{BTN_START}$"), text_start),
                MessageHandler(filters.TEXT & filters.Regex(f"^{BTN_STOP}$"), text_stop),
                MessageHandler(filters.TEXT & filters.Regex(f"^{BTN_SETTINGS}$"), text_settings),
                MessageHandler(filters.TEXT & filters.Regex(f"^{BTN_QUERY}$"), text_query),
                MessageHandler(filters.TEXT & filters.Regex(f"^{BTN_COUNT}$"), text_count),
                MessageHandler(filters.TEXT & filters.Regex(f"^{BTN_BLACKLIST}$"), text_blacklist),
                MessageHandler(filters.TEXT & filters.Regex(f"^{BTN_BACK}$"), go_back),
            ],
            SET_QUERY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, set_query),
            ],
            SET_COUNT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, set_count),
                MessageHandler(filters.TEXT & filters.Regex(f"^{BTN_BACK}$"), go_back),
            ],
            BL_MENU: [
                MessageHandler(filters.TEXT & filters.Regex(rf"^{re.escape(BTN_BL_MODE)}"), bl_toggle_mode),
                MessageHandler(filters.TEXT & filters.Regex(f"^{BTN_BL_SHOW}$"), bl_show),
                MessageHandler(filters.TEXT & filters.Regex(f"^{BTN_BL_ADD}$"), bl_add_prompt),
                MessageHandler(filters.TEXT & filters.Regex(f"^{BTN_BL_REMOVE}$"), bl_remove_prompt),
                MessageHandler(filters.TEXT & filters.Regex(f"^{BTN_BACK}$"), go_back),
            ],
            BL_ADD_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, bl_add_name),
            ],
            BL_REMOVE_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, bl_remove_name),
            ],
        },
        fallbacks=[CommandHandler("start", cmd_start)],
        name="main_conv",
        persistent=False,
    )

    application.add_handler(conv)
    application.add_handler(CommandHandler("ricardo", cmd_ricardo))

    # Startup webhook/polling
    if webhook_base:
        webhook_url = _ensure_webhook_url(webhook_base, webhook_path)
        logger.info("Starting webhook on 0.0.0.0:%s url=%s", port, webhook_url)
        application.run_webhook(
            listen="0.0.0.0",
            port=port,
            url_path=webhook_path.lstrip("/"),
            webhook_url=webhook_url,
            drop_pending_updates=True,
            bootstrap_retries=-1,  # keep retrying if Telegram temporarily unreachable
        )
    else:
        logger.info("Starting polling (WEBHOOK_BASE_URL is empty)")
        application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
