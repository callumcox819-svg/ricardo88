import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict

from dotenv import load_dotenv

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

from ricardo_parser import ricardo_collect_items

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("ricardo_bot")

RESULTS_DIR = Path("Results")
RESULTS_DIR.mkdir(exist_ok=True)

def safe_filename(s: str) -> str:
    s = s.strip().replace(" ", "_")
    return "".join(ch for ch in s if ch.isalnum() or ch in ("_", "-", "."))[:80] or "query"

def save_json(items: List[Dict], query: str) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = f"ricardo_{safe_filename(query)}_{ts}.json"
    path = RESULTS_DIR / name
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"items": items}, f, ensure_ascii=False, indent=2)
    return path

def save_txt(items: List[Dict], query: str) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = f"ricardo_{safe_filename(query)}_{ts}.txt"
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

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Ricardo Bot ✅\n"
        "Команда: /ricardo Имя Фамилия [json|txt]\n"
        "Пример: /ricardo Max Mustermann json"
    )

async def ricardo_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("Напиши: /ricardo Имя Фамилия [json|txt]")
        return

    fmt = "json"
    if args and args[-1].lower() in ("json", "txt"):
        fmt = args[-1].lower()
        query = " ".join(args[:-1]).strip()
    else:
        query = " ".join(args).strip()

    if not query:
        await update.message.reply_text("Запрос пустой. Пример: /ricardo Max Mustermann")
        return

    msg = await update.message.reply_text("Начал работу ✅\nСобираю объявления: 0/30")

    # main
    items = ricardo_collect_items(query=query, pages=3, max_items=30)

    await msg.edit_text(f"Начал работу ✅\nСобираю объявления: {len(items)}/30")

    path = save_txt(items, query) if fmt == "txt" else save_json(items, query)
    await update.message.reply_document(document=open(path, "rb"))

def main():
    load_dotenv()
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise SystemExit("BOT_TOKEN is missing. Create .env from .env.example or set Railway Variables.")

    app = ApplicationBuilder().token(token).build()

    # On start: clear webhook + pending updates (helps avoid conflicts)
    async def _on_start(app_):
        try:
            await app_.bot.delete_webhook(drop_pending_updates=True)
        except Exception as e:
            logger.warning("delete_webhook failed: %s", e)

    app.post_init = _on_start

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("ricardo", ricardo_cmd))

    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
