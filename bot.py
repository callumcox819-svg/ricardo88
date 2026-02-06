import asyncio
import json
import logging
import re
import time
import os
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Optional, Any, Tuple

import requests
from bs4 import BeautifulSoup
from telegram import Update, InputFile, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.request import HTTPXRequest
from ricardo_parser import ricardo_collect_items

from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    ConversationHandler,
    filters,
)

# =============== НАСТРОЙКИ ===============

# ВСТАВЬ СВОЙ ТОКЕН СЮДА (одна строка)
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

OWNER_ID = int(os.getenv("OWNER_ID", "7416000184"))

BASE_URL = "https://www.kleinanzeigen.de"
MAIN_URL = BASE_URL + "/"
CATEGORIES_URL = BASE_URL + "/s-kategorien.html"

DEFAULT_BATCH_SIZE = 30  # быстрее отдаём результаты
MAX_CATEGORIES = 12
CATEGORY_MAX_PAGES = 2
SCAN_INTERVAL = 20
PER_AD_DELAY = 0.05
CATEGORY_FAST_PAGES = 2  # сканируем первые страницы каждой категории каждый цикл
CATEGORY_DEEP_PAGES_PER_CYCLE = 1  # дополнительно сканируем 1 "глубокую" страницу, чтобы покрывать до 3 часов


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}

# Быстрый фильтр по URL (как было)
EXCLUDED_CATEGORY_PATTERNS = [
    "auto-rad-boot",
    "/s-autos",
    "autos",
    "/s-auto-",
    "immobilien",
    "/s-immobilien",
    "/s-wohnung-",
    "/s-haus-",
    "wohnung-mieten",
    "wohnung-kaufen",
    "haus-mieten",
    "haus-kaufen",
]

# Жёсткие regex по URL категории
EXCLUDED_CATEGORY_REGEX = [
    re.compile(r"/auto-rad-boot", re.IGNORECASE),
    re.compile(r"/s-autos\b", re.IGNORECASE),
    re.compile(r"/s-auto-", re.IGNORECASE),
    re.compile(r"/autos?\b", re.IGNORECASE),

    re.compile(r"/motorrad", re.IGNORECASE),
    re.compile(r"/moped", re.IGNORECASE),
    re.compile(r"/roller", re.IGNORECASE),
    re.compile(r"/quad", re.IGNORECASE),

    re.compile(r"/anhaenger|/anhänger|trailer|wohnwagen|caravan", re.IGNORECASE),
    re.compile(r"/boot|yacht|schiff|jetski", re.IGNORECASE),
    re.compile(r"/traktor|trecker|landmaschine|agrar", re.IGNORECASE),

    re.compile(r"/immobilien", re.IGNORECASE),
    re.compile(r"/s-immobilien", re.IGNORECASE),
    re.compile(r"/wohnung|/haus|mieten|kaufen|vermieten", re.IGNORECASE),
]

# Текстовые слова категории (только из breadcrumb!)
EXCLUDED_CATEGORY_TEXT_KEYWORDS = [
    "Auto, Rad & Boot",
    "Autos",
    "Motorräder",
    "Motorrad",
    "Roller",
    "Anhänger",
    "Anhaenger",
    "Wohnwagen",
    "Boot",
    "Yacht",
    "Traktor",
    "Immobilien",
    "Wohnung",
    "Haus",
    "Grundstück",
    "Garage",
]

# Доп. страховка: бан по title (не основной)
BANNED_TITLE_KEYWORDS = [
    # транспорт
    "auto", "pkw", "kfz", "wagen", "fahrzeug", "leasing",
    "lkw", "truck", "transporter", "van", "sprinter",
    "motorrad", "moped", "roller", "scooter", "bike", "quad",
    "anhänger", "anhaenger", "trailer", "wohnwagen", "caravan",
    "boot", "yacht", "schiff", "jetski",
    "traktor", "trecker", "tractor", "landmaschine", "agrar",
    "reifen", "felgen",

    # недвижимость
    "immobilien", "wohnung", "haus", "miete", "kaufen", "vermieten",
    "zimmer", "apartment", "appartement", "makler",
]

MAX_AD_AGE_MINUTES = 3 * 60

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# =============== GLOBAL ERROR HANDLER ===============
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Логируем любые необработанные исключения, чтобы PTB не писал 'No error handlers...'"""
    try:
        logger.exception("Необработанная ошибка при обработке апдейта %s: %s", update, context.error)
    except Exception:
        logger.exception("Необработанная ошибка (не удалось залогировать update)")

ADMIN_CHOOSE, ADMIN_ADD, ADMIN_REMOVE, ADMIN_CONFIRM_STOP, SETTINGS_WAIT_VALUE, ADMIN_BROADCAST = range(6)

# =============== PERSISTENCE (JSON 1 FILE) ===============

STATE_DIR = Path("Profile")
STATE_FILE = STATE_DIR / "state.json"
_STATE_LOCK = asyncio.Lock()

def _safe_int_list_to_set(val: Any) -> Set[int]:
    if not isinstance(val, list):
        return set()
    out: Set[int] = set()
    for x in val:
        try:
            out.add(int(x))
        except Exception:
            pass
    return out

def _safe_str_list_to_set(val: Any) -> Set[str]:
    if not isinstance(val, list):
        return set()
    out: Set[str] = set()
    for x in val:
        try:
            out.add(str(x))
        except Exception:
            pass
    return out

def dump_state_from_bot_data(bot_data: dict) -> dict:
    state: Dict[str, Any] = {}

    allowed = bot_data.get("allowed_users", set())
    known = bot_data.get("known_chats", set())
    seen = bot_data.get("global_seen_links", set())

    state["allowed_users"] = sorted(list(allowed)) if isinstance(allowed, set) else []
    state["known_chats"] = sorted(list(known)) if isinstance(known, set) else []
    state["global_seen_links"] = sorted(list(seen)) if isinstance(seen, set) else []

    batch_sizes: Dict[str, int] = {}
    formats: Dict[str, str] = {}

    for k, v in bot_data.items():
        if isinstance(k, str) and k.startswith("batch_size_"):
            try:
                batch_sizes[k] = int(v)
            except Exception:
                pass
        if isinstance(k, str) and k.startswith("format_"):
            try:
                formats[k] = str(v)
            except Exception:
                pass

    state["batch_sizes"] = batch_sizes
    state["formats"] = formats
    return state

def load_state_into_bot_data(bot_data: dict, state: dict) -> None:
    try:
        bot_data["allowed_users"] = _safe_int_list_to_set(state.get("allowed_users", []))
        bot_data["known_chats"] = _safe_int_list_to_set(state.get("known_chats", []))
        bot_data["global_seen_links"] = _safe_str_list_to_set(state.get("global_seen_links", []))

        batch_sizes = state.get("batch_sizes", {})
        if isinstance(batch_sizes, dict):
            for k, v in batch_sizes.items():
                if isinstance(k, str) and k.startswith("batch_size_"):
                    try:
                        bot_data[k] = int(v)
                    except Exception:
                        pass

        formats = state.get("formats", {})
        if isinstance(formats, dict):
            for k, v in formats.items():
                if isinstance(k, str) and k.startswith("format_"):
                    bot_data[k] = str(v).lower()
    except Exception as e:
        logger.warning("Не удалось применить state в bot_data: %s", e)

async def save_state(bot_data: dict) -> None:
    async with _STATE_LOCK:
        try:
            STATE_DIR.mkdir(parents=True, exist_ok=True)
            state = dump_state_from_bot_data(bot_data)
            tmp = STATE_FILE.with_suffix(".tmp")
            tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(STATE_FILE)
            logger.info("State сохранён: %s", STATE_FILE)
        except Exception as e:
            logger.error("Ошибка сохранения state: %s", e)

async def load_state(bot_data: dict) -> None:
    async with _STATE_LOCK:
        try:
            if not STATE_FILE.exists():
                logger.info("State файл не найден (%s). Стартуем с пустыми данными.", STATE_FILE)
                return
            raw = STATE_FILE.read_text(encoding="utf-8")
            state = json.loads(raw)
            if isinstance(state, dict):
                load_state_into_bot_data(bot_data, state)
                logger.info("State загружен из %s", STATE_FILE)
        except Exception as e:
            logger.error("Ошибка загрузки state: %s", e)

async def periodic_state_save(context: ContextTypes.DEFAULT_TYPE) -> None:
    await save_state(context.application.bot_data)

# =============== HTTP & УТИЛИТЫ ===============

def http_get(url: str) -> Optional[str]:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        logger.error("Ошибка HTTP %s: %s", url, e)
        return None

def soup_from_html(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html5lib")

def normalize_link(href: str) -> Optional[str]:
    if not href:
        return None
    href = href.strip()

    if href.startswith("//"):
        href = "https:" + href
    elif href.startswith("/"):
        href = BASE_URL + href
    elif not href.startswith("http"):
        return None

    if not href.startswith(BASE_URL):
        return None

    return href

def normalize_price_to_eur(raw: str) -> str:
    if not raw:
        return raw

    m = re.search(r"(\d[\d\.\,]*)", raw)
    if not m:
        return raw.strip()

    num = m.group(1)
    num = num.replace(".", "").replace(",", ".")
    try:
        val = float(num)
        return f"{val:.1f} EUR"
    except ValueError:
        return raw.strip()

def is_admin(user_id: int) -> bool:
    return user_id == OWNER_ID


async def ricardo_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """One-shot Ricardo parser: private sellers, fixed price, no bids, seller name must be 'Name Surname'."""
    chat_id = update.effective_chat.id
    bot_data = context.application.bot_data

    if not context.args:
        await update.message.reply_text("Формат: /ricardo Имя Фамилия [json|txt]")
        return

    # optional last arg format
    fmt = "json"
    args = context.args[:]
    if args and args[-1].lower() in ("json", "txt"):
        fmt = args[-1].lower()
        args = args[:-1]

    query = " ".join(args).strip()
    if not query:
        await update.message.reply_text("Формат: /ricardo Имя Фамилия [json|txt]")
        return

    batch_size = get_batch_size_for_chat(bot_data, chat_id)

    msg = await update.message.reply_text(f"🔎 Ricardo: ищу '{query}' (до {batch_size} объявлений)...")

    loop = asyncio.get_running_loop()
    # ricardo_collect_items is sync; run in executor
    items = await loop.run_in_executor(None, ricardo_collect_items, query, 3, 80, 0.2)

    # limit to batch_size
    items = (items or [])[:batch_size]

    if not items:
        await msg.edit_text("Ничего не нашёл по ТЗ (частник + без ставок + Имя Фамилия).")
        return

    filepath = await loop.run_in_executor(
        None, save_results_to_file, items, chat_id, 1, "ricardo", query, Path("results"), fmt
    )

    with filepath.open("rb") as f:
        await context.bot.send_document(
            chat_id=chat_id,
            document=InputFile(f, filename=filepath.name),
            caption="Ricardo парсинг готов",
        )

    try:
        await msg.edit_text("✅ Готово, отправил файл.")
    except Exception:
        pass

def get_batch_size_for_chat(bot_data: dict, chat_id: int) -> int:
    return bot_data.get(f"batch_size_{chat_id}", DEFAULT_BATCH_SIZE)

def get_output_format_for_chat(bot_data: dict, chat_id: int) -> str:
    fmt = bot_data.get(f"format_{chat_id}", "json")
    fmt = str(fmt).lower()
    if fmt not in ("json", "txt"):
        fmt = "json"
    return fmt

def get_main_keyboard(user_id: int) -> List[List[str]]:
    rows = [["Старт", "Стоп"], ["Настройки"]]
    if is_admin(user_id):
        rows[1].append("Админ")
    return rows

def is_banned_by_title(title: str) -> bool:
    t = (title or "").strip().lower()
    if not t:
        return False
    return any(kw in t for kw in BANNED_TITLE_KEYWORDS)

def is_shop_listing(soup: BeautifulSoup) -> bool:
    marker = soup.find(string=re.compile(r"Gewerblicher Anbieter", re.IGNORECASE))
    return marker is not None

# =============== ЖЁСТКИЙ БАН АВТО/ИММО ===============

def is_excluded_url(url: str) -> bool:
    if not url:
        return False
    u = url.lower()
    if any(pat in u for pat in EXCLUDED_CATEGORY_PATTERNS):
        return True
    return any(rx.search(u) for rx in EXCLUDED_CATEGORY_REGEX)

def extract_ad_category_signals(soup: BeautifulSoup) -> List[str]:
    """
    ВАЖНО: берём только breadcrumb/хлебные крошки и явные ссылки на категорию.
    НЕ трогаем nav меню сайта, иначе будет ложный бан почти на всех страницах.
    """
    signals: List[str] = []

    breadcrumb_root = (
        soup.select_one("nav[aria-label*='Brot']")          # Brotkrumen
        or soup.select_one("nav[aria-label*='crumb']")     # breadcrumb
        or soup.select_one("[data-testid*='breadcrumb']")
        or soup.select_one("ol[class*='bread']")
        or soup.select_one("ul[class*='bread']")
        or soup.select_one("div[class*='bread']")
    )

    if breadcrumb_root:
        for a in breadcrumb_root.select("a[href]"):
            href = a.get("href", "").strip()
            txt = a.get_text(" ", strip=True)

            if href:
                full = normalize_link(href) or href
                if full:
                    signals.append(full)
            if txt:
                signals.append(txt)

    # fallback: только явные ссылки на категорию, ограниченно
    if not signals:
        picked = 0
        for a in soup.select("a[href*='/s-kategorie/'], a[href*='/s-kategorien.html'], a[href^='/s-'][href*='/c']"):
            href = a.get("href", "").strip()
            if not href:
                continue
            full = normalize_link(href) or href
            if full:
                signals.append(full)
                picked += 1
            if picked >= 10:
                break

    return signals

def is_banned_by_category(soup: BeautifulSoup) -> bool:
    signals = extract_ad_category_signals(soup)

    # по URL сигналам
    for s in signals:
        if s.startswith("http") or s.startswith("/"):
            full = s if s.startswith("http") else (normalize_link(s) or s)
            if full and is_excluded_url(full):
                return True

    # по тексту (ТОЛЬКО breadcrumb)
    blob = " ".join(signals).lower()
    return any(k.lower() in blob for k in EXCLUDED_CATEGORY_TEXT_KEYWORDS)

# =============== КАТЕГОРИИ + ПАГИНАЦИЯ ===============

def extract_category_links_from_main(_html: str, limit: int = MAX_CATEGORIES) -> List[str]:
    cats: List[str] = []

    cat_html = http_get(CATEGORIES_URL)
    if not cat_html:
        return cats

    soup = soup_from_html(cat_html)

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()

        if not (href.startswith("/s-") and "/c" in href):
            continue

        url = normalize_link(href)
        if not url:
            continue

        if is_excluded_url(url):
            continue

        if url not in cats:
            cats.append(url)

        if len(cats) >= limit:
            break

    return cats

def find_next_page_url(soup: BeautifulSoup) -> Optional[str]:
    cand = (
        soup.select_one("a[rel='next']")
        or soup.select_one("a[aria-label*='Weiter']")
        or soup.find("a", string=re.compile(r"Weiter|Nächste", re.IGNORECASE))
    )
    if not cand:
        return None

    href = cand.get("href", "")
    return normalize_link(href)

def extract_ad_links_from_category(
    start_url: str,
    max_pages: int = CATEGORY_MAX_PAGES,
    limit_per_category: int = 200,
    return_next: bool = False,
) -> Any:
    links: List[str] = []
    page_url = start_url
    pages = 0
    next_url: Optional[str] = None

    while page_url and pages < max_pages and len(links) < limit_per_category:
        html = http_get(page_url)
        if not html:
            break

        soup = soup_from_html(html)

        for a in soup.find_all("a", href=True):
            href = normalize_link(a["href"])
            if not href:
                continue

            if "/s-anzeige/" in href:
                # здесь URL не всегда содержит категорию, но фильтр лишним не будет
                if is_excluded_url(href):
                    continue
                if href not in links:
                    links.append(href)

            if len(links) >= limit_per_category:
                break

        pages += 1
        if pages >= max_pages:
            break

        next_url = find_next_page_url(soup)
        page_url = next_url

    return (links, next_url) if return_next else links

# =============== ПРОФИЛЬ ПРОДАВЦА ===============

def parse_seller_profile(url: str) -> Dict:
    html = http_get(url)
    if not html:
        return {}

    soup = soup_from_html(html)
    result: Dict = {
        "person_name": "",
        "person_reg_date": "",
        "rating": None,
        "ads_number": None,
    }

    name_tag = (
        soup.select_one("h2")
        or soup.select_one("h1")
        or soup.select_one("div[class*='seller-name']")
        or soup.select_one("header h2")
    )
    if name_tag:
        result["person_name"] = name_tag.get_text(strip=True)

    reg_tag = soup.find(string=re.compile(r"Aktiv seit", re.IGNORECASE))
    if reg_tag:
        result["person_reg_date"] = reg_tag.strip()

    badge_container = (
        soup.select_one(".badge-list")
        or soup.select_one("div[class*='badge']")
    )
    if badge_container:
        badges = [
            span for span in badge_container.find_all("span")
            if span.get_text(strip=True)
        ]
        if badges:
            result["rating"] = len(badges)

    ads_text = soup.find(string=re.compile(r"Anzeigen", re.IGNORECASE))
    if ads_text:
        nums = re.findall(r"\d+", ads_text)
        if nums:
            try:
                result["ads_number"] = int(nums[0])
            except ValueError:
                pass

    return result

# =============== ВРЕМЯ ПУБЛИКАЦИИ ===============

def extract_ad_age_minutes(soup: BeautifulSoup) -> Tuple[Optional[int], str]:
    age_text = ""
    age_minutes: Optional[int] = None

    candidates = soup.find_all(
        string=re.compile(
            r"vor\s+\d+\s+(Minute|Minuten|Std\.?|Stunde|Stunden|Tag|Tage|Tagen)",
            re.IGNORECASE,
        )
    )

    for t in candidates:
        s = t.strip()
        if not s:
            continue
        age_text = s
        m = re.search(
            r"vor\s+(\d+)\s+(Minute|Minuten|Std\.?|Stunde|Stunden|Tag|Tage|Tagen)",
            s,
            re.IGNORECASE,
        )
        if not m:
            continue

        n = int(m.group(1))
        unit = m.group(2).lower()

        if unit.startswith("minute"):
            age_minutes = n
        elif unit.startswith("stunde") or unit.startswith("std"):
            age_minutes = n * 60
        elif unit.startswith("tag"):
            age_minutes = n * 24 * 60

        break

    return age_minutes, age_text

# =============== ИМЯ ПРОДАВЦА ===============

def extract_seller_name_from_ad_page(soup: BeautifulSoup) -> str:
    marker = soup.find(string=re.compile(r"Privater Nutzer|Gewerblicher Anbieter", re.IGNORECASE))
    if marker:
        for parent in marker.parents:
            name_tag = parent.select_one("[data-testid='seller-name']")
            if name_tag and name_tag.get_text(strip=True):
                return name_tag.get_text(strip=True)

            candidates = []
            for tag in parent.find_all(["span", "div", "p"], recursive=True):
                txt = tag.get_text(strip=True)
                if not txt:
                    continue
                if len(txt) <= 2:
                    continue
                if re.search(
                    r"Privater Nutzer|Gewerblicher Anbieter|Aktiv seit|TOP Zufriedenheit|freundlich|zuverlässig",
                    txt,
                    re.IGNORECASE,
                ):
                    continue
                candidates.append(txt)

            if candidates:
                return candidates[0]

    block = (
        soup.select_one("section[data-testid='seller-profile']")
        or soup.select_one("div[data-testid='seller-profile']")
        or soup.select_one("div[class*='seller']")
    )
    if block:
        h = block.find("h2") or block.find("h3")
        if h and h.get_text(strip=True):
            return h.get_text(strip=True)

        for tag in block.find_all(["span", "div"], recursive=True):
            txt = tag.get_text(strip=True)
            if txt and len(txt) > 2:
                return txt

    return ""

# =============== ПАРСИНГ ОБЪЯВЛЕНИЯ ===============

def parse_ad_page(url: str) -> Optional[Dict]:
    html = http_get(url)
    if not html:
        return None

    soup = soup_from_html(html)

    # ✅ ЖЕСТКО режем по категории (но аккуратно, без меню)
    if is_banned_by_category(soup):
        return None

    age_minutes, age_text = extract_ad_age_minutes(soup)
    if age_minutes is not None and age_minutes > MAX_AD_AGE_MINUTES:
        return None

    if is_shop_listing(soup):
        return None

    title_tag = soup.select_one("h1")
    item_title = title_tag.get_text(strip=True) if title_tag else ""

    if not item_title:
        og_title = soup.find("meta", attrs={"property": "og:title"})
        if og_title:
            item_title = og_title.get("content", "").strip()

    if is_banned_by_title(item_title):
        return None

    raw_price = ""
    price_tag = (
        soup.select_one("[data-testid='ad-price']")
        or soup.select_one("span[class*='price']")
        or soup.select_one("div[class*='price']")
    )
    if price_tag:
        raw_price = price_tag.get_text(strip=True)
    else:
        found = soup.find(string=re.compile(r"\d[\d\.,]*\s*€|VB|zu verschenken", re.IGNORECASE))
        if found:
            raw_price = found.strip()

    item_price = normalize_price_to_eur(raw_price) if raw_price else ""

    seller_link_tag = (
        soup.select_one("a[href*='/s-seller/']")
        or soup.select_one("a[href*='/s-profil/']")
        or soup.select_one("a[href*='/s-anbieter/']")
    )

    person_link = ""
    if seller_link_tag:
        href = normalize_link(seller_link_tag.get("href"))
        if href:
            person_link = href

    photo_tag = (
        soup.select_one("img[src*='api/v1/prod-ads/images']")
        or soup.select_one("img[src*='img.kleinanzeigen']")
        or soup.select_one("img[class*='gallery']")
    )
    item_photo = photo_tag.get("src", "") if photo_tag else ""

    if "logo-kleinanzeigen-horizontal" in item_photo:
        item_photo = ""

    if not item_photo:
        og_image = soup.find("meta", attrs={"property": "og:image"})
        if og_image:
            item_photo = og_image.get("content", "").strip()

    seller_name = extract_seller_name_from_ad_page(soup)

    seller_info: Dict = {}
    if not seller_name and person_link:
        seller_info = parse_seller_profile(person_link)
        seller_name = seller_info.get("person_name", "")
    else:
        seller_info = {}

    item: Dict = {
        "item_title": item_title,
        "item_photo": item_photo,
        "ads_number": seller_info.get("ads_number"),
        "parser_views": 0,
        "ads_number_bought": None,
        "ads_number_sold": None,
        "gender": "",
        "email": "",
        "person_reg_date": seller_info.get("person_reg_date", ""),
        "item_price": item_price,
        "views": None,
        "rating": seller_info.get("rating"),
        "created_date": "",
        "created_real_date": age_text,
        "phone": "",
        "item_desc": "",
        "location": "",
        "item_link": url,
        "person_link": person_link,
        "item_person_name": seller_name,
    }

    return item

# =============== TXT ===============

def items_to_txt(items: List[Dict]) -> str:
    lines: List[str] = []
    sep_line = "=" * 51

    for it in items:
        title = (it.get("item_title") or "").strip() or "Без названия"
        link = (it.get("item_link") or "").strip() or "-"
        seller = (it.get("item_person_name") or "").strip() or "Privater Nutzer"

        lines.append(f"📱{title}")
        lines.append(f"🔗 Ссылка на товар ({link})")
        lines.append(f"💼 Продавец: {seller}")
        lines.append(sep_line)

    return "\n".join(lines)

# =============== SAVE FILE ===============


def sanitize_filename_part(s: str, max_len: int = 40) -> str:
    s = re.sub(r"\s+", "_", (s or "").strip())
    s = re.sub(r"[^A-Za-z0-9_\-]+", "", s)
    return s[:max_len] or "query"

def save_results_to_file(
    items: List[Dict],
    chat_id: int,
    batch_index: int,
    prefix: str,
    query: str = "",
    directory: Path = Path("results"),
    fmt: str = "json",
) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    fmt = (fmt or "json").lower()

    qpart = sanitize_filename_part(query)
    if fmt == "txt":
        filename = f"{prefix}_{chat_id}_{batch_index}_{qpart}_{timestamp}.txt"
        filepath = directory / filename
        filepath.write_text(items_to_txt(items), encoding="utf-8")
    else:
        filename = f"{prefix}_{chat_id}_{batch_index}_{qpart}_{timestamp}.json"
        filepath = directory / filename
        data = {"items": items}
        filepath.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info("Сохранили %s объявлений в %s", len(items), filepath)
    return filepath

def save_batch_to_file(
    items: List[Dict],
    chat_id: int,
    batch_index: int,
    directory: Path = Path("results"),
    fmt: str = "json",
) -> Path:
    # backward-compatible wrapper
    return save_results_to_file(items, chat_id, batch_index, prefix="kleinanzeigen", query="", directory=directory, fmt=fmt)

# =============== WATCHER ===============

async def kleinanzeigen_watcher(chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.info("Запущен watcher для чата %s", chat_id)

    bot_data = context.application.bot_data
    running_key = f"running_{chat_id}"

    seen_links: Set[str] = set()
    batch: List[Dict] = []
    batch_index = 1

    global_seen: Set[str] = bot_data.setdefault(f"seen_links_{chat_id}", set())  # type: ignore[assignment]
    # Миграция со старого ключа (если был) чтобы не слать старьё при обновлении
    if not global_seen and bot_data.get("global_seen_links"):
        try:
            global_seen.update(set(bot_data.get("global_seen_links", set())))
        except Exception:
            pass
    seen_sellers: Set[str] = set()

    batch_size = get_batch_size_for_chat(bot_data, chat_id)
    loop = asyncio.get_running_loop()

    progress_message = await context.bot.send_message(
        chat_id=chat_id,
        text=f"Собираю объявления: 0/{batch_size}",
    )
    last_progress_update = 0.0

    async def update_progress():
        nonlocal last_progress_update
        now = time.time()
        if now - last_progress_update >= 10 or len(batch) == batch_size:
            last_progress_update = now
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_message.message_id,
                    text=f"Собираю объявления: {len(batch)}/{batch_size}",
                )
            except Exception as e:
                logger.warning("Не удалось обновить сообщение прогресса: %s", e)

    CHUNK_SIZE = 20

    try:
        while bot_data.get(running_key, False):
            categories = extract_category_links_from_main("", limit=MAX_CATEGORIES)
            logger.info("Найдено категорий (после фильтра): %s", len(categories))

            for cat_url in categories:
                if not bot_data.get(running_key, False):
                    break
                # Быстрый проход по первым страницам (самые свежие объявления)
                fast_links, fast_next = await loop.run_in_executor(
                    None, extract_ad_links_from_category, cat_url, CATEGORY_FAST_PAGES, 200, True
                )

                # Дополнительно — 1 "глубокая" страница, чтобы со временем покрывать объявления до 3 часов назад,
                # даже при очень большом трафике (когда 3 часа могут быть далеко в пагинации).
                cursor_key = f"category_cursor_{chat_id}"
                cursors: Dict[str, str] = bot_data.setdefault(cursor_key, {})  # type: ignore[assignment]

                deep_links: List[str] = []
                cursor_url = cursors.get(cat_url) or fast_next
                if cursor_url:
                    deep_links, deep_next = await loop.run_in_executor(
                        None, extract_ad_links_from_category, cursor_url, CATEGORY_DEEP_PAGES_PER_CYCLE, 200, True
                    )
                    if deep_next and deep_next != cursor_url:
                        cursors[cat_url] = deep_next
                    elif fast_next:
                        # если дальше некуда — держим курсор на следующей после fast-страниц
                        cursors[cat_url] = fast_next
                    else:
                        cursors.pop(cat_url, None)
                else:
                    if fast_next:
                        cursors[cat_url] = fast_next

                # Склеиваем без дублей, сохраняя порядок
                cat_links = list(dict.fromkeys(fast_links + deep_links))

                new_links = [l for l in cat_links if l not in seen_links and l not in global_seen]

                for i in range(0, len(new_links), CHUNK_SIZE):
                    if not bot_data.get(running_key, False):
                        break

                    chunk = new_links[i:i + CHUNK_SIZE]
                    for link in chunk:
                        seen_links.add(link)

                    tasks = [loop.run_in_executor(None, parse_ad_page, link) for link in chunk]
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    for link, ad_data in zip(chunk, results):
                        if not bot_data.get(running_key, False):
                            break

                        if isinstance(ad_data, Exception):
                            logger.warning("Ошибка при парсинге %s: %s", link, ad_data)
                            continue

                        if ad_data:
                            seller_id = ad_data.get("person_link") or ad_data.get("item_person_name") or ""
                            if seller_id:
                                if seller_id in seen_sellers:
                                    continue
                                seen_sellers.add(seller_id)

                            batch.append(ad_data)
                            await update_progress()

                        if len(batch) >= batch_size:
                            for item in batch:
                                global_seen.add(item["item_link"])

                            output_format = get_output_format_for_chat(bot_data, chat_id)
                            filepath = await loop.run_in_executor(
                                None, save_batch_to_file, batch, chat_id, batch_index, Path("results"), output_format
                            )

                            with filepath.open("rb") as f:
                                await context.bot.send_document(
                                    chat_id=chat_id,
                                    document=InputFile(f, filename=filepath.name),
                                    caption=f"Готов {batch_index} парсинг",
                                )

                            await save_state(bot_data)

                            batch_index += 1
                            batch = []
                            seen_sellers.clear()

                            try:
                                await context.bot.edit_message_text(
                                    chat_id=chat_id,
                                    message_id=progress_message.message_id,
                                    text=f"Собираю объявления: 0/{batch_size}",
                                )
                                last_progress_update = time.time()
                            except Exception as e:
                                logger.warning("Не удалось сбросить прогресс-сообщение: %s", e)

                    await asyncio.sleep(PER_AD_DELAY)

            await asyncio.sleep(SCAN_INTERVAL)

    except asyncio.CancelledError:
        logger.info("Watcher для чата %s отменён", chat_id)
    finally:
        if batch:
            for item in batch:
                global_seen.add(item["item_link"])

            output_format = get_output_format_for_chat(bot_data, chat_id)
            filepath = await loop.run_in_executor(
                None, save_batch_to_file, batch, chat_id, batch_index, Path("results"), output_format
            )
            with filepath.open("rb") as f:
                await context.bot.send_document(
                    chat_id=chat_id,
                    document=InputFile(f, filename=filepath.name),
                    caption=f"Готов {batch_index} парсинг",
                )

            await save_state(bot_data)

            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=progress_message.message_id,
                    text=f"Сбор остановлен, итоговый прогресс: {len(batch)}/{batch_size}",
                )
            except Exception as e:
                logger.warning("Не удалось обновить финальный прогресс: %s", e)

        logger.info("Watcher для чата %s завершён", chat_id)

# =============== АДМИНКА ===============

async def admin_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("У тебя нет прав администратора.")
        return ConversationHandler.END

    bot_data = context.application.bot_data
    allowed = bot_data.setdefault("allowed_users", set())  # type: ignore[assignment]
    allowed.add(OWNER_ID)
    await save_state(bot_data)

    keyboard = [
        ["Добавить доступ", "Отобрать доступ"],
        ["Завершить процессы", "Рестарт"],
        ["Список допущенных", "Рассылка"],
        ["Выход"],
    ]
    await update.message.reply_text(
        "Админ-панель. Выбери действие:",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
    )
    return ADMIN_CHOOSE

async def admin_choose(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip()
    bot_data = context.application.bot_data

    if text == "Добавить доступ":
        await update.message.reply_text("Введи ID пользователя:", reply_markup=ReplyKeyboardRemove())
        return ADMIN_ADD

    if text == "Отобрать доступ":
        await update.message.reply_text("Введи ID пользователя:", reply_markup=ReplyKeyboardRemove())
        return ADMIN_REMOVE

    if text == "Завершить процессы":
        await update.message.reply_text("Завершить ВСЕ процессы? (да/нет)", reply_markup=ReplyKeyboardRemove())
        return ADMIN_CONFIRM_STOP

    if text == "Рестарт":
        global_seen: Set[str] = bot_data.get("global_seen_links", set())  # type: ignore[assignment]
        if isinstance(global_seen, set):
            global_seen.clear()

        restarted = 0
        for key, value in list(bot_data.items()):
            if not str(key).startswith("task_"):
                continue

            chat_id_str = str(key).split("_", 1)[1]
            try:
                cid = int(chat_id_str)
            except ValueError:
                continue

            running_key = f"running_{cid}"
            if not bot_data.get(running_key, False):
                continue

            task = value
            if isinstance(task, asyncio.Task) and not task.done():
                task.cancel()

            new_task = asyncio.create_task(kleinanzeigen_watcher(cid, context))
            bot_data[key] = new_task
            restarted += 1

        await save_state(bot_data)
        await update.message.reply_text(f"Рестарт выполнен. Перезапущено процессов: {restarted}")

    if text == "Список допущенных":
        allowed: Set[int] = bot_data.get("allowed_users", set())  # type: ignore[assignment]
        ids = ", ".join(str(i) for i in sorted(allowed)) if allowed else "пусто"
        await update.message.reply_text(f"Список пользователей с доступом:\n{ids}")

    if text == "Рассылка":
        await update.message.reply_text("Введи текст рассылки:", reply_markup=ReplyKeyboardRemove())
        return ADMIN_BROADCAST

    if text == "Выход":
        await update.message.reply_text("Выход из админ-панели.", reply_markup=ReplyKeyboardRemove())
        return ConversationHandler.END

    keyboard = [
        ["Добавить доступ", "Отобрать доступ"],
        ["Завершить процессы", "Рестарт"],
        ["Список допущенных", "Рассылка"],
        ["Выход"],
    ]
    await update.message.reply_text(
        "Админ-панель. Выбери действие:",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
    )
    return ADMIN_CHOOSE

async def admin_add(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    bot_data = context.application.bot_data
    allowed: Set[int] = bot_data.setdefault("allowed_users", set())  # type: ignore[assignment]
    try:
        user_id = int(update.message.text.strip())
        allowed.add(user_id)
        await save_state(bot_data)
        await update.message.reply_text(f"Пользователь {user_id} добавлен.")
    except ValueError:
        await update.message.reply_text("Нужен числовой ID.")
    return await admin_start(update, context)

async def admin_remove(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    bot_data = context.application.bot_data
    allowed: Set[int] = bot_data.setdefault("allowed_users", set())  # type: ignore[assignment]
    try:
        user_id = int(update.message.text.strip())
        if user_id in allowed:
            allowed.remove(user_id)
            await save_state(bot_data)
            await update.message.reply_text(f"Пользователь {user_id} удалён.")
        else:
            await update.message.reply_text("Этого ID нет в списке.")
    except ValueError:
        await update.message.reply_text("Нужен числовой ID.")
    return await admin_start(update, context)

async def admin_confirm_stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    bot_data = context.application.bot_data
    t = update.message.text.strip().lower()
    if t in ("да", "yes", "y", "ага", "да!"):
        stopped = 0
        for key, value in list(bot_data.items()):
            if str(key).startswith("running_"):
                bot_data[key] = False
            if str(key).startswith("task_"):
                task = value
                if isinstance(task, asyncio.Task) and not task.done():
                    task.cancel()
                    stopped += 1
        await update.message.reply_text(f"Остановлено задач: {stopped}")
    else:
        await update.message.reply_text("Ок, отменено.")
    return await admin_start(update, context)

async def admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    if not is_admin(user.id):
        await update.message.reply_text("Нет прав.")
        return ConversationHandler.END

    text = update.message.text.strip()
    bot_data = context.application.bot_data
    known_chats: Set[int] = bot_data.get("known_chats", set())  # type: ignore[assignment]
    sent = 0

    for cid in list(known_chats):
        try:
            await context.bot.send_message(chat_id=cid, text=text)
            sent += 1
        except Exception as e:
            logger.warning("Не удалось отправить в %s: %s", cid, e)

    await update.message.reply_text(f"Рассылка отправлена в {sent} чатов.")
    return await admin_start(update, context)

async def admin_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Админ-панель закрыта.", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END

# =============== НАСТРОЙКИ ===============

async def settings_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    chat_id = update.effective_chat.id
    bot_data = context.application.bot_data

    current_size = get_batch_size_for_chat(bot_data, chat_id)
    current_fmt = get_output_format_for_chat(bot_data, chat_id)
    fmt_label = "JSON" if current_fmt == "json" else "TXT"

    context.chat_data["settings_mode"] = None

    keyboard = ReplyKeyboardMarkup([["Изменить количество", "Выбрать формат"], ["Назад"]], resize_keyboard=True)
    await update.message.reply_text(
        f"Настройки:\n• размер выдачи: {current_size}\n• формат: {fmt_label}\n\nВыбери:",
        reply_markup=keyboard,
    )
    return SETTINGS_WAIT_VALUE

async def settings_set(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    chat_id = update.effective_chat.id
    user = update.effective_user
    bot_data = context.application.bot_data

    text = update.message.text.strip()
    mode = context.chat_data.get("settings_mode")

    if text.lower() in ("назад", "отмена"):
        keyboard = ReplyKeyboardMarkup(get_main_keyboard(user.id), resize_keyboard=True)
        await update.message.reply_text("Выход из настроек.", reply_markup=keyboard)
        context.chat_data["settings_mode"] = None
        return ConversationHandler.END

    if mode is None:
        if text == "Изменить количество":
            context.chat_data["settings_mode"] = "batch"
            await update.message.reply_text("Введи число 5–1000:", reply_markup=ReplyKeyboardRemove())
            return SETTINGS_WAIT_VALUE

        if text == "Выбрать формат":
            context.chat_data["settings_mode"] = "format"
            keyboard = ReplyKeyboardMarkup([["JSON", "TXT"], ["Назад"]], resize_keyboard=True)
            await update.message.reply_text("Выбери формат:", reply_markup=keyboard)
            return SETTINGS_WAIT_VALUE

        await update.message.reply_text("Не понял. Выбери пункт меню.")
        return SETTINGS_WAIT_VALUE

    if mode == "batch":
        try:
            val = int(text)
            if val < 5 or val > 1000:
                await update.message.reply_text("Диапазон 5–1000. Введи ещё раз или 'Назад'.")
                return SETTINGS_WAIT_VALUE
            bot_data[f"batch_size_{chat_id}"] = val
            await save_state(bot_data)
            keyboard = ReplyKeyboardMarkup(get_main_keyboard(user.id), resize_keyboard=True)
            await update.message.reply_text(f"Ок, теперь по {val} объявлений в файл.", reply_markup=keyboard)
            context.chat_data["settings_mode"] = None
            return ConversationHandler.END
        except ValueError:
            await update.message.reply_text("Нужное число. Введи ещё раз или 'Назад'.")
            return SETTINGS_WAIT_VALUE

    if mode == "format":
        t = text.strip().upper()
        if t not in ("JSON", "TXT"):
            await update.message.reply_text("Жми JSON или TXT, либо 'Назад'.")
            return SETTINGS_WAIT_VALUE
        bot_data[f"format_{chat_id}"] = t.lower()
        await save_state(bot_data)
        keyboard = ReplyKeyboardMarkup(get_main_keyboard(user.id), resize_keyboard=True)
        await update.message.reply_text(f"Формат установлен: {t}.", reply_markup=keyboard)
        context.chat_data["settings_mode"] = None
        return ConversationHandler.END

    keyboard = ReplyKeyboardMarkup(get_main_keyboard(user.id), resize_keyboard=True)
    await update.message.reply_text("Сбой в настройках, вернул в меню.", reply_markup=keyboard)
    context.chat_data["settings_mode"] = None
    return ConversationHandler.END

async def settings_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    keyboard = ReplyKeyboardMarkup(get_main_keyboard(user.id), resize_keyboard=True)
    await update.message.reply_text("Настройки отменены.", reply_markup=keyboard)
    context.chat_data["settings_mode"] = None
    return ConversationHandler.END

# =============== КОМАНДЫ ===============

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    user = update.effective_user
    bot_data = context.application.bot_data

    allowed: Set[int] = bot_data.setdefault("allowed_users", set())  # type: ignore[assignment]
    allowed.add(OWNER_ID)

    known_chats: Set[int] = bot_data.setdefault("known_chats", set())  # type: ignore[assignment]
    known_chats.add(chat_id)

    await save_state(bot_data)

    if not (is_admin(user.id) or user.id in allowed):
        await update.message.reply_text("У тебя нет доступа. Обратись к администратору.")
        return

    running_key = f"running_{chat_id}"
    task_key = f"task_{chat_id}"

    keyboard = ReplyKeyboardMarkup(get_main_keyboard(user.id), resize_keyboard=True)

    if bot_data.get(running_key):
        await update.message.reply_text("Уже работаю ✅", reply_markup=keyboard)
        return

    bot_data[running_key] = True

    await update.message.reply_text("Начал работу ✅", reply_markup=keyboard)

    task = asyncio.create_task(kleinanzeigen_watcher(chat_id, context))
    bot_data[task_key] = task

async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    user = update.effective_user
    bot_data = context.application.bot_data

    running_key = f"running_{chat_id}"
    task_key = f"task_{chat_id}"

    bot_data[running_key] = False

    task: Optional[asyncio.Task] = bot_data.get(task_key)  # type: ignore[assignment]
    if task and not task.done():
        task.cancel()

    keyboard = ReplyKeyboardMarkup(get_main_keyboard(user.id), resize_keyboard=True)
    await update.message.reply_text("Мониторинг остановлен ⏹", reply_markup=keyboard)

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    keyboard = ReplyKeyboardMarkup(get_main_keyboard(user.id), resize_keyboard=True)
    await update.message.reply_text(
        "/start — запустить\n"
        "/stop — остановить\n"
        "/admin — админка\n\n"
        "Кнопки: Старт/Стоп/Настройки/Админ\n"
        "Состояние сохраняется в Profile/state.json",
        reply_markup=keyboard,
    )

# =============== LIFECYCLE ===============

async def on_startup(app) -> None:
    await load_state(app.bot_data)

    # JobQueue может быть None, если PTB установлен без [job-queue]
    try:
        if app.job_queue is not None:
            app.job_queue.run_repeating(periodic_state_save, interval=60, first=60)
        else:
            logger.warning("JobQueue отсутствует (PTB без [job-queue]). Periodic save отключен.")
    except Exception as e:
        logger.warning("Не удалось запустить periodic save: %s", e)

async def on_shutdown(app) -> None:
    await save_state(app.bot_data)

def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("Не найден BOT_TOKEN. Создай .env (см. .env.example) или выставь переменную окружения BOT_TOKEN")

    request = HTTPXRequest(connect_timeout=15, read_timeout=45, write_timeout=45, pool_timeout=45)
    get_updates_request = HTTPXRequest(connect_timeout=15, read_timeout=60, write_timeout=60, pool_timeout=60)

    application = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .request(request)
        .get_updates_request(get_updates_request)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .build()
    )

    application.add_error_handler(error_handler)

    application.add_handler(CommandHandler("start", start_cmd))
    application.add_handler(CommandHandler("stop", stop_cmd))
    application.add_handler(CommandHandler("help", help_cmd))
    application.add_handler(CommandHandler("ricardo", ricardo_cmd))

    application.add_handler(MessageHandler(filters.Regex("^Старт$"), start_cmd))
    application.add_handler(MessageHandler(filters.Regex("^Стоп$"), stop_cmd))

    admin_conv = ConversationHandler(
        entry_points=[
            CommandHandler("admin", admin_start),
            MessageHandler(filters.Regex("^Админ$"), admin_start),
        ],
        states={
            ADMIN_CHOOSE: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_choose)],
            ADMIN_ADD: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add)],
            ADMIN_REMOVE: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_remove)],
            ADMIN_CONFIRM_STOP: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_confirm_stop)],
            ADMIN_BROADCAST: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_broadcast)],
        },
        fallbacks=[CommandHandler("cancel", admin_cancel)],
    )
    application.add_handler(admin_conv)

    settings_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^Настройки$"), settings_start)],
        states={SETTINGS_WAIT_VALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND, settings_set)]},
        fallbacks=[CommandHandler("cancel", settings_cancel)],
    )
    application.add_handler(settings_conv)

    application.run_polling()

if __name__ == "__main__":
    main()
