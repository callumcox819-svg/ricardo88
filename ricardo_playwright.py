
import asyncio
import json
import re
from typing import Any, Dict, List, Optional, Tuple
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

from proxy_manager import next_proxy

POPULAR_CATEGORIES: Dict[str, str] = {
    "Одежда и аксессуары": "https://www.ricardo.ch/de/c/kleider-accessoires-403/",
    "Женские аксессуары": "https://www.ricardo.ch/de/c/damenmode-accessoires-402/",
    "Спорт": "https://www.ricardo.ch/de/c/sport-freizeit-410/",
    "Дом и быт": "https://www.ricardo.ch/de/c/wohnen-haushalt-405/",
    "Сад и инструменты": "https://www.ricardo.ch/de/c/garten-heimwerken-406/",
    "Дети и младенцы": "https://www.ricardo.ch/de/c/baby-kind-407/",
    "Смартфоны": "https://www.ricardo.ch/de/c/handys-smartphones-416/",
    "Ноутбуки": "https://www.ricardo.ch/de/c/notebooks-418/",
    "Компьютеры и сети": "https://www.ricardo.ch/de/c/computer-netzwerk-417/",
    "Часы": "https://www.ricardo.ch/de/c/uhren-schmuck-408/",
    "Косметика и уход": "https://www.ricardo.ch/de/c/beauty-gesundheit-412/",
    "Все подряд": "__ALL__",
}

def _is_cf_page(html: str) -> bool:
    h = html.lower()
    return ("cloudflare" in h and "attention required" in h) or "cf-chl-" in h or "checking your browser" in h

def _extract_next_data(html: str) -> Optional[dict]:
    soup = BeautifulSoup(html, "lxml")
    tag = soup.find("script", {"id": "__NEXT_DATA__"})
    if not tag or not tag.string:
        return None
    try:
        return json.loads(tag.string)
    except Exception:
        return None

def _walk(obj: Any):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk(v)
    elif isinstance(obj, list):
        for x in obj:
            yield from _walk(x)

def _guess_item_dicts(next_data: dict) -> List[dict]:
    candidates = []
    for d in _walk(next_data):
        # heuristic: has title and image/url-ish fields
        title = d.get("title") or d.get("name")
        if not isinstance(title, str) or len(title) < 3:
            continue
        if any(k in d for k in ("buy_now_price", "buyNowPrice", "has_buy_now", "hasBuyNow", "bids_count", "bidsCount")) and any(k in d for k in ("image", "img", "url", "href")):
            candidates.append(d)
    return candidates

def _norm_price(v: Any) -> Any:
    return v

def _pick(d: dict, *keys):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None

def _normalize_item(d: dict) -> dict:
    title = _pick(d, "title", "name")
    url = _pick(d, "url", "href", "link")
    if isinstance(url, dict):
        url = _pick(url, "url", "href")
    if isinstance(url, str) and url.startswith("/"):
        url = "https://www.ricardo.ch" + url

    image = _pick(d, "image", "img", "imageUrl", "image_url")
    if isinstance(image, dict):
        image = _pick(image, "url", "src")
    price = _pick(d, "buy_now_price", "buyNowPrice", "price", "buyNow")
    bids = _pick(d, "bids_count", "bidsCount")
    has_buy = _pick(d, "has_buy_now", "hasBuyNow")
    has_auction = _pick(d, "has_auction", "hasAuction")

    return {
        "raw_has_buy_now": has_buy,
        "raw_has_auction": has_auction,
        "raw_bids": bids,
        "item_title": title or "",
        "item_price": _norm_price(price) or "",
        "item_link": url or "",
        "item_photo": image or "",
        "item_person_name": "",  # filled later from detail page
    }

def _is_fixed_price(item: dict) -> bool:
    # strict-ish: buy-now true AND bids == 0
    hb = item.get("raw_has_buy_now")
    if hb is False or hb in ("false", "False", 0):
        return False
    bids = item.get("raw_bids")
    try:
        bids_i = int(bids or 0)
    except Exception:
        bids_i = 0
    return bids_i == 0

async def _fetch_html(url: str, proxy: Optional[str]) -> str:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, proxy={"server": proxy} if proxy else None)
        page = await browser.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        # wait a bit for next data
        try:
            await page.wait_for_selector("script#__NEXT_DATA__", timeout=15000)
        except Exception:
            pass
        html = await page.content()
        await browser.close()
        return html

async def _get_seller_from_detail(url: str, proxy: Optional[str]) -> str:
    try:
        html = await _fetch_html(url, proxy)
        nd = _extract_next_data(html)
        if not nd:
            return ""
        for d in _walk(nd):
            # heuristics for seller fields
            for k in ("sellerName", "seller_name", "username", "userName", "displayName", "nick"):
                v = d.get(k)
                if isinstance(v, str) and 2 <= len(v) <= 80:
                    return v
        # fallback: find visible name in page text (last resort)
        soup = BeautifulSoup(html, "lxml")
        txt = soup.get_text(" ", strip=True)
        m = re.search(r"\bVerkäufer\b\s+([A-Za-zÀ-ÿ0-9 _.-]{2,60})", txt)
        return m.group(1).strip() if m else ""
    except Exception:
        return ""

async def ricardo_collect_items(urls: List[str], max_items: int, fetch_sellers: bool = True) -> List[dict]:
    # Expand "Все подряд" to all popular categories
    if "__ALL__" in urls:
        urls = [u for k, u in POPULAR_CATEGORIES.items() if k != "Все подряд"]

    proxies_tried = 0
    proxies_total = 5  # max rotations per run
    last_err = None

    while proxies_tried < proxies_total:
        proxy = next_proxy()
        proxies_tried += 1
        try:
            all_items: List[dict] = []
            for url in urls:
                html = await _fetch_html(url, proxy)
                if _is_cf_page(html):
                    raise RuntimeError("Cloudflare page detected")
                nd = _extract_next_data(html)
                if not nd:
                    continue
                cands = _guess_item_dicts(nd)
                for c in cands:
                    it = _normalize_item(c)
                    if it["item_link"]:
                        all_items.append(it)

            # Deduplicate by link
            seen = set()
            uniq = []
            for it in all_items:
                lk = it["item_link"]
                if not lk or lk in seen:
                    continue
                seen.add(lk)
                uniq.append(it)

            # Filter fixed price
            filtered = [it for it in uniq if _is_fixed_price(it)]
            filtered = filtered[:max_items]

            if fetch_sellers:
                # Fetch seller names sequentially to keep stable
                for it in filtered:
                    if it["item_link"]:
                        it["item_person_name"] = await _get_seller_from_detail(it["item_link"], proxy)

            return filtered
        except (PWTimeout, Exception) as e:
            last_err = e
            continue

    raise RuntimeError(f"Failed to scrape after proxy rotation: {last_err}")
