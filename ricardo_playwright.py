\
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

def _pick(d: dict, *keys):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None

def _looks_like_item(d: dict) -> bool:
    title = _pick(d, "title", "name")
    url = _pick(d, "url", "href", "link")
    if not isinstance(title, str) or len(title) < 3:
        return False
    if url is None:
        return False
    # typical fields in ricardo
    return any(k in d for k in ("has_buy_now", "hasBuyNow", "bids_count", "bidsCount", "buy_now_price", "buyNowPrice", "buyNowPriceAmount"))

def _normalize_item(d: dict) -> dict:
    title = _pick(d, "title", "name") or ""
    url = _pick(d, "url", "href", "link")
    if isinstance(url, dict):
        url = _pick(url, "url", "href")
    if isinstance(url, str) and url.startswith("/"):
        url = "https://www.ricardo.ch" + url

    image = _pick(d, "image", "img", "imageUrl", "image_url")
    if isinstance(image, dict):
        image = _pick(image, "url", "src")

    price = _pick(d, "buy_now_price", "buyNowPrice", "buyNowPriceAmount", "price")
    bids = _pick(d, "bids_count", "bidsCount")
    has_buy = _pick(d, "has_buy_now", "hasBuyNow")

    return {
        "item_person_name": "",
        "item_photo": image or "",
        "item_link": url or "",
        "item_price": price if price is not None else "",
        "item_title": title,
        "raw_has_buy_now": has_buy,
        "raw_bids": bids,
    }

def _is_fixed_price_no_bids(item: dict) -> bool:
    hb = item.get("raw_has_buy_now")
    # accept True / "true" / 1
    if hb in (False, "false", "False", 0):
        return False
    try:
        bids = int(item.get("raw_bids") or 0)
    except Exception:
        bids = 0
    return bids == 0

async def _fetch_html(url: str, proxy_url: Optional[str]) -> str:
    async with async_playwright() as p:
        launch_kwargs = {"headless": True, "args": ["--no-sandbox", "--disable-dev-shm-usage"]}
        if proxy_url:
            # IMPORTANT: Playwright expects dict with server str
            launch_kwargs["proxy"] = {"server": proxy_url}
        browser = await p.chromium.launch(**launch_kwargs)
        page = await browser.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=90000)
        try:
            await page.wait_for_selector("script#__NEXT_DATA__", timeout=15000)
        except Exception:
            pass
        html = await page.content()
        await browser.close()
        return html

def _extract_items_from_next(next_data: dict) -> List[dict]:
    # Heuristic: collect dicts that look like items, then dedupe by link.
    items = []
    for d in _walk(next_data):
        if isinstance(d, dict) and _looks_like_item(d):
            items.append(d)
    return items

async def _get_seller_from_detail(url: str, proxy_url: Optional[str]) -> str:
    try:
        html = await _fetch_html(url, proxy_url)
        nd = _extract_next_data(html)
        if nd:
            for d in _walk(nd):
                for k in ("sellerName", "seller_name", "username", "userName", "displayName", "nick"):
                    v = d.get(k) if isinstance(d, dict) else None
                    if isinstance(v, str) and 2 <= len(v) <= 80:
                        return v.strip()
        # fallback: simple regex in visible text
        soup = BeautifulSoup(html, "lxml")
        txt = soup.get_text(" ", strip=True)
        m = re.search(r"\bVerkäufer\b\s+([A-Za-zÀ-ÿ0-9 _.-]{2,60})", txt)
        return m.group(1).strip() if m else ""
    except Exception:
        return ""

async def ricardo_collect_items(urls: List[str], max_items: int, fetch_sellers: bool = True) -> List[dict]:
    # Expand ALL
    if "__ALL__" in urls:
        urls = [u for k, u in POPULAR_CATEGORIES.items() if k != "Все подряд"]

    last_err: Optional[Exception] = None
    # rotate proxies up to len(proxies) times (but cap to 8)
    for _ in range(8):
        proxy_url = next_proxy()
        _last_proxy_used = proxy_url
        try:
            collected: List[dict] = []
            for url in urls:
                html = await _fetch_html(url, proxy_url)
                if _is_cf_page(html):
                    raise RuntimeError("Cloudflare page detected")
                nd = _extract_next_data(html)
                if not nd:
                    continue
                raw_items = _extract_items_from_next(nd)
                for r in raw_items:
                    it = _normalize_item(r)
                    if it.get("item_link"):
                        collected.append(it)

            # dedupe
            seen = set()
            uniq = []
            for it in collected:
                lk = it.get("item_link", "")
                if not lk or lk in seen:
                    continue
                seen.add(lk)
                uniq.append(it)

            filtered = [it for it in uniq if _is_fixed_price_no_bids(it)]
            filtered = filtered[:max_items]

            if fetch_sellers:
                for it in filtered:
                    lk = it.get("item_link")
                    if lk:
                        it["item_person_name"] = await _get_seller_from_detail(lk, proxy_url)

            return filtered
        except (PWTimeout, Exception) as e:
            last_err = e
            continue

    raise RuntimeError(f"Failed to scrape after proxy rotation (last_proxy={_last_proxy_used}): {last_err}")
