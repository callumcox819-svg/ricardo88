import asyncio
import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

import requests

from proxy_manager import next_proxy, normalize_proxy

# NOTE:
# Ricardo has 2 kinds of category pages:
# - Overview pages: /de/c/o/<slug>-<id>/  (mostly subcategories)
# - Listing pages : /de/c/<slug>-<id>/    (actual listings)
#
# We accept both: if it's an overview page we expand it to listing subcategories.

POPULAR_CATEGORIES: Dict[str, str] = {
    # Overview (as user provided)
    "Одежда и аксессуары": "https://www.ricardo.ch/de/c/o/kleidung-accessoires-40748/",
    "Женские аксессуары": "https://www.ricardo.ch/de/c/o/damenmode-40843/",
    "Дом и быт": "https://www.ricardo.ch/de/c/o/haushalt-wohnen-40295/",
    "Спорт": "https://www.ricardo.ch/de/c/o/sports-41875/",
    "Сад и инструменты": "https://www.ricardo.ch/de/c/o/handwerk-garten-39825/",
    "Смартфоны": "https://www.ricardo.ch/de/c/o/handy-festnetz-funk-39940/",
    "Компьютеры и сети": "https://www.ricardo.ch/de/c/o/computer-netzwerk-39091/",
    "Дети и младенцы": "https://www.ricardo.ch/de/c/o/kind-baby-40520/",
    "Часы": "https://www.ricardo.ch/de/c/o/uhren-schmuck-42272/",
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

def _parse_dt(v: Any) -> Optional[str]:
    """
    Best-effort parse for 'published_at' from various shapes.
    Returns ISO string in UTC, or None.
    """
    if v is None:
        return None
    # epoch seconds/ms
    if isinstance(v, (int, float)):
        ts = float(v)
        if ts > 10_000_000_000:  # ms
            ts = ts / 1000.0
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        except Exception:
            return None
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        # try iso
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z"):
            try:
                dt = datetime.strptime(s, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc).isoformat()
            except Exception:
                pass
        # sometimes "2025-01-27"
        try:
            dt = datetime.strptime(s[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except Exception:
            return None
    return None

def _looks_like_item(d: dict) -> bool:
    title = _pick(d, "title", "name")
    url = _pick(d, "url", "href", "link")
    if not isinstance(title, str) or len(title) < 3:
        return False
    if url is None:
        return False
    # Heuristic fields seen in Ricardo Next.js payloads
    return any(k in d for k in ("has_buy_now", "hasBuyNow", "bids_count", "bidsCount", "buy_now_price", "buyNowPrice", "buyNowPriceAmount", "listingId", "id"))

def _normalize_item(d: dict) -> dict:
    title = _pick(d, "title", "name") or ""
    url = _pick(d, "url", "href", "link")
    if isinstance(url, dict):
        url = _pick(url, "url", "href")
    if isinstance(url, str) and url.startswith("/"):
        url = "https://www.ricardo.ch" + url

    # price is messy; keep raw
    price = _pick(d, "buy_now_price", "buyNowPrice", "buyNowPriceAmount", "price", "startPrice", "startingPrice")
    # published
    published_at = _parse_dt(_pick(d, "published_at", "publishedAt", "createdDate", "created_at", "startDate", "start_date", "startDateTime", "start_date_time"))

    # images (single thumbnail)
    image = _pick(d, "image", "img", "imageUrl", "image_url", "thumbnailUrl", "thumbnail_url")
    if isinstance(image, dict):
        image = _pick(image, "url", "src")

    return {
        "title": title,
        "price": price if price is not None else "",
        "url": url or "",
        "description": "",
        "images": [image] if isinstance(image, str) and image else [],
        "location": "",
        "seller": {"name": "", "url": ""},
        "published_at": published_at or "",
    }

def _extract_items_from_next(next_data: dict) -> List[dict]:
    items = []
    for d in _walk(next_data):
        if isinstance(d, dict) and _looks_like_item(d):
            items.append(d)
    return items

def _expand_overview_links(html: str) -> List[str]:
    """
    Expand /de/c/o/... pages to real listing category URLs /de/c/<slug>-<id>/
    """
    soup = BeautifulSoup(html, "lxml")
    out: List[str] = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not isinstance(href, str):
            continue
        # only category listing pages, not /c/o/
        if re.match(r"^/de/c/[^/]+-\d+/?$", href) and "/de/c/o/" not in href:
            full = "https://www.ricardo.ch" + (href if href.endswith("/") else href + "/")
            if full not in seen:
                seen.add(full)
                out.append(full)
    return out

async def _fetch_html(url: str, proxy_url: Optional[str]) -> str:
    async with async_playwright() as p:
        launch_kwargs = {"headless": True, "args": ["--no-sandbox", "--disable-dev-shm-usage"]}
        if proxy_url:
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

def _looks_like_img_url(s: str) -> bool:
    return isinstance(s, str) and len(s) > 10 and (s.startswith("http://") or s.startswith("https://")) and any(ext in s.lower() for ext in (".jpg", ".jpeg", ".png", ".webp"))

async def _get_detail(url: str, proxy_url: Optional[str]) -> Dict[str, Any]:
    """
    Best-effort enrichment from item page.
    """
    html = await _fetch_html(url, proxy_url)
    nd = _extract_next_data(html)
    if not nd:
        return {}
    desc = ""
    loc = ""
    seller_name = ""
    seller_url = ""
    published_at = ""
    images: List[str] = []

    for d in _walk(nd):
        if not isinstance(d, dict):
            continue
        if not desc:
            v = _pick(d, "description", "shortDescription", "longDescription", "body")
            if isinstance(v, str) and len(v) > 10:
                desc = v.strip()
        if not loc:
            v = _pick(d, "location", "city", "zip", "postalCode", "postal_code")
            if isinstance(v, str) and 2 <= len(v) <= 80:
                loc = v.strip()
        if not seller_name:
            v = _pick(d, "sellerName", "seller_name", "username", "userName", "displayName", "nick")
            if isinstance(v, str) and 2 <= len(v) <= 80:
                seller_name = v.strip()
        if not seller_url:
            v = _pick(d, "sellerUrl", "seller_url", "profileUrl", "profile_url", "userUrl", "user_url")
            if isinstance(v, str) and v.startswith("/"):
                seller_url = "https://www.ricardo.ch" + v
            elif isinstance(v, str) and v.startswith("http"):
                seller_url = v
        if not published_at:
            pv = _pick(d, "published_at", "publishedAt", "createdDate", "created_at", "startDate", "startDateTime")
            iso = _parse_dt(pv)
            if iso:
                published_at = iso

        # collect images
        for key in ("image", "imageUrl", "url", "src"):
            iv = d.get(key)
            if isinstance(iv, str) and _looks_like_img_url(iv):
                images.append(iv)

    # de-dupe images
    uniq_imgs = []
    seen = set()
    for u in images:
        if u not in seen:
            seen.add(u)
            uniq_imgs.append(u)
    return {
        "description": desc,
        "location": loc,
        "seller": {"name": seller_name, "url": seller_url},
        "published_at": published_at,
        "images": uniq_imgs[:10],
    }

async def ricardo_collect_items(urls: List[str], max_items: int, fetch_sellers: bool = True) -> List[dict]:
    """
    Collect items from Ricardo category/search pages.
    - Accepts listing pages (/de/c/...) and overview pages (/de/c/o/...) and expands overviews.
    - No extra filters (auction + buy-now are both allowed).
    """
    if "__ALL__" in urls:
        urls = [u for k, u in POPULAR_CATEGORIES.items() if k != "Все подряд"]

    last_err: Optional[Exception] = None
    _last_proxy_used: Optional[str] = None

    for _ in range(8):
        proxy_url = next_proxy()
        _last_proxy_used = proxy_url
        try:
            collected: List[dict] = []
            for url in urls:
                html = await _fetch_html(url, proxy_url)
                if _is_cf_page(html):
                    raise RuntimeError("Cloudflare page detected")

                # Expand overview categories to listing categories
                expanded_urls = []
                if "/de/c/o/" in url:
                    expanded_urls = _expand_overview_links(html)
                target_urls = expanded_urls or [url]

                for tu in target_urls:
                    html2 = html if tu == url else await _fetch_html(tu, proxy_url)
                    nd = _extract_next_data(html2)
                    if not nd:
                        continue
                    raw_items = _extract_items_from_next(nd)
                    for r in raw_items:
                        it = _normalize_item(r)
                        if it.get("url"):
                            collected.append(it)
                    if len(collected) >= max_items * 3:
                        break

            # dedupe by url
            seen = set()
            uniq: List[dict] = []
            for it in collected:
                lk = it.get("url", "")
                if not lk or lk in seen:
                    continue
                seen.add(lk)
                uniq.append(it)

            # Enrich details (seller, description, images, location, published_at)
            if fetch_sellers:
                for it in uniq[: max_items * 2]:
                    lk = it.get("url")
                    if not lk:
                        continue
                    det = await _get_detail(lk, proxy_url)
                    if det:
                        it.update({k: v for k, v in det.items() if v})
                # keep order

            return uniq[:max_items]
        except (PWTimeout, Exception) as e:
            last_err = e
            continue

    raise RuntimeError(f"Failed to scrape after proxy rotation (last_proxy={_last_proxy_used}): {last_err}")


async def proxy_smoke_test(proxy: str):
    """Проверка прокси, которую вызывает админ-кнопка "Тест прокси".

    Возвращает (ok, details). Не тянем Playwright, чтобы не делать запуск тяжёлым.
    """
    p = normalize_proxy(proxy) or proxy
    test_url = "https://www.ricardo.ch/robots.txt"

    def _do_request():
        try:
            r = requests.get(
                test_url,
                timeout=15,
                proxies={"http": p, "https": p},
                headers={"User-Agent": "Mozilla/5.0"},
            )
            return r.status_code, r.text[:200]
        except Exception as e:
            return None, str(e)

    code, info = await asyncio.to_thread(_do_request) if hasattr(asyncio, "to_thread") else (None, "asyncio.to_thread missing")
    if code is None:
        return False, f"FAIL: {info}"
    ok = 200 <= int(code) < 400
    return ok, f"HTTP {code}: {info}"
