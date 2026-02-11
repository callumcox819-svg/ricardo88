
import re
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
import requests
from bs4 import BeautifulSoup

import proxy_manager

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
    "Все подряд": "https://www.ricardo.ch/de/",
}

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

def _requests_proxies() -> Optional[Dict[str, str]]:
    p = proxy_manager.next_proxy()
    if not p:
        return None
    # normalize to socks5h for DNS through proxy
    if p.startswith("socks5://"):
        p = "socks5h://" + p[len("socks5://"):]
    return {"http": p, "https": p}

def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7,ru;q=0.6",
        "Origin": "https://www.ricardo.ch",
        "Referer": "https://www.ricardo.ch/de/",
        "Connection": "keep-alive",
    })
    return s

def _discover_api_url(list_page_url: str, sess: requests.Session, timeout: int = 30) -> str:
    # The list pages embed a request to /api/sff/v4/search?... in HTML (Next.js).
    # We fetch the HTML and regex out the first occurrence.
    prox = _requests_proxies()
    r = sess.get(list_page_url, timeout=timeout, proxies=prox)
    r.raise_for_status()
    html = r.text
    m = re.search(r'(/api/sff/v4/search\?[^"\']+)', html)
    if m:
        return "https://www.ricardo.ch" + m.group(1)
    # fallback: build from category URL /c/<slug>-<id>/
    m2 = re.search(r'/c/([a-z0-9\-]+)-(\d+)/', list_page_url)
    if m2:
        slug, cid = m2.group(1), m2.group(2)
        original = f"/de/c/{slug}-{cid}/"
        return "https://www.ricardo.ch/api/sff/v4/search?categorySeoSlug=%s&categoryId=%s&locale=de&nextPageOffset=0&originalUrl=%s" % (slug, cid, requests.utils.quote(original, safe=""))
    # last resort: search without category (homepage)
    return "https://www.ricardo.ch/api/sff/v4/search?locale=de&nextPageOffset=0&originalUrl=%2Fde%2F"

def _set_next_offset(url: str, offset: int) -> str:
    # replace or add nextPageOffset
    if "nextPageOffset=" in url:
        return re.sub(r'nextPageOffset=\d+', f'nextPageOffset={offset}', url)
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}nextPageOffset={offset}"

def _parse_dt(val: Any) -> Optional[datetime]:
    if not val:
        return None
    if isinstance(val, (int, float)):
        # assume ms epoch
        try:
            return datetime.fromtimestamp(float(val)/1000.0, tz=timezone.utc)
        except Exception:
            return None
    if isinstance(val, str):
        # ISO
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except Exception:
            return None
    return None

def _extract_search_items(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    # actor can change field names; we try common keys
    for key in ("items", "results", "products", "auctions", "listings"):
        v = data.get(key)
        if isinstance(v, list):
            return v
    # some responses are like {"data": {"items":[...]}}
    d = data.get("data")
    if isinstance(d, dict):
        for key in ("items", "results", "products", "listings"):
            v = d.get(key)
            if isinstance(v, list):
                return v
    return []

def _extract_next_offset(data: Dict[str, Any], current_offset: int) -> Optional[int]:
    # common keys
    for key in ("nextPageOffset", "next_page_offset", "nextOffset"):
        v = data.get(key)
        if isinstance(v, int):
            return v
        if isinstance(v, str) and v.isdigit():
            return int(v)
    d = data.get("data")
    if isinstance(d, dict):
        for key in ("nextPageOffset", "next_page_offset", "nextOffset"):
            v = d.get(key)
            if isinstance(v, int):
                return v
            if isinstance(v, str) and v.isdigit():
                return int(v)
    # if total + offset present
    total = data.get("totalCount") or (d.get("totalCount") if isinstance(d, dict) else None)
    page_size = data.get("pageSize") or (d.get("pageSize") if isinstance(d, dict) else None)
    if isinstance(total, int) and isinstance(page_size, int):
        nxt = current_offset + page_size
        if nxt >= total:
            return None
        return nxt
    return current_offset + 20  # guess

def _detail_from_ldjson(html: str, url: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    # try ld+json
    for s in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            obj = json.loads(s.get_text(strip=True) or "{}")
        except Exception:
            continue
        # sometimes it is {"@context":..,"@graph":[...]}
        if isinstance(obj, dict) and "@graph" in obj and isinstance(obj["@graph"], list):
            # find Product/Offer
            for node in obj["@graph"]:
                if isinstance(node, dict) and node.get("@type") in ("Product", "Offer", "Thing", "WebPage"):
                    # product usually has name/description/image
                    pass
        if isinstance(obj, dict):
            if obj.get("@type") in ("Product", "Thing") or "description" in obj or "name" in obj:
                images = obj.get("image")
                if isinstance(images, str):
                    images = [images]
                elif not isinstance(images, list):
                    images = []
                offers = obj.get("offers") or {}
                price = None
                currency = None
                if isinstance(offers, dict):
                    price = offers.get("price") or offers.get("lowPrice")
                    currency = offers.get("priceCurrency")
                seller_name = None
                if isinstance(offers, dict):
                    seller = offers.get("seller")
                    if isinstance(seller, dict):
                        seller_name = seller.get("name")
                return {
                    "title": obj.get("name"),
                    "description": obj.get("description"),
                    "images": images,
                    "price": price,
                    "currency": currency,
                    "seller_name": seller_name,
                    "url": url,
                }
    return {"url": url}

def fetch_listing_detail(url: str, sess: requests.Session, timeout: int = 30) -> Dict[str, Any]:
    prox = _requests_proxies()
    r = sess.get(url, timeout=timeout, proxies=prox)
    r.raise_for_status()
    html = r.text
    d = _detail_from_ldjson(html, url)

    # try extract seller visible name in page as fallback (basic regex)
    if not d.get("seller_name"):
        m = re.search(r'"sellerNickname"\s*:\s*"([^"]+)"', html)
        if m:
            d["seller_name"] = m.group(1)
    # location
    m = re.search(r'"zip"\s*:\s*"([^"]+)"', html)
    if m:
        d["zip"] = m.group(1)
    m = re.search(r'"city"\s*:\s*"([^"]+)"', html)
    if m:
        d["city"] = m.group(1)
    return d

def ricardo_collect_items(
    list_page_urls: List[str],
    max_items: int,
    hours_back: int = 12,
    seen_sellers: Optional[set] = None,
    seen_urls: Optional[set] = None,
    timeout: int = 30,
) -> List[Dict[str, Any]]:
    """
    Collect items from Ricardo using the public frontend search API (/api/sff/v4/search).
    - list_page_urls: category URLs or homepage.
    - max_items: how many items to collect
    - hours_back: consider listings newer than this window (best-effort, depends on API fields)
    - seen_sellers/seen_urls: used to avoid duplicates
    """
    seen_sellers = seen_sellers or set()
    seen_urls = seen_urls or set()
    sess = _session()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)

    out: List[Dict[str, Any]] = []
    for list_url in list_page_urls:
        try:
            api_url = _discover_api_url(list_url, sess, timeout=timeout)
        except Exception:
            continue

        offset = 0
        while len(out) < max_items:
            url = _set_next_offset(api_url, offset)
            try:
                prox = _requests_proxies()
                r = sess.get(url, timeout=timeout, proxies=prox)
                r.raise_for_status()
                data = r.json()
            except Exception:
                break

            items = _extract_search_items(data)
            if not items:
                break

            for it in items:
                if len(out) >= max_items:
                    break
                # best-effort fields
                page_url = it.get("url") or it.get("itemUrl") or it.get("link")
                if page_url and page_url.startswith("/"):
                    page_url = "https://www.ricardo.ch" + page_url
                if not page_url:
                    iid = it.get("id") or it.get("item_id")
                    if iid:
                        page_url = f"https://www.ricardo.ch/de/a/x-{iid}/"
                if not page_url or page_url in seen_urls:
                    continue

                # try time filter
                created = _parse_dt(it.get("createdDate") or it.get("created_at") or it.get("created"))
                if created and created < cutoff:
                    # stop this category (sorted by new)
                    break

                # detail fetch for seller/description/images etc
                try:
                    detail = fetch_listing_detail(page_url, sess, timeout=timeout)
                except Exception:
                    continue

                seller = (detail.get("seller_name") or it.get("seller") or it.get("sellerNickname") or "").strip()
                if seller and seller in seen_sellers:
                    continue

                # merge
                item_out = {
                    "title": detail.get("title") or it.get("title") or it.get("name"),
                    "price": detail.get("price") or it.get("buy_now_price") or it.get("price"),
                    "currency": detail.get("currency") or "CHF",
                    "url": page_url,
                    "images": detail.get("images") or [it.get("image")] if it.get("image") else [],
                    "description": detail.get("description") or it.get("description"),
                    "seller": seller,
                    "location": " ".join([x for x in [detail.get("zip"), detail.get("city")] if x]),
                    "created_at": created.isoformat() if created else None,
                }
                seen_urls.add(page_url)
                if seller:
                    seen_sellers.add(seller)
                out.append(item_out)

            # advance offset
            nxt = _extract_next_offset(data, offset)
            if nxt is None or nxt == offset:
                break
            offset = nxt
            time.sleep(0.3)

        if len(out) >= max_items:
            break

    return out
