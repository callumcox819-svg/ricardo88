import asyncio
import json
import re
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from proxy_manager import next_proxy

# Popular categories shown in bot UI (names must match bot buttons)
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

SEARCH_URL = "https://www.ricardo.ch/api/rmf/search"

def _slug_from_category_url(url: str) -> Optional[str]:
    if not url:
        return None
    if url == "__ALL__":
        return None
    m = re.search(r"/c/([^/]+)/?", url)
    if m:
        return m.group(1)
    # allow passing already a slug
    if re.match(r"^[a-z0-9\-]+$", url):
        return url
    return None

def _requests_proxies(proxy_url: Optional[str]) -> Optional[dict]:
    if not proxy_url:
        return None
    # requests wants scheme included (socks5:// or http://)
    return {"http": proxy_url, "https": proxy_url}

def _get_json(url: str, params: dict, proxy_url: Optional[str]) -> dict:
    r = requests.get(url, params=params, proxies=_requests_proxies(proxy_url), timeout=30)
    r.raise_for_status()
    return r.json()

def _get_text(url: str, proxy_url: Optional[str]) -> str:
    r = requests.get(url, proxies=_requests_proxies(proxy_url), timeout=45)
    r.raise_for_status()
    return r.text

def _parse_iso(dt: str) -> Optional[datetime]:
    if not dt or not isinstance(dt, str):
        return None
    try:
        # handle Z
        if dt.endswith("Z"):
            return datetime.fromisoformat(dt.replace("Z", "+00:00"))
        return datetime.fromisoformat(dt)
    except Exception:
        return None

def _find_pdp_jsonld(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")
    tag = soup.find("script", {"id": "pdp-json-ld", "type": "application/ld+json"})
    if not tag or not tag.string:
        return {}
    try:
        return json.loads(tag.string)
    except Exception:
        return {}

def _normalize_from_jsonld(url: str, data: dict) -> Dict[str, Any]:
    offers = data.get("offers") or {}
    seller = data.get("seller") or {}
    image = data.get("image")
    if isinstance(image, list):
        item_photo = image[0] if image else ""
        images = image
    else:
        item_photo = image or ""
        images = [image] if image else []
    return {
        "item_person_name": seller.get("name") or "",
        "item_photo": item_photo or "",
        "item_link": url,
        "item_price": offers.get("price") or "",
        "item_title": data.get("name") or "",
        "item_desc": data.get("description") or "",
        "item_location": (data.get("availableAtOrFrom") or {}).get("address", {}).get("addressLocality") if isinstance(data.get("availableAtOrFrom"), dict) else "",
        "created_date": data.get("datePosted") or "",
        "images": images,
        "raw": data,
    }

def _extract_items_block(search_json: dict) -> List[dict]:
    # API may return different keys; support common ones
    for key in ("items", "results", "data", "listings"):
        v = search_json.get(key)
        if isinstance(v, list) and v:
            return v
    # sometimes nested
    if isinstance(search_json.get("result"), dict):
        for key in ("items", "results"):
            v = search_json["result"].get(key)
            if isinstance(v, list):
                return v
    return []

def _item_url_from_search_item(it: dict) -> Optional[str]:
    u = it.get("url") or it.get("itemUrl") or it.get("link")
    if isinstance(u, str):
        if u.startswith("/"):
            return "https://www.ricardo.ch" + u
        if u.startswith("http"):
            return u
    # sometimes id + slug
    iid = it.get("id") or it.get("itemId") or it.get("item_id")
    if iid:
        # best effort; real url will be fetched from jsonld anyway
        return f"https://www.ricardo.ch/de/a/{iid}/"
    return None

def _created_from_search_item(it: dict) -> Optional[datetime]:
    for k in ("createdDate", "created_date", "createdAt", "created_at", "startDate", "start_date"):
        dt = it.get(k)
        if isinstance(dt, str):
            d = _parse_iso(dt)
            if d:
                return d
    return None

async def proxy_smoke_test(test_url: str = "https://www.ricardo.ch/de/") -> Dict[str, Any]:
    proxy = next_proxy()
    if not proxy:
        return {"ok": False, "error": "proxy list empty"}
    try:
        html = await asyncio.to_thread(_get_text, test_url, proxy)
        return {"ok": True, "proxy": proxy, "bytes": len(html)}
    except Exception as e:
        return {"ok": False, "proxy": proxy, "error": str(e)}

async def ricardo_collect_items(
    urls: List[str],
    max_items: int = 30,
    fetch_sellers: bool = True,
    hours_back: int = 12,
) -> List[Dict[str, Any]]:
    """Collect up to max_items items across selected categories or all.

    urls: list of category URLs OR ['__ALL__'].
    hours_back: fixed 12h by default (per latest spec).
    """
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=hours_back)

    # build list of slugs; None means all
    slugs: List[Optional[str]] = []
    for u in urls or ["__ALL__"]:
        slug = _slug_from_category_url(u)
        slugs.append(slug)

    out: List[Dict[str, Any]] = []
    seen_links = set()

    # iterate pages per slug until enough
    for slug in slugs:
        page = 1
        while len(out) < max_items and page <= 10:
            proxy = next_proxy()
            params = {"page": page}
            if slug:
                params["categorySeoSlug"] = slug
            try:
                sj = await asyncio.to_thread(_get_json, SEARCH_URL, params, proxy)
            except Exception:
                # retry once without proxy
                sj = await asyncio.to_thread(_get_json, SEARCH_URL, params, None)

            items_block = _extract_items_block(sj)
            if not items_block:
                break

            # newest first expected; still filter by created date if present
            for it in items_block:
                if len(out) >= max_items:
                    break
                url = _item_url_from_search_item(it)
                if not url or url in seen_links:
                    continue
                created = _created_from_search_item(it)
                if created and created.tzinfo is None:
                    created = created.replace(tzinfo=timezone.utc)
                if created and created < cutoff:
                    # if results are ordered by recency, we can stop this slug
                    continue

                seen_links.add(url)
                # fetch detail json-ld for full fields
                try:
                    html = await asyncio.to_thread(_get_text, url, proxy)
                except Exception:
                    html = await asyncio.to_thread(_get_text, url, None)

                data = _find_pdp_jsonld(html)
                if not data:
                    # still return minimal
                    out.append({
                        "item_person_name": "",
                        "item_photo": "",
                        "item_link": url,
                        "item_price": "",
                        "item_title": it.get("title") or it.get("name") or "",
                        "created_date": created.isoformat() if created else "",
                        "raw": it,
                    })
                    continue

                norm = _normalize_from_jsonld(url, data)
                # fallback created_date from search if jsonld missing
                if not norm.get("created_date") and created:
                    norm["created_date"] = created.isoformat()
                # time filter using jsonld if possible
                dt = _parse_iso(norm.get("created_date") or "") or created
                if dt and dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                if dt and dt < cutoff:
                    continue

                out.append(norm)

            page += 1

    return out
