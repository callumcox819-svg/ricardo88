import asyncio
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from proxy_manager import next_proxy

BASE = "https://www.ricardo.ch"

# Популярные категории (SEO slug из URL)
POPULAR_CATEGORIES: Dict[str, str] = {
    "Одежда и аксессуары": "kleider-accessoires-403",
    "Женские аксессуары": "damenmode-accessoires-402",
    "Спорт": "sport-freizeit-410",
    "Дом и быт": "wohnen-haushalt-405",
    "Сад и инструменты": "garten-heimwerken-406",
    "Дети и младенцы": "baby-kind-407",
    "Смартфоны": "handys-smartphones-416",
    "Ноутбуки": "notebooks-418",
    "Компьютеры и сети": "computer-netzwerk-417",
    "Часы": "uhren-schmuck-408",
    "Косметика и уход": "beauty-gesundheit-412",
    "Все подряд": "__ALL__",
}

DEFAULT_LOCALE = "de"

def _requests_proxies(proxy_url: Optional[str]) -> Optional[Dict[str, str]]:
    if not proxy_url:
        return None
    return {"http": proxy_url, "https": proxy_url}

def _safe_json_loads(s: str) -> Optional[Any]:
    try:
        return json.loads(s)
    except Exception:
        return None

def _extract_json_ld(html: str) -> List[Any]:
    soup = BeautifulSoup(html, "lxml")
    out: List[Any] = []
    for sc in soup.find_all("script"):
        t = (sc.get("type") or "").lower().strip()
        if t != "application/ld+json":
            continue
        payload = sc.string or sc.get_text() or ""
        payload = payload.strip()
        if not payload:
            continue
        j = _safe_json_loads(payload)
        if j is not None:
            out.append(j)
    # Some pages use an id like pdp-json-ld with type application/ld+json already covered.
    return out

def _find_product_node(j: Any) -> Optional[Dict[str, Any]]:
    # Handles {"@graph":[...]} and direct Product object
    if isinstance(j, dict):
        if j.get("@type") == "Product":
            return j
        graph = j.get("@graph")
        if isinstance(graph, list):
            for node in graph:
                if isinstance(node, dict) and node.get("@type") == "Product":
                    return node
            # sometimes Product nested
            for node in graph:
                if isinstance(node, dict):
                    for v in node.values():
                        if isinstance(v, dict) and v.get("@type") == "Product":
                            return v
    if isinstance(j, list):
        for it in j:
            p = _find_product_node(it)
            if p:
                return p
    return None

def _parse_date(dt_val: Any) -> Optional[datetime]:
    if not dt_val:
        return None
    if isinstance(dt_val, (int, float)):
        try:
            return datetime.fromtimestamp(float(dt_val), tz=timezone.utc)
        except Exception:
            return None
    if isinstance(dt_val, str):
        s = dt_val.strip()
        # ISO
        try:
            if s.endswith("Z"):
                return datetime.fromisoformat(s.replace("Z", "+00:00"))
            return datetime.fromisoformat(s)
        except Exception:
            pass
        # fallback: "2026-02-11T10:34:53.333Z" handled above; other formats skip.
    return None

def _normalize_images(img: Any) -> List[str]:
    if not img:
        return []
    if isinstance(img, str):
        return [img]
    if isinstance(img, list):
        return [x for x in img if isinstance(x, str)]
    return []

def _normalize_price(offers: Any) -> Tuple[Optional[float], Optional[str]]:
    # offers can be dict or list
    def one(off: Dict[str, Any]) -> Tuple[Optional[float], Optional[str]]:
        price = off.get("price")
        currency = off.get("priceCurrency")
        try:
            p = float(price) if price is not None else None
        except Exception:
            p = None
        return p, currency
    if isinstance(offers, dict):
        return one(offers)
    if isinstance(offers, list):
        for o in offers:
            if isinstance(o, dict):
                return one(o)
    return None, None

def _normalize_seller(offers: Any) -> Tuple[str, str]:
    # returns (seller_name, seller_url)
    def one(off: Dict[str, Any]) -> Tuple[str, str]:
        seller = off.get("seller") or {}
        if isinstance(seller, dict):
            return (seller.get("name") or "").strip(), (seller.get("@id") or seller.get("url") or "").strip()
        return "", ""
    if isinstance(offers, dict):
        return one(offers)
    if isinstance(offers, list):
        for o in offers:
            if isinstance(o, dict):
                name, url = one(o)
                if name or url:
                    return name, url
    return "", ""

def fetch_item_details(url: str, timeout: int = 35) -> Dict[str, Any]:
    """
    Fetch listing page HTML and extract JSON-LD Product fields.
    Uses proxy rotation from proxy_manager.
    """
    proxy = next_proxy()
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
        "Accept-Language": "de,en;q=0.9",
    }
    r = requests.get(url, headers=headers, timeout=timeout, proxies=_requests_proxies(proxy))
    r.raise_for_status()
    html = r.text

    jlds = _extract_json_ld(html)
    product = None
    for j in jlds:
        product = _find_product_node(j)
        if product:
            break

    title = ""
    desc = ""
    imgs: List[str] = []
    created_dt: Optional[datetime] = None
    price, currency = (None, None)
    seller_name = ""
    seller_url = ""

    if product:
        title = (product.get("name") or "").strip()
        desc = (product.get("description") or "").strip()
        imgs = _normalize_images(product.get("image"))
        offers = product.get("offers")
        price, currency = _normalize_price(offers)
        seller_name, seller_url = _normalize_seller(offers)
        created_dt = _parse_date(product.get("datePosted") or product.get("releaseDate") or product.get("datePublished"))

    # fallback title from <title>
    if not title:
        m = re.search(r"<title>(.*?)</title>", html, flags=re.I | re.S)
        if m:
            title = re.sub(r"\s+", " ", m.group(1)).strip()

    return {
        "item_title": title,
        "item_desc": desc,
        "item_photo": imgs[0] if imgs else "",
        "images": imgs,
        "item_price": price,
        "currency": currency,
        "item_link": url,
        "item_person_name": seller_name,
        "person_link": seller_url,
        "created_real_date": created_dt.isoformat() if created_dt else "",
    }

def _extract_slug_from_url(url: str) -> Optional[str]:
    # https://www.ricardo.ch/de/c/kleider-accessoires-403/  -> kleider-accessoires-403
    try:
        p = urlparse(url)
        parts = [x for x in p.path.split("/") if x]
        if "c" in parts:
            idx = parts.index("c")
            if idx + 1 < len(parts):
                return parts[idx + 1]
        return None
    except Exception:
        return None

def api_search(category_slug: Optional[str], next_offset: int = 0, locale: str = DEFAULT_LOCALE, timeout: int = 35) -> Dict[str, Any]:
    proxy = next_proxy()
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "de,en;q=0.9",
        "Referer": BASE + "/",
    }
    params: Dict[str, Any] = {
        "nextPageOffset": next_offset,
        "locale": locale,
        "sort": "createdDateDesc",
        "seller_nickname": "",
        "brandName": "",
    }
    if category_slug and category_slug != "__ALL__":
        params["categorySeoSlug"] = category_slug
        params["originalUrl"] = f"{BASE}/{locale}/c/{category_slug}/"
    else:
        params["originalUrl"] = f"{BASE}/{locale}/?sort=createdDateDesc"

    r = requests.get(f"{BASE}/api/rmf/search", params=params, headers=headers, timeout=timeout, proxies=_requests_proxies(proxy))
    r.raise_for_status()
    return r.json()

def _find_items_list(payload: Any) -> List[Dict[str, Any]]:
    # Try common keys
    if isinstance(payload, dict):
        for k in ("items", "results", "ads", "listings", "data"):
            v = payload.get(k)
            if isinstance(v, list) and (not v or isinstance(v[0], dict)):
                return v  # type: ignore
        # deep scan
        for v in payload.values():
            if isinstance(v, list) and (not v or isinstance(v[0], dict)):
                # heuristic: contains id/title/url
                if not v:
                    continue
                sample = v[0]
                keys = set(sample.keys())
                if {"id", "title"} & keys or {"seoUrl", "url"} & keys:
                    return v  # type: ignore
    return []

def _item_url_from_search_item(it: Dict[str, Any]) -> Optional[str]:
    for k in ("seoUrl", "url", "itemUrl", "href"):
        v = it.get(k)
        if isinstance(v, str) and v.startswith("http"):
            return v
        if isinstance(v, str) and v.startswith("/"):
            return urljoin(BASE, v)
    # Sometimes slug/id
    slug = it.get("seo_slug") or it.get("seoSlug")
    if isinstance(slug, str):
        return urljoin(BASE, f"/de/a/{slug}/")
    return None

def _created_from_search_item(it: Dict[str, Any]) -> Optional[datetime]:
    for k in ("createdDate", "created_date", "createdAt", "created"):
        d = it.get(k)
        dt = _parse_date(d)
        if dt:
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return None

async def ricardo_collect_items(urls: List[str], max_items: int, hours_window: int = 12) -> List[Dict[str, Any]]:
    """
    urls: list of category URLs or "__ALL__" marker
    Returns normalized items with details (title, description, images, seller, url, created).
    """
    # category slugs
    slugs: List[Optional[str]] = []
    for u in urls:
        if u == "__ALL__":
            slugs.append("__ALL__")
        else:
            # allow either full url or slug
            if "http" in u:
                slugs.append(_extract_slug_from_url(u))
            else:
                slugs.append(u)

    deadline = datetime.now(timezone.utc) - timedelta(hours=hours_window)

    collected: List[Dict[str, Any]] = []
    seen_urls: set = set()

    # For each selected category: paginate a bit
    for slug in slugs:
        offset = 0
        safety_pages = 8  # avoid infinite
        for _ in range(safety_pages):
            payload = await asyncio.to_thread(api_search, slug if slug != "__ALL__" else None, offset)
            items = _find_items_list(payload)
            if not items:
                break

            # compute next offset
            next_off = None
            if isinstance(payload, dict):
                for k in ("nextPageOffset", "next_offset", "nextOffset"):
                    if isinstance(payload.get(k), int):
                        next_off = int(payload[k])
                        break
                if next_off is None:
                    paging = payload.get("paging") or payload.get("page")
                    if isinstance(paging, dict):
                        no = paging.get("nextPageOffset") or paging.get("next_offset") or paging.get("nextOffset")
                        if isinstance(no, int):
                            next_off = int(no)
            if next_off is None:
                next_off = offset + len(items)

            # iterate items
            for it in items:
                url = _item_url_from_search_item(it)
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)

                created_dt = _created_from_search_item(it)
                if created_dt and created_dt < deadline:
                    # search results are sorted by created desc -> can stop this category
                    break

                # fetch details (HTML json-ld)
                try:
                    detail = await asyncio.to_thread(fetch_item_details, url)
                except Exception:
                    continue

                # apply time window after detail too
                dt2 = _parse_date(detail.get("created_real_date"))
                if dt2 and dt2.tzinfo is None:
                    dt2 = dt2.replace(tzinfo=timezone.utc)
                if dt2 and dt2 < deadline:
                    continue

                collected.append(detail)
                if len(collected) >= max_items:
                    return collected

            offset = next_off
    return collected
