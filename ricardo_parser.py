
import os
import requests
from typing import List, Dict, Any

ACTOR_ID = "ecomscrape~ricardo-product-search-scraper"

# Popular categories (can be adjusted later)
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
    "Все подряд": "https://www.ricardo.ch/de/s/?sort=createdDateDesc",
}

def apify_run(urls: List[str], max_items: int) -> List[Dict[str, Any]]:
    token = os.getenv("APIFY_TOKEN", "").strip()
    if not token:
        raise RuntimeError("APIFY_TOKEN is not set")

    endpoint = f"https://api.apify.com/v2/acts/{ACTOR_ID}/run-sync-get-dataset-items?token={token}"
    payload = {
        # most actors accept this field name
        "urls": urls,
        "maxItems": max_items,
        # keep retries low, actor handles itself
    }
    r = requests.post(endpoint, json=payload, timeout=180)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        return []
    return data

def normalize_item(it: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "item_title": it.get("title") or "",
        "item_price": it.get("buy_now_price") or it.get("price") or "",
        "item_photo": it.get("image") or "",
        "item_link": it.get("url") or "",
        "item_person_name": it.get("seller_name") or it.get("seller") or "",
    }

def filter_no_bids_buy_now(it: Dict[str, Any]) -> bool:
    # TZ: only fixed price items (Buy Now) and no bids
    if not it.get("has_buy_now"):
        return False
    try:
        bids = int(it.get("bids_count") or 0)
    except Exception:
        bids = 0
    return bids == 0

def ricardo_collect_items(*, urls: List[str], max_items: int) -> List[Dict[str, Any]]:
    raw = apify_run(urls=urls, max_items=max_items)
    out: List[Dict[str, Any]] = []
    for it in raw:
        if filter_no_bids_buy_now(it):
            out.append(normalize_item(it))
    return out
