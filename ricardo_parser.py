
import os
import requests
from typing import List, Dict, Any

ACTOR_ID = "ecomscrape/ricardo-product-search-scraper"

POPULAR_CATEGORIES = {
    "👕 Одежда и аксессуары": "https://www.ricardo.ch/de/c/kleider-accessoires-403/",
    "👠 Женские аксессуары": "https://www.ricardo.ch/de/c/damenmode-accessoires-402/",
    "⚽ Спорт": "https://www.ricardo.ch/de/c/sport-freizeit-410/",
    "🏠 Дом и быт": "https://www.ricardo.ch/de/c/wohnen-haushalt-405/",
    "🛠 Инструменты и сад": "https://www.ricardo.ch/de/c/garten-heimwerken-406/",
    "👶 Дети и младенцы": "https://www.ricardo.ch/de/c/baby-kind-407/",
    "📱 Смартфоны": "https://www.ricardo.ch/de/c/handys-smartphones-416/",
    "💻 Ноутбуки": "https://www.ricardo.ch/de/c/notebooks-418/",
    "🖥 Компьютеры и сети": "https://www.ricardo.ch/de/c/computer-netzwerk-417/",
    "⌚ Часы": "https://www.ricardo.ch/de/c/uhren-schmuck-408/",
    "💄 Косметика и уход": "https://www.ricardo.ch/de/c/beauty-gesundheit-412/",
    "🎮 Игры и консоли": "https://www.ricardo.ch/de/c/games-konsolen-419/",
    "🚗 Авто аксессуары": "https://www.ricardo.ch/de/c/auto-motorrad-411/",
}

def apify_search(urls: List[str], max_items: int = 30) -> List[Dict[str, Any]]:
    token = os.getenv("APIFY_TOKEN", "").strip()
    if not token:
        raise RuntimeError("APIFY_TOKEN is not set")

    endpoint = f"https://api.apify.com/v2/acts/{ACTOR_ID}/run-sync-get-dataset-items?token={token}"

    payload = {"urls": urls, "maxItems": max_items}

    resp = requests.post(endpoint, json=payload, timeout=120)
    resp.raise_for_status()
    data = resp.json()

    results = []
    for it in data:
        if not it.get("has_buy_now"):
            continue
        if int(it.get("bids_count") or 0) != 0:
            continue

        results.append({
            "item_title": it.get("title"),
            "item_price": it.get("buy_now_price"),
            "item_url": it.get("url"),
            "item_image": it.get("image"),
            "item_person_name": it.get("seller_name", ""),
        })

    return results


# Compatibility wrapper for previous bot versions
def ricardo_collect_items(query=None, pages=1, max_items=30):
    # If user selected categories in future, urls list will be built there.
    urls = ["https://www.ricardo.ch/de/"]
    return apify_search(urls, max_items=max_items)
