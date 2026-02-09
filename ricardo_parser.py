# ricardo_parser.py
# Ricardo.ch parser per TZ:
# - Fixed price only ("Sofort kaufen"), no auctions/bids
# - Seller name must be "Name Surname" (2+ words)
# - Best-effort heuristics for private sellers
#
# NOTE: Ricardo can change markup & has anti-bot measures. If requests gets blocked,
# you may need Playwright later. This module keeps things simple and fast.

import re
import time
from typing import Dict, List, Optional
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.ricardo.ch"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0 Safari/537.36"
    )
}

NAME_SURNAME_RE = re.compile(
    r"^[A-Za-zÀ-ÖØ-öø-ÿÄÖÜäöüß'\-]+\s+[A-Za-zÀ-ÖØ-öø-ÿÄÖÜäöüß'\-]+(?:\s+[A-Za-zÀ-ÖØ-öø-ÿÄÖÜäöüß'\-]+)*$"
)

def http_get(url: str, timeout: int = 25) -> Optional[str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        if r.status_code != 200:
            return None
        return r.text
    except Exception:
        return None

def soup_from_html(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")

def normalize_link(href: str) -> str:
    if not href:
        return ""
    href = href.strip()
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return BASE_URL + href
    return ""

def is_name_surname(seller_name: str) -> bool:
    s = (seller_name or "").strip()
    if not s:
        return False
    s = re.sub(r"\s+", " ", s)
    return bool(NAME_SURNAME_RE.match(s))

def is_fixed_price_no_bids(page_text: str) -> bool:
    t = (page_text or "").lower()
    has_buy_now = ("sofort kaufen" in t) or ("sofortkaufen" in t)

    # Auction signals
    has_auction_signals = ("gebote" in t) or ("bieten" in t) or ("auktion" in t)

    if not has_buy_now:
        return False

    if has_auction_signals:
        # allow ONLY if explicitly 0 bids
        if re.search(r"\b0\s+gebote\b", t):
            return True
        return False

    return True

def extract_og(soup: BeautifulSoup, prop: str) -> str:
    tag = soup.find("meta", attrs={"property": prop})
    return (tag.get("content", "") if tag else "").strip()

def extract_title(soup: BeautifulSoup) -> str:
    h1 = soup.select_one("h1")
    if h1:
        return h1.get_text(strip=True)
    return extract_og(soup, "og:title")

def extract_image(soup: BeautifulSoup) -> str:
    og = extract_og(soup, "og:image")
    if og:
        return og
    img = soup.select_one("img")
    if img and img.get("src"):
        return img["src"].strip()
    return ""

def extract_price_chf(soup: BeautifulSoup) -> str:
    # Often present in og:description
    og_desc = extract_og(soup, "og:description")
    if og_desc:
        m = re.search(r"(\d[\d'., ]*)\s*CHF", og_desc)
        if m:
            val = re.sub(r"\s+", " ", m.group(1)).strip()
            return f"{val} CHF"

    text = soup.get_text(" ", strip=True)
    m = re.search(r"(\d[\d'., ]*)\s*CHF", text)
    if m:
        val = re.sub(r"\s+", " ", m.group(1)).strip()
        return f"{val} CHF"

    return ""

def extract_seller_name(soup: BeautifulSoup) -> str:
    # Try to find label "Verkäufer" and then nearby link/text.
    seller_label = soup.find(string=re.compile(r"verk(ä|ae)ufer", re.IGNORECASE))
    if seller_label:
        parent = getattr(seller_label, "parent", None)
        if parent:
            container = parent.find_parent()
            if container:
                # sometimes name is in <p> or <a>
                a = container.find("a")
                if a:
                    txt = a.get_text(" ", strip=True)
                    if txt:
                        return txt
                p = container.find("p")
                if p:
                    txt = p.get_text(" ", strip=True)
                    if txt:
                        return txt

    # Fallback: profile/shop links
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if "/user/" in href or "/shop/" in href or "/de/u/" in href:
            txt = a.get_text(" ", strip=True)
            if txt and len(txt) <= 80:
                return txt

    # Last resort: common MUI typography blocks (your screenshot suggests this)
    # Grab short text tokens near avatar blocks
    candidates = []
    for p in soup.select("p"):
        txt = p.get_text(" ", strip=True)
        if txt and 2 <= len(txt) <= 60 and is_name_surname(txt):
            candidates.append(txt)
    if candidates:
        return candidates[0]

    return ""

def is_private_seller(page_text: str, seller_name: str) -> bool:
    # Heuristics: if page mentions shop/merchant, treat as non-private
    t = (page_text or "").lower()
    commercial_signals = ["gewerblich", "händler", "shop", "firma", "unternehmen", "professional"]
    if any(w in t for w in commercial_signals):
        return False

    # Strict rule from TZ
    return is_name_surname(seller_name)

def ricardo_parse_ad_page(url: str) -> Optional[Dict]:
    html = http_get(url)
    if not html:
        return None

    soup = soup_from_html(html)
    page_text = soup.get_text(" ", strip=True)

    if not is_fixed_price_no_bids(page_text):
        return None

    item_title = extract_title(soup)
    item_photo = extract_image(soup)
    item_price = extract_price_chf(soup)
    seller_name = extract_seller_name(soup)

    if not is_private_seller(page_text, seller_name):
        return None

    return {
        "item_title": item_title,
        "item_photo": item_photo,
        "ads_number": None,
        "parser_views": 0,
        "ads_number_bought": None,
        "ads_number_sold": None,
        "gender": "",
        "email": "",
        "person_reg_date": "",
        "item_price": item_price,
        "views": None,
        "rating": None,
        "created_date": "",
        "created_real_date": "",
        "phone": "",
        "item_desc": "",
        "location": "",
        "item_link": url,
        "person_link": "",
        "item_person_name": seller_name,
    }

def ricardo_search_links(query: str, page: int = 1, limit: int = 120) -> List[str]:
    q_raw = (query or "").strip()
    if not q_raw:
        url = f"{BASE_URL}/de/s/"
    else:
        q = quote(q_raw)
        url = f"{BASE_URL}/de/s/{q}/"
    if page > 1:
        url += f"?page={page}"

    html = http_get(url)
    if not html:
        return []

    soup = soup_from_html(html)
    links: List[str] = []

    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        full = normalize_link(href)
        if not full:
            continue
        if "/a/" in full:
            if full not in links:
                links.append(full)
        if len(links) >= limit:
            break

    return links

def ricardo_collect_items(
    query: str,
    pages: int = 3,
    per_page_links: int = 120,
    delay: float = 0.25,
    max_items: int = 30,
) -> List[Dict]:
    results: List[Dict] = []
    seen = set()

    for p in range(1, pages + 1):
        links = ricardo_search_links(query=query, page=p, limit=per_page_links)
        for link in links:
            if link in seen:
                continue
            seen.add(link)

            it = ricardo_parse_ad_page(link)
            if it:
                results.append(it)

            if len(results) >= max_items:
                return results

            time.sleep(delay)

    return results
