
import requests, datetime, re, json
from bs4 import BeautifulSoup
from typing import List, Dict, Optional

SEARCH_URL = "https://www.ricardo.ch/api/rmf/search"

def fetch_search(category_slug: Optional[str], page:int=1) -> Dict:
    params = {"page": page}
    if category_slug and category_slug != "all":
        params["categorySeoSlug"] = category_slug
    r = requests.get(SEARCH_URL, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def extract_item_urls(search_json: Dict) -> List[str]:
    urls=[]
    for it in search_json.get("items", []) or search_json.get("results", []) or []:
        u = it.get("url") or it.get("itemUrl")
        if u and u.startswith("/"):
            u = "https://www.ricardo.ch"+u
        if u:
            urls.append(u)
    return urls

def parse_pdp_jsonld(url:str) -> Dict:
    html = requests.get(url, timeout=20).text
    soup=BeautifulSoup(html,"html.parser")
    script=soup.find("script", {"id":"pdp-json-ld"})
    data={}
    if script and script.string:
        try:
            data=json.loads(script.string)
        except Exception:
            pass
    return {
        "item_title": data.get("name"),
        "item_link": url,
        "item_photo": (data.get("image") or [None])[0] if isinstance(data.get("image"), list) else data.get("image"),
        "item_price": (data.get("offers") or {}).get("price"),
        "item_person_name": (data.get("seller") or {}).get("name") or (data.get("brand") or {}).get("name"),
        "created_date": data.get("datePosted"),
        "item_desc": data.get("description"),
        "raw": data,
    }
