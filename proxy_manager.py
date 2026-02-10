
import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse

PROFILE_DIR = Path("Profile")
PROXIES_FILE = PROFILE_DIR / "proxies.json"

def _load() -> Dict[str, Any]:
    if not PROXIES_FILE.exists():
        return {"index": 0, "proxies": []}
    try:
        return json.loads(PROXIES_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"index": 0, "proxies": []}

def _save(d: Dict[str, Any]) -> None:
    tmp = PROXIES_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(PROXIES_FILE)

def normalize_proxy(line: str) -> Optional[Dict[str, str]]:
    s = (line or "").strip()
    if not s:
        return None
    if "://" not in s:
        s = "socks5://" + s
    u = urlparse(s)
    if not u.scheme or not u.hostname or not u.port:
        return None
    server = f"{u.scheme}://{u.hostname}:{u.port}"
    d: Dict[str, str] = {"server": server}
    if u.username:
        d["username"] = u.username
    if u.password:
        d["password"] = u.password
    return d

def set_proxies(lines: List[str]) -> int:
    prox: List[Dict[str, str]] = []
    for ln in lines:
        p = normalize_proxy(ln)
        if p:
            prox.append(p)
    d = _load()
    d["proxies"] = prox
    d["index"] = 0
    _save(d)
    return len(prox)

def get_proxies() -> List[Dict[str, str]]:
    return _load().get("proxies", [])

def clear_proxies() -> None:
    d = _load()
    d["proxies"] = []
    d["index"] = 0
    _save(d)

def next_proxy() -> Optional[Dict[str, str]]:
    d = _load()
    prox = d.get("proxies", [])
    if not prox:
        return None
    idx = int(d.get("index", 0)) % len(prox)
    p = prox[idx]
    d["index"] = (idx + 1) % len(prox)
    _save(d)
    return p
