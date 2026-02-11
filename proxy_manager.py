\
import json
from pathlib import Path
from typing import List, Dict, Any, Optional

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
    PROFILE_DIR.mkdir(exist_ok=True)
    tmp = PROXIES_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(PROXIES_FILE)

def normalize_proxy(line: str) -> Optional[str]:
    raw = (line or "").strip()
    if not raw:
        return None

    # Allow inputs like:
    #   "HTTPS proxy.example.com:38174:user:pass"
    #   "proxy.example.com:38174:user:pass"
    #   "http://user:pass@proxy.example.com:38174"
    #   "socks5://user:pass@host:port"
    #   "host:port"
    # Normalize whitespace but preserve possible "SCHEME <rest>" pattern.
    raw = " ".join(raw.replace("\t", " ").split())

    scheme: Optional[str] = None
    rest: str = raw
    first = raw.split(" ", 1)[0].lower()
    if first in {"http", "https", "socks5", "socks5h"} and " " in raw:
        scheme, rest = raw.split(" ", 1)
        scheme = scheme.lower()
        rest = rest.strip()

    # Remove any remaining spaces in the payload
    rest = "".join(rest.split())

    if "://" in rest:
        scheme2, rest2 = rest.split("://", 1)
        scheme = (scheme2 or scheme or "").lower()
        rest = rest2

    if scheme:
        scheme = scheme.lower().replace("socks5h", "socks5")

    # Accept "host:port:user:pass" (common provider format)
    # and convert to "scheme://user:pass@host:port"
    if "@" not in rest:
        parts = rest.split(":")
        if len(parts) == 4:
            host, port, user, pwd = parts
            scheme_final = scheme or "http"
            return f"{scheme_final}://{user}:{pwd}@{host}:{port}"
        if len(parts) == 2:
            host, port = parts
            scheme_final = scheme or "socks5"
            return f"{scheme_final}://{host}:{port}"

    # Already in "user:pass@host:port" form
    scheme_final = scheme or "socks5"
    return f"{scheme_final}://{rest}"

def set_proxies(lines: List[str]) -> int:
    prox: List[str] = []
    for ln in lines:
        p = normalize_proxy(ln)
        if p:
            prox.append(p)
    _save({"index": 0, "proxies": prox})
    return len(prox)

def get_proxies() -> List[str]:
    return _load().get("proxies", [])

def clear_proxies() -> None:
    _save({"index": 0, "proxies": []})

def next_proxy() -> Optional[str]:
    d = _load()
    prox = d.get("proxies", [])
    if not prox:
        return None
    idx = int(d.get("index", 0)) % len(prox)
    p = prox[idx]
    d["index"] = (idx + 1) % len(prox)
    _save(d)
    return p
