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
    line = (line or "").strip()
    if not line:
        return None
    if "://" not in line:
        # default to socks5 for your case
        return "socks5://" + line
    return line

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
