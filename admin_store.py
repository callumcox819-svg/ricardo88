\
import json
from pathlib import Path
from typing import Dict, Any, List

PROFILE_DIR = Path("Profile")
ADMIN_FILE = PROFILE_DIR / "admin.json"

def _load() -> Dict[str, Any]:
    if not ADMIN_FILE.exists():
        return {"allowed_users": []}
    try:
        return json.loads(ADMIN_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"allowed_users": []}

def _save(d: Dict[str, Any]) -> None:
    PROFILE_DIR.mkdir(exist_ok=True)
    tmp = ADMIN_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(ADMIN_FILE)

def list_allowed() -> List[int]:
    d = _load()
    out = []
    for x in d.get("allowed_users", []):
        try:
            out.append(int(x))
        except Exception:
            pass
    return sorted(set(out))

def add_allowed(user_id: int) -> None:
    d = _load()
    lst = set(list_allowed())
    lst.add(int(user_id))
    d["allowed_users"] = sorted(lst)
    _save(d)

def remove_allowed(user_id: int) -> None:
    d = _load()
    lst = set(list_allowed())
    try:
        lst.remove(int(user_id))
    except KeyError:
        pass
    d["allowed_users"] = sorted(lst)
    _save(d)
