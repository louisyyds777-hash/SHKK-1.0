# -*- coding: utf-8 -*-
"""画线持久化：data_store/drawings/{code}.json（契约照搬 chanlun-DA 的 viz/drawings.py）"""
import json
import os
import threading

import data_source

_LOCK = threading.Lock()


def _path(code):
    code = str(code).strip()
    return os.path.join(data_source.STORE, "drawings", f"{code}.json")


def load(code):
    with _LOCK:
        p = _path(code)
        if not os.path.exists(p):
            return {"version": 0, "items": []}
        try:
            with open(p, encoding="utf-8") as f:
                d = json.load(f)
            if isinstance(d, dict) and isinstance(d.get("items"), list):
                return d
        except Exception:  # noqa: BLE001
            pass
        return {"version": 0, "items": []}


def save(code, items):
    old = load(code)
    d = {"version": int(old.get("version", 0)) + 1, "items": items}
    with _LOCK:
        data_source._atomic_write(_path(code), json.dumps(d, ensure_ascii=False))
    return d
