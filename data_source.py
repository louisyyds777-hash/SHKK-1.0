# -*- coding: utf-8 -*-
"""
手绘K线 · 数据层
取数路径照搬 chanlun-DA.v1.0（src/chanda/data）：
  腾讯主源：分钟K(mkline ≤800根) / 日K(fqkline 前复权 640根/页全史翻页) / 报价(名称解析)
  新浪弱源：分钟K历史打底(≤1970根) / 全部A股列表(搜索池)
字段坑（来自该仓库实测）：
  腾讯分钟线行序 [时间,开,收,高,低,量(手)]，日线行序 [日期,开,收,高,低,量(手)]
  新浪行序标准 [开,高,低,收]，量=股(除以100归一手)
  时间标签一律为收盘时刻，ts_open = 标签 - 周期
周线/月线：仓库未实现，这里用"日线全史重采样"（等价于其 resample.py 思路）
"""
import json
import os
import re
import threading
import time
from collections import OrderedDict
from datetime import datetime, timedelta, timezone

import requests

BJ = timezone(timedelta(hours=8))
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

_sess = requests.Session()          # 连接复用：翻页/搜索池省掉每次TCP+TLS握手
_sess.headers.update(UA)

BASE_MKLINE = "https://ifzq.gtimg.cn/appstock/app/kline/mkline"
BASE_FQKLINE = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
BASE_QUOTE = "https://qt.gtimg.cn/q="
SINA_KLINE = "https://quotes.sina.cn/cn/api/jsonp_v2.php/var%20_d=/CN_MarketDataService.getKLineData"
SINA_LIST = ("https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/"
             "Market_Center.getHQNodeData")

TF_MS = {"5m": 5 * 60_000, "30m": 30 * 60_000, "60m": 60 * 60_000}
TF_MK = {"5m": "m5", "30m": "m30", "60m": "m60"}
PERIODS = ("5m", "30m", "60m", "1d", "1w", "1M")

STORE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_store")


def _now_ms():
    return int(time.time() * 1000)


def http_text(url, encoding="utf-8"):
    last = None
    for i in range(3):
        try:
            r = _sess.get(url, timeout=15)
            r.encoding = encoding
            return r.text
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(0.8 * (i + 1))
    raise last


def normalize_symbol(code):
    code = str(code).strip().lower()
    code = re.sub(r"^(sh|sz|bj)", "", code)
    code = code.split(".")[0]
    if not code.isdigit():
        return None
    if code[0] in "659":
        return "sh" + code
    if code[0] in "0123":
        return "sz" + code
    return "sh" + code


# ---------------- 腾讯主源 ----------------

def fetch_mkline_tx(symbol, tf, count=800):
    """分钟K，返回 [ts_open, o, h, l, c, v(手)] 升序"""
    param = f"{symbol},{TF_MK[tf]},,{min(count, 800)}"
    data = json.loads(http_text(f"{BASE_MKLINE}?param={param}"))
    rows = data["data"][symbol][TF_MK[tf]]
    ms = TF_MS[tf]
    out = []
    for row in rows:
        ts = int(datetime.strptime(row[0], "%Y%m%d%H%M")
                 .replace(tzinfo=BJ).timestamp() * 1000) - ms
        out.append([ts, float(row[1]), float(row[3]), float(row[4]),
                    float(row[2]), float(row[5])])
    return out


def fetch_kline_tx(symbol, tf="day", end="", count=640, fq=""):
    """日K（tf 也支持 week/month）。fq="" 不复权（真实价格，腾讯qfq算法会给历史负价，弃用）"""
    param = f"{symbol},{tf},,{end},{count},{fq}"
    data = json.loads(http_text(f"{BASE_FQKLINE}?param={param}"))
    d = data["data"][symbol]
    rows = d.get(fq + tf) or d.get(tf) or []
    out = []
    for row in rows:
        ts = int(datetime.strptime(row[0], "%Y-%m-%d")
                 .replace(tzinfo=BJ).timestamp() * 1000)
        out.append([ts, float(row[1]), float(row[3]), float(row[4]),
                    float(row[2]), float(row[5])])
    return out


def fetch_daily_full(symbol, before=""):
    """日K全史：640根/页向前翻页。before=已缓存最早交易日，从其前一天起补（增量补全史）"""
    all_rows = []
    end = before
    while True:
        rows = fetch_kline_tx(symbol, "day", end=end, count=640)
        if not rows:
            break
        all_rows = rows + all_rows
        if len(rows) < 640:
            break
        first_day = datetime.fromtimestamp(rows[0][0] / 1000, BJ)
        end = (first_day - timedelta(days=1)).strftime("%Y-%m-%d")
        time.sleep(0.15)
    return all_rows


def fetch_name(code):
    """腾讯报价解析股票名称（GBK，~分隔）"""
    sym = normalize_symbol(code)
    if not sym:
        return None
    txt = http_text(BASE_QUOTE + sym, "gbk")
    for line in txt.strip().split(";"):
        if "=" in line:
            parts = line.split("~")
            if len(parts) > 35 and parts[2] == sym[2:]:
                return parts[1]
    return None


# ---------------- 新浪弱源 ----------------

def fetch_mkline_sina(symbol, tf, count=1970):
    scale = {"5m": 5, "30m": 30, "60m": 60}[tf]
    url = f"{SINA_KLINE}?symbol={symbol}&scale={scale}&ma=no&datalen={count}"
    txt = http_text(url)
    data = json.loads(txt[txt.find("(") + 1: txt.rfind(")")])
    ms = TF_MS[tf]
    out = []
    for row in data:
        ts = int(datetime.strptime(row["day"], "%Y-%m-%d %H:%M:%S")
                 .replace(tzinfo=BJ).timestamp() * 1000) - ms
        out.append([ts, float(row["open"]), float(row["high"]),
                    float(row["low"]), float(row["close"]),
                    float(row["volume"]) / 100.0])
    return out


def build_search_pool():
    """新浪全部A股列表（hs_a 节点，100/页），返回 {code: name}"""
    pool = OrderedDict()
    for page in range(1, 90):
        url = f"{SINA_LIST}?page={page}&num=100&sort=symbol&asc=1&node=hs_a"
        txt = None
        for enc in ("gbk", "utf-8"):
            try:
                txt = http_text(url, enc)
                json.loads(txt)
                break
            except Exception:  # noqa: BLE001
                txt = None
        if txt is None:
            break
        data = json.loads(txt)
        if not data:
            break
        for it in data:
            code, name = it.get("code"), it.get("name")
            if code and name:
                pool[code] = name
        time.sleep(0.12)
    return dict(pool)


# ---------------- 周线/月线重采样 ----------------

def resample_daily(bars, tf):
    """日线 [ts,o,h,l,c,v] → 周线/月线（开=首日开 高=最高 低=最低 收=末日收 量=求和）"""
    buckets = OrderedDict()
    for ts, o, h, l, c, v in bars:
        d = datetime.fromtimestamp(ts / 1000, BJ)
        if tf == "1w":
            d0 = d - timedelta(days=d.weekday())          # 本周周一
        else:
            d0 = d.replace(day=1)                          # 本月一号
        k = d0.strftime("%Y-%m-%d")
        b = buckets.get(k)
        if b is None:
            buckets[k] = [ts, o, h, l, c, v]
        else:
            b[2] = max(b[2], h)
            b[3] = min(b[3], l)
            b[4] = c
            b[5] += v
    return list(buckets.values())


def drop_partial(bars, tf):
    """剔除未收盘K线（纪律照搬仓库：未收盘bar绝不进图）"""
    now = _now_ms()
    if tf in TF_MS:
        dur = TF_MS[tf]
        return [b for b in bars if b[0] + dur <= now]
    nowd = datetime.now(BJ)
    out = []
    for b in bars:
        d = datetime.fromtimestamp(b[0] / 1000, BJ)
        if tf == "1d":
            if d.date() < nowd.date():
                out.append(b)
        elif tf == "1w":
            week_start = nowd.date() - timedelta(days=nowd.weekday())
            if d.date() < week_start:
                out.append(b)
        else:  # 1M
            if (d.year, d.month) != (nowd.year, nowd.month):
                out.append(b)
    return out


def merge_bars(old, new):
    """同 ts 新值覆盖旧值（qfq 复权刷新依赖此语义）"""
    m = {b[0]: b for b in old}
    for b in new:
        m[b[0]] = b
    return [m[k] for k in sorted(m)]


# ---------------- 缓存与编排 ----------------

def _atomic_write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    for _ in range(8):
        try:
            os.replace(tmp, path)
            return
        except PermissionError:
            time.sleep(0.01)
    os.replace(tmp, path)


class KlineStore:
    """K线缓存：data_store/kline/{symbol}/{tf}.json。
    秒开策略：有缓存立即返回（过期→后台增量刷新）；冷缓存同步只发1个快请求先出图，
    全史/新浪打底/重采样回退全部挪到后台线程补齐（stale-while-revalidate）。"""

    def __init__(self):
        self.lock = threading.RLock()
        self.mem = {}
        self.last_fetch = {}
        self.refreshing = set()        # 正在后台刷新的 (sym, tf)
        self.names = {}

    def _path(self, sym, tf):
        return os.path.join(STORE, "kline", sym, tf + ".json")

    def _load(self, sym, tf):
        p = self._path(sym, tf)
        if os.path.exists(p):
            try:
                with open(p, encoding="utf-8") as f:
                    return json.load(f)
            except Exception:  # noqa: BLE001
                pass
        return []

    def _save(self, sym, tf, bars):
        _atomic_write(self._path(sym, tf), json.dumps(bars))

    def _fetch_upstream(self, sym, tf, deep):
        """实际拉数并合并入缓存（网络在锁外，不阻塞其他 symbol/period 请求）。
        deep=False 快路径：只发1个请求（分钟=腾讯800根，日=最近一页，周月=直取）。
        deep=True 深挖：日K补全史翻页、分钟新浪打底到~1970根、周月失败回退日线重采样。"""
        with self.lock:
            bars = self.mem.get((sym, tf)) or []
        if tf in TF_MS:
            rows = fetch_mkline_tx(sym, tf, 800)
            if deep and len(bars) < 1900:            # 浅窗 → 新浪打底
                try:
                    rows = merge_bars(fetch_mkline_sina(sym, tf, 1970), rows)
                except Exception:  # noqa: BLE001
                    pass
            new = merge_bars(bars, rows)
        elif tf == "1d":
            if deep:
                before = ""
                if bars:
                    first = datetime.fromtimestamp(bars[0][0] / 1000, BJ)
                    before = (first - timedelta(days=1)).strftime("%Y-%m-%d")
                new = merge_bars(bars, fetch_daily_full(sym, before))
            else:
                new = merge_bars(bars, fetch_kline_tx(sym, "day"))
        else:                                        # 1w / 1M：腾讯周/月线直取，失败回退日线重采样
            api_tf = {"1w": "week", "1M": "month"}[tf]
            try:
                rows = fetch_kline_tx(sym, api_tf, count=640)
                if rows:
                    new = merge_bars(bars, rows)
                else:
                    raise ValueError("empty " + api_tf)
            except Exception:  # noqa: BLE001
                if deep or not bars:
                    daily = self._raw(sym, "1d")
                    new = resample_daily(daily, tf)
                else:
                    new = bars
        with self.lock:
            self.mem[(sym, tf)] = new
            self._save(sym, tf, new)
        return new

    def _refresh_async(self, sym, tf, deep):
        key = (sym, tf)
        with self.lock:
            if key in self.refreshing:
                return
            self.refreshing.add(key)

        def run():
            try:
                self._fetch_upstream(sym, tf, deep)
            except Exception:  # noqa: BLE001
                pass
            finally:
                with self.lock:
                    self.refreshing.discard(key)
        threading.Thread(target=run, daemon=True).start()

    def _raw(self, sym, tf):
        """带增量更新的原始（含未收盘尾根）序列。
        任何情况下最多同步等1个快请求；深挖一律后台。"""
        key = (sym, tf)
        with self.lock:
            if key not in self.mem:
                self.mem[key] = self._load(sym, tf)
            bars = self.mem[key]
            fresh = time.time() - self.last_fetch.get(key, 0) <= 60
            if not fresh:
                self.last_fetch[key] = time.time()      # 先占位，防并发重复拉
        if bars:
            if not fresh:                               # 有缓存：立即返回，后台增量刷新
                self._refresh_async(sym, tf, deep=False)
            return bars
        try:                                            # 冷缓存：同步快路径先出图
            self._fetch_upstream(sym, tf, deep=False)
        except Exception:  # noqa: BLE001
            pass
        self._refresh_async(sym, tf, deep=True)         # 后台补全史/打底
        with self.lock:
            return self.mem.get(key, [])

    def get(self, code, tf):
        sym = normalize_symbol(code)
        if not sym or tf not in PERIODS:
            return []
        return drop_partial(self._raw(sym, tf), tf)

    def name_of(self, code):
        code = str(code).strip()
        sym = normalize_symbol(code)
        short = sym[2:] if sym else code
        with self.lock:
            if short in self.names:
                return self.names[short]
        pool = search_pool()
        name = pool.get(short) if pool else None
        if not name:
            try:
                name = fetch_name(short)
            except Exception:  # noqa: BLE001
                name = None
        if name:
            with self.lock:
                self.names[short] = name
        return name


# ---------------- 搜索池 ----------------

_pool_lock = threading.Lock()
_pool = None
_pool_state = {"building": False, "built_at": 0}


def _pool_path():
    return os.path.join(STORE, "search_pool.json")


def search_pool():
    global _pool
    with _pool_lock:
        if _pool is not None:
            return _pool
        if os.path.exists(_pool_path()):
            try:
                with open(_pool_path(), encoding="utf-8") as f:
                    d = json.load(f)
                if time.time() - d.get("built_at", 0) < 7 * 86400:
                    _pool = d["pool"]
                    return _pool
            except Exception:  # noqa: BLE001
                pass
        return None


def build_pool_async():
    def run():
        time.sleep(2.0)                             # 让首页K线快路径先抢到带宽
        with _pool_lock:
            if _pool_state["building"]:
                return
            _pool_state["building"] = True
        try:
            pool = build_search_pool()
            if len(pool) > 100:
                global _pool
                with _pool_lock:
                    _pool = pool
                    _pool_state["built_at"] = time.time()
                _atomic_write(_pool_path(), json.dumps(
                    {"built_at": _pool_state["built_at"], "pool": pool}))
        except Exception:  # noqa: BLE001
            pass
        finally:
            with _pool_lock:
                _pool_state["building"] = False
    threading.Thread(target=run, daemon=True).start()


def search(q, limit=20):
    pool = search_pool()
    if not pool:
        return None
    q = q.strip().lower()
    out = []
    for code, name in pool.items():
        if q in code or q in name.lower():
            out.append({"code": code, "name": name})
            if len(out) >= limit:
                break
    return out
