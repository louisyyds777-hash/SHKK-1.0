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
加密货币（USDT永续现货对）：Binance主源(klines 1000根/页，免密钥) + OKX备源(market/candles 300根/页)，
  7×24交易、日线按UTC收盘，未收盘剔除不能沿用A股的北京时间日历判断
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
UTC = timezone.utc
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

_sess = requests.Session()          # 连接复用：翻页/搜索池省掉每次TCP+TLS握手
_sess.headers.update(UA)

BASE_MKLINE = "https://ifzq.gtimg.cn/appstock/app/kline/mkline"
BASE_FQKLINE = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
BASE_QUOTE = "https://qt.gtimg.cn/q="
SINA_KLINE = "https://quotes.sina.cn/cn/api/jsonp_v2.php/var%20_d=/CN_MarketDataService.getKLineData"
SINA_LIST = ("https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/"
             "Market_Center.getHQNodeData")

BINANCE_HOSTS = ("https://api.binance.com", "https://data-api.binance.vision")   # 后者为纯行情镜像
BINANCE_TF = {"1m": "1m", "5m": "5m", "30m": "30m", "60m": "1h", "1d": "1d", "1w": "1w", "1M": "1M"}
OKX_TF = {"1m": "1m", "5m": "5m", "30m": "30m", "60m": "1H", "1d": "1D", "1w": "1W", "1M": "1M"}
CRYPTO_MS = {"1m": 60_000, "5m": 5 * 60_000, "30m": 30 * 60_000, "60m": 60 * 60_000,
             "1d": 86_400_000, "1w": 604_800_000}

CRYPTO = {                          # 交易品种表：现货USDT对 → 中文名
    "BTCUSDT": "比特币",
    "ETHUSDT": "以太坊",
    "SOLUSDT": "Solana",
    "BNBUSDT": "BNB",
    "DOGEUSDT": "狗狗币",
    "UNIUSDT": "Uniswap",
    "XRPUSDT": "瑞波币",
}
_CRYPTO_ALIAS = {}
for _c, _n in CRYPTO.items():
    _base = _c[:-4]
    for _a in (_c, _base, _base + "-USDT", _base + "/USDT", _n):
        _CRYPTO_ALIAS[_a.lower()] = _c

TF_MS = {"1m": 60_000, "5m": 5 * 60_000, "30m": 30 * 60_000, "60m": 60 * 60_000}
TF_MK = {"1m": "m1", "5m": "m5", "30m": "m30", "60m": "m60"}
PERIODS = ("1m", "5m", "30m", "60m", "1d", "1w", "1M")

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


def normalize_crypto(code):
    """BTC / btcusdt / BTC-USDT / 比特币 → 'BTCUSDT'，非加密输入返回 None"""
    c = str(code).strip().lower().replace(" ", "")
    return _CRYPTO_ALIAS.get(c)


def short_code(code):
    """任意输入 → 展示/存储用短代码：A股6位数字，加密'BTCUSDT'，无法识别原样返回"""
    c = normalize_crypto(code)
    if c:
        return c
    s = normalize_symbol(code)
    return s[2:] if s else str(code).strip()


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


# ---------------- 加密货币源 ----------------

def fetch_crypto_binance(sym, tf, count=1000, end_ms=None):
    """Binance现货K线，升序 [ts_open, o, h, l, c, v(币)]。末根可能是未收盘bar（由drop剔除）"""
    q = f"symbol={sym}&interval={BINANCE_TF[tf]}&limit={min(count, 1000)}"
    if end_ms:
        q += f"&endTime={end_ms}"
    last = None
    for host in BINANCE_HOSTS:
        try:
            data = json.loads(http_text(f"{host}/api/v3/klines?{q}"))
            return [[int(r[0]), float(r[1]), float(r[2]), float(r[3]),
                     float(r[4]), float(r[5])] for r in data]
        except Exception as e:  # noqa: BLE001
            last = e
    raise last


def fetch_crypto_okx(sym, tf, count=300):
    """OKX备源：降序返回，confirm=0的未收盘行直接丢弃"""
    inst = sym[:-4] + "-USDT"
    url = (f"https://www.okx.com/api/v5/market/candles?instId={inst}"
           f"&bar={OKX_TF[tf]}&limit={min(count, 300)}")
    data = json.loads(http_text(url))
    if data.get("code") != "0":
        raise ValueError("okx api code " + str(data.get("code")))
    out = [[int(r[0]), float(r[1]), float(r[2]), float(r[3]),
            float(r[4]), float(r[5])] for r in data["data"] if r[8] != "0"]
    out.reverse()
    return out


def fetch_crypto_deep(sym, tf, max_pages=6):
    """深挖翻页：向前翻到历史翻完或达页数上限（仅在冷缓存时触发，与缓存重叠的bar由merge按ts去重）。
    日线全史≤4页；分钟6页≈6000根（全史太大，只取近期窗口）"""
    out = []
    end = None
    for _ in range(max_pages):
        rows = fetch_crypto_binance(sym, tf, 1000, end)
        if not rows:
            break
        out = rows + out
        if len(rows) < 1000:
            break
        end = rows[0][0] - 1
        time.sleep(0.15)
    return out


def drop_partial_crypto(bars, tf):
    """7×24市场未收盘剔除：除月线外都能用 固定时长 判断；月线按UTC自然月收尾"""
    now = _now_ms()
    if tf in CRYPTO_MS:
        dur = CRYPTO_MS[tf]
        return [b for b in bars if b[0] + dur <= now]
    out = []
    for b in bars:
        d = datetime.fromtimestamp(b[0] / 1000, UTC)
        y, m = (d.year + 1, 1) if d.month == 12 else (d.year, d.month + 1)
        month_end = int(datetime(y, m, 1, tzinfo=UTC).timestamp() * 1000)
        if month_end <= now:
            out.append(b)
    return out


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
        self.src_info = {}             # (sym, tf) -> {"src","ms","at"} 最近一次成功上游打点

    def _mark_src(self, sym, tf, src, t0):
        with self.lock:
            self.src_info[(sym, tf)] = {"src": src, "ms": int((time.time() - t0) * 1000),
                                        "at": int(time.time())}

    def _path(self, sym, tf):
        # Windows文件名不区分大小写：1m(分钟)与1M(月)会撞同一个文件，落盘名必须错开
        file_tf = "1min" if tf == "1m" else tf
        return os.path.join(STORE, "kline", sym, file_tf + ".json")

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

    def _fetch_crypto(self, sym, tf, deep):
        """加密品种实际拉数：Binance主源（快路径1页1000根；深挖翻页全史），失败落OKX备源"""
        with self.lock:
            bars = self.mem.get((sym, tf)) or []
        t0 = time.time()
        try:
            if deep:
                rows = fetch_crypto_deep(sym, tf)
            else:
                rows = fetch_crypto_binance(sym, tf, 1000)
            self._mark_src(sym, tf, "Binance", t0)
        except Exception:  # noqa: BLE001
            t0 = time.time()
            rows = fetch_crypto_okx(sym, tf)
            self._mark_src(sym, tf, "OKX备", t0)
        new = merge_bars(bars, rows)
        with self.lock:
            self.mem[(sym, tf)] = new
            self._save(sym, tf, new)
        return new

    def _fetch_upstream(self, sym, tf, deep):
        """实际拉数并合并入缓存（网络在锁外，不阻塞其他 symbol/period 请求）。
        deep=False 快路径：只发1个请求（分钟=腾讯800根，日=最近一页，周月=直取）。
        deep=True 深挖：日K补全史翻页、分钟新浪打底到~1970根、周月失败回退日线重采样。"""
        if sym in CRYPTO:
            return self._fetch_crypto(sym, tf, deep)
        with self.lock:
            bars = self.mem.get((sym, tf)) or []
        if tf in TF_MS:
            t0 = time.time()
            rows = fetch_mkline_tx(sym, tf, 800)
            self._mark_src(sym, tf, "腾讯", t0)
            if deep and len(bars) < 1900:            # 浅窗 → 新浪打底
                try:
                    rows = merge_bars(fetch_mkline_sina(sym, tf, 1970), rows)
                except Exception:  # noqa: BLE001
                    pass
            new = merge_bars(bars, rows)
        elif tf == "1d":
            t0 = time.time()
            if deep:
                before = ""
                if bars:
                    first = datetime.fromtimestamp(bars[0][0] / 1000, BJ)
                    before = (first - timedelta(days=1)).strftime("%Y-%m-%d")
                new = merge_bars(bars, fetch_daily_full(sym, before))
            else:
                new = merge_bars(bars, fetch_kline_tx(sym, "day"))
            self._mark_src(sym, tf, "腾讯", t0)
        else:                                        # 1w / 1M：腾讯周/月线直取，失败回退日线重采样
            api_tf = {"1w": "week", "1M": "month"}[tf]
            t0 = time.time()
            try:
                rows = fetch_kline_tx(sym, api_tf, count=640)
                if rows:
                    new = merge_bars(bars, rows)
                else:
                    raise ValueError("empty " + api_tf)
                self._mark_src(sym, tf, "腾讯", t0)
            except Exception:  # noqa: BLE001
                if deep or not bars:
                    daily = self._raw(sym, "1d")
                    new = resample_daily(daily, tf)
                    self._mark_src(sym, tf, "日线重采样", t0)
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
        if tf not in PERIODS:
            return []
        csym = normalize_crypto(code)
        if csym:
            return drop_partial_crypto(self._raw(csym, tf), tf)
        sym = normalize_symbol(code)
        if not sym:
            return []
        return drop_partial(self._raw(sym, tf), tf)

    def meta_of(self, code, tf):
        """当前品种最近一次成功上游的来源信息（进程内存级；空=纯缓存未打点）"""
        sym = normalize_crypto(code) or normalize_symbol(code)
        if not sym:
            return {}
        with self.lock:
            return dict(self.src_info.get((sym, tf)) or {})

    def name_of(self, code):
        csym = normalize_crypto(code)
        if csym:
            return CRYPTO[csym]
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


def crypto_search(q, limit=20):
    """加密品种内置搜索：代码/去USDT写法/中文名 模糊匹配，不依赖网络"""
    ql = str(q).strip().lower()
    if not ql:
        return []
    out = []
    for code, name in CRYPTO.items():
        base = code[:-4].lower()
        if ql in code.lower() or ql in base or ql in name.lower():
            out.append({"code": code, "name": name})
            if len(out) >= limit:
                break
    return out


def search(q, limit=20):
    hits = crypto_search(q, limit)
    pool = search_pool()
    if not pool:                       # A股池未就绪：加密命中也能出结果（ready）
        return hits if hits else None
    ql = q.strip().lower()
    out = list(hits)
    for code, name in pool.items():
        if len(out) >= limit:
            break
        if ql in code or ql in name.lower():
            out.append({"code": code, "name": name})
    return out


# ---------------- 数据源健康探测 ----------------

_status_cache = {"at": 0.0, "data": None}
_status_lock = threading.Lock()


def _probe(url, enc="utf-8"):
    """轻量连通性探测：4秒超时单次请求，不带重试（区别于拉数路径）"""
    t0 = time.time()
    try:
        r = _sess.get(url, timeout=4)
        r.encoding = enc
        ok = r.status_code == 200 and len(r.text) > 10
    except Exception:  # noqa: BLE001
        ok = False
    return {"ok": bool(ok), "ms": int((time.time() - t0) * 1000)}


def source_status():
    """四个上游的连通性体检：并发探测、30秒内存缓存，不阻塞K线请求"""
    now = time.time()
    with _status_lock:
        if _status_cache["data"] and now - _status_cache["at"] < 30:
            return _status_cache["data"]
    jobs = {
        "A股·腾讯": lambda: _probe(BASE_QUOTE + "sh600519", "gbk"),
        "A股·新浪": lambda: _probe(
            SINA_LIST + "?page=1&num=1&sort=symbol&asc=1&node=hs_a", "gbk"),
        "加密·Binance": lambda: _probe(
            BINANCE_HOSTS[0] + "/api/v3/klines?symbol=BTCUSDT&interval=1d&limit=1"),
        "加密·OKX": lambda: _probe(
            "https://www.okx.com/api/v5/market/candles?instId=BTC-USDT&bar=1D&limit=1"),
    }
    out = {}

    def run(name, fn):
        out[name] = fn()

    ths = [threading.Thread(target=run, args=(n, f), daemon=True) for n, f in jobs.items()]
    for t in ths:
        t.start()
    for t in ths:
        t.join(5)
    data = {"at": int(now), "sources": out}
    with _status_lock:
        _status_cache.update(at=now, data=data)
    return data
