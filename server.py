# -*- coding: utf-8 -*-
"""
手绘K线 · 本地服务（形态照搬 chanlun-DA.v1.0：stdlib ThreadingHTTPServer，零框架）
启动：python server.py  →  自动打开浏览器 http://127.0.0.1:8791/
API：
  GET  /api/kline?symbol=600519&period=1d        真实K线（分钟/日/周/月）
  GET  /api/search?q=茅台                        股票搜索（新浪全A池，7天缓存）
  GET  /api/name?code=600519                     代码→名称（腾讯报价）
  GET  /api/drawings?symbol=600519               读取画线
  POST /api/drawings?symbol=600519  {items:[..]} 整体保存画线
"""
import json
import os
import sys
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import data_source
import drawings_store

PORT = 8791
ROOT = os.path.dirname(os.path.abspath(__file__))
STATIC = os.path.join(ROOT, "static")

MIME = {".html": "text/html; charset=utf-8", ".js": "application/javascript; charset=utf-8",
        ".css": "text/css; charset=utf-8", ".png": "image/png", ".ico": "image/x-icon"}

store = data_source.KlineStore()


def _log(msg):
    """stdout 在 pythonw 下是 None，print 会崩；同时落一份日志到 data_store/server.log"""
    line = time.strftime("%Y-%m-%d %H:%M:%S ") + msg
    try:
        print(line)
    except Exception:  # noqa: BLE001
        pass
    try:
        os.makedirs(data_source.STORE, exist_ok=True)
        with open(os.path.join(data_source.STORE, "server.log"), "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:  # noqa: BLE001
        pass


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # 安静模式
        pass

    # ---------- 工具 ----------
    def _json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _file(self, name):
        path = os.path.join(STATIC, name)
        if not os.path.isfile(path):
            self._json({"error": "not found"}, 404)
            return
        with open(path, "rb") as f:
            body = f.read()
        ext = os.path.splitext(name)[1]
        self.send_response(200)
        self.send_header("Content-Type", MIME.get(ext, "application/octet-stream"))
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    # ---------- 路由 ----------
    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        try:
            if u.path in ("/", "/index.html"):
                self._file("index.html")
            elif u.path == "/rough.js":
                self._file("rough.js")
            elif u.path == "/api/kline":
                sym = (q.get("symbol") or [""])[0]
                period = (q.get("period") or ["1d"])[0]
                bars = store.get(sym, period)
                name = store.name_of(sym)
                if not bars:
                    self._json({"error": f"无数据：{sym} {period}"}, 404)
                    return
                self._json({"code": data_source.normalize_symbol(sym)[2:],
                            "name": name or sym, "period": period, "bars": bars})
            elif u.path == "/api/search":
                kw = (q.get("q") or [""])[0]
                res = data_source.search(kw) if kw else []
                if res is None:
                    self._json({"ready": False, "items": []})
                else:
                    self._json({"ready": True, "items": res})
            elif u.path == "/api/name":
                code = (q.get("code") or [""])[0]
                self._json({"code": code, "name": store.name_of(code)})
            elif u.path == "/api/drawings":
                sym = (q.get("symbol") or [""])[0]
                code = data_source.normalize_symbol(sym)
                self._json(drawings_store.load(code[2:] if code else sym))
            elif u.path == "/api/ping":
                self._json({"ok": True})
            else:
                self._json({"error": "not found"}, 404)
        except Exception as e:  # noqa: BLE001
            self._json({"error": str(e)}, 500)

    def do_POST(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        try:
            if u.path == "/api/drawings":
                sym = (q.get("symbol") or [""])[0]
                code = data_source.normalize_symbol(sym)
                code = code[2:] if code else sym
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length) or b"{}")
                items = body.get("items")
                if not isinstance(items, list):
                    self._json({"error": "items must be a list"}, 400)
                    return
                self._json(drawings_store.save(code, items))
            else:
                self._json({"error": "not found"}, 404)
        except Exception as e:  # noqa: BLE001
            self._json({"error": str(e)}, 500)


def main():
    # 端口被占用 → 服务已在跑，直接开浏览器即可
    try:
        srv = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    except OSError:
        _log("端口 %d 已被占用：服务已在运行，直接打开页面" % PORT)
        webbrowser.open(f"http://127.0.0.1:{PORT}/")
        return
    _log(f"手绘K线服务启动 http://127.0.0.1:{PORT}/")
    data_source.build_pool_async()          # 后台构建A股搜索池（首次约1分钟）
    webbrowser.open(f"http://127.0.0.1:{PORT}/")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    except Exception as e:  # noqa: BLE001
        _log("服务异常退出：" + str(e))


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:  # noqa: BLE001  # pythonw 下 stdout 为 None
        pass
    main()
