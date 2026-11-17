#!/usr/bin/env python3
"""ミニ・フィードリーダー（Python標準ライブラリのみ）

  python3 server.py      → http://127.0.0.1:8765 を開く

環境変数:
  PORT=8765               待ち受けるポート(127.0.0.1 だけで待ち受ける)
  REFRESH_MINUTES=30      自動更新の間隔
  FEED_DB=./feeds.db      データ(SQLite)の置き場所
  FEED_ALLOW_PRIVATE=1    内部・プライベートのアドレスへの取得を許す(既定は拒否。サーバーで公開するときは付けない)
"""
import html
import http.client
import ipaddress
import json
import os
import re
import sqlite3
import threading
import time
import hashlib
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BASE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("FEED_DB") or os.path.join(BASE, "feeds.db")
PORT = int(os.environ.get("PORT", 8765))
REFRESH_MINUTES = int(os.environ.get("REFRESH_MINUTES", 30))
UA = "Mozilla/5.0 (compatible; MiniFeedReader/1.0)"
MAX_BYTES = 10 * 1024 * 1024  # 1回の取得の上限(これを超えるものはフィードとして扱わない)
ALLOW_PRIVATE = os.environ.get("FEED_ALLOW_PRIVATE") == "1"

# ---------------------------------------------------------------- DB
db_lock = threading.Lock()
refresh_lock = threading.Lock()
CONN = sqlite3.connect(DB_PATH, check_same_thread=False)
CONN.row_factory = sqlite3.Row


def init_db():
    with db_lock:
        CONN.executescript("""
        CREATE TABLE IF NOT EXISTS feeds(
          id INTEGER PRIMARY KEY, url TEXT UNIQUE NOT NULL, title TEXT,
          site_url TEXT, last_fetched INTEGER, error TEXT);
        CREATE TABLE IF NOT EXISTS items(
          id INTEGER PRIMARY KEY,
          feed_id INTEGER NOT NULL REFERENCES feeds(id) ON DELETE CASCADE,
          guid TEXT NOT NULL, title TEXT, link TEXT, author TEXT, content TEXT,
          published INTEGER, read INTEGER NOT NULL DEFAULT 0,
          starred INTEGER NOT NULL DEFAULT 0, UNIQUE(feed_id, guid));
        CREATE INDEX IF NOT EXISTS idx_items_pub ON items(published DESC);
        CREATE TABLE IF NOT EXISTS categories(name TEXT PRIMARY KEY, position INTEGER NOT NULL DEFAULT 0);
        """)
        # 後から足した列(カテゴリと並び順)。古い feeds.db にもそのまま足す
        cols = {r[1] for r in CONN.execute("PRAGMA table_info(feeds)")}
        if "category" not in cols:
            CONN.execute("ALTER TABLE feeds ADD COLUMN category TEXT NOT NULL DEFAULT ''")
        if "position" not in cols:
            CONN.execute("ALTER TABLE feeds ADD COLUMN position INTEGER NOT NULL DEFAULT 0")
        CONN.commit()
        CONN.execute("PRAGMA foreign_keys=ON")


def q(sql, args=()):
    with db_lock:
        cur = CONN.execute(sql, args)
        CONN.commit()
        return [dict(r) for r in cur.fetchall()]


def ex(sql, args=()):
    with db_lock:
        cur = CONN.execute(sql, args)
        CONN.commit()
        return cur.lastrowid


# ---------------------------------------------------------------- 取得・解析
class NotAFeed(Exception):
    pass


# サーバー内部・プライベートのアドレスには接続しない(SSRF 対策)。
# 名前を引いた時点ではなく「実際に接続した相手」の IP で確かめるので、リダイレクト先や DNS の書き換えでもすり抜けない
def _check_peer(sock):
    if ALLOW_PRIVATE:
        return
    ip = ipaddress.ip_address(sock.getpeername()[0])
    if getattr(ip, "ipv4_mapped", None):
        ip = ip.ipv4_mapped
    if not ip.is_global:
        sock.close()
        raise ValueError(f"内部・プライベートのアドレス({ip})には接続しません")


class _SafeHTTPConnection(http.client.HTTPConnection):
    def connect(self):
        super().connect()
        _check_peer(self.sock)


class _SafeHTTPSConnection(http.client.HTTPSConnection):
    def connect(self):
        super().connect()
        _check_peer(self.sock)


class _SafeHTTPHandler(urllib.request.HTTPHandler):
    def http_open(self, req):
        return self.do_open(_SafeHTTPConnection, req)


class _SafeHTTPSHandler(urllib.request.HTTPSHandler):
    def https_open(self, req):
        return self.do_open(_SafeHTTPSConnection, req, context=self._context)


class _SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if urllib.parse.urlparse(newurl).scheme not in ("http", "https"):
            raise ValueError("http(s) 以外へのリダイレクトには従いません")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


# file:// や ftp:// の処理は入れない
_OPENER = urllib.request.OpenerDirector()
for _h in (urllib.request.ProxyHandler({}), urllib.request.UnknownHandler(), _SafeHTTPHandler(), _SafeHTTPSHandler(),
           urllib.request.HTTPDefaultErrorHandler(), _SafeRedirectHandler(), urllib.request.HTTPErrorProcessor()):
    _OPENER.add_handler(_h)


def fetch(url):
    if urllib.parse.urlparse(url).scheme not in ("http", "https"):
        raise ValueError("http(s) の URL を指定してください")
    req = urllib.request.Request(url, headers={
        "User-Agent": UA,
        "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, text/html;q=0.8, */*;q=0.5",
    })
    with _OPENER.open(req, timeout=20) as r:
        data = r.read(MAX_BYTES + 1)
        if len(data) > MAX_BYTES:
            raise ValueError(f"大きすぎます({MAX_BYTES // 1024 // 1024}MB 超)")
        return data, r.geturl()


def lname(el):
    return el.tag.rsplit("}", 1)[-1] if isinstance(el.tag, str) else ""


def child(el, *names):
    if el is None:
        return None
    for c in el:
        if lname(c) in names:
            return c
    return None


def text(el, *names):
    c = child(el, *names)
    return (c.text or "").strip() if c is not None else ""


def clean(s):
    return html.unescape(re.sub(r"<[^>]+>", "", s or "")).strip()


XML_ENTS = {"amp", "lt", "gt", "quot", "apos"}


def parse_xml(data):
    try:
        return ET.fromstring(data)
    except ET.ParseError:
        # 壊れたXML（&nbsp; や裸の & など）をできるだけ救済する
        s = data.decode("utf-8", "replace") if isinstance(data, bytes) else data
        s = re.sub(r"^\s*<\?xml[^>]*\?>", "", s)
        s = re.sub(r"&(?!#?\w+;)", "&amp;", s)
        s = re.sub(r"&([a-zA-Z]\w*);",
                   lambda m: m.group(0) if m.group(1) in XML_ENTS else html.unescape(m.group(0)), s)
        return ET.fromstring(s)


def parse_date(s):
    if not s:
        return None
    try:
        return int(parsedate_to_datetime(s).timestamp())
    except Exception:
        pass
    try:
        d = datetime.fromisoformat(s.strip().replace("Z", "+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return int(d.timestamp())
    except Exception:
        return None


def atom_link(el):
    first = ""
    for c in el:
        if lname(c) == "link":
            href = c.get("href", "")
            first = first or href
            if c.get("rel", "alternate") == "alternate":
                return href
    return first


def atom_content(c):
    if c is None:
        return ""
    if len(c):
        return (c.text or "") + "".join(ET.tostring(x, encoding="unicode") for x in c)
    return (c.text or "").strip()


def make_guid(*parts):
    return hashlib.sha1("|".join(parts).encode()).hexdigest()


def parse_feed(root):
    kind = lname(root)
    items = []
    if kind in ("rss", "RDF"):
        ch = child(root, "channel")
        if ch is None:
            raise NotAFeed()
        meta = {"title": clean(text(ch, "title")), "site_url": text(ch, "link")}
        for it in (ch if kind == "rss" else root):
            if lname(it) != "item":
                continue
            title = clean(text(it, "title"))
            link = text(it, "link")
            guid = text(it, "guid") or it.get("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about", "")
            if not link and guid.startswith("http"):
                link = guid
            items.append({
                "title": title, "link": link,
                "guid": guid or link or make_guid(title),
                "author": clean(text(it, "creator") or text(it, "author")),
                "content": text(it, "encoded") or text(it, "description"),
                "published": parse_date(text(it, "pubDate") or text(it, "date") or text(it, "published")),
            })
    elif kind == "feed":
        meta = {"title": clean(text(root, "title")), "site_url": atom_link(root)}
        for e in root:
            if lname(e) != "entry":
                continue
            title = clean(text(e, "title"))
            link = atom_link(e)
            items.append({
                "title": title, "link": link,
                "guid": text(e, "id") or link or make_guid(title),
                "author": clean(text(child(e, "author"), "name")),
                "content": atom_content(child(e, "content")) or atom_content(child(e, "summary")),
                "published": parse_date(text(e, "published") or text(e, "updated")),
            })
    else:
        raise NotAFeed()
    return meta, items


class LinkFinder(HTMLParser):
    """HTMLページから <link rel="alternate" type="application/rss+xml"> を探す"""

    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag != "link":
            return
        a = dict(attrs)
        rel = (a.get("rel") or "").lower()
        typ = (a.get("type") or "").lower()
        if "alternate" in rel and ("rss" in typ or "atom" in typ) and a.get("href"):
            self.links.append(a["href"])


def load(url, discover=True):
    data, final = fetch(url)
    try:
        meta, items = parse_feed(parse_xml(data))
        return url, meta, items
    except (ET.ParseError, NotAFeed):
        if not discover:
            raise ValueError("フィードとして解釈できませんでした")
        f = LinkFinder()
        f.feed(data.decode("utf-8", "replace"))
        if not f.links:
            raise ValueError("このページにフィードが見つかりませんでした")
        return load(urllib.parse.urljoin(final, f.links[0]), discover=False)


def store(feed_id, items):
    now = int(time.time())
    added = 0
    with db_lock:
        for i, it in enumerate(items):
            cur = CONN.execute(
                "INSERT OR IGNORE INTO items(feed_id,guid,title,link,author,content,published) VALUES(?,?,?,?,?,?,?)",
                (feed_id, it["guid"], it["title"], it["link"], it["author"], it["content"],
                 it["published"] or now - i))
            added += cur.rowcount
        CONN.commit()
    return added


def normalize_url(url):
    url = (url or "").strip()
    if url and not re.match(r"https?://", url, re.I):
        url = "https://" + url
    return url


def ensure_category(name):
    """カテゴリがなければ最後に足す(空文字 = 未分類は足さない)"""
    if name:
        ex("INSERT OR IGNORE INTO categories(name, position) VALUES(?, (SELECT COALESCE(MAX(position), 0) + 1 FROM categories))",
           (name,))


def add_feed(url, category=""):
    category = clean_category(category)
    url = normalize_url(url)
    if not url:
        raise ValueError("URLを入力してください")
    furl, meta, items = load(url)
    if q("SELECT id FROM feeds WHERE url=?", (furl,)):
        raise ValueError("既に登録されています")
    ensure_category(category)
    # 新しいフィードはそのカテゴリの最後に並べる
    fid = ex("INSERT INTO feeds(url,title,site_url,last_fetched,category,position) "
             "VALUES(?,?,?,?,?,(SELECT COALESCE(MAX(position), 0) + 1 FROM feeds))",
             (furl, meta["title"] or furl, meta["site_url"], int(time.time()), category))
    store(fid, items)
    return fid


def refresh_feed(f):
    now = int(time.time())
    try:
        _, meta, items = load(f["url"], discover=False)
        n = store(f["id"], items)
        ex("UPDATE feeds SET last_fetched=?, error=NULL, title=COALESCE(NULLIF(title,''),?) WHERE id=?",
           (now, meta["title"], f["id"]))
        return n
    except Exception as e:
        ex("UPDATE feeds SET last_fetched=?, error=? WHERE id=?", (now, str(e)[:200], f["id"]))
        return 0


def refresh_all(feed_id=None):
    with refresh_lock:
        feeds = q("SELECT id,url FROM feeds" + (" WHERE id=?" if feed_id else ""),
                  (feed_id,) if feed_id else ())
        with ThreadPoolExecutor(8) as pool:
            return sum(pool.map(refresh_feed, feeds))


def background_refresher():
    while True:
        try:
            refresh_all()
        except Exception as e:
            print("refresh error:", e)
        time.sleep(REFRESH_MINUTES * 60)


# ---------------------------------------------------------------- OPML
def import_opml(src):
    """OPML を読み込む。フィードを囲む outline(xmlUrl のないもの)の名前をカテゴリにする"""
    root = parse_xml(src.encode() if isinstance(src, str) else src)
    found = []

    def walk(el, cat):
        for o in el:
            if lname(o) != "outline":
                walk(o, cat)
            elif o.get("xmlUrl"):
                found.append((o.get("xmlUrl"), cat))
            else:
                walk(o, (o.get("text") or o.get("title") or "").strip() or cat)

    walk(root, "")
    for _, cat in found:  # カテゴリは OPML に出てきた順に足す
        ensure_category(clean_category(cat))

    def one(uc):
        try:
            add_feed(*uc)
            return True
        except Exception:
            return False

    with ThreadPoolExecutor(6) as pool:
        results = list(pool.map(one, found))
    return {"added": sum(results), "skipped": len(results) - sum(results)}


def export_opml():
    """カテゴリは outline で囲んで書き出す(並び順もそのまま)"""
    def line(r, indent):
        return (f'{indent}<outline type="rss" text="{html.escape(r["title"] or "")}" title="{html.escape(r["title"] or "")}" '
                f'xmlUrl="{html.escape(r["url"])}" htmlUrl="{html.escape(r["site_url"] or "")}"/>')
    rows = list_feeds()
    out = []
    for c in list_categories():
        inner = [line(r, "      ") for r in rows if r["category"] == c["name"]]
        out.append(f'    <outline text="{html.escape(c["name"])}" title="{html.escape(c["name"])}">' +
                   ("\n" + "\n".join(inner) + "\n    " if inner else "") + "</outline>")
    out += [line(r, "    ") for r in rows if not r["category"]]
    return ('<?xml version="1.0" encoding="UTF-8"?>\n<opml version="2.0">\n  <head><title>Feeds</title></head>\n'
            "  <body>\n" + "\n".join(out) + "\n  </body>\n</opml>\n")


# ---------------------------------------------------------------- カテゴリと並び順
def clean_category(name):
    name = re.sub(r"\s+", " ", str(name or "")).strip()
    if len(name) > 40:
        raise ValueError("カテゴリ名は40文字までです")
    return name


def list_categories():
    return q("SELECT name, position FROM categories ORDER BY position, name")


def list_feeds():
    """カテゴリの順 → カテゴリ内の順。未分類は最後"""
    return q("""SELECT f.*, (SELECT COUNT(*) FROM items i WHERE i.feed_id=f.id AND i.read=0) AS unread
                FROM feeds f LEFT JOIN categories c ON c.name = f.category
                ORDER BY f.category = '', c.position, f.position, lower(f.title)""")


def save_layout(b):
    """編集モードの結果をまとめて保存する。categories: カテゴリ名の並び、feeds: [{id, category}] の並び"""
    cats = [clean_category(c) for c in b.get("categories") or []]
    if "" in cats or len(set(cats)) != len(cats):
        raise ValueError("カテゴリ名が空か、重複しています")
    feeds = b.get("feeds") or []
    with db_lock:
        try:
            CONN.execute("DELETE FROM categories")
            CONN.executemany("INSERT INTO categories(name, position) VALUES(?, ?)", [(c, i) for i, c in enumerate(cats)])
            for i, f in enumerate(feeds):
                cat = clean_category(f.get("category"))
                if cat and cat not in cats:
                    raise ValueError(f"存在しないカテゴリです: {cat}")
                CONN.execute("UPDATE feeds SET category=?, position=? WHERE id=?", (cat, i, int(f["id"])))
            CONN.commit()
        except Exception:
            CONN.rollback()
            raise


# ---------------------------------------------------------------- HTTP
def excerpt(s, n=140):
    t = re.sub(r"\s+", " ", clean(s))
    return t[:n] + ("…" if len(t) > n else "")


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def send(self, body, ctype="application/json; charset=utf-8", code=200, extra=None):
        if not isinstance(body, bytes):
            body = (json.dumps(body, ensure_ascii=False) if "json" in ctype else body).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def ok_host(self):
        # DNSリバインディング対策：ローカルホスト以外からのリクエストを拒否
        host = (self.headers.get("Host") or "").rsplit(":", 1)[0]
        return host in ("127.0.0.1", "localhost", "[::1]")

    def body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n) or b"{}")

    def route(self, method):
        if not self.ok_host():
            return self.send({"error": "forbidden"}, code=403)
        if method != "GET" and not (self.headers.get("Content-Type") or "").startswith("application/json"):
            # 他サイトからのフォーム送信（CSRF）を防ぐ
            return self.send({"error": "JSON only"}, code=415)
        u = urllib.parse.urlparse(self.path)
        p, qs = u.path, dict(urllib.parse.parse_qsl(u.query))
        try:
            return self.dispatch(method, p, qs)
        except Exception as e:
            return self.send({"error": str(e)}, code=400)

    def dispatch(self, method, p, qs):
        m = re.fullmatch(r"/api/(feeds|items)/(\d+)", p)
        if method == "GET":
            if p in ("/", "/index.html"):
                with open(os.path.join(BASE, "index.html"), "rb") as f:
                    return self.send(f.read(), "text/html; charset=utf-8")
            if p == "/api/feeds":
                return self.send(list_feeds())
            if p == "/api/categories":
                return self.send(list_categories())
            if p == "/api/items":
                where, args = [], []
                feed = qs.get("feed", "all")
                if feed == "starred":
                    where.append("i.starred=1")
                elif feed.isdigit():
                    where.append("i.feed_id=?")
                    args.append(int(feed))
                elif feed.startswith("cat:"):
                    where.append("f.category=?")
                    args.append(feed[4:])
                if qs.get("unread") == "1":
                    where.append("i.read=0")
                sql = ("SELECT i.id,i.feed_id,i.title,i.link,i.author,i.published,i.read,i.starred,"
                       "substr(i.content,1,1500) AS content,f.title AS feed_title FROM items i "
                       "JOIN feeds f ON f.id=i.feed_id")
                if where:
                    sql += " WHERE " + " AND ".join(where)
                sql += " ORDER BY i.published DESC LIMIT ?"
                args.append(min(int(qs.get("limit", 300)), 1000))
                rows = q(sql, args)
                for r in rows:
                    r["excerpt"] = excerpt(r.pop("content"))
                return self.send(rows)
            if m and m.group(1) == "items":
                rows = q("SELECT i.*, f.title AS feed_title FROM items i JOIN feeds f ON f.id=i.feed_id WHERE i.id=?",
                         (int(m.group(2)),))
                return self.send(rows[0] if rows else {"error": "not found"}, code=200 if rows else 404)
            if p == "/api/opml":
                return self.send(export_opml(), "text/x-opml; charset=utf-8",
                                 extra={"Content-Disposition": 'attachment; filename="feeds.opml"'})
        elif method == "POST":
            b = self.body()
            if p == "/api/feeds":
                fid = add_feed(b.get("url"))
                return self.send({"id": fid})
            if p == "/api/refresh":
                return self.send({"added": refresh_all(b.get("feed") if str(b.get("feed", "")).isdigit() else None)})
            if p == "/api/mark-all-read":
                feed = str(b.get("feed", "all"))
                if feed.isdigit():
                    ex("UPDATE items SET read=1 WHERE feed_id=?", (int(feed),))
                elif feed == "starred":
                    ex("UPDATE items SET read=1 WHERE starred=1")
                elif feed.startswith("cat:"):
                    ex("UPDATE items SET read=1 WHERE feed_id IN (SELECT id FROM feeds WHERE category=?)", (feed[4:],))
                else:
                    ex("UPDATE items SET read=1")
                return self.send({"ok": True})
            if p == "/api/opml":
                return self.send(import_opml(b.get("text", "")))
            if p == "/api/layout":
                save_layout(b)
                return self.send({"ok": True})
            if m and m.group(1) == "items":
                for k in ("read", "starred"):
                    if k in b:
                        ex(f"UPDATE items SET {k}=? WHERE id=?", (1 if b[k] else 0, int(m.group(2))))
                return self.send({"ok": True})
            if m and m.group(1) == "feeds":
                if b.get("title"):
                    ex("UPDATE feeds SET title=? WHERE id=?", (b["title"].strip(), int(m.group(2))))
                return self.send({"ok": True})
        elif method == "DELETE":
            if m and m.group(1) == "feeds":
                fid = int(m.group(2))
                ex("DELETE FROM items WHERE feed_id=?", (fid,))
                ex("DELETE FROM feeds WHERE id=?", (fid,))
                return self.send({"ok": True})
        return self.send({"error": "not found"}, code=404)

    def do_GET(self):
        self.route("GET")

    def do_POST(self):
        self.route("POST")

    def do_DELETE(self):
        self.route("DELETE")


if __name__ == "__main__":
    init_db()
    threading.Thread(target=background_refresher, daemon=True).start()
    print(f"Feed reader: http://127.0.0.1:{PORT}  （{REFRESH_MINUTES}分ごとに自動更新）")
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
