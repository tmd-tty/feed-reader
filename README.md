# Feed Reader

Python 標準ライブラリだけで動くローカル用フィードリーダー。

```bash
python3 server.py
```

→ http://127.0.0.1:8765 を開く

- RSS 2.0 / RSS 1.0 (RDF) / Atom に対応。サイトの URL を入れるとフィードを自動検出
- 30 分ごとに自動更新（`REFRESH_MINUTES=10 python3 server.py` で変更可）
- 未読管理・スター・OPML の読込/書出
- キー操作: `j`/`k` 次/前、`v` 元記事、`s` スター、`m` 未読切替、`r` 更新
- データは同じフォルダの `feeds.db`（SQLite）に保存
- 記事本文はスクリプト無効のサンドボックス iframe で表示
