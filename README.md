# Feed Reader

Python 標準ライブラリだけで動くローカル用フィードリーダー。

```bash
python3 server.py
```

→ http://127.0.0.1:8765 を開く

- RSS 2.0 / RSS 1.0 (RDF) / Atom に対応。サイトの URL を入れるとフィードを自動検出
- 30 分ごとに自動更新（`REFRESH_MINUTES=10 python3 server.py` で変更可）
- 未読管理・スター・OPML の読込/書出(カテゴリは OPML の outline のまとまりとして読み書きする)
- カテゴリ: メニューでカテゴリごとにまとめて表示。見出しを押すとカテゴリの記事をまとめて読める。▸ で折りたたみ(この端末に保存)
- 編集モード(メニュー上の「編集」): カテゴリの追加・名前変更・削除、フィードの名前変更・カテゴリの付け替え・削除、カテゴリとフィードの並べ替え(▲▼)。削除は2回押しで確定
- キー操作: `j`/`k` 次/前、`v` 元記事、`s` スター、`m` 未読切替、`r` 更新
- データは同じフォルダの `feeds.db`（SQLite）に保存
- 記事本文はスクリプト無効のサンドボックス iframe で表示

## サーバーでの公開(https://dash.tmd9.xyz/feed/)

ConoHa サーバーで常駐させ、ダッシュボードと同じ nginx の下(`/feed/`)で公開している。パスワード認証・HTTPS はダッシュボードと共通。

- 常駐: deploy ユーザーの systemd(ユーザー単位)`feed-reader.service`。落ちたら5秒後に再起動する。設定は `shellscripts/server/feed-reader.service`
- データ: サーバーの `~/.local/share/feed-reader/feeds.db`(リポジトリの外)
- 更新: main に push すると GitHub Actions(`.github/workflows/deploy.yml`)がサーバーに合図し、サーバーが GitHub から取り直して再起動する(`shellscripts/server/deploy-pages feed-reader`)
- 画面と API は相対パス(`api/...`)で呼ぶので、`/feed/` の下でもそのまま動く。nginx は Host を `127.0.0.1` にして渡す(DNS リバインディング対策をそのまま使う)
- 取得先が内部・プライベートのアドレス(127.0.0.1、169.254.169.254 など)なら、リダイレクト先も含めて接続しない。1回の取得は 10MB まで
- 手元で動かすときは今までどおり `python3 server.py`(内部のアドレスのフィードを読みたいときだけ `FEED_ALLOW_PRIVATE=1`)
