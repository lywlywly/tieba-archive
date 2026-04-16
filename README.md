# tieba-archive

A Baidu Tieba archiver that stores forums, threads, posts, comments, users, and images into a local database. For browsing archived data, use [`tieba-viewer`](https://github.com/lywlywly/tieba-viewer). For an all-in-one Docker Compose setup (`archive + viewer + PostgreSQL`), use [`tieba-stack`](https://github.com/lywlywly/tieba-stack).

## Tech Stack

- Python 3.14
- SQLModel (SQLite/PostgreSQL)
- Built on Baidu Tieba API via [`aiotieba`](https://github.com/lumina37/aiotieba)

## Quick Start

```bash
uv sync
uv run python main.py
```

## Docker

Build:

```bash
docker build -t tieba-archive .
```

Run:

```bash
docker run --rm --name tieba-archive \
  -e DB_MODE=postgresql \
  -e SQLITE_PATH=tieba.db \
  -e POSTGRES_URL='postgresql+psycopg://<db_user>:<db_password>@host.docker.internal:5432/<db_name>' \
  -e CONCURRENCY_LIMIT=2 \
  -e OUTPUT_DIR=output \
  -e BDUSS='<your_bduss>' \
  -e FORUM_NAMES='["forum_name_1","forum_name_2","forum_name_3"]' \
  -e PAGES=5 \
  -e SLEEP=300 \
  -v "$(pwd)/blacklist.txt:/app/blacklist.txt:ro" \
  -v "$(pwd)/output:/app/output" \
  tieba-archive
```

## Config

The app reads `config.yaml` (optional) and all keys can be overridden by env vars.

See [`config.example.yaml`](config.example.yaml) for a commented sample.

## Notes

- `blacklist.txt` is optional and should contain one thread ID (`tid`) per line to skip crawling. If missing, blacklist is empty.
- Downloaded images are sharded by filename prefix under `output/by_prefix/...`.
