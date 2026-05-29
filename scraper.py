#!/usr/bin/env python3
"""
Early-Leopard-8351 Reddit scraper
- Comments: Arctic Shift API (full history, no auth needed)
- Thread context: Reddit public JSON (no cookies needed)
- Upserts to Supabase

Usage:
    python scraper.py          # incremental (only new comments)
    python scraper.py --full   # full backfill (all history)
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from supabase import create_client, Client

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv()

SUPABASE_URL   = os.environ["SUPABASE_URL"]
SUPABASE_KEY   = os.environ["SUPABASE_KEY"]
REDDIT_USERNAME = "Early-Leopard-8351"
USER_AGENT      = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
ARCTIC_BASE     = "https://arctic-shift.photon-reddit.com"
REQUEST_DELAY   = 2.0   # seconds between requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Arctic Shift — comments
# ---------------------------------------------------------------------------

ARCTIC_FIELDS = "id,author,body,created_utc,subreddit,score,parent_id,link_id"


def iter_arctic_comments(known_ids: set, full: bool, session: requests.Session):
    """
    Yields raw comment dicts from Arctic Shift, paginating via created_utc asc.
    In incremental mode, stops as soon as a full page is entirely known.
    """
    after = None

    while True:
        params = {
            "author": REDDIT_USERNAME,
            "limit":  100,
            "sort":   "asc",
            "fields": ARCTIC_FIELDS,
        }
        if after:
            params["after"] = after

        log.info("Arctic Shift: fetching comments (after=%s)", after or "start")
        try:
            resp = session.get(f"{ARCTIC_BASE}/api/comments/search", params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json().get("data", [])
        except Exception as e:
            log.warning("Arctic Shift request failed: %s", e)
            break

        if not data:
            log.info("Arctic Shift: no more results.")
            break

        new_count = 0
        for item in data:
            if item.get("id") not in known_ids:
                yield item
                new_count += 1

        log.info("  got %d items, %d new.", len(data), new_count)

        # In incremental mode: stop if entire page was already known
        if not full and new_count == 0:
            log.info("Arctic Shift: all items on page already known — stopping.")
            break

        if len(data) < 100:
            log.info("Arctic Shift: reached end of history.")
            break

        # Advance cursor: use last item's created_utc + 1s
        after = str(int(data[-1].get("created_utc", 0)) + 1)
        time.sleep(REQUEST_DELAY)


def map_comment(item: dict) -> dict:
    # Reconstruct permalink from parts (Arctic Shift doesn't return it)
    link_id   = item.get("link_id", "").removeprefix("t3_")
    subreddit = item.get("subreddit", "")
    cid       = item.get("id", "")
    permalink = f"/r/{subreddit}/comments/{link_id}/_/{cid}/" if link_id else None

    return {
        "id":          cid,
        "author":      item.get("author"),
        "body":        item.get("body"),
        "created_utc": int(item["created_utc"]) if item.get("created_utc") is not None else None,
        "subreddit":   subreddit,
        "permalink":   permalink,
        "score":       item.get("score"),
        "parent_id":   item.get("parent_id"),
        "link_id":     item.get("link_id"),
        "captured_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Reddit JSON — thread context (no cookies needed)
# ---------------------------------------------------------------------------

def fetch_thread(subreddit: str, post_id: str, session: requests.Session) -> tuple[dict | None, dict]:
    """
    Returns (post_data, comments_by_id) using Arctic Shift APIs.
    - Post data: /api/posts/ids
    - Comments: /api/comments/tree
    """
    post_data: dict | None = None
    comments_by_id: dict[str, dict] = {}

    # Fetch post metadata
    try:
        resp = session.get(f"{ARCTIC_BASE}/api/posts/ids", params={"ids": post_id}, timeout=15)
        resp.raise_for_status()
        posts = resp.json().get("data", [])
        if posts:
            post_data = posts[0]
    except Exception as e:
        log.warning("Arctic Shift: failed to fetch post %s: %s", post_id, e)

    # Fetch comment tree
    try:
        resp = session.get(
            f"{ARCTIC_BASE}/api/comments/tree",
            params={"link_id": f"t3_{post_id}", "limit": 9999},
            timeout=30,
        )
        resp.raise_for_status()
        items = resp.json().get("data", [])
        for item in items:
            if isinstance(item, dict) and item.get("id") and item.get("kind") != "more":
                comments_by_id[item["id"]] = item
    except Exception as e:
        log.warning("Arctic Shift: failed to fetch tree %s: %s", post_id, e)

    return post_data, comments_by_id


def fetch_all_context(
    comments: list[dict],
    known_post_ids: set,
    known_ctx_comment_ids: set,
    full: bool,
    session: requests.Session,
) -> tuple[list[dict], list[dict]]:
    """
    For each unique post referenced by comments, fetch thread context.
    Returns (post_context_rows, comment_context_rows).
    """
    post_map: dict[str, list[dict]] = {}
    for c in comments:
        post_id = c.get("link_id", "").removeprefix("t3_")
        if post_id:
            post_map.setdefault(post_id, []).append(c)

    posts_to_fetch = set(post_map.keys()) if full else {p for p in post_map if p not in known_post_ids}
    log.info("Fetching context for %d unique posts...", len(posts_to_fetch))

    post_ctx_rows:     list[dict] = []
    comment_ctx_rows:  list[dict] = []
    now = datetime.now(timezone.utc).isoformat()

    for post_id in posts_to_fetch:
        el_comments = post_map[post_id]
        subreddit   = el_comments[0].get("subreddit", "")

        log.info("  Thread: r/%s/%s", subreddit, post_id)
        post_data, comments_by_id = fetch_thread(subreddit, post_id, session)

        if post_data:
            post_ctx_rows.append({
                "id":          post_data.get("id", post_id),
                "title":       post_data.get("title"),
                "author":      post_data.get("author"),
                "selftext":    post_data.get("selftext"),
                "subreddit":   post_data.get("subreddit"),
                "permalink":   post_data.get("permalink"),
                "created_utc": int(post_data["created_utc"]) if post_data.get("created_utc") is not None else None,
                "captured_at": now,
            })

        for el_c in el_comments:
            parent_id_full = el_c.get("parent_id", "")
            if not parent_id_full.startswith("t1_"):
                continue
            parent_comment_id = parent_id_full.removeprefix("t1_")
            if not full and parent_comment_id in known_ctx_comment_ids:
                continue
            parent = comments_by_id.get(parent_comment_id)
            if not parent:
                continue
            comment_ctx_rows.append({
                "id":          parent_comment_id,
                "author":      parent.get("author"),
                "body":        parent.get("body"),
                "created_utc": int(parent["created_utc"]) if parent.get("created_utc") is not None else None,
                "post_id":     post_id,
                "permalink":   parent.get("permalink"),
                "captured_at": now,
            })

        time.sleep(REQUEST_DELAY)

    return post_ctx_rows, comment_ctx_rows


# ---------------------------------------------------------------------------
# Supabase helpers
# ---------------------------------------------------------------------------

def get_existing_ids(supabase: Client, table: str) -> set:
    log.info("Loading existing IDs from %s...", table)
    ids, offset, page = set(), 0, 1000
    while True:
        resp = supabase.table(table).select("id").range(offset, offset + page - 1).execute()
        rows = resp.data or []
        ids.update(r["id"] for r in rows)
        if len(rows) < page:
            break
        offset += page
    log.info("  %d IDs loaded.", len(ids))
    return ids


def upsert_in_chunks(supabase: Client, table: str, rows: list[dict], chunk: int = 100) -> int:
    total = 0
    for i in range(0, len(rows), chunk):
        batch = rows[i:i + chunk]
        if batch:
            supabase.table(table).upsert(batch, on_conflict="id").execute()
            total += len(batch)
    return total


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(full: bool):
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    session  = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    mode = "FULL BACKFILL" if full else "INCREMENTAL"
    log.info("=== Early Leopard scraper — %s ===", mode)

    # ---- Comments (Arctic Shift) ----
    known_comment_ids = get_existing_ids(supabase, "reddit_comments")
    new_comments: list[dict] = []

    for item in iter_arctic_comments(known_comment_ids, full, session):
        new_comments.append(item)

    n = upsert_in_chunks(supabase, "reddit_comments", [map_comment(c) for c in new_comments])
    log.info("Comments upserted: %d", n)

    # ---- Thread context (Reddit JSON) ----
    known_post_ctx_ids    = get_existing_ids(supabase, "reddit_post_context")
    known_comment_ctx_ids = get_existing_ids(supabase, "reddit_comment_context")

    # Load all stored comments for context resolution
    stored = supabase.table("reddit_comments").select("id,subreddit,link_id,parent_id").execute().data or []

    if full:
        # Re-fetch context for every comment in the database
        all_for_ctx = stored
    else:
        # Only fetch context for comments whose post context is missing
        all_for_ctx = [c for c in stored if c.get("link_id", "").removeprefix("t3_") not in known_post_ctx_ids]
        all_for_ctx += new_comments

    post_ctx_rows, comment_ctx_rows = fetch_all_context(
        all_for_ctx, known_post_ctx_ids, known_comment_ctx_ids, full, session
    )

    n_posts = upsert_in_chunks(supabase, "reddit_post_context",    post_ctx_rows)
    n_ctxc  = upsert_in_chunks(supabase, "reddit_comment_context", comment_ctx_rows)
    log.info("Post context upserted: %d", n_posts)
    log.info("Comment context upserted: %d", n_ctxc)

    log.info("=== Done. ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Early Leopard Reddit scraper")
    parser.add_argument("--full", action="store_true", help="Full backfill")
    args = parser.parse_args()
    run(args.full)
