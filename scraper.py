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
import re
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

ARCTIC_COMMENT_FIELDS = "id,author,body,created_utc,subreddit,score,parent_id,link_id"
ARCTIC_POST_FIELDS    = "id,author,title,selftext,url,created_utc,subreddit,score,num_comments"


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
            "fields": ARCTIC_COMMENT_FIELDS,
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


def iter_arctic_posts(known_ids: set, full: bool, session: requests.Session):
    """Yields raw post dicts from Arctic Shift."""
    after = None
    while True:
        params = {
            "author": REDDIT_USERNAME,
            "limit":  100,
            "sort":   "asc",
            "fields": ARCTIC_POST_FIELDS,
        }
        if after:
            params["after"] = after

        log.info("Arctic Shift: fetching posts (after=%s)", after or "start")
        try:
            resp = session.get(f"{ARCTIC_BASE}/api/posts/search", params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json().get("data", [])
        except Exception as e:
            log.warning("Arctic Shift posts request failed: %s", e)
            break

        if not data:
            log.info("Arctic Shift: no more posts.")
            break

        new_count = 0
        for item in data:
            if item.get("id") not in known_ids:
                yield item
                new_count += 1

        log.info("  got %d posts, %d new.", len(data), new_count)

        if not full and new_count == 0:
            log.info("Arctic Shift: all posts already known — stopping.")
            break

        if len(data) < 100:
            log.info("Arctic Shift: reached end of post history.")
            break

        after = str(int(data[-1].get("created_utc", 0)) + 1)
        time.sleep(REQUEST_DELAY)


def map_post(item: dict) -> dict:
    pid       = item.get("id", "")
    subreddit = item.get("subreddit", "")
    permalink = f"/r/{subreddit}/comments/{pid}/" if pid else None
    return {
        "id":           pid,
        "author":       item.get("author"),
        "title":        item.get("title"),
        "selftext":     item.get("selftext"),
        "url":          item.get("url"),
        "created_utc":  int(item["created_utc"]) if item.get("created_utc") is not None else None,
        "subreddit":    subreddit,
        "permalink":    permalink,
        "score":        item.get("score"),
        "num_comments": item.get("num_comments"),
        "captured_at":  datetime.now(timezone.utc).isoformat(),
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

    # Fetch comment tree — Arctic Shift returns a nested structure with replies
    def walk_tree(nodes: list):
        for node in nodes:
            if not isinstance(node, dict):
                continue
            if node.get("kind") == "more":
                continue
            cid = node.get("id")
            if cid:
                comments_by_id[cid] = node
            # Recurse into replies (can be a list or absent)
            replies = node.get("replies") or []
            if isinstance(replies, list):
                walk_tree(replies)

    try:
        resp = session.get(
            f"{ARCTIC_BASE}/api/comments/tree",
            params={"link_id": f"t3_{post_id}", "limit": 9999},
            timeout=30,
        )
        resp.raise_for_status()
        items = resp.json().get("data", [])
        walk_tree(items)
        log.debug("  tree: %d comments indexed for post %s", len(comments_by_id), post_id)
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

    # Always skip already-archived posts unless reindex_context is set
    posts_to_fetch = set(post_map.keys()) if full == 'reindex' else {p for p in post_map if p not in known_post_ids}
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
            # Extract media info
            media_url, media_type = None, None
            reddit_video = (post_data.get("media") or {}).get("reddit_video")
            if reddit_video:
                media_url  = reddit_video.get("fallback_url")
                media_type = "video"
            elif post_data.get("url") and re.search(
                r'(i\.redd\.it|i\.imgur\.com|\.(jpg|jpeg|png|gif|webp))(\?|$)',
                post_data["url"], re.I
            ):
                media_url  = post_data["url"]
                media_type = "image"

            post_ctx_rows.append({
                "id":          post_data.get("id", post_id),
                "title":       post_data.get("title"),
                "author":      post_data.get("author"),
                "selftext":    post_data.get("selftext"),
                "subreddit":   post_data.get("subreddit"),
                "permalink":   post_data.get("permalink"),
                "created_utc": int(post_data["created_utc"]) if post_data.get("created_utc") is not None else None,
                "media_url":   media_url,
                "media_type":  media_type,
                "captured_at": now,
            })

        # Walk full ancestor chain for each EL comment
        seen_in_thread: set[str] = set()
        for el_c in el_comments:
            current_parent = el_c.get("parent_id", "")
            while current_parent.startswith("t1_"):
                ancestor_id = current_parent.removeprefix("t1_")
                if ancestor_id in seen_in_thread:
                    break
                seen_in_thread.add(ancestor_id)
                if not full and ancestor_id in known_ctx_comment_ids:
                    # Already stored — but still need to follow chain upward
                    ancestor = comments_by_id.get(ancestor_id)
                    current_parent = ancestor.get("parent_id", "") if ancestor else ""
                    continue
                ancestor = comments_by_id.get(ancestor_id)
                if not ancestor:
                    break
                comment_ctx_rows.append({
                    "id":          ancestor_id,
                    "author":      ancestor.get("author"),
                    "body":        ancestor.get("body"),
                    "created_utc": int(ancestor["created_utc"]) if ancestor.get("created_utc") is not None else None,
                    "post_id":     post_id,
                    "permalink":   ancestor.get("permalink"),
                    "parent_id":   ancestor.get("parent_id"),
                    "captured_at": now,
                })
                current_parent = ancestor.get("parent_id", "")

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

def run(full: bool, reindex_context: bool = False):
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    session  = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    # mode string for logging
    if reindex_context:
        mode = "REINDEX CONTEXT"
    elif full:
        mode = "FULL BACKFILL"
    else:
        mode = "INCREMENTAL"
    log.info("=== Early Leopard scraper — %s ===", mode)

    # ---- Comments (Arctic Shift) ----
    known_comment_ids = get_existing_ids(supabase, "reddit_comments")
    new_comments: list[dict] = []

    if not reindex_context:
        for item in iter_arctic_comments(known_comment_ids, full, session):
            new_comments.append(item)

    n = upsert_in_chunks(supabase, "reddit_comments", [map_comment(c) for c in new_comments])
    log.info("Comments upserted: %d", n)

    # ---- Thread context (Reddit JSON) ----
    known_post_ctx_ids    = get_existing_ids(supabase, "reddit_post_context")
    known_comment_ctx_ids = get_existing_ids(supabase, "reddit_comment_context")

    # Load all stored comments for context resolution
    stored = supabase.table("reddit_comments").select("id,subreddit,link_id,parent_id").execute().data or []

    # --reindex-context: force re-fetch all contexts; otherwise skip already-known posts
    ctx_mode = 'reindex' if reindex_context else False
    all_for_ctx = stored if reindex_context else (
        [c for c in stored if c.get("link_id", "").removeprefix("t3_") not in known_post_ctx_ids]
        + new_comments
    )

    post_ctx_rows, comment_ctx_rows = fetch_all_context(
        all_for_ctx, known_post_ctx_ids, known_comment_ctx_ids, ctx_mode, session
    )

    n_posts = upsert_in_chunks(supabase, "reddit_post_context",    post_ctx_rows)
    n_ctxc  = upsert_in_chunks(supabase, "reddit_comment_context", comment_ctx_rows)
    log.info("Post context upserted: %d", n_posts)
    log.info("Comment context upserted: %d", n_ctxc)

    # ---- Posts (submitted) ----
    known_post_ids = get_existing_ids(supabase, "reddit_posts")
    new_posts: list[dict] = []

    for item in iter_arctic_posts(known_post_ids, full, session):
        new_posts.append(item)

    n = upsert_in_chunks(supabase, "reddit_posts", [map_post(p) for p in new_posts])
    log.info("Posts upserted: %d", n)

    log.info("=== Done. ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Early Leopard Reddit scraper")
    parser.add_argument("--full",             action="store_true", help="Full backfill (all comments, missing context only)")
    parser.add_argument("--reindex-context",  action="store_true", help="Re-fetch ALL thread context regardless of what's stored")
    args = parser.parse_args()
    run(full=args.full, reindex_context=args.reindex_context)
