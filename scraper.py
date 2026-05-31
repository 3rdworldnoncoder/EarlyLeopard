#!/usr/bin/env python3
"""
Early-Leopard-8351 Reddit scraper + AAT (Aaron Alogs Tracker)
- EL comments/posts: Arctic Shift API (full history, no auth needed)
- AAT: top posters in r/Steeltoebeggingshow and r/steeltoe
- Upserts to Supabase

Usage:
    python scraper.py                 # incremental (EL + AAT)
    python scraper.py --full          # full backfill (EL + AAT)
    python scraper.py --skip-aat      # EL only
    python scraper.py --aat-only      # AAT only
"""

import argparse
import logging
import os
import re
import signal
import sys
import time
from datetime import datetime, timedelta, timezone

import httpx
import requests
from dotenv import load_dotenv
from supabase import Client, create_client
from supabase.lib.client_options import SyncClientOptions

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv()

SUPABASE_URL    = os.environ["SUPABASE_URL"]
SUPABASE_KEY    = os.environ["SUPABASE_KEY"]
REDDIT_USERNAME = "Early-Leopard-8351"
USER_AGENT      = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
ARCTIC_BASE     = "https://arctic-shift.photon-reddit.com"
REQUEST_DELAY   = 2.0   # seconds between requests

# AAT config
AAT_SUBREDDITS      = ["Steeltoebeggingshow", "steeltoe"]
AAT_MIN_WORD_COUNT  = 3   # minimum words per comment to store
AAT_SKIP_AUTHORS    = {"[deleted]", "AutoModerator", "BotDefense", "reddit"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Arctic Shift — generic paginated comment/post iterators
# ---------------------------------------------------------------------------

ARCTIC_COMMENT_FIELDS = "id,author,body,created_utc,subreddit,score,parent_id,link_id"
ARCTIC_POST_FIELDS    = "id,author,title,selftext,url,created_utc,subreddit,score,num_comments"


def iter_arctic_comments(
    known_ids: set,
    full: bool,
    session: requests.Session,
    filter_params: dict | None = None,
    after_utc: int | None = None,
):
    """
    Yields raw comment dicts from Arctic Shift, paginating via created_utc asc.

    filter_params: extra query params passed to Arctic Shift, e.g.
        {"author": "Early-Leopard-8351"}   — EL mode (default)
        {"subreddit": "steeltoe"}          — AAT mode

    after_utc: unix timestamp to start from (inclusive). Used to limit
        historical backfill to a recent window (e.g. last 6 months).

    In incremental mode, stops as soon as a full page is entirely known.
    """
    if filter_params is None:
        filter_params = {"author": REDDIT_USERNAME}

    after = str(after_utc) if after_utc is not None else None

    while True:
        params = {
            "limit":  100,
            "sort":   "asc",
            "fields": ARCTIC_COMMENT_FIELDS,
            **filter_params,
        }
        if after:
            params["after"] = after

        log.info("Arctic Shift: fetching comments %s (after=%s)", filter_params, after or "start")
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

        if not full and new_count == 0:
            log.info("Arctic Shift: all items on page already known — stopping.")
            break

        if len(data) < 100:
            log.info("Arctic Shift: reached end of history.")
            break

        after = str(int(data[-1].get("created_utc", 0)) + 1)
        time.sleep(REQUEST_DELAY)


def iter_arctic_posts(
    known_ids: set,
    full: bool,
    session: requests.Session,
    filter_params: dict | None = None,
    after_utc: int | None = None,
):
    """Yields raw post dicts from Arctic Shift."""
    if filter_params is None:
        filter_params = {"author": REDDIT_USERNAME}

    after = str(after_utc) if after_utc is not None else None
    while True:
        params = {
            "limit":  100,
            "sort":   "asc",
            "fields": ARCTIC_POST_FIELDS,
            **filter_params,
        }
        if after:
            params["after"] = after

        log.info("Arctic Shift: fetching posts %s (after=%s)", filter_params, after or "start")
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


def map_comment(item: dict) -> dict:
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


def map_aat_comment(item: dict) -> dict:
    """Maps a raw Arctic Shift comment to the aat_author_comments schema."""
    body       = item.get("body", "") or ""
    word_count = len(body.split())
    cid        = item.get("id", "")
    subreddit  = item.get("subreddit", "")
    link_id    = item.get("link_id", "").removeprefix("t3_")
    permalink  = f"/r/{subreddit}/comments/{link_id}/_/{cid}/" if link_id else None

    return {
        "id":          cid,
        "author":      item.get("author"),
        "subreddit":   subreddit,
        "type":        "comment",
        "body":        body,
        "word_count":  word_count,
        "score":       item.get("score"),
        "permalink":   permalink,
        "created_utc": int(item["created_utc"]) if item.get("created_utc") is not None else None,
        "inserted_at": datetime.now(timezone.utc).isoformat(),
    }


def map_aat_post(item: dict) -> dict:
    """Maps a raw Arctic Shift post to the aat_author_comments schema (type='post')."""
    pid       = item.get("id", "")
    subreddit = item.get("subreddit", "")
    title     = item.get("title", "") or ""
    selftext  = item.get("selftext", "") or ""
    # Combine title + selftext as the text body; title alone is still useful
    body      = f"{title}\n\n{selftext}".strip() if selftext else title
    word_count = len(body.split())
    permalink  = f"/r/{subreddit}/comments/{pid}/" if pid else None

    return {
        "id":          pid,
        "author":      item.get("author"),
        "subreddit":   subreddit,
        "type":        "post",
        "body":        body,
        "word_count":  word_count,
        "score":       item.get("score"),
        "permalink":   permalink,
        "created_utc": int(item["created_utc"]) if item.get("created_utc") is not None else None,
        "inserted_at": datetime.now(timezone.utc).isoformat(),
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

    try:
        resp = session.get(f"{ARCTIC_BASE}/api/posts/ids", params={"ids": post_id}, timeout=15)
        resp.raise_for_status()
        posts = resp.json().get("data", [])
        if posts:
            post_data = posts[0]
    except Exception as e:
        log.warning("Arctic Shift: failed to fetch post %s: %s", post_id, e)

    def walk_tree(nodes: list):
        for node in nodes:
            if not isinstance(node, dict):
                continue
            if node.get("kind") == "more":
                continue
            if "data" in node and isinstance(node["data"], dict):
                node = node["data"]
            cid = node.get("id")
            if cid:
                comments_by_id[cid] = node
            replies = node.get("replies") or []
            if isinstance(replies, dict):
                replies = replies.get("data", {}).get("children", [])
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
    refresh_ctx_comment_ids: set[str] | None = None,
) -> tuple[list[dict], list[dict]]:
    """
    For each unique post referenced by comments, fetch thread context.
    Returns (post_context_rows, comment_context_rows).
    """
    refresh_ctx_comment_ids = refresh_ctx_comment_ids or set()
    post_map: dict[str, list[dict]] = {}
    for c in comments:
        post_id = c.get("link_id", "").removeprefix("t3_")
        if post_id:
            post_map.setdefault(post_id, []).append(c)

    posts_to_fetch = set(post_map.keys()) if full == 'reindex' else {p for p in post_map if p not in known_post_ids}
    log.info("Fetching context for %d unique posts...", len(posts_to_fetch))

    post_ctx_rows:    list[dict] = []
    comment_ctx_rows: list[dict] = []
    now = datetime.now(timezone.utc).isoformat()

    for post_id in posts_to_fetch:
        el_comments = post_map[post_id]
        subreddit   = el_comments[0].get("subreddit", "")

        log.info("  Thread: r/%s/%s", subreddit, post_id)
        post_data, comments_by_id = fetch_thread(subreddit, post_id, session)

        if post_data:
            media_url, media_type, audio_url = None, None, None
            reddit_video = (post_data.get("media") or {}).get("reddit_video")
            if reddit_video:
                media_url  = reddit_video.get("fallback_url")
                media_type = "video"
                audio_url = fetch_and_store_audio(supabase, post_data.get("id", post_id), media_url, session)
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
                "audio_url":   audio_url,
                "captured_at": now,
            })

        seen_in_thread: set[str] = set()
        for el_c in el_comments:
            current_parent = el_c.get("parent_id", "")
            while current_parent.startswith("t1_"):
                ancestor_id = current_parent.removeprefix("t1_")
                if ancestor_id in seen_in_thread:
                    break
                seen_in_thread.add(ancestor_id)
                if not full and ancestor_id in known_ctx_comment_ids and ancestor_id not in refresh_ctx_comment_ids:
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


def get_null_parent_context_comment_ids(supabase: Client) -> tuple[set[str], set[str]]:
    log.info("Loading reddit_comment_context rows with NULL parent_id...")
    resp = (
        supabase.table("reddit_comment_context")
        .select("id,post_id")
        .is_("parent_id", None)
        .execute()
    )
    rows = resp.data or []
    ids = {r["id"] for r in rows}
    post_ids = {r["post_id"] for r in rows if r.get("post_id")}
    log.info("  %d null-parent comment context rows found.", len(ids))
    return ids, post_ids


AUDIO_BUCKET = "reddit-audio"

def fetch_and_store_audio(supabase: Client, post_id: str, video_url: str | None, session: requests.Session) -> str | None:
    """
    Derives the Reddit audio URL, downloads it server-side (no CORS issue),
    uploads to Supabase Storage, and returns the public URL.
    Returns empty string if no audio track exists, None on error.
    """
    if not video_url or "v.redd.it" not in video_url:
        return None
    base = video_url.split("?")[0]
    audio_src = re.sub(r"(DASH|CMAF)_\d+\.mp4$", lambda m: f"{m.group(1)}_AUDIO_128.mp4", base, flags=re.IGNORECASE)
    if audio_src == base:
        m = re.search(r"v\.redd\.it/([^/?]+)", base)
        if not m:
            return None
        audio_src = f"https://v.redd.it/{m.group(1)}/DASH_AUDIO_128.mp4"
    try:
        r = session.get(audio_src, timeout=30)
        if r.status_code == 403 or len(r.content) < 1000:
            return ""
        r.raise_for_status()
        path = f"{post_id}.mp4"
        supabase.storage.from_(AUDIO_BUCKET).upload(
            path=path, file=r.content,
            file_options={"content-type": "audio/mp4", "upsert": "true"},
        )
        return supabase.storage.from_(AUDIO_BUCKET).get_public_url(path)
    except Exception as e:
        log.warning("  Audio fetch/upload failed for %s: %s", post_id, e)
        return None


def upsert_in_chunks(supabase: Client, table: str, rows: list[dict], chunk: int = 100) -> int:
    total = 0
    for i in range(0, len(rows), chunk):
        batch = rows[i:i + chunk]
        if batch:
            supabase.table(table).upsert(batch, on_conflict="id").execute()
            total += len(batch)
    return total


# ---------------------------------------------------------------------------
# AAT — subreddit scraper
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Graceful shutdown — CTRL+C flushes buffered rows before exit
# ---------------------------------------------------------------------------

class _GracefulExit(Exception):
    pass

def _install_sigint_handler():
    """Replace the default SIGINT handler with one that raises _GracefulExit."""
    def handler(sig, frame):
        log.warning("SIGINT received — flushing buffered data before exit…")
        raise _GracefulExit()
    signal.signal(signal.SIGINT, handler)


def _aat_skip(author: str, body: str) -> bool:
    """Returns True if this item should be excluded from AAT."""
    if not author or author in AAT_SKIP_AUTHORS:
        return True
    if author.startswith("bot_") or author.lower().endswith("bot"):
        return True
    if body in ("[deleted]", "[removed]", ""):
        return True
    if len(body.split()) < AAT_MIN_WORD_COUNT:
        return True
    return False


def get_aat_cursor(supabase: Client) -> int | None:
    """
    Returns the max created_utc stored in aat_author_comments, or None if empty.
    Used as the starting cursor for incremental runs so we don't re-scan history.
    """
    try:
        resp = (
            supabase.table("aat_author_comments")
            .select("created_utc")
            .order("created_utc", desc=True)
            .limit(1)
            .execute()
        )
        rows = resp.data or []
        if rows and rows[0].get("created_utc"):
            ts = int(rows[0]["created_utc"])
            log.info("AAT incremental cursor: %d (%s)", ts, datetime.fromtimestamp(ts, tz=timezone.utc).isoformat())
            return ts
    except Exception as e:
        log.warning("Could not fetch AAT cursor: %s", e)
    return None


def run_aat(supabase: Client, session: requests.Session, full: bool):
    """
    Scrapes comments AND posts from AAT_SUBREDDITS and upserts to aat_author_comments.
    Skips bots, deleted accounts, and very short items.
    type='comment' for comments, type='post' for OPs.
    """
    AAT_LOOKBACK_DAYS = 180

    if full:
        # Full backfill: limit to last 180 days to avoid scraping entire history
        after_utc = int((datetime.now(timezone.utc) - timedelta(days=AAT_LOOKBACK_DAYS)).timestamp())
        label = f"FULL (since {AAT_LOOKBACK_DAYS}d ago)"
    else:
        # Incremental: start from the newest item already stored, not from time zero
        after_utc = get_aat_cursor(supabase)
        label = f"INCREMENTAL (since cursor)" if after_utc else "INCREMENTAL (no cursor — first run?)"

    log.info("=== AAT scraper — %s ===", label)

    known_aat_ids = get_existing_ids(supabase, "aat_author_comments")
    new_rows: list[dict] = []

    try:
        for sub in AAT_SUBREDDITS:
            log.info("--- r/%s: comments ---", sub)
            for item in iter_arctic_comments(
                known_ids=known_aat_ids,
                full=full,
                session=session,
                filter_params={"subreddit": sub},
                after_utc=after_utc,
            ):
                author = item.get("author", "") or ""
                body   = item.get("body", "") or ""
                if _aat_skip(author, body):
                    continue
                row = map_aat_comment(item)
                new_rows.append(row)
                known_aat_ids.add(item["id"])

            time.sleep(REQUEST_DELAY)

            log.info("--- r/%s: posts ---", sub)
            for item in iter_arctic_posts(
                known_ids=known_aat_ids,
                full=full,
                session=session,
                filter_params={"subreddit": sub},
                after_utc=after_utc,
            ):
                author = item.get("author", "") or ""
                title  = item.get("title", "") or ""
                if _aat_skip(author, title):
                    continue
                row = map_aat_post(item)
                new_rows.append(row)
                known_aat_ids.add(item["id"])

            time.sleep(REQUEST_DELAY)

    except _GracefulExit:
        log.warning("Interrupted. Flushing %d buffered rows…", len(new_rows))

    n = upsert_in_chunks(supabase, "aat_author_comments", new_rows)
    log.info("AAT items upserted: %d", n)


# ---------------------------------------------------------------------------
# Main — Early Leopard
# ---------------------------------------------------------------------------

def run(full: bool, reindex_context: bool = False, skip_aat: bool = False, aat_only: bool = False):
    _install_sigint_handler()

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    session  = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    if not aat_only:
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
            try:
                for item in iter_arctic_comments(known_comment_ids, full, session):
                    new_comments.append(item)
            except _GracefulExit:
                log.warning("Interrupted during EL comments. Flushing %d rows…", len(new_comments))
                upsert_in_chunks(supabase, "reddit_comments", [map_comment(c) for c in new_comments])
                sys.exit(0)

        n = upsert_in_chunks(supabase, "reddit_comments", [map_comment(c) for c in new_comments])
        log.info("Comments upserted: %d", n)

        # ---- Thread context (Reddit JSON) ----
        known_post_ctx_ids    = get_existing_ids(supabase, "reddit_post_context")
        known_comment_ctx_ids = get_existing_ids(supabase, "reddit_comment_context")
        null_parent_ctx_ids, null_parent_post_ids = get_null_parent_context_comment_ids(supabase)

        stored = supabase.table("reddit_comments").select("id,subreddit,link_id,parent_id").execute().data or []

        ctx_mode = 'reindex' if reindex_context else False
        if reindex_context:
            all_for_ctx = stored
        else:
            posts_to_refresh = {
                c.get("link_id", "").removeprefix("t3_")
                for c in stored
                if c.get("link_id", "").removeprefix("t3_") not in known_post_ctx_ids
                or c.get("link_id", "").removeprefix("t3_") in null_parent_post_ids
            }
            comment_ids_seen = set()
            all_for_ctx = []
            for c in stored + new_comments:
                cid = c.get("id")
                if not cid or cid in comment_ids_seen:
                    continue
                post_id = c.get("link_id", "").removeprefix("t3_")
                if post_id and post_id in posts_to_refresh:
                    all_for_ctx.append(c)
                    comment_ids_seen.add(cid)
            all_for_ctx.extend([c for c in new_comments if c.get("id") not in comment_ids_seen])

        post_ctx_rows, comment_ctx_rows = fetch_all_context(
            all_for_ctx,
            known_post_ctx_ids,
            known_comment_ctx_ids,
            ctx_mode,
            session,
            refresh_ctx_comment_ids=null_parent_ctx_ids,
        )

        n_posts = upsert_in_chunks(supabase, "reddit_post_context",    post_ctx_rows)
        n_ctxc  = upsert_in_chunks(supabase, "reddit_comment_context", comment_ctx_rows)
        log.info("Post context upserted: %d", n_posts)
        log.info("Comment context upserted: %d", n_ctxc)

        # ---- Posts (submitted) ----
        known_post_ids = get_existing_ids(supabase, "reddit_posts")
        new_posts: list[dict] = []

        try:
            for item in iter_arctic_posts(known_post_ids, full, session):
                new_posts.append(item)
        except _GracefulExit:
            log.warning("Interrupted during EL posts. Flushing %d rows…", len(new_posts))
            upsert_in_chunks(supabase, "reddit_posts", [map_post(p) for p in new_posts])
            sys.exit(0)

        n = upsert_in_chunks(supabase, "reddit_posts", [map_post(p) for p in new_posts])
        log.info("Posts upserted: %d", n)

    # ---- AAT ----
    if not skip_aat:
        run_aat(supabase, session, full)

    log.info("=== Done. ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Early Leopard + AAT Reddit scraper")
    parser.add_argument("--full",            action="store_true", help="Full backfill (all comments, missing context only)")
    parser.add_argument("--reindex-context", action="store_true", help="Re-fetch ALL thread context regardless of what's stored")
    parser.add_argument("--skip-aat",        action="store_true", help="Skip AAT subreddit scraping (EL only)")
    parser.add_argument("--aat-only",        action="store_true", help="Run AAT scraping only, skip EL")
    args = parser.parse_args()
    run(
        full=args.full,
        reindex_context=args.reindex_context,
        skip_aat=args.skip_aat,
        aat_only=args.aat_only,
    )
