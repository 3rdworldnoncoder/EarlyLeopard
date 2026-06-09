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
import json
import logging
import math
import os
import re
import signal
import sys
import time
import zlib
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
# AAT Stylometry — A1, A2, A3 (pure Python, no NLP dependencies)
# Mirrors crossplatform_analysis.js. A4-A6 remain client-side only.
# ---------------------------------------------------------------------------

_AAT_CHUNK_WORDS = 80
_AAT_PASS_PCT    = 75
_AAT_MIN_WORDS   = 500   # mirrors MIN_WORDS in AATTab.jsx

_FUNCTION_WORDS = {
    'the','a','an','i','you','he','she','it','we','they','me','him','her','us','them',
    'my','your','his','its','our','their','this','that','these','those',
    'is','am','are','was','were','be','been','being','have','has','had','do','does','did',
    'will','would','could','should','may','might','shall','can','must','not',
    'in','on','at','by','for','with','about','to','from','of','and','but','or','so',
    'as','if','then','when','where','which','who','what','how',
}

# Order must match a3RefRates in crossplatform_model.json
_A3_PATTERNS = [
    re.compile(r'\bdont\b'),
    re.compile(r':'),
    re.compile(r"\bdidn't\b", re.I),
    re.compile(r'\bsaid\b', re.I),
    re.compile(r'\bliterally\b', re.I),
    re.compile(r'\bdidnt\b'),
    re.compile(r'\blmao\b', re.I),
    re.compile(r"\bit's\b", re.I),
    re.compile(r'\.\.\.'),
    re.compile(r"\bwasn't\b", re.I),
    re.compile(r'\bthis is\b', re.I),
    re.compile(r'\bbecause\b', re.I),
    re.compile(r'(?:^|[.!?]\s+)Also\b'),
    re.compile(r'(?:^|[.!?]\s+)It\b'),
    re.compile(r'(?:^|[.!?]\s+)You\b'),
    re.compile(r"\bisn't\b", re.I),
    re.compile(r'\btrue\b', re.I),
    re.compile(r'\bi would\b', re.I),
    re.compile(r'\byeah?\b', re.I),
    re.compile(r"\bthere's\b", re.I),
    re.compile(r'\blol\b', re.I),
    re.compile(r'\bsort of\b', re.I),
]


def _aat_tokenize(text: str) -> list[str]:
    text = re.sub(r'https?://\S+', '', text)
    text = re.sub(r'@\w+', '', text)
    text = re.sub(r"[^a-zA-Z']", ' ', text)
    return [w.lower() for w in text.split() if w]


def _normalize(vec: list[float]) -> list[float]:
    n = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [v / n for v in vec]


def _chunk_text(text: str) -> list[str]:
    tokens = text.split()
    return [
        ' '.join(tokens[i:i + _AAT_CHUNK_WORDS])
        for i in range(0, len(tokens) - _AAT_CHUNK_WORDS + 1, _AAT_CHUNK_WORDS)
    ]


def _cosine(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    na  = math.sqrt(sum(x * x for x in a))
    nb  = math.sqrt(sum(x * x for x in b))
    return dot / (na * nb) if na and nb else 0.0


def _pctile(score: float, sorted_list: list[float]) -> int:
    if not sorted_list:
        return 50
    return round(sum(1 for s in sorted_list if s < score) / len(sorted_list) * 100)


def _word_length_profile(chunks: list[str]) -> list[float]:
    dist = [0] * 15
    total = 0
    for c in chunks:
        for w in _aat_tokenize(c):
            length = min(len(w.replace("'", '')), 15)
            if length > 0:
                dist[length - 1] += 1
                total += 1
    if not total:
        return [0.0] * 15
    return _normalize([c / total for c in dist])


def _fw_ngrams(text: str, n: int = 3) -> dict[str, int]:
    words = [w for w in _aat_tokenize(text) if w in _FUNCTION_WORDS]
    s = ' ' + ' '.join(words) + ' '
    counts: dict[str, int] = {}
    for i in range(len(s) - n + 1):
        g = s[i:i + n]
        counts[g] = counts.get(g, 0) + 1
    return counts


def _fw_profile(chunks: list[str], vocab: list[str]) -> list[float]:
    vocab_set = set(vocab)
    counts: dict[str, int] = {}
    for c in chunks:
        for g, v in _fw_ngrams(c).items():
            if g in vocab_set:
                counts[g] = counts.get(g, 0) + v
    total = sum(counts.values()) or 1
    return _normalize([counts.get(g, 0) / total for g in vocab])


def _presence_rate(chunks: list[str], pattern: re.Pattern) -> float:
    if not chunks:
        return 0.0
    return sum(1 for c in chunks if pattern.search(c)) / len(chunks)


def _a3_score(chunks: list[str], ref_rates: list[float]) -> float:
    rates = [_presence_rate(chunks, pat) for pat in _A3_PATTERNS]
    n = len(rates)
    return 1.0 - sum(
        max(0.0, abs(r - ref) / max(ref, 0.05))
        for r, ref in zip(rates, ref_rates)
    ) / n


def _ncd_similarity(samples: list[str], cand_text: str) -> float:
    """A5: Normalized Compression Distance.
    Mirrors ncdSimilarity() in crossplatform_analysis.js.
    Uses zlib.compress (DEFLATE + zlib header) == CompressionStream('deflate') in Chrome."""
    target_len = len(cand_text) * 1.5
    sample = min(samples, key=lambda s: abs(len(s) - target_len))

    def c_size(text: str) -> int:
        return len(zlib.compress(text.encode('utf-8')))

    cr  = c_size(sample)
    cc  = c_size(cand_text)
    crc = c_size(sample + ' ' + cand_text)
    denom = max(cr, cc)
    if denom == 0:
        return 0.0
    return max(0.0, 1.0 - (crc - min(cr, cc)) / denom)


def _unmasking_curve(
    chunks: list[str],
    a3_ref_rates: list[float],
    a6_impostors: list[float],
    rounds: int = 6,
    remove_per_round: int = 3,
) -> dict:
    """A6: Unmasking curve (Koppel 2007).
    Mirrors unmaskingCurve() in crossplatform_analysis.js exactly.
    Uses same _A3_PATTERNS order as A3_TESTS in JS."""
    feat_idx = list(range(len(_A3_PATTERNS)))   # original indices, mirrors feats=[...A3_TESTS]
    raw_curve: list[float] = []

    for _ in range(rounds):
        if len(feat_idx) <= remove_per_round + 1:
            break
        n = len(feat_idx)
        cand_rates = [_presence_rate(chunks, _A3_PATTERNS[i]) for i in feat_idx]
        ref_rates  = [a3_ref_rates[i] or 0.1 for i in feat_idx]  # mirrors JS: || 0.1

        score = 1.0 - sum(
            max(0.0, abs(cand_rates[j] - ref_rates[j]) / max(ref_rates[j], 0.05))
            for j in range(n)
        ) / n
        raw_curve.append(score)

        # Remove features with highest deviation (mirrors JS disc.sort + filter)
        disc = sorted(range(n), key=lambda j: abs(cand_rates[j] - ref_rates[j]), reverse=True)
        rm   = set(disc[:remove_per_round])
        feat_idx = [feat_idx[j] for j in range(n) if j not in rm]

    raw_auc  = sum(raw_curve) / len(raw_curve) if raw_curve else 0.0
    raw_drop = (raw_curve[0] - raw_curve[-1]) if len(raw_curve) > 1 else 0.0

    auc  = _pctile(raw_auc, a6_impostors)   # a6_impostors stores raw 0–1 values
    drop = round(raw_drop * 100)
    return {
        'auc':    auc,
        'drop':   drop,
        'robust': auc >= 70 and drop < 30,
    }


def _score_precomputed(text: str, model: dict) -> dict | None:
    """Compute A1/A2/A3/A5/A6 scores (all server-side analyses).
    Returns None if text is too short to chunk.
    A4 (POS bigrams via wink-nlp) remains client-side only."""
    chunks = _chunk_text(text)
    if not chunks:
        return None

    # A1 — word length distribution (Mendenhall)
    a1s    = _cosine(_word_length_profile(chunks), model['a1Ref'])
    a1_pct = _pctile(a1s, model['a1Impostors'])
    a1_pass = a1_pct >= _AAT_PASS_PCT

    # A2 — function-word char n-grams (Overdorf & Greenstadt)
    a2s    = _cosine(_fw_profile(chunks, model['a2Vocab']), model['a2Ref'])
    a2_pct = _pctile(a2s, model['a2Impostors'])
    a2_pass = a2_pct >= _AAT_PASS_PCT

    # A3 — function-word presence rates (Koppel)
    a3s    = _a3_score(chunks, model['a3RefRates'])
    a3_pct = _pctile(a3s, model['a3Impostors'])
    a3_pass = a3_pct >= _AAT_PASS_PCT

    # A5 — Normalized Compression Distance (Cilibrasi & Vitanyi)
    a5s    = _ncd_similarity(model['a5Samples'], text)
    a5_pct = _pctile(a5s, model['a5Impostors'])
    a5_pass = a5_pct >= _AAT_PASS_PCT

    # A6 — Unmasking curve (Koppel 2007)
    a6     = _unmasking_curve(chunks, model['a3RefRates'], model['a6Impostors'])
    a6_pass = a6['robust']
    a6_auc  = a6['auc']

    return {
        'a1_pass': a1_pass, 'a1_pct': a1_pct,
        'a2_pass': a2_pass, 'a2_pct': a2_pct,
        'a3_pass': a3_pass, 'a3_pct': a3_pct,
        'a5_pass': a5_pass, 'a5_pct': a5_pct,
        'a6_pass': a6_pass, 'a6_auc': a6_auc,
        'agree_3': sum([a1_pass, a2_pass, a3_pass]),                           # kept for compat
        'agree_5': sum([a1_pass, a2_pass, a3_pass, a5_pass, a6_pass]),         # pre-computed total
    }


def load_aat_model() -> dict:
    """Load A1/A2/A3 model data from crossplatform_model.json (same directory)."""
    path = os.path.join(os.path.dirname(__file__), 'crossplatform_model.json')
    with open(path, encoding='utf-8') as f:
        return json.load(f)

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


def upsert_in_chunks(supabase: Client, table: str, rows: list[dict], chunk: int = 100, conflict_col: str = "id") -> int:
    total = 0
    for i in range(0, len(rows), chunk):
        batch = rows[i:i + chunk]
        if batch:
            supabase.table(table).upsert(batch, on_conflict=conflict_col).execute()
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

    # In incremental mode the after_utc cursor already positions us at new
    # content, so loading all existing IDs is redundant — upsert handles any
    # duplicate edge cases (same-second timestamps).  Full mode still needs
    # the set to skip already-stored items and for the early-stop logic.
    if full:
        known_aat_ids = get_existing_ids(supabase, "aat_author_comments")
    else:
        known_aat_ids = set()

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


def run_aat_scoring(supabase: Client, model: dict):
    """
    Compute A1/A2/A3/A5/A6 stylometric scores for all qualified authors and upsert
    to aat_author_scores.  Only re-scores authors who have new content since
    their last computed_at, skipping unchanged ones for efficiency.
    A4 (POS bigrams via wink-nlp) remains client-side only.
    """
    log.info("=== AAT scoring — A1/A2/A3/A5/A6 pre-computation ===")

    # 1. Load existing computed_at per author
    existing: dict[str, str] = {}
    try:
        resp = supabase.table("aat_author_scores").select("author,computed_at").execute()
        for row in (resp.data or []):
            existing[row['author']] = row.get('computed_at') or ''
    except Exception as e:
        log.warning("Could not load existing scores: %s", e)

    # 2. Aggregate word counts, max inserted_at, subreddits, and HoDS stats per author
    log.info("Aggregating author metadata...")
    author_meta: dict[str, dict] = {}
    offset, page = 0, 1000
    while True:
        resp = (
            supabase.table("aat_author_comments")
            .select("author,word_count,inserted_at,subreddit,created_utc")
            .range(offset, offset + page - 1)
            .execute()
        )
        rows = resp.data or []
        for row in rows:
            a = row.get('author')
            if not a or a == '[deleted]':
                continue
            if a not in author_meta:
                author_meta[a] = {
                    'word_count': 0, 'max_inserted_at': '', 'subreddits': set(),
                    'item_count': 0, 'days': set(), 'posts_by_day': {}, 'night_count': 0,
                }
            author_meta[a]['word_count'] += row.get('word_count') or 0
            author_meta[a]['item_count'] += 1
            ins = row.get('inserted_at') or ''
            if ins > author_meta[a]['max_inserted_at']:
                author_meta[a]['max_inserted_at'] = ins
            sub = row.get('subreddit') or ''
            if sub:
                author_meta[a]['subreddits'].add(sub)
            utc = row.get('created_utc')
            if utc:
                dt  = datetime.fromtimestamp(utc, tz=timezone.utc)
                day = dt.strftime('%Y-%m-%d')
                author_meta[a]['days'].add(day)
                author_meta[a]['posts_by_day'][day] = author_meta[a]['posts_by_day'].get(day, 0) + 1
                if 1 <= dt.hour <= 5:
                    author_meta[a]['night_count'] += 1
        if len(rows) < page:
            break
        offset += page

    # 3. Determine which authors need (re-)scoring
    to_score = []
    skipped  = 0
    for author, meta in author_meta.items():
        if meta['word_count'] < _AAT_MIN_WORDS:
            continue
        computed_at = existing.get(author, '')
        if computed_at and meta['max_inserted_at'] <= computed_at:
            skipped += 1
            continue   # no new content since last score
        to_score.append((author, meta['word_count']))

    qualified_total = sum(1 for m in author_meta.values() if m['word_count'] >= _AAT_MIN_WORDS)
    log.info(
        "Qualified authors: %d — to score: %d, skipped (unchanged): %d",
        qualified_total, len(to_score), skipped,
    )

    # 3b. Upsert HoDS activity stats for ALL qualified authors regardless of whether
    #     their stylometric scores need recomputing. This ensures item_count,
    #     distinct_days, max_posts_day, night_pct are always current, and populates
    #     them on first deployment without requiring --rescore-all.
    def _hods_stats(meta: dict) -> dict:
        item_count    = meta['item_count']
        distinct_days = len(meta['days'])
        max_posts_day = max(meta['posts_by_day'].values()) if meta['posts_by_day'] else 0
        night_pct     = round(meta['night_count'] / item_count * 100) if item_count else 0
        return {
            'item_count':    item_count,
            'distinct_days': distinct_days,
            'max_posts_day': max_posts_day,
            'night_pct':     night_pct,
        }

    stats_rows = [
        {'author': a, **_hods_stats(m)}
        for a, m in author_meta.items()
        if m['word_count'] >= _AAT_MIN_WORDS
    ]
    upsert_in_chunks(supabase, "aat_author_scores", stats_rows, chunk=100, conflict_col="author")
    log.info("HoDS stats upserted for %d authors", len(stats_rows))

    # 4. Fetch body text for all qualified authors in batched IN queries
    #    (one request per batch instead of one per author)
    AUTHOR_BATCH = 50
    texts_by_author: dict[str, list[str]] = {a: [] for a, _ in to_score}
    qualified_authors = list(texts_by_author.keys())

    for batch_start in range(0, len(qualified_authors), AUTHOR_BATCH):
        batch = qualified_authors[batch_start:batch_start + AUTHOR_BATCH]
        offset, row_page = 0, 1000
        while True:
            resp = (
                supabase.table("aat_author_comments")
                .select("author,body")
                .in_("author", batch)
                .range(offset, offset + row_page - 1)
                .execute()
            )
            rows = resp.data or []
            for row in rows:
                a = row.get('author')
                if a in texts_by_author:
                    texts_by_author[a].append(row.get('body') or '')
            if len(rows) < row_page:
                break
            offset += row_page
        log.info("  Fetched body for authors %d-%d of %d",
                 batch_start + 1, batch_start + len(batch), len(qualified_authors))

    # 5. Score each author from collected text
    scored_rows: list[dict] = []
    for author, word_count in to_score:
        text   = ' '.join(texts_by_author.get(author, []))
        result = _score_precomputed(text, model)
        if result is None:
            log.info("  Skipped %s — insufficient chunks after combining body", author)
            continue
        scored_rows.append({
            'author':      author,
            'word_count':  word_count,
            'subreddits':  sorted(author_meta[author]['subreddits']),
            'computed_at': datetime.now(timezone.utc).isoformat(),
            **_hods_stats(author_meta[author]),
            **result,
        })
        log.info(
            "  Scored %-30s agree=%d/5  (A1:%s A2:%s A3:%s A5:%s A6:%s)",
            author, result['agree_5'],
            'P' if result['a1_pass'] else 'F',
            'P' if result['a2_pass'] else 'F',
            'P' if result['a3_pass'] else 'F',
            'P' if result['a5_pass'] else 'F',
            'P' if result['a6_pass'] else 'F',
        )

    n = upsert_in_chunks(supabase, "aat_author_scores", scored_rows, chunk=50, conflict_col="author")
    log.info("AAT scores upserted: %d", n)


# ---------------------------------------------------------------------------
# Main — Early Leopard
# ---------------------------------------------------------------------------

def run(full: bool, reindex_context: bool = False, skip_aat: bool = False, aat_only: bool = False):
    _install_sigint_handler()

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    session  = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    aat_model = load_aat_model()

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
        run_aat_scoring(supabase, aat_model)

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
