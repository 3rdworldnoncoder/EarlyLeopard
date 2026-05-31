#!/usr/bin/env python3
"""
Backfill audio for existing Reddit video posts.

For every reddit_post_context row where:
  - media_type = 'video'
  - media_url  contains v.redd.it
  - audio_url  is NULL

Downloads the audio track from v.redd.it (server-side, no CORS issue),
uploads it to Supabase Storage bucket 'reddit-audio', and stores the
public URL in the audio_url column.

Usage:
    python backfill_audio.py            # processes all missing rows
    python backfill_audio.py --dry-run  # prints what would be done, no writes
"""

import argparse
import io
import logging
import os
import re
import sys
import time

import requests
from dotenv import load_dotenv
from supabase import Client, create_client

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
BUCKET       = "reddit-audio"
USER_AGENT   = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Audio URL derivation
# Same formula as reddit-video-downloader Chrome extensions:
# github.com/Lekhak123/Reddit-video-downloader-extension
# ---------------------------------------------------------------------------

def derive_audio_url(video_url: str) -> str | None:
    """
    Derives the Reddit audio URL from the video fallback URL.
    DASH videos (DASH_720.mp4)  → DASH_AUDIO_128.mp4
    CMAF videos (CMAF_1080.mp4) → CMAF_AUDIO_128.mp4  (preserve prefix!)
    """
    if not video_url or "v.redd.it" not in video_url:
        return None
    base = video_url.split("?")[0]
    # Preserve format prefix: DASH→DASH_AUDIO_128, CMAF→CMAF_AUDIO_128
    audio = re.sub(r"(DASH|CMAF)_\d+\.mp4$", lambda m: f"{m.group(1)}_AUDIO_128.mp4", base, flags=re.IGNORECASE)
    if audio == base:
        m = re.search(r"v\.redd\.it/([^/?]+)", base)
        if not m:
            return None
        audio = f"https://v.redd.it/{m.group(1)}/DASH_AUDIO_128.mp4"
    return audio

# ---------------------------------------------------------------------------
# Download + upload
# ---------------------------------------------------------------------------

def download_audio(url: str, session: requests.Session) -> bytes | None:
    """Downloads audio bytes from v.redd.it. Returns None if unavailable."""
    try:
        r = session.get(url, timeout=30, stream=True)
        if r.status_code == 403:
            log.debug("  Audio 403 (no audio track): %s", url)
            return None
        r.raise_for_status()
        data = r.content
        if len(data) < 1000:
            log.debug("  Audio too small (%d bytes), skipping", len(data))
            return None
        return data
    except Exception as e:
        log.warning("  Download error: %s", e)
        return None


def upload_to_storage(supabase: Client, post_id: str, data: bytes) -> str | None:
    """Uploads audio bytes to Supabase Storage, returns public URL."""
    path = f"{post_id}.mp4"
    try:
        supabase.storage.from_(BUCKET).upload(
            path=path,
            file=data,
            file_options={"content-type": "audio/mp4", "upsert": "true"},
        )
        public_url = supabase.storage.from_(BUCKET).get_public_url(path)
        return public_url
    except Exception as e:
        log.error("  Storage upload error for %s: %s", post_id, e)
        # Print existing buckets to help diagnose naming mismatches
        try:
            buckets = supabase.storage.list_buckets()
            log.error("  Existing buckets: %s", [b.name for b in buckets])
            log.error("  Script is using bucket name: '%s'", BUCKET)
        except Exception:
            pass
        return None

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def ensure_bucket(supabase: Client) -> bool:
    """Creates the storage bucket if it doesn't exist. Returns True on success."""
    try:
        buckets = supabase.storage.list_buckets()
        if any(b.name == BUCKET for b in buckets):
            log.info("Bucket '%s' already exists.", BUCKET)
            return True
        supabase.storage.create_bucket(BUCKET, options={"public": True})
        log.info("Created bucket '%s'.", BUCKET)
        return True
    except Exception as e:
        log.error("Could not create bucket '%s': %s", BUCKET, e)
        return False


def run(dry_run: bool = False):
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    if not dry_run:
        if not ensure_bucket(supabase):
            log.error("Aborting — bucket setup failed.")
            sys.exit(1)

    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Referer": "https://www.reddit.com/",
    })

    # Fetch all video posts missing audio_url
    log.info("Fetching video posts with missing audio_url...")
    result = (
        supabase.table("reddit_post_context")
        .select("id, media_url")
        .eq("media_type", "video")
        .ilike("media_url", "%v.redd.it%")
        .is_("audio_url", "null")
        .execute()
    )
    rows = result.data or []
    log.info("Found %d rows to process", len(rows))

    ok = skip = fail = 0

    for i, row in enumerate(rows, 1):
        post_id   = row["id"]
        video_url = row["media_url"]
        audio_url = derive_audio_url(video_url)

        log.info("[%d/%d] %s → %s", i, len(rows), post_id, audio_url)

        if not audio_url:
            log.warning("  Could not derive audio URL, skipping")
            skip += 1
            continue

        if dry_run:
            log.info("  [DRY RUN] would download and upload")
            ok += 1
            continue

        data = download_audio(audio_url, session)
        if not data:
            log.info("  No audio track, marking as empty string to skip future runs")
            supabase.table("reddit_post_context").update({"audio_url": ""}).eq("id", post_id).execute()
            skip += 1
            time.sleep(0.5)
            continue

        public_url = upload_to_storage(supabase, post_id, data)
        if not public_url:
            fail += 1
            time.sleep(0.5)
            continue

        supabase.table("reddit_post_context").update({"audio_url": public_url}).eq("id", post_id).execute()
        log.info("  ✓ stored: %s", public_url)
        ok += 1
        time.sleep(0.5)  # be polite to v.redd.it

    log.info("Done — ok=%d  skipped=%d  failed=%d", ok, skip, fail)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
