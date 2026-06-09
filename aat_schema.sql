-- AAT (Aaron Alogs Tracker) — Supabase schema
-- Run once in the Supabase SQL editor

CREATE TABLE IF NOT EXISTS aat_author_comments (
    id          TEXT        PRIMARY KEY,
    author      TEXT        NOT NULL,
    subreddit   TEXT        NOT NULL,
    type        TEXT        NOT NULL DEFAULT 'comment', -- 'comment' | 'post'
    body        TEXT        NOT NULL,
    word_count  INTEGER,
    score       INTEGER,
    permalink   TEXT,
    created_utc BIGINT,
    inserted_at TIMESTAMPTZ DEFAULT now()
);

-- Fast lookups by author (for frontend profile computation)
CREATE INDEX IF NOT EXISTS idx_aat_comments_author    ON aat_author_comments (author);
-- Fast incremental scraping (cursor pagination)
CREATE INDEX IF NOT EXISTS idx_aat_comments_created   ON aat_author_comments (created_utc);
-- Filter by sub
CREATE INDEX IF NOT EXISTS idx_aat_comments_subreddit ON aat_author_comments (subreddit);

-- Enable Row Level Security (public read, no anon write)
ALTER TABLE aat_author_comments ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Public read" ON aat_author_comments
    FOR SELECT USING (true);

-- ---------------------------------------------------------------------------
-- aat_author_scores: A1/A2/A3/A5/A6 pre-computed per author by the scraper.
-- A4 (POS bigrams via wink-nlp) remains client-side only (JS-specific tagger).
-- Updated after each AAT run; client uses this for instant ranking.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS aat_author_scores (
    author      TEXT        PRIMARY KEY,
    a1_pass     BOOLEAN,
    a2_pass     BOOLEAN,
    a3_pass     BOOLEAN,
    a1_pct      SMALLINT,
    a2_pct      SMALLINT,
    a3_pct      SMALLINT,
    a5_pass     BOOLEAN,
    a5_pct      SMALLINT,
    a6_pass     BOOLEAN,
    a6_auc      SMALLINT,
    agree_3       SMALLINT,   -- 0-3: A1/A2/A3 (kept for backward compat)
    agree_5       SMALLINT,   -- 0-5: A1/A2/A3/A5/A6 pre-computed total
    word_count    INTEGER,
    subreddits    TEXT[]      DEFAULT '{}',
    item_count    INTEGER,    -- total posts + comments scraped
    max_posts_day SMALLINT,   -- peak posts in a single calendar day (UTC)
    night_pct     SMALLINT,   -- % of posts made 01:00-05:59 UTC
    distinct_days SMALLINT,   -- number of distinct days with at least one post
    computed_at   TIMESTAMPTZ DEFAULT now()
);

-- Migrations for existing deployments (safe to run on a live DB):
-- ALTER TABLE aat_author_scores ADD COLUMN IF NOT EXISTS subreddits TEXT[] DEFAULT '{}';
-- ALTER TABLE aat_author_scores ADD COLUMN IF NOT EXISTS a5_pass BOOLEAN;
-- ALTER TABLE aat_author_scores ADD COLUMN IF NOT EXISTS a5_pct  SMALLINT;
-- ALTER TABLE aat_author_scores ADD COLUMN IF NOT EXISTS a6_pass BOOLEAN;
-- ALTER TABLE aat_author_scores ADD COLUMN IF NOT EXISTS a6_auc  SMALLINT;
-- ALTER TABLE aat_author_scores ADD COLUMN IF NOT EXISTS agree_5 SMALLINT;
-- ALTER TABLE aat_author_scores ADD COLUMN IF NOT EXISTS item_count    INTEGER;
-- ALTER TABLE aat_author_scores ADD COLUMN IF NOT EXISTS max_posts_day SMALLINT;
-- ALTER TABLE aat_author_scores ADD COLUMN IF NOT EXISTS night_pct     SMALLINT;
-- ALTER TABLE aat_author_scores ADD COLUMN IF NOT EXISTS distinct_days SMALLINT;

-- Fast ranking queries (agree_5 descending, then word count)
CREATE INDEX IF NOT EXISTS idx_aat_scores_ranking
    ON aat_author_scores (agree_5 DESC, word_count DESC);

ALTER TABLE aat_author_scores ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Public read" ON aat_author_scores
    FOR SELECT USING (true);
