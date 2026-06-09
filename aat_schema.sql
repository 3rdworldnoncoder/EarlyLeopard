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
-- aat_author_scores: A1/A2/A3 pre-computed per author by the scraper.
-- Updated after each AAT run; client uses this for instant ranking without
-- client-side Phase 2 scoring.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS aat_author_scores (
    author      TEXT        PRIMARY KEY,
    a1_pass     BOOLEAN,
    a2_pass     BOOLEAN,
    a3_pass     BOOLEAN,
    a1_pct      SMALLINT,
    a2_pct      SMALLINT,
    a3_pct      SMALLINT,
    agree_3     SMALLINT,   -- 0-3: how many of A1/A2/A3 passed
    word_count  INTEGER,
    computed_at TIMESTAMPTZ DEFAULT now()
);

-- Fast ranking queries (agree descending, then word count)
CREATE INDEX IF NOT EXISTS idx_aat_scores_ranking
    ON aat_author_scores (agree_3 DESC, word_count DESC);

ALTER TABLE aat_author_scores ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Public read" ON aat_author_scores
    FOR SELECT USING (true);
