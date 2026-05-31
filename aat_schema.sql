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
