-- Early Leopard Reddit archive
-- Run once in your Supabase SQL editor

-- -----------------------------------------------------------------------
-- Comments by Early-Leopard-8351
-- -----------------------------------------------------------------------
create table if not exists reddit_comments (
    id           text primary key,
    author       text,
    body         text,
    created_utc  bigint,
    subreddit    text,
    permalink    text,
    score        integer,
    parent_id    text,   -- t3_xxx (post) or t1_xxx (comment)
    link_id      text,   -- always t3_xxx (parent post)
    captured_at  timestamptz default now()
);

create index if not exists reddit_comments_created_utc_idx on reddit_comments (created_utc desc);
create index if not exists reddit_comments_subreddit_idx   on reddit_comments (subreddit);
create index if not exists reddit_comments_link_id_idx     on reddit_comments (link_id);


-- -----------------------------------------------------------------------
-- Posts submitted by Early-Leopard-8351
-- -----------------------------------------------------------------------
create table if not exists reddit_posts (
    id           text primary key,
    author       text,
    title        text,
    selftext     text,
    url          text,
    created_utc  bigint,
    subreddit    text,
    permalink    text,
    score        integer,
    num_comments integer,
    captured_at  timestamptz default now()
);

create index if not exists reddit_posts_created_utc_idx on reddit_posts (created_utc desc);


-- -----------------------------------------------------------------------
-- Context: parent posts for each comment
-- (analogous to conversation_tweets in the Twitter archive)
-- -----------------------------------------------------------------------
create table if not exists reddit_post_context (
    id           text primary key,   -- post id (without t3_ prefix)
    title        text,
    author       text,
    selftext     text,
    subreddit    text,
    permalink    text,
    created_utc  bigint,
    captured_at  timestamptz default now()
);

create index if not exists reddit_post_context_subreddit_idx on reddit_post_context (subreddit);


-- -----------------------------------------------------------------------
-- Context: parent comments (when EL replies to another comment)
-- -----------------------------------------------------------------------
create table if not exists reddit_comment_context (
    id           text primary key,   -- comment id (without t1_ prefix)
    author       text,
    body         text,
    created_utc  bigint,
    post_id      text references reddit_post_context(id),
    permalink    text,
    parent_id    text,
    captured_at  timestamptz default now()
);

create index if not exists reddit_comment_context_post_id_idx on reddit_comment_context (post_id);

-- Migration: add media fields to reddit_post_context
alter table reddit_post_context add column if not exists media_url  text;
alter table reddit_post_context add column if not exists media_type text; -- 'video' | 'image' | null

-- Migration: add parent_id to reddit_comment_context (needed for chain walking in UI)
alter table reddit_comment_context add column if not exists parent_id text;
