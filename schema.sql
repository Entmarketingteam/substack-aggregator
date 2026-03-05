-- Substack Aggregator Schema
-- Run once in Supabase SQL editor

CREATE TABLE IF NOT EXISTS substack_sources (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  author TEXT,
  base_url TEXT NOT NULL,
  rss_url TEXT NOT NULL,
  substack_handle TEXT,
  tags TEXT[] DEFAULT '{}',
  active BOOLEAN DEFAULT true,
  last_synced_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS substack_posts (
  id BIGSERIAL PRIMARY KEY,
  source_id TEXT NOT NULL REFERENCES substack_sources(id),
  post_id TEXT,                    -- Substack internal ID
  slug TEXT NOT NULL,
  title TEXT NOT NULL,
  subtitle TEXT,
  url TEXT NOT NULL UNIQUE,
  published_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ,
  audience TEXT DEFAULT 'everyone', -- 'everyone', 'only_paid', 'only_free'
  is_paywalled BOOLEAN DEFAULT false,
  content_html TEXT,
  content_markdown TEXT,
  truncated_preview TEXT,
  wordcount INTEGER,
  cover_image TEXT,
  tags TEXT[] DEFAULT '{}',
  reaction_count INTEGER DEFAULT 0,
  comment_count INTEGER DEFAULT 0,
  ingested_at TIMESTAMPTZ DEFAULT NOW(),
  full_content_fetched BOOLEAN DEFAULT false
);

CREATE INDEX IF NOT EXISTS idx_substack_posts_source ON substack_posts(source_id);
CREATE INDEX IF NOT EXISTS idx_substack_posts_published ON substack_posts(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_substack_posts_paywalled ON substack_posts(is_paywalled, full_content_fetched);
CREATE INDEX IF NOT EXISTS idx_substack_posts_slug ON substack_posts(source_id, slug);
