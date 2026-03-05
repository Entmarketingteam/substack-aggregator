"""
Substack content aggregator — main ingestion script.

Usage:
  python ingest.py                      # Sync new posts from all sources
  python ingest.py --source arbitrage-andy
  python ingest.py --full-sync          # Re-fetch all (incl. existing)
  python ingest.py --export-obsidian    # Export all to Obsidian vault
  python ingest.py --status             # Show DB stats

Env vars (Doppler example-project):
  SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
  SUBSTACK_EMAIL, SUBSTACK_PASSWORD     — paid account for full content
  SUBSTACK_SESSION_COOKIE               — optional: direct cookie
  OBSIDIAN_VAULT_PATH                   — defaults to Documents/1st vault
"""
import os, sys, json, logging, argparse, re, time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ingest")

SOURCES_FILE = Path(__file__).parent / "sources.json"
REQUEST_DELAY = 0.4  # seconds between API calls


def load_sources(source_id_filter=None):
    with open(SOURCES_FILE) as f:
        sources = json.load(f)
    if source_id_filter:
        sources = [s for s in sources if s["id"] == source_id_filter]
    return [s for s in sources if s.get("active", True)]


def ingest_source(client, db, source, new_only=True):
    from substack_client import html_to_markdown

    source_id = source["id"]
    base_url = source["base_url"]
    logger.info(f"--- Syncing: {source['name']} ({source_id}) ---")

    # Upsert source record
    db.upsert_source({
        "id": source_id,
        "name": source["name"],
        "author": source.get("author", ""),
        "base_url": base_url,
        "rss_url": source["rss_url"],
        "substack_handle": source.get("substack_handle", ""),
        "tags": source.get("tags", []),
        "active": True,
    })

    # Get existing slugs to avoid re-fetching
    existing_slugs = db.get_existing_slugs(source_id)
    logger.info(f"  Existing posts in DB: {len(existing_slugs)}")

    # 1. Get post list (metadata only — body_html is null in list endpoint)
    raw_posts = client.get_all_posts(base_url)
    logger.info(f"  API returned {len(raw_posts)} posts")

    # Filter to new ones only
    if new_only:
        raw_posts = [p for p in raw_posts if p.get("slug", "") not in existing_slugs]
        logger.info(f"  New posts to fetch: {len(raw_posts)}")

    if not raw_posts:
        logger.info(f"  Nothing new for {source['name']}")
        db.update_source_synced(source_id)
        return 0

    # 2. Fetch full detail for each post (this gets body_html)
    to_insert = []
    for i, raw in enumerate(raw_posts):
        slug = raw.get("slug", "")
        if not slug:
            continue

        logger.info(f"  [{i+1}/{len(raw_posts)}] Fetching: {raw.get('title', slug)[:60]}")
        detail = client.get_post_detail(base_url, slug)
        if not detail:
            # Fall back to list data if detail fails
            detail = raw

        normalized = client.normalize_post(detail, source_id)
        if normalized["url"] or normalized["slug"]:
            # Ensure URL is set even if not in detail
            if not normalized["url"]:
                normalized["url"] = f"{base_url}/p/{slug}"
            to_insert.append(normalized)

        time.sleep(REQUEST_DELAY)

    # 3. Also check RSS for any posts the API might miss (older ones)
    rss_posts = client.parse_rss(source["rss_url"])
    api_slugs = {p.get("slug", "") for p in raw_posts}
    for rss_post in rss_posts:
        slug = rss_post.get("slug", "")
        if not slug or slug in api_slugs or (new_only and slug in existing_slugs):
            continue
        content_html = rss_post.get("content_html", "")
        to_insert.append({
            "source_id": source_id,
            "post_id": None,
            "slug": slug,
            "title": rss_post["title"],
            "subtitle": rss_post.get("subtitle", ""),
            "url": rss_post["url"],
            "published_at": rss_post.get("published_at"),
            "updated_at": None,
            "audience": "everyone",
            "is_paywalled": False,
            "content_html": content_html,
            "content_markdown": html_to_markdown(content_html),
            "truncated_preview": "",
            "wordcount": None,
            "cover_image": "",
            "reaction_count": 0,
            "comment_count": 0,
            "full_content_fetched": bool(content_html and len(content_html) > 500),
        })

    if to_insert:
        inserted = db.insert_posts(to_insert)
        logger.info(f"  Inserted {inserted} posts for {source['name']}")

        # Log content stats
        full = sum(1 for p in to_insert if p.get("full_content_fetched"))
        paywalled = sum(1 for p in to_insert if p.get("is_paywalled"))
        logger.info(f"  Full content: {full}/{len(to_insert)} | Paywalled: {paywalled}")
    else:
        logger.info(f"  No posts to insert for {source['name']}")

    db.update_source_synced(source_id)
    return len(to_insert)


def show_status(db):
    """Print stats about what's in the DB."""
    import urllib.parse

    # Get source stats
    _, sources = db._request("GET", "substack_sources", params={"select": "id,name,last_synced_at"})
    sources = sources or []

    print("\n=== Substack Aggregator Status ===")
    for s in sources:
        _, posts = db._request("GET", "substack_posts", params={
            "select": "id,full_content_fetched,is_paywalled",
            "source_id": f"eq.{s['id']}",
        })
        posts = posts or []
        full = sum(1 for p in posts if p.get("full_content_fetched"))
        paywalled = sum(1 for p in posts if p.get("is_paywalled"))
        synced = s.get("last_synced_at", "never")[:19] if s.get("last_synced_at") else "never"
        print(f"\n  {s['name']} ({s['id']})")
        print(f"    Total: {len(posts)} | Full content: {full} | Paywalled: {paywalled}")
        print(f"    Last synced: {synced}")

    # Recent posts
    recent = db.get_recent_posts(limit=5)
    print(f"\n  Recent posts:")
    for p in recent:
        flag = "[FULL]" if p.get("full_content_fetched") else "[PARTIAL]"
        pub = (p.get("published_at") or "")[:10]
        print(f"    {flag} {pub} | {p.get('source_id')} | {p.get('title', '')[:60]}")
    print()


def export_to_obsidian(db, vault_path):
    """Export all posts as Markdown files to the Obsidian vault."""
    vault = Path(vault_path)
    substack_dir = vault / "06-Research" / "Substacks"
    substack_dir.mkdir(parents=True, exist_ok=True)

    _, posts = db._request("GET", "substack_posts", params={
        "select": "source_id,slug,title,subtitle,url,published_at,audience,content_markdown,cover_image",
        "order": "published_at.desc",
        "limit": "2000",
    })
    posts = posts or []

    # Group by source
    by_source = {}
    for p in posts:
        sid = p["source_id"]
        by_source.setdefault(sid, []).append(p)

    total = 0
    for source_id, source_posts in by_source.items():
        source_dir = substack_dir / source_id
        source_dir.mkdir(exist_ok=True)

        for post in source_posts:
            content = post.get("content_markdown", "")
            if not content:
                continue

            title = post.get("title", "Untitled")
            safe_title = re.sub(r'[<>:"/\\|?*\n\r]', "", title)[:80].strip()
            pub_date = (post.get("published_at") or "")[:10]
            filename = f"{pub_date} {safe_title}.md" if pub_date else f"{safe_title}.md"

            frontmatter = (
                f"---\n"
                f'title: "{title}"\n'
                f"source: {source_id}\n"
                f"url: {post.get('url', '')}\n"
                f"published: {pub_date}\n"
                f"audience: {post.get('audience', 'unknown')}\n"
                f"tags: [substack, {source_id}]\n"
                f"---\n\n"
            )

            (source_dir / filename).write_text(frontmatter + content, encoding="utf-8")
            total += 1

    logger.info(f"Exported {total} posts to {substack_dir}")
    print(f"\nExported {total} posts to:\n  {substack_dir}")
    return total


def main():
    parser = argparse.ArgumentParser(description="Substack content aggregator")
    parser.add_argument("--source", help="Only sync this source ID")
    parser.add_argument("--full-sync", action="store_true", help="Re-sync all posts (not just new)")
    parser.add_argument("--export-obsidian", action="store_true", help="Export to Obsidian vault")
    parser.add_argument("--status", action="store_true", help="Show DB stats")
    parser.add_argument("--no-auth", action="store_true", help="Skip Substack auth")
    args = parser.parse_args()

    from substack_client import SubstackClient
    from db import SupabaseDB

    client = SubstackClient()
    db = SupabaseDB()

    if args.status:
        show_status(db)
        return

    # Authenticate if credentials available
    if not args.no_auth:
        session_cookie = os.environ.get("SUBSTACK_SESSION_COOKIE", "")
        email = os.environ.get("SUBSTACK_EMAIL", "")
        password = os.environ.get("SUBSTACK_PASSWORD", "")

        if session_cookie:
            client._session_cookie = session_cookie
            client.authenticated = True
            logger.info("Using stored Substack session cookie")
        elif email and password:
            logger.info(f"Authenticating as {email}...")
            ok = client.login(email, password)
            if ok:
                logger.info("Substack authenticated")
                if client._session_cookie:
                    logger.info("Add SUBSTACK_SESSION_COOKIE to Doppler for faster future runs:")
                    logger.info(f"  doppler secrets set --project example-project --config prd SUBSTACK_SESSION_COOKIE='{client._session_cookie}'")
            else:
                logger.warning("Auth failed — fetching public content only")
        else:
            logger.info("No Substack credentials — fetching public content only")
            logger.info("Add SUBSTACK_EMAIL + SUBSTACK_PASSWORD to Doppler example-project for paid content")

    sources = load_sources(args.source)
    if not sources:
        logger.error(f"No active sources found{' for: ' + args.source if args.source else ''}")
        sys.exit(1)

    new_only = not args.full_sync
    total_new = 0

    for source in sources:
        try:
            new = ingest_source(client, db, source, new_only=new_only)
            total_new += new
        except Exception as e:
            logger.error(f"Failed to sync {source['id']}: {e}", exc_info=True)

    logger.info(f"\nDone. Total new posts ingested: {total_new}")

    if args.export_obsidian:
        vault_path = os.environ.get(
            "OBSIDIAN_VAULT_PATH",
            r"C:\Users\ethan.atchley\Documents\1st vault"
        )
        export_to_obsidian(db, vault_path)


if __name__ == "__main__":
    main()
