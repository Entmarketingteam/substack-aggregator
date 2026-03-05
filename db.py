"""
Supabase database operations for substack aggregator.
"""
import os, json, logging
import urllib.request, urllib.parse

logger = logging.getLogger(__name__)


def _get_supabase_config():
    supabase_url = os.environ.get("SUPABASE_URL", "")
    supabase_key = (
        os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        or os.environ.get("SUPABASE_SERVICE_KEY", "")
    )

    if not supabase_url or not supabase_key:
        raise RuntimeError("Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in Doppler (example-project)")

    return supabase_url.rstrip("/"), supabase_key


class SupabaseDB:
    def __init__(self):
        self.url, self.key = _get_supabase_config()
        self.headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        }

    def _request(self, method: str, path: str, data=None, params=None) -> tuple[int, any]:
        url = f"{self.url}/rest/v1/{path}"
        if params:
            url += "?" + urllib.parse.urlencode(params)

        headers = dict(self.headers)
        if method == "GET":
            headers["Accept"] = "application/json"
            headers.pop("Prefer", None)

        req = urllib.request.Request(url, headers=headers, method=method)
        if data is not None:
            req.data = json.dumps(data).encode()

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read().decode()
                return resp.status, json.loads(body) if body else None
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            logger.error(f"Supabase {method} {path}: {e.code} — {body[:300]}")
            raise

    def upsert_source(self, source: dict) -> bool:
        headers_upsert = dict(self.headers)
        headers_upsert["Prefer"] = "resolution=merge-duplicates,return=minimal"
        url = f"{self.url}/rest/v1/substack_sources"
        req = urllib.request.Request(url, headers=headers_upsert, method="POST")
        req.data = json.dumps(source).encode()
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.status in (200, 201)
        except urllib.error.HTTPError as e:
            logger.error(f"upsert_source failed: {e.code} — {e.read().decode()[:200]}")
            return False

    def get_existing_slugs(self, source_id: str) -> set[str]:
        """Return set of slugs already in DB for a source."""
        status, data = self._request(
            "GET",
            "substack_posts",
            params={"select": "slug", "source_id": f"eq.{source_id}"},
        )
        if data:
            return {row["slug"] for row in data}
        return set()

    def get_unfetched_paid_posts(self, source_id: str) -> list[dict]:
        """Get paid posts that don't have full content yet."""
        status, data = self._request(
            "GET",
            "substack_posts",
            params={
                "select": "id,source_id,slug,url",
                "source_id": f"eq.{source_id}",
                "is_paywalled": "eq.true",
                "full_content_fetched": "eq.false",
                "order": "published_at.desc",
            },
        )
        return data or []

    def insert_posts(self, posts: list[dict]) -> int:
        """Bulk insert posts, skip duplicates by URL. Returns count inserted."""
        if not posts:
            return 0

        headers_upsert = dict(self.headers)
        headers_upsert["Prefer"] = "resolution=ignore-duplicates,return=minimal"
        url = f"{self.url}/rest/v1/substack_posts"
        req = urllib.request.Request(url, headers=headers_upsert, method="POST")
        req.data = json.dumps(posts).encode()

        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                logger.info(f"Inserted {len(posts)} posts: HTTP {resp.status}")
                return len(posts)
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            logger.error(f"insert_posts failed: {e.code} — {body[:300]}")
            # Try one by one if bulk fails
            inserted = 0
            for post in posts:
                try:
                    req2 = urllib.request.Request(url, headers=headers_upsert, method="POST")
                    req2.data = json.dumps(post).encode()
                    with urllib.request.urlopen(req2, timeout=30):
                        inserted += 1
                except Exception as ex:
                    logger.warning(f"Skip post {post.get('slug')}: {ex}")
            return inserted

    def update_post_content(self, post_id: int, content_html: str, content_md: str, wordcount: int = None):
        """Update a post's content after fetching paid article."""
        data = {
            "content_html": content_html,
            "content_markdown": content_md,
            "full_content_fetched": True,
        }
        if wordcount:
            data["wordcount"] = wordcount

        url = f"{self.url}/rest/v1/substack_posts?id=eq.{post_id}"
        headers_patch = dict(self.headers)
        headers_patch["Prefer"] = "return=minimal"
        req = urllib.request.Request(url, headers=headers_patch, method="PATCH")
        req.data = json.dumps(data).encode()
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.status in (200, 204)
        except urllib.error.HTTPError as e:
            logger.error(f"update_post_content failed: {e.read().decode()[:200]}")
            return False

    def update_source_synced(self, source_id: str):
        from datetime import datetime, timezone
        url = f"{self.url}/rest/v1/substack_sources?id=eq.{source_id}"
        req = urllib.request.Request(url, headers=self.headers, method="PATCH")
        req.data = json.dumps({"last_synced_at": datetime.now(timezone.utc).isoformat()}).encode()
        try:
            with urllib.request.urlopen(req, timeout=10):
                pass
        except Exception as e:
            logger.warning(f"Failed to update sync time: {e}")

    def get_recent_posts(self, limit: int = 20, source_id: str = None) -> list[dict]:
        params = {
            "select": "title,url,published_at,audience,full_content_fetched,source_id",
            "order": "published_at.desc",
            "limit": str(limit),
        }
        if source_id:
            params["source_id"] = f"eq.{source_id}"
        _, data = self._request("GET", "substack_posts", params=params)
        return data or []
