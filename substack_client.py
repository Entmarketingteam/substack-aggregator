"""
Substack client — RSS + authenticated API access.
Handles free content via RSS and paid content via session cookie auth.
"""
import os, json, time, re, logging
import urllib.request, urllib.error, urllib.parse
import http.cookiejar
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from html.parser import HTMLParser

logger = logging.getLogger(__name__)


class HTMLToMarkdown(HTMLParser):
    """Minimal HTML -> Markdown converter."""

    def __init__(self):
        super().__init__()
        self.result = []
        self._in_tags = []
        self._skip = False
        self._pending_href = ""
        self._ol_counter = 1

    def handle_starttag(self, tag, attrs):
        self._in_tags.append(tag)
        attr_dict = dict(attrs)
        if tag == "h1":
            self.result.append("\n# ")
        elif tag == "h2":
            self.result.append("\n## ")
        elif tag == "h3":
            self.result.append("\n### ")
        elif tag == "h4":
            self.result.append("\n#### ")
        elif tag == "p":
            self.result.append("\n\n")
        elif tag == "br":
            self.result.append("  \n")
        elif tag == "strong" or tag == "b":
            self.result.append("**")
        elif tag == "em" or tag == "i":
            self.result.append("*")
        elif tag == "a":
            href = attr_dict.get("href", "")
            self.result.append("[")
            self._pending_href = href
        elif tag == "ul":
            self.result.append("\n")
        elif tag == "ol":
            self.result.append("\n")
            self._ol_counter = 1
        elif tag == "li":
            parent = self._in_tags[-2] if len(self._in_tags) >= 2 else ""
            if parent == "ol":
                self.result.append(f"\n{self._ol_counter}. ")
                self._ol_counter += 1
            else:
                self.result.append("\n- ")
        elif tag == "blockquote":
            self.result.append("\n> ")
        elif tag == "hr":
            self.result.append("\n\n---\n\n")
        elif tag == "img":
            alt = attr_dict.get("alt", "image")
            src = attr_dict.get("src", "")
            self.result.append(f"\n![{alt}]({src})\n")
        elif tag in ("script", "style"):
            self._skip = True

    def handle_endtag(self, tag):
        if self._in_tags and self._in_tags[-1] == tag:
            self._in_tags.pop()
        if tag in ("strong", "b"):
            self.result.append("**")
        elif tag in ("em", "i"):
            self.result.append("*")
        elif tag == "a":
            self.result.append(f"]({self._pending_href})")
            self._pending_href = ""
        elif tag in ("h1", "h2", "h3", "h4"):
            self.result.append("\n")
        elif tag in ("script", "style"):
            self._skip = False

    def handle_data(self, data):
        if not self._skip:
            self.result.append(data)

    def get_markdown(self):
        text = "".join(self.result)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()


def html_to_markdown(html: str) -> str:
    if not html:
        return ""
    parser = HTMLToMarkdown()
    parser.feed(html)
    return parser.get_markdown()


class SubstackClient:
    BASE_AUTH_URL = "https://substack.com"

    def __init__(self):
        self.cj = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.cj)
        )
        self.authenticated = False
        self._session_cookie = os.environ.get("SUBSTACK_SESSION_COOKIE", "")

    def _request(self, url: str, method="GET", data=None, headers=None):
        default_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122 Safari/537.36",
            "Accept": "application/json, text/html, */*",
        }
        if headers:
            default_headers.update(headers)
        if self._session_cookie:
            default_headers["Cookie"] = f"substack.sid={self._session_cookie}"

        req = urllib.request.Request(url, headers=default_headers, method=method)
        if data:
            req.data = json.dumps(data).encode()
            req.add_header("Content-Type", "application/json")

        try:
            with self.opener.open(req, timeout=30) as resp:
                content_type = resp.headers.get("Content-Type", "")
                raw = resp.read().decode("utf-8", errors="replace")
                if "json" in content_type:
                    return json.loads(raw)
                return raw
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            logger.warning(f"HTTP {e.code} for {url}: {body[:200]}")
            raise
        except Exception as e:
            logger.error(f"Request failed for {url}: {e}")
            raise

    def login(self, email: str, password: str) -> bool:
        """Authenticate with Substack. Stores session cookie."""
        try:
            resp = self._request(
                f"{self.BASE_AUTH_URL}/api/v1/login",
                method="POST",
                data={"email": email, "password": password, "captcha_response": ""},
            )
            for cookie in self.cj:
                if "substack.sid" in cookie.name:
                    self._session_cookie = cookie.value
                    self.authenticated = True
                    logger.info("Substack auth successful")
                    return True
            if isinstance(resp, dict) and resp.get("token"):
                self._session_cookie = resp["token"]
                self.authenticated = True
                return True
            logger.warning("Login succeeded but no session cookie found")
            return False
        except Exception as e:
            logger.error(f"Login failed: {e}")
            return False

    def get_posts(self, base_url: str, limit: int = 50, offset: int = 0) -> list:
        """Fetch post list from a publication's API."""
        url = f"{base_url}/api/v1/posts?limit={limit}&offset={offset}"
        try:
            result = self._request(url)
            return result if isinstance(result, list) else []
        except Exception as e:
            logger.error(f"Failed to fetch posts from {base_url}: {e}")
            return []

    def get_post_detail(self, base_url: str, slug: str):
        """Fetch full post content by slug."""
        url = f"{base_url}/api/v1/posts/{slug}"
        try:
            result = self._request(url)
            return result if isinstance(result, dict) else None
        except Exception as e:
            logger.error(f"Failed to fetch post {slug} from {base_url}: {e}")
            return None

    def get_all_posts(self, base_url: str) -> list:
        """Paginate through all posts."""
        all_posts = []
        offset = 0
        limit = 50
        while True:
            batch = self.get_posts(base_url, limit=limit, offset=offset)
            if not batch:
                break
            all_posts.extend(batch)
            logger.info(f"  {base_url}: fetched {len(all_posts)} posts total")
            if len(batch) < limit:
                break
            offset += limit
            time.sleep(0.5)
        return all_posts

    def parse_rss(self, rss_url: str) -> list:
        """Parse RSS feed, returns normalized post dicts."""
        try:
            req = urllib.request.Request(
                rss_url,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "application/rss+xml, application/xml, text/xml",
                },
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()

            root = ET.fromstring(raw)
            ns = {
                "content": "http://purl.org/rss/1.0/modules/content/",
                "dc": "http://purl.org/dc/elements/1.1/",
            }

            posts = []
            channel = root.find("channel")
            if channel is None:
                return posts

            for item in channel.findall("item"):
                title = item.findtext("title", "").strip()
                link = item.findtext("link", "").strip()
                description = item.findtext("description", "").strip()
                pub_date = item.findtext("pubDate", "")
                content_encoded = item.findtext("content:encoded", "", ns)
                slug = link.rstrip("/").split("/")[-1] if link else ""

                published_at = None
                if pub_date:
                    try:
                        from email.utils import parsedate_to_datetime
                        published_at = parsedate_to_datetime(pub_date).isoformat()
                    except Exception:
                        pass

                posts.append({
                    "title": title,
                    "url": link,
                    "slug": slug,
                    "subtitle": description,
                    "published_at": published_at,
                    "content_html": content_encoded or description,
                    "source": "rss",
                })
            return posts
        except Exception as e:
            logger.error(f"RSS parse failed for {rss_url}: {e}")
            return []

    @staticmethod
    def normalize_post(raw: dict, source_id: str) -> dict:
        """Normalize API post response to our schema."""
        link = raw.get("canonical_url") or raw.get("url", "")
        if not link:
            base = raw.get("publication", {}).get("base_url", "") if isinstance(raw.get("publication"), dict) else ""
            slug = raw.get("slug", "")
            link = f"{base}/p/{slug}" if base else ""

        content_html = raw.get("body_html") or ""
        wordcount = raw.get("wordcount") or 0
        is_truncated = (
            raw.get("free_unlock_required", False)
            or (wordcount > 100 and len(content_html) < 500)
        )

        return {
            "source_id": source_id,
            "post_id": str(raw.get("id", "")),
            "slug": raw.get("slug", ""),
            "title": raw.get("title", ""),
            "subtitle": raw.get("subtitle", ""),
            "url": link,
            "published_at": raw.get("post_date"),
            "updated_at": raw.get("updated_at"),
            "audience": raw.get("audience", "everyone"),
            "is_paywalled": raw.get("audience", "") == "only_paid",
            "content_html": content_html,
            "content_markdown": html_to_markdown(content_html),
            "truncated_preview": (raw.get("truncated_body_text") or "")[:500],
            "wordcount": wordcount or None,
            "cover_image": raw.get("cover_image") or "",
            "reaction_count": raw.get("reaction_count") or 0,
            "comment_count": raw.get("comment_count") or 0,
            "full_content_fetched": bool(content_html) and not is_truncated,
        }
