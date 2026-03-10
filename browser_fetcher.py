"""
Browser-based fetcher for paid Substack content.
Uses Playwright to authenticate and extract full article body.
"""
import os, json, logging, time
from pathlib import Path

logger = logging.getLogger(__name__)

AUTH_STATE_FILE = Path(__file__).parent / ".browser_auth_state.json"


def get_browser_session(playwright, headless=True):
    browser = playwright.chromium.launch(headless=headless)
    context_kwargs = {
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122",
        "viewport": {"width": 1280, "height": 800},
    }
    if AUTH_STATE_FILE.exists():
        context_kwargs["storage_state"] = str(AUTH_STATE_FILE)
    context = browser.new_context(**context_kwargs)
    return browser, context


def login_and_save_state(email: str, password: str, headless: bool = True) -> bool:
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122",
            viewport={"width": 1280, "height": 800},
        )
        page = context.new_page()

        try:
            logger.info("Opening Substack sign-in...")
            page.goto("https://substack.com/sign-in", wait_until="networkidle", timeout=30000)
            time.sleep(2)

            # Fill email
            email_input = page.wait_for_selector('input[type="email"], input[name="email"]', timeout=10000)
            email_input.fill(email)
            time.sleep(0.5)

            # Click "Sign in with password" link instead of Continue
            try:
                pw_link = page.wait_for_selector('a:has-text("Sign in with password"), button:has-text("Sign in with password")', timeout=5000)
                pw_link.click()
                time.sleep(1)
            except Exception:
                logger.info("No 'Sign in with password' link, trying Continue...")
                page.press('input[type="email"]', "Enter")
                time.sleep(1)

            # Fill password
            try:
                pw_input = page.wait_for_selector('input[type="password"]', timeout=8000)
                pw_input.fill(password)
                time.sleep(0.5)
                pw_input.press("Enter")
            except Exception as e:
                logger.warning(f"Password field issue: {e}")
                page.screenshot(path="login_debug.png")
                return False

            # Wait for redirect to reader or home
            try:
                page.wait_for_url(lambda url: "reader" in url or "/home" in url or url == "https://substack.com/", timeout=20000)
                logger.info("Login successful")
            except Exception:
                # Check if we're actually logged in despite URL not matching
                page.screenshot(path="login_debug.png")
                # Check for user avatar/profile indicator
                try:
                    page.wait_for_selector('[data-testid="user-avatar"], .reader-header-user, img[alt*="avatar"]', timeout=5000)
                    logger.info("Login appears successful (found user element)")
                except Exception:
                    logger.warning("Can't confirm login - check login_debug.png")

            # Save auth state regardless
            context.storage_state(path=str(AUTH_STATE_FILE))
            logger.info(f"Auth state saved to {AUTH_STATE_FILE}")
            return True

        except Exception as e:
            logger.error(f"Login failed: {e}")
            try:
                page.screenshot(path="login_debug.png")
                logger.info("Debug screenshot: login_debug.png")
            except Exception:
                pass
            return False
        finally:
            browser.close()


def fetch_post_content_browser(url: str, headless: bool = True) -> dict | None:
    from playwright.sync_api import sync_playwright

    if not AUTH_STATE_FILE.exists():
        logger.error("No browser auth state. Run: python browser_fetcher.py --login")
        return None

    with sync_playwright() as p:
        browser, context = get_browser_session(p, headless=headless)
        page = context.new_page()

        try:
            logger.info(f"Fetching: {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_selector(".available-content, .body.markup, article", timeout=15000)
            time.sleep(2)

            content_html = page.evaluate("""
                () => {
                    const selectors = [
                        '.available-content',
                        '.body.markup',
                        'div[class*="body-markup"]',
                        '.post-content',
                        'article .markup',
                    ];
                    for (const sel of selectors) {
                        const el = document.querySelector(sel);
                        if (el) return el.innerHTML;
                    }
                    const article = document.querySelector('article');
                    return article ? article.innerHTML : '';
                }
            """)

            title = page.evaluate("""
                () => {
                    const h1 = document.querySelector('h1.post-title, h1[class*="title"], article h1');
                    return h1 ? h1.textContent.trim() : document.title;
                }
            """)

            is_paywalled = page.evaluate("""
                () => !!document.querySelector('.paywall-cta, .subscribe-prompt, [class*="paywall"]')
            """)

            if is_paywalled:
                logger.warning(f"Paywall still showing for {url}")

            logger.info(f"Fetched {len(content_html or '')} chars")
            return {"content_html": content_html or "", "title": title, "is_paywalled": is_paywalled, "url": url}

        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None
        finally:
            browser.close()


def manual_login_and_save_state() -> bool:
    """Open a visible browser, wait for user to log in manually, then save auth state."""
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            channel="chrome",
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            viewport={"width": 1280, "height": 800},
        )
        page = context.new_page()
        page.goto("https://substack.com/sign-in", wait_until="domcontentloaded")

        print(">>> Browser is open. Log in to your Substack account.")
        print(">>> Press ENTER here once you are fully logged in...", flush=True)
        input()

        context.storage_state(path=str(AUTH_STATE_FILE))
        logger.info(f"Auth state saved to {AUTH_STATE_FILE}")
        browser.close()
        return True


def fetch_all_paid_posts(sources: list, db, headless: bool = True):
    from playwright.sync_api import sync_playwright
    from substack_client import html_to_markdown

    if not AUTH_STATE_FILE.exists():
        logger.error("No browser auth state. Run: python browser_fetcher.py --login first")
        return 0

    total_updated = 0

    with sync_playwright() as p:
        browser, context = get_browser_session(p, headless=headless)
        page = context.new_page()

        for source in sources:
            source_id = source["id"]
            unfetched = db.get_unfetched_paid_posts(source_id)

            if not unfetched:
                logger.info(f"No unfetched paid posts for {source['name']}")
                continue

            logger.info(f"Fetching {len(unfetched)} paid posts for {source['name']}")

            for post in unfetched:
                url = post["url"]
                post_id = post["id"]
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_selector(".available-content, .body.markup, article", timeout=15000)
                    time.sleep(1.5)

                    content_html = page.evaluate("""
                        () => {
                            const selectors = ['.available-content', '.body.markup', 'div[class*="body-markup"]'];
                            for (const sel of selectors) {
                                const el = document.querySelector(sel);
                                if (el) return el.innerHTML;
                            }
                            const article = document.querySelector('article');
                            return article ? article.innerHTML : '';
                        }
                    """)

                    if not content_html or len(content_html) < 300:
                        logger.warning(f"Short/no content for {url} ({len(content_html or '')} chars)")
                        continue

                    content_md = html_to_markdown(content_html)
                    db.update_post_content(post_id, content_html, content_md)
                    logger.info(f"  Updated: {post.get('slug')} ({len(content_html)} chars)")
                    total_updated += 1
                    time.sleep(0.5)

                except Exception as e:
                    logger.warning(f"Failed {url}: {e}")

        browser.close()

    logger.info(f"Browser fetch complete. Updated {total_updated} posts.")
    return total_updated


if __name__ == "__main__":
    import argparse, sys
    sys.path.insert(0, str(Path(__file__).parent))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument("--login", action="store_true", help="Log in and save browser auth state")
    parser.add_argument("--manual-login", action="store_true", help="Open browser for manual login, then save auth state")
    parser.add_argument("--fetch-paid", action="store_true", help="Fetch all unfetched paid posts")
    parser.add_argument("--test-url", help="Test fetch a single URL")
    parser.add_argument("--no-headless", action="store_true", help="Show browser window")
    args = parser.parse_args()

    headless = not args.no_headless
    email = os.environ.get("SUBSTACK_EMAIL", "")
    password = os.environ.get("SUBSTACK_PASSWORD", "")

    if args.manual_login:
        ok = manual_login_and_save_state()
        print("Auth state saved!" if ok else "Failed")

    elif args.login:
        ok = login_and_save_state(email, password, headless=headless)
        print("Login:", "OK — auth state saved" if ok else "FAILED — check login_debug.png")

    elif args.test_url:
        result = fetch_post_content_browser(args.test_url, headless=headless)
        if result:
            print(f"Title: {result['title']}")
            print(f"Content: {len(result['content_html'])} chars | Paywalled: {result['is_paywalled']}")
            print(f"Preview:\n{result['content_html'][:500]}")

    elif args.fetch_paid:
        import json as _json
        from db import SupabaseDB

        with open(Path(__file__).parent / "sources.json") as f:
            sources = _json.load(f)
        db = SupabaseDB()
        updated = fetch_all_paid_posts(sources, db, headless=headless)
        print(f"Updated {updated} posts")
