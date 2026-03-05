"""
Inject the existing Substack session cookie into browser auth state.
Avoids needing to re-login via the browser form.
"""
import os, json, urllib.parse
from pathlib import Path

AUTH_STATE_FILE = Path(__file__).parent / ".browser_auth_state.json"

cookie_raw = os.environ.get("SUBSTACK_SESSION_COOKIE", "")
cookie_decoded = urllib.parse.unquote(cookie_raw)

state = {
    "cookies": [
        {
            "name": "substack.sid",
            "value": cookie_decoded,
            "domain": ".substack.com",
            "path": "/",
            "secure": True,
            "httpOnly": True,
            "sameSite": "None",
        },
        {
            "name": "substack.sid",
            "value": cookie_decoded,
            "domain": ".dtcprophet.com",
            "path": "/",
            "secure": True,
            "httpOnly": True,
            "sameSite": "None",
        },
        {
            "name": "substack.sid",
            "value": cookie_decoded,
            "domain": ".arbletter.arbitrageandy.us",
            "path": "/",
            "secure": True,
            "httpOnly": True,
            "sameSite": "None",
        },
    ],
    "origins": []
}

AUTH_STATE_FILE.write_text(json.dumps(state, indent=2))
print(f"Auth state written with cookie: {cookie_decoded[:30]}...")
print(f"File: {AUTH_STATE_FILE}")
