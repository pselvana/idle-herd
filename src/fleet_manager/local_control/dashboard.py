"""Authenticated, read-only local dashboard using the menu bar snapshot protocol."""

import base64
import hashlib
from importlib.resources import files

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter()

_ASSETS = files("fleet_manager.local_control").joinpath("assets")
_SCRIPT = _ASSETS.joinpath("dashboard.js").read_text()
_STYLE = _ASSETS.joinpath("dashboard.css").read_text()
_HASH = base64.b64encode(hashlib.sha256(_SCRIPT.encode()).digest()).decode()
_STYLE_HASH = base64.b64encode(hashlib.sha256(_STYLE.encode()).digest()).decode()
_HTML = (
    _ASSETS.joinpath("dashboard.html")
    .read_text()
    .replace("STYLE", _STYLE)
    .replace("SCRIPT", _SCRIPT)
)


@router.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
async def dashboard():
    return HTMLResponse(
        _HTML,
        headers={
            "Content-Security-Policy": (
                f"default-src 'none'; script-src 'sha256-{_HASH}'; "
                f"style-src 'sha256-{_STYLE_HASH}'; connect-src 'self'; "
                "base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
            ),
            "Cache-Control": "no-store",
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
            "Referrer-Policy": "no-referrer",
        },
    )
