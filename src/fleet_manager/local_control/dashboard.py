"""Small read-only local dashboard; it never embeds or stores credentials."""

import base64
import hashlib

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter()

_SCRIPT = """\
'use strict';
let token = '';
const output = document.getElementById('status');
const message = document.getElementById('message');
async function refresh() {
  if (!token) return;
  try {
    const response = await fetch('/v1/node/status', {
      headers: {'Authorization': 'Bearer ' + token}, cache: 'no-store', redirect: 'error'
    });
    if (!response.ok) throw new Error('Status request failed (' + response.status + ')');
    output.textContent = JSON.stringify(await response.json(), null, 2);
    message.textContent = 'Connected';
  } catch (error) {
    output.textContent = '';
    message.textContent = String(error.message);
  }
}
document.getElementById('connect').addEventListener('submit', function (event) {
  event.preventDefault();
  const input = document.getElementById('token');
  token = input.value.trim();
  input.value = '';
  refresh();
});
document.getElementById('disconnect').addEventListener('click', function () {
  token = '';
  output.textContent = '';
  message.textContent = 'Disconnected';
});
setInterval(refresh, 5000);
"""
_HASH = base64.b64encode(hashlib.sha256(_SCRIPT.encode()).digest()).decode()
_HTML = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>Idle-Herd status</title></head><body>
<main><h1>Idle-Herd status</h1><p>Read-only local status. Enter your read token to connect.
Find it in the control-credentials.json file in your Idle-Herd data directory.</p>
<form id="connect"><label for="token">Read token</label>
<input id="token" type="password" autocomplete="off" required>
<button type="submit">Connect</button></form>
<button id="disconnect" type="button">Disconnect</button>
<p id="message" role="status">Disconnected</p><pre id="status"></pre></main>
<script>{_SCRIPT}</script></body></html>"""


@router.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
async def dashboard():
    return HTMLResponse(
        _HTML,
        headers={
            "Content-Security-Policy": (
                f"default-src 'none'; script-src 'sha256-{_HASH}'; connect-src 'self'; "
                "base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
            ),
            "Cache-Control": "no-store",
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
            "Referrer-Policy": "no-referrer",
        },
    )
