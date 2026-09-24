import itertools
import json
from urllib.parse import urlsplit, urlunsplit

import websockets


def websocket_url(base_url: str) -> str:
    parts = urlsplit(base_url)
    scheme = "wss" if parts.scheme in ("https", "wss") else "ws"
    return urlunsplit((scheme, parts.netloc, "/api/websocket", "", ""))


async def fetch_registries(url: str, token: str, commands: list[str]) -> dict[str, list[dict]]:
    ids = itertools.count(1)

    async with websockets.connect(url) as conn:
        msg = json.loads(await conn.recv())
        if msg["type"] != "auth_required":
            raise RuntimeError(f"unexpected handshake message: {msg}")

        await conn.send(json.dumps({"type": "auth", "access_token": token}))
        msg = json.loads(await conn.recv())
        if msg["type"] != "auth_ok":
            raise RuntimeError(f"authentication failed: {msg}")

        results = {}
        for command in commands:
            await conn.send(json.dumps({"id": next(ids), "type": command}))
            msg = json.loads(await conn.recv())
            if not msg.get("success"):
                raise RuntimeError(f"{command} failed: {msg}")
            results[command] = msg["result"]

        return results
