import itertools
import json
from datetime import datetime
from urllib.parse import urlsplit, urlunsplit

import httpx
import websockets

ENTITY_CHUNK_SIZE = 40


def websocket_url(base_url: str) -> str:
    parts = urlsplit(base_url)
    scheme = "wss" if parts.scheme in ("https", "wss") else "ws"
    return urlunsplit((scheme, parts.netloc, "/api/websocket", "", ""))


def rest_url(base_url: str) -> str:
    parts = urlsplit(base_url)
    scheme = "https" if parts.scheme in ("https", "wss") else "http"
    return urlunsplit((scheme, parts.netloc, "", "", ""))


def _chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def fetch_all_entity_ids(base_url: str, token: str) -> list[str]:
    resp = httpx.get(
        f"{base_url.rstrip('/')}/api/states",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    resp.raise_for_status()
    return [state["entity_id"] for state in resp.json()]


def fetch_history(
    base_url: str, token: str, start: datetime, end: datetime, entities: list[str]
) -> list[dict]:
    url = f"{base_url.rstrip('/')}/api/history/period/{start.isoformat()}"
    rows: list[dict] = []
    for chunk in _chunked(entities, ENTITY_CHUNK_SIZE):
        resp = httpx.get(
            url,
            params={"end_time": end.isoformat(), "filter_entity_id": ",".join(chunk)},
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        for entity_states in resp.json():
            rows.extend(entity_states)
    return rows


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
