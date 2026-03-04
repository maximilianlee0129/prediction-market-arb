"""
WebSocket broadcast manager for pushing live arb updates to frontend clients.
"""
from __future__ import annotations

import json
from typing import Any

from fastapi import WebSocket

from backend.utils.logger import get_logger

logger = get_logger(__name__)


class ConnectionManager:
    """Manages active WebSocket connections and broadcasts messages."""

    def __init__(self) -> None:
        self._connections: list[WebSocket] = []

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self._connections.append(ws)
        logger.info(f"WS client connected ({len(self._connections)} total)")

    def disconnect(self, ws: WebSocket) -> None:
        if ws in self._connections:
            self._connections.remove(ws)
        logger.info(f"WS client disconnected ({len(self._connections)} total)")

    async def broadcast(self, data: dict[str, Any]) -> None:
        """Send JSON data to all connected clients, removing dead connections."""
        dead: list[WebSocket] = []
        payload = json.dumps(data)

        for ws in self._connections:
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(ws)

        for ws in dead:
            self.disconnect(ws)

    @property
    def client_count(self) -> int:
        return len(self._connections)


manager = ConnectionManager()
