"""WebSocket connection manager for real-time campaign telemetry and event streaming."""

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Set
from fastapi import WebSocket

from smartdialer.core.logging import get_logger

logger = get_logger("websocket_manager")


class ConnectionManager:
    """Manages active WebSocket subscribers grouped by campaign_id."""

    def __init__(self) -> None:
        self._rooms: Dict[str, Set[WebSocket]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, campaign_id: str) -> None:
        await websocket.accept()
        async with self._lock:
            self._rooms.setdefault(campaign_id, set()).add(websocket)
        logger.info("WebSocket client connected", campaign_id=campaign_id)

    async def disconnect(self, websocket: WebSocket, campaign_id: str) -> None:
        async with self._lock:
            clients = self._rooms.get(campaign_id)
            if clients and websocket in clients:
                clients.remove(websocket)
            if clients is not None and len(clients) == 0:
                self._rooms.pop(campaign_id, None)
        logger.info("WebSocket client disconnected", campaign_id=campaign_id)

    async def broadcast_to_campaign(self, campaign_id: str, event_type: str, data: Dict[str, Any]) -> None:
        """Broadcast a structured JSON event to all WebSocket clients subscribed to campaign_id."""
        async with self._lock:
            targets: List[WebSocket] = list(self._rooms.get(campaign_id, set())) + list(
                self._rooms.get("all", set())
            )

        if not targets:
            return

        payload = {
            "event_type": event_type,
            "campaign_id": campaign_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": data,
        }

        stale_sockets: List[tuple[str, WebSocket]] = []
        for ws in targets:
            try:
                await ws.send_json(payload)
            except Exception:
                # Identify room for cleanup
                for room_key in (campaign_id, "all"):
                    if ws in self._rooms.get(room_key, set()):
                        stale_sockets.append((room_key, ws))

        if stale_sockets:
            async with self._lock:
                for room_key, ws in stale_sockets:
                    clients = self._rooms.get(room_key)
                    if clients and ws in clients:
                        clients.remove(ws)

    def active_connections_count(self, campaign_id: str | None = None) -> int:
        if campaign_id:
            return len(self._rooms.get(campaign_id, set()))
        return sum(len(s) for s in self._rooms.values())


ws_manager = ConnectionManager()
