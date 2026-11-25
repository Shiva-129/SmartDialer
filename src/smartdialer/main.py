"""FastAPI entrypoint for SmartDialer REST API, Webhooks, WebSockets, and Operations Console."""

from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

from smartdialer.api.routes import router
from smartdialer.api.ws import ws_manager
from smartdialer.core.logging import get_logger
from smartdialer.db.migrations import run_migrations
from smartdialer.db.session import SyncSessionFactory, sync_engine
from smartdialer.services.worker import SmartDialerWorker

logger = get_logger("main")

STATIC_DIR = Path(__file__).parent / "static"


def create_app(
    enable_worker: bool = True,
    run_startup_migrations: bool = True,
) -> FastAPI:
    """Create and configure the FastAPI application instance."""

    @asynccontextmanager
    async def lifespan(app_instance: FastAPI) -> AsyncGenerator[None, None]:
        if run_startup_migrations:
            run_migrations(sync_engine)

        worker: SmartDialerWorker | None = None
        if enable_worker:
            worker = SmartDialerWorker(session_factory=SyncSessionFactory)
            app_instance.state.worker = worker
            await worker.start()
            logger.info("SmartDialer API + Worker started")
        else:
            app_instance.state.worker = None
            logger.info("SmartDialer API started (background worker disabled)")

        try:
            yield
        finally:
            if worker:
                await worker.stop()
                logger.info("SmartDialer API + Worker stopped cleanly")

    app_instance = FastAPI(
        title="SmartDialer API",
        description="Fault-Tolerant Progressive & Predictive Outbound Dialer with Mandatory Safety Controller",
        version="0.8.0",
        lifespan=lifespan,
    )

    app_instance.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app_instance.include_router(router)

    @app_instance.get("/", response_class=HTMLResponse, include_in_schema=False)
    def serve_dashboard() -> HTMLResponse:
        index_path = STATIC_DIR / "index.html"
        return HTMLResponse(content=index_path.read_text(encoding="utf-8"))

    @app_instance.websocket("/ws/campaigns/{campaign_id}")
    async def campaign_websocket(websocket: WebSocket, campaign_id: str) -> None:
        await ws_manager.connect(websocket, campaign_id)
        try:
            while True:
                # Keep socket open and accept optional ping/keepalive frames
                msg = await websocket.receive_text()
                if msg == "ping":
                    await websocket.send_text("pong")
        except WebSocketDisconnect:
            await ws_manager.disconnect(websocket, campaign_id)

    return app_instance


app = create_app()
