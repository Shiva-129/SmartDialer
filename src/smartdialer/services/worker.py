"""Background worker daemon for outbox dispatching and reconciliation sweeping (FIND-MED-02)."""

import asyncio
import signal
import sys
from typing import Any, Dict, List, Optional
from sqlalchemy.orm import Session, sessionmaker

from smartdialer.core.config import settings
from smartdialer.core.logging import get_logger
from smartdialer.db.session import sync_engine
from smartdialer.recovery.outbox_dispatcher import OutboxDispatcher
from smartdialer.recovery.reconciler import ReconciliationSweeper

logger = get_logger("worker_daemon")


class SmartDialerWorker:
    """
    Background worker service continuously running:
    1. Transactional Outbox dispatcher loop (polls and dispatches pending carrier calls).
    2. State reconciliation sweeper loop (reclaims stale reservations, expired wrap-ups, orphaned calls).
    
    Supports graceful shutdown, health inspection, and deterministic single-cycle execution.
    """

    def __init__(
        self,
        session_factory: Optional[sessionmaker] = None,
        outbox_poll_interval_sec: float = 0.25,
        reconcile_poll_interval_sec: float = 5.0,
    ) -> None:
        self.session_factory = session_factory or sessionmaker(bind=sync_engine, expire_on_commit=False)
        self.outbox_poll_interval = outbox_poll_interval_sec
        self.reconcile_poll_interval = reconcile_poll_interval_sec
        self._running = False
        self._tasks: List[asyncio.Task] = []

    @property
    def is_running(self) -> bool:
        return self._running

    def run_once(self) -> Dict[str, Any]:
        """
        Execute exactly one iteration of both the outbox dispatcher and reconciler.
        Ideal for deterministic test assertions.
        """
        with self.session_factory() as session:
            dispatched = OutboxDispatcher.dispatch_pending(session=session)
            sweeper_stats = ReconciliationSweeper.run_sweep(session=session)
            return {
                "dispatched_count": len(dispatched),
                "dispatched_ids": dispatched,
                "sweeper_stats": sweeper_stats,
            }

    async def _outbox_loop(self) -> None:
        logger.info("Outbox dispatcher loop started", interval=self.outbox_poll_interval)
        while self._running:
            try:
                def _sync_dispatch():
                    with self.session_factory() as session:
                        return OutboxDispatcher.dispatch_pending(session=session)

                dispatched = await asyncio.to_thread(_sync_dispatch)
                if dispatched:
                    logger.debug("Dispatched outbox batch", count=len(dispatched))
            except Exception as exc:
                logger.error("Error in outbox dispatcher loop", error=str(exc))
            await asyncio.sleep(self.outbox_poll_interval)

    async def _reconciler_loop(self) -> None:
        logger.info("Reconciliation sweeper loop started", interval=self.reconcile_poll_interval)
        while self._running:
            try:
                def _sync_sweep():
                    with self.session_factory() as session:
                        return ReconciliationSweeper.run_sweep(session=session)

                stats = await asyncio.to_thread(_sync_sweep)
                total = (
                    stats.reclaimed_agents_count
                    + stats.reclaimed_borrowers_count
                    + stats.timed_out_calls_count
                    + stats.stale_outbox_unlocked_count
                )
                if total > 0:
                    logger.info(
                        "Reconciliation sweep reclaimed stale entities",
                        reclaimed_agents=stats.reclaimed_agents_count,
                        reclaimed_borrowers=stats.reclaimed_borrowers_count,
                        timed_out_calls=stats.timed_out_calls_count,
                    )
            except Exception as exc:
                logger.error("Error in reconciliation sweeper loop", error=str(exc))
            await asyncio.sleep(self.reconcile_poll_interval)

    async def start(self) -> None:
        """Start background daemon loops."""
        if self._running:
            return
        self._running = True
        logger.info("SmartDialer background worker daemon starting")
        self._tasks = [
            asyncio.create_task(self._outbox_loop(), name="outbox_dispatcher_loop"),
            asyncio.create_task(self._reconciler_loop(), name="reconciler_sweeper_loop"),
        ]

    async def stop(self) -> None:
        """Gracefully stop background daemon loops."""
        if not self._running:
            return
        logger.info("SmartDialer background worker daemon stopping")
        self._running = False
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        logger.info("SmartDialer background worker daemon stopped")


def run_worker_main():
    """CLI entry point for running the background worker daemon standalone."""
    worker = SmartDialerWorker()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _handle_exit(sig, frame):
        logger.info(f"Received signal {sig}, initiating shutdown...")
        loop.create_task(worker.stop())

    if sys.platform != "win32":
        loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(worker.stop()))
        loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(worker.stop()))
    else:
        signal.signal(signal.SIGINT, _handle_exit)
        signal.signal(signal.SIGTERM, _handle_exit)

    async def _run():
        await worker.start()
        while worker.is_running:
            await asyncio.sleep(1)

    try:
        loop.run_until_complete(_run())
    finally:
        loop.close()


if __name__ == "__main__":
    run_worker_main()
