from __future__ import annotations

import logging
import threading
from typing import Sequence

from app import db
from app.config import Settings
from app.services import deletion, inventory


LOGGER = logging.getLogger("cleanup_app.jobs")


class JobManager:
    def __init__(self, settings: Settings, db_path: str) -> None:
        self.settings = settings
        self.db_path = db_path
        self._lock = threading.Lock()

    def ensure_idle(self) -> None:
        running = db.list_running_jobs(self.db_path)
        if running:
            raise RuntimeError("Another job is already queued or running.")

    def start_scan(self, excluded_workspaces_raw: str = "") -> int:
        with self._lock:
            self.ensure_idle()
            metadata = {"excluded_workspaces": excluded_workspaces_raw}
            job_id = db.create_job(self.db_path, "scan", "Inventory scan queued", metadata=metadata)
            thread = threading.Thread(target=self._run_scan, args=(job_id, excluded_workspaces_raw), daemon=True)
            thread.start()
            return job_id

    def start_delete(self, run_id: int, store_ids: Sequence[int], excluded_workspaces_raw: str = "") -> int:
        with self._lock:
            self.ensure_idle()
            metadata = {
                "run_id": run_id,
                "store_ids": list(store_ids),
                "excluded_workspaces": excluded_workspaces_raw,
            }
            job_id = db.create_job(self.db_path, "delete", "Delete job queued", metadata=metadata)
            thread = threading.Thread(
                target=self._run_delete,
                args=(job_id, run_id, list(store_ids), excluded_workspaces_raw),
                daemon=True,
            )
            thread.start()
            return job_id

    def _run_scan(self, job_id: int, excluded_workspaces_raw: str) -> None:
        try:
            db.update_job(self.db_path, job_id, status="running", message="Inventory scan running", started=True)
            run_id = inventory.run_inventory_scan(
                self.settings,
                self.db_path,
                excluded_workspaces_raw=excluded_workspaces_raw,
            )
            db.update_job(
                self.db_path,
                job_id,
                status="completed",
                message="Inventory scan completed",
                run_id=run_id,
                finished=True,
            )
        except Exception as exc:
            LOGGER.exception("Scan job %s failed", job_id)
            db.update_job(
                self.db_path,
                job_id,
                status="failed",
                message="Inventory scan failed",
                error_text=str(exc),
                finished=True,
            )

    def _run_delete(
        self,
        job_id: int,
        run_id: int,
        store_ids: Sequence[int],
        excluded_workspaces_raw: str,
    ) -> None:
        try:
            db.update_job(self.db_path, job_id, status="running", message="Delete job running", started=True)
            result = deletion.execute_delete_job(self.db_path, self.settings, run_id, store_ids)
            db.update_job(
                self.db_path,
                job_id,
                status="running",
                message="Delete completed. Refreshing inventory snapshot",
                metadata=result,
            )
            refreshed_run_id = inventory.run_inventory_scan(
                self.settings,
                self.db_path,
                excluded_workspaces_raw=excluded_workspaces_raw,
            )
            metadata = dict(result)
            metadata["refreshed_run_id"] = refreshed_run_id
            db.update_job(
                self.db_path,
                job_id,
                status="completed",
                message="Delete job completed and inventory refreshed",
                run_id=refreshed_run_id,
                metadata=metadata,
                finished=True,
            )
        except Exception as exc:
            LOGGER.exception("Delete job %s failed", job_id)
            db.update_job(
                self.db_path,
                job_id,
                status="failed",
                message="Delete job failed",
                error_text=str(exc),
                finished=True,
            )
