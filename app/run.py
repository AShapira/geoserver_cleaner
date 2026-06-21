from __future__ import annotations

import logging
import os

import uvicorn

from app.config import Settings
from app.logging_utils import configure_app_logging


LOGGER = logging.getLogger("geoserver_cleaner.run")


def main() -> None:
    settings = Settings.from_env()
    configure_app_logging(settings)
    runtime = (os.getenv("APP_RUNTIME", "web").strip().lower() or "web")
    if runtime != "web":
        raise RuntimeError("Unsupported APP_RUNTIME value: {}".format(runtime))
    LOGGER.info(
        "Starting runtime entrypoint",
        extra={
            "event": "runtime_entrypoint_start",
            "runtime": runtime,
            "host": os.getenv("APP_HOST", "0.0.0.0"),
            "port": int(os.getenv("APP_PORT", "8000")),
        },
    )
    host = os.getenv("APP_HOST", "0.0.0.0")
    port = int(os.getenv("APP_PORT", "8000"))
    uvicorn.run("app.main:app", host=host, port=port, log_config=None, access_log=False)


if __name__ == "__main__":
    main()
