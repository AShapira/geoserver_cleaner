from __future__ import annotations

import os

import uvicorn


def main() -> None:
    runtime = (os.getenv("APP_RUNTIME", "web").strip().lower() or "web")
    if runtime == "mcp":
        from app.mcp.server import run_stdio_server

        run_stdio_server()
        return
    if runtime != "web":
        raise RuntimeError("Unsupported APP_RUNTIME value: {}".format(runtime))
    host = os.getenv("APP_HOST", "0.0.0.0")
    port = int(os.getenv("APP_PORT", "8000"))
    uvicorn.run("app.main:app", host=host, port=port)


if __name__ == "__main__":
    main()
