from __future__ import annotations

import logging
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urljoin
from urllib.request import Request

from app.config import Settings
from app.reporting.core import GeoServerClient


LOGGER = logging.getLogger("geoserver_cleaner.geoserver")


def delete_store(settings: Settings, workspace: str, store_kind: str, store_name: str) -> None:
    client = GeoServerClient(
        base_url=settings.geoserver_url,
        username=settings.geoserver_username,
        password=settings.geoserver_password,
        timeout=settings.timeout,
        insecure=settings.insecure,
    )
    workspace_q = quote(workspace, safe="")
    store_q = quote(store_name, safe="")
    endpoint = "rest/workspaces/{}/{}/{}?recurse=true&purge=all".format(workspace_q, store_kind, store_q)
    url = urljoin(client.base_url, endpoint)
    request = Request(
        url,
        method="DELETE",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
    )
    LOGGER.info(
        "Sending GeoServer delete request",
        extra={
            "event": "geoserver_delete_request",
            "workspace": workspace,
            "store_name": store_name,
            "store_kind": store_kind,
            "url": url,
            "timeout": client.timeout,
        },
    )
    try:
        with client.opener.open(request, timeout=client.timeout):
            LOGGER.info(
                "GeoServer delete request completed",
                extra={
                    "event": "geoserver_delete_complete",
                    "workspace": workspace,
                    "store_name": store_name,
                    "store_kind": store_kind,
                },
            )
            return
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:300]
        LOGGER.error(
            "GeoServer delete request failed with HTTP error",
            extra={
                "event": "geoserver_delete_http_error",
                "workspace": workspace,
                "store_name": store_name,
                "store_kind": store_kind,
                "status_code": exc.code,
                "detail": detail,
            },
        )
        raise RuntimeError(
            "GeoServer delete failed with HTTP {} for {}/{} ({}): {}".format(
                exc.code,
                workspace,
                store_name,
                store_kind,
                detail,
            )
        ) from exc
    except URLError as exc:
        LOGGER.error(
            "GeoServer delete request failed with URL error",
            extra={
                "event": "geoserver_delete_url_error",
                "workspace": workspace,
                "store_name": store_name,
                "store_kind": store_kind,
                "reason": str(exc.reason),
            },
        )
        raise RuntimeError(
            "GeoServer delete failed for {}/{} ({}): {}".format(
                workspace,
                store_name,
                store_kind,
                exc.reason,
            )
        ) from exc
