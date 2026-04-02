from __future__ import annotations

from urllib.error import HTTPError, URLError
from urllib.parse import quote, urljoin
from urllib.request import Request

import geoserver_store_report as report

from app.config import Settings


def delete_store(settings: Settings, workspace: str, store_kind: str, store_name: str) -> None:
    client = report.GeoServerClient(
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
    try:
        with client.opener.open(request, timeout=client.timeout):
            return
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:300]
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
        raise RuntimeError(
            "GeoServer delete failed for {}/{} ({}): {}".format(
                workspace,
                store_name,
                store_kind,
                exc.reason,
            )
        ) from exc
