"""CSV and HTML rendering utilities for GeoServer Cleaner reports."""

from __future__ import annotations

import csv
import html
import io
import json
import os
from datetime import datetime
from typing import List, Sequence

from app.reporting.core import bytes_to_gb

REPORT_TITLE = "GeoServer Cleaner Report"

HTML_COLUMNS = [
    ("row_kind", "Row Type", "text"),
    ("workspace", "Workspace", "text"),
    ("store_name", "Store", "text"),
    ("store_type", "Store Type", "text"),
    ("layer_names", "Layer Names", "text"),
    ("configured_path", "Configured Path", "text"),
    ("resolved_path", "Resolved Path", "text"),
    ("path_kind", "Path Kind", "text"),
    ("size_bytes", "Size (Bytes)", "number"),
    ("size_gb", "Size (GB)", "number"),
    ("file_count", "Files", "number"),
    ("status", "Status", "text"),
    ("notes", "Notes", "text"),
]

def write_csv(path: str, rows: Sequence[dict]) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent and not os.path.isdir(parent):
        os.makedirs(parent, exist_ok=True)

    with open(path, "wb") as handle:
        handle.write(build_csv_bytes(rows))


def build_csv_bytes(rows: Sequence[dict]) -> bytes:
    fieldnames = [column[0] for column in HTML_COLUMNS]
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue().encode("utf-8-sig")


def build_html_summary(rows: Sequence[dict], excluded_workspaces: Sequence[str]) -> dict:
    store_rows = [row for row in rows if row["row_kind"] == "store"]
    orphan_rows = [row for row in rows if row["row_kind"] == "orphaned"]
    ok_rows = [row for row in store_rows if row["status"] == "ok"]
    issue_rows = [row for row in store_rows if row["status"] != "ok"]
    tracked_size_bytes = sum(int(row["size_bytes"]) for row in ok_rows)
    return {
        "store_count": len(store_rows),
        "orphan_count": len(orphan_rows),
        "issue_count": len(issue_rows),
        "tracked_size_gb": bytes_to_gb(tracked_size_bytes),
        "excluded_workspaces": ", ".join(excluded_workspaces) if excluded_workspaces else "None",
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def json_for_html_script(value) -> str:
    return json.dumps(value, ensure_ascii=False).replace("</", "<\\/")


def build_html_row_payload(rows: Sequence[dict]) -> List[dict]:
    payload: List[dict] = []
    keys = [column[0] for column in HTML_COLUMNS]
    for row in rows:
        payload.append({key: row.get(key, "") for key in keys})
    return payload


def build_html_report_text(
    rows: Sequence[dict],
    excluded_workspaces: Sequence[str],
    geoserver_url: str,
    data_dir: str,
) -> str:
    summary = build_html_summary(rows, excluded_workspaces)
    header_cells = []
    for key, label, sort_type in HTML_COLUMNS:
        header_cells.append(
            '<th class="sortable" data-key="{key}" data-type="{sort_type}">{label}<span class="sort-indicator"></span></th>'.format(
                key=html.escape(key),
                sort_type=html.escape(sort_type),
                label=html.escape(label),
            )
        )
    column_data = json_for_html_script(
        [{"key": key, "label": label, "type": sort_type} for key, label, sort_type in HTML_COLUMNS]
    )
    row_data = json_for_html_script(build_html_row_payload(rows))

    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>GeoServer Cleaner Report</title>
  <style>
    :root {{
      --paper: #f6f1e8;
      --ink: #1e2a32;
      --muted: #5d6b75;
      --line: #dccfbe;
      --panel: #fffdf8;
      --accent: #a2471b;
      --ok: #e2f1e8;
      --warn: #fff1d6;
      --error: #fde3e1;
      --orphan: #f3e7ff;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", "Trebuchet MS", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top right, rgba(162, 71, 27, 0.14), transparent 24rem),
        linear-gradient(180deg, #f9f5ee 0%, var(--paper) 100%);
    }}
    .page {{ max-width: 1480px; margin: 0 auto; padding: 2rem 1.5rem 3rem; }}
    .hero {{
      display: grid;
      gap: 1rem;
      padding: 1.5rem;
      border: 1px solid var(--line);
      border-radius: 1.25rem;
      background: linear-gradient(135deg, rgba(255,255,255,0.95), rgba(245,236,222,0.92));
      box-shadow: 0 18px 45px rgba(40, 37, 30, 0.08);
    }}
    .hero h1 {{ margin: 0; font-size: clamp(1.8rem, 2.8vw, 3rem); letter-spacing: 0.02em; }}
    .hero p {{ margin: 0; color: var(--muted); max-width: 72rem; line-height: 1.55; }}
    .summary-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 0.9rem;
      margin-top: 1rem;
    }}
    .summary-card {{
      padding: 1rem;
      border-radius: 1rem;
      border: 1px solid var(--line);
      background: var(--panel);
    }}
    .summary-card .label {{
      font-size: 0.82rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
    }}
    .summary-card .value {{ margin-top: 0.35rem; font-size: 1.45rem; font-weight: 700; }}
    .meta {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 0.9rem;
      margin-top: 1.1rem;
    }}
    .meta-card {{
      padding: 1rem;
      border-radius: 1rem;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.78);
    }}
    .meta-card strong {{ display: block; margin-bottom: 0.35rem; }}
    .toolbar {{
      display: grid;
      grid-template-columns: minmax(0, 1.7fr) minmax(18rem, 1fr);
      gap: 1rem;
      align-items: end;
      margin: 1.5rem 0 1rem;
    }}
    .toolbar-copy .hint {{ color: var(--muted); font-size: 0.95rem; line-height: 1.5; }}
    .toolbar-actions {{
      display: flex;
      gap: 0.9rem;
      flex-wrap: wrap;
      align-items: center;
      justify-content: flex-end;
    }}
    .search {{
      flex: 1 1 18rem;
      min-width: min(24rem, 100%);
      padding: 0.85rem 1rem;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(255,255,255,0.85);
      font: inherit;
    }}
    .page-size-shell {{
      display: inline-flex;
      align-items: center;
      gap: 0.55rem;
      padding: 0.35rem 0.35rem 0.35rem 0.9rem;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(255,255,255,0.85);
      color: var(--muted);
      font-size: 0.95rem;
      white-space: nowrap;
    }}
    .page-size {{
      border: 0;
      background: transparent;
      color: var(--ink);
      font: inherit;
      padding-right: 0.2rem;
    }}
    .results-bar {{
      display: flex;
      gap: 0.9rem;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 0.85rem;
    }}
    .result-summary {{
      color: var(--muted);
      font-size: 0.95rem;
    }}
    .pager {{
      display: flex;
      gap: 0.55rem;
      align-items: center;
      flex-wrap: wrap;
    }}
    .pager-button {{
      padding: 0.6rem 0.85rem;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(255,255,255,0.92);
      color: var(--ink);
      font: inherit;
      cursor: pointer;
    }}
    .pager-button:disabled {{
      opacity: 0.45;
      cursor: not-allowed;
    }}
    .page-status {{
      min-width: 7rem;
      text-align: center;
      color: var(--muted);
      font-size: 0.95rem;
    }}
    .table-shell {{
      overflow: auto;
      border-radius: 1rem;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.88);
      box-shadow: 0 14px 32px rgba(30, 42, 50, 0.08);
    }}
    table {{ width: 100%; border-collapse: collapse; min-width: 1200px; }}
    thead th {{
      position: sticky;
      top: 0;
      z-index: 1;
      padding: 0.95rem 0.9rem;
      background: #eadfce;
      text-align: left;
      font-size: 0.88rem;
      border-bottom: 1px solid var(--line);
      cursor: pointer;
      user-select: none;
      white-space: nowrap;
    }}
    tbody td {{
      padding: 0.82rem 0.9rem;
      vertical-align: top;
      border-bottom: 1px solid rgba(220, 207, 190, 0.65);
      font-size: 0.92rem;
      line-height: 1.45;
      word-break: break-word;
    }}
    tbody tr:nth-child(even) {{ background: rgba(250, 246, 239, 0.6); }}
    tbody tr.status-ok {{ background: var(--ok); }}
    tbody tr.status-missing,
    tbody tr.status-unresolved {{ background: var(--warn); }}
    tbody tr.status-error {{ background: var(--error); }}
    tbody tr.status-orphaned {{ background: var(--orphan); }}
    .sort-indicator {{ display: inline-block; width: 0.9rem; margin-left: 0.35rem; color: var(--accent); }}
    th[data-direction="asc"] .sort-indicator::after {{ content: "↑"; }}
    th[data-direction="desc"] .sort-indicator::after {{ content: "↓"; }}
    .empty-state {{ padding: 1rem 0; color: var(--muted); }}
    @media (max-width: 920px) {{
      .toolbar {{
        grid-template-columns: 1fr;
      }}
      .toolbar-actions {{
        justify-content: stretch;
      }}
      .search {{
        min-width: 100%;
      }}
      .results-bar {{
        align-items: stretch;
      }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <div>
        <h1>GeoServer Cleaner Report</h1>
        <p>Sortable inventory of included GeoServer stores and orphaned disk usage. Click any column header to sort, and use the filter box to narrow the table during review.</p>
      </div>
      <div class="summary-grid">
        <div class="summary-card"><div class="label">Store Rows</div><div class="value">{store_count}</div></div>
        <div class="summary-card"><div class="label">Orphan Rows</div><div class="value">{orphan_count}</div></div>
        <div class="summary-card"><div class="label">Store Issues</div><div class="value">{issue_count}</div></div>
        <div class="summary-card"><div class="label">Tracked Size</div><div class="value">{tracked_size_gb} GB</div></div>
      </div>
      <div class="meta">
        <div class="meta-card"><strong>GeoServer URL</strong><span>{geoserver_url}</span></div>
        <div class="meta-card"><strong>Data Directory</strong><span>{data_dir}</span></div>
        <div class="meta-card"><strong>Excluded Workspaces</strong><span>{excluded_workspaces}</span></div>
        <div class="meta-card"><strong>Generated</strong><span>{generated_at}</span></div>
      </div>
    </section>
    <div class="toolbar">
      <div class="toolbar-copy">
        <div class="hint">Statuses are color coded: green for scanned stores, amber for unresolved or missing paths, red for failures, and violet for orphaned data. Large reports stay responsive because the table renders one page at a time.</div>
      </div>
      <div class="toolbar-actions">
        <input id="rowFilter" class="search" type="search" placeholder="Filter rows by workspace, store, path, status, or notes">
        <label class="page-size-shell">Rows per page
          <select id="pageSize" class="page-size">
            <option value="50">50</option>
            <option value="100" selected>100</option>
            <option value="250">250</option>
            <option value="500">500</option>
          </select>
        </label>
      </div>
    </div>
    <div class="results-bar">
      <div id="resultSummary" class="result-summary"></div>
      <div class="pager">
        <button id="firstPage" class="pager-button" type="button">First</button>
        <button id="prevPage" class="pager-button" type="button">Previous</button>
        <span id="pageStatus" class="page-status"></span>
        <button id="nextPage" class="pager-button" type="button">Next</button>
        <button id="lastPage" class="pager-button" type="button">Last</button>
      </div>
    </div>
    <div class="table-shell">
      <table id="reportTable">
        <thead><tr>{header_cells}</tr></thead>
        <tbody></tbody>
      </table>
    </div>
    <p id="emptyState" class="empty-state" hidden>No rows match the current filter.</p>
  </div>
  <script id="reportColumns" type="application/json">{column_data}</script>
  <script id="reportRows" type="application/json">{row_data}</script>
  <script>
    const columns = JSON.parse(document.getElementById("reportColumns").textContent);
    const sourceRows = JSON.parse(document.getElementById("reportRows").textContent);
    const table = document.getElementById("reportTable");
    const tbody = table.querySelector("tbody");
    const headers = Array.from(table.querySelectorAll("th.sortable"));
    const filterInput = document.getElementById("rowFilter");
    const pageSizeSelect = document.getElementById("pageSize");
    const resultSummary = document.getElementById("resultSummary");
    const pageStatus = document.getElementById("pageStatus");
    const firstPageButton = document.getElementById("firstPage");
    const prevPageButton = document.getElementById("prevPage");
    const nextPageButton = document.getElementById("nextPage");
    const lastPageButton = document.getElementById("lastPage");
    const emptyState = document.getElementById("emptyState");
    const columnTypes = Object.fromEntries(columns.map(column => [column.key, column.type]));
    const allRows = sourceRows.map((row, index) => ({{
      ...row,
      __index: index,
      __search: columns.map(column => String(row[column.key] ?? "")).join(" ").toLowerCase(),
    }}));
    const state = {{
      query: "",
      page: 1,
      pageSize: Number(pageSizeSelect.value) || 100,
      sortKey: "",
      sortDirection: "asc",
    }};
    let filteredRows = allRows.slice();
    let filterTimer = null;

    function escapeHtml(value) {{
      return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }}
    function normalizeValue(value, type) {{
      if (type === "number") {{
        const parsed = Number(value);
        return Number.isNaN(parsed) ? Number.NEGATIVE_INFINITY : parsed;
      }}
      return String(value ?? "").toLowerCase();
    }}
    function totalPages() {{
      return Math.max(1, Math.ceil(filteredRows.length / state.pageSize));
    }}
    function applyFilterAndSort() {{
      const query = state.query;
      filteredRows = query ? allRows.filter(row => row.__search.includes(query)) : allRows.slice();
      if (!state.sortKey) {{
        return;
      }}
      const type = columnTypes[state.sortKey] || "text";
      const direction = state.sortDirection === "asc" ? 1 : -1;
      filteredRows.sort((left, right) => {{
        const leftValue = normalizeValue(left[state.sortKey], type);
        const rightValue = normalizeValue(right[state.sortKey], type);
        if (leftValue < rightValue) return -1 * direction;
        if (leftValue > rightValue) return 1 * direction;
        return left.__index - right.__index;
      }});
    }}
    function renderPage() {{
      const totalRows = filteredRows.length;
      const pages = totalPages();
      state.page = Math.min(Math.max(state.page, 1), pages);
      if (!totalRows) {{
        tbody.innerHTML = "";
        resultSummary.textContent = `Showing 0 of 0 filtered rows (${{allRows.length}} total)`;
        pageStatus.textContent = "Page 0 of 0";
        firstPageButton.disabled = true;
        prevPageButton.disabled = true;
        nextPageButton.disabled = true;
        lastPageButton.disabled = true;
        emptyState.hidden = false;
        return;
      }}
      const start = (state.page - 1) * state.pageSize;
      const pageRows = filteredRows.slice(start, start + state.pageSize);
      tbody.innerHTML = pageRows.map(row => {{
        const statusClass = `status-${{String(row.status || "").toLowerCase()}}`;
        const cells = columns.map(column => `<td data-key="${{escapeHtml(column.key)}}">${{escapeHtml(row[column.key])}}</td>`).join("");
        return `<tr class="report-row ${{statusClass}}">${{cells}}</tr>`;
      }}).join("");
      const end = start + pageRows.length;
      resultSummary.textContent = `Showing ${{start + 1}}-${{end}} of ${{totalRows}} filtered rows (${{allRows.length}} total)`;
      pageStatus.textContent = `Page ${{state.page}} of ${{pages}}`;
      firstPageButton.disabled = state.page <= 1;
      prevPageButton.disabled = state.page <= 1;
      nextPageButton.disabled = state.page >= pages;
      lastPageButton.disabled = state.page >= pages;
      emptyState.hidden = true;
    }}
    function refreshView(resetPage) {{
      if (resetPage) {{
        state.page = 1;
      }}
      applyFilterAndSort();
      renderPage();
    }}
    headers.forEach(header => header.addEventListener("click", () => {{
      const key = header.dataset.key;
      if (state.sortKey === key) {{
        state.sortDirection = state.sortDirection === "asc" ? "desc" : "asc";
      }} else {{
        state.sortKey = key;
        state.sortDirection = header.dataset.type === "number" ? "desc" : "asc";
      }}
      headers.forEach(item => delete item.dataset.direction);
      header.dataset.direction = state.sortDirection;
      refreshView(false);
    }}));
    filterInput.addEventListener("input", () => {{
      window.clearTimeout(filterTimer);
      filterTimer = window.setTimeout(() => {{
        state.query = filterInput.value.trim().toLowerCase();
        refreshView(true);
      }}, 120);
    }});
    pageSizeSelect.addEventListener("change", () => {{
      state.pageSize = Number(pageSizeSelect.value) || 100;
      refreshView(true);
    }});
    firstPageButton.addEventListener("click", () => {{
      state.page = 1;
      renderPage();
    }});
    prevPageButton.addEventListener("click", () => {{
      state.page -= 1;
      renderPage();
    }});
    nextPageButton.addEventListener("click", () => {{
      state.page += 1;
      renderPage();
    }});
    lastPageButton.addEventListener("click", () => {{
      state.page = totalPages();
      renderPage();
    }});
    refreshView(true);
  </script>
</body>
</html>
""".format(
        store_count=summary["store_count"],
        orphan_count=summary["orphan_count"],
        issue_count=summary["issue_count"],
        tracked_size_gb=summary["tracked_size_gb"],
        geoserver_url=html.escape(geoserver_url),
        data_dir=html.escape(os.path.abspath(data_dir)),
        excluded_workspaces=html.escape(summary["excluded_workspaces"]),
        generated_at=html.escape(summary["generated_at"]),
        header_cells="".join(header_cells),
        column_data=column_data,
        row_data=row_data,
    )


def write_html_report(
    path: str,
    rows: Sequence[dict],
    excluded_workspaces: Sequence[str],
    geoserver_url: str,
    data_dir: str,
) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent and not os.path.isdir(parent):
        os.makedirs(parent, exist_ok=True)

    with open(path, "w", encoding="utf-8") as handle:
        handle.write(
            build_html_report_text(
                rows,
                excluded_workspaces,
                geoserver_url,
                data_dir,
            )
        )


