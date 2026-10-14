"""Generate an HTML report from CZMon policy-result JSON."""

from __future__ import annotations

import argparse
import html
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any


FIELD_LABELS = {
  "policy_name": "Policy name",
  "policy_status": "Policy status",
  "policy_description": "Description",
  "priority": "Priority",
  "issue": "Issue",
  "owner": "Owner",
  "date_time": "Date and time",
  "cluster_detailes": "Cluster details",
}


def _display_value(value: Any) -> str:
  """Convert JSON values into text that is safe to insert into HTML."""
  if value is None:
    return ""
  if isinstance(value, (dict, list)):
    value = json.dumps(value, indent=2, ensure_ascii=False)
  return html.escape(str(value))


def _status_class(status: Any) -> str:
  """Return a CSS class for boolean and string policy statuses."""
  if isinstance(status, bool):
    return "pass" if status else "fail"
  return "pass" if str(status).strip().lower() == "true" else "fail"


def _priority_class(priority: Any) -> str:
  """Return a CSS-safe priority class for a policy."""
  priority_text = str(priority).strip().upper()
  return priority_text.lower() if priority_text in {"P0", "P1", "P2"} else "other"


def render_policy_report(policy_results: Mapping[str, Any]) -> str:
  """Render policy JSON data as a standalone HTML document.

  Each top-level JSON key is the policy ID, such as ``"policy_id": {...}``,
  and becomes the card heading. The nested object is displayed as labelled
  fields, including any additional fields not defined in ``FIELD_LABELS``.
  """
  if not isinstance(policy_results, Mapping):
    raise ValueError("Policy report JSON must be an object keyed by policy ID.")

  cards: list[str] = []
  passed_count = 0
  failed_count = 0
  priority_counts = {"P0": 0, "P1": 0, "P2": 0}
  for policy_id, policy in policy_results.items():
    if not isinstance(policy, Mapping):
      raise ValueError(f"Policy '{policy_id}' must contain a JSON object.")

    status = policy.get("policy_status", False)
    status_class = _status_class(status)
    priority = policy.get("priority", "")
    priority_class = _priority_class(priority)
    if status_class == "pass":
      passed_count += 1
    else:
      failed_count += 1
    if str(priority).strip().upper() in priority_counts:
      priority_counts[str(priority).strip().upper()] += 1

    detail_rows: list[str] = []
    for field, value in policy.items():
      if field in {"policy_status", "policy_description", "priority"}:
        continue
      label = FIELD_LABELS.get(field, field.replace("_", " ").title())
      value_html = _display_value(value)
      if field == "issue" and not value:
        value_html = '<span class="empty">No issue</span>'

      detail_rows.append(
        "<tr>"
        f"<th>{html.escape(label)}</th>"
        f"<td>{value_html}</td>"
        "</tr>"
      )

    cards.append(
      f'<section class="policy-card" data-status="{status_class}" '
      f'data-priority="{priority_class}">'
      '<div class="card-header">'
      f"<h2>{html.escape(str(policy_id))}</h2>"
      "</div>"
      '<dl class="policy-summary">'
      f'<div><dt>Policy status</dt><dd><span class="status {status_class}">'
      f"{'Passed' if status_class == 'pass' else 'Failed'}</span></dd></div>"
      f"<div><dt>Description</dt><dd>{_display_value(policy.get('policy_description', ''))}</dd></div>"
      f'<div><dt>Priority</dt><dd><span class="priority {priority_class}">{_display_value(priority)}</span></dd></div>'
      "</dl>"
      "<details>"
      "<summary>View complete details</summary>"
      "<table>"
      f"{''.join(detail_rows) or '<tr><td>No additional details</td></tr>'}"
      "</table>"
      "</details>"
      "</section>"
    )

  return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>CZMon Policy Dashboard</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{ background: linear-gradient(135deg, #0a3d75 0%, #115a9d 45%, #1877bb 100%); color: #172b4d; font-family: Arial, sans-serif; margin: 0; min-height: 100vh; }}
    .site-header {{ align-items: center; background: #062b55; display: flex; justify-content: space-between; min-height: 70px; padding: 0 4vw; }}
    .nutanix-logo {{ color: #fff; font-size: 1.45rem; font-weight: 800; letter-spacing: .12em; margin: 0; }}
    .nutanix-logo span {{ color: #a4d65e; }}
    .site-header small {{ color: #c7d4d9; }}
    main {{ margin: 0 auto; max-width: 1600px; padding: 2rem 4vw; width: 100%; }}
    h1 {{ margin-bottom: .25rem; }}
    .subtitle {{ color: #5e6c84; margin: 0 0 1.5rem; }}
    .summary {{ display: grid; gap: 1rem; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); margin-bottom: 1.5rem; }}
    .metric {{ background: #fff; border: 1px solid #dbe5e8; border-radius: 8px; box-shadow: 0 2px 8px #001b361f; padding: 1rem; }}
    .metric-label {{ color: #5e6c84; display: block; font-size: .8rem; font-weight: 700; text-transform: uppercase; }}
    .metric-value {{ display: block; font-size: 2rem; font-weight: 700; margin-top: .3rem; }}
    .controls {{ align-items: center; display: flex; flex-wrap: wrap; gap: 1rem; justify-content: space-between; margin-bottom: 1rem; }}
    .filters {{ align-items: center; display: flex; flex-wrap: wrap; gap: .5rem; }}
    .filter {{ background: #fff; border: 1px solid #dfe1e6; border-radius: 999px; color: #172b4d; cursor: pointer; padding: .45rem .85rem; }}
    .filter.active {{ background: #075cab; border-color: #075cab; color: #fff; }}
    .entity-limit {{ color: #344563; font-size: .9rem; font-weight: 700; }}
    select {{ background: #fff; border: 1px solid #a5b8bf; border-radius: 5px; color: #172b4d; margin-left: .4rem; padding: .45rem; }}
    .policy-grid {{ display: grid; gap: .75rem; grid-template-columns: 1fr; }}
    .policy-card {{ background: #fff; border: 1px solid #dbe5e8; border-radius: 8px; box-shadow: 0 2px 8px #001b361f; overflow: hidden; padding: 1rem 1.25rem; }}
    .card-header {{ align-items: center; display: flex; gap: .75rem; justify-content: space-between; }}
    .policy-card h2 {{ font-size: 1.05rem; margin: 0; overflow-wrap: anywhere; }}
    .status, .priority {{ border-radius: 999px; display: inline-block; font-weight: 700; padding: .25rem .65rem; }}
    .pass {{ background: #dff7e7; color: #146c2e; }}
    .fail {{ background: #ffe2e0; color: #ad1f1f; }}
    .priority {{ background: #e9edf5; color: #344563; }}
    .priority.p0 {{ background: #ffe2e0; color: #ad1f1f; }}
    .priority.p1 {{ background: #fff1cc; color: #805d00; }}
    .priority.p2 {{ background: #dff7e7; color: #146c2e; }}
    .policy-summary {{ display: grid; gap: 1rem; grid-template-columns: 1fr 3fr 1fr; margin: 1rem 0; }}
    .policy-summary div {{ border-left: 1px solid #dfe8ea; min-width: 0; padding-left: 1rem; }}
    .policy-summary div:first-child {{ border-left: 0; padding-left: 0; }}
    .policy-summary dt {{ color: #5e6c84; font-size: .8rem; font-weight: 700; }}
    .policy-summary dd {{ margin: 0; overflow-wrap: anywhere; }}
    details {{ border-top: 1px solid #dfe8ea; padding-top: .8rem; }}
    summary {{ color: #1f7a8c; cursor: pointer; font-size: .9rem; font-weight: 700; }}
    table {{ border-collapse: collapse; margin-top: 1rem; width: 100%; }}
    th, td {{ border-top: 1px solid #dfe1e6; padding: .7rem; text-align: left; vertical-align: top; }}
    th {{ width: 30%; }}
    td {{ white-space: pre-wrap; word-break: break-word; }}
    .empty {{ color: #6b778c; font-style: italic; }}
    .hidden {{ display: none; }}
    .pagination {{ align-items: center; display: flex; gap: .75rem; justify-content: center; margin: 1.5rem 0 .5rem; }}
    .pagination button {{ background: #fff; border: 1px solid #dbe5e8; border-radius: 5px; color: #075cab; cursor: pointer; font-weight: 700; padding: .5rem .9rem; }}
    .pagination button:disabled {{ color: #8993a4; cursor: not-allowed; }}
    .pagination span {{ color: #fff; font-size: .9rem; font-weight: 700; }}
    @media (max-width: 760px) {{ .site-header {{ align-items: flex-start; flex-direction: column; gap: .25rem; padding: 1rem 5vw; }} main {{ padding: 1.5rem 5vw; }} .policy-summary {{ grid-template-columns: 1fr 1fr; }} .policy-summary div:nth-child(odd) {{ border-left: 0; padding-left: 0; }} }}
    @media (max-width: 500px) {{ .policy-summary {{ grid-template-columns: 1fr; }} .policy-summary div {{ border-left: 0; border-top: 1px solid #dfe8ea; padding: .7rem 0 0; }} .policy-summary div:first-child {{ border-top: 0; padding-top: 0; }} }}
  </style>
</head>
<body>
  <header class="site-header">
    <p class="nutanix-logo">NU<span>T</span>ANIX</p>
    <small>CZMon Policy Compliance</small>
  </header>
  <main>
    <h1>CZMon Policy Dashboard</h1>
    <p class="subtitle">Current policy compliance results</p>
    <section class="summary" aria-label="Policy summary">
      <div class="metric"><span class="metric-label">Total policies</span><span class="metric-value">{len(policy_results)}</span></div>
      <div class="metric"><span class="metric-label">Passed</span><span class="metric-value pass">{passed_count}</span></div>
      <div class="metric"><span class="metric-label">Failed</span><span class="metric-value fail">{failed_count}</span></div>
      <div class="metric"><span class="metric-label">P0</span><span class="metric-value">{priority_counts["P0"]}</span></div>
      <div class="metric"><span class="metric-label">P1</span><span class="metric-value">{priority_counts["P1"]}</span></div>
      <div class="metric"><span class="metric-label">P2</span><span class="metric-value">{priority_counts["P2"]}</span></div>
    </section>
    <div class="controls">
      <div class="filters" aria-label="Policy filters">
        <button class="filter active" data-filter="all">All</button>
        <button class="filter" data-filter="pass">Passed</button>
        <button class="filter" data-filter="fail">Failed</button>
        <button class="filter" data-filter="p0">P0</button>
        <button class="filter" data-filter="p1">P1</button>
        <button class="filter" data-filter="p2">P2</button>
      </div>
      <label class="entity-limit" for="entity-limit">Policies displayed
        <select id="entity-limit">
          <option value="50" selected>50</option>
          <option value="100">100</option>
        </select>
      </label>
    </div>
    <section class="policy-grid">
      {''.join(cards) or '<p>No policy results were found.</p>'}
    </section>
    <nav class="pagination" aria-label="Policy pages">
      <button id="previous-page" type="button">Previous</button>
      <span id="page-indicator"></span>
      <button id="next-page" type="button">Next</button>
    </nav>
  </main>
  <script>
    let activeFilter = "all";
    let entityLimit = 50;
    let currentPage = 0;
    const cards = Array.from(document.querySelectorAll(".policy-card"));
    const previousPageButton = document.querySelector("#previous-page");
    const nextPageButton = document.querySelector("#next-page");
    const pageIndicator = document.querySelector("#page-indicator");

    function updateVisiblePolicies() {{
      const filteredCards = cards.filter((card) => activeFilter === "all"
          || card.dataset.status === activeFilter
          || card.dataset.priority === activeFilter);
      const pageCount = Math.max(1, Math.ceil(filteredCards.length / entityLimit));
      currentPage = Math.min(currentPage, pageCount - 1);
      const firstItem = currentPage * entityLimit;
      const visibleCards = new Set(filteredCards.slice(firstItem, firstItem + entityLimit));
      cards.forEach((card) => {{
        card.classList.toggle("hidden", !visibleCards.has(card));
      }});
      previousPageButton.disabled = currentPage === 0;
      nextPageButton.disabled = currentPage >= pageCount - 1;
      pageIndicator.textContent = `Page ${{currentPage + 1}} of ${{pageCount}}`;
    }}

    document.querySelectorAll(".filter").forEach((button) => {{
      button.addEventListener("click", () => {{
        activeFilter = button.dataset.filter;
        currentPage = 0;
        document.querySelectorAll(".filter").forEach((item) => item.classList.remove("active"));
        button.classList.add("active");
        updateVisiblePolicies();
      }});
    }});
    document.querySelector("#entity-limit").addEventListener("change", (event) => {{
      entityLimit = Number(event.target.value);
      currentPage = 0;
      updateVisiblePolicies();
    }});
    previousPageButton.addEventListener("click", () => {{
      currentPage -= 1;
      updateVisiblePolicies();
    }});
    nextPageButton.addEventListener("click", () => {{
      currentPage += 1;
      updateVisiblePolicies();
    }});
    updateVisiblePolicies();
  </script>
</body>
</html>
"""


def generate_html_report(json_path: str | Path, html_path: str | Path) -> Path:
  """Read a policy JSON file and write its formatted HTML report."""
  source = Path(json_path)
  destination = Path(html_path)
  try:
    with source.open(encoding="utf-8") as json_file:
      policy_results = json.load(json_file)
  except json.JSONDecodeError as error:
    raise ValueError(f"Invalid JSON in '{source}': {error.msg}") from error

  destination.parent.mkdir(parents=True, exist_ok=True)
  destination.write_text(render_policy_report(policy_results), encoding="utf-8")
  return destination


def main() -> None:
  """Generate an HTML report from the command line."""
  parser = argparse.ArgumentParser(description="Generate a CZMon policy HTML report.")
  parser.add_argument("json_file", help="Path to the input policy JSON file.")
  parser.add_argument(
    "html_file",
    nargs="?",
    default="policy_report.html",
    help="Path for the generated report (default: policy_report.html).",
  )
  args = parser.parse_args()
  output_path = generate_html_report(args.json_file, args.html_file)
  print(f"HTML report written to {output_path}")


if __name__ == "__main__":
  main()
