"""
On-failure handler for Astro's Dag Trigger alert channel.

Point an Astro alert (DAG failure) at this DAG via a "Dag Trigger" notification
channel. Astro triggers it with the alert payload as dag_run.conf. It has no
schedule and only runs when triggered.

Test manually by triggering with this config:
{
  "alertId": "test-001",
  "alertType": "DAG_FAILURE",
  "dagName": "on_failure_handler_simple",
  "message": "DAG run failed for DAG customer_orders_etl",
  "airflowDagRunId": "scheduled__2026-10-02T06:00:00+00:00",
  "logFailureSummaries": {"load_to_snowflake": "Object does not exist"}
}
"""

import json
import re

from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowSkipException
from pendulum import datetime, parse

HANDLER_DAG_ID = "on_failure_handler_simple"
MAX_SUMMARY_CHARS = 2000

# `dagName` in the payload is the DAG being triggered (this one), not the failed
# DAG, so the failed DAG ID has to be parsed from the message. Formats seen:
#   DAG_FAILURE:      "DAG run failed for DAG my_dag"
#   PIPELINE_FAILURE: "[Astro Alerts] Pipeline failure detected on dag my_dag. ..."
FAILED_DAG_RE = re.compile(r"(?:for|on)\s+DAG\s+['\"]?([\w.-]+)", re.IGNORECASE)
URL_RE = re.compile(r"https?://[^\s\"'\\]+")
MESSAGE_FIELD_RE = {
    "start_time": re.compile(r"Start time:\s*([^\n\\]+?)\.?\s*(?:\\n|\n|$)"),
    "failed_at": re.compile(r"Failed at:\s*([^\n\\]+?)\.?\s*(?:\\n|\n|$)"),
}


def _parse_run_id(run_id):
    """Split an Airflow run ID like 'scheduled__2026-10-02T06:00:00+00:00'."""
    if not run_id or "__" not in run_id:
        return None, None
    run_type, _, ts = run_id.partition("__")
    try:
        return run_type, parse(ts).to_iso8601_string()
    except Exception:
        return run_type, None


def _truncate(text, limit=MAX_SUMMARY_CHARS):
    text = str(text).strip()
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n... [truncated, {len(text) - limit} more chars]"


@dag(
    dag_id=HANDLER_DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=10,
    default_args={"retries": 2},
    tags=["alerts"],
    doc_md=__doc__,
)
def on_failure_handler_simple():
    @task
    def get_failure_info(**context):
        conf = dict(context["dag_run"].conf or {})
        print("Raw conf:", json.dumps(conf, indent=2, default=str))

        message = str(conf.get("message") or "")
        match = FAILED_DAG_RE.search(message)
        dag_id = match.group(1).rstrip(".") if match else "unknown"

        # The handler must not open tickets for its own failures, otherwise an
        # alert scoped to "all DAGs" re-triggers this DAG in a loop.
        if dag_id == HANDLER_DAG_ID:
            raise AirflowSkipException("Alert is for the failure handler itself; skipping to avoid a loop.")

        summaries = conf.get("logFailureSummaries") or {}
        if not isinstance(summaries, dict):
            summaries = {"unknown": summaries}

        run_id = conf.get("airflowDagRunId")
        run_type, logical_date = _parse_run_id(run_id)
        urls = URL_RE.findall(message)

        return {
            "dag_id": dag_id,
            "run_id": run_id,
            "run_type": run_type,
            "logical_date": logical_date,
            "start_time": (m.group(1) if (m := MESSAGE_FIELD_RE["start_time"].search(message)) else None),
            "failed_at": (m.group(1) if (m := MESSAGE_FIELD_RE["failed_at"].search(message)) else None),
            "astro_ui_url": urls[0] if urls else None,
            "task_ids": list(summaries.keys()),
            "summaries": {k: _truncate(v) for k, v in summaries.items()},
            "alert_id": conf.get("alertId"),
            "alert_type": conf.get("alertType"),
            "message": message,
            "note": conf.get("note"),
            "handler_run_id": context["dag_run"].run_id,
            "raw_conf": conf,
        }

    @task
    def create_mock_servicenow_ticket(info: dict):
        failed_tasks = ", ".join(info["task_ids"]) or "unknown"
        lines = [
            "Airflow DAG failure detected via Astro alert.",
            "",
            "=== FAILURE DETAILS ===",
            f"Failed DAG ID:      {info['dag_id']}",
            f"Failed Run ID:      {info['run_id'] or 'not provided'}",
            f"Run type:           {info['run_type'] or 'unknown'}",
            f"Logical date:       {info['logical_date'] or 'unknown'}",
            f"Run start time:     {info['start_time'] or 'not provided'}",
            f"Failed at:          {info['failed_at'] or 'not provided'}",
            f"Failed task IDs:    {failed_tasks}",
            f"# of failed tasks:  {len(info['task_ids'])}",
            f"Astro UI link:      {info['astro_ui_url'] or 'not provided'}",
            "",
            "=== FAILURE SUMMARIES (per task) ===",
        ]
        for task_id, summary in info["summaries"].items():
            lines += [f"--- {task_id} ---", summary, ""]
        if not info["summaries"]:
            lines += ["(none provided - check task logs in Airflow)", ""]

        lines += [
            "=== SUGGESTED NEXT STEPS ===",
            f"1. Open DAG '{info['dag_id']}' in Airflow, run '{info['run_id']}'.",
            f"2. Review logs for failed task(s): {failed_tasks}.",
            "3. Fix the root cause, then clear the failed task(s) to re-run.",
            "",
            "=== ALERT DETAILS ===",
            f"Alert ID:        {info['alert_id']}",
            f"Alert type:      {info['alert_type']}",
            f"Message:         {info['message']}",
            f"Note:            {info['note'] or '-'}",
            f"Handler run ID:  {HANDLER_DAG_ID} / {info['handler_run_id']}",
            "",
            "=== RAW ALERT PAYLOAD ===",
            json.dumps(info["raw_conf"], indent=2, default=str),
        ]

        ticket = {
            "number": "INC0000001",
            "short_description": f"Airflow DAG failed: {info['dag_id']} [{failed_tasks}]"[:160],
            "description": "\n".join(lines),
            "category": "Airflow",
            # Stable per failed run so retries/duplicate alerts can be de-duplicated in ServiceNow.
            "correlation_id": f"{info['dag_id']}|{info['run_id']}",
        }

        # MOCK: no real call to ServiceNow
        print("[MOCK ServiceNow] would create incident:")
        print(json.dumps({k: v for k, v in ticket.items() if k != "description"}, indent=2))
        print("----- description -----")
        print(ticket["description"])
        return ticket["number"]

    @task
    def notify(info: dict, ticket_number: str):
        # MOCK: just prints. Swap in Slack/email as needed.
        print(
            f"ALERT: {info['dag_id']} failed (run {info['run_id']}). "
            f"Failed tasks: {', '.join(info['task_ids']) or 'unknown'}. "
            f"Ticket: {ticket_number}"
            + (f". Astro UI: {info['astro_ui_url']}" if info["astro_ui_url"] else "")
        )

    info = get_failure_info()
    ticket = create_mock_servicenow_ticket(info)
    notify(info, ticket)


on_failure_handler_simple()
