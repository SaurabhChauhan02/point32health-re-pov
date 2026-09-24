"""Claims demo pipeline.

Picks up a claims CSV from SFTP, profiles it, scores it, loads it into the
Oracle STAGING schema, then moves the source file into an archive folder.

STAGING.CLAIMS_RAW is created and owned by the DBA, not by this DAG. The DAG
only truncates and reloads it.
"""

from __future__ import annotations

import posixpath
from datetime import datetime

from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.sensors.sftp import SFTPSensor

SFTP_CONN_ID = "sftp_claims"
ORACLE_CONN_ID = "oracle_staging"

# The SFTP login names the blob container, so the session already starts
# inside `inbound` and these paths are relative to it.
CLAIMS_FILE = "claims_data.csv"
ARCHIVE_DIR = "archive"

STAGING_TABLE = "STAGING.CLAIMS_RAW"


@dag(
    dag_id="claims_sftp_to_oracle_staging",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "claims"],
    doc_md=__doc__,
)
def claims_sftp_to_oracle_staging():
    # Deferral releases the worker slot while the triggerer polls for the inbound file.
    wait_for_file = SFTPSensor(
        task_id="wait_for_file",
        sftp_conn_id=SFTP_CONN_ID,
        path=CLAIMS_FILE,
        poke_interval=30,
        timeout=1800,
        deferrable=True,
    )

    @task
    def extract_claims() -> dict:
        """Read SFTP once and return a JSON-serializable claims payload.

        Remote Execution tasks can run in different pods, so their local filesystems
        are not shared. Passing plain Python data through XCom lets Airflow move the
        payload between tasks; this deployment offloads XComs to Azure object storage.
        """
        # Keep pandas out of DAG parsing and convert its values to JSON-safe Python types.
        import pandas as pd

        with SFTPHook(ssh_conn_id=SFTP_CONN_ID).get_conn() as sftp:
            size_kb = sftp.stat(CLAIMS_FILE).st_size / 1024
            with sftp.open(CLAIMS_FILE, "rb") as source:
                df = pd.read_csv(source, dtype=str)

        normalized = df.astype(object).where(df.notna(), None)
        return {
            "columns": list(normalized.columns),
            "rows": normalized.values.tolist(),
            "size_kb": round(size_kb, 1),
        }

    @task
    def profile_claims(claims: dict) -> dict:
        rows = claims["rows"]
        columns = claims["columns"]
        metrics = {
            "rows": len(rows),
            "columns": len(columns),
            "nulls": sum(value is None for row in rows for value in row),
            "size_kb": claims["size_kb"],
        }
        print(f"{CLAIMS_FILE}: {metrics['rows']} rows, {metrics['columns']} columns, "
              f"{metrics['nulls']} nulls, {metrics['size_kb']} KB")
        return metrics

    @task
    def score_claims(metrics: dict) -> float:
        """Stands in for the real claims scoring service."""
        index = round(100 * (1 - metrics["nulls"] / (metrics["rows"] * metrics["columns"])), 2)
        print(f"Claims completeness index: {index}")
        return index

    # Truncate rather than delete-by-run-id: this is a full reload each time.
    clear_staging = SQLExecuteQueryOperator(
        task_id="clear_staging",
        conn_id=ORACLE_CONN_ID,
        sql=f"TRUNCATE TABLE {STAGING_TABLE}",
    )

    @task
    def load_claims(claims: dict, **context) -> int:
        columns = [*claims["columns"], "LOAD_RUN_ID"]
        rows = [
            [*row, context["dag_run"].run_id]
            for row in claims["rows"]
        ]

        OracleHook(oracle_conn_id=ORACLE_CONN_ID).insert_rows(
            table=STAGING_TABLE,
            rows=rows,
            target_fields=columns,
            commit_every=500,
        )
        print(f"Loaded {len(rows)} rows into {STAGING_TABLE}")
        return len(rows)

    @task(trigger_rule="all_done")
    def archive_claims(**context) -> str | None:
        """Archive the input even when an earlier task fails."""
        with SFTPHook(ssh_conn_id=SFTP_CONN_ID).get_conn() as sftp:
            try:
                sftp.stat(CLAIMS_FILE)
            except FileNotFoundError:
                print(f"Nothing to archive: {CLAIMS_FILE} was not found")
                return None

            try:
                sftp.stat(ARCHIVE_DIR)
            except FileNotFoundError:
                sftp.mkdir(ARCHIVE_DIR)

            stamp = context["dag_run"].run_after.strftime("%Y%m%dT%H%M%S")
            target = posixpath.join(ARCHIVE_DIR, f"claims_data_{stamp}.csv")
            # Rename on the SFTP server instead of downloading and uploading the file again.
            sftp.rename(CLAIMS_FILE, target)
        print(f"Archived {CLAIMS_FILE} to {target}")
        return target

    claims = extract_claims()
    wait_for_file >> claims
    metrics = profile_claims(claims)
    loaded = load_claims(claims)

    score_claims(metrics) >> clear_staging >> loaded >> archive_claims()


claims_sftp_to_oracle_staging()
