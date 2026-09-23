"""Claims demo pipeline.

Picks up a claims CSV from SFTP, profiles it, scores it, loads it into the
Oracle STAGING schema, then zips the source file into an archive folder.

STAGING.CLAIMS_RAW is created and owned by the DBA, not by this DAG. The DAG
only truncates and reloads it.
"""

from __future__ import annotations

import io
import posixpath
import zipfile
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


def read_claims_csv():
    """Stream the CSV off SFTP into a DataFrame.

    Under remote execution each task gets its own pod, so there is no shared
    /tmp to hand a downloaded file between tasks - every task re-reads it.
    """
    # Imported here rather than at module level to keep DAG parsing fast.
    import pandas as pd

    with SFTPHook(ssh_conn_id=SFTP_CONN_ID).get_conn().open(CLAIMS_FILE, "rb") as f:
        return pd.read_csv(f, dtype=str)


@dag(
    dag_id="claims_sftp_to_oracle_staging",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "claims"],
    doc_md=__doc__,
)
def claims_sftp_to_oracle_staging():
    wait_for_file = SFTPSensor(
        task_id="wait_for_file",
        sftp_conn_id=SFTP_CONN_ID,
        path=CLAIMS_FILE,
        deferrable=True,
    )

    @task
    def profile_claims() -> dict:
        size_kb = SFTPHook(ssh_conn_id=SFTP_CONN_ID).get_conn().stat(CLAIMS_FILE).st_size / 1024
        df = read_claims_csv()

        metrics = {
            "rows": len(df),
            "columns": len(df.columns),
            "nulls": int(df.isna().sum().sum()),
            "size_kb": round(size_kb, 1),
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
    def load_claims(**context) -> int:
        df = read_claims_csv()
        df["LOAD_RUN_ID"] = context["dag_run"].run_id

        # Oracle wants None for empty cells, not the NaN that pandas produces.
        rows = df.astype(object).where(df.notna(), None).values.tolist()

        OracleHook(oracle_conn_id=ORACLE_CONN_ID).insert_rows(
            table=STAGING_TABLE,
            rows=rows,
            target_fields=list(df.columns),
            commit_every=500,
        )
        print(f"Loaded {len(rows)} rows into {STAGING_TABLE}")
        return len(rows)

    @task
    def archive_claims(**context) -> str:
        hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)
        if not hook.path_exists(ARCHIVE_DIR):
            hook.create_directory(ARCHIVE_DIR)

        sftp = hook.get_conn()
        stamp = context["dag_run"].run_after.strftime("%Y%m%dT%H%M%S")
        target = posixpath.join(ARCHIVE_DIR, f"claims_data_{stamp}.zip")

        # Zip in memory so nothing has to touch the worker's local disk.
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            with sftp.open(CLAIMS_FILE, "rb") as f:
                archive.writestr(CLAIMS_FILE, f.read())
        buffer.seek(0)

        sftp.putfo(buffer, target, confirm=True)
        sftp.remove(CLAIMS_FILE)
        print(f"Archived {CLAIMS_FILE} to {target}")
        return target

    metrics = profile_claims()

    wait_for_file >> metrics >> score_claims(metrics) >> clear_staging >> load_claims() >> archive_claims()


claims_sftp_to_oracle_staging()
