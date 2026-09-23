"""Demo pipeline: pull a claims CSV from SFTP, profile it, index it, land it in an
Oracle STAGING schema, then zip and archive the source file back on the SFTP server.

Runs on Astro remote execution, where every task executes in its own pod. There is no
shared local filesystem between tasks, so each task that needs the file streams it
from SFTP rather than relying on a file left behind by an upstream task.
"""

from __future__ import annotations

import hashlib
import io
import posixpath
import re
import zipfile
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.sensors.sftp import SFTPSensor

SFTP_CONN_ID = "sftp_claims"
ORACLE_CONN_ID = "oracle_staging"

DEFAULT_PARAMS = {
    "remote_dir": "/inbound",
    "filename": "claims_data.csv",
    "archive_dir": "/inbound/archive",
    "staging_schema": "STAGING",
    "staging_table": "CLAIMS_RAW",
}


def _remote_path(params: dict) -> str:
    return posixpath.join(params["remote_dir"], params["filename"])


def _oracle_identifier(name: str) -> str:
    """Turn a CSV header into a safe, unquoted Oracle identifier."""
    ident = re.sub(r"\W+", "_", name.strip().upper()).strip("_")
    if not ident or ident[0].isdigit():
        ident = f"C_{ident}"
    return ident[:30]


@dag(
    dag_id="claims_sftp_to_oracle_staging",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    params=DEFAULT_PARAMS,
    tags=["demo", "claims", "sftp", "oracle"],
    doc_md=__doc__,
)
def claims_sftp_to_oracle_staging():
    wait_for_file = SFTPSensor(
        task_id="wait_for_claims_file",
        sftp_conn_id=SFTP_CONN_ID,
        path="{{ params.remote_dir }}/{{ params.filename }}",
        poke_interval=30,
        timeout=60 * 30,
        deferrable=True,
    )

    @task
    def profile_claims_file(params: dict) -> dict:
        """Read the CSV off SFTP and log shape, size, and column-level metrics."""
        import pandas as pd

        path = _remote_path(params)
        hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)
        client = hook.get_conn()

        stat = client.stat(path)
        with client.open(path, "rb") as fh:
            fh.prefetch()
            raw = fh.read()

        frame = pd.read_csv(io.BytesIO(raw))
        null_counts = {c: int(frame[c].isna().sum()) for c in frame.columns}

        metrics = {
            "remote_path": path,
            "file_type": posixpath.splitext(params["filename"])[1].lstrip(".").lower() or "unknown",
            "size_bytes": int(stat.st_size),
            "size_mb": round(int(stat.st_size) / (1024 * 1024), 4),
            "modified_at": datetime.utcfromtimestamp(stat.st_mtime).isoformat(),
            "sha256": hashlib.sha256(raw).hexdigest(),
            "total_rows": int(len(frame)),
            "total_columns": int(len(frame.columns)),
            "columns": list(frame.columns),
            "dtypes": {c: str(t) for c, t in frame.dtypes.items()},
            "null_counts": null_counts,
            "total_nulls": int(sum(null_counts.values())),
        }

        print("===== claims_data profile =====")
        print(f"  remote path    : {metrics['remote_path']}")
        print(f"  file type      : {metrics['file_type']}")
        print(f"  size           : {metrics['size_bytes']:,} bytes ({metrics['size_mb']} MB)")
        print(f"  last modified  : {metrics['modified_at']}")
        print(f"  sha256         : {metrics['sha256']}")
        print(f"  rows           : {metrics['total_rows']:,}")
        print(f"  columns        : {metrics['total_columns']}")
        for col in frame.columns:
            print(f"    - {col:<30} dtype={metrics['dtypes'][col]:<10} nulls={null_counts[col]}")
        print(f"  total nulls    : {metrics['total_nulls']:,}")
        print("===============================")

        return metrics

    @task
    def compute_claims_index(metrics: dict) -> float:
        """Mocked index function. Stands in for a real scoring/indexing service."""
        cells = max(metrics["total_rows"] * metrics["total_columns"], 1)
        completeness = 1 - (metrics["total_nulls"] / cells)
        index_value = round(completeness * 100, 2)

        print(f"[mock index] evaluated {cells:,} cells")
        print(f"[mock index] claims completeness index = {index_value}")
        return index_value

    @task
    def load_to_staging(metrics: dict, params: dict, **context) -> int:
        """Land the raw CSV in Oracle as VARCHAR2 columns, keyed by this DAG run."""
        import pandas as pd

        path = _remote_path(params)
        sftp = SFTPHook(ssh_conn_id=SFTP_CONN_ID)
        with sftp.get_conn().open(path, "rb") as fh:
            fh.prefetch()
            frame = pd.read_csv(fh, dtype=str, keep_default_na=False)

        columns = [_oracle_identifier(c) for c in frame.columns]
        table = f"{params['staging_schema']}.{params['staging_table']}"
        run_id = context["dag_run"].run_id

        oracle = OracleHook(oracle_conn_id=ORACLE_CONN_ID)
        column_ddl = ",\n            ".join(f"{c} VARCHAR2(4000)" for c in columns)
        oracle.run(
            f"""
            DECLARE
              v_exists NUMBER;
            BEGIN
              SELECT COUNT(*) INTO v_exists FROM all_tables
               WHERE owner = '{params["staging_schema"]}'
                 AND table_name = '{params["staging_table"]}';
              IF v_exists = 0 THEN
                EXECUTE IMMEDIATE 'CREATE TABLE {table} (
            {column_ddl},
            LOAD_RUN_ID VARCHAR2(250),
            LOADED_AT TIMESTAMP,
            SOURCE_SHA256 VARCHAR2(64)
                )';
              END IF;
            END;
            """
        )

        # Idempotent: this run owns its own slice of the staging table.
        oracle.run(f"DELETE FROM {table} WHERE LOAD_RUN_ID = :run_id", parameters={"run_id": run_id})

        loaded_at = datetime.utcnow()
        rows = [
            list(record) + [run_id, loaded_at, metrics["sha256"]]
            for record in frame.itertuples(index=False, name=None)
        ]
        target_fields = columns + ["LOAD_RUN_ID", "LOADED_AT", "SOURCE_SHA256"]

        oracle.insert_rows(table=table, rows=rows, target_fields=target_fields, commit_every=1000)

        print(f"Loaded {len(rows):,} rows into {table} for run_id={run_id}")
        return len(rows)

    @task
    def archive_on_sftp(params: dict, **context) -> str:
        """Zip the source file in place and move the archive into archive/."""
        source = _remote_path(params)
        archive_dir = params["archive_dir"]
        stem = posixpath.splitext(params["filename"])[0]
        stamp = context["dag_run"].run_after.strftime("%Y%m%dT%H%M%S")
        archive_path = posixpath.join(archive_dir, f"{stem}_{stamp}.zip")

        hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)
        if not hook.path_exists(archive_dir):
            hook.create_directory(archive_dir)

        client = hook.get_conn()
        with client.open(source, "rb") as fh:
            fh.prefetch()
            payload = fh.read()

        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(params["filename"], payload)
        buffer.seek(0)

        client.putfo(buffer, archive_path, confirm=True)
        client.remove(source)

        print(f"Archived {source} -> {archive_path} ({buffer.getbuffer().nbytes:,} zipped bytes)")
        return archive_path

    metrics = profile_claims_file()
    index_value = compute_claims_index(metrics)
    loaded = load_to_staging(metrics)
    archived = archive_on_sftp()

    wait_for_file >> metrics >> index_value >> loaded >> archived


claims_sftp_to_oracle_staging()
