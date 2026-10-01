"""Claims parallel-load demo.

Waits for a 6,000-row claims CSV on SFTP, loads it into Oracle as 6 parallel
1,000-row chunks, then logs a per-chunk load summary.

STAGING.CLAIMS_PARALLEL_RAW is created by the DBA, not by this DAG.
"""

from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.sdk import dag, task

SFTP_CONN_ID = "sftp_claims"
ORACLE_CONN_ID = "oracle_staging"
CLAIMS_FILE = "claims_data_6k.csv"
TARGET_TABLE = "STAGING.CLAIMS_PARALLEL_RAW"
CHUNKS = [1, 2, 3, 4, 5, 6]
ROWS_PER_CHUNK = 1000


@dag(
    dag_id="claims_sftp_to_oracle_parallel",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "claims", "parallel"],
    doc_md=__doc__,
)
def claims_sftp_to_oracle_parallel():
    wait_for_file = SFTPSensor(
        task_id="wait_for_file",
        sftp_conn_id=SFTP_CONN_ID,
        path=CLAIMS_FILE,
        poke_interval=30,
        timeout=1800,
        deferrable=True,
    )

    clear_table = SQLExecuteQueryOperator(
        task_id="clear_table",
        conn_id=ORACLE_CONN_ID,
        sql=f"TRUNCATE TABLE {TARGET_TABLE}",
    )

    @task
    def load_chunk(chunk: int):
        import pandas as pd

        with SFTPHook(ssh_conn_id=SFTP_CONN_ID).get_conn() as sftp, sftp.open(CLAIMS_FILE) as f:
            df = pd.read_csv(f, parse_dates=["SERVICE_DATE"])

        rows = df.iloc[(chunk - 1) * ROWS_PER_CHUNK : chunk * ROWS_PER_CHUNK].assign(CHUNK_ID=chunk)
        OracleHook(oracle_conn_id=ORACLE_CONN_ID).insert_rows(
            table=TARGET_TABLE,
            rows=list(rows.itertuples(index=False, name=None)),
            target_fields=list(rows.columns),
            commit_every=ROWS_PER_CHUNK,
        )

    load_summary = SQLExecuteQueryOperator(
        task_id="load_summary",
        conn_id=ORACLE_CONN_ID,
        sql=f"""
            SELECT NVL(TO_CHAR(CHUNK_ID), 'TOTAL')           AS CHUNK,
                   COUNT(*)                                 AS ROWS_LOADED,
                   TO_CHAR(MIN(LOADED_AT), 'HH24:MI:SS.FF3') AS FIRST_ROW_AT,
                   TO_CHAR(MAX(LOADED_AT), 'HH24:MI:SS.FF3') AS LAST_ROW_AT
            FROM {TARGET_TABLE}
            GROUP BY ROLLUP (CHUNK_ID)
            ORDER BY CHUNK_ID NULLS LAST
        """,
        show_return_value_in_logs=True,
    )

    load_chunks = [load_chunk.override(task_id=f"load_chunk_{n}")(n) for n in CHUNKS]

    wait_for_file >> clear_table >> load_chunks >> load_summary


claims_sftp_to_oracle_parallel()
