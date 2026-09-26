"""Minimal SSH demo based on DM#HRP_ODSSTG_MBR_LD_DAY."""

from datetime import timedelta

import pendulum
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sdk import dag

SSH_CONN_ID = "azure_member_demo_vm"
DEMO_DIR = "/tmp/airflow-member-demo/{{ ds_nodash }}"


@dag(
    dag_id="member_load_ssh_demo",
    description="Trigger a simple member-load workflow on an Azure VM over SSH",
    start_date=pendulum.datetime(2018, 4, 24, tz="America/New_York"),
    schedule="0 7 * * 1-5",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "member-data-team",
        "retries": 6,
        "retry_delay": timedelta(seconds=180),
    },
    tags=["demo", "member", "ssh", "azure"],
    doc_md=__doc__,
)
def member_load_ssh_demo():
    # Proves that the Airflow worker can connect to the Azure VM and run a command.
    verify_vm = SSHOperator(
        task_id="verify_azure_vm",
        ssh_conn_id=SSH_CONN_ID,
        command="hostname && whoami && date",
    )

    # Creates a dated workspace that stands in for the legacy process-control initialization.
    initialize = SSHOperator(
        task_id="initialize_member_load",
        ssh_conn_id=SSH_CONN_ID,
        command=f"mkdir -p {DEMO_DIR} && echo initialized > {DEMO_DIR}/status.txt",
    )

    # Represents the member-history load and writes a completion marker on the VM.
    load_member_history = SSHOperator(
        task_id="load_member_history",
        ssh_conn_id=SSH_CONN_ID,
        command=f"echo member_history_complete > {DEMO_DIR}/member_history.txt",
    )

    # Represents the member cross-reference load as one branch of the parallel fan-out.
    load_member_cross_reference = SSHOperator(
        task_id="load_member_cross_reference",
        ssh_conn_id=SSH_CONN_ID,
        command=f"echo member_cross_reference_complete > {DEMO_DIR}/member_cross_reference.txt",
    )

    # Represents the member-risk load as another independently monitored VM command.
    load_member_risk = SSHOperator(
        task_id="load_member_risk",
        ssh_conn_id=SSH_CONN_ID,
        command=f"echo member_risk_complete > {DEMO_DIR}/member_risk.txt",
    )

    # Runs only after every branch succeeds and lists the evidence created on the VM.
    complete = SSHOperator(
        task_id="complete_member_load",
        ssh_conn_id=SSH_CONN_ID,
        command=f"echo complete >> {DEMO_DIR}/status.txt && ls -la {DEMO_DIR}",
    )

    verify_vm >> initialize >> [
        load_member_history,
        load_member_cross_reference,
        load_member_risk,
    ] >> complete


member_load_ssh_demo()
