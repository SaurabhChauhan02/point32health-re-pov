from datetime import timedelta

import pendulum
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sdk import dag

SSH_CONN_ID = "azure_member_demo_vm"
DEMO_DIR = "/tmp/airflow-member-demo/{{ run_id | replace(':', '-') | replace('+', '-') }}"


@dag(
    dag_id="member_load_ssh_demo",
    description="Test trigger for log retrival ",
    start_date=pendulum.datetime(2018, 4, 24, tz="America/New_York"),
    schedule="0 7 * * 1-5",
    catchup=False,
    is_paused_upon_creation=True,
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
    # Connect to the Azure VM before starting any work. The hostname and username in
    # the task log prove that the command ran on the VM rather than on an Airflow worker.
    verify_vm = SSHOperator(
        task_id="verify_azure_vm",
        ssh_conn_id=SSH_CONN_ID,
        command="hostname && whoami && date",
    )

    # Create a separate workspace for this run before releasing the load tasks. This
    # represents the process-control initialization and gives every run its own evidence.
    initialize = SSHOperator(
        task_id="initialize_member_load",
        ssh_conn_id=SSH_CONN_ID,
        command=f"mkdir -p {DEMO_DIR} && echo initialized > {DEMO_DIR}/status.txt",
    )

    # Represent the member-history load with a remote command. The marker file provides
    # a simple result that can be inspected directly on the VM after the demo.
    load_member_history = SSHOperator(
        task_id="load_member_history",
        ssh_conn_id=SSH_CONN_ID,
        command=f"echo member_history_complete > {DEMO_DIR}/member_history.txt",
    )

    

    # The sequence mirrors the scheduler requirement: connectivity first, initialization
    # second, parallel member loads third, and a single completion point at the end.
    verify_vm >> initialize >> [
        load_member_history,
        load_member_cross_reference,
        load_member_risk,
    ] >> complete


member_load_ssh_demo()