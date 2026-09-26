"""# Member Load SSH Demo

This DAG shows how Astro can replace the scheduling and dependency-management
portion of `DM#HRP_ODSSTG_MBR_LD_DAY` while the workload continues to run on a
customer-managed VM.

The weekday 7:00 AM Eastern schedule is retained to show how the original calendar
maps to Airflow. The DAG is created paused and should be triggered manually during
the demo. Its commands create small marker files instead of running Informatica.
"""

from datetime import timedelta

import pendulum
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sdk import dag

SSH_CONN_ID = "azure_member_demo_vm"
DEMO_DIR = "/tmp/airflow-member-demo/{{ run_id | replace(':', '-') | replace('+', '-') }}"


@dag(
    dag_id="member_load_ssh_demo",
    description="Trigger a simple member-load workflow on an Azure VM over SSH",
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
    # First prove the remote worker can retrieve the SSH connection and run on the Azure VM.
    verify_vm = SSHOperator(
        task_id="verify_azure_vm",
        ssh_conn_id=SSH_CONN_ID,
        command="hostname && whoami && date",
    )

    # Create one workspace per DAG run, representing the legacy process-control initialization.
    initialize = SSHOperator(
        task_id="initialize_member_load",
        ssh_conn_id=SSH_CONN_ID,
        command=f"mkdir -p {DEMO_DIR} && echo initialized > {DEMO_DIR}/status.txt",
    )

    # Start a representative member-history job and leave a marker that the customer can inspect.
    load_member_history = SSHOperator(
        task_id="load_member_history",
        ssh_conn_id=SSH_CONN_ID,
        command=f"echo member_history_complete > {DEMO_DIR}/member_history.txt",
    )

    # Run the cross-reference branch in parallel to demonstrate independent remote jobs.
    load_member_cross_reference = SSHOperator(
        task_id="load_member_cross_reference",
        ssh_conn_id=SSH_CONN_ID,
        command=f"echo member_cross_reference_complete > {DEMO_DIR}/member_cross_reference.txt",
    )

    # Run a third member-domain branch so Airflow's fan-out is visible in the Graph view.
    load_member_risk = SSHOperator(
        task_id="load_member_risk",
        ssh_conn_id=SSH_CONN_ID,
        command=f"echo member_risk_complete > {DEMO_DIR}/member_risk.txt",
    )

    # Join the branches and display the VM files only when every remote job has succeeded.
    complete = SSHOperator(
        task_id="complete_member_load",
        ssh_conn_id=SSH_CONN_ID,
        command=f"echo complete >> {DEMO_DIR}/status.txt && ls -la {DEMO_DIR}",
    )

    # These dependencies prevent the load branches from starting before initialization completes.
    verify_vm >> initialize >> [
        load_member_history,
        load_member_cross_reference,
        load_member_risk,
    ] >> complete


member_load_ssh_demo()
