from datetime import timedelta

import pendulum
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sdk import dag

SSH_CONN_ID = "azure_member_demo_vm"

DEMO_DIR = (
    "/tmp/airflow-member-demo/"
    "{{ run_id | replace(':', '-') | replace('+', '-') }}"
)

RUN_SCRIPT = f"{DEMO_DIR}/run_member_load.sh"


@dag(
    dag_id="member_load_test_script_peeyush",
    description="Test trigger for log retrieval",
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

    # 1. Verify that the command is actually running on the Azure VM.
    verify_vm = SSHOperator(
        task_id="verify_azure_vm",
        ssh_conn_id=SSH_CONN_ID,
        command="hostname && whoami && date",
    )

    # 2. Create a unique workspace and shell script for this DAG run.
    create_test_script = SSHOperator(
        task_id="create_test_script",
        ssh_conn_id=SSH_CONN_ID,
        command=f"""
set -euo pipefail

mkdir -p "{DEMO_DIR}"

cat > "{RUN_SCRIPT}" <<'SCRIPT'
#!/usr/bin/env bash

set -euo pipefail

echo "========================================"
echo " Starting member load test run"
echo "========================================"

echo ""
echo "[1/4] Initializing workspace..."
mkdir -p "{DEMO_DIR}"
echo "initialized" > "{DEMO_DIR}/status.txt"
echo "Initialization complete"

echo ""
echo "[2/4] Loading member history..."
echo "member_history_complete" > "{DEMO_DIR}/member_history.txt"
echo "Member history load complete"

echo ""
echo "[3/4] Loading member cross reference..."
echo "member_cross_reference_complete" > "{DEMO_DIR}/member_cross_reference.txt"
echo "Member cross reference load complete"

echo ""
echo "[4/4] Loading member risk..."
echo "member_risk_complete" > "{DEMO_DIR}/member_risk.txt"
echo "Member risk load complete"

echo ""
echo "========================================"
echo " Test run complete"
echo "========================================"

echo ""
echo "Run directory:"
echo "{DEMO_DIR}"

echo ""
echo "Generated files:"
ls -la "{DEMO_DIR}"

echo ""
echo "Result:"
cat <<RESULT
{{
  "status": "success",
  "workspace": "{DEMO_DIR}",
  "member_history": "complete",
  "member_cross_reference": "complete",
  "member_risk": "complete"
}}
RESULT
SCRIPT

chmod +x "{RUN_SCRIPT}"

echo "Created test script:"
echo "{RUN_SCRIPT}"
""",
    )

    # 3. Execute the generated shell script.
    # stdout/stderr from the script will be returned in the Airflow task log.
    execute_test_script = SSHOperator(
        task_id="execute_test_script",
        ssh_conn_id=SSH_CONN_ID,
        command=f"""
set -euo pipefail

echo "Executing test script: {RUN_SCRIPT}"
echo ""

"{RUN_SCRIPT}"

EXIT_CODE=$?

echo ""
echo "========================================"
echo " Shell script exit code: $EXIT_CODE"
echo "========================================"

exit $EXIT_CODE
""",
    )

    # 4. Read back the generated result so the final task also returns evidence.
    collect_output = SSHOperator(
        task_id="collect_test_output",
        ssh_conn_id=SSH_CONN_ID,
        command=f"""
set -euo pipefail

echo "========================================"
echo " Test run output"
echo "========================================"

echo ""
echo "--- status.txt ---"
cat "{DEMO_DIR}/status.txt"

echo ""
echo "--- member_history.txt ---"
cat "{DEMO_DIR}/member_history.txt"

echo ""
echo "--- member_cross_reference.txt ---"
cat "{DEMO_DIR}/member_cross_reference.txt"

echo ""
echo "--- member_risk.txt ---"
cat "{DEMO_DIR}/member_risk.txt"

echo ""
echo "========================================"
echo " Output collection complete"
echo "========================================"
""",
    )

    verify_vm >> create_test_script >> execute_test_script >> collect_output


member_load_ssh_demo()