"""
Apache Airflow DAG — TEP Chemical Hazard Detection Pipeline.

Orchestrates the full pipeline:
    1. ingest_raw_data   → batch ingest CSVs into PostgreSQL
    2. dbt_run           → run dbt transformations
    3. dbt_test          → run dbt data quality tests
    4. train_model       → train RandomForest fault classifier
    5. alert_anomalies   → check fault_summary for high anomaly rates

Schedule: @hourly
"""

import os
import sys
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger("tep_pipeline_dag")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_DIR = r"C:\Users\abc\Desktop\MAJOR\tep-hazard-pipeline"

# Add project directory to sys.path so we can import local modules
sys.path.insert(0, PROJECT_DIR)

# ---------------------------------------------------------------------------
# Default DAG arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner": "tep_pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "depends_on_past": False,
}

# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def run_ingestion(**kwargs):
    """Execute batch ingestion."""
    from ingestion.batch_ingest import main as ingest_main
    logger.info("Starting batch ingestion...")
    ingest_main()
    logger.info("Batch ingestion complete.")


def run_ml_training(**kwargs):
    """Execute ML model training."""
    from ml_model.train import main as train_main
    logger.info("Starting ML model training...")
    train_main()
    logger.info("ML training complete.")


def run_anomaly_alerts(**kwargs):
    """Query fault_summary and alert on high anomaly percentages."""
    from dotenv import load_dotenv
    from sqlalchemy import create_engine, text

    env_path = os.path.join(PROJECT_DIR, ".env")
    load_dotenv(dotenv_path=env_path)

    db_url = os.getenv("DB_URL", "postgresql://tep_user:tep_pass@localhost/tep_warehouse")
    engine = create_engine(db_url)

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT fault_label, anomaly_pct "
                    "FROM marts.fault_summary "
                    "WHERE anomaly_pct > 20 "
                    "ORDER BY anomaly_pct DESC"
                )
            )
            rows = result.fetchall()

            if rows:
                logger.warning("⚠️  HIGH ANOMALY ALERT — %d fault types exceed 20%% anomaly rate:", len(rows))
                for row in rows:
                    logger.warning(
                        "  Fault %d → %.2f%% anomaly rate",
                        row[0],
                        row[1],
                    )
            else:
                logger.info("✅ No fault types exceed 20%% anomaly threshold.")
    except Exception as exc:
        logger.error("Anomaly alert check failed: %s", exc)
        raise
    finally:
        engine.dispose()


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="tep_hazard_pipeline",
    default_args=default_args,
    description="TEP Chemical Hazard Detection — full pipeline orchestration",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["tep", "chemical", "hazard", "ml"],
) as dag:

    # Task 1 — Ingest raw data
    ingest_raw_data = PythonOperator(
        task_id="ingest_raw_data",
        python_callable=run_ingestion,
    )

    # Task 2 — dbt run (transformations)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {PROJECT_DIR} && dbt run --project-dir dbt_project",
    )

    # Task 3 — dbt test (data quality)
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {PROJECT_DIR} && dbt test --project-dir dbt_project",
    )

    # Task 4 — Train ML model
    train_model = PythonOperator(
        task_id="train_model",
        python_callable=run_ml_training,
    )

    # Task 5 — Alert on anomalies
    alert_anomalies = PythonOperator(
        task_id="alert_anomalies",
        python_callable=run_anomaly_alerts,
    )

    # Chain: ingest >> dbt_run >> dbt_test >> train_model >> alert_anomalies
    ingest_raw_data >> dbt_run >> dbt_test >> train_model >> alert_anomalies
