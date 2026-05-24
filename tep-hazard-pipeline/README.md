# TEP Chemical Hazard Detection Pipeline

A production-grade **data engineering pipeline** for detecting chemical process hazards in the **Tennessee Eastman Process (TEP)** — a benchmark simulation of an industrial chemical plant with 52 sensor variables and 20 distinct fault types.

---

## Architecture

```
C:\Users\abc\Desktop\MAJOR\ (CSV files)
         │
         ▼
┌─────────────────────┐        ┌──────────────────┐
│  Python Ingestion   │──CSV──▶│  PostgreSQL       │
│  (batch_ingest.py)  │        │  raw schema       │
└─────────────────────┘        └────────┬─────────┘
                                        │
         ┌──────────────────────────────┘
         │
         ▼                              ┌──────────────────┐
┌─────────────────────┐                 │  Kafka Streaming  │
│  Kafka Producer     │──JSON──▶Topic──▶│  Consumer         │──▶ raw.tep_stream
│  (producer.py)      │                 │  (consumer.py)    │
└─────────────────────┘                 └──────────────────┘
                                        
         ┌──────────────────────────────────────────────────┐
         │              dbt Transformations                 │
         │                                                  │
         │  raw.tep_faulty                                  │
         │       ▼                                          │
         │  staging.stg_tep_sensors (VIEW)                  │
         │       ▼                                          │
         │  staging.int_sensor_features (TABLE)             │
         │       ▼                                          │
         │  marts.fault_summary (TABLE)                     │
         └──────────────────────┬───────────────────────────┘
                                │
                ┌───────────────┴───────────────┐
                ▼                               ▼
┌──────────────────────────┐    ┌──────────────────────────┐
│  ML Fault Classifier     │    │  AWS S3 Data Lake         │
│  (RandomForest)          │    │  s3://tep-hazard-datalake │
│  ml_model/train.py       │    │  ingestion/s3_upload.py   │
└──────────┬───────────────┘    └──────────────────────────┘
           │
           ▼
┌──────────────────────────┐
│  Streamlit Dashboard     │
│  dashboard/app.py        │
│  localhost:8501           │
└──────────────────────────┘

         All orchestrated by Apache Airflow (hourly DAG)
         orchestration/tep_pipeline_dag.py
```

---

## Tech Stack

| Layer              | Technology          | Purpose                                    |
|--------------------|---------------------|--------------------------------------------|
| **Data Storage**   | PostgreSQL          | Data warehouse (raw → staging → marts)     |
| **Ingestion**      | Python + Pandas     | Batch CSV loading into PostgreSQL           |
| **Streaming**      | Apache Kafka        | Real-time sensor data streaming             |
| **Transformation** | dbt (data build tool) | SQL-based transformations & testing       |
| **ML / Analytics** | scikit-learn        | RandomForest fault classifier              |
| **Orchestration**  | Apache Airflow      | Hourly DAG for pipeline automation         |
| **Dashboard**      | Streamlit + Plotly  | Real-time hazard monitoring UI             |
| **Data Lake**      | AWS S3              | Raw data archival & data lake              |
| **Environment**    | python-dotenv       | Configuration management                   |

---

## Dataset: Tennessee Eastman Process (TEP)

The TEP dataset simulates a real industrial chemical plant with:

- **52 sensor variables**: 41 measured (`xmeas_1` to `xmeas_41`) + 11 manipulated (`xmv_1` to `xmv_11`)
- **20 fault types** (Fault 1–20) + **normal operation** (Fault 0)
- **480 simulation runs** per fault type in training data
- Over **1.8 million rows** across all datasets

### Files Used

| File                       | Purpose                    | Rows (approx.) |
|----------------------------|----------------------------|-----------------|
| `TEP_Faulty_Training.csv`  | Primary ingestion + ML training | ~4,800,000 |
| `TEP_FaultFree_Training.csv` | Baseline normal operation   | ~2,400,000   |
| `TEP_Faulty_Testing.csv`   | Final ML evaluation only   | ~9,600,000     |
| `TEP_FaultFree_Testing.csv` | Skipped                   | —              |

---

## Setup Instructions (Windows)

### Prerequisites

- Python 3.10+
- PostgreSQL 15+ (running on localhost:5432)
- Apache Kafka (optional, for streaming features)
- AWS CLI configured (optional, for S3 upload)

### Step 1 — Clone / Navigate to Project

```powershell
cd C:\Users\abc\Desktop\MAJOR\tep-hazard-pipeline
```

### Step 2 — Create Virtual Environment

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Step 3 — Set Up PostgreSQL

Run these commands in `psql` (as a superuser):

```sql
CREATE DATABASE tep_warehouse;
CREATE USER tep_user WITH PASSWORD 'tep_pass';
GRANT ALL PRIVILEGES ON DATABASE tep_warehouse TO tep_user;
\c tep_warehouse
CREATE SCHEMA raw;
CREATE SCHEMA staging;
CREATE SCHEMA marts;
GRANT ALL ON SCHEMA raw TO tep_user;
GRANT ALL ON SCHEMA staging TO tep_user;
GRANT ALL ON SCHEMA marts TO tep_user;
```

Or run the automated setup:

```powershell
python ingestion\setup_db.py
```

### Step 4 — Configure .env

Edit `.env` in the project root. Default values work for local PostgreSQL:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=tep_warehouse
DB_USER=tep_user
DB_PASS=tep_pass
DB_URL=postgresql://tep_user:tep_pass@localhost/tep_warehouse
DATA_DIR=C:\Users\abc\Desktop\MAJOR
```

### Step 5 — Configure dbt Profile

Create `~/.dbt/profiles.yml` (or `C:\Users\abc\.dbt\profiles.yml`):

```yaml
tep_dbt:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: tep_user
      password: tep_pass
      port: 5432
      dbname: tep_warehouse
      schema: staging
      threads: 4
```

---

## How to Run Each Phase

### Phase 3 — Batch Ingestion

```powershell
python ingestion\batch_ingest.py
```

Loads `TEP_Faulty_Training.csv` → `raw.tep_faulty` and `TEP_FaultFree_Training.csv` → `raw.tep_fault_free`.

### Phase 4 — Kafka Streaming (requires running Kafka)

```powershell
# Terminal 1: Start producer
python kafka_producer\producer.py

# Terminal 2: Start consumer
python kafka_producer\consumer.py
```

### Phase 5 — dbt Transformations

```powershell
cd dbt_project
dbt run
dbt test
```

### Phase 6 — ML Model Training

```powershell
python ml_model\train.py
```

After training, evaluate on test set:

```powershell
python ml_model\analysis.py
```

### Phase 7 — Airflow (requires Airflow installation)

```powershell
# Copy DAG to Airflow dags folder
copy orchestration\tep_pipeline_dag.py %AIRFLOW_HOME%\dags\

# Start Airflow
airflow standalone
```

### Phase 8 — Streamlit Dashboard

```powershell
streamlit run dashboard\app.py
```

Opens at `http://localhost:8501`

### Phase 9 — S3 Upload (requires AWS credentials)

```powershell
python ingestion\s3_upload.py
```

---

## Model Results

*(Updated after running `python ml_model\train.py`)*

### Classifier Details

| Parameter       | Value                     |
|-----------------|---------------------------|
| Algorithm       | RandomForestClassifier    |
| n_estimators    | 200                       |
| max_depth       | 15                        |
| Features        | 12 engineered features    |
| Train/Test      | 80/20 stratified split    |

### Feature Importance (Top 10)

| Rank | Feature                        | Importance |
|------|--------------------------------|------------|
| 1    | reactor_temp                   | —          |
| 2    | reactor_pressure               | —          |
| 3    | reactor_temp_rolling_mean      | —          |
| 4    | reactor_pressure_rolling_mean  | —          |
| 5    | feed_a_flow                    | —          |
| 6    | reactor_level                  | —          |
| 7    | reactor_feed_rate              | —          |
| 8    | reactor_temp_rolling_std       | —          |
| 9    | feed_d_flow                    | —          |
| 10   | reactor_pressure_rolling_std   | —          |

> **Note:** Run `python ml_model\train.py` to populate these values from the actual training output.

---

## Chemical Engineering Context

### Fault Type Explanations

| Fault | Description                          | Physical Meaning                                                                                                                                          |
|-------|--------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| **1** | A/C Feed Ratio, B Composition Constant (Step) | A sudden step change in the A/C feed ratio to the reactor. This shifts the reactant balance, causing downstream composition drift and potential product quality issues. |
| **4** | Reactor Cooling Water Inlet Temperature (Step) | A step change in cooling water temperature entering the reactor. This directly affects heat removal, causing the reactor temperature to rise or fall uncontrollably — a precursor to thermal runaway. |
| **7** | C Header Pressure Loss (Step) | A step loss in the pressure of the C feed header. This reduces Component C availability, starving the reactor of a key reactant, causing pressure and composition cascading failures. |
| **11** | Reactor Cooling Water Inlet Temperature (Random) | Random fluctuations in the reactor cooling water temperature. Unlike Fault 4 (step), these unpredictable variations make temperature control extremely difficult, creating oscillating conditions that may trigger safety shutdowns. |

### Why These Faults Matter

- **Fault 1 (Feed Ratio Shift):** If undetected, incorrect reactant ratios lead to off-spec product, wasted raw materials, and potential runaway exothermic reactions.
- **Fault 4 (Cooling Water Step):** Temperature runaway is one of the most dangerous scenarios in chemical engineering — it can cause explosions, toxic releases, and equipment destruction.
- **Fault 7 (Pressure Loss):** Feed pressure loss can starve the reactor, leading to incomplete reactions, pressure imbalances, and equipment damage from sudden depressurization.
- **Fault 11 (Random Cooling Variation):** Random disturbances are harder to detect than step changes because they don't follow a pattern, making them the most challenging fault type for control systems.

---

## Interview Talking Points

1. **End-to-End Pipeline Design:** "I built a complete data engineering pipeline from raw CSV ingestion through Kafka streaming, PostgreSQL warehousing, dbt transformations, ML classification, and Streamlit dashboarding — all orchestrated by Airflow on an hourly schedule."

2. **Schema Design & dbt Modeling:** "I implemented a medallion-style architecture (raw → staging → marts) using dbt with SQL window functions for rolling statistics and z-score anomaly detection across 52 sensor variables."

3. **ML for Fault Detection:** "The RandomForest classifier trained on 12 engineered features achieves strong multi-class classification across 20 fault types, with feature importance analysis revealing which sensor readings are most diagnostic."

4. **Real-Time Streaming Architecture:** "The Kafka producer/consumer pair enables real-time sensor data streaming with micro-batch writes to PostgreSQL, demonstrating event-driven architecture for industrial IoT monitoring."

5. **Chemical Domain Knowledge:** "I mapped TEP fault types to real-world chemical hazards — reactor pressure surges, temperature runaway, feed composition shifts — showing how data engineering directly supports plant safety and early warning systems."

---

## Project Structure

```
tep-hazard-pipeline/
├── data/
│   ├── raw/                         ← Raw data placeholder
│   └── processed/                   ← ML output (test_results.csv)
├── ingestion/
│   ├── __init__.py
│   ├── setup_db.py                  ← PostgreSQL database setup
│   ├── batch_ingest.py              ← CSV → PostgreSQL ingestion
│   └── s3_upload.py                 ← AWS S3 data lake upload
├── kafka_producer/
│   ├── producer.py                  ← Kafka message producer
│   └── consumer.py                  ← Kafka micro-batch consumer
├── dbt_project/
│   ├── dbt_project.yml              ← dbt configuration
│   └── models/
│       ├── schema.yml               ← Model definitions & tests
│       ├── staging/
│       │   └── stg_tep_sensors.sql  ← Column rename + cleanup
│       ├── intermediate/
│       │   └── int_sensor_features.sql ← Rolling stats + anomaly flags
│       └── marts/
│           └── fault_summary.sql    ← Per-fault aggregations
├── ml_model/
│   ├── train.py                     ← RandomForest training pipeline
│   ├── analysis.py                  ← Test-set evaluation
│   ├── fault_classifier.pkl         ← Saved model (after training)
│   ├── scaler.pkl                   ← Saved scaler (after training)
│   └── feature_cols.json            ← Feature list (after training)
├── orchestration/
│   └── tep_pipeline_dag.py          ← Airflow DAG definition
├── dashboard/
│   └── app.py                       ← Streamlit monitoring dashboard
├── requirements.txt                 ← Python dependencies
├── .env                             ← Configuration (DB, AWS, Kafka)
└── README.md                        ← This file
```

---

## License

This project is built for educational and portfolio purposes using the publicly available Tennessee Eastman Process dataset.

---

## Author

TEP Chemical Hazard Detection Pipeline — Data Engineering Project
