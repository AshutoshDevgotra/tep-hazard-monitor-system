"""
Fault Injection Simulator for TEP Chemical Hazard Detection Pipeline.

Injects fake dangerous sensor readings into the database to demonstrate
the alarm system detecting and responding to hazardous conditions.

Can be run standalone or called from the dashboard.
"""

import os
import sys
import logging
from datetime import datetime

import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("fault_injector")

# ---------------------------------------------------------------------------
# Load environment
# ---------------------------------------------------------------------------
ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
load_dotenv(dotenv_path=ENV_PATH)

DB_URL = os.getenv("DB_URL", "postgresql://tep_user:tep_pass@localhost/tep_warehouse")

# ---------------------------------------------------------------------------
# Fault scenarios — each defines extreme sensor values
# ---------------------------------------------------------------------------
FAULT_SCENARIOS = {
    "thermal_runaway": {
        "name": "🔥 Thermal Runaway (Fault 4)",
        "description": "Reactor cooling water fails → temperature spikes uncontrollably",
        "fault_label": 4,
        "reactor_temp": 155.0,        # Normal ~120.4, this is dangerously high
        "reactor_pressure": 2800.0,   # Normal ~2705, elevated from heat
        "reactor_level": 70.0,
        "feed_a_flow": 0.25,
        "feed_d_flow": 3600.0,
        "reactor_feed_rate": 26.0,
        "xmv_1": 60.0,
        "xmv_3": 40.0,
        "xmv_4": 90.0,               # Steam valve wide open trying to compensate
    },
    "pressure_surge": {
        "name": "💥 Pressure Surge (Fault 7)",
        "description": "C header pressure loss → reactor pressure drops, then surges",
        "fault_label": 7,
        "reactor_temp": 122.0,
        "reactor_pressure": 2950.0,   # Dangerous pressure spike
        "reactor_level": 85.0,        # Level rising due to blockage
        "feed_a_flow": 0.15,
        "feed_d_flow": 3200.0,
        "reactor_feed_rate": 18.0,    # Feed rate dropping
        "xmv_1": 45.0,
        "xmv_3": 70.0,               # Recycle valve forced open
        "xmv_4": 50.0,
    },
    "feed_contamination": {
        "name": "☠️ Feed Contamination (Fault 1)",
        "description": "A/C feed ratio shifts → wrong reactant balance, toxic byproducts",
        "fault_label": 1,
        "reactor_temp": 121.5,
        "reactor_pressure": 2720.0,
        "reactor_level": 68.0,
        "feed_a_flow": 0.55,          # Feed A abnormally high
        "feed_d_flow": 4200.0,        # Feed D also abnormal
        "reactor_feed_rate": 32.0,    # Overfeed condition
        "xmv_1": 75.0,               # D feed valve wide open
        "xmv_3": 30.0,
        "xmv_4": 55.0,
    },
    "cooling_oscillation": {
        "name": "🌡️ Unstable Cooling (Fault 11)",
        "description": "Random cooling water temp fluctuations → reactor temp oscillates",
        "fault_label": 11,
        "reactor_temp": 135.0,        # Temperature swinging high
        "reactor_pressure": 2680.0,
        "reactor_level": 62.0,
        "feed_a_flow": 0.26,
        "feed_d_flow": 3700.0,
        "reactor_feed_rate": 25.0,
        "xmv_1": 55.0,
        "xmv_3": 45.0,
        "xmv_4": 80.0,               # Steam valve hunting
    },
}


def get_engine():
    """Create SQLAlchemy engine."""
    try:
        return create_engine(DB_URL)
    except Exception as exc:
        logger.error("Failed to create engine: %s", exc)
        raise


def inject_fault(scenario_key, num_samples=100):
    """
    Inject fake faulty sensor readings into staging.int_sensor_features.

    Parameters
    ----------
    scenario_key : str — key from FAULT_SCENARIOS
    num_samples : int — number of fake samples to inject

    Returns
    -------
    dict with injection details
    """
    if scenario_key not in FAULT_SCENARIOS:
        logger.error("Unknown scenario: %s", scenario_key)
        return None

    scenario = FAULT_SCENARIOS[scenario_key]
    logger.info("Injecting fault scenario: %s", scenario["name"])
    logger.info("Description: %s", scenario["description"])

    engine = get_engine()

    # Generate fake samples with jitter (so they look realistic, not identical)
    np.random.seed(42)
    records = []
    for i in range(num_samples):
        jitter = np.random.normal(0, 0.5)
        record = {
            "simulation_run": 99999,  # Special demo run ID
            "sample_num": i + 1,
            "fault_label": scenario["fault_label"],
            "feed_a_flow": scenario["feed_a_flow"] + np.random.normal(0, 0.02),
            "feed_d_flow": scenario["feed_d_flow"] + np.random.normal(0, 50),
            "reactor_feed_rate": scenario["reactor_feed_rate"] + np.random.normal(0, 0.5),
            "reactor_pressure": scenario["reactor_pressure"] + np.random.normal(0, 15),
            "reactor_level": scenario["reactor_level"] + np.random.normal(0, 1),
            "reactor_temp": scenario["reactor_temp"] + np.random.normal(0, 2) + (jitter * 0.5),
            "product_sep_temp": 80.0 + np.random.normal(0, 1),
            "product_sep_pressure": 2633.0 + np.random.normal(0, 10),
            "stripper_pressure": 3102.0 + np.random.normal(0, 8),
            "stripper_temp": 65.7 + np.random.normal(0, 0.5),
            "d_feed_valve": scenario["xmv_1"] + np.random.normal(0, 1),
            "recycle_valve": scenario["xmv_3"] + np.random.normal(0, 1),
            "steam_valve": scenario["xmv_4"] + np.random.normal(0, 1),
            "transformed_at": datetime.utcnow(),
            # Rolling stats — set to normal baseline so the extreme values trigger anomalies
            "reactor_temp_rolling_mean": 120.4,
            "reactor_temp_rolling_std": 0.3,
            "reactor_pressure_rolling_mean": 2705.0,
            "reactor_pressure_rolling_std": 12.0,
            # Anomaly flags — mark as anomalous
            "temp_anomaly_flag": 1 if scenario["reactor_temp"] > 125 else 0,
            "pressure_anomaly_flag": 1 if scenario["reactor_pressure"] > 2800 else 0,
            "combined_anomaly_flag": 1,
        }
        records.append(record)

    df = pd.DataFrame(records)

    try:
        # First remove any previous demo data
        with engine.begin() as conn:
            conn.execute(text(
                "DELETE FROM staging.int_sensor_features WHERE simulation_run = 99999"
            ))
            logger.info("Cleared previous demo data.")

        # Insert new demo data
        df.to_sql(
            name="int_sensor_features",
            con=engine,
            schema="staging",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=50,
        )
        logger.info("✅ Injected %d faulty samples (simulation_run=99999)", num_samples)

        # Update fault_summary mart table
        _refresh_fault_summary(engine)

        result = {
            "scenario": scenario["name"],
            "description": scenario["description"],
            "fault_label": scenario["fault_label"],
            "samples_injected": num_samples,
            "reactor_temp": scenario["reactor_temp"],
            "reactor_pressure": scenario["reactor_pressure"],
            "normal_temp": 120.4,
            "normal_pressure": 2705.0,
            "temp_deviation": round(scenario["reactor_temp"] - 120.4, 1),
            "pressure_deviation": round(scenario["reactor_pressure"] - 2705.0, 1),
        }
        logger.info("Injection complete: %s", result)
        return result

    except Exception as exc:
        logger.error("Fault injection failed: %s", exc)
        raise
    finally:
        engine.dispose()


def _refresh_fault_summary(engine):
    """Rebuild the fault_summary mart after injection."""
    try:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS marts.fault_summary"))
            conn.execute(text("""
                CREATE TABLE marts.fault_summary AS
                SELECT
                    fault_label,
                    COUNT(*) AS total_samples,
                    ROUND(AVG(reactor_temp)::NUMERIC, 2) AS avg_reactor_temp,
                    ROUND(AVG(reactor_pressure)::NUMERIC, 2) AS avg_reactor_pressure,
                    SUM(combined_anomaly_flag) AS anomaly_count,
                    ROUND(
                        (SUM(combined_anomaly_flag)::NUMERIC / COUNT(*)) * 100, 2
                    ) AS anomaly_pct
                FROM staging.int_sensor_features
                GROUP BY fault_label
                ORDER BY fault_label
            """))
        logger.info("✅ marts.fault_summary refreshed.")
    except Exception as exc:
        logger.error("Failed to refresh fault_summary: %s", exc)


def clear_injected_data():
    """Remove all demo injected data."""
    engine = get_engine()
    try:
        with engine.begin() as conn:
            conn.execute(text(
                "DELETE FROM staging.int_sensor_features WHERE simulation_run = 99999"
            ))
        _refresh_fault_summary(engine)
        logger.info("✅ All demo data cleared.")
    except Exception as exc:
        logger.error("Failed to clear demo data: %s", exc)
    finally:
        engine.dispose()


def main():
    """Interactive CLI for fault injection."""
    logger.info("=" * 60)
    logger.info("TEP Fault Injection Simulator")
    logger.info("=" * 60)

    print("\nAvailable fault scenarios:")
    for key, scenario in FAULT_SCENARIOS.items():
        print(f"  {key:25s} → {scenario['name']}")
    print(f"  {'clear':25s} → Remove all injected data")

    choice = input("\nEnter scenario key: ").strip().lower()

    if choice == "clear":
        clear_injected_data()
    elif choice in FAULT_SCENARIOS:
        result = inject_fault(choice, num_samples=100)
        if result:
            print(f"\n✅ Injected: {result['scenario']}")
            print(f"   Reactor Temp: {result['reactor_temp']}°C (normal: {result['normal_temp']}°C, deviation: +{result['temp_deviation']}°C)")
            print(f"   Reactor Pressure: {result['reactor_pressure']} kPa (normal: {result['normal_pressure']} kPa, deviation: +{result['pressure_deviation']} kPa)")
            print(f"\n   → Open http://localhost:8501 to see the alarms!")
    else:
        print(f"Unknown scenario: {choice}")


if __name__ == "__main__":
    main()
