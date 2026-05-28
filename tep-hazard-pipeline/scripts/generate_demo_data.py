"""
Generate realistic demo CSV data for Streamlit Cloud deployment.

Creates small CSV snapshots that mirror the PostgreSQL tables so the
dashboard can run without a database connection.

Output files:
    data/demo/fault_summary_demo.csv      (~21 rows)
    data/demo/sensor_features_demo.csv    (~2,100 rows — 10 sim runs × ~210 samples)
"""

import os
import sys
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Output directory
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEMO_DIR = os.path.join(PROJECT_ROOT, "data", "demo")
os.makedirs(DEMO_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# TEP baseline sensor values (normal operation ~ Fault 0)
# ---------------------------------------------------------------------------
NORMAL = {
    "reactor_temp": 120.4,
    "reactor_pressure": 2705.0,
}

# Fault-specific deviations (how each fault shifts reactor temp & pressure)
FAULT_PROFILES = {
    0:  {"temp_shift": 0.0,   "press_shift": 0.0,   "temp_std": 0.3,  "press_std": 12.0, "anomaly_rate": 0.002},
    1:  {"temp_shift": 1.1,   "press_shift": 15.0,  "temp_std": 0.8,  "press_std": 18.0, "anomaly_rate": 0.004},
    2:  {"temp_shift": 0.8,   "press_shift": 10.0,  "temp_std": 0.6,  "press_std": 15.0, "anomaly_rate": 0.003},
    3:  {"temp_shift": 2.5,   "press_shift": 5.0,   "temp_std": 1.2,  "press_std": 14.0, "anomaly_rate": 0.004},
    4:  {"temp_shift": 8.0,   "press_shift": 40.0,  "temp_std": 2.5,  "press_std": 25.0, "anomaly_rate": 0.008},
    5:  {"temp_shift": 1.5,   "press_shift": 8.0,   "temp_std": 0.9,  "press_std": 16.0, "anomaly_rate": 0.003},
    6:  {"temp_shift": -2.0,  "press_shift": -30.0, "temp_std": 1.5,  "press_std": 22.0, "anomaly_rate": 0.006},
    7:  {"temp_shift": 1.0,   "press_shift": -50.0, "temp_std": 1.0,  "press_std": 30.0, "anomaly_rate": 0.007},
    8:  {"temp_shift": 3.0,   "press_shift": 20.0,  "temp_std": 1.8,  "press_std": 20.0, "anomaly_rate": 0.005},
    9:  {"temp_shift": 2.2,   "press_shift": 6.0,   "temp_std": 1.3,  "press_std": 14.0, "anomaly_rate": 0.004},
    10: {"temp_shift": 1.8,   "press_shift": 7.0,   "temp_std": 1.1,  "press_std": 15.0, "anomaly_rate": 0.003},
    11: {"temp_shift": 5.0,   "press_shift": -10.0, "temp_std": 3.0,  "press_std": 20.0, "anomaly_rate": 0.006},
    12: {"temp_shift": 1.2,   "press_shift": 5.0,   "temp_std": 0.8,  "press_std": 13.0, "anomaly_rate": 0.003},
    13: {"temp_shift": 0.5,   "press_shift": 3.0,   "temp_std": 0.4,  "press_std": 11.0, "anomaly_rate": 0.002},
    14: {"temp_shift": 4.0,   "press_shift": 25.0,  "temp_std": 2.0,  "press_std": 22.0, "anomaly_rate": 0.005},
    15: {"temp_shift": 1.0,   "press_shift": 8.0,   "temp_std": 0.7,  "press_std": 14.0, "anomaly_rate": 0.003},
    16: {"temp_shift": 3.5,   "press_shift": 30.0,  "temp_std": 1.5,  "press_std": 20.0, "anomaly_rate": 0.005},
    17: {"temp_shift": 1.5,   "press_shift": 12.0,  "temp_std": 0.9,  "press_std": 16.0, "anomaly_rate": 0.003},
    18: {"temp_shift": 1.8,   "press_shift": 15.0,  "temp_std": 1.0,  "press_std": 17.0, "anomaly_rate": 0.004},
    19: {"temp_shift": 5.5,   "press_shift": 35.0,  "temp_std": 2.2,  "press_std": 24.0, "anomaly_rate": 0.007},
    20: {"temp_shift": 6.0,   "press_shift": 45.0,  "temp_std": 2.8,  "press_std": 28.0, "anomaly_rate": 0.008},
}

SAMPLES_PER_FAULT = 480  # matches TEP training data structure


def generate_fault_summary():
    """Generate marts.fault_summary equivalent (21 rows, one per fault type)."""
    rows = []
    for fault_label in range(21):
        profile = FAULT_PROFILES[fault_label]
        total_samples = SAMPLES_PER_FAULT * 10  # 10 simulation runs per fault
        avg_temp = round(NORMAL["reactor_temp"] + profile["temp_shift"], 2)
        avg_press = round(NORMAL["reactor_pressure"] + profile["press_shift"], 2)
        anomaly_pct = round(profile["anomaly_rate"] * 100, 2)
        anomaly_count = int(total_samples * profile["anomaly_rate"])

        rows.append({
            "fault_label": fault_label,
            "total_samples": total_samples,
            "avg_reactor_temp": avg_temp,
            "avg_reactor_pressure": avg_press,
            "anomaly_count": anomaly_count,
            "anomaly_pct": anomaly_pct,
        })

    df = pd.DataFrame(rows)
    return df


def generate_sensor_features(num_sim_runs=10, samples_per_run=210):
    """
    Generate staging.int_sensor_features equivalent.

    Creates a few simulation runs with realistic sensor time-series.
    """
    np.random.seed(42)
    all_rows = []

    # Spread simulation runs across different fault types
    fault_assignments = [0, 1, 4, 7, 11, 0, 3, 8, 14, 20]

    for sim_idx in range(num_sim_runs):
        sim_run = sim_idx + 1
        fault_label = fault_assignments[sim_idx % len(fault_assignments)]
        profile = FAULT_PROFILES[fault_label]

        for sample in range(1, samples_per_run + 1):
            base_temp = NORMAL["reactor_temp"] + profile["temp_shift"]
            base_press = NORMAL["reactor_pressure"] + profile["press_shift"]

            # Add time-varying drift for realism (fault onset at ~sample 80)
            if fault_label > 0 and sample > 80:
                onset_factor = min(1.0, (sample - 80) / 50.0)
                temp_val = base_temp + np.random.normal(0, profile["temp_std"]) * onset_factor
                press_val = base_press + np.random.normal(0, profile["press_std"]) * onset_factor
            else:
                temp_val = NORMAL["reactor_temp"] + np.random.normal(0, 0.3)
                press_val = NORMAL["reactor_pressure"] + np.random.normal(0, 12.0)

            # Rolling stats (baseline from normal operation)
            temp_rolling_mean = round(NORMAL["reactor_temp"] + np.random.normal(0, 0.1), 4)
            press_rolling_mean = round(NORMAL["reactor_pressure"] + np.random.normal(0, 3.0), 4)
            temp_rolling_std = round(abs(0.3 + np.random.normal(0, 0.05)), 4)
            press_rolling_std = round(abs(12.0 + np.random.normal(0, 1.0)), 4)

            # Anomaly flags — z-score > 3
            temp_z = abs(temp_val - temp_rolling_mean) / max(temp_rolling_std, 0.01)
            press_z = abs(press_val - press_rolling_mean) / max(press_rolling_std, 0.01)
            temp_anomaly = 1 if temp_z > 3 else 0
            press_anomaly = 1 if press_z > 3 else 0
            combined = 1 if (temp_anomaly or press_anomaly) else 0

            all_rows.append({
                "simulation_run": sim_run,
                "sample_num": sample,
                "fault_label": fault_label,
                "reactor_temp": round(temp_val, 4),
                "reactor_pressure": round(press_val, 4),
                "reactor_temp_rolling_mean": temp_rolling_mean,
                "reactor_pressure_rolling_mean": press_rolling_mean,
                "temp_anomaly_flag": temp_anomaly,
                "pressure_anomaly_flag": press_anomaly,
                "combined_anomaly_flag": combined,
            })

    return pd.DataFrame(all_rows)


def main():
    print("=" * 60)
    print("TEP Demo Data Generator")
    print("=" * 60)

    # Generate fault summary
    print("\n[1/2] Generating fault_summary_demo.csv ...")
    df_summary = generate_fault_summary()
    summary_path = os.path.join(DEMO_DIR, "fault_summary_demo.csv")
    df_summary.to_csv(summary_path, index=False)
    print(f"  ✅ Saved {len(df_summary)} rows → {summary_path}")
    print(f"  📊 File size: {os.path.getsize(summary_path):,} bytes")

    # Generate sensor features
    print("\n[2/2] Generating sensor_features_demo.csv ...")
    df_features = generate_sensor_features(num_sim_runs=10, samples_per_run=210)
    features_path = os.path.join(DEMO_DIR, "sensor_features_demo.csv")
    df_features.to_csv(features_path, index=False)
    print(f"  ✅ Saved {len(df_features)} rows → {features_path}")
    print(f"  📊 File size: {os.path.getsize(features_path):,} bytes")

    # Summary
    print("\n" + "=" * 60)
    print("Demo data ready for Streamlit Cloud deployment!")
    print(f"  📁 {DEMO_DIR}")
    print(f"  📄 fault_summary_demo.csv   ({len(df_summary)} rows)")
    print(f"  📄 sensor_features_demo.csv ({len(df_features)} rows)")
    print("=" * 60)


if __name__ == "__main__":
    main()
