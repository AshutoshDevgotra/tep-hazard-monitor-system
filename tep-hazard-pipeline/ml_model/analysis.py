"""
ML Analysis Script for TEP Chemical Hazard Detection Pipeline.

Loads the trained fault classifier and evaluates it on the unseen
TEP_Faulty_Testing.csv dataset. Reports per-fault-type accuracy
and identifies the hardest-to-detect fault types.

Outputs:
    - data/processed/test_results.csv
"""

import os
import sys
import json
import logging
import pickle

import numpy as np
import pandas as pd
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("ml_analysis")

# ---------------------------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------------------------
ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
load_dotenv(dotenv_path=ENV_PATH)

DATA_DIR = os.getenv("DATA_DIR", r"C:\Users\abc\Desktop\MAJOR")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
MODEL_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(MODEL_DIR)
MODEL_PATH = os.path.join(MODEL_DIR, "fault_classifier.pkl")
SCALER_PATH = os.path.join(MODEL_DIR, "scaler.pkl")
FEATURES_PATH = os.path.join(MODEL_DIR, "feature_cols.json")
OUTPUT_PATH = os.path.join(PROJECT_DIR, "data", "processed", "test_results.csv")

# Column mapping: raw CSV column → feature name used during training
COLUMN_MAP = {
    "xmeas_9": "reactor_temp",
    "xmeas_7": "reactor_pressure",
    "xmeas_8": "reactor_level",
    "xmeas_1": "feed_a_flow",
    "xmeas_2": "feed_d_flow",
    "xmeas_6": "reactor_feed_rate",
}


def load_artifacts():
    """Load the trained model, scaler, and feature column list."""
    try:
        with open(MODEL_PATH, "rb") as f:
            model = pickle.load(f)
        logger.info("Model loaded from %s", MODEL_PATH)

        with open(SCALER_PATH, "rb") as f:
            scaler = pickle.load(f)
        logger.info("Scaler loaded from %s", SCALER_PATH)

        with open(FEATURES_PATH, "r") as f:
            feature_cols = json.load(f)
        logger.info("Feature columns: %s", feature_cols)

        return model, scaler, feature_cols

    except FileNotFoundError as exc:
        logger.error("Artifact file not found: %s", exc)
        raise
    except Exception as exc:
        logger.error("Failed to load artifacts: %s", exc)
        raise


def load_test_data():
    """Load TEP_Faulty_Testing.csv and engineer required features."""
    filepath = os.path.join(DATA_DIR, "TEP_Faulty_Testing.csv")
    logger.info("Loading test data from %s", filepath)

    try:
        df = pd.read_csv(filepath)
        logger.info("Loaded %d rows × %d columns", len(df), len(df.columns))
    except FileNotFoundError:
        logger.error("Test file not found: %s", filepath)
        raise
    except Exception as exc:
        logger.error("Error reading test CSV: %s", exc)
        raise

    return df


def engineer_features(df):
    """
    Rename columns and compute rolling features to match the training pipeline.
    """
    logger.info("Engineering features for test data...")

    # Rename raw columns
    df = df.rename(columns=COLUMN_MAP)

    # Compute rolling stats per simulation run
    df = df.sort_values(["simulationRun", "sample"])

    df["reactor_temp_rolling_mean"] = (
        df.groupby("simulationRun")["reactor_temp"]
        .transform(lambda x: x.rolling(window=10, min_periods=1).mean())
    )
    df["reactor_temp_rolling_std"] = (
        df.groupby("simulationRun")["reactor_temp"]
        .transform(lambda x: x.rolling(window=10, min_periods=1).std().fillna(0))
    )
    df["reactor_pressure_rolling_mean"] = (
        df.groupby("simulationRun")["reactor_pressure"]
        .transform(lambda x: x.rolling(window=10, min_periods=1).mean())
    )
    df["reactor_pressure_rolling_std"] = (
        df.groupby("simulationRun")["reactor_pressure"]
        .transform(lambda x: x.rolling(window=10, min_periods=1).std().fillna(0))
    )

    # Anomaly flags
    df["temp_anomaly_flag"] = np.where(
        (df["reactor_temp_rolling_std"] > 0)
        & (
            np.abs(df["reactor_temp"] - df["reactor_temp_rolling_mean"])
            / df["reactor_temp_rolling_std"]
            > 3
        ),
        1,
        0,
    )
    df["pressure_anomaly_flag"] = np.where(
        (df["reactor_pressure_rolling_std"] > 0)
        & (
            np.abs(df["reactor_pressure"] - df["reactor_pressure_rolling_mean"])
            / df["reactor_pressure_rolling_std"]
            > 3
        ),
        1,
        0,
    )

    logger.info("Feature engineering complete.")
    return df


def run_predictions(model, scaler, feature_cols, df):
    """Run predictions and compute per-fault-type accuracy."""
    logger.info("Running predictions on test data...")

    # Prepare feature matrix
    df_model = df.dropna(subset=feature_cols)
    X_test = df_model[feature_cols].values
    y_true = df_model["faultNumber"].values.astype(int)

    # Scale and predict
    X_test_scaled = scaler.transform(X_test)
    y_pred = model.predict(X_test_scaled)

    # Per-fault-type accuracy
    df_results = pd.DataFrame({
        "fault_label": y_true,
        "predicted": y_pred,
        "correct": (y_true == y_pred).astype(int),
    })

    per_fault = (
        df_results.groupby("fault_label")
        .agg(
            total_samples=("correct", "count"),
            correct_predictions=("correct", "sum"),
        )
        .reset_index()
    )
    per_fault["accuracy_pct"] = round(
        per_fault["correct_predictions"] / per_fault["total_samples"] * 100, 2
    )
    per_fault = per_fault.sort_values("accuracy_pct", ascending=True)

    logger.info("Per-Fault-Type Accuracy:")
    for _, row in per_fault.iterrows():
        logger.info(
            "  Fault %2d: %6.2f%% accuracy (%d / %d)",
            int(row["fault_label"]),
            row["accuracy_pct"],
            int(row["correct_predictions"]),
            int(row["total_samples"]),
        )

    # Hardest to detect
    hardest = per_fault.head(5)
    logger.info("\nHardest-to-detect fault types:")
    for _, row in hardest.iterrows():
        logger.info(
            "  Fault %2d → %.2f%% accuracy",
            int(row["fault_label"]),
            row["accuracy_pct"],
        )

    return df_results, per_fault


def save_results(df_results):
    """Save full prediction results to CSV."""
    try:
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        df_results.to_csv(OUTPUT_PATH, index=False)
        logger.info("Results saved → %s", OUTPUT_PATH)
    except Exception as exc:
        logger.error("Failed to save results: %s", exc)
        raise


def main():
    """Full analysis pipeline on unseen test data."""
    logger.info("=" * 60)
    logger.info("TEP Fault Classifier — Test Analysis")
    logger.info("=" * 60)

    try:
        # 1. Load trained artifacts
        model, scaler, feature_cols = load_artifacts()

        # 2. Load test data
        df = load_test_data()

        # 3. Engineer features
        df = engineer_features(df)

        # 4. Run predictions
        df_results, per_fault = run_predictions(model, scaler, feature_cols, df)

        # 5. Save results
        save_results(df_results)

        logger.info("🏁 Test analysis COMPLETE.")

    except Exception as exc:
        logger.critical("Analysis FAILED: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
