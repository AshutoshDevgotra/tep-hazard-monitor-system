"""
ML Training Script for TEP Chemical Hazard Detection Pipeline.

Trains a RandomForestClassifier on engineered sensor features from
staging.int_sensor_features to classify fault types (0-20).

Outputs:
    - ml_model/fault_classifier.pkl   (trained model)
    - ml_model/scaler.pkl             (fitted StandardScaler)
    - ml_model/feature_cols.json      (feature column list)
    - Classification report + confusion matrix + feature importances
"""

import os
import sys
import json
import logging
import pickle

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("ml_train")

# ---------------------------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------------------------
ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
load_dotenv(dotenv_path=ENV_PATH)

DB_URL = os.getenv("DB_URL", "postgresql://tep_user:tep_pass@localhost/tep_warehouse")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
FEATURE_COLS = [
    "reactor_temp",
    "reactor_pressure",
    "reactor_level",
    "feed_a_flow",
    "feed_d_flow",
    "reactor_feed_rate",
    "reactor_temp_rolling_mean",
    "reactor_temp_rolling_std",
    "reactor_pressure_rolling_mean",
    "reactor_pressure_rolling_std",
    "temp_anomaly_flag",
    "pressure_anomaly_flag",
]

TARGET_COL = "fault_label"

MODEL_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(MODEL_DIR, "fault_classifier.pkl")
SCALER_PATH = os.path.join(MODEL_DIR, "scaler.pkl")
FEATURES_PATH = os.path.join(MODEL_DIR, "feature_cols.json")


# Maximum rows per fault type to sample (keeps memory manageable)
SAMPLE_PER_FAULT = 25000


def load_data():
    """Load a stratified sample of engineered features from staging.int_sensor_features."""
    logger.info("Connecting to database...")
    try:
        engine = create_engine(DB_URL)
        # Stratified sample: take SAMPLE_PER_FAULT rows per fault_label
        query = (
            "SELECT * FROM ("
            "  SELECT *, ROW_NUMBER() OVER (PARTITION BY fault_label ORDER BY RANDOM()) AS rn"
            "  FROM staging.int_sensor_features"
            f") sub WHERE rn <= {SAMPLE_PER_FAULT}"
        )
        logger.info("Loading stratified sample (%d per fault type)...", SAMPLE_PER_FAULT)
        df = pd.read_sql(query, engine)
        engine.dispose()
        logger.info("Loaded %d rows × %d columns from staging.int_sensor_features", len(df), len(df.columns))
        return df
    except Exception as exc:
        logger.error("Failed to load data from database: %s", exc)
        raise


def prepare_data(df):
    """Select features and target, drop nulls, split into train/test."""
    logger.info("Preparing data...")

    # Select feature columns and target
    cols_needed = FEATURE_COLS + [TARGET_COL]
    df_model = df[cols_needed].copy()

    # Drop rows with any null features
    before = len(df_model)
    df_model = df_model.dropna(subset=FEATURE_COLS)
    after = len(df_model)
    if before != after:
        logger.info("Dropped %d rows with NULL features (%d → %d)", before - after, before, after)

    X = df_model[FEATURE_COLS].values
    y = df_model[TARGET_COL].values.astype(int)

    logger.info("Feature matrix shape: %s", X.shape)
    logger.info("Target distribution:\n%s", pd.Series(y).value_counts().sort_index().to_string())

    # Train/test split — 80/20, stratified
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=42
    )
    logger.info("Train set: %d samples | Test set: %d samples", len(X_train), len(X_test))

    return X_train, X_test, y_train, y_test


def train_model(X_train, y_train):
    """Scale features and train RandomForestClassifier."""
    logger.info("Scaling features with StandardScaler...")
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)

    logger.info("Training RandomForestClassifier (n_estimators=150, max_depth=15)...")
    model = RandomForestClassifier(
        n_estimators=150,
        max_depth=15,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train_scaled, y_train)
    logger.info("✅ Model training complete.")

    return model, scaler


def evaluate_model(model, scaler, X_test, y_test):
    """Evaluate the model and print metrics."""
    logger.info("Evaluating model on test set...")

    X_test_scaled = scaler.transform(X_test)
    y_pred = model.predict(X_test_scaled)

    # Classification report
    report = classification_report(y_test, y_pred, zero_division=0)
    logger.info("Classification Report:\n%s", report)

    # Confusion matrix
    cm = confusion_matrix(y_test, y_pred)
    logger.info("Confusion Matrix:\n%s", cm)

    # Feature importances — top 10
    importances = model.feature_importances_
    feat_imp = sorted(
        zip(FEATURE_COLS, importances),
        key=lambda x: x[1],
        reverse=True,
    )
    logger.info("Top 10 Feature Importances:")
    for rank, (feat, imp) in enumerate(feat_imp[:10], 1):
        logger.info("  %2d. %-35s  %.4f", rank, feat, imp)

    return y_pred


def save_artifacts(model, scaler):
    """Persist model, scaler, and feature list to disk."""
    try:
        with open(MODEL_PATH, "wb") as f:
            pickle.dump(model, f)
        logger.info("Model saved → %s", MODEL_PATH)

        with open(SCALER_PATH, "wb") as f:
            pickle.dump(scaler, f)
        logger.info("Scaler saved → %s", SCALER_PATH)

        with open(FEATURES_PATH, "w") as f:
            json.dump(FEATURE_COLS, f, indent=2)
        logger.info("Feature list saved → %s", FEATURES_PATH)

    except Exception as exc:
        logger.error("Failed to save artifacts: %s", exc)
        raise


def main():
    """Full ML training pipeline."""
    logger.info("=" * 60)
    logger.info("TEP Fault Classifier — Training Pipeline")
    logger.info("=" * 60)

    try:
        # 1. Load data
        df = load_data()

        # 2. Prepare train/test split
        X_train, X_test, y_train, y_test = prepare_data(df)

        # 3. Train model
        model, scaler = train_model(X_train, y_train)

        # 4. Evaluate
        evaluate_model(model, scaler, X_test, y_test)

        # 5. Save artifacts
        save_artifacts(model, scaler)

        logger.info("🏁 Training pipeline COMPLETE.")

    except Exception as exc:
        logger.critical("Training pipeline FAILED: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
