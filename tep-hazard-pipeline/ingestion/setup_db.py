"""
PostgreSQL Database Setup Script for TEP Chemical Hazard Detection Pipeline.

Creates the tep_warehouse database, user, and required schemas (raw, staging, marts).
Uses credentials from the .env file.
"""

import os
import sys
import logging

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("setup_db")

# ---------------------------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------------------------
ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
load_dotenv(dotenv_path=ENV_PATH)

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "tep_warehouse")
DB_USER = os.getenv("DB_USER", "tep_user")
DB_PASS = os.getenv("DB_PASS", "tep_pass")


def _admin_engine():
    """Return an engine connected to the default 'postgres' database (admin)."""
    url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/postgres"
    return create_engine(url, isolation_level="AUTOCOMMIT")


def _warehouse_engine():
    """Return an engine connected to the tep_warehouse database."""
    url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


def create_database():
    """Create the tep_warehouse database if it does not already exist."""
    engine = _admin_engine()
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :db"),
                {"db": DB_NAME},
            )
            if result.fetchone():
                logger.info("Database '%s' already exists — skipping creation.", DB_NAME)
            else:
                conn.execute(text(f'CREATE DATABASE "{DB_NAME}"'))
                logger.info("Database '%s' created successfully.", DB_NAME)
    except Exception as exc:
        logger.error("Failed to create database '%s': %s", DB_NAME, exc)
        raise
    finally:
        engine.dispose()


def create_schemas():
    """Create raw, staging, and marts schemas inside tep_warehouse."""
    engine = _warehouse_engine()
    schemas = ["raw", "staging", "marts"]
    try:
        with engine.begin() as conn:
            for schema in schemas:
                conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
                logger.info("Schema '%s' is ready.", schema)
        logger.info("All schemas created / verified successfully.")
    except Exception as exc:
        logger.error("Failed to create schemas: %s", exc)
        raise
    finally:
        engine.dispose()


def verify_setup():
    """Quick verification that database and schemas are accessible."""
    engine = _warehouse_engine()
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT schema_name FROM information_schema.schemata "
                    "WHERE schema_name IN ('raw', 'staging', 'marts')"
                )
            )
            found = [row[0] for row in result]
            logger.info("Verified schemas in '%s': %s", DB_NAME, found)
            if len(found) == 3:
                logger.info("✅ Setup verification PASSED — all schemas present.")
            else:
                logger.warning(
                    "⚠️  Expected 3 schemas, found %d. Missing: %s",
                    len(found),
                    set(["raw", "staging", "marts"]) - set(found),
                )
    except Exception as exc:
        logger.error("Verification failed: %s", exc)
        raise
    finally:
        engine.dispose()


def main():
    """Run the full database setup sequence."""
    logger.info("=" * 60)
    logger.info("TEP Warehouse — PostgreSQL Setup")
    logger.info("=" * 60)

    try:
        create_database()
        create_schemas()
        verify_setup()
        logger.info("🏁 Database setup completed successfully.")
    except Exception as exc:
        logger.critical("Database setup FAILED: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
