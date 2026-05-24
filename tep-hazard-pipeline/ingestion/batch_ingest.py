"""
Batch Ingestion Script for TEP Chemical Hazard Detection Pipeline.

Reads TEP CSV files from DATA_DIR and loads them into PostgreSQL raw schema.
- TEP_Faulty_Training.csv     → raw.tep_faulty
- TEP_FaultFree_Training.csv  → raw.tep_fault_free

Uses PostgreSQL COPY protocol for fast bulk loading (~10x faster than to_sql).
Each record is enriched with: ingested_at, source_file, is_faulty.
"""

import os
import sys
import io
import logging
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("batch_ingest")

# ---------------------------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------------------------
ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
load_dotenv(dotenv_path=ENV_PATH)

DATA_DIR = os.getenv("DATA_DIR", r"C:\Users\abc\Desktop\MAJOR")
DB_URL = os.getenv("DB_URL", "postgresql://tep_user:tep_pass@localhost/tep_warehouse")

# Files to ingest — (filename, target table, is_faulty flag)
INGEST_MANIFEST = [
    ("TEP_Faulty_Training.csv", "tep_faulty", 1),
    ("TEP_FaultFree_Training.csv", "tep_fault_free", 0),
]

CHUNK_SIZE = 100000  # rows per chunk for reading large CSVs


def get_engine():
    """Create and return a SQLAlchemy engine."""
    try:
        engine = create_engine(DB_URL)
        logger.info("SQLAlchemy engine created for: %s", DB_URL.split("@")[-1])
        return engine
    except Exception as exc:
        logger.error("Failed to create database engine: %s", exc)
        raise


def copy_dataframe_to_db(engine, df, table_name, schema="raw"):
    """
    Fast bulk insert using PostgreSQL COPY protocol via psycopg2.

    Writes the DataFrame to a StringIO buffer as TSV, then uses
    copy_expert to stream it directly into the table.
    """
    # Write df to in-memory CSV buffer
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, sep="\t", na_rep="\\N")
    buffer.seek(0)

    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        copy_sql = f'COPY {schema}.{table_name} FROM STDIN WITH (FORMAT csv, DELIMITER E\'\\t\', NULL \'\\N\')'
        cursor.copy_expert(copy_sql, buffer)
        raw_conn.commit()
    except Exception as exc:
        raw_conn.rollback()
        raise exc
    finally:
        cursor.close()
        raw_conn.close()


def create_table_from_df(engine, df, table_name, schema="raw"):
    """Create the target table using a small sample (lets pandas infer types)."""
    sample = df.head(0)  # empty df with correct dtypes
    sample.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists="replace",
        index=False,
    )
    logger.info("Table %s.%s created.", schema, table_name)


def ingest_file(engine, filepath, table_name, is_faulty):
    """
    Read a CSV file in chunks and bulk-load into PostgreSQL using COPY.
    """
    logger.info("Reading file: %s", filepath)
    start_time = datetime.utcnow()

    try:
        # Read first chunk to create table structure
        first_chunk = True
        total_rows = 0

        for chunk_num, df in enumerate(
            pd.read_csv(filepath, chunksize=CHUNK_SIZE), start=1
        ):
            # Enrich the dataframe
            df["ingested_at"] = datetime.utcnow().isoformat()
            df["source_file"] = os.path.basename(filepath)
            df["is_faulty"] = is_faulty

            if first_chunk:
                # Create table from first chunk's structure
                create_table_from_df(engine, df, table_name, schema="raw")
                first_chunk = False

            # Bulk load via COPY
            copy_dataframe_to_db(engine, df, table_name, schema="raw")
            total_rows += len(df)
            logger.info(
                "  Chunk %d: loaded %d rows (total: %d)",
                chunk_num, len(df), total_rows,
            )

        elapsed = (datetime.utcnow() - start_time).total_seconds()
        logger.info(
            "✅ Loaded %d rows into raw.%s in %.1f seconds",
            total_rows, table_name, elapsed,
        )
        return total_rows

    except FileNotFoundError:
        logger.error("File not found: %s", filepath)
        raise
    except Exception as exc:
        logger.error("Error ingesting %s: %s", filepath, exc)
        raise


def main():
    """Run the batch ingestion for all files in the manifest."""
    logger.info("=" * 60)
    logger.info("TEP Batch Ingestion — START")
    logger.info("=" * 60)
    logger.info("Data directory : %s", DATA_DIR)

    engine = get_engine()
    total_rows = 0

    try:
        for filename, table_name, is_faulty in INGEST_MANIFEST:
            filepath = os.path.join(DATA_DIR, filename)
            rows = ingest_file(engine, filepath, table_name, is_faulty)
            total_rows += rows

        logger.info("-" * 60)
        logger.info("🏁 Ingestion COMPLETE — %d total rows loaded.", total_rows)
    except Exception as exc:
        logger.critical("Ingestion FAILED: %s", exc)
        sys.exit(1)
    finally:
        engine.dispose()
        logger.info("Database engine disposed.")


if __name__ == "__main__":
    main()
