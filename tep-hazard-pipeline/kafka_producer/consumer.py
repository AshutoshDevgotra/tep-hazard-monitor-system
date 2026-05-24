"""
Kafka Consumer for TEP Chemical Hazard Detection Pipeline.

Subscribes to 'tep-sensor-stream', micro-batches 50 records at a time,
and writes each batch to the raw.tep_stream table in PostgreSQL.
"""

import os
import sys
import json
import logging

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaConsumer
from sqlalchemy import create_engine

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("kafka_consumer")

# ---------------------------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------------------------
ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
load_dotenv(dotenv_path=ENV_PATH)

DB_URL = os.getenv("DB_URL", "postgresql://tep_user:tep_pass@localhost/tep_warehouse")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "tep-sensor-stream"
BATCH_SIZE = 50


def create_consumer():
    """Instantiate and return a KafkaConsumer."""
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="tep-consumer-group",
            consumer_timeout_ms=-1,  # run indefinitely
        )
        logger.info("KafkaConsumer subscribed to '%s' at %s", TOPIC, KAFKA_BOOTSTRAP)
        return consumer
    except Exception as exc:
        logger.error("Failed to create KafkaConsumer: %s", exc)
        raise


def get_engine():
    """Create and return a SQLAlchemy engine."""
    try:
        engine = create_engine(DB_URL)
        logger.info("SQLAlchemy engine created.")
        return engine
    except Exception as exc:
        logger.error("Failed to create database engine: %s", exc)
        raise


def write_batch(engine, batch, batch_num):
    """Write a list of dicts to raw.tep_stream in PostgreSQL."""
    try:
        df = pd.DataFrame(batch)
        df.to_sql(
            name="tep_stream",
            con=engine,
            schema="raw",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=BATCH_SIZE,
        )
        logger.info(
            "✅ Batch #%d — wrote %d rows to raw.tep_stream",
            batch_num,
            len(df),
        )
    except Exception as exc:
        logger.error("Failed to write batch #%d: %s", batch_num, exc)
        raise


def main():
    """Entry point — consume from Kafka and write micro-batches to PostgreSQL."""
    logger.info("=" * 60)
    logger.info("TEP Kafka Consumer — START")
    logger.info("=" * 60)

    consumer = None
    engine = None

    try:
        consumer = create_consumer()
        engine = get_engine()

        batch = []
        batch_num = 0
        total_rows = 0

        for message in consumer:
            batch.append(message.value)

            if len(batch) >= BATCH_SIZE:
                batch_num += 1
                write_batch(engine, batch, batch_num)
                total_rows += len(batch)
                batch = []

        # Flush remaining records
        if batch:
            batch_num += 1
            write_batch(engine, batch, batch_num)
            total_rows += len(batch)

        logger.info("🏁 Consumer finished — %d total rows written.", total_rows)

    except KeyboardInterrupt:
        logger.warning("Consumer stopped by user (KeyboardInterrupt).")
        # Flush remaining batch on shutdown
        if batch and engine:
            batch_num += 1
            write_batch(engine, batch, batch_num)
            logger.info("Flushed remaining %d rows before exit.", len(batch))
    except Exception as exc:
        logger.critical("Consumer FAILED: %s", exc)
        sys.exit(1)
    finally:
        if consumer:
            consumer.close()
            logger.info("KafkaConsumer closed.")
        if engine:
            engine.dispose()
            logger.info("Database engine disposed.")


if __name__ == "__main__":
    main()
