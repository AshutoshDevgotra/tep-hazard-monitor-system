"""
Kafka Producer for TEP Chemical Hazard Detection Pipeline.

Reads TEP_Faulty_Training.csv row-by-row and publishes each record
as a JSON message to the 'tep-sensor-stream' Kafka topic.
"""

import os
import sys
import json
import time
import logging
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("kafka_producer")

# ---------------------------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------------------------
ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
load_dotenv(dotenv_path=ENV_PATH)

DATA_DIR = os.getenv("DATA_DIR", r"C:\Users\abc\Desktop\MAJOR")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "tep-sensor-stream"
SLEEP_INTERVAL = 0.1  # seconds between messages
LOG_EVERY_N = 100  # log progress every N rows


def create_producer():
    """Instantiate and return a KafkaProducer."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
        )
        logger.info("KafkaProducer connected to %s", KAFKA_BOOTSTRAP)
        return producer
    except Exception as exc:
        logger.error("Failed to create KafkaProducer: %s", exc)
        raise


def publish_rows(producer, df):
    """Iterate over dataframe rows and publish each as a JSON message."""
    total = len(df)
    logger.info("Publishing %d rows to topic '%s' ...", total, TOPIC)

    for idx, row in df.iterrows():
        message = row.to_dict()
        # Enrich with metadata
        message["timestamp"] = datetime.utcnow().isoformat()
        message["is_faulty"] = 1

        try:
            producer.send(TOPIC, value=message)
        except Exception as exc:
            logger.error("Failed to send row %d: %s", idx, exc)
            continue

        if (idx + 1) % LOG_EVERY_N == 0:
            logger.info("Sent %d / %d rows (%.1f%%)", idx + 1, total, (idx + 1) / total * 100)

        time.sleep(SLEEP_INTERVAL)

    producer.flush()
    logger.info("✅ All %d rows published to '%s'.", total, TOPIC)


def main():
    """Entry point — load CSV and stream to Kafka."""
    logger.info("=" * 60)
    logger.info("TEP Kafka Producer — START")
    logger.info("=" * 60)

    filepath = os.path.join(DATA_DIR, "TEP_Faulty_Training.csv")
    logger.info("Source file: %s", filepath)

    try:
        df = pd.read_csv(filepath)
        logger.info("Loaded %d rows from CSV.", len(df))
    except FileNotFoundError:
        logger.error("File not found: %s", filepath)
        sys.exit(1)
    except Exception as exc:
        logger.error("Error reading CSV: %s", exc)
        sys.exit(1)

    producer = None
    try:
        producer = create_producer()
        publish_rows(producer, df)
        logger.info("🏁 Kafka Producer finished.")
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as exc:
        logger.critical("Producer FAILED: %s", exc)
        sys.exit(1)
    finally:
        if producer:
            producer.close()
            logger.info("KafkaProducer closed.")


if __name__ == "__main__":
    main()
