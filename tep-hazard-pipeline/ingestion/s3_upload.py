"""
AWS S3 Upload Script for TEP Chemical Hazard Detection Pipeline.

Uploads raw TEP CSV files to an S3 data lake bucket with progress logging.
    TEP_Faulty_Training.csv    → s3://bucket/raw/faulty/
    TEP_FaultFree_Training.csv → s3://bucket/raw/fault_free/
"""

import os
import sys
import logging

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("s3_upload")

# ---------------------------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------------------------
ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
load_dotenv(dotenv_path=ENV_PATH)

DATA_DIR = os.getenv("DATA_DIR", r"C:\Users\abc\Desktop\MAJOR")
AWS_BUCKET = os.getenv("AWS_BUCKET", "tep-hazard-datalake")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")

# Files to upload — (local filename, S3 prefix)
UPLOAD_MANIFEST = [
    ("TEP_Faulty_Training.csv", "raw/faulty/"),
    ("TEP_FaultFree_Training.csv", "raw/fault_free/"),
]


def get_s3_client():
    """Create and return a boto3 S3 client."""
    try:
        client = boto3.client("s3", region_name=AWS_REGION)
        logger.info("S3 client created for region '%s'.", AWS_REGION)
        return client
    except Exception as exc:
        logger.error("Failed to create S3 client: %s", exc)
        raise


def create_bucket(s3_client):
    """Create the S3 bucket if it does not already exist."""
    try:
        s3_client.head_bucket(Bucket=AWS_BUCKET)
        logger.info("Bucket '%s' already exists.", AWS_BUCKET)
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        if error_code in ("404", "NoSuchBucket"):
            try:
                if AWS_REGION == "us-east-1":
                    s3_client.create_bucket(Bucket=AWS_BUCKET)
                else:
                    s3_client.create_bucket(
                        Bucket=AWS_BUCKET,
                        CreateBucketConfiguration={
                            "LocationConstraint": AWS_REGION,
                        },
                    )
                logger.info("✅ Bucket '%s' created in region '%s'.", AWS_BUCKET, AWS_REGION)
            except ClientError as create_exc:
                logger.error("Failed to create bucket '%s': %s", AWS_BUCKET, create_exc)
                raise
        elif error_code == "403":
            logger.warning(
                "Bucket '%s' exists but access is denied. Proceeding with upload attempt.",
                AWS_BUCKET,
            )
        else:
            logger.error("Unexpected error checking bucket: %s", exc)
            raise


def format_size(size_bytes):
    """Format file size in human-readable form."""
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"


def upload_file(s3_client, local_path, s3_prefix):
    """Upload a single file to S3 with progress logging."""
    filename = os.path.basename(local_path)
    s3_key = s3_prefix + filename
    file_size = os.path.getsize(local_path)

    logger.info(
        "Uploading: %s (%s) → s3://%s/%s",
        filename,
        format_size(file_size),
        AWS_BUCKET,
        s3_key,
    )

    try:
        # Upload with callback for progress
        uploaded_bytes = [0]

        def progress_callback(bytes_transferred):
            uploaded_bytes[0] += bytes_transferred
            pct = (uploaded_bytes[0] / file_size) * 100
            if pct % 25 < 1 or uploaded_bytes[0] == file_size:
                logger.info(
                    "  Progress: %s / %s (%.1f%%)",
                    format_size(uploaded_bytes[0]),
                    format_size(file_size),
                    pct,
                )

        s3_client.upload_file(
            Filename=local_path,
            Bucket=AWS_BUCKET,
            Key=s3_key,
            Callback=progress_callback,
        )
        logger.info("✅ Upload SUCCESS: s3://%s/%s", AWS_BUCKET, s3_key)
        return True

    except ClientError as exc:
        logger.error("❌ Upload FAILED for %s: %s", filename, exc)
        return False
    except FileNotFoundError:
        logger.error("❌ File not found: %s", local_path)
        return False


def main():
    """Run the S3 upload for all files in the manifest."""
    logger.info("=" * 60)
    logger.info("TEP S3 Upload — START")
    logger.info("=" * 60)
    logger.info("Data directory : %s", DATA_DIR)
    logger.info("Target bucket  : s3://%s", AWS_BUCKET)
    logger.info("Region         : %s", AWS_REGION)

    try:
        s3_client = get_s3_client()
        create_bucket(s3_client)

        results = []
        for filename, s3_prefix in UPLOAD_MANIFEST:
            local_path = os.path.join(DATA_DIR, filename)
            success = upload_file(s3_client, local_path, s3_prefix)
            results.append((filename, success))

        # Summary
        logger.info("-" * 60)
        logger.info("Upload Summary:")
        for filename, success in results:
            status = "✅ SUCCESS" if success else "❌ FAILED"
            logger.info("  %s — %s", filename, status)

        failed = [f for f, s in results if not s]
        if failed:
            logger.warning("⚠️  %d file(s) failed to upload.", len(failed))
        else:
            logger.info("🏁 All files uploaded successfully.")

    except Exception as exc:
        logger.critical("S3 upload FAILED: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
