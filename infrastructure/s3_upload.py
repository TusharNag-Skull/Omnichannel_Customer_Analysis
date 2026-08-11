# =============================================================================
# S3 Upload Script
# =============================================================================
# Uploads all 9 Olist CSV files from the local data directory to AWS S3.
#
# WHAT THIS SCRIPT DOES:
# 1. Connects to your AWS S3 bucket using credentials from your AWS CLI config
# 2. Uploads each CSV file into a subfolder named after the dataset
#    (e.g., s3://bucket/raw/customers/olist_customers_dataset.csv)
# 3. Prints progress and row counts for verification
#
# WHY WE ORGANIZE BY SUBFOLDER:
# Putting each dataset in its own folder inside S3 makes it easy to:
# - Re-upload just one dataset without touching others
# - Set different access policies per dataset if needed
# - List and debug files for a specific entity
# This is standard practice at companies like Netflix and Uber.
#
# PREREQUISITES:
# - AWS CLI configured (run 'aws configure' first)
# - boto3 installed (pip install boto3)
# - S3 bucket already created
#
# USAGE:
#   python infrastructure/s3_upload.py
# =============================================================================

import os
import boto3
from botocore.exceptions import ClientError


# ---- Configuration ----
# Update these to match your actual setup

S3_BUCKET_NAME = "retail-omnichannel-analytics"
S3_RAW_PREFIX = "raw"
LOCAL_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")

# Maps each dataset name to its CSV filename.
# This is the same mapping as in pipeline_config.py.
# We duplicate it here instead of importing because this script runs locally
# (not on Databricks), and the import path would be different.

DATASETS = {
    "customers":            "olist_customers_dataset.csv",
    "orders":               "olist_orders_dataset.csv",
    "order_items":          "olist_order_items_dataset.csv",
    "order_payments":       "olist_order_payments_dataset.csv",
    "order_reviews":        "olist_order_reviews_dataset.csv",
    "products":             "olist_products_dataset.csv",
    "sellers":              "olist_sellers_dataset.csv",
    "geolocation":          "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}


def get_row_count(file_path):
    """Count rows in a CSV file (excluding header).

    We count rows before upload so we can verify later that Bronze ingestion
    loaded the same number of rows. This is our first data quality checkpoint.
    """
    with open(file_path, "r", encoding="utf-8") as f:
        # Subtract 1 for the header row
        row_count = sum(1 for _ in f) - 1
    return row_count


def upload_file_to_s3(s3_client, local_path, bucket, s3_key):
    """Upload a single file to S3.

    Returns True if successful, False otherwise.
    We wrap this in a try-except because network uploads can fail for many
    reasons (timeout, permissions, bucket doesn't exist). In production,
    we would add retry logic here.
    """
    try:
        s3_client.upload_file(local_path, bucket, s3_key)
        return True
    except ClientError as error:
        print(f"  ERROR uploading {local_path}: {error}")
        return False


def main():
    """Upload all CSV datasets to S3."""

    print("=" * 60)
    print("S3 UPLOAD — Retail Omnichannel Customer Analytics")
    print("=" * 60)
    print(f"Bucket:    s3://{S3_BUCKET_NAME}")
    print(f"Prefix:    {S3_RAW_PREFIX}/")
    print(f"Local Dir: {LOCAL_DATA_DIR}")
    print("-" * 60)

    # Create the S3 client.
    # boto3 automatically reads credentials from ~/.aws/credentials
    # which is set up by running 'aws configure'.
    s3_client = boto3.client("s3")

    success_count = 0
    fail_count = 0

    for dataset_name, filename in DATASETS.items():
        local_path = os.path.join(LOCAL_DATA_DIR, filename)

        # Check if the file exists locally before attempting upload
        if not os.path.exists(local_path):
            print(f"  SKIP {filename} — file not found at {local_path}")
            fail_count += 1
            continue

        # Count rows for verification
        row_count = get_row_count(local_path)
        file_size_mb = os.path.getsize(local_path) / (1024 * 1024)

        # Build the S3 key (path inside the bucket)
        # Example: raw/customers/olist_customers_dataset.csv
        s3_key = f"{S3_RAW_PREFIX}/{dataset_name}/{filename}"

        print(f"  Uploading {dataset_name}...")
        print(f"    File:  {filename}")
        print(f"    Size:  {file_size_mb:.1f} MB")
        print(f"    Rows:  {row_count:,}")
        print(f"    To:    s3://{S3_BUCKET_NAME}/{s3_key}")

        uploaded = upload_file_to_s3(s3_client, local_path, S3_BUCKET_NAME, s3_key)

        if uploaded:
            print(f"    Status: SUCCESS")
            success_count += 1
        else:
            print(f"    Status: FAILED")
            fail_count += 1

        print()

    # Summary
    print("-" * 60)
    print(f"Upload Complete: {success_count} succeeded, {fail_count} failed")
    print("=" * 60)


if __name__ == "__main__":
    main()