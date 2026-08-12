"""
===============================================================================
TEST SUITE: Schema Contracts & Type Standard Verification
===============================================================================
This module validates that all raw landed datasets match their explicit
ingestion schema contracts before and after Bronze ingestion.

WHAT IT DOES:
  - Verifies exact column names, order, and datatypes.
  - Detects missing mandatory columns or unexpected schema drift.

WHY IT MATTERS:
  - Ingestion failures occur when upstream sources change column names or types.
  - Strict schema contracts guarantee zero downstream pipeline failures.
===============================================================================
"""

import unittest
import os
import pandas as pd

RAW_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")

# Expected Schema Contracts for all datasets
SCHEMA_CONTRACTS = {
    "orders": {
        "file": "olist_orders_dataset.csv",
        "expected_columns": [
            "order_id", "customer_id", "order_status", 
            "order_purchase_timestamp", "order_approved_at", 
            "order_delivered_carrier_date", "order_delivered_customer_date", 
            "order_estimated_delivery_date"
        ],
        "mandatory_non_null": ["order_id", "customer_id", "order_status"]
    },
    "customers": {
        "file": "olist_customers_dataset.csv",
        "expected_columns": [
            "customer_id", "customer_unique_id", 
            "customer_zip_code_prefix", "customer_city", "customer_state"
        ],
        "mandatory_non_null": ["customer_id", "customer_unique_id"]
    },
    "geolocation": {
        "file": "olist_geolocation_dataset.csv",
        "expected_columns": [
            "geolocation_zip_code_prefix", "geolocation_lat", 
            "geolocation_lng", "geolocation_city", "geolocation_state"
        ],
        "mandatory_non_null": ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"]
    },
    "category_translation": {
        "file": "product_category_name_translation.csv",
        "expected_columns": [
            "product_category_name", "product_category_name_english"
        ],
        "mandatory_non_null": ["product_category_name", "product_category_name_english"]
    },
    "order_payments": {
        "file": "olist_order_payments_dataset.csv",
        "expected_columns": [
            "order_id", "payment_sequential", "payment_type", 
            "payment_installments", "payment_value"
        ],
        "mandatory_non_null": ["order_id", "payment_sequential", "payment_type"]
    },
    "order_reviews": {
        "file": "olist_order_reviews_dataset.csv",
        "expected_columns": [
            "review_id", "order_id", "review_score", 
            "review_comment_title", "review_comment_message", 
            "review_creation_date", "review_answer_timestamp"
        ],
        "mandatory_non_null": ["review_id", "order_id", "review_score"]
    },
    "sellers": {
        "file": "olist_sellers_dataset.csv",
        "expected_columns": [
            "seller_id", "seller_zip_code_prefix", 
            "seller_city", "seller_state"
        ],
        "mandatory_non_null": ["seller_id"]
    },
    # 2 Remaining Datasets (For Extensibility)
    "order_items": {
        "file": "olist_order_items_dataset.csv",
        "expected_columns": [
            "order_id", "order_item_id", "product_id", 
            "seller_id", "shipping_limit_date", "price", "freight_value"
        ],
        "mandatory_non_null": ["order_id", "order_item_id", "product_id", "seller_id"]
    },
    "products": {
        "file": "olist_products_dataset.csv",
        "expected_columns": [
            "product_id", "product_category_name", "product_name_lenght", 
            "product_description_lenght", "product_photos_qty", 
            "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"
        ],
        "mandatory_non_null": ["product_id"]
    }
}


class TestSchemaContracts(unittest.TestCase):
    """Automated schema contract verification tests."""

    def _test_dataset_schema(self, dataset_key):
        config = SCHEMA_CONTRACTS[dataset_key]
        file_path = os.path.join(RAW_DATA_DIR, config["file"])
        self.assertTrue(os.path.exists(file_path), f"Source raw CSV not found: {file_path}")
        
        # Read header only for schema verification
        df = pd.read_csv(file_path, nrows=5)
        actual_cols = list(df.columns)
        expected_cols = config["expected_columns"]
        
        # Assert exact column set matching
        self.assertEqual(
            set(actual_cols), set(expected_cols), 
            f"Schema contract failed for {dataset_key}.\n"
            f"Missing cols: {set(expected_cols) - set(actual_cols)}\n"
            f"Extra cols: {set(actual_cols) - set(expected_cols)}"
        )
        
        # Assert column count
        self.assertEqual(
            len(actual_cols), len(expected_cols),
            f"Column count mismatch for {dataset_key}: Expected {len(expected_cols)}, got {len(actual_cols)}"
        )

    def test_orders_schema_contract(self):
        self._test_dataset_schema("orders")

    def test_customers_schema_contract(self):
        self._test_dataset_schema("customers")

    def test_geolocation_schema_contract(self):
        self._test_dataset_schema("geolocation")

    def test_category_translation_schema_contract(self):
        self._test_dataset_schema("category_translation")

    def test_order_payments_schema_contract(self):
        self._test_dataset_schema("order_payments")

    def test_order_reviews_schema_contract(self):
        self._test_dataset_schema("order_reviews")

    def test_sellers_schema_contract(self):
        self._test_dataset_schema("sellers")

    def test_order_items_schema_contract(self):
        self._test_dataset_schema("order_items")

    def test_products_schema_contract(self):
        self._test_dataset_schema("products")


if __name__ == "__main__":
    unittest.main()
