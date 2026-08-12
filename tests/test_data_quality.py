"""
===============================================================================
TEST SUITE: Data Quality (DQ) Validation Engine
===============================================================================
This module executes fail-fast Data Quality (DQ) assertions across all
landed datasets.

DQ CHECK CATEGORIES:
  1. Completeness / Null Checks (Primary keys must be 100% non-null)
  2. Primary Key Uniqueness Gates (No duplicates on canonical primary keys)
  3. Value Range & Domain Bounds (Valid payment values, review scores, state codes)
  4. Referential Integrity (Foreign key links across entity tables)

WHY IT MATTERS:
  - Ensures bad data is flagged before entering Gold aggregations or Snowflake.
===============================================================================
"""

import unittest
import os
import pandas as pd

RAW_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")


class TestDataQuality(unittest.TestCase):
    """Data Quality check suite."""

    @classmethod
    def setUpClass(cls):
        """Load datasets for performance across tests."""
        cls.orders_df = pd.read_csv(os.path.join(RAW_DATA_DIR, "olist_orders_dataset.csv"))
        cls.customers_df = pd.read_csv(os.path.join(RAW_DATA_DIR, "olist_customers_dataset.csv"))
        cls.geolocation_df = pd.read_csv(os.path.join(RAW_DATA_DIR, "olist_geolocation_dataset.csv"))
        cls.category_df = pd.read_csv(os.path.join(RAW_DATA_DIR, "product_category_name_translation.csv"))
        cls.payments_df = pd.read_csv(os.path.join(RAW_DATA_DIR, "olist_order_payments_dataset.csv"))
        cls.reviews_df = pd.read_csv(os.path.join(RAW_DATA_DIR, "olist_order_reviews_dataset.csv"))
        cls.sellers_df = pd.read_csv(os.path.join(RAW_DATA_DIR, "olist_sellers_dataset.csv"))

    # ================================================================
    # 1. NULL / COMPLETENESS CHECKS
    # ================================================================
    def test_orders_primary_keys_non_null(self):
        """Verify order_id and customer_id have 0% nulls."""
        null_order_ids = self.orders_df["order_id"].isnull().sum()
        null_customer_ids = self.orders_df["customer_id"].isnull().sum()
        self.assertEqual(null_order_ids, 0, f"Found {null_order_ids} null order_ids in orders dataset")
        self.assertEqual(null_customer_ids, 0, f"Found {null_customer_ids} null customer_ids in orders dataset")

    def test_customers_primary_keys_non_null(self):
        """Verify customer_id and customer_unique_id have 0% nulls."""
        null_cust_ids = self.customers_df["customer_id"].isnull().sum()
        null_unique_ids = self.customers_df["customer_unique_id"].isnull().sum()
        self.assertEqual(null_cust_ids, 0, f"Found {null_cust_ids} null customer_ids")
        self.assertEqual(null_unique_ids, 0, f"Found {null_unique_ids} null customer_unique_ids")

    def test_sellers_primary_keys_non_null(self):
        """Verify seller_id has 0% nulls."""
        null_sellers = self.sellers_df["seller_id"].isnull().sum()
        self.assertEqual(null_sellers, 0, f"Found {null_sellers} null seller_ids")

    def test_payments_order_id_non_null(self):
        """Verify payment order_id has 0% nulls."""
        null_payments = self.payments_df["order_id"].isnull().sum()
        self.assertEqual(null_payments, 0, f"Found {null_payments} null order_ids in payments")

    def test_reviews_primary_keys_non_null(self):
        """Verify review_id and order_id have 0% nulls."""
        null_reviews = self.reviews_df["review_id"].isnull().sum()
        null_orders = self.reviews_df["order_id"].isnull().sum()
        self.assertEqual(null_reviews, 0, f"Found {null_reviews} null review_ids")
        self.assertEqual(null_orders, 0, f"Found {null_orders} null order_ids in reviews")

    # ================================================================
    # 2. UNIQUENESS CHECKS
    # ================================================================
    def test_orders_primary_key_uniqueness(self):
        """Verify order_id is 100% unique in Orders dataset."""
        total_rows = len(self.orders_df)
        unique_orders = self.orders_df["order_id"].nunique()
        self.assertEqual(total_rows, unique_orders, f"Duplicate order_ids detected: {total_rows - unique_orders}")

    def test_customers_primary_key_uniqueness(self):
        """Verify customer_id (order grain key) is 100% unique in Customers dataset."""
        total_rows = len(self.customers_df)
        unique_customers = self.customers_df["customer_id"].nunique()
        self.assertEqual(total_rows, unique_customers, f"Duplicate customer_ids detected: {total_rows - unique_customers}")

    def test_sellers_primary_key_uniqueness(self):
        """Verify seller_id is 100% unique in Sellers dataset."""
        total_rows = len(self.sellers_df)
        unique_sellers = self.sellers_df["seller_id"].nunique()
        self.assertEqual(total_rows, unique_sellers, f"Duplicate seller_ids detected: {total_rows - unique_sellers}")

    def test_category_translation_uniqueness(self):
        """Verify product_category_name is unique in category translation dataset."""
        total_rows = len(self.category_df)
        unique_categories = self.category_df["product_category_name"].nunique()
        self.assertEqual(total_rows, unique_categories, f"Duplicate category names detected: {total_rows - unique_categories}")

    # ================================================================
    # 3. VALUE RANGE & DOMAIN BOUNDARY CHECKS
    # ================================================================
    def test_payment_values_non_negative(self):
        """Verify payment_value is non-negative (>= 0)."""
        invalid_payments = (self.payments_df["payment_value"] < 0).sum()
        self.assertEqual(invalid_payments, 0, f"Found {invalid_payments} payments with negative value")

    def test_payment_installments_positive(self):
        """Verify payment_installments is at least 0 (most >= 1)."""
        invalid_installments = (self.payments_df["payment_installments"] < 0).sum()
        self.assertEqual(invalid_installments, 0, f"Found {invalid_installments} negative payment installments")

    def test_review_score_range(self):
        """Verify review_score is strictly between 1 and 5."""
        out_of_bounds = (~self.reviews_df["review_score"].between(1, 5)).sum()
        self.assertEqual(out_of_bounds, 0, f"Found {out_of_bounds} review scores outside 1-5 range")

    def test_customer_state_format(self):
        """Verify customer_state contains valid 2-letter state codes."""
        invalid_states = self.customers_df["customer_state"].str.len() != 2
        invalid_count = invalid_states.sum()
        self.assertEqual(invalid_count, 0, f"Found {invalid_count} customer state codes not equal to 2 letters")

    def test_seller_state_format(self):
        """Verify seller_state contains valid 2-letter state codes."""
        invalid_states = self.sellers_df["seller_state"].str.len() != 2
        invalid_count = invalid_states.sum()
        self.assertEqual(invalid_count, 0, f"Found {invalid_count} seller state codes not equal to 2 letters")

    # ================================================================
    # 4. REFERENTIAL INTEGRITY CHECKS
    # ================================================================
    def test_reviews_order_id_referential_integrity(self):
        """Verify review order_ids match existing order_ids in Orders dataset."""
        order_set = set(self.orders_df["order_id"])
        orphaned_reviews = (~self.reviews_df["order_id"].isin(order_set)).sum()
        self.assertEqual(orphaned_reviews, 0, f"Found {orphaned_reviews} review records with invalid order_ids")

    def test_payments_order_id_referential_integrity(self):
        """Verify payment order_ids match existing order_ids in Orders dataset."""
        order_set = set(self.orders_df["order_id"])
        orphaned_payments = (~self.payments_df["order_id"].isin(order_set)).sum()
        self.assertEqual(orphaned_payments, 0, f"Found {orphaned_payments} payment records with invalid order_ids")


if __name__ == "__main__":
    unittest.main()
