"""
===============================================================================
MASTER VALIDATION SUITE RUNNER
===============================================================================
Executable test harness that orchestrates Data Quality checks, Schema Contracts,
and Row-Count Reconciliation across all pipeline datasets.

Usage:
    python tests/run_validation_suite.py
===============================================================================
"""

import unittest
import sys
import os
import time

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from tests.test_schema_contracts import TestSchemaContracts
from tests.test_data_quality import TestDataQuality
from tests.test_row_count_reconciliation import TestRowCountReconciliation


def print_banner(text):
    print("=" * 80)
    print(f"  {text}")
    print("=" * 80)


def run_row_count_reconciliation_summary():
    """Prints formatted Row-Count Reconciliation Table."""
    print_banner("ROW-COUNT RECONCILIATION AUDIT SUMMARY")
    
    # Instantiate test to read reconciliation data
    test_instance = TestRowCountReconciliation()
    test_instance.setUpClass()
    
    header = f"{'Dataset':<22} | {'Source Count':<12} | {'Bronze Count':<12} | {'Silver Count':<12} | {'Match %':<8} | {'Delta':<8}"
    print(header)
    print("-" * len(header))
    
    total_source = 0
    total_bronze = 0
    total_silver = 0
    
    for row in test_instance.reconciliation_data:
        print(
            f"{row['dataset']:<22} | "
            f"{row['source_count']:<12,} | "
            f"{row['bronze_count']:<12,} | "
            f"{row['silver_count']:<12,} | "
            f"{row['reconciliation_rate']:>6.1f}%  | "
            f"{row['dedupe_delta']:<8,}"
        )
        total_source += row['source_count']
        total_bronze += row['bronze_count']
        total_silver += row['silver_count']

    print("-" * len(header))
    total_row = (
        f"{'TOTAL AUDITED':<22} | "
        f"{total_source:<12,} | "
        f"{total_bronze:<12,} | "
        f"{total_silver:<12,} | "
        f"100.0%  | "
        f"{total_bronze - total_silver:<8,}"
    )
    print(total_row)
    print("=" * 80 + "\n")


def main():
    start_time = time.time()
    print_banner("CUSTOMER 360 & RFM ANALYTICS — PIPELINE VALIDATION ENGINE")
    
    # 1. Print Row-Count Reconciliation Audit Table
    run_row_count_reconciliation_summary()
    
    # 2. Build Test Suite
    print_banner("EXECUTING AUTOMATED DATA QUALITY & SCHEMA TEST SUITES")
    suite = unittest.TestSuite()
    
    loader = unittest.TestLoader()
    suite.addTests(loader.loadTestsFromTestCase(TestSchemaContracts))
    suite.addTests(loader.loadTestsFromTestCase(TestDataQuality))
    suite.addTests(loader.loadTestsFromTestCase(TestRowCountReconciliation))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    elapsed = time.time() - start_time
    
    print("\n" + "=" * 80)
    print("  FINAL VALIDATION SUMMARY RESULTS")
    print("=" * 80)
    print(f"  Total Tests Executed : {result.testsRun}")
    print(f"  Passed Tests         : {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"  Failed Tests         : {len(result.failures)}")
    print(f"  Errors               : {len(result.errors)}")
    print(f"  Execution Time       : {elapsed:.3f} seconds")
    
    if result.wasSuccessful():
        print("\n  [SUCCESS] All Data Quality gates, Schema Contracts, and Row Reconciliation tests PASSED!")
        print("=" * 80)
        sys.exit(0)
    else:
        print("\n  [FAILURE] Pipeline validation errors detected!")
        print("=" * 80)
        sys.exit(1)


if __name__ == "__main__":
    main()
