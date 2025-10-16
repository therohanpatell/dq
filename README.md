# PySpark BigQuery Metrics and Data Quality Pipeline - Complete Handover Documentation

**Version:** 1.0  
**Last Updated:** 2024  
**Author:** Development Team  
**Purpose:** Complete technical handover documentation for maintenance and enhancement

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Code Flow / Execution Order](#2-code-flow--execution-order)
3. [Modules and Functions](#3-modules-and-functions)
4. [Configuration and Environment](#4-configuration-and-environment)
5. [Logging & Error Handling](#5-logging--error-handling)
6. [Dependencies and Data Flow](#6-dependencies-and-data-flow)
7. [Execution Guide](#7-execution-guide)
8. [New Features](#8-new-features)
   - [8.1 Error Resilience](#81-error-resilience)
   - [8.2 Overwrite Functionality](#82-overwrite-functionality)
   - [8.3 Configuration Validator](#83-configuration-validator)
9. [Maintenance Notes](#9-maintenance-notes)
10. [File Dependency Diagram](#10-file-dependency-diagram)

---

## 1. Project Overview

### 1.1 Purpose

This is a **dual-mode PySpark pipeline** that processes metrics and performs data quality (DQ) validation checks against BigQuery data. The system:

- **Metrics Mode**: Executes SQL queries to calculate business metrics, writes results to BigQuery tables, and creates reconciliation records for audit tracking
- **DQ Mode**: Executes data quality validation checks, compares actual results against expected outputs, and writes validation results to BigQuery

### 1.2 Key Features

- Dual-mode operation (metrics vs. data quality)
- Dynamic SQL placeholder replacement ({currently}, {partition_info})
- Robust error handling with detailed reconciliation tracking
- Schema alignment with BigQuery tables
- Overwrite capability for existing metrics
- Comprehensive logging and error categorization
- Support for multiple comparison types in DQ validation

### 1.3 Module Responsibilities

| Module | Purpose |
|--------|---------|
| **main.py** | Entry point, CLI argument parsing, pipeline orchestration |
| **pipeline.py** | Core metrics pipeline logic, SQL execution, metrics processing |
| **dq_pipeline.py** | Data quality pipeline logic, check execution, validation |
| **bigquery.py** | All BigQuery operations (read, write, schema, queries) |
| **config.py** | Configuration constants, schemas, error categories |
| **utils.py** | Utility functions (date, numeric, SQL, validation, string) |
| **validation.py** | DQ configuration validation engine |
| **comparison.py** | DQ result comparison engine (4 comparison types) |
| **exceptions.py** | Custom exception classes |


### 1.4 System Input/Output

**Inputs:**
- JSON configuration files stored in Google Cloud Storage (GCS)
- Command-line arguments (mode, dates, table names, dependencies)
- BigQuery partition metadata tables
- Google Cloud credentials (implicit via environment)

**Outputs:**
- Metrics written to BigQuery target tables (metrics mode)
- Reconciliation records written to BigQuery recon table (metrics mode)
- DQ validation results written to BigQuery DQ results table (DQ mode)
- Console logs with detailed execution information

---

## 2. Code Flow / Execution Order

### 2.1 Entry Point

**File:** `main.py`  
**Entry Function:** `if __name__ == "__main__":`

The main script creates a `PipelineOrchestrator` instance and executes the `run()` method.

### 2.2 Execution Sequence - Metrics Mode

```
1. main.py::main()
   └─> PipelineOrchestrator.__init__()
   └─> parse_arguments()
   └─> validate_and_parse_dependencies()
   └─> log_pipeline_info()
   └─> execute_pipeline_steps()
       ├─> managed_spark_session() [context manager]
       ├─> create_bigquery_operations()
       ├─> MetricsPipeline.__init__()
       │
       ├─> STEP 0: validate_partition_info_table()
       │   └─> BigQueryOperations.validate_partition_info_table()
       │
       ├─> STEP 1: read_json_from_gcs()
       │   └─> validate_gcs_path()
       │   └─> Spark.read.json()
       │
       ├─> STEP 2: validate_json()
       │   └─> ValidationUtils.validate_json_record() [for each record]
       │
       ├─> STEP 3: process_metrics()
       │   ├─> check_dependencies_exist()
       │   ├─> Filter by dependencies
       │   ├─> Group by target_table
       │   └─> For each metric:
       │       ├─> execute_sql()
       │       │   ├─> replace_sql_placeholders()
       │       │   └─> BigQueryOperations.execute_sql_with_results()
       │       └─> Create Spark DataFrame per target_table
       │
       ├─> STEP 4: write_metrics_to_tables()
       │   └─> For each target_table:
       │       ├─> align_schema_with_bq()
       │       └─> write_to_bq_with_overwrite()
       │           ├─> check_existing_metrics()
       │           ├─> delete_metrics() [if exists]
       │           └─> write_dataframe_to_table()
       │
       └─> STEP 5: create_and_write_recon_records()
           ├─> create_recon_records_from_write_results()
           │   └─> build_recon_record() [for each metric]
           └─> write_recon_to_bq()
```


### 2.3 Execution Sequence - DQ Mode

```
1. main.py::main()
   └─> PipelineOrchestrator.__init__()
   └─> parse_arguments()
   └─> log_pipeline_info()
   └─> execute_dq_pipeline_steps()
       ├─> managed_spark_session() [context manager]
       ├─> create_bigquery_operations()
       ├─> DQPipeline.__init__()
       │
       ├─> STEP 1: read_and_validate_dq_config()
       │   ├─> _read_json_from_gcs()
       │   └─> ValidationEngine.validate_dq_json()
       │       └─> validate_dq_record() [for each check]
       │
       ├─> STEP 2: execute_dq_checks()
       │   ├─> Filter active checks
       │   └─> For each active check:
       │       └─> execute_single_check()
       │           ├─> replace_sql_placeholders()
       │           ├─> _execute_dq_sql()
       │           ├─> ComparisonEngine.compare()
       │           └─> build_dq_result_record()
       │
       └─> STEP 3: write_dq_results()
           ├─> Create Spark DataFrame
           └─> BigQueryOperations.write_dataframe_to_table()
```

### 2.4 Data Flow Between Modules

```
main.py
  ├─> config.py (imports schemas, constants)
  ├─> exceptions.py (imports custom exceptions)
  ├─> utils.py (imports DateUtils, managed_spark_session)
  ├─> pipeline.py (creates MetricsPipeline instance)
  ├─> dq_pipeline.py (creates DQPipeline instance)
  └─> bigquery.py (creates BigQueryOperations instance)

pipeline.py
  ├─> config.py (schemas, constants)
  ├─> exceptions.py (raises custom exceptions)
  ├─> utils.py (all utility classes)
  └─> bigquery.py (delegates all BQ operations)

dq_pipeline.py
  ├─> config.py (schemas, constants)
  ├─> exceptions.py (raises custom exceptions)
  ├─> utils.py (DateUtils, ResultSerializer, SQLUtils)
  ├─> validation.py (ValidationEngine)
  ├─> comparison.py (ComparisonEngine)
  └─> bigquery.py (delegates all BQ operations)

bigquery.py
  ├─> config.py (schemas, constants)
  ├─> exceptions.py (raises custom exceptions)
  └─> utils.py (StringUtils, NumericUtils)

validation.py
  └─> exceptions.py (raises ValidationError)

comparison.py
  └─> (no dependencies - pure logic)

utils.py
  ├─> config.py (constants)
  └─> exceptions.py (raises custom exceptions)
```

---

## 3. Modules and Functions

### 3.1 main.py

**Purpose:** Application entry point, CLI argument parsing, pipeline orchestration


#### Class: PipelineOrchestrator

**Methods:**

1. **`__init__(self)`**
   - Initializes orchestrator
   - Sets pipeline to None (initialized later with Spark session)

2. **`parse_arguments(self) -> argparse.Namespace`**
   - Parses command-line arguments
   - Validates mode-specific required arguments
   - Returns: Parsed arguments namespace
   - Arguments:
     - `--mode`: 'metrics' or 'dq' (default: 'metrics')
     - `--gcs_path`: GCS path to JSON config (required)
     - `--run_date`: Business date YYYY-MM-DD (required)
     - `--dependencies`: Comma-separated dependencies (metrics mode)
     - `--partition_info_table`: Partition metadata table (metrics mode)
     - `--env`: Environment name (metrics mode)
     - `--recon_table`: Reconciliation table (metrics mode)
     - `--dq_target_table`: DQ results table (DQ mode)
     - `--dq_partition_info_table`: Partition table for DQ (optional)

3. **`validate_and_parse_dependencies(self, dependencies_str: str) -> list`**
   - Splits comma-separated dependencies
   - Validates at least one dependency exists
   - Returns: List of dependency strings

4. **`log_pipeline_info(self, args: Namespace, dependencies: list = None) -> None`**
   - Logs pipeline configuration and behavior
   - Different output for metrics vs DQ mode

5. **`execute_pipeline_steps(self, args: Namespace, dependencies: list) -> None`**
   - Executes metrics pipeline with 5 steps
   - Handles errors and creates failure recon records
   - Uses managed Spark session context manager

6. **`execute_dq_pipeline_steps(self, args: Namespace) -> None`**
   - Executes DQ pipeline with 3 steps
   - Handles errors with detailed logging

7. **`write_metrics_to_tables(self, metrics_dfs: dict) -> tuple`**
   - Writes metrics DataFrames to BigQuery
   - Returns: (successful_writes, failed_write_metrics)

8. **`create_and_write_recon_records(self, ...) -> None`**
   - Creates reconciliation records from write results
   - Writes to BigQuery recon table

9. **`log_pipeline_statistics(self, ...) -> None`**
   - Logs comprehensive pipeline execution statistics

10. **`log_dq_pipeline_statistics(self, dq_config: list, dq_results: list) -> None`**
    - Logs DQ pipeline statistics (pass/fail, severity, category)

#### Function: main()

- Entry point function
- Creates PipelineOrchestrator and calls run()
- Handles KeyboardInterrupt and unexpected errors
- Returns appropriate exit codes


### 3.2 pipeline.py

**Purpose:** Core metrics pipeline logic, SQL execution, metrics processing

#### Class: MetricsPipeline

**Attributes:**
- `spark`: SparkSession instance
- `bq_operations`: BigQueryOperations instance
- `execution_id`: Unique UUID for pipeline run
- `processed_metrics`: List of successfully processed metric IDs
- `target_tables`: Set of unique target tables used

**Key Methods:**

1. **`validate_partition_info_table(self, partition_info_table: str) -> None`**
   - Validates partition metadata table exists and has required structure
   - Raises: MetricsPipelineError if validation fails

2. **`read_json_from_gcs(self, gcs_path: str) -> List[Dict]`**
   - Reads JSON configuration from GCS
   - Validates GCS path format
   - Returns: List of metric configuration dictionaries

3. **`validate_json(self, json_data: List[Dict]) -> List[Dict]`**
   - Validates required fields in each record
   - Checks for duplicate metric_ids
   - Validates target_table format (project.dataset.table)
   - Returns: Validated data

4. **`replace_sql_placeholders(self, sql: str, run_date: str, partition_info_table: str) -> str`**
   - Replaces {currently} with run_date
   - Replaces {partition_info} with partition_dt from metadata table
   - Returns: SQL with placeholders replaced

5. **`execute_sql(self, sql: str, run_date: str, partition_info_table: str, metric_id: str) -> Dict`**
   - Executes SQL query with placeholder replacement
   - Returns: Dictionary with metric results (numerator, denominator, metric_output, business_data_date)

6. **`process_metrics(self, json_data: List[Dict], run_date: str, dependencies: List[str], partition_info_table: str) -> Tuple`**
   - Filters data by dependencies
   - Groups records by target_table
   - Executes SQL for each metric
   - Creates Spark DataFrames per target_table
   - Returns: (metrics_dfs, successful_metrics, failed_metrics)

7. **`write_to_bq_with_overwrite(self, df: DataFrame, target_table: str) -> Tuple[List[str], List[Dict]]`**
   - Checks for existing metrics
   - Deletes existing metrics (overwrite)
   - Writes DataFrame to BigQuery
   - Returns: (successful_metric_ids, failed_metrics)

8. **`build_recon_record(self, metric_record: Dict, sql: str, run_date: str, env: str, execution_status: str, partition_dt: str, ...) -> Dict`**
   - Builds reconciliation record with comprehensive None value protection
   - Extracts source/target table info from SQL
   - Returns: Recon record dictionary

9. **`create_recon_records_from_write_results(self, ...) -> List[Dict]`**
   - Creates recon records for all metrics (successful and failed)
   - Returns: List of recon record dictionaries

10. **`write_recon_to_bq(self, recon_records: List[Dict], recon_table: str) -> None`**
    - Writes reconciliation records to BigQuery


### 3.3 dq_pipeline.py

**Purpose:** Data quality pipeline logic, check execution, validation

#### Class: DQPipeline

**Attributes:**
- `spark`: SparkSession instance
- `bq_operations`: BigQueryOperations instance

**Key Methods:**

1. **`read_and_validate_dq_config(self, gcs_path: str) -> List[Dict]`**
   - Reads DQ configuration from GCS
   - Validates using ValidationEngine
   - Returns: List of validated DQ check configurations

2. **`replace_sql_placeholders(self, sql: str, run_date: str, partition_info_table: str = None) -> str`**
   - Similar to metrics pipeline placeholder replacement
   - Requires partition_info_table if {partition_info} is used
   - Returns: SQL with placeholders replaced

3. **`execute_dq_checks(self, dq_config: List[Dict], run_date: str, partition_info_table: str = None) -> List[Dict]`**
   - Filters active checks (active=true)
   - Executes each check individually
   - Tracks pass/fail counts and execution time
   - Returns: List of DQ result records

4. **`execute_single_check(self, check_config: Dict, run_date: str, partition_info_table: str = None) -> Dict`**
   - Replaces SQL placeholders
   - Executes SQL query
   - Performs validation comparison
   - Handles errors gracefully
   - Returns: DQ result record

5. **`build_dq_result_record(self, check_config: Dict, actual_result: any, validation_status: str, failure_reason: str, execution_duration: float, run_date: str) -> Dict`**
   - Serializes expected_output and actual_result
   - Adds timestamps and partition date
   - Returns: DQ result record dictionary

6. **`write_dq_results(self, dq_results: List[Dict], dq_target_table: str) -> None`**
   - Creates Spark DataFrame from results
   - Writes to BigQuery DQ results table

7. **`_read_json_from_gcs(self, gcs_path: str) -> List[Dict]`**
   - Private method to read JSON from GCS
   - Uses Python json module for accurate type preservation

8. **`_execute_dq_sql(self, sql: str, check_id: str) -> any`**
   - Private method to execute DQ SQL query
   - Returns single value, empty list, or list of dicts depending on query

### 3.4 bigquery.py

**Purpose:** All BigQuery operations (read, write, schema, queries)

#### Class: BigQueryOperations

**Attributes:**
- `spark`: SparkSession instance
- `bq_client`: BigQuery Client instance (location: europe-west2)

**Key Methods:**

1. **`get_table_schema(self, table_name: str) -> List[SchemaField]`**
   - Gets BigQuery table schema
   - Raises: BigQueryError if table not found

2. **`align_dataframe_schema_with_bq(self, df: DataFrame, target_table: str) -> DataFrame`**
   - Aligns Spark DataFrame with BigQuery schema
   - Drops extra columns, reorders columns, converts types
   - Returns: Aligned DataFrame


3. **`execute_query(self, query: str, timeout: int = 180) -> RowIterator`**
   - Executes BigQuery SQL query
   - Default timeout: 180 seconds
   - Raises: BigQueryError on timeout or failure

4. **`execute_sql_with_results(self, sql: str, metric_id: str = None) -> Dict`**
   - Executes SQL and returns structured results for metrics
   - Validates denominator is not zero or negative
   - Validates business_data_date format
   - Returns: Dictionary with metric fields

5. **`get_partition_date(self, project_dataset: str, table_name: str, partition_info_table: str) -> Optional[str]`**
   - Queries partition metadata table
   - Returns: Latest partition_dt as string or None

6. **`check_existing_metrics(self, metric_ids: List[str], partition_dt: str, target_table: str) -> List[str]`**
   - Checks which metrics already exist for given partition
   - Returns: List of existing metric IDs

7. **`write_dataframe_to_table(self, df: DataFrame, target_table: str, write_mode: str = "append") -> None`**
   - Writes Spark DataFrame to BigQuery
   - Uses direct write method
   - Raises: BigQueryError on failure

8. **`write_metrics_with_overwrite(self, df: DataFrame, target_table: str) -> Tuple[List[str], List[Dict]]`**
   - Checks existing metrics
   - Deletes existing metrics (overwrite operation)
   - Writes DataFrame
   - Returns: (successful_metric_ids, failed_metrics)

9. **`write_recon_records(self, recon_records: List[Dict], recon_table: str) -> None`**
   - Validates recon records before DataFrame creation
   - Creates Spark DataFrame with RECON_SCHEMA
   - Writes to BigQuery recon table

10. **`delete_metrics(self, metric_ids: List[str], partition_dt: str, target_table: str) -> None`**
    - Deletes metrics from BigQuery table
    - Used for overwrite operation

11. **`validate_partition_info_table(self, partition_info_table: str) -> None`**
    - Validates partition metadata table exists
    - Checks for required columns (project_dataset, table_name, partition_dt)

#### Function: create_bigquery_operations(spark: SparkSession) -> BigQueryOperations

- Factory function to create BigQueryOperations instance
- Creates BigQuery client with location="europe-west2"
- Returns: BigQueryOperations instance

### 3.5 config.py

**Purpose:** Configuration constants, schemas, error categories

#### Function: setup_logging() -> Logger

- Configures logging with INFO level
- Format: '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
- Returns: Logger instance

#### Class: PipelineConfig

**Constants:**

- `METRICS_SCHEMA`: StructType for metrics output table (11 fields)
- `RECON_SCHEMA`: StructType for reconciliation table (29 fields)
- `DQ_RESULTS_SCHEMA`: StructType for DQ results table (16 fields)
- `REQUIRED_JSON_FIELDS`: List of required fields in metric JSON
- `SPARK_CONFIGS`: Dictionary of Spark optimization settings
- `QUERY_TIMEOUT`: 180 seconds
- `RECON_MODULE_ID`: '103'
- `RECON_MODULE_TYPE`: 'Metrics'
- `MAX_ERROR_MESSAGE_LENGTH`: 500 characters
- `ERROR_CATEGORIES`: Dictionary mapping error codes to descriptions


#### Class: ValidationConfig

**Methods:**

- `get_default_recon_values() -> Dict[str, str]`: Returns default 'NA' values for optional recon fields

### 3.6 utils.py

**Purpose:** Utility functions (date, numeric, SQL, validation, string)

#### Class: DateUtils

1. **`validate_date_format(date_str: str) -> None`**
   - Validates YYYY-MM-DD format
   - Raises: ValidationError if invalid

2. **`get_current_partition_dt() -> str`**
   - Returns: Current date in YYYY-MM-DD format

3. **`get_current_timestamp() -> datetime`**
   - Returns: Current UTC timestamp

#### Class: NumericUtils

1. **`normalize_numeric_value(value: Union[int, float, Decimal, None]) -> Optional[str]`**
   - Converts numeric values to string representation
   - Preserves precision using Decimal
   - Returns: String or None

2. **`safe_decimal_conversion(value: Optional[str]) -> Optional[Decimal]`**
   - Converts string to Decimal for BigQuery
   - Returns: Decimal or None

3. **`validate_denominator(denominator_str: Optional[str], metric_id: Optional[str] = None) -> None`**
   - Validates denominator is not zero or negative
   - Raises: MetricsPipelineError if invalid

#### Class: SQLUtils

1. **`find_placeholder_positions(sql: str) -> List[Tuple[str, int, int]]`**
   - Finds {currently} and {partition_info} placeholders
   - Returns: List of (placeholder_type, start_pos, end_pos)

2. **`get_table_for_placeholder(sql: str, placeholder_pos: int) -> Optional[Tuple[str, str]]`**
   - Finds table associated with placeholder
   - Returns: (dataset, table_name) or None

3. **`get_source_table_info(sql: str) -> Tuple[Optional[str], Optional[str]]`**
   - Extracts source table from SQL
   - Returns: (dataset_name, table_name) or (None, None)

#### Class: ValidationUtils

1. **`validate_json_record(record: Dict, index: int, existing_metric_ids: set) -> None`**
   - Validates single JSON record
   - Checks required fields, duplicates, target_table format
   - Raises: ValidationError if invalid

#### Class: StringUtils

1. **`clean_error_message(error_message: str) -> str`**
   - Cleans and limits error message length
   - Returns: Cleaned error message (max 500 chars)

2. **`format_error_with_category(error_message: str, error_category: str) -> str`**
   - Formats error with category prefix
   - Returns: "[CATEGORY] Description: message"

3. **`escape_sql_string(value: str) -> str`**
   - Escapes single quotes for SQL
   - Returns: Escaped string

#### Class: ResultSerializer

1. **`serialize_result(result: Any) -> str`**
   - Serializes result to JSON string for BigQuery storage
   - Handles None, primitives, lists, dicts, Decimal
   - Returns: JSON string

2. **`deserialize_result(result_str: str) -> Any`**
   - Deserializes JSON string back to original type
   - Returns: Deserialized value


#### Class: ExecutionUtils

1. **`generate_execution_id() -> str`**
   - Generates unique UUID for pipeline run
   - Returns: UUID string

#### Function: managed_spark_session(app_name: str = "MetricsPipeline")

- Context manager for Spark session with proper cleanup
- Applies Spark configurations from PipelineConfig
- Yields: SparkSession instance
- Ensures spark.stop() is called even on errors

### 3.7 validation.py

**Purpose:** DQ configuration validation engine

#### Class: ValidationEngine

**Constants:**
- `VALID_SEVERITIES`: ["High", "Medium", "Low"]
- `VALID_COMPARISON_TYPES`: ["numeric_condition", "set_match", "not_in_result", "row_match"]
- `REQUIRED_FIELDS`: List of 8 required fields for DQ checks

**Methods:**

1. **`validate_dq_json(json_data: List[Dict]) -> List[Dict]`**
   - Validates entire DQ configuration
   - Validates each record individually
   - Returns: Validated data
   - Raises: ValidationError if invalid

2. **`validate_dq_record(record: Dict, index: int) -> None`**
   - Validates single DQ record
   - Checks required fields, severity, comparison_type compatibility
   - Raises: ValidationError if invalid

3. **`validate_comparison_type(comparison_type: str, expected_output: Any) -> None`**
   - Validates comparison_type and expected_output compatibility
   - Rules:
     - numeric_condition: expected_output must be string (e.g., ">=10", "0")
     - set_match/not_in_result: expected_output must be non-empty list
     - row_match: expected_output must be non-empty list of dicts
   - Raises: ValidationError if incompatible

4. **`validate_severity(severity: str) -> None`**
   - Validates severity is one of: High, Medium, Low
   - Raises: ValidationError if invalid

### 3.8 comparison.py

**Purpose:** DQ result comparison engine (4 comparison types)

#### Class: ComparisonEngine

**Constants:**
- `NUMERIC_OPERATORS`: [">=", "<=", "==", "!=", ">", "<"]

**Methods:**

1. **`compare(actual_result: Any, expected_output: Any, comparison_type: str) -> Tuple[str, str]`**
   - Main comparison dispatcher
   - Returns: (validation_status, failure_reason)
   - validation_status: "PASS" or "FAIL"

2. **`compare_numeric_condition(actual: Any, expected: str) -> Tuple[str, str]`**
   - Compares numeric value against condition (e.g., ">=10")
   - Parses operator and expected value
   - Extracts numeric value from actual result
   - Returns: ("PASS", "") or ("FAIL", reason)

3. **`compare_set_match(actual: List, expected: List) -> Tuple[str, str]`**
   - Compares result set against expected set (order-independent)
   - Identifies missing and extra values
   - Returns: ("PASS", "") or ("FAIL", "Missing values: [...]; Extra values: [...]")

4. **`compare_not_in_result(actual: List, expected: List) -> Tuple[str, str]`**
   - Verifies disallowed values not in result
   - Returns: ("PASS", "") or ("FAIL", "Found disallowed values: [...]")

5. **`compare_row_match(actual: List[Dict], expected: List[Dict]) -> Tuple[str, str]`**
   - Compares result rows against expected rows (order-independent)
   - Normalizes rows to comparable tuples
   - Identifies missing and extra rows
   - Returns: ("PASS", "") or ("FAIL", reason)


**Helper Methods:**

- `_parse_numeric_condition(condition: str) -> Tuple[str, float]`: Parses condition string
- `_extract_numeric_value(result: Any) -> float`: Extracts numeric value from various formats
- `_normalize_to_list(result: Any) -> List`: Normalizes result to list
- `_normalize_values(values: List) -> List`: Normalizes values for comparison
- `_normalize_to_row_list(result: Any) -> List[Dict]`: Normalizes to list of dicts
- `_normalize_row(row: Dict) -> Tuple`: Normalizes row to comparable tuple
- `_tuple_to_dict(row_tuple: Tuple) -> Dict`: Converts tuple back to dict

### 3.9 exceptions.py

**Purpose:** Custom exception classes

#### Class: MetricsPipelineError(Exception)

- Base exception for pipeline errors
- Stores metric_id for error tracking
- Adds metric context to error message

#### Class: ValidationError(MetricsPipelineError)

- Exception for data validation errors

#### Class: SQLExecutionError(MetricsPipelineError)

- Exception for SQL execution errors

#### Class: BigQueryError(MetricsPipelineError)

- Exception for BigQuery operations errors

#### Class: GCSError(MetricsPipelineError)

- Exception for GCS operations errors

---

## 4. Configuration and Environment

### 4.1 Required Python Version

- **Python 3.7+** (recommended: Python 3.8 or 3.9)

### 4.2 Key Libraries

**Core Dependencies:**
```
pyspark>=3.0.0
google-cloud-bigquery>=2.0.0
google-cloud-storage>=1.0.0
```

**Standard Library:**
```python
import argparse
import sys
import json
import re
import uuid
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union, Any
from decimal import Decimal
from contextlib import contextmanager
```

**Google Cloud:**
```python
from google.api_core.exceptions import DeadlineExceeded
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, GoogleCloudError
```

**PySpark:**
```python
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    TimestampType, DecimalType, IntegerType, ArrayType, DoubleType
)
```

### 4.3 Configuration Files

**No external configuration files required.** All configuration is:
- Hardcoded in `config.py` (schemas, constants)
- Passed via command-line arguments
- Stored in JSON files on GCS (metric/DQ configurations)

### 4.4 Environment Variables

**Google Cloud Authentication:**
- `GOOGLE_APPLICATION_CREDENTIALS`: Path to service account JSON key file
- Or use default application credentials (gcloud auth application-default login)

**Spark Configuration:**
- Set via `PipelineConfig.SPARK_CONFIGS` in code
- Can be overridden via Spark submit arguments


### 4.5 Input Data Locations

**GCS Paths:**
- Metric configurations: `gs://bucket-name/path/to/metrics_config.json`
- DQ configurations: `gs://bucket-name/path/to/dq_config.json`

**BigQuery Tables:**
- Partition metadata: `project.dataset.partition_info`
- Metrics target tables: Specified in JSON config (target_table field)
- Reconciliation table: Specified via `--recon_table` argument
- DQ results table: Specified via `--dq_target_table` argument

### 4.6 Output Data Locations

**BigQuery Tables:**
- Metrics: Written to target_table specified in JSON config
- Reconciliation: Written to recon_table specified in CLI args
- DQ Results: Written to dq_target_table specified in CLI args

**Logs:**
- Console output (stdout/stderr)
- Can be redirected to files or captured by orchestration tools

### 4.7 Credentials

**Google Cloud Service Account:**
- Requires permissions:
  - BigQuery Data Editor (read/write tables)
  - BigQuery Job User (run queries)
  - Storage Object Viewer (read GCS files)
- Set via `GOOGLE_APPLICATION_CREDENTIALS` environment variable
- Or use default application credentials

---

## 5. Logging & Error Handling

### 5.1 Logging Setup

**Configuration:**
- Level: `INFO`
- Format: `'%(asctime)s - %(name)s - %(levelname)s - %(message)s'`
- Output: Console (stdout)
- Setup: `config.setup_logging()` function

**Log Levels Used:**
- `INFO`: Normal execution flow, step completion, statistics
- `WARNING`: Non-critical issues (e.g., no placeholders found, empty results)
- `ERROR`: Errors that don't stop execution (e.g., individual metric failures)
- `DEBUG`: Detailed information (SQL queries, intermediate values)

**Key Logging Points:**
1. Pipeline configuration and arguments
2. Each execution step (0-5 for metrics, 1-3 for DQ)
3. SQL query execution (original and final with replacements)
4. Record counts and processing statistics
5. Success/failure for each metric or check
6. Final pipeline statistics

### 5.2 Error Handling Strategy

**Exception Hierarchy:**
```
MetricsPipelineError (base)
├── ValidationError (data validation)
├── SQLExecutionError (SQL query failures)
├── BigQueryError (BQ operations)
└── GCSError (GCS operations)
```

**Error Handling Patterns:**

1. **Individual Metric Failures (Graceful Degradation):**
   - Metrics are processed individually in try-except blocks
   - Failed metrics are logged and tracked
   - Pipeline continues processing remaining metrics
   - Failed metrics get recon records with error details

2. **Pipeline-Level Failures (Abort with Recon):**
   - Critical errors (e.g., GCS read failure) abort pipeline
   - Attempts to create failure recon records before exit
   - Re-raises original exception after cleanup

3. **Validation Errors (Fail Fast):**
   - JSON validation errors stop pipeline immediately
   - Clear error messages indicate which record/field failed
   - No partial processing for invalid configurations


**Error Categories:**

Defined in `PipelineConfig.ERROR_CATEGORIES`:
- `PARTITION_VALIDATION_ERROR`: Partition table validation failed
- `GCS_READ_ERROR`: Failed to read JSON from GCS
- `JSON_VALIDATION_ERROR`: JSON data validation failed
- `SQL_EXECUTION_ERROR`: SQL query execution failed
- `SQL_TIMEOUT_ERROR`: SQL query timed out
- `SQL_TABLE_NOT_FOUND_ERROR`: SQL table not found
- `SQL_SYNTAX_ERROR`: SQL syntax error
- `BIGQUERY_WRITE_ERROR`: BigQuery write operation failed
- `RECON_CREATION_ERROR`: Recon record creation failed
- `PIPELINE_EXECUTION_ERROR`: General pipeline execution error
- `DQ_VALIDATION_ERROR`: DQ validation check failed
- `DQ_CONFIG_ERROR`: DQ configuration validation failed
- `DQ_COMPARISON_ERROR`: DQ comparison operation failed

### 5.3 Retry Logic

**No automatic retries implemented.** Reasons:
- BigQuery operations have built-in retries
- Spark operations have built-in fault tolerance
- Pipeline is designed to be idempotent (can be re-run safely)

**Recommendation:** Implement retries at orchestration level (e.g., Airflow, Cloud Composer)

### 5.4 Timeout Handling

**Query Timeout:**
- Default: 180 seconds (`PipelineConfig.QUERY_TIMEOUT`)
- Applied to all BigQuery query operations
- Raises: `BigQueryError` or `SQLExecutionError` on timeout

---

## 6. Dependencies and Data Flow

### 6.1 External Dependencies

**Python Packages:**
- `pyspark`: Distributed data processing
- `google-cloud-bigquery`: BigQuery client library
- `google-cloud-storage`: GCS client library (implicit via Spark)

**Google Cloud Services:**
- BigQuery: Data warehouse for metrics and DQ results
- Cloud Storage (GCS): Configuration file storage
- IAM: Service account authentication

**Infrastructure:**
- Spark cluster or local Spark installation
- Network access to Google Cloud APIs

### 6.2 Internal Dependencies

**Module Import Graph:**
```
main.py
├── config.py
├── exceptions.py
├── utils.py (DateUtils, managed_spark_session)
├── pipeline.py
│   ├── config.py
│   ├── exceptions.py
│   ├── utils.py (all classes)
│   └── bigquery.py
├── dq_pipeline.py
│   ├── config.py
│   ├── exceptions.py
│   ├── utils.py (DateUtils, ResultSerializer, SQLUtils)
│   ├── validation.py
│   ├── comparison.py
│   └── bigquery.py
└── bigquery.py
    ├── config.py
    ├── exceptions.py
    └── utils.py (StringUtils, NumericUtils)

validation.py
└── exceptions.py

comparison.py
└── (no dependencies)

utils.py
├── config.py
└── exceptions.py

exceptions.py
└── (no dependencies)

config.py
└── (no dependencies)
```

### 6.3 Data Flow

**Metrics Mode:**
```
GCS JSON Config
    ↓
Read & Validate
    ↓
Filter by Dependencies
    ↓
Execute SQL Queries → BigQuery
    ↓
Create Spark DataFrames
    ↓
Write to Target Tables → BigQuery
    ↓
Create Recon Records
    ↓
Write to Recon Table → BigQuery
```

**DQ Mode:**
```
GCS JSON Config
    ↓
Read & Validate
    ↓
Filter Active Checks
    ↓
Execute SQL Queries → BigQuery
    ↓
Compare Results
    ↓
Create Result Records
    ↓
Write to DQ Results Table → BigQuery
```


### 6.4 Data Sharing Between Modules

**Function Calls:**
- All modules communicate via function calls and return values
- No shared state or global variables (except logger)
- Pipeline instances passed as parameters

**Spark DataFrames:**
- Created in pipeline modules
- Passed to BigQuery operations for writing
- Not persisted to disk (in-memory processing)

**Dictionaries/Lists:**
- Configuration data passed as Python dicts/lists
- Results accumulated in lists
- No intermediate file storage

---

## 7. Execution Guide

### 7.1 Prerequisites

1. **Install Dependencies:**
   ```bash
   pip install pyspark google-cloud-bigquery google-cloud-storage
   ```

2. **Set Up Google Cloud Authentication:**
   ```bash
   # Option 1: Service account key file
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
   
   # Option 2: Default application credentials
   gcloud auth application-default login
   ```

3. **Prepare Configuration Files:**
   - Upload metric/DQ configuration JSON to GCS
   - Ensure BigQuery tables exist (partition_info, recon, dq_results)

### 7.2 Running Metrics Pipeline

**Basic Command:**
```bash
python main.py \
  --mode metrics \
  --gcs_path gs://bucket/path/metrics_config.json \
  --run_date 2024-01-15 \
  --dependencies "dep1,dep2,dep3" \
  --partition_info_table project.dataset.partition_info \
  --env PRD \
  --recon_table project.dataset.recon_table
```

**With Spark Submit:**
```bash
spark-submit \
  --master local[*] \
  --conf spark.sql.adaptive.enabled=true \
  main.py \
  --mode metrics \
  --gcs_path gs://bucket/path/metrics_config.json \
  --run_date 2024-01-15 \
  --dependencies "dep1,dep2,dep3" \
  --partition_info_table project.dataset.partition_info \
  --env PRD \
  --recon_table project.dataset.recon_table
```

**Arguments Explained:**
- `--mode metrics`: Run in metrics mode
- `--gcs_path`: GCS path to JSON configuration file
- `--run_date`: Business date in YYYY-MM-DD format (used for {currently} placeholder)
- `--dependencies`: Comma-separated list of dependencies to process
- `--partition_info_table`: BigQuery table containing partition metadata
- `--env`: Environment name (e.g., BLD, PRD, DEV)
- `--recon_table`: BigQuery table for reconciliation records

### 7.3 Running DQ Pipeline

**Basic Command:**
```bash
python main.py \
  --mode dq \
  --gcs_path gs://bucket/path/dq_config.json \
  --run_date 2024-01-15 \
  --dq_target_table project.dataset.dq_results
```

**With Optional Partition Info:**
```bash
python main.py \
  --mode dq \
  --gcs_path gs://bucket/path/dq_config.json \
  --run_date 2024-01-15 \
  --dq_target_table project.dataset.dq_results \
  --dq_partition_info_table project.dataset.partition_info
```

**Arguments Explained:**
- `--mode dq`: Run in DQ mode
- `--gcs_path`: GCS path to DQ configuration JSON
- `--run_date`: Business date in YYYY-MM-DD format
- `--dq_target_table`: BigQuery table for DQ results
- `--dq_partition_info_table`: (Optional) Partition metadata table for {partition_info} placeholder


### 7.4 Sample Metrics Configuration JSON

```json
[
  {
    "metric_id": "M001",
    "metric_name": "Total Sales",
    "metric_type": "COUNT",
    "metric_description": "Total number of sales transactions",
    "frequency": "DAILY",
    "sql": "SELECT 'M001' as metric_id, 'Total Sales' as metric_name, 'COUNT' as metric_type, COUNT(*) as numerator_value, 1 as denominator_value, COUNT(*) as metric_output, '{currently}' as business_data_date FROM `project.dataset.sales` WHERE date = '{currently}'",
    "dependency": "sales_data",
    "target_table": "project.dataset.metrics_output"
  }
]
```

**Required Fields:**
- `metric_id`: Unique identifier
- `metric_name`: Descriptive name
- `metric_type`: Type of metric
- `sql`: SQL query (must return: numerator_value, denominator_value, metric_output, business_data_date)
- `dependency`: Dependency name (used for filtering)
- `target_table`: BigQuery table in format project.dataset.table

**Optional Fields:**
- `metric_description`: Description
- `frequency`: Frequency (e.g., DAILY, WEEKLY)

### 7.5 Sample DQ Configuration JSON

```json
[
  {
    "check_id": "CHK001",
    "category": "Completeness",
    "sql_query": "SELECT COUNT(*) FROM `project.dataset.table` WHERE date = '{currently}' AND column IS NULL",
    "description": "Check for null values in critical column",
    "severity": "High",
    "expected_output": "0",
    "comparison_type": "numeric_condition",
    "active": true,
    "impacted_downstream": ["report1", "dashboard2"],
    "tags": ["data_quality", "completeness"]
  }
]
```

**Required Fields:**
- `check_id`: Unique identifier
- `category`: Category (e.g., Completeness, Accuracy, Consistency)
- `sql_query`: SQL query to execute
- `description`: Description of the check
- `severity`: High, Medium, or Low
- `expected_output`: Expected result (format depends on comparison_type)
- `comparison_type`: numeric_condition, set_match, not_in_result, or row_match
- `active`: true or false (only active checks are executed)

**Optional Fields:**
- `impacted_downstream`: List of downstream systems/reports
- `tags`: List of tags for categorization

### 7.6 Expected Output

**Successful Execution:**
```
================================================================================
PIPELINE CONFIGURATION:
--------------------------------------------------------------------------------
Mode: METRICS
GCS Path: gs://bucket/path/metrics_config.json
Run Date: 2024-01-15
Dependencies: ['dep1', 'dep2'] (2 total)
...
================================================================================
STARTING METRICS PIPELINE EXECUTION
================================================================================
...
STEP 3: PROCESSING METRICS
...
   Total successful metrics: 10
   Total failed metrics: 0
   Target tables with data: 2
...
PIPELINE EXECUTION COMPLETED SUCCESSFULLY!
================================================================================
```

**Exit Codes:**
- `0`: Success
- `1`: Failure (errors occurred)
- `130`: Interrupted by user (Ctrl+C)

### 7.7 Output Locations

**BigQuery Tables:**
- Metrics: `project.dataset.metrics_output` (or as specified in JSON)
- Reconciliation: `project.dataset.recon_table` (as specified in CLI)
- DQ Results: `project.dataset.dq_results` (as specified in CLI)

**Logs:**
- Console output (can be redirected to file)
- Example: `python main.py ... > pipeline.log 2>&1`


---

## 8. New Features

### 8.1 Error Resilience

**Feature:** DQ pipeline continues executing all checks even when individual checks fail due to configuration errors, SQL errors, or comparison errors.

**Implementation Date:** 2024-01-15

**Problem Solved:**
Previously, if one DQ check had a configuration error (e.g., invalid severity "Critical"), the entire pipeline would stop and remaining checks wouldn't execute.

**How It Works:**

1. **Configuration Validation Per-Check**: Each check's configuration is validated during execution, not upfront
2. **Error Handling**: All errors are caught and logged, but execution continues
3. **Result Recording**: Failed checks are marked as "FAIL" with detailed error messages in the DQ results table

**Error Types Handled:**

| Error Type | Example | Behavior |
|------------|---------|----------|
| Configuration Error | Invalid severity "Critical" | Mark as FAIL, log error, continue |
| SQL Execution Error | Table not found, syntax error | Mark as FAIL, log error, continue |
| Comparison Error | Type conversion failure | Mark as FAIL, log error, continue |
| Unexpected Error | Any other error | Mark as FAIL, log error, continue |

**Code Location:**
- `dq_pipeline.py::execute_single_check()` - Main error handling logic
- `dq_pipeline.py::read_and_validate_dq_config()` - Only validates basic structure

**Benefits:**
- Complete visibility into all checks (not just the first failure)
- Comprehensive error reporting
- All results written to BigQuery for analysis
- Better troubleshooting with detailed error messages

**Documentation:** See `ERROR_RESILIENCE_IMPLEMENTATION.md` for detailed implementation guide

---

### 8.2 Overwrite Functionality

**Feature:** DQ pipeline now uses the same overwrite logic as metrics pipeline - running the pipeline multiple times in the same day replaces previous results instead of creating duplicates.

**Implementation Date:** 2024-01-15

**Problem Solved:**
Running the DQ pipeline twice in the same day created duplicate records in the DQ results table. Users wanted only the latest run's results to be kept.

**How It Works:**

1. **Check for Existing Records**: Query BigQuery for existing checks with same `check_id` and `partition_dt`
2. **Delete Existing Records**: If found, delete them using SQL DELETE statement
3. **Write New Records**: Write new results using append mode (after deletion = overwrite)

**Key Points:**
- **Partition-based**: Only overwrites records with the same `partition_dt` (current date)
- **Check-specific**: Only overwrites specific `check_id` values in the current run
- **Safe Operation**: Uses DELETE + APPEND pattern (same as metrics pipeline)
- **No Data Loss**: Previous days' data is preserved

**Code Location:**
- `bigquery.py::check_existing_dq_checks()` - Checks for existing records
- `bigquery.py::delete_dq_checks()` - Deletes existing records
- `bigquery.py::write_dq_checks_with_overwrite()` - Main overwrite orchestration
- `dq_pipeline.py::write_dq_results()` - Updated to use overwrite method

**Documentation:** See `DQ_OVERWRITE_FUNCTIONALITY.md` for detailed implementation guide

---

### 8.3 Configuration Validator

**Feature:** Standalone Python script to validate DQ JSON configuration files before deployment, catching errors early and preventing deployment failures.

**Implementation Date:** 2024-01-15

**Script:** `validate_dq_config.py`

**Usage:**
```bash
# Validate a configuration file
python validate_dq_config.py sample_dq_config.json

# Exit codes:
# 0 = Valid (ready for deployment)
# 1 = Invalid (fix errors first)
```

**What It Validates:**

| Validation | Description |
|------------|-------------|
| Required Fields | Ensures all 8 required fields are present |
| Field Types | Checks strings are strings, booleans are booleans, etc. |
| Severity Values | Must be "High", "Medium", or "Low" (case-sensitive) |
| Comparison Types | Must be valid type (numeric_condition, set_match, etc.) |
| Expected Output Format | Must match comparison_type requirements |
| Non-empty Strings | check_id, category, sql_query cannot be empty |

**CI/CD Integration:**

**GitHub Actions:**
```yaml
- name: Validate DQ Configuration
  run: python validate_dq_config.py sample_dq_config.json
```

**Pre-commit Hook:**
```bash
python validate_dq_config.py sample_dq_config.json || exit 1
```

**Benefits:**
- Fast validation (runs in seconds)
- Early error detection (before deployment)
- Clear error messages
- CI/CD friendly (exit codes for automation)
- No dependencies (Python standard library only)

**Documentation:**
- `VALIDATOR_README.md` - Comprehensive documentation
- `VALIDATOR_QUICK_START.md` - Quick reference guide

---

## 9. Maintenance Notes

### 9.1 Known Issues and Limitations

1. **BigQuery Location Hardcoded:**
   - BigQuery client location is hardcoded to "europe-west2"
   - **Impact:** Cannot process data in other regions without code change
   - **Workaround:** Modify `bigquery.py` line: `bigquery.Client(location="europe-west2")`

2. **No Automatic Retries:**
   - Pipeline does not implement automatic retry logic
   - **Impact:** Transient failures require manual re-run
   - **Workaround:** Implement retries at orchestration level (Airflow, Cloud Composer)

3. **Single Partition Date per Run:**
   - Pipeline processes one partition date per execution
   - **Impact:** Cannot backfill multiple dates in single run
   - **Workaround:** Loop over dates in orchestration script

4. **Memory Constraints:**
   - All metrics for a target table are collected in memory before writing
   - **Impact:** Large number of metrics may cause OOM errors
   - **Workaround:** Process in smaller batches or increase Spark memory

5. **No Incremental Processing:**
   - Pipeline always processes all metrics for specified dependencies
   - **Impact:** Cannot process only changed metrics
   - **Workaround:** Filter JSON configuration before upload

6. **Placeholder Limitation:**
   - {partition_info} placeholder requires table reference before it in SQL
   - **Impact:** Cannot use placeholder at start of query
   - **Workaround:** Restructure SQL to reference table before placeholder

7. **Error Message Truncation:**
   - Error messages truncated to 500 characters in recon records
   - **Impact:** Full error details may be lost
   - **Workaround:** Check console logs for full error messages

### 8.2 Common Troubleshooting

**Issue: "Table not found" errors**
- **Cause:** BigQuery table doesn't exist or incorrect permissions
- **Solution:** Verify table exists and service account has access

**Issue: "Invalid GCS path" errors**
- **Cause:** GCS path format incorrect or file doesn't exist
- **Solution:** Ensure path starts with `gs://` and file exists

**Issue: "Validation failed" errors**
- **Cause:** JSON configuration missing required fields or invalid format
- **Solution:** Validate JSON against schema, check required fields

**Issue: "Denominator is zero" errors**
- **Cause:** SQL query returns zero denominator
- **Solution:** Fix SQL query or add WHERE clause to filter out zero denominators

**Issue: "Timeout" errors**
- **Cause:** SQL query takes longer than 180 seconds
- **Solution:** Optimize query or increase `QUERY_TIMEOUT` in config.py

**Issue: "Spark session creation failed"**
- **Cause:** Spark not installed or configured incorrectly
- **Solution:** Install PySpark, verify JAVA_HOME is set

### 8.3 Performance Optimization Tips

1. **Partition Pruning:**
   - Always include partition filters in SQL queries
   - Use {currently} or {partition_info} placeholders

2. **Batch Size:**
   - Process metrics in smaller dependency groups
   - Avoid processing all dependencies in single run

3. **Spark Configuration:**
   - Adjust Spark memory settings based on data volume
   - Enable adaptive query execution (already enabled)

4. **BigQuery Optimization:**
   - Use clustered tables for target tables
   - Partition target tables by partition_dt

5. **Parallel Processing:**
   - Metrics within same target_table are processed sequentially
   - Different target_tables can be processed in parallel (future enhancement)


### 8.4 Suggested Improvements

**High Priority:**

1. **Configurable BigQuery Location:**
   - Make BigQuery location configurable via CLI argument or config file
   - Benefit: Support multi-region deployments

2. **Parallel Target Table Processing:**
   - Process different target tables in parallel using Spark
   - Benefit: Significant performance improvement for multi-table scenarios

3. **Incremental Processing:**
   - Add support for processing only changed/new metrics
   - Benefit: Faster execution for large configurations

4. **Retry Logic:**
   - Implement exponential backoff retry for transient failures
   - Benefit: Improved reliability

5. **Monitoring and Alerting:**
   - Add metrics export (e.g., to Cloud Monitoring)
   - Benefit: Better observability

**Medium Priority:**

6. **Configuration Validation Tool:**
   - Standalone tool to validate JSON configurations before upload
   - Benefit: Catch errors early

7. **Dry Run Mode:**
   - Add `--dry-run` flag to validate without executing
   - Benefit: Safe testing of configurations

8. **Partial Failure Handling:**
   - Continue processing even if some checks fail validation
   - Benefit: Process valid checks, report invalid ones

9. **Schema Evolution:**
   - Auto-detect schema changes in target tables
   - Benefit: Reduce manual schema management

10. **Query Result Caching:**
    - Cache query results for reuse across metrics
    - Benefit: Reduce BigQuery costs

**Low Priority:**

11. **Web UI:**
    - Build web interface for configuration management
    - Benefit: Easier for non-technical users

12. **Historical Comparison:**
    - Compare current results with historical data
    - Benefit: Detect anomalies

13. **Custom Comparison Functions:**
    - Allow user-defined comparison logic
    - Benefit: More flexible validation

### 8.5 Code Quality Recommendations

1. **Add Unit Tests:**
   - Test utility functions (DateUtils, NumericUtils, SQLUtils)
   - Test comparison logic (ComparisonEngine)
   - Test validation logic (ValidationEngine)

2. **Add Integration Tests:**
   - Test end-to-end pipeline with mock BigQuery
   - Test error handling scenarios

3. **Type Hints:**
   - Already present, maintain consistency
   - Add mypy type checking to CI/CD

4. **Documentation:**
   - Add docstring examples for complex functions
   - Create API documentation using Sphinx

5. **Code Coverage:**
   - Aim for >80% code coverage
   - Focus on critical paths (SQL execution, validation)

### 8.6 Security Considerations

1. **Credential Management:**
   - Never hardcode credentials in code
   - Use service accounts with minimal required permissions
   - Rotate service account keys regularly

2. **SQL Injection:**
   - Current implementation uses parameterized queries where possible
   - Validate and sanitize user inputs
   - Escape special characters in SQL strings

3. **Data Access:**
   - Implement row-level security in BigQuery
   - Use separate service accounts for different environments

4. **Audit Logging:**
   - Enable BigQuery audit logs
   - Monitor for suspicious query patterns

5. **Secrets Management:**
   - Use Secret Manager for sensitive configuration
   - Avoid storing secrets in GCS


---

## 10. File Dependency Diagram

### 10.1 Module Dependency Graph

```
┌─────────────────────────────────────────────────────────────────┐
│                           main.py                                │
│                     (Entry Point)                                │
└────────┬────────────────────────────────────────────────────────┘
         │
         ├─────────────────────────────────────────────────────────┐
         │                                                          │
         ▼                                                          ▼
┌──────────────────┐                                    ┌──────────────────┐
│   pipeline.py    │                                    │  dq_pipeline.py  │
│ (Metrics Logic)  │                                    │   (DQ Logic)     │
└────────┬─────────┘                                    └────────┬─────────┘
         │                                                        │
         ├────────────────────────────────────────────────────────┤
         │                                                        │
         ▼                                                        ▼
┌──────────────────────────────────────────────────────────────────┐
│                        bigquery.py                                │
│                  (BigQuery Operations)                            │
└────────┬─────────────────────────────────────────────────────────┘
         │
         ├─────────────────────────────────────────────────────────┐
         │                                                          │
         ▼                                                          ▼
┌──────────────────┐                                    ┌──────────────────┐
│    utils.py      │                                    │   config.py      │
│   (Utilities)    │                                    │ (Configuration)  │
└────────┬─────────┘                                    └──────────────────┘
         │
         │
         ▼
┌──────────────────┐
│  exceptions.py   │
│   (Exceptions)   │
└──────────────────┘


         ┌─────────────────────────────────────────────────────────┐
         │              dq_pipeline.py                              │
         └────────┬────────────────────────────────────────────────┘
                  │
                  ├──────────────────────────────────────────────────┐
                  │                                                   │
                  ▼                                                   ▼
         ┌──────────────────┐                              ┌──────────────────┐
         │  validation.py   │                              │  comparison.py   │
         │  (DQ Validation) │                              │ (DQ Comparison)  │
         └────────┬─────────┘                              └──────────────────┘
                  │
                  ▼
         ┌──────────────────┐
         │  exceptions.py   │
         └──────────────────┘
```

### 9.2 Import Dependencies Matrix

| Module         | Imports From                                                    |
|----------------|-----------------------------------------------------------------|
| main.py        | config, exceptions, utils, pipeline, dq_pipeline, bigquery     |
| pipeline.py    | config, exceptions, utils, bigquery                             |
| dq_pipeline.py | config, exceptions, utils, validation, comparison, bigquery     |
| bigquery.py    | config, exceptions, utils                                       |
| validation.py  | exceptions                                                      |
| comparison.py  | (none)                                                          |
| utils.py       | config, exceptions                                              |
| config.py      | (none)                                                          |
| exceptions.py  | (none)                                                          |

### 9.3 Execution Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    START: main.py                                │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
                    ┌────────────────┐
                    │ Parse Arguments│
                    └────────┬───────┘
                             │
                             ▼
                    ┌────────────────┐
                    │  Mode Check    │
                    └────────┬───────┘
                             │
                ┌────────────┴────────────┐
                │                         │
                ▼                         ▼
    ┌───────────────────┐     ┌───────────────────┐
    │  Metrics Mode     │     │    DQ Mode        │
    │  (pipeline.py)    │     │  (dq_pipeline.py) │
    └─────────┬─────────┘     └─────────┬─────────┘
              │                         │
              ▼                         ▼
    ┌───────────────────┐     ┌───────────────────┐
    │ Read & Validate   │     │ Read & Validate   │
    │ JSON Config       │     │ DQ Config         │
    └─────────┬─────────┘     └─────────┬─────────┘
              │                         │
              ▼                         ▼
    ┌───────────────────┐     ┌───────────────────┐
    │ Process Metrics   │     │ Execute DQ Checks │
    │ (Execute SQL)     │     │ (Execute SQL +    │
    │                   │     │  Compare Results) │
    └─────────┬─────────┘     └─────────┬─────────┘
              │                         │
              ▼                         ▼
    ┌───────────────────┐     ┌───────────────────┐
    │ Write to BigQuery │     │ Write DQ Results  │
    │ (Target Tables)   │     │ to BigQuery       │
    └─────────┬─────────┘     └─────────┬─────────┘
              │                         │
              ▼                         │
    ┌───────────────────┐              │
    │ Create & Write    │              │
    │ Recon Records     │              │
    └─────────┬─────────┘              │
              │                         │
              └────────────┬────────────┘
                           │
                           ▼
                  ┌────────────────┐
                  │  END: Success  │
                  │  or Failure    │
                  └────────────────┘
```


### 9.4 Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    INPUT SOURCES                                 │
├─────────────────────────────────────────────────────────────────┤
│  • GCS JSON Config (gs://bucket/path/config.json)               │
│  • Command Line Arguments (--mode, --run_date, etc.)            │
│  • BigQuery Partition Metadata Table                            │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PROCESSING LAYER                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  1. Read & Validate Configuration                        │  │
│  │     • Parse JSON                                         │  │
│  │     • Validate required fields                           │  │
│  │     • Check data types                                   │  │
│  └──────────────────────────────────────────────────────────┘  │
│                             │                                   │
│                             ▼                                   │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  2. SQL Placeholder Replacement                          │  │
│  │     • Replace {currently} with run_date                  │  │
│  │     • Replace {partition_info} with partition_dt         │  │
│  └──────────────────────────────────────────────────────────┘  │
│                             │                                   │
│                             ▼                                   │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  3. Execute SQL Queries                                  │  │
│  │     • Query BigQuery                                     │  │
│  │     • Extract results                                    │  │
│  │     • Handle errors                                      │  │
│  └──────────────────────────────────────────────────────────┘  │
│                             │                                   │
│                             ▼                                   │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  4. Process Results                                      │  │
│  │     • Create Spark DataFrames (Metrics)                  │  │
│  │     • Compare with expected (DQ)                         │  │
│  │     • Build result records                               │  │
│  └──────────────────────────────────────────────────────────┘  │
│                             │                                   │
│                             ▼                                   │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  5. Write to BigQuery                                    │  │
│  │     • Align schema                                       │  │
│  │     • Check existing records                             │  │
│  │     • Delete existing (overwrite)                        │  │
│  │     • Write new records                                  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                             │                                   │
└─────────────────────────────┼───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    OUTPUT DESTINATIONS                           │
├─────────────────────────────────────────────────────────────────┤
│  • BigQuery Metrics Tables (project.dataset.metrics_*)          │
│  • BigQuery Recon Table (project.dataset.recon_table)           │
│  • BigQuery DQ Results Table (project.dataset.dq_results)       │
│  • Console Logs (stdout/stderr)                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 10. Quick Reference

### 10.1 Common Commands

**Run Metrics Pipeline:**
```bash
python main.py --mode metrics --gcs_path gs://bucket/metrics.json \
  --run_date 2024-01-15 --dependencies "dep1,dep2" \
  --partition_info_table project.dataset.partition_info \
  --env PRD --recon_table project.dataset.recon
```

**Run DQ Pipeline:**
```bash
python main.py --mode dq --gcs_path gs://bucket/dq.json \
  --run_date 2024-01-15 --dq_target_table project.dataset.dq_results
```

**Check Logs:**
```bash
# View last 100 lines
tail -n 100 pipeline.log

# Search for errors
grep -i "error" pipeline.log

# Search for specific metric
grep "M001" pipeline.log
```

### 10.2 Key File Locations

| File | Purpose |
|------|---------|
| main.py | Entry point, orchestration |
| pipeline.py | Metrics pipeline logic |
| dq_pipeline.py | DQ pipeline logic |
| bigquery.py | BigQuery operations |
| config.py | Configuration and schemas |
| utils.py | Utility functions |
| validation.py | DQ validation engine |
| comparison.py | DQ comparison engine |
| exceptions.py | Custom exceptions |


### 10.3 Important Constants

| Constant | Value | Location |
|----------|-------|----------|
| QUERY_TIMEOUT | 180 seconds | config.py |
| RECON_MODULE_ID | '103' | config.py |
| MAX_ERROR_MESSAGE_LENGTH | 500 chars | config.py |
| BigQuery Location | europe-west2 | bigquery.py |
| Log Level | INFO | config.py |

### 10.4 SQL Placeholders

| Placeholder | Replaced With | Example |
|-------------|---------------|---------|
| {currently} | run_date argument | '2024-01-15' |
| {partition_info} | partition_dt from metadata table | '2024-01-14' |

### 10.5 DQ Comparison Types

| Type | Expected Output Format | Example |
|------|------------------------|---------|
| numeric_condition | String with operator | ">=10", "0", "==100" |
| set_match | List of values | ["A", "B", "C"] |
| not_in_result | List of disallowed values | ["ERROR", "INVALID"] |
| row_match | List of dictionaries | [{"col1": "val1", "col2": "val2"}] |

### 10.6 Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Failure (errors occurred) |
| 130 | Interrupted by user (Ctrl+C) |

### 10.7 Required BigQuery Permissions

**Service Account Needs:**
- `bigquery.jobs.create` - Run queries
- `bigquery.tables.get` - Read table schemas
- `bigquery.tables.getData` - Read table data
- `bigquery.tables.updateData` - Write table data
- `storage.objects.get` - Read GCS files

**Recommended Roles:**
- BigQuery Data Editor
- BigQuery Job User
- Storage Object Viewer

---

## 11. Appendix

### 11.1 Metrics Schema (BigQuery)

```sql
CREATE TABLE `project.dataset.metrics_output` (
  metric_id STRING NOT NULL,
  metric_name STRING NOT NULL,
  metric_type STRING NOT NULL,
  metric_description STRING,
  frequency STRING,
  numerator_value NUMERIC(38, 9),
  denominator_value NUMERIC(38, 9),
  metric_output NUMERIC(38, 9),
  business_data_date STRING NOT NULL,
  partition_dt STRING NOT NULL,
  pipeline_execution_ts TIMESTAMP NOT NULL
)
PARTITION BY DATE(PARSE_DATE('%Y-%m-%d', partition_dt))
CLUSTER BY metric_id;
```

### 11.2 Reconciliation Schema (BigQuery)

```sql
CREATE TABLE `project.dataset.recon_table` (
  module_id STRING NOT NULL,
  module_type_nm STRING NOT NULL,
  source_databs_nm STRING,
  source_table_nm STRING,
  source_column_nm STRING,
  source_file_nm STRING,
  source_contrl_file_nm STRING,
  source_server_nm STRING NOT NULL,
  target_databs_nm STRING,
  target_table_nm STRING,
  target_column_nm STRING,
  target_file_nm STRING,
  target_contrl_file_nm STRING,
  target_server_nm STRING NOT NULL,
  source_vl STRING NOT NULL,
  target_vl STRING NOT NULL,
  clcltn_ds STRING,
  excldd_vl STRING,
  excldd_reason_tx STRING,
  tolrnc_pc STRING,
  rcncln_exact_pass_in STRING NOT NULL,
  rcncln_tolrnc_pass_in STRING,
  latest_source_parttn_dt STRING NOT NULL,
  latest_target_parttn_dt STRING NOT NULL,
  load_ts STRING NOT NULL,
  schdld_dt DATE NOT NULL,
  source_system_id STRING NOT NULL,
  schdld_yr INT64 NOT NULL,
  Job_Name STRING NOT NULL
)
PARTITION BY schdld_dt
CLUSTER BY source_system_id;
```

### 11.3 DQ Results Schema (BigQuery)

```sql
CREATE TABLE `project.dataset.dq_results` (
  check_id STRING NOT NULL,
  category STRING NOT NULL,
  description STRING,
  severity STRING NOT NULL,
  sql_query STRING NOT NULL,
  expected_output STRING,
  actual_result STRING,
  comparison_type STRING NOT NULL,
  validation_status STRING NOT NULL,
  impacted_downstream ARRAY<STRING>,
  tags ARRAY<STRING>,
  execution_timestamp TIMESTAMP NOT NULL,
  error_message STRING,
  execution_duration FLOAT64,
  run_dt STRING NOT NULL,
  partition_dt STRING NOT NULL
)
PARTITION BY DATE(PARSE_DATE('%Y-%m-%d', partition_dt))
CLUSTER BY check_id, validation_status;
```


### 11.4 Sample Orchestration (Airflow DAG)

```python
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator
)
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'metrics_pipeline',
    default_args=default_args,
    description='Daily metrics pipeline',
    schedule_interval='0 2 * * *',  # 2 AM daily
    catchup=False,
)

# Metrics pipeline task
metrics_job = {
    'reference': {'project_id': 'your-project'},
    'placement': {'cluster_name': 'your-cluster'},
    'pyspark_job': {
        'main_python_file_uri': 'gs://bucket/scripts/main.py',
        'args': [
            '--mode', 'metrics',
            '--gcs_path', 'gs://bucket/config/metrics.json',
            '--run_date', '{{ ds }}',
            '--dependencies', 'dep1,dep2',
            '--partition_info_table', 'project.dataset.partition_info',
            '--env', 'PRD',
            '--recon_table', 'project.dataset.recon_table',
        ],
    },
}

submit_metrics = DataprocSubmitJobOperator(
    task_id='submit_metrics_pipeline',
    job=metrics_job,
    region='europe-west2',
    project_id='your-project',
    dag=dag,
)

# DQ pipeline task
dq_job = {
    'reference': {'project_id': 'your-project'},
    'placement': {'cluster_name': 'your-cluster'},
    'pyspark_job': {
        'main_python_file_uri': 'gs://bucket/scripts/main.py',
        'args': [
            '--mode', 'dq',
            '--gcs_path', 'gs://bucket/config/dq.json',
            '--run_date', '{{ ds }}',
            '--dq_target_table', 'project.dataset.dq_results',
            '--dq_partition_info_table', 'project.dataset.partition_info',
        ],
    },
}

submit_dq = DataprocSubmitJobOperator(
    task_id='submit_dq_pipeline',
    job=dq_job,
    region='europe-west2',
    project_id='your-project',
    dag=dag,
)

# Set dependencies
submit_metrics >> submit_dq
```

### 11.5 Monitoring Queries

**Check Recent Metrics:**
```sql
SELECT 
  partition_dt,
  COUNT(DISTINCT metric_id) as metric_count,
  COUNT(*) as record_count
FROM `project.dataset.metrics_output`
WHERE partition_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY partition_dt
ORDER BY partition_dt DESC;
```

**Check Recon Status:**
```sql
SELECT 
  schdld_dt,
  rcncln_exact_pass_in,
  COUNT(*) as count
FROM `project.dataset.recon_table`
WHERE schdld_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY schdld_dt, rcncln_exact_pass_in
ORDER BY schdld_dt DESC, rcncln_exact_pass_in;
```

**Check DQ Results:**
```sql
SELECT 
  partition_dt,
  validation_status,
  severity,
  COUNT(*) as check_count
FROM `project.dataset.dq_results`
WHERE partition_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY partition_dt, validation_status, severity
ORDER BY partition_dt DESC, severity, validation_status;
```

**Find Failed Metrics:**
```sql
SELECT 
  source_system_id as metric_id,
  Job_Name as metric_name,
  excldd_reason_tx as error_message,
  schdld_dt
FROM `project.dataset.recon_table`
WHERE rcncln_exact_pass_in = 'Failed'
  AND schdld_dt = CURRENT_DATE()
ORDER BY source_system_id;
```

**Find Failed DQ Checks:**
```sql
SELECT 
  check_id,
  description,
  severity,
  error_message,
  execution_timestamp
FROM `project.dataset.dq_results`
WHERE validation_status = 'FAIL'
  AND partition_dt = CURRENT_DATE()
ORDER BY severity DESC, check_id;
```

---

## 12. Contact and Support

### 12.1 Development Team

- **Primary Contact:** [Your Name/Team]
- **Email:** [team-email@company.com]
- **Slack Channel:** #data-pipeline-support

### 12.2 Documentation Updates

This documentation should be updated when:
- New features are added
- Configuration changes are made
- Known issues are discovered or resolved
- Performance optimizations are implemented

**Last Updated:** 2024  
**Version:** 1.0  
**Next Review Date:** [Set appropriate date]

---

## 13. Glossary

| Term | Definition |
|------|------------|
| **Dependency** | A logical grouping of metrics that can be processed together |
| **Partition Date** | The date used to partition data in BigQuery tables |
| **Recon Record** | Reconciliation record tracking metric execution status |
| **DQ Check** | Data Quality validation check |
| **Placeholder** | Dynamic value in SQL ({currently}, {partition_info}) |
| **Target Table** | BigQuery table where metrics are written |
| **Comparison Type** | Method used to validate DQ check results |
| **Overwrite** | Delete existing records and write new ones |
| **GCS** | Google Cloud Storage |
| **BQ** | BigQuery |

---

**END OF DOCUMENTATION**

For questions or clarifications, please contact the development team.

