# Comprehensive Athena Data Type Generator

This advanced data generator creates sample data covering **ALL AWS Athena supported data types** with realistic patterns and multiple formats.

## Features

- ✅ **Complete Data Type Coverage**: All Athena primitive and complex types (except STRUCT)
- ✅ **Multiple Formats**: Parquet, CSV, JSON with proper type handling
- ✅ **Realistic Data**: Business domain-specific datasets (e-commerce, financial, IoT)
- ✅ **SDV Support**: Optional integration with Synthetic Data Vault for advanced patterns
- ✅ **Partitioned Tables**: Time-based partitioning for large datasets
- ✅ **Production Ready**: Proper null handling, data distributions, and edge cases

## Athena Data Types Covered

### Primitive Types
- **BOOLEAN**: True/False values with nulls
- **TINYINT**: 8-bit integers (-128 to 127)
- **SMALLINT**: 16-bit integers (-32,768 to 32,767)
- **INT/INTEGER**: 32-bit integers
- **BIGINT**: 64-bit integers
- **FLOAT**: 32-bit floating point
- **DOUBLE**: 64-bit floating point
- **DECIMAL(precision, scale)**: Fixed-point numbers with configurable precision
- **CHAR(n)**: Fixed-length strings
- **VARCHAR(n)**: Variable-length strings
- **STRING**: Unbounded strings
- **BINARY**: Binary data (base64 encoded for storage)
- **DATE**: Date values (2020-2024 range)
- **TIMESTAMP**: Date and time with microsecond precision
- **INTERVAL**: Time intervals (stored as strings)

### Complex Types
- **ARRAY<T>**: Arrays of integers, strings, and floats
- **MAP<K,V>**: Key-value pairs with mixed types
- **STRUCT**: TODO - implement STRUCT support

## Installation

```bash
# Required packages
pip install pandas numpy pyarrow boto3

# Optional: For advanced synthetic data generation
pip install sdv
```

## Usage

```bash
python athena_comprehensive_data_generator.py \
    --s3-bucket your-bucket \
    --s3-prefix data/athena-types \
    --database-name athena_test_db
```

## Generated Tables

### 1. All Data Types Tables

#### `all_types_tiny_parquet` (100 rows)
Comprehensive table with every Athena data type for quick testing.

#### `all_types_small_parquet` (1,000 rows)
Small dataset with all data types in Parquet format.

#### `all_types_medium_csv` (10,000 rows)
Medium dataset in CSV format with JSON-encoded complex types.

#### `all_types_small_json` (1,000 rows)
JSON format with proper type serialization.

### 2. E-commerce Domain Tables

#### `ecommerce_orders_small_parquet` (1,000 rows, partitioned)
```sql
order_id            STRING
customer_id         BIGINT
order_date          DATE
order_timestamp     TIMESTAMP
order_amount        DOUBLE
discount_percent    BIGINT
shipping_cost       DOUBLE
is_prime_member     BOOLEAN
payment_method      STRING
order_status        STRING
product_categories  ARRAY<STRING>
order_tags          MAP<STRING,STRING>
customer_tier       STRING
warehouse_code      CHAR(5)
tracking_number     VARCHAR(20)
notes               STRING
loyalty_points      BIGINT
items_count         TINYINT
total_weight_kg     DOUBLE
-- Partitioned by: year INT, month INT
```

#### `ecommerce_orders_medium_parquet` (10,000 rows, partitioned)
Medium-sized e-commerce dataset for performance testing.

### 3. Financial Domain Tables

#### `financial_transactions_medium_parquet` (10,000 rows, partitioned)
```sql
transaction_id      STRING
account_number      CHAR(12)
transaction_date    DATE
transaction_timestamp TIMESTAMP
transaction_amount  DECIMAL(12,2)
balance_before      DECIMAL(15,2)
balance_after       DECIMAL(15,2)
transaction_type    STRING
merchant_name       VARCHAR(50)
merchant_category   STRING
is_international    BOOLEAN
currency_code       CHAR(3)
exchange_rate       DOUBLE
risk_score          TINYINT
fraud_flag          BOOLEAN
processing_time_ms  INT
transaction_metadata MAP<STRING,STRING>
-- Partitioned by: year INT, month INT
```

### 4. IoT Sensor Data Tables

#### `iot_sensor_data_large_parquet` (100,000 rows, partitioned)
```sql
device_id           STRING
sensor_timestamp    TIMESTAMP
temperature_c       DOUBLE
humidity_percent    TINYINT
pressure_hpa        DOUBLE
battery_level       TINYINT
signal_strength_dbm TINYINT
is_online           BOOLEAN
firmware_version    CHAR(8)
location_lat        DOUBLE
location_lon        DOUBLE
error_codes         ARRAY<INT>
sensor_readings     MAP<STRING,STRING>
maintenance_due     DATE
uptime_hours        INT
-- Partitioned by: year INT, month INT, day INT
```

## Data Characteristics

### Numeric Types
- **Integer ranges**: Properly bounded to type limits
- **Decimals**: High precision financial calculations
- **Floats/Doubles**: Scientific and measurement data
- **Distributions**: Normal, uniform, and log-normal as appropriate

### String Types
- **CHAR**: Fixed-length codes and identifiers
- **VARCHAR**: Variable length with realistic patterns
- **STRING**: Free text with varying lengths
- **Patterns**: Emails, IDs, addresses, descriptions

### Temporal Types
- **DATE**: 2020-2024 range with business patterns
- **TIMESTAMP**: Microsecond precision with timezone awareness
- **INTERVAL**: Various time spans (days, hours, months)

### Complex Types
- **ARRAY**: Variable length (1-10 elements)
- **MAP**: Dynamic key-value pairs (1-5 pairs)
- **Nested data**: JSON-compatible structures

### Data Quality
- **Null values**: 5% randomly distributed
- **Edge cases**: Min/max values for numeric types
- **Realistic patterns**: Business-appropriate distributions
- **Referential integrity**: Consistent IDs across related data

## Output Structure

```
s3://your-bucket/data/athena-types/
├── parquet/
│   ├── all_types_tiny_parquet/
│   ├── all_types_small_parquet/
│   ├── ecommerce_orders_small_parquet/
│   │   ├── year=2022/
│   │   │   ├── month=1/
│   │   │   └── month=2/
│   │   └── year=2023/
│   ├── financial_transactions_medium_parquet/
│   └── iot_sensor_data_large_parquet/
├── csv/
│   ├── all_types_medium_csv/
│   └── financial_transactions_small_csv/
└── json/
    ├── all_types_small_json/
    └── iot_sensor_data_small_json/
```

## DDL Generation

The script automatically generates `athena_tables_ddl.sql` with:
- Database creation statement
- CREATE TABLE statements for all tables
- Proper type mappings
- Partition definitions
- MSCK REPAIR commands for partitioned tables

## Testing with Iceberg Migration

These tables are perfect for testing your Iceberg migration because:

1. **Type Coverage**: Tests all data type conversions
2. **Size Variety**: From 100 to 100K rows for performance testing
3. **Format Testing**: Parquet, CSV, and JSON source formats
4. **Partition Testing**: Both partitioned and non-partitioned tables
5. **Complex Types**: Arrays and maps for advanced scenarios
6. **Business Logic**: Realistic data patterns and relationships

### Example Migration Test

```bash
# Test with small all-types table
aws glue start-job-run --job-name iceberg-unified-migration --arguments '{
    "--MIGRATION_TYPE": "newtable",
    "--SOURCE_DATABASE": "athena_test_db",
    "--SOURCE_TABLE": "all_types_small_parquet",
    "--TARGET_DATABASE": "athena_test_db",
    "--TARGET_TABLE": "all_types_small_iceberg",
    "--TARGET_S3_LOCATION": "s3://your-bucket/iceberg/all_types_small/"
}'

# Test with partitioned e-commerce data
aws glue start-job-run --job-name iceberg-unified-migration --arguments '{
    "--MIGRATION_TYPE": "inplace",
    "--SOURCE_DATABASE": "athena_test_db",
    "--SOURCE_TABLE": "ecommerce_orders_medium_parquet",
    "--TARGET_DATABASE": "athena_test_db",
    "--TARGET_TABLE": "ecommerce_orders_medium_parquet",
    "--TEMP_S3_LOCATION": "s3://your-bucket/temp/"
}'
```

## Validation Points

After migration, validate:

1. **Data Types**: All types converted correctly
2. **Null Handling**: NULL values preserved
3. **Complex Types**: Arrays and maps intact
4. **Decimal Precision**: Financial calculations accurate
5. **Binary Data**: Base64 encoding handled
6. **Partitions**: Time-based partitions maintained
7. **Row Counts**: No data loss
8. **Query Performance**: Improved with Iceberg

## Customization

### Add New Data Types
Edit `generate_comprehensive_dataframe()` to add custom columns.

### Modify Data Patterns
Adjust the generator functions for different distributions.

### Change Table Sizes
Modify `self.row_counts` dictionary for different scales.

### Add Business Domains
Create new domain-specific generators in `generate_business_specific_data()`.

## Troubleshooting

1. **Memory Issues**: For large datasets, run on EC2 with sufficient RAM
2. **S3 Permissions**: Ensure proper IAM roles for S3 write access
3. **Type Serialization**: Complex types are JSON-encoded for CSV/JSON
4. **Binary Data**: Automatically base64 encoded for compatibility

## Next Steps

1. Run the generator to create test data
2. Execute the DDL statements in Athena
3. Test Iceberg migration with various table types
4. Validate the comprehensive validation framework
5. Measure performance improvements

This generator provides the most comprehensive test dataset for validating your Iceberg migration across all Athena data types and real-world scenarios!
