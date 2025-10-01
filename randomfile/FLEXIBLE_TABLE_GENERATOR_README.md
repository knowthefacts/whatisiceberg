# Flexible Athena Table Generator with STRUCT Support

A powerful Python tool for creating AWS Athena tables with custom schemas, including support for complex nested data types like STRUCT, ARRAY, and MAP.

## Features

- ✅ **Full STRUCT Support**: Create deeply nested STRUCT types
- ✅ **All Athena Data Types**: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL, CHAR, VARCHAR, STRING, BINARY, DATE, TIMESTAMP, ARRAY, MAP, STRUCT
- ✅ **Multiple Formats**: CSV, Parquet, JSON
- ✅ **Configurable Sizes**: Small (10K), Medium (100K), Large (5M rows)
- ✅ **Partitioning Support**: Create partitioned tables automatically
- ✅ **Automatic Table Creation**: Creates database, table, uploads data, and discovers partitions

## Installation

```bash
pip install pandas numpy pyarrow boto3
```

## AWS Configuration

Ensure AWS credentials are configured:
```bash
aws configure
```

Or set environment variables:
```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_DEFAULT_REGION=us-east-1
```

## Usage

### Basic Syntax

```bash
python athena_flexible_table_generator.py \
  --s3-bucket <BUCKET> \
  --s3-prefix <PREFIX> \
  --database <DATABASE_NAME> \
  --athena-output <S3_OUTPUT_LOCATION> \
  --table-name <TABLE_NAME> \
  --schema "<SCHEMA_DEFINITION>" \
  --format <csv|parquet|json> \
  --size <small|medium|large> \
  [--partitions <PARTITION_COLUMNS>]
```

### Parameters

| Parameter | Required | Description | Example |
|-----------|----------|-------------|---------|
| `--s3-bucket` | Yes | S3 bucket name | `my-data-bucket` |
| `--s3-prefix` | Yes | S3 prefix path | `data/athena/tables` |
| `--database` | Yes | Athena database name | `test_database` |
| `--athena-output` | Yes | S3 location for Athena query results | `s3://my-bucket/athena-results/` |
| `--table-name` | Yes | Table name | `customers` |
| `--schema` | Yes | Column definitions | See schema format below |
| `--format` | No | File format (default: parquet) | `parquet`, `csv`, `json` |
| `--size` | No | Data size (default: small) | `small`, `medium`, `large` |
| `--partitions` | No | Comma-separated partition columns | `year,month` |

### Schema Format

Schema is defined as: `"column1:TYPE1,column2:TYPE2,..."`

**Important**: For complex types (STRUCT, ARRAY, MAP), use proper nesting syntax.

## Examples

### 1. Simple Table with Basic Types

```bash
python athena_flexible_table_generator.py \
  --s3-bucket my-data-bucket \
  --s3-prefix data/users \
  --database analytics \
  --athena-output s3://my-data-bucket/athena-results/ \
  --table-name users \
  --schema "id:INT,name:STRING,age:TINYINT,email:VARCHAR(100),created_date:DATE" \
  --format parquet \
  --size small
```

**Generated Table**:
- 10,000 rows
- Columns: id (INT), name (STRING), age (TINYINT), email (VARCHAR(100)), created_date (DATE)
- Format: Parquet
- No partitions

### 2. Table with STRUCT Type

```bash
python athena_flexible_table_generator.py \
  --s3-bucket my-data-bucket \
  --s3-prefix data/customers \
  --database analytics \
  --athena-output s3://my-data-bucket/athena-results/ \
  --table-name customers \
  --schema "customer_id:BIGINT,name:STRING,contact:STRUCT<email:STRING,phone:STRING>,created_at:TIMESTAMP" \
  --format parquet \
  --size medium
```

**Generated Table**:
- 100,000 rows
- Includes STRUCT column `contact` with nested fields `email` and `phone`
- Example data: `contact: {email: "user@example.com", phone: "555-0123"}`

### 3. Nested STRUCT (Struct within Struct)

```bash
python athena_flexible_table_generator.py \
  --s3-bucket my-data-bucket \
  --s3-prefix data/customers \
  --database analytics \
  --athena-output s3://my-data-bucket/athena-results/ \
  --table-name customer_profiles \
  --schema "id:BIGINT,name:STRING,address:STRUCT<street:STRING,city:STRING,zip:INT,location:STRUCT<lat:DOUBLE,lon:DOUBLE>>" \
  --format parquet \
  --size small
```

**Generated Table**:
- STRUCT with nested STRUCT
- `address.location.lat` and `address.location.lon` are accessible

### 4. Table with ARRAY Type

```bash
python athena_flexible_table_generator.py \
  --s3-bucket my-data-bucket \
  --s3-prefix data/orders \
  --database analytics \
  --athena-output s3://my-data-bucket/athena-results/ \
  --table-name orders \
  --schema "order_id:BIGINT,customer_id:INT,product_ids:ARRAY<INT>,tags:ARRAY<STRING>,amount:DECIMAL(10,2)" \
  --format parquet \
  --size medium
```

**Generated Table**:
- Arrays of integers and strings
- Example: `product_ids: [101, 205, 303]`, `tags: ["electronics", "sale"]`

### 5. Table with MAP Type

```bash
python athena_flexible_table_generator.py \
  --s3-bucket my-data-bucket \
  --s3-prefix data/events \
  --database analytics \
  --athena-output s3://my-data-bucket/athena-results/ \
  --table-name user_events \
  --schema "event_id:STRING,user_id:INT,event_type:STRING,properties:MAP<STRING,STRING>,metrics:MAP<STRING,DOUBLE>" \
  --format json \
  --size small
```

**Generated Table**:
- MAP columns for key-value pairs
- Example: `properties: {"browser": "chrome", "os": "windows"}`

### 6. Partitioned Table

```bash
python athena_flexible_table_generator.py \
  --s3-bucket my-data-bucket \
  --s3-prefix data/transactions \
  --database analytics \
  --athena-output s3://my-data-bucket/athena-results/ \
  --table-name daily_transactions \
  --schema "transaction_id:BIGINT,amount:DECIMAL(12,2),status:STRING,customer_id:INT" \
  --partitions "year,month,day" \
  --format parquet \
  --size large
```

**Generated Table**:
- 5,000,000 rows
- Partitioned by year, month, day
- S3 structure: `s3://bucket/prefix/year=2023/month=05/day=15/`
- Automatically runs `MSCK REPAIR TABLE` to discover partitions

### 7. Complex Nested Structure

```bash
python athena_flexible_table_generator.py \
  --s3-bucket my-data-bucket \
  --s3-prefix data/events \
  --database analytics \
  --athena-output s3://my-data-bucket/athena-results/ \
  --table-name complex_events \
  --schema "event_id:STRING,timestamp:TIMESTAMP,user:STRUCT<id:INT,name:STRING,profile:STRUCT<age:INT,country:STRING>>,items:ARRAY<STRING>,metadata:MAP<STRING,STRING>" \
  --format parquet \
  --size medium
```

**Generated Table**:
- Nested STRUCT (user.profile)
- ARRAY of strings
- MAP of string keys to string values
- All in one table

### 8. E-commerce Order with Multiple Complex Types

```bash
python athena_flexible_table_generator.py \
  --s3-bucket my-data-bucket \
  --s3-prefix data/ecommerce \
  --database analytics \
  --athena-output s3://my-data-bucket/athena-results/ \
  --table-name order_details \
  --schema "order_id:BIGINT,order_date:DATE,customer:STRUCT<id:INT,name:STRING,email:STRING>,shipping_address:STRUCT<street:STRING,city:STRING,state:STRING,zip:INT>,items:ARRAY<STRING>,payment:STRUCT<method:STRING,amount:DECIMAL(10,2),currency:CHAR(3)>,tags:MAP<STRING,STRING>" \
  --partitions "year,month" \
  --format parquet \
  --size large
```

**Generated Table**:
- Multiple STRUCT columns
- ARRAY column
- MAP column
- Partitioned by year and month
- 5,000,000 rows

### 9. IoT Sensor Data

```bash
python athena_flexible_table_generator.py \
  --s3-bucket my-data-bucket \
  --s3-prefix data/iot \
  --database analytics \
  --athena-output s3://my-data-bucket/athena-results/ \
  --table-name sensor_readings \
  --schema "device_id:STRING,timestamp:TIMESTAMP,location:STRUCT<lat:DOUBLE,lon:DOUBLE>,readings:MAP<STRING,DOUBLE>,alerts:ARRAY<STRING>,metadata:STRUCT<firmware:STRING,battery:TINYINT>" \
  --partitions "year,month,day" \
  --format parquet \
  --size large
```

### 10. CSV Format with Partitions

```bash
python athena_flexible_table_generator.py \
  --s3-bucket my-data-bucket \
  --s3-prefix data/sales \
  --database analytics \
  --athena-output s3://my-data-bucket/athena-results/ \
  --table-name sales_data \
  --schema "sale_id:BIGINT,product_name:STRING,quantity:INT,price:DECIMAL(10,2),sale_date:DATE" \
  --partitions "region" \
  --format csv \
  --size medium
```

**Note**: CSV format has limited support for complex types (STRUCT, ARRAY, MAP are serialized as JSON strings).

## Supported Athena Data Types

### Primitive Types

| Type | Syntax | Example |
|------|--------|---------|
| BOOLEAN | `BOOLEAN` | `is_active:BOOLEAN` |
| TINYINT | `TINYINT` | `age:TINYINT` |
| SMALLINT | `SMALLINT` | `count:SMALLINT` |
| INT/INTEGER | `INT` or `INTEGER` | `id:INT` |
| BIGINT | `BIGINT` | `transaction_id:BIGINT` |
| FLOAT | `FLOAT` | `score:FLOAT` |
| DOUBLE | `DOUBLE` | `latitude:DOUBLE` |
| DECIMAL | `DECIMAL(p,s)` | `amount:DECIMAL(10,2)` |
| CHAR | `CHAR(n)` | `code:CHAR(5)` |
| VARCHAR | `VARCHAR(n)` | `email:VARCHAR(100)` |
| STRING | `STRING` | `description:STRING` |
| BINARY | `BINARY` | `data:BINARY` |
| DATE | `DATE` | `created_date:DATE` |
| TIMESTAMP | `TIMESTAMP` | `created_at:TIMESTAMP` |

### Complex Types

| Type | Syntax | Example |
|------|--------|---------|
| ARRAY | `ARRAY<type>` | `tags:ARRAY<STRING>` |
| MAP | `MAP<key_type,value_type>` | `properties:MAP<STRING,INT>` |
| STRUCT | `STRUCT<field1:type1,field2:type2>` | `address:STRUCT<city:STRING,zip:INT>` |

### Nested Complex Types

```
# Array of structs
products:ARRAY<STRUCT<id:INT,name:STRING>>

# Struct with array
user:STRUCT<id:INT,tags:ARRAY<STRING>>

# Struct with nested struct
person:STRUCT<name:STRING,address:STRUCT<city:STRING,zip:INT>>

# Map with struct values
data:MAP<STRING,STRUCT<count:INT,total:DOUBLE>>
```

## Partition Column Support

The tool automatically generates appropriate values for common partition columns:

| Partition Column | Generated Values |
|-----------------|------------------|
| `year` | 2020-2024 |
| `month` | 1-12 |
| `day` | 1-28 |
| `region` | us-east, us-west, eu-west, ap-south |
| `category` | A, B, C, D |
| Custom | part1, part2, part3 (default) |

## Output

The tool performs the following actions:

1. **Generates Data**: Creates realistic synthetic data based on schema
2. **Saves Locally**: Temporarily saves to `/tmp/`
3. **Uploads to S3**: Uploads to specified S3 location
4. **Creates Database**: Executes `CREATE DATABASE IF NOT EXISTS`
5. **Creates Table**: Executes `CREATE TABLE` DDL in Athena
6. **Discovers Partitions**: If partitioned, runs `MSCK REPAIR TABLE`
7. **Cleanup**: Removes temporary local files

## Querying the Data

After table creation, query using Athena:

```sql
-- Simple query
SELECT * FROM analytics.users LIMIT 10;

-- Query STRUCT fields
SELECT 
  customer_id,
  name,
  contact.email,
  contact.phone
FROM analytics.customers
LIMIT 10;

-- Query nested STRUCT
SELECT 
  id,
  address.city,
  address.location.lat,
  address.location.lon
FROM analytics.customer_profiles
LIMIT 10;

-- Query ARRAY
SELECT 
  order_id,
  product_ids,
  CARDINALITY(product_ids) as num_products
FROM analytics.orders
WHERE CARDINALITY(product_ids) > 2;

-- Query MAP
SELECT 
  event_id,
  properties['browser'] as browser,
  properties['os'] as os
FROM analytics.user_events;

-- Query partitioned table
SELECT *
FROM analytics.daily_transactions
WHERE year = 2023 AND month = 5;
```

## File Formats

### Parquet (Recommended)
- ✅ Best performance
- ✅ Columnar storage
- ✅ Full support for all data types
- ✅ Compression (Snappy)
- ✅ Ideal for partitioning

### JSON
- ✅ Human-readable
- ✅ Good for nested structures
- ⚠️ Slower than Parquet
- ✅ Full support for complex types

### CSV
- ✅ Simple format
- ⚠️ Limited complex type support (serialized as JSON strings)
- ⚠️ Slower for large datasets
- ⚠️ No native partitioning support

## Troubleshooting

### Permission Errors

Ensure your AWS IAM user/role has these permissions:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket",
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "glue:CreateDatabase",
        "glue:CreateTable"
      ],
      "Resource": "*"
    }
  ]
}
```

### Schema Parsing Errors

- Ensure proper escaping in shell: use quotes around schema
- For complex nested types, verify bracket matching
- Use commas only at the top level (not inside STRUCT/ARRAY/MAP definitions)

### Large Dataset Generation

For `large` size (5M rows):
- Requires sufficient memory (recommend 4GB+ available)
- May take several minutes
- Parquet format recommended for efficiency

## Advanced Usage

### Custom Partition Values

Modify the `generate_dataframe` method to add custom partition logic for your use case.

### Custom Data Generators

Extend the `generate_data_for_type` method to add domain-specific data generation.

### Batch Table Creation

Create a script to generate multiple tables:

```bash
#!/bin/bash

# Create multiple tables
for table in users orders products; do
  python athena_flexible_table_generator.py \
    --s3-bucket my-bucket \
    --s3-prefix data/$table \
    --database mydb \
    --athena-output s3://my-bucket/results/ \
    --table-name $table \
    --schema "$(cat schemas/${table}_schema.txt)" \
    --format parquet \
    --size medium
done
```

## Performance Tips

1. **Use Parquet**: 10-100x faster queries than CSV/JSON
2. **Partition Large Tables**: Reduces scan time
3. **Use Appropriate Types**: TINYINT/SMALLINT for small numbers saves space
4. **Compress Data**: Parquet with Snappy compression (default)
5. **Limit Partitions**: Avoid over-partitioning (max 20K partitions per table)

## License

MIT License

## Support

For issues or questions, please check:
- AWS Athena Documentation: https://docs.aws.amazon.com/athena/
- PyArrow Documentation: https://arrow.apache.org/docs/python/

