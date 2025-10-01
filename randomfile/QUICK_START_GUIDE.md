# Quick Start Guide - Flexible Athena Table Generator

Get started in 5 minutes with creating Athena tables with STRUCT support!

## Prerequisites

```bash
# Install dependencies
pip install pandas numpy pyarrow boto3

# Configure AWS credentials
aws configure
```

## Quick Examples

### 1. Simple Table (30 seconds)

```bash
python athena_flexible_table_generator.py \
  --s3-bucket my-bucket \
  --s3-prefix data/test \
  --database test_db \
  --athena-output s3://my-bucket/athena-results/ \
  --table-name users \
  --schema "id:INT,name:STRING,email:STRING" \
  --format parquet \
  --size small
```

**What this does:**
- Creates `test_db` database (if not exists)
- Generates 10,000 rows of user data
- Uploads to `s3://my-bucket/data/test/users/`
- Creates Athena table `test_db.users`

**Query it:**
```sql
SELECT * FROM test_db.users LIMIT 10;
```

### 2. Table with STRUCT (1 minute)

```bash
python athena_flexible_table_generator.py \
  --s3-bucket my-bucket \
  --s3-prefix data/customers \
  --database test_db \
  --athena-output s3://my-bucket/athena-results/ \
  --table-name customers \
  --schema "id:INT,name:STRING,contact:STRUCT<email:STRING,phone:STRING>" \
  --format parquet \
  --size small
```

**Query the STRUCT:**
```sql
SELECT 
  id,
  name,
  contact.email,
  contact.phone
FROM test_db.customers
LIMIT 10;
```

### 3. Partitioned Table (2 minutes)

```bash
python athena_flexible_table_generator.py \
  --s3-bucket my-bucket \
  --s3-prefix data/orders \
  --database test_db \
  --athena-output s3://my-bucket/athena-results/ \
  --table-name orders \
  --schema "order_id:BIGINT,amount:DECIMAL(10,2),status:STRING" \
  --partitions "year,month" \
  --format parquet \
  --size medium
```

**Query with partitions:**
```sql
SELECT * 
FROM test_db.orders 
WHERE year = 2024 AND month = 10
LIMIT 10;
```

## Common Use Cases

### Use Case 1: Customer Data with Address

```bash
python athena_flexible_table_generator.py \
  --s3-bucket my-bucket \
  --s3-prefix data/customers \
  --database crm \
  --athena-output s3://my-bucket/athena-results/ \
  --table-name customer_profiles \
  --schema "customer_id:BIGINT,name:STRING,email:STRING,address:STRUCT<street:STRING,city:STRING,state:CHAR(2),zip:INT>" \
  --format parquet \
  --size medium
```

### Use Case 2: E-commerce Orders

```bash
python athena_flexible_table_generator.py \
  --s3-bucket my-bucket \
  --s3-prefix data/orders \
  --database ecommerce \
  --athena-output s3://my-bucket/athena-results/ \
  --table-name orders \
  --schema "order_id:BIGINT,customer:STRUCT<id:INT,name:STRING>,items:ARRAY<STRING>,total:DECIMAL(10,2)" \
  --partitions "year,month" \
  --format parquet \
  --size large
```

### Use Case 3: IoT Sensor Data

```bash
python athena_flexible_table_generator.py \
  --s3-bucket my-bucket \
  --s3-prefix data/sensors \
  --database iot \
  --athena-output s3://my-bucket/athena-results/ \
  --table-name telemetry \
  --schema "device_id:STRING,timestamp:TIMESTAMP,location:STRUCT<lat:DOUBLE,lon:DOUBLE>,readings:MAP<STRING,DOUBLE>" \
  --partitions "year,month,day" \
  --format parquet \
  --size large
```

## Parameter Reference

| Parameter | Required | Values | Default |
|-----------|----------|--------|---------|
| `--s3-bucket` | ✅ Yes | Bucket name | - |
| `--s3-prefix` | ✅ Yes | S3 path | - |
| `--database` | ✅ Yes | Database name | - |
| `--athena-output` | ✅ Yes | S3 path | - |
| `--table-name` | ✅ Yes | Table name | - |
| `--schema` | ✅ Yes | See schema format | - |
| `--format` | ❌ No | `parquet`, `csv`, `json` | `parquet` |
| `--size` | ❌ No | `small`, `medium`, `large` | `small` |
| `--partitions` | ❌ No | Comma-separated | None |

### Data Sizes

| Size | Rows | Best For |
|------|------|----------|
| `small` | 10,000 | Testing, development |
| `medium` | 100,000 | Small production datasets |
| `large` | 5,000,000 | Large production datasets |

### Schema Format

Basic format: `column1:TYPE1,column2:TYPE2,...`

**Common types:**
- `INT`, `BIGINT`, `TINYINT`, `SMALLINT`
- `STRING`, `VARCHAR(n)`, `CHAR(n)`
- `DECIMAL(p,s)` - e.g., `DECIMAL(10,2)`
- `DATE`, `TIMESTAMP`
- `BOOLEAN`
- `ARRAY<type>` - e.g., `ARRAY<STRING>`
- `MAP<k,v>` - e.g., `MAP<STRING,INT>`
- `STRUCT<f1:t1,f2:t2>` - e.g., `STRUCT<name:STRING,age:INT>`

## Tips

1. **Start Small**: Always test with `--size small` first

2. **Use Parquet**: Best performance for Athena queries
   ```bash
   --format parquet
   ```

3. **Partition Large Tables**: Use year/month for time-series data
   ```bash
   --partitions "year,month"
   ```

4. **Quote Schema**: Always quote the schema parameter
   ```bash
   --schema "id:INT,name:STRING"
   ```

5. **Check AWS Costs**: Be mindful of S3 storage and Athena query costs

## Troubleshooting

### Error: "No AWS credentials found"
```bash
aws configure
# or
export AWS_ACCESS_KEY_ID=xxx
export AWS_SECRET_ACCESS_KEY=yyy
```

### Error: "Permission denied for S3"
Ensure your IAM user/role has:
- `s3:PutObject`
- `s3:GetObject`
- `s3:ListBucket`

### Error: "Athena query failed"
Ensure your IAM user/role has:
- `athena:StartQueryExecution`
- `athena:GetQueryExecution`
- `glue:CreateDatabase`
- `glue:CreateTable`

### Error: "Schema parsing failed"
- Check bracket matching: `STRUCT<...>`
- Use commas between fields
- Quote in shell: `--schema "..."`

## Next Steps

1. **Read Full Documentation**: See `FLEXIBLE_TABLE_GENERATOR_README.md`

2. **STRUCT Examples**: Check `STRUCT_SCHEMA_EXAMPLES.md` for complex schemas

3. **Run Examples**: Use `example_usage.sh` for pre-built examples

4. **Programmatic Usage**: See `example_programmatic_usage.py` for Python API

5. **Query Your Data**: Open AWS Athena console and start querying!

## Help

```bash
# Show all options
python athena_flexible_table_generator.py --help
```

## Files in This Package

| File | Purpose |
|------|---------|
| `athena_flexible_table_generator.py` | Main script |
| `QUICK_START_GUIDE.md` | This file - quick start |
| `FLEXIBLE_TABLE_GENERATOR_README.md` | Full documentation |
| `STRUCT_SCHEMA_EXAMPLES.md` | STRUCT schema examples |
| `example_usage.sh` | Bash script with examples |
| `example_programmatic_usage.py` | Python API examples |

## Support Data Types

✅ All Athena data types supported:
- **Primitives**: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL, CHAR, VARCHAR, STRING, BINARY, DATE, TIMESTAMP
- **Complex**: ARRAY, MAP, STRUCT (including nested)

## Quick Reference Card

```bash
# Basic table
python athena_flexible_table_generator.py \
  --s3-bucket BUCKET \
  --s3-prefix PREFIX \
  --database DB \
  --athena-output s3://BUCKET/results/ \
  --table-name TABLE \
  --schema "col1:TYPE1,col2:TYPE2" \
  --format parquet \
  --size small

# With STRUCT
--schema "id:INT,data:STRUCT<field1:STRING,field2:INT>"

# With partition
--partitions "year,month"

# Nested STRUCT
--schema "id:INT,addr:STRUCT<city:STRING,loc:STRUCT<lat:DOUBLE,lon:DOUBLE>>"

# With ARRAY
--schema "id:INT,tags:ARRAY<STRING>"

# With MAP
--schema "id:INT,props:MAP<STRING,STRING>"
```

---

**Happy querying!** 🚀

