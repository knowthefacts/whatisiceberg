#!/bin/bash

# Example Usage Script for Flexible Athena Table Generator
# This script demonstrates various use cases for creating Athena tables

# IMPORTANT: Update these variables with your AWS details
S3_BUCKET="your-bucket-name"
S3_PREFIX="athena-data/examples"
DATABASE="example_database"
ATHENA_OUTPUT="s3://${S3_BUCKET}/athena-results/"

echo "=========================================="
echo "Athena Flexible Table Generator Examples"
echo "=========================================="
echo ""
echo "Using:"
echo "  S3 Bucket: ${S3_BUCKET}"
echo "  S3 Prefix: ${S3_PREFIX}"
echo "  Database: ${DATABASE}"
echo "  Athena Output: ${ATHENA_OUTPUT}"
echo ""
echo "=========================================="

# Example 1: Simple table with basic types
echo ""
echo "Example 1: Simple Users Table"
echo "------------------------------"
python athena_flexible_table_generator.py \
  --s3-bucket "${S3_BUCKET}" \
  --s3-prefix "${S3_PREFIX}/users" \
  --database "${DATABASE}" \
  --athena-output "${ATHENA_OUTPUT}" \
  --table-name users_basic \
  --schema "user_id:INT,username:STRING,email:VARCHAR(100),age:TINYINT,signup_date:DATE,is_active:BOOLEAN" \
  --format parquet \
  --size small

# Example 2: Table with STRUCT - Customer Contact Info
echo ""
echo "Example 2: Customers with STRUCT (Contact Info)"
echo "------------------------------------------------"
python athena_flexible_table_generator.py \
  --s3-bucket "${S3_BUCKET}" \
  --s3-prefix "${S3_PREFIX}/customers" \
  --database "${DATABASE}" \
  --athena-output "${ATHENA_OUTPUT}" \
  --table-name customers_with_struct \
  --schema "customer_id:BIGINT,name:STRING,contact:STRUCT<email:STRING,phone:VARCHAR(20),preferred:STRING>,registration_date:TIMESTAMP" \
  --format parquet \
  --size small

# Example 3: Nested STRUCT - Address with Location
echo ""
echo "Example 3: Nested STRUCT (Address with GPS Location)"
echo "-----------------------------------------------------"
python athena_flexible_table_generator.py \
  --s3-bucket "${S3_BUCKET}" \
  --s3-prefix "${S3_PREFIX}/locations" \
  --database "${DATABASE}" \
  --athena-output "${ATHENA_OUTPUT}" \
  --table-name customer_addresses \
  --schema "customer_id:BIGINT,name:STRING,address:STRUCT<street:STRING,city:STRING,state:CHAR(2),zip:INT,coordinates:STRUCT<latitude:DOUBLE,longitude:DOUBLE>>" \
  --format parquet \
  --size small

# Example 4: Table with ARRAY - Product Tags
echo ""
echo "Example 4: Products with ARRAY (Tags)"
echo "--------------------------------------"
python athena_flexible_table_generator.py \
  --s3-bucket "${S3_BUCKET}" \
  --s3-prefix "${S3_PREFIX}/products" \
  --database "${DATABASE}" \
  --athena-output "${ATHENA_OUTPUT}" \
  --table-name products_with_tags \
  --schema "product_id:BIGINT,name:STRING,price:DECIMAL(10,2),categories:ARRAY<STRING>,feature_ids:ARRAY<INT>" \
  --format parquet \
  --size medium

# Example 5: Table with MAP - Event Properties
echo ""
echo "Example 5: Events with MAP (Properties)"
echo "----------------------------------------"
python athena_flexible_table_generator.py \
  --s3-bucket "${S3_BUCKET}" \
  --s3-prefix "${S3_PREFIX}/events" \
  --database "${DATABASE}" \
  --athena-output "${ATHENA_OUTPUT}" \
  --table-name user_events \
  --schema "event_id:STRING,user_id:BIGINT,event_type:STRING,event_time:TIMESTAMP,properties:MAP<STRING,STRING>,metrics:MAP<STRING,DOUBLE>" \
  --format json \
  --size small

# Example 6: Complex E-commerce Order
echo ""
echo "Example 6: E-commerce Orders (Complex Schema)"
echo "----------------------------------------------"
python athena_flexible_table_generator.py \
  --s3-bucket "${S3_BUCKET}" \
  --s3-prefix "${S3_PREFIX}/orders" \
  --database "${DATABASE}" \
  --athena-output "${ATHENA_OUTPUT}" \
  --table-name ecommerce_orders \
  --schema "order_id:BIGINT,order_date:DATE,customer:STRUCT<id:INT,name:STRING,email:STRING>,shipping:STRUCT<address:STRING,city:STRING,zip:INT,method:STRING>,items:ARRAY<STRING>,totals:STRUCT<subtotal:DECIMAL(10,2),tax:DECIMAL(10,2),total:DECIMAL(10,2)>,metadata:MAP<STRING,STRING>" \
  --partitions "year,month" \
  --format parquet \
  --size medium

# Example 7: IoT Sensor Data - Partitioned Time Series
echo ""
echo "Example 7: IoT Sensor Data (Partitioned by Date)"
echo "-------------------------------------------------"
python athena_flexible_table_generator.py \
  --s3-bucket "${S3_BUCKET}" \
  --s3-prefix "${S3_PREFIX}/iot_sensors" \
  --database "${DATABASE}" \
  --athena-output "${ATHENA_OUTPUT}" \
  --table-name sensor_telemetry \
  --schema "device_id:STRING,timestamp:TIMESTAMP,location:STRUCT<lat:DOUBLE,lon:DOUBLE,altitude:FLOAT>,sensors:MAP<STRING,DOUBLE>,alerts:ARRAY<STRING>,status:STRUCT<battery:TINYINT,signal:SMALLINT,online:BOOLEAN>" \
  --partitions "year,month,day" \
  --format parquet \
  --size large

# Example 8: Financial Transactions
echo ""
echo "Example 8: Financial Transactions (High Precision)"
echo "---------------------------------------------------"
python athena_flexible_table_generator.py \
  --s3-bucket "${S3_BUCKET}" \
  --s3-prefix "${S3_PREFIX}/transactions" \
  --database "${DATABASE}" \
  --athena-output "${ATHENA_OUTPUT}" \
  --table-name financial_transactions \
  --schema "transaction_id:BIGINT,account_number:CHAR(12),timestamp:TIMESTAMP,amount:DECIMAL(18,4),currency:CHAR(3),transaction_type:STRING,counterparty:STRUCT<name:STRING,account:STRING,bank:STRING>,fees:ARRAY<DECIMAL(10,2)>,metadata:MAP<STRING,STRING>" \
  --partitions "year,month" \
  --format parquet \
  --size medium

# Example 9: Social Media Posts
echo ""
echo "Example 9: Social Media Posts (Nested Structures)"
echo "--------------------------------------------------"
python athena_flexible_table_generator.py \
  --s3-bucket "${S3_BUCKET}" \
  --s3-prefix "${S3_PREFIX}/social" \
  --database "${DATABASE}" \
  --athena-output "${ATHENA_OUTPUT}" \
  --table-name social_posts \
  --schema "post_id:STRING,created_at:TIMESTAMP,author:STRUCT<user_id:BIGINT,username:STRING,verified:BOOLEAN>,content:STRING,engagement:STRUCT<likes:INT,shares:INT,comments:INT>,tags:ARRAY<STRING>,mentions:ARRAY<STRING>,media:ARRAY<STRUCT<type:STRING,url:STRING>>" \
  --format json \
  --size medium

# Example 10: CSV Format Example
echo ""
echo "Example 10: Simple CSV Format Table"
echo "------------------------------------"
python athena_flexible_table_generator.py \
  --s3-bucket "${S3_BUCKET}" \
  --s3-prefix "${S3_PREFIX}/csv_data" \
  --database "${DATABASE}" \
  --athena-output "${ATHENA_OUTPUT}" \
  --table-name sales_csv \
  --schema "sale_id:BIGINT,product_name:STRING,quantity:INT,unit_price:DECIMAL(10,2),sale_date:DATE,customer_id:INT" \
  --format csv \
  --size small

echo ""
echo "=========================================="
echo "All examples completed!"
echo "=========================================="
echo ""
echo "You can now query these tables in Athena:"
echo ""
echo "SELECT * FROM ${DATABASE}.users_basic LIMIT 10;"
echo "SELECT customer_id, contact.email FROM ${DATABASE}.customers_with_struct LIMIT 10;"
echo "SELECT customer_id, address.coordinates.latitude FROM ${DATABASE}.customer_addresses LIMIT 10;"
echo "SELECT product_id, categories FROM ${DATABASE}.products_with_tags LIMIT 10;"
echo "SELECT event_id, properties['key1'] FROM ${DATABASE}.user_events LIMIT 10;"
echo ""

