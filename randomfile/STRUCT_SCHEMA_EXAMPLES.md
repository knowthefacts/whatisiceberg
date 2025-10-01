# STRUCT Schema Examples for Athena

Quick reference guide for common STRUCT patterns in the Flexible Athena Table Generator.

## Basic STRUCT Patterns

### 1. Contact Information

```
contact:STRUCT<email:STRING,phone:STRING,fax:STRING>
```

**Query**:
```sql
SELECT 
  customer_id,
  contact.email,
  contact.phone
FROM table_name;
```

### 2. Address

```
address:STRUCT<street:STRING,city:STRING,state:CHAR(2),zip:INT,country:STRING>
```

**Query**:
```sql
SELECT 
  customer_id,
  address.city,
  address.state
FROM table_name
WHERE address.country = 'USA';
```

### 3. Person/User Info

```
user:STRUCT<id:BIGINT,username:STRING,email:STRING,verified:BOOLEAN>
```

**Query**:
```sql
SELECT 
  post_id,
  user.username,
  user.verified
FROM table_name
WHERE user.verified = true;
```

### 4. Date Range

```
date_range:STRUCT<start_date:DATE,end_date:DATE,duration_days:INT>
```

**Query**:
```sql
SELECT 
  event_id,
  date_range.start_date,
  date_range.end_date
FROM table_name;
```

### 5. Monetary Amount

```
payment:STRUCT<amount:DECIMAL(10,2),currency:CHAR(3),method:STRING>
```

**Query**:
```sql
SELECT 
  order_id,
  payment.amount,
  payment.currency
FROM table_name
WHERE payment.method = 'CREDIT_CARD';
```

## Nested STRUCT Patterns

### 6. Address with GPS Coordinates

```
address:STRUCT<street:STRING,city:STRING,state:STRING,zip:INT,location:STRUCT<lat:DOUBLE,lon:DOUBLE>>
```

**Query**:
```sql
SELECT 
  property_id,
  address.city,
  address.location.lat,
  address.location.lon
FROM table_name;
```

### 7. User with Profile

```
user:STRUCT<id:INT,name:STRING,profile:STRUCT<age:INT,country:STRING,language:STRING>>
```

**Query**:
```sql
SELECT 
  user_id,
  user.name,
  user.profile.age,
  user.profile.country
FROM table_name
WHERE user.profile.age > 18;
```

### 8. Organization Hierarchy

```
organization:STRUCT<name:STRING,department:STRUCT<name:STRING,team:STRUCT<name:STRING,size:INT>>>
```

**Query**:
```sql
SELECT 
  employee_id,
  organization.name,
  organization.department.name,
  organization.department.team.name
FROM table_name;
```

### 9. Product with Pricing

```
product:STRUCT<id:BIGINT,name:STRING,pricing:STRUCT<base_price:DECIMAL(10,2),discount:DECIMAL(5,2),final_price:DECIMAL(10,2),currency:CHAR(3)>>
```

**Query**:
```sql
SELECT 
  order_id,
  product.name,
  product.pricing.final_price,
  product.pricing.currency
FROM table_name
WHERE product.pricing.discount > 0;
```

### 10. Transaction Details

```
transaction:STRUCT<id:STRING,timestamp:TIMESTAMP,details:STRUCT<type:STRING,status:STRING,amount:DECIMAL(12,4)>>
```

## STRUCT with ARRAY

### 11. User with Multiple Emails

```
user:STRUCT<id:INT,name:STRING,emails:ARRAY<STRING>>
```

**Query**:
```sql
SELECT 
  user_id,
  user.name,
  user.emails
FROM table_name;
```

### 12. Product with Reviews

```
product:STRUCT<id:BIGINT,name:STRING,reviews:ARRAY<STRUCT<rating:TINYINT,comment:STRING,date:DATE>>>
```

**Note**: Currently, ARRAY<STRUCT<...>> requires special handling. Simplify to:
```
product:STRUCT<id:BIGINT,name:STRING>,reviews:ARRAY<STRING>
```

### 13. Order with Line Items

```
order:STRUCT<order_id:BIGINT,date:DATE,total:DECIMAL(10,2)>,items:ARRAY<STRING>
```

## STRUCT with MAP

### 14. User with Preferences

```
user:STRUCT<id:INT,name:STRING>,preferences:MAP<STRING,STRING>
```

**Query**:
```sql
SELECT 
  user.name,
  preferences['theme'],
  preferences['language']
FROM table_name;
```

### 15. Device with Telemetry

```
device:STRUCT<id:STRING,model:STRING,firmware:STRING>,telemetry:MAP<STRING,DOUBLE>
```

**Query**:
```sql
SELECT 
  device.id,
  device.model,
  telemetry['temperature'],
  telemetry['humidity']
FROM table_name;
```

## Complex Real-World Examples

### 16. E-commerce Order

```bash
python athena_flexible_table_generator.py \
  --s3-bucket my-bucket \
  --s3-prefix data/orders \
  --database shop \
  --athena-output s3://my-bucket/results/ \
  --table-name orders \
  --schema "order_id:BIGINT,order_date:DATE,customer:STRUCT<id:INT,name:STRING,email:STRING,tier:STRING>,shipping:STRUCT<address:STRING,city:STRING,zip:INT,carrier:STRING,tracking:STRING>,billing:STRUCT<subtotal:DECIMAL(10,2),tax:DECIMAL(10,2),shipping:DECIMAL(10,2),total:DECIMAL(10,2)>,items:ARRAY<STRING>,tags:MAP<STRING,STRING>" \
  --partitions "year,month" \
  --format parquet \
  --size medium
```

**Query**:
```sql
SELECT 
  order_id,
  order_date,
  customer.name,
  customer.email,
  customer.tier,
  shipping.city,
  shipping.carrier,
  billing.total,
  CARDINALITY(items) as item_count
FROM shop.orders
WHERE year = 2024 
  AND customer.tier = 'GOLD'
  AND billing.total > 100.00;
```

### 17. IoT Sensor Platform

```bash
python athena_flexible_table_generator.py \
  --s3-bucket my-bucket \
  --s3-prefix data/iot \
  --database iot_platform \
  --athena-output s3://my-bucket/results/ \
  --table-name sensor_data \
  --schema "device_id:STRING,timestamp:TIMESTAMP,device:STRUCT<model:STRING,firmware:STRING,manufacturer:STRING>,location:STRUCT<name:STRING,coordinates:STRUCT<lat:DOUBLE,lon:DOUBLE,altitude:FLOAT>>,readings:MAP<STRING,DOUBLE>,status:STRUCT<battery:TINYINT,signal:SMALLINT,online:BOOLEAN,errors:ARRAY<STRING>>" \
  --partitions "year,month,day" \
  --format parquet \
  --size large
```

**Query**:
```sql
SELECT 
  device_id,
  timestamp,
  device.model,
  location.name,
  location.coordinates.lat,
  location.coordinates.lon,
  readings['temperature'],
  readings['humidity'],
  status.battery,
  status.online
FROM iot_platform.sensor_data
WHERE year = 2024 
  AND month = 10
  AND status.battery < 20
  AND status.online = true;
```

### 18. Financial Trading Platform

```bash
python athena_flexible_table_generator.py \
  --s3-bucket my-bucket \
  --s3-prefix data/trades \
  --database trading \
  --athena-output s3://my-bucket/results/ \
  --table-name trades \
  --schema "trade_id:BIGINT,timestamp:TIMESTAMP,trader:STRUCT<id:INT,name:STRING,account:STRING,tier:STRING>,instrument:STRUCT<symbol:STRING,type:STRING,exchange:STRING>,execution:STRUCT<quantity:INT,price:DECIMAL(18,6),total:DECIMAL(18,2),currency:CHAR(3),commission:DECIMAL(10,2)>,counterparty:STRUCT<id:STRING,name:STRING>,metadata:MAP<STRING,STRING>" \
  --partitions "year,month" \
  --format parquet \
  --size large
```

**Query**:
```sql
SELECT 
  trade_id,
  timestamp,
  trader.name,
  trader.tier,
  instrument.symbol,
  instrument.exchange,
  execution.quantity,
  execution.price,
  execution.total,
  execution.commission
FROM trading.trades
WHERE year = 2024
  AND instrument.type = 'STOCK'
  AND execution.total > 10000.00
  AND trader.tier = 'INSTITUTIONAL';
```

### 19. Healthcare Patient Records

```bash
python athena_flexible_table_generator.py \
  --s3-bucket my-bucket \
  --s3-prefix data/patients \
  --database healthcare \
  --athena-output s3://my-bucket/results/ \
  --table-name patient_visits \
  --schema "visit_id:BIGINT,visit_date:DATE,patient:STRUCT<id:STRING,name:STRING,dob:DATE,gender:CHAR(1)>,provider:STRUCT<id:INT,name:STRING,specialty:STRING,facility:STRING>,diagnosis:STRUCT<primary_code:STRING,description:STRING,severity:STRING>,vitals:STRUCT<temperature:FLOAT,blood_pressure:STRING,heart_rate:SMALLINT,weight:DECIMAL(5,2)>,medications:ARRAY<STRING>,notes:MAP<STRING,STRING>" \
  --partitions "year,month" \
  --format parquet \
  --size medium
```

**Query**:
```sql
SELECT 
  visit_id,
  visit_date,
  patient.id,
  patient.name,
  provider.name,
  provider.specialty,
  diagnosis.description,
  vitals.temperature,
  vitals.heart_rate,
  CARDINALITY(medications) as medication_count
FROM healthcare.patient_visits
WHERE year = 2024
  AND provider.specialty = 'CARDIOLOGY'
  AND vitals.heart_rate > 100;
```

### 20. Social Media Analytics

```bash
python athena_flexible_table_generator.py \
  --s3-bucket my-bucket \
  --s3-prefix data/social \
  --database social_analytics \
  --athena-output s3://my-bucket/results/ \
  --table-name posts \
  --schema "post_id:STRING,created_at:TIMESTAMP,author:STRUCT<user_id:BIGINT,username:STRING,display_name:STRING,verified:BOOLEAN,followers:INT>,content:STRUCT<text:STRING,language:CHAR(2),char_count:INT,has_media:BOOLEAN>,engagement:STRUCT<likes:INT,shares:INT,comments:INT,views:INT,engagement_rate:FLOAT>,tags:ARRAY<STRING>,mentions:ARRAY<STRING>,metadata:MAP<STRING,STRING>" \
  --partitions "year,month" \
  --format json \
  --size large
```

**Query**:
```sql
SELECT 
  post_id,
  created_at,
  author.username,
  author.verified,
  author.followers,
  content.language,
  engagement.likes,
  engagement.shares,
  engagement.engagement_rate,
  CARDINALITY(tags) as tag_count
FROM social_analytics.posts
WHERE year = 2024
  AND author.verified = true
  AND engagement.likes > 1000
  AND content.language = 'EN';
```

## Tips for Writing STRUCT Schemas

1. **Naming**: Use descriptive field names (e.g., `customer_id` not `cid`)

2. **Nesting**: Limit nesting to 3-4 levels for readability

3. **Grouping**: Group related fields together
   - ✅ `address:STRUCT<street:STRING,city:STRING,zip:INT>`
   - ❌ Separate: `street:STRING,city:STRING,zip:INT`

4. **Types**: Choose appropriate types
   - Use `CHAR(n)` for fixed-length codes (e.g., currency, state codes)
   - Use `VARCHAR(n)` for variable-length strings with known max
   - Use `STRING` for unlimited text
   - Use `DECIMAL(p,s)` for monetary values

5. **Arrays vs Maps**:
   - Use `ARRAY<T>` for ordered lists
   - Use `MAP<K,V>` for key-value pairs

6. **Quoting**: Use quotes in shell to protect special characters:
   ```bash
   --schema "field1:TYPE,field2:STRUCT<nested:TYPE>"
   ```

7. **Validation**: Test with small dataset first (`--size small`)

## Common Mistakes to Avoid

❌ **Missing brackets**:
```
# Wrong
address:STRUCT<city:STRING,zip:INT
# Correct
address:STRUCT<city:STRING,zip:INT>
```

❌ **Wrong separators inside STRUCT**:
```
# Wrong
address:STRUCT<city:STRING;zip:INT>
# Correct
address:STRUCT<city:STRING,zip:INT>
```

❌ **Top-level separator inside nested types**:
```
# Wrong
user:STRUCT<name:STRING>,age:INT,address:STRUCT<city:STRING>
# Correct (commas only at top level between columns)
user:STRUCT<name:STRING,age:INT,address:STRUCT<city:STRING>>
```

❌ **Spaces in type names**:
```
# Wrong
amount:DECIMAL (10,2)
# Correct
amount:DECIMAL(10,2)
```

## Testing Your Schema

Start with a simple test:

```bash
# Test with 100 rows first
python athena_flexible_table_generator.py \
  --s3-bucket test-bucket \
  --s3-prefix test \
  --database test_db \
  --athena-output s3://test-bucket/results/ \
  --table-name test_table \
  --schema "id:INT,data:STRUCT<field1:STRING,field2:INT>" \
  --format parquet \
  --size small
```

Then query to verify:
```sql
SELECT id, data.field1, data.field2 
FROM test_db.test_table 
LIMIT 5;
```

If successful, scale up to your production schema and size!

