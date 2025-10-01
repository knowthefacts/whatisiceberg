"""
Example: Programmatic usage of Flexible Athena Table Generator

This demonstrates how to use the FlexibleAthenaTableGenerator class
directly in Python code, rather than through the command line.
"""

from athena_flexible_table_generator import FlexibleAthenaTableGenerator

# Configuration
S3_BUCKET = "your-bucket-name"
S3_PREFIX = "athena-data/programmatic"
DATABASE_NAME = "analytics_db"
ATHENA_OUTPUT_LOCATION = f"s3://{S3_BUCKET}/athena-results/"


def example_1_simple_table():
    """Example 1: Create a simple table with basic types"""
    print("\n" + "="*60)
    print("Example 1: Simple User Table")
    print("="*60)
    
    generator = FlexibleAthenaTableGenerator(
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX,
        database_name=DATABASE_NAME,
        athena_output_location=ATHENA_OUTPUT_LOCATION
    )
    
    # Create database
    generator.create_database()
    
    # Define schema
    schema = {
        'user_id': 'INT',
        'username': 'STRING',
        'email': 'VARCHAR(100)',
        'age': 'TINYINT',
        'signup_date': 'DATE',
        'is_active': 'BOOLEAN'
    }
    
    # Create table with data
    generator.create_table_with_data(
        table_name='users_simple',
        schema=schema,
        partition_columns=[],
        file_format='parquet',
        size='small'
    )
    
    print("\n✅ Table created: users_simple")
    print("Query: SELECT * FROM analytics_db.users_simple LIMIT 10;")


def example_2_struct_table():
    """Example 2: Table with STRUCT type"""
    print("\n" + "="*60)
    print("Example 2: Customers with STRUCT")
    print("="*60)
    
    generator = FlexibleAthenaTableGenerator(
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX,
        database_name=DATABASE_NAME,
        athena_output_location=ATHENA_OUTPUT_LOCATION
    )
    
    schema = {
        'customer_id': 'BIGINT',
        'name': 'STRING',
        'contact': 'STRUCT<email:STRING,phone:STRING,mobile:STRING>',
        'registered_at': 'TIMESTAMP'
    }
    
    generator.create_table_with_data(
        table_name='customers_contact',
        schema=schema,
        partition_columns=[],
        file_format='parquet',
        size='small'
    )
    
    print("\n✅ Table created: customers_contact")
    print("Query: SELECT customer_id, contact.email, contact.phone FROM analytics_db.customers_contact LIMIT 10;")


def example_3_nested_struct():
    """Example 3: Nested STRUCT"""
    print("\n" + "="*60)
    print("Example 3: Nested STRUCT (Address with Location)")
    print("="*60)
    
    generator = FlexibleAthenaTableGenerator(
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX,
        database_name=DATABASE_NAME,
        athena_output_location=ATHENA_OUTPUT_LOCATION
    )
    
    schema = {
        'property_id': 'BIGINT',
        'name': 'STRING',
        'address': 'STRUCT<street:STRING,city:STRING,state:CHAR(2),zip:INT,location:STRUCT<latitude:DOUBLE,longitude:DOUBLE>>'
    }
    
    generator.create_table_with_data(
        table_name='properties_location',
        schema=schema,
        partition_columns=[],
        file_format='parquet',
        size='small'
    )
    
    print("\n✅ Table created: properties_location")
    print("Query: SELECT property_id, address.city, address.location.latitude FROM analytics_db.properties_location LIMIT 10;")


def example_4_with_arrays():
    """Example 4: Table with ARRAY columns"""
    print("\n" + "="*60)
    print("Example 4: Products with ARRAY")
    print("="*60)
    
    generator = FlexibleAthenaTableGenerator(
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX,
        database_name=DATABASE_NAME,
        athena_output_location=ATHENA_OUTPUT_LOCATION
    )
    
    schema = {
        'product_id': 'BIGINT',
        'name': 'STRING',
        'price': 'DECIMAL(10,2)',
        'tags': 'ARRAY<STRING>',
        'related_product_ids': 'ARRAY<INT>'
    }
    
    generator.create_table_with_data(
        table_name='products_tags',
        schema=schema,
        partition_columns=[],
        file_format='parquet',
        size='medium'
    )
    
    print("\n✅ Table created: products_tags")
    print("Query: SELECT product_id, name, tags FROM analytics_db.products_tags WHERE CARDINALITY(tags) > 2 LIMIT 10;")


def example_5_with_maps():
    """Example 5: Table with MAP columns"""
    print("\n" + "="*60)
    print("Example 5: Events with MAP")
    print("="*60)
    
    generator = FlexibleAthenaTableGenerator(
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX,
        database_name=DATABASE_NAME,
        athena_output_location=ATHENA_OUTPUT_LOCATION
    )
    
    schema = {
        'event_id': 'STRING',
        'user_id': 'BIGINT',
        'event_type': 'STRING',
        'timestamp': 'TIMESTAMP',
        'properties': 'MAP<STRING,STRING>',
        'metrics': 'MAP<STRING,DOUBLE>'
    }
    
    generator.create_table_with_data(
        table_name='user_events_map',
        schema=schema,
        partition_columns=[],
        file_format='json',
        size='small'
    )
    
    print("\n✅ Table created: user_events_map")
    print("Query: SELECT event_id, properties['key1'], metrics['metric1'] FROM analytics_db.user_events_map LIMIT 10;")


def example_6_partitioned_table():
    """Example 6: Partitioned table"""
    print("\n" + "="*60)
    print("Example 6: Partitioned Orders Table")
    print("="*60)
    
    generator = FlexibleAthenaTableGenerator(
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX,
        database_name=DATABASE_NAME,
        athena_output_location=ATHENA_OUTPUT_LOCATION
    )
    
    schema = {
        'order_id': 'BIGINT',
        'customer_id': 'INT',
        'amount': 'DECIMAL(10,2)',
        'status': 'STRING',
        'order_date': 'DATE'
    }
    
    generator.create_table_with_data(
        table_name='orders_partitioned',
        schema=schema,
        partition_columns=['year', 'month'],
        file_format='parquet',
        size='medium'
    )
    
    print("\n✅ Table created: orders_partitioned")
    print("Query: SELECT * FROM analytics_db.orders_partitioned WHERE year=2024 AND month=10 LIMIT 10;")


def example_7_complex_ecommerce():
    """Example 7: Complex e-commerce schema"""
    print("\n" + "="*60)
    print("Example 7: Complex E-commerce Orders")
    print("="*60)
    
    generator = FlexibleAthenaTableGenerator(
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX,
        database_name=DATABASE_NAME,
        athena_output_location=ATHENA_OUTPUT_LOCATION
    )
    
    schema = {
        'order_id': 'BIGINT',
        'order_date': 'DATE',
        'customer': 'STRUCT<id:INT,name:STRING,email:STRING,tier:STRING>',
        'shipping': 'STRUCT<address:STRING,city:STRING,state:CHAR(2),zip:INT,method:STRING>',
        'billing': 'STRUCT<subtotal:DECIMAL(10,2),tax:DECIMAL(10,2),shipping_cost:DECIMAL(10,2),total:DECIMAL(10,2)>',
        'items': 'ARRAY<STRING>',
        'tags': 'MAP<STRING,STRING>'
    }
    
    generator.create_table_with_data(
        table_name='ecommerce_orders_complex',
        schema=schema,
        partition_columns=['year', 'month'],
        file_format='parquet',
        size='large'
    )
    
    print("\n✅ Table created: ecommerce_orders_complex")
    print("""
Query examples:
  - SELECT order_id, customer.name, billing.total FROM analytics_db.ecommerce_orders_complex LIMIT 10;
  - SELECT customer.tier, AVG(billing.total) FROM analytics_db.ecommerce_orders_complex WHERE year=2024 GROUP BY customer.tier;
  - SELECT order_id, CARDINALITY(items) as item_count FROM analytics_db.ecommerce_orders_complex WHERE year=2024 AND month=10;
    """)


def example_8_iot_sensors():
    """Example 8: IoT sensor data with nested structures"""
    print("\n" + "="*60)
    print("Example 8: IoT Sensor Telemetry")
    print("="*60)
    
    generator = FlexibleAthenaTableGenerator(
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX,
        database_name=DATABASE_NAME,
        athena_output_location=ATHENA_OUTPUT_LOCATION
    )
    
    schema = {
        'device_id': 'STRING',
        'timestamp': 'TIMESTAMP',
        'location': 'STRUCT<lat:DOUBLE,lon:DOUBLE,altitude:FLOAT>',
        'readings': 'MAP<STRING,DOUBLE>',
        'alerts': 'ARRAY<STRING>',
        'status': 'STRUCT<battery:TINYINT,signal:SMALLINT,online:BOOLEAN>'
    }
    
    generator.create_table_with_data(
        table_name='iot_sensor_data',
        schema=schema,
        partition_columns=['year', 'month', 'day'],
        file_format='parquet',
        size='large'
    )
    
    print("\n✅ Table created: iot_sensor_data")
    print("""
Query examples:
  - SELECT device_id, location.lat, location.lon, status.battery FROM analytics_db.iot_sensor_data WHERE year=2024 LIMIT 10;
  - SELECT device_id, readings['temperature'], readings['humidity'] FROM analytics_db.iot_sensor_data WHERE status.online=true;
  - SELECT COUNT(*) FROM analytics_db.iot_sensor_data WHERE status.battery < 20 AND year=2024;
    """)


def example_9_batch_table_creation():
    """Example 9: Create multiple tables in batch"""
    print("\n" + "="*60)
    print("Example 9: Batch Table Creation")
    print("="*60)
    
    generator = FlexibleAthenaTableGenerator(
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX,
        database_name=DATABASE_NAME,
        athena_output_location=ATHENA_OUTPUT_LOCATION
    )
    
    # Create database once
    generator.create_database()
    
    # Define multiple tables
    tables = [
        {
            'name': 'users',
            'schema': {
                'user_id': 'INT',
                'username': 'STRING',
                'email': 'STRING'
            },
            'partitions': [],
            'format': 'parquet',
            'size': 'small'
        },
        {
            'name': 'products',
            'schema': {
                'product_id': 'BIGINT',
                'name': 'STRING',
                'price': 'DECIMAL(10,2)',
                'tags': 'ARRAY<STRING>'
            },
            'partitions': [],
            'format': 'parquet',
            'size': 'small'
        },
        {
            'name': 'orders',
            'schema': {
                'order_id': 'BIGINT',
                'user_id': 'INT',
                'product_id': 'BIGINT',
                'amount': 'DECIMAL(10,2)',
                'order_date': 'DATE'
            },
            'partitions': ['year', 'month'],
            'format': 'parquet',
            'size': 'medium'
        }
    ]
    
    # Create all tables
    for table_config in tables:
        print(f"\nCreating table: {table_config['name']}...")
        generator.create_table_with_data(
            table_name=table_config['name'],
            schema=table_config['schema'],
            partition_columns=table_config['partitions'],
            file_format=table_config['format'],
            size=table_config['size']
        )
        print(f"✅ Created: {table_config['name']}")
    
    print("\n✅ All tables created successfully!")


def example_10_custom_data_generation():
    """Example 10: Generate data without creating table (for testing)"""
    print("\n" + "="*60)
    print("Example 10: Generate Data Only (No Table Creation)")
    print("="*60)
    
    generator = FlexibleAthenaTableGenerator(
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX,
        database_name=DATABASE_NAME,
        athena_output_location=ATHENA_OUTPUT_LOCATION
    )
    
    # Define schema
    schema = {
        'id': 'INT',
        'name': 'STRING',
        'address': 'STRUCT<city:STRING,zip:INT>'
    }
    
    # Generate DataFrame
    df = generator.generate_dataframe(schema, num_rows=100, partition_columns=[])
    
    print(f"\n✅ Generated DataFrame with {len(df)} rows")
    print(f"Columns: {list(df.columns)}")
    print("\nFirst 5 rows:")
    print(df.head())
    
    # You can now use this DataFrame for other purposes
    # - Save to local file
    # - Process with pandas
    # - Validate data
    # - etc.
    
    # Example: Save to local CSV
    df.to_csv('/tmp/test_data.csv', index=False)
    print("\n✅ Saved to /tmp/test_data.csv")


if __name__ == "__main__":
    print("\n" + "="*60)
    print("Flexible Athena Table Generator - Programmatic Examples")
    print("="*60)
    print("\nIMPORTANT: Update S3_BUCKET and DATABASE_NAME constants at the top of this file")
    print(f"\nCurrent configuration:")
    print(f"  S3 Bucket: {S3_BUCKET}")
    print(f"  Database: {DATABASE_NAME}")
    print(f"  Athena Output: {ATHENA_OUTPUT_LOCATION}")
    
    # Uncomment the examples you want to run:
    
    # example_1_simple_table()
    # example_2_struct_table()
    # example_3_nested_struct()
    # example_4_with_arrays()
    # example_5_with_maps()
    # example_6_partitioned_table()
    # example_7_complex_ecommerce()
    # example_8_iot_sensors()
    # example_9_batch_table_creation()
    # example_10_custom_data_generation()
    
    print("\n" + "="*60)
    print("To run examples, uncomment the desired example_*() calls above")
    print("="*60 + "\n")

