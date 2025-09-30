"""
Comprehensive Athena Data Generator
Generates sample data covering all AWS Athena supported data types
Uses SDV (Synthetic Data Vault) and other Python packages for realistic data generation
"""

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, date, timedelta
import json
import boto3
import os
import random
import string
from decimal import Decimal
from typing import Dict, List, Any
import argparse

# Optional: SDV for more realistic synthetic data
try:
    from sdv.single_table import GaussianCopulaSynthesizer
    from sdv.metadata import SingleTableMetadata
    SDV_AVAILABLE = True
except ImportError:
    print("SDV not installed. Install with: pip install sdv")
    SDV_AVAILABLE = False

class AthenaDataTypeGenerator:
    """
    Generates sample data for all AWS Athena supported data types
    
    Athena Data Types:
    - Primitive: BOOLEAN, TINYINT, SMALLINT, INT/INTEGER, BIGINT, FLOAT, DOUBLE, DECIMAL, 
                 CHAR, VARCHAR, STRING, BINARY, DATE, TIMESTAMP, INTERVAL
    - Complex: ARRAY, MAP, STRUCT (TODO: implement STRUCT support)
    """
    
    def __init__(self, s3_bucket: str, s3_prefix: str, database_name: str):
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix.rstrip('/')
        self.database_name = database_name
        
        # AWS clients
        self.s3_client = boto3.client('s3')
        self.athena_client = boto3.client('athena')
        
        # Data generation settings
        self.row_counts = {
            'tiny': 100,
            'small': 1000,
            'medium': 10000,
            'large': 100000
        }
        
        # Set random seeds for reproducibility
        np.random.seed(42)
        random.seed(42)
    
    def generate_boolean_data(self, num_rows: int) -> List[bool]:
        """Generate BOOLEAN data"""
        return np.random.choice([True, False, None], size=num_rows, p=[0.45, 0.45, 0.1]).tolist()
    
    def generate_tinyint_data(self, num_rows: int) -> List[int]:
        """Generate TINYINT data (-128 to 127)"""
        return np.random.randint(-128, 128, size=num_rows, dtype=np.int8).tolist()
    
    def generate_smallint_data(self, num_rows: int) -> List[int]:
        """Generate SMALLINT data (-32,768 to 32,767)"""
        return np.random.randint(-32768, 32768, size=num_rows, dtype=np.int16).tolist()
    
    def generate_integer_data(self, num_rows: int) -> List[int]:
        """Generate INT/INTEGER data (-2,147,483,648 to 2,147,483,647)"""
        return np.random.randint(-2147483648, 2147483647, size=num_rows, dtype=np.int32).tolist()
    
    def generate_bigint_data(self, num_rows: int) -> List[int]:
        """Generate BIGINT data"""
        return np.random.randint(-9223372036854775808, 9223372036854775807, 
                                size=num_rows, dtype=np.int64).tolist()
    
    def generate_float_data(self, num_rows: int) -> List[float]:
        """Generate FLOAT data (32-bit)"""
        return np.random.uniform(-1e38, 1e38, size=num_rows).astype(np.float32).tolist()
    
    def generate_double_data(self, num_rows: int) -> List[float]:
        """Generate DOUBLE data (64-bit)"""
        return np.random.uniform(-1e308, 1e308, size=num_rows).astype(np.float64).tolist()
    
    def generate_decimal_data(self, num_rows: int, precision: int = 10, scale: int = 2) -> List[str]:
        """Generate DECIMAL(precision, scale) data"""
        decimals = []
        for _ in range(num_rows):
            # Generate number with specified precision and scale
            integer_part = random.randint(0, 10**(precision-scale) - 1)
            decimal_part = random.randint(0, 10**scale - 1)
            decimal_str = f"{integer_part}.{str(decimal_part).zfill(scale)}"
            decimals.append(decimal_str)
        return decimals
    
    def generate_char_data(self, num_rows: int, length: int = 10) -> List[str]:
        """Generate CHAR(n) data - fixed length strings"""
        chars = []
        for _ in range(num_rows):
            # Generate exactly 'length' characters
            char_str = ''.join(random.choices(string.ascii_letters + string.digits, k=length))
            chars.append(char_str)
        return chars
    
    def generate_varchar_data(self, num_rows: int, max_length: int = 50) -> List[str]:
        """Generate VARCHAR(n) data - variable length strings"""
        varchars = []
        for _ in range(num_rows):
            # Generate variable length up to max_length
            length = random.randint(1, max_length)
            varchar_str = ''.join(random.choices(string.ascii_letters + string.digits + ' ', k=length))
            varchars.append(varchar_str)
        return varchars
    
    def generate_string_data(self, num_rows: int) -> List[str]:
        """Generate STRING data - variable length strings"""
        string_types = [
            lambda: f"Product_{random.randint(1000, 9999)}",
            lambda: f"User_{random.randint(1, 1000)}@example.com",
            lambda: f"{random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston'])}, {random.choice(['NY', 'CA', 'IL', 'TX'])}",
            lambda: ' '.join(random.choices(['The', 'Quick', 'Brown', 'Fox', 'Jumps', 'Over', 'Lazy', 'Dog'], k=random.randint(3, 8)))
        ]
        return [random.choice(string_types)() for _ in range(num_rows)]
    
    def generate_binary_data(self, num_rows: int) -> List[bytes]:
        """Generate BINARY data"""
        binaries = []
        for _ in range(num_rows):
            # Generate random bytes
            length = random.randint(5, 20)
            binary_data = bytes(random.randint(0, 255) for _ in range(length))
            binaries.append(binary_data)
        return binaries
    
    def generate_date_data(self, num_rows: int) -> List[date]:
        """Generate DATE data"""
        start_date = date(2020, 1, 1)
        end_date = date(2024, 12, 31)
        date_range = (end_date - start_date).days
        
        dates = []
        for _ in range(num_rows):
            random_days = random.randint(0, date_range)
            random_date = start_date + timedelta(days=random_days)
            dates.append(random_date)
        return dates
    
    def generate_timestamp_data(self, num_rows: int) -> List[datetime]:
        """Generate TIMESTAMP data"""
        start_timestamp = datetime(2020, 1, 1, 0, 0, 0)
        end_timestamp = datetime(2024, 12, 31, 23, 59, 59)
        
        timestamps = []
        for _ in range(num_rows):
            # Generate random timestamp between start and end
            time_between = end_timestamp - start_timestamp
            random_seconds = random.randint(0, int(time_between.total_seconds()))
            random_timestamp = start_timestamp + timedelta(seconds=random_seconds)
            timestamps.append(random_timestamp)
        return timestamps
    
    def generate_interval_data(self, num_rows: int) -> List[str]:
        """Generate INTERVAL data (represented as strings)"""
        intervals = []
        interval_types = [
            lambda: f"{random.randint(1, 30)} days",
            lambda: f"{random.randint(1, 24)} hours",
            lambda: f"{random.randint(1, 60)} minutes",
            lambda: f"{random.randint(1, 365)} days {random.randint(0, 23)} hours",
            lambda: f"{random.randint(1, 12)} months"
        ]
        
        for _ in range(num_rows):
            intervals.append(random.choice(interval_types)())
        return intervals
    
    def generate_array_data(self, num_rows: int, element_type: str = 'int') -> List[List[Any]]:
        """Generate ARRAY data"""
        arrays = []
        for _ in range(num_rows):
            array_length = random.randint(1, 10)
            if element_type == 'int':
                array = [random.randint(0, 100) for _ in range(array_length)]
            elif element_type == 'string':
                array = [f"item_{i}" for i in range(array_length)]
            elif element_type == 'float':
                array = [round(random.uniform(0, 100), 2) for _ in range(array_length)]
            arrays.append(array)
        return arrays
    
    def generate_map_data(self, num_rows: int) -> List[Dict[str, Any]]:
        """Generate MAP data"""
        maps = []
        for _ in range(num_rows):
            map_size = random.randint(1, 5)
            map_data = {}
            for i in range(map_size):
                key = f"key_{i}"
                value = random.choice([
                    random.randint(0, 100),
                    f"value_{random.randint(0, 100)}",
                    round(random.uniform(0, 100), 2)
                ])
                map_data[key] = value
            maps.append(map_data)
        return maps
    
    def generate_comprehensive_dataframe(self, num_rows: int, include_complex: bool = True) -> pd.DataFrame:
        """Generate DataFrame with all Athena data types"""
        
        data = {
            # Primitive types
            'id': list(range(1, num_rows + 1)),
            'bool_col': self.generate_boolean_data(num_rows),
            'tinyint_col': self.generate_tinyint_data(num_rows),
            'smallint_col': self.generate_smallint_data(num_rows),
            'int_col': self.generate_integer_data(num_rows),
            'bigint_col': self.generate_bigint_data(num_rows),
            'float_col': self.generate_float_data(num_rows),
            'double_col': self.generate_double_data(num_rows),
            'decimal_col': self.generate_decimal_data(num_rows, 10, 2),
            'decimal_high_precision': self.generate_decimal_data(num_rows, 38, 10),
            'char_col': self.generate_char_data(num_rows, 10),
            'varchar_col': self.generate_varchar_data(num_rows, 50),
            'string_col': self.generate_string_data(num_rows),
            'binary_col': self.generate_binary_data(num_rows),
            'date_col': self.generate_date_data(num_rows),
            'timestamp_col': self.generate_timestamp_data(num_rows),
            'interval_col': self.generate_interval_data(num_rows),
        }
        
        if include_complex:
            # Complex types (arrays and maps)
            data.update({
                'array_int': self.generate_array_data(num_rows, 'int'),
                'array_string': self.generate_array_data(num_rows, 'string'),
                'array_float': self.generate_array_data(num_rows, 'float'),
                'map_col': self.generate_map_data(num_rows),
            })
        
        # Add some null values randomly
        df = pd.DataFrame(data)
        
        # Randomly introduce nulls in some columns (except id)
        null_columns = ['bool_col', 'varchar_col', 'decimal_col', 'date_col', 'timestamp_col']
        for col in null_columns:
            null_mask = np.random.random(num_rows) < 0.05  # 5% nulls
            df.loc[null_mask, col] = None
        
        return df
    
    def generate_business_specific_data(self, num_rows: int, domain: str = 'ecommerce') -> pd.DataFrame:
        """Generate domain-specific realistic data using patterns"""
        
        if domain == 'ecommerce':
            data = {
                'order_id': [f"ORD-{str(i).zfill(8)}" for i in range(1, num_rows + 1)],
                'customer_id': np.random.randint(1000, 50000, num_rows),
                'order_date': self.generate_date_data(num_rows),
                'order_timestamp': self.generate_timestamp_data(num_rows),
                'order_amount': np.round(np.random.lognormal(4, 1.5, num_rows), 2),
                'discount_percent': np.random.choice([0, 5, 10, 15, 20, 25], num_rows, p=[0.4, 0.2, 0.2, 0.1, 0.05, 0.05]),
                'shipping_cost': np.round(np.random.uniform(0, 50, num_rows), 2),
                'is_prime_member': np.random.choice([True, False], num_rows, p=[0.3, 0.7]),
                'payment_method': np.random.choice(['Credit Card', 'Debit Card', 'PayPal', 'Apple Pay', 'Google Pay'], num_rows),
                'order_status': np.random.choice(['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled'], num_rows, 
                                               p=[0.1, 0.15, 0.2, 0.5, 0.05]),
                'product_categories': self.generate_array_data(num_rows, 'string'),
                'order_tags': self.generate_map_data(num_rows),
                'customer_tier': np.random.choice(['Bronze', 'Silver', 'Gold', 'Platinum'], num_rows, p=[0.5, 0.3, 0.15, 0.05]),
                'warehouse_code': self.generate_char_data(num_rows, 5),
                'tracking_number': self.generate_varchar_data(num_rows, 20),
                'notes': self.generate_string_data(num_rows),
                'loyalty_points': np.random.randint(0, 10000, num_rows),
                'items_count': np.random.randint(1, 20, num_rows, dtype=np.int8),
                'total_weight_kg': np.round(np.random.uniform(0.1, 50, num_rows), 3),
                'estimated_delivery_days': np.random.randint(1, 10, num_rows, dtype=np.int8),
            }
            
        elif domain == 'financial':
            data = {
                'transaction_id': [f"TXN-{str(i).zfill(10)}" for i in range(1, num_rows + 1)],
                'account_number': self.generate_char_data(num_rows, 12),
                'transaction_date': self.generate_date_data(num_rows),
                'transaction_timestamp': self.generate_timestamp_data(num_rows),
                'transaction_amount': self.generate_decimal_data(num_rows, 12, 2),
                'balance_before': self.generate_decimal_data(num_rows, 15, 2),
                'balance_after': self.generate_decimal_data(num_rows, 15, 2),
                'transaction_type': np.random.choice(['Deposit', 'Withdrawal', 'Transfer', 'Payment', 'Fee'], num_rows),
                'merchant_name': self.generate_varchar_data(num_rows, 50),
                'merchant_category': np.random.choice(['Grocery', 'Gas', 'Restaurant', 'Shopping', 'Utilities', 'Other'], num_rows),
                'is_international': np.random.choice([True, False], num_rows, p=[0.1, 0.9]),
                'currency_code': self.generate_char_data(num_rows, 3),
                'exchange_rate': np.round(np.random.uniform(0.5, 2.0, num_rows), 6),
                'risk_score': np.random.randint(0, 100, num_rows, dtype=np.int8),
                'fraud_flag': np.random.choice([True, False], num_rows, p=[0.01, 0.99]),
                'processing_time_ms': np.random.randint(10, 5000, num_rows, dtype=np.int32),
                'transaction_metadata': self.generate_map_data(num_rows),
            }
            
        elif domain == 'iot':
            data = {
                'device_id': [f"DEV-{str(i % 1000).zfill(6)}" for i in range(num_rows)],
                'sensor_timestamp': self.generate_timestamp_data(num_rows),
                'temperature_c': np.round(np.random.normal(22, 5, num_rows), 2),
                'humidity_percent': np.random.randint(20, 80, num_rows, dtype=np.int8),
                'pressure_hpa': np.round(np.random.normal(1013, 20, num_rows), 1),
                'battery_level': np.random.randint(0, 100, num_rows, dtype=np.int8),
                'signal_strength_dbm': np.random.randint(-100, -30, num_rows, dtype=np.int8),
                'is_online': np.random.choice([True, False], num_rows, p=[0.95, 0.05]),
                'firmware_version': self.generate_char_data(num_rows, 8),
                'location_lat': np.round(np.random.uniform(-90, 90, num_rows), 6),
                'location_lon': np.round(np.random.uniform(-180, 180, num_rows), 6),
                'error_codes': self.generate_array_data(num_rows, 'int'),
                'sensor_readings': self.generate_map_data(num_rows),
                'maintenance_due': self.generate_date_data(num_rows),
                'uptime_hours': np.random.randint(0, 8760, num_rows, dtype=np.int32),
            }
        
        df = pd.DataFrame(data)
        
        # Add partitioning columns
        if 'order_date' in df.columns:
            df['year'] = pd.to_datetime(df['order_date']).dt.year
            df['month'] = pd.to_datetime(df['order_date']).dt.month
        elif 'transaction_date' in df.columns:
            df['year'] = pd.to_datetime(df['transaction_date']).dt.year
            df['month'] = pd.to_datetime(df['transaction_date']).dt.month
        elif 'sensor_timestamp' in df.columns:
            df['year'] = pd.to_datetime(df['sensor_timestamp']).dt.year
            df['month'] = pd.to_datetime(df['sensor_timestamp']).dt.month
            df['day'] = pd.to_datetime(df['sensor_timestamp']).dt.day
        
        return df
    
    def save_as_parquet(self, df: pd.DataFrame, table_name: str, partitioned: bool = False) -> str:
        """Save DataFrame as Parquet with proper type handling"""
        local_path = f"/tmp/{table_name}"
        os.makedirs(local_path, exist_ok=True)
        
        # Handle binary columns for Parquet
        for col in df.columns:
            if df[col].dtype == 'object' and isinstance(df[col].iloc[0], bytes):
                # Convert bytes to base64 string for Parquet compatibility
                import base64
                df[col] = df[col].apply(lambda x: base64.b64encode(x).decode('utf-8') if x is not None else None)
        
        if partitioned and any(col in df.columns for col in ['year', 'month', 'day']):
            # Save as partitioned dataset
            partition_cols = [col for col in ['year', 'month', 'day'] if col in df.columns]
            table = pa.Table.from_pandas(df)
            pq.write_to_dataset(
                table,
                root_path=local_path,
                partition_cols=partition_cols,
                compression='snappy'
            )
        else:
            # Save as single file
            df.to_parquet(f"{local_path}/data.parquet", engine='pyarrow', compression='snappy')
        
        return local_path
    
    def save_as_csv(self, df: pd.DataFrame, table_name: str) -> str:
        """Save DataFrame as CSV with proper type handling"""
        local_path = f"/tmp/{table_name}.csv"
        
        # Convert complex types to JSON strings for CSV
        df_csv = df.copy()
        for col in df_csv.columns:
            if df_csv[col].dtype == 'object':
                # Check if column contains lists, dicts, or bytes
                sample = df_csv[col].dropna().iloc[0] if len(df_csv[col].dropna()) > 0 else None
                if isinstance(sample, (list, dict)):
                    df_csv[col] = df_csv[col].apply(lambda x: json.dumps(x) if x is not None else None)
                elif isinstance(sample, bytes):
                    import base64
                    df_csv[col] = df_csv[col].apply(lambda x: base64.b64encode(x).decode('utf-8') if x is not None else None)
        
        df_csv.to_csv(local_path, index=False)
        return local_path
    
    def save_as_json(self, df: pd.DataFrame, table_name: str) -> str:
        """Save DataFrame as JSON Lines with proper type handling"""
        local_path = f"/tmp/{table_name}.json"
        
        # Convert types for JSON serialization
        df_json = df.copy()
        for col in df_json.columns:
            if df_json[col].dtype == 'object':
                sample = df_json[col].dropna().iloc[0] if len(df_json[col].dropna()) > 0 else None
                if isinstance(sample, bytes):
                    import base64
                    df_json[col] = df_json[col].apply(lambda x: base64.b64encode(x).decode('utf-8') if x is not None else None)
            elif pd.api.types.is_datetime64_any_dtype(df_json[col]):
                df_json[col] = df_json[col].astype(str)
            elif df_json[col].dtype.name == 'date':
                df_json[col] = df_json[col].astype(str)
        
        # Save as JSON Lines (one JSON object per line)
        df_json.to_json(local_path, orient='records', lines=True, date_format='iso')
        return local_path
    
    def upload_to_s3(self, local_path: str, s3_key: str) -> None:
        """Upload file or directory to S3"""
        if os.path.isdir(local_path):
            # Upload directory (for partitioned data)
            for root, dirs, files in os.walk(local_path):
                for file in files:
                    if file.endswith('.parquet'):
                        local_file = os.path.join(root, file)
                        relative_path = os.path.relpath(local_file, local_path)
                        s3_file_key = f"{s3_key}/{relative_path}".replace("\\", "/")
                        self.s3_client.upload_file(local_file, self.s3_bucket, s3_file_key)
                        print(f"Uploaded: s3://{self.s3_bucket}/{s3_file_key}")
        else:
            # Upload single file
            self.s3_client.upload_file(local_path, self.s3_bucket, s3_key)
            print(f"Uploaded: s3://{self.s3_bucket}/{s3_key}")
    
    def create_athena_table_ddl(self, table_name: str, df: pd.DataFrame, format_type: str, 
                               s3_location: str, partitioned: bool = False) -> str:
        """Generate CREATE TABLE DDL for Athena"""
        
        # Map pandas/Python types to Athena types
        type_mapping = {
            'int8': 'TINYINT',
            'int16': 'SMALLINT', 
            'int32': 'INT',
            'int64': 'BIGINT',
            'float32': 'FLOAT',
            'float64': 'DOUBLE',
            'bool': 'BOOLEAN',
            'object': 'STRING',  # Default for object types
            'datetime64[ns]': 'TIMESTAMP',
            'date': 'DATE'
        }
        
        # Build column definitions
        columns = []
        partition_cols = ['year', 'month', 'day']
        
        for col in df.columns:
            if col not in partition_cols:
                dtype = str(df[col].dtype)
                
                # Special handling for specific columns
                if 'decimal' in col:
                    if 'high_precision' in col:
                        athena_type = 'DECIMAL(38,10)'
                    else:
                        athena_type = 'DECIMAL(10,2)'
                elif 'char_' in col:
                    athena_type = 'CHAR(10)' if col == 'char_col' else 'CHAR(5)'
                elif 'varchar_' in col:
                    athena_type = 'VARCHAR(50)'
                elif 'binary' in col:
                    athena_type = 'BINARY'
                elif 'interval' in col:
                    athena_type = 'STRING'  # Athena doesn't have native INTERVAL
                elif 'array' in col:
                    if 'int' in col:
                        athena_type = 'ARRAY<INT>'
                    elif 'string' in col:
                        athena_type = 'ARRAY<STRING>'
                    elif 'float' in col:
                        athena_type = 'ARRAY<DOUBLE>'
                    else:
                        athena_type = 'ARRAY<STRING>'
                elif 'map' in col:
                    athena_type = 'MAP<STRING,STRING>'
                else:
                    athena_type = type_mapping.get(dtype, 'STRING')
                
                columns.append(f"  `{col}` {athena_type}")
        
        columns_def = ",\n".join(columns)
        
        # Build CREATE TABLE statement
        if format_type == 'parquet':
            if partitioned and any(col in df.columns for col in partition_cols):
                partition_clause = "PARTITIONED BY (\n"
                partition_defs = []
                for col in partition_cols:
                    if col in df.columns:
                        partition_defs.append(f"  `{col}` INT")
                partition_clause += ",\n".join(partition_defs) + "\n)"
                
                ddl = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS `{self.database_name}`.`{table_name}` (
{columns_def}
)
{partition_clause}
STORED AS PARQUET
LOCATION '{s3_location}'
TBLPROPERTIES ('parquet.compression'='SNAPPY')
"""
            else:
                ddl = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS `{self.database_name}`.`{table_name}` (
{columns_def}
)
STORED AS PARQUET
LOCATION '{s3_location}'
TBLPROPERTIES ('parquet.compression'='SNAPPY')
"""
        
        elif format_type == 'csv':
            ddl = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS `{self.database_name}`.`{table_name}` (
{columns_def}
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '{s3_location}'
TBLPROPERTIES ('skip.header.line.count'='1')
"""
        
        elif format_type == 'json':
            ddl = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS `{self.database_name}`.`{table_name}` (
{columns_def}
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '{s3_location}'
"""
        
        return ddl
    
    def generate_all_test_tables(self):
        """Generate comprehensive test tables with all data types"""
        
        print(f"\n🚀 Generating comprehensive Athena test data")
        print(f"📍 Target: s3://{self.s3_bucket}/{self.s3_prefix}/")
        print(f"📊 Database: {self.database_name}\n")
        
        # Table configurations
        table_configs = [
            # All data types tables
            {
                'name': 'all_types_tiny_parquet',
                'generator': lambda rows: self.generate_comprehensive_dataframe(rows, include_complex=True),
                'size': 'tiny',
                'format': 'parquet',
                'partitioned': False,
                'description': 'All Athena data types - tiny dataset'
            },
            {
                'name': 'all_types_small_parquet',
                'generator': lambda rows: self.generate_comprehensive_dataframe(rows, include_complex=True),
                'size': 'small',
                'format': 'parquet',
                'partitioned': False,
                'description': 'All Athena data types - small dataset'
            },
            {
                'name': 'all_types_medium_csv',
                'generator': lambda rows: self.generate_comprehensive_dataframe(rows, include_complex=True),
                'size': 'medium',
                'format': 'csv',
                'partitioned': False,
                'description': 'All Athena data types - CSV format'
            },
            {
                'name': 'all_types_small_json',
                'generator': lambda rows: self.generate_comprehensive_dataframe(rows, include_complex=True),
                'size': 'small',
                'format': 'json',
                'partitioned': False,
                'description': 'All Athena data types - JSON format'
            },
            
            # Business domain tables
            {
                'name': 'ecommerce_orders_small_parquet',
                'generator': lambda rows: self.generate_business_specific_data(rows, 'ecommerce'),
                'size': 'small',
                'format': 'parquet',
                'partitioned': True,
                'description': 'E-commerce orders with realistic data'
            },
            {
                'name': 'ecommerce_orders_medium_parquet',
                'generator': lambda rows: self.generate_business_specific_data(rows, 'ecommerce'),
                'size': 'medium',
                'format': 'parquet',
                'partitioned': True,
                'description': 'E-commerce orders - medium dataset'
            },
            {
                'name': 'financial_transactions_medium_parquet',
                'generator': lambda rows: self.generate_business_specific_data(rows, 'financial'),
                'size': 'medium',
                'format': 'parquet',
                'partitioned': True,
                'description': 'Financial transactions with decimal precision'
            },
            {
                'name': 'iot_sensor_data_large_parquet',
                'generator': lambda rows: self.generate_business_specific_data(rows, 'iot'),
                'size': 'large',
                'format': 'parquet',
                'partitioned': True,
                'description': 'IoT sensor data - time series'
            },
            {
                'name': 'financial_transactions_small_csv',
                'generator': lambda rows: self.generate_business_specific_data(rows, 'financial'),
                'size': 'small',
                'format': 'csv',
                'partitioned': False,
                'description': 'Financial data in CSV format'
            },
            {
                'name': 'iot_sensor_data_small_json',
                'generator': lambda rows: self.generate_business_specific_data(rows, 'iot'),
                'size': 'small',
                'format': 'json',
                'partitioned': False,
                'description': 'IoT data in JSON format'
            }
        ]
        
        # Generate synthetic data using SDV if available
        if SDV_AVAILABLE:
            print("📦 SDV is available - will generate additional synthetic datasets\n")
        
        # Process each table configuration
        ddl_statements = []
        
        for config in table_configs:
            print(f"\n{'='*60}")
            print(f"📊 Generating: {config['name']}")
            print(f"📝 Description: {config['description']}")
            print(f"📈 Size: {config['size']} ({self.row_counts[config['size']]:,} rows)")
            print(f"💾 Format: {config['format'].upper()}")
            print(f"🗂️  Partitioned: {config['partitioned']}")
            
            # Generate data
            num_rows = self.row_counts[config['size']]
            df = config['generator'](num_rows)
            
            print(f"✅ Generated {len(df)} rows with {len(df.columns)} columns")
            
            # Save locally
            if config['format'] == 'parquet':
                local_path = self.save_as_parquet(df, config['name'], config['partitioned'])
            elif config['format'] == 'csv':
                local_path = self.save_as_csv(df, config['name'])
            elif config['format'] == 'json':
                local_path = self.save_as_json(df, config['name'])
            
            # Upload to S3
            s3_key = f"{self.s3_prefix}/{config['format']}/{config['name']}"
            self.upload_to_s3(local_path, s3_key)
            
            # Generate DDL
            s3_location = f"s3://{self.s3_bucket}/{s3_key}/"
            ddl = self.create_athena_table_ddl(
                config['name'], 
                df, 
                config['format'],
                s3_location,
                config['partitioned']
            )
            
            ddl_statements.append({
                'table': config['name'],
                'ddl': ddl,
                'partitioned': config['partitioned']
            })
            
            # Clean up local files
            import shutil
            if os.path.isdir(local_path):
                shutil.rmtree(local_path)
            else:
                os.remove(local_path)
        
        # Save DDL statements to file
        ddl_file = 'athena_tables_ddl.sql'
        with open(ddl_file, 'w') as f:
            f.write(f"-- Athena Tables DDL for database: {self.database_name}\n")
            f.write(f"-- Generated on: {datetime.now().isoformat()}\n\n")
            
            f.write(f"-- Create database\n")
            f.write(f"CREATE DATABASE IF NOT EXISTS {self.database_name};\n\n")
            
            for ddl_info in ddl_statements:
                f.write(f"-- Table: {ddl_info['table']}\n")
                f.write(ddl_info['ddl'])
                f.write("\n")
                if ddl_info['partitioned']:
                    f.write(f"-- Run this after table creation to discover partitions:\n")
                    f.write(f"-- MSCK REPAIR TABLE {self.database_name}.{ddl_info['table']};\n")
                f.write("\n")
        
        print(f"\n{'='*60}")
        print(f"✅ Data generation complete!")
        print(f"📄 DDL statements saved to: {ddl_file}")
        print(f"📍 Data location: s3://{self.s3_bucket}/{self.s3_prefix}/")
        print(f"\n📋 Tables generated:")
        for config in table_configs:
            print(f"   - {config['name']:<40} ({config['size']:<6} - {config['format']:<7})")
        print(f"{'='*60}\n")
        
        # Print Athena data types covered
        print("📊 Athena Data Types Coverage:")
        print("   Primitive Types:")
        print("   ✓ BOOLEAN")
        print("   ✓ TINYINT (8-bit)")
        print("   ✓ SMALLINT (16-bit)")
        print("   ✓ INT/INTEGER (32-bit)")
        print("   ✓ BIGINT (64-bit)")
        print("   ✓ FLOAT (32-bit)")
        print("   ✓ DOUBLE (64-bit)")
        print("   ✓ DECIMAL (configurable precision)")
        print("   ✓ CHAR (fixed-length)")
        print("   ✓ VARCHAR (variable-length)")
        print("   ✓ STRING")
        print("   ✓ BINARY")
        print("   ✓ DATE")
        print("   ✓ TIMESTAMP")
        print("   ✓ INTERVAL (as STRING)")
        print("\n   Complex Types:")
        print("   ✓ ARRAY<T>")
        print("   ✓ MAP<K,V>")
        print("   ✗ STRUCT (TODO: implement STRUCT support)")
        

def main():
    parser = argparse.ArgumentParser(description='Generate comprehensive Athena test data with all data types')
    parser.add_argument('--s3-bucket', required=True, help='S3 bucket name')
    parser.add_argument('--s3-prefix', required=True, help='S3 prefix for data')
    parser.add_argument('--database-name', required=True, help='Athena database name')
    
    args = parser.parse_args()
    
    # Create generator
    generator = AthenaDataTypeGenerator(
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        database_name=args.database_name
    )
    
    # Generate all test tables
    generator.generate_all_test_tables()


if __name__ == "__main__":
    main()
