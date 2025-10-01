"""
Flexible Athena Table Generator with STRUCT Support
Generates Athena tables with custom schema including STRUCT types
Supports CSV, Parquet, and JSON formats with configurable row counts
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
from typing import Dict, List, Any, Optional, Tuple
import argparse
import time


class FlexibleAthenaTableGenerator:
    """
    Generates Athena tables with custom schemas including STRUCT support
    """
    
    SIZE_MAPPING = {
        'small': 10000,
        'medium': 100000,
        'large': 5000000
    }
    
    def __init__(self, s3_bucket: str, s3_prefix: str, database_name: str, 
                 athena_output_location: str):
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix.rstrip('/')
        self.database_name = database_name
        self.athena_output_location = athena_output_location
        
        # AWS clients
        self.s3_client = boto3.client('s3')
        self.athena_client = boto3.client('athena')
        
        # Set random seeds for reproducibility
        np.random.seed(42)
        random.seed(42)
    
    def generate_data_for_type(self, data_type: str, num_rows: int, 
                              column_name: str = "") -> List[Any]:
        """
        Generate data based on Athena data type specification
        Supports: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, 
                  DECIMAL(p,s), CHAR(n), VARCHAR(n), STRING, BINARY, DATE, 
                  TIMESTAMP, ARRAY<type>, MAP<k,v>, STRUCT<field:type,...>
        """
        data_type = data_type.strip().upper()
        
        # BOOLEAN
        if data_type == 'BOOLEAN':
            return np.random.choice([True, False, None], size=num_rows, 
                                   p=[0.45, 0.45, 0.1]).tolist()
        
        # Integer types
        elif data_type == 'TINYINT':
            return np.random.randint(-128, 128, size=num_rows, dtype=np.int8).tolist()
        
        elif data_type == 'SMALLINT':
            return np.random.randint(-32768, 32768, size=num_rows, dtype=np.int16).tolist()
        
        elif data_type in ['INT', 'INTEGER']:
            return np.random.randint(-2147483648, 2147483647, size=num_rows, 
                                    dtype=np.int32).tolist()
        
        elif data_type == 'BIGINT':
            return np.random.randint(-9223372036854775808, 9223372036854775807, 
                                    size=num_rows, dtype=np.int64).tolist()
        
        # Float types
        elif data_type == 'FLOAT':
            return np.random.uniform(-1e6, 1e6, size=num_rows).astype(np.float32).tolist()
        
        elif data_type == 'DOUBLE':
            return np.random.uniform(-1e6, 1e6, size=num_rows).astype(np.float64).tolist()
        
        # DECIMAL
        elif data_type.startswith('DECIMAL'):
            precision, scale = self._parse_decimal_params(data_type)
            return self._generate_decimal_data(num_rows, precision, scale)
        
        # String types
        elif data_type.startswith('CHAR'):
            length = self._parse_length_param(data_type, default=10)
            return self._generate_char_data(num_rows, length)
        
        elif data_type.startswith('VARCHAR'):
            max_length = self._parse_length_param(data_type, default=50)
            return self._generate_varchar_data(num_rows, max_length)
        
        elif data_type == 'STRING':
            return self._generate_string_data(num_rows)
        
        # BINARY
        elif data_type == 'BINARY':
            return self._generate_binary_data(num_rows)
        
        # Date/Time types
        elif data_type == 'DATE':
            return self._generate_date_data(num_rows)
        
        elif data_type == 'TIMESTAMP':
            return self._generate_timestamp_data(num_rows)
        
        # ARRAY
        elif data_type.startswith('ARRAY'):
            element_type = self._parse_array_type(data_type)
            return self._generate_array_data(num_rows, element_type)
        
        # MAP
        elif data_type.startswith('MAP'):
            key_type, value_type = self._parse_map_types(data_type)
            return self._generate_map_data(num_rows, key_type, value_type)
        
        # STRUCT
        elif data_type.startswith('STRUCT'):
            fields = self._parse_struct_fields(data_type)
            return self._generate_struct_data(num_rows, fields)
        
        else:
            # Default to STRING
            return self._generate_string_data(num_rows)
    
    def _parse_decimal_params(self, data_type: str) -> Tuple[int, int]:
        """Parse DECIMAL(precision, scale)"""
        try:
            params = data_type[data_type.index('(')+1:data_type.index(')')].split(',')
            precision = int(params[0].strip())
            scale = int(params[1].strip()) if len(params) > 1 else 0
            return precision, scale
        except:
            return 10, 2  # Default
    
    def _parse_length_param(self, data_type: str, default: int) -> int:
        """Parse CHAR(n) or VARCHAR(n)"""
        try:
            return int(data_type[data_type.index('(')+1:data_type.index(')')])
        except:
            return default
    
    def _parse_array_type(self, data_type: str) -> str:
        """Parse ARRAY<type>"""
        try:
            start = data_type.index('<') + 1
            end = data_type.rindex('>')
            return data_type[start:end].strip()
        except:
            return 'STRING'
    
    def _parse_map_types(self, data_type: str) -> Tuple[str, str]:
        """Parse MAP<key_type, value_type>"""
        try:
            start = data_type.index('<') + 1
            end = data_type.rindex('>')
            types = data_type[start:end].split(',')
            key_type = types[0].strip()
            value_type = types[1].strip()
            return key_type, value_type
        except:
            return 'STRING', 'STRING'
    
    def _parse_struct_fields(self, data_type: str) -> Dict[str, str]:
        """
        Parse STRUCT<field1:type1, field2:type2, ...>
        Example: STRUCT<name:STRING, age:INT, address:STRUCT<city:STRING, zip:INT>>
        """
        try:
            start = data_type.index('<') + 1
            end = data_type.rindex('>')
            struct_def = data_type[start:end]
            
            fields = {}
            depth = 0
            current_field = ""
            
            for char in struct_def:
                if char == '<':
                    depth += 1
                    current_field += char
                elif char == '>':
                    depth -= 1
                    current_field += char
                elif char == ',' and depth == 0:
                    # Field separator
                    field_name, field_type = current_field.split(':', 1)
                    fields[field_name.strip()] = field_type.strip()
                    current_field = ""
                else:
                    current_field += char
            
            # Add last field
            if current_field:
                field_name, field_type = current_field.split(':', 1)
                fields[field_name.strip()] = field_type.strip()
            
            return fields
        except Exception as e:
            print(f"Error parsing STRUCT: {e}")
            return {'field1': 'STRING'}
    
    def _generate_decimal_data(self, num_rows: int, precision: int, scale: int) -> List[str]:
        """Generate DECIMAL data"""
        decimals = []
        for _ in range(num_rows):
            integer_part = random.randint(0, 10**(precision-scale) - 1)
            decimal_part = random.randint(0, 10**scale - 1)
            decimal_str = f"{integer_part}.{str(decimal_part).zfill(scale)}"
            decimals.append(decimal_str)
        return decimals
    
    def _generate_char_data(self, num_rows: int, length: int) -> List[str]:
        """Generate CHAR(n) data"""
        return [''.join(random.choices(string.ascii_letters + string.digits, k=length)) 
                for _ in range(num_rows)]
    
    def _generate_varchar_data(self, num_rows: int, max_length: int) -> List[str]:
        """Generate VARCHAR(n) data"""
        return [''.join(random.choices(string.ascii_letters + string.digits + ' ', 
                k=random.randint(1, max_length))) for _ in range(num_rows)]
    
    def _generate_string_data(self, num_rows: int) -> List[str]:
        """Generate STRING data"""
        string_types = [
            lambda: f"Product_{random.randint(1000, 9999)}",
            lambda: f"User_{random.randint(1, 1000)}@example.com",
            lambda: ' '.join(random.choices(['Alpha', 'Beta', 'Gamma', 'Delta'], 
                            k=random.randint(2, 5)))
        ]
        return [random.choice(string_types)() for _ in range(num_rows)]
    
    def _generate_binary_data(self, num_rows: int) -> List[bytes]:
        """Generate BINARY data"""
        return [bytes(random.randint(0, 255) for _ in range(random.randint(5, 20))) 
                for _ in range(num_rows)]
    
    def _generate_date_data(self, num_rows: int) -> List[date]:
        """Generate DATE data"""
        start_date = date(2020, 1, 1)
        end_date = date(2024, 12, 31)
        date_range = (end_date - start_date).days
        return [start_date + timedelta(days=random.randint(0, date_range)) 
                for _ in range(num_rows)]
    
    def _generate_timestamp_data(self, num_rows: int) -> List[datetime]:
        """Generate TIMESTAMP data"""
        start_ts = datetime(2020, 1, 1, 0, 0, 0)
        end_ts = datetime(2024, 12, 31, 23, 59, 59)
        time_between = int((end_ts - start_ts).total_seconds())
        return [start_ts + timedelta(seconds=random.randint(0, time_between)) 
                for _ in range(num_rows)]
    
    def _generate_array_data(self, num_rows: int, element_type: str) -> List[List[Any]]:
        """Generate ARRAY data"""
        arrays = []
        for _ in range(num_rows):
            array_length = random.randint(1, 10)
            elements = self.generate_data_for_type(element_type, array_length)
            arrays.append(elements)
        return arrays
    
    def _generate_map_data(self, num_rows: int, key_type: str, 
                          value_type: str) -> List[Dict[Any, Any]]:
        """Generate MAP data"""
        maps = []
        for _ in range(num_rows):
            map_size = random.randint(1, 5)
            keys = self.generate_data_for_type(key_type, map_size)
            values = self.generate_data_for_type(value_type, map_size)
            map_data = dict(zip([str(k) for k in keys], values))
            maps.append(map_data)
        return maps
    
    def _generate_struct_data(self, num_rows: int, 
                             fields: Dict[str, str]) -> List[Dict[str, Any]]:
        """Generate STRUCT data"""
        structs = []
        for _ in range(num_rows):
            struct_data = {}
            for field_name, field_type in fields.items():
                field_value = self.generate_data_for_type(field_type, 1)[0]
                struct_data[field_name] = field_value
            structs.append(struct_data)
        return structs
    
    def generate_dataframe(self, schema: Dict[str, str], num_rows: int, 
                          partition_columns: List[str] = None) -> pd.DataFrame:
        """
        Generate DataFrame based on schema
        
        Args:
            schema: Dict of column_name: data_type
            num_rows: Number of rows to generate
            partition_columns: List of column names that are partitions
        """
        data = {}
        partition_cols = partition_columns or []
        
        # Generate data for non-partition columns
        for col_name, data_type in schema.items():
            if col_name not in partition_cols:
                print(f"  Generating {num_rows:,} values for '{col_name}' ({data_type})")
                data[col_name] = self.generate_data_for_type(data_type, num_rows, col_name)
        
        df = pd.DataFrame(data)
        
        # Add partition columns
        for partition_col in partition_cols:
            if partition_col == 'year':
                # Generate years between 2020-2024
                df[partition_col] = np.random.choice([2020, 2021, 2022, 2023, 2024], 
                                                     size=num_rows)
            elif partition_col == 'month':
                # Generate months 1-12
                df[partition_col] = np.random.randint(1, 13, size=num_rows)
            elif partition_col == 'day':
                # Generate days 1-28 (safe for all months)
                df[partition_col] = np.random.randint(1, 29, size=num_rows)
            elif partition_col == 'region':
                # Geographic regions
                df[partition_col] = np.random.choice(['us-east', 'us-west', 'eu-west', 
                                                     'ap-south'], size=num_rows)
            elif partition_col == 'category':
                # Generic categories
                df[partition_col] = np.random.choice(['A', 'B', 'C', 'D'], size=num_rows)
            else:
                # Default to string partition
                df[partition_col] = np.random.choice(['part1', 'part2', 'part3'], 
                                                    size=num_rows)
        
        return df
    
    def save_as_parquet(self, df: pd.DataFrame, table_name: str, 
                       partition_columns: List[str] = None) -> str:
        """Save DataFrame as Parquet"""
        local_path = f"/tmp/{table_name}"
        os.makedirs(local_path, exist_ok=True)
        
        # Handle binary columns
        for col in df.columns:
            if df[col].dtype == 'object' and len(df[col].dropna()) > 0:
                sample = df[col].dropna().iloc[0]
                if isinstance(sample, bytes):
                    import base64
                    df[col] = df[col].apply(lambda x: base64.b64encode(x).decode('utf-8') 
                                          if x is not None else None)
        
        if partition_columns and len(partition_columns) > 0:
            # Save as partitioned dataset
            table = pa.Table.from_pandas(df)
            pq.write_to_dataset(
                table,
                root_path=local_path,
                partition_cols=partition_columns,
                compression='snappy'
            )
        else:
            # Save as single file
            df.to_parquet(f"{local_path}/data.parquet", engine='pyarrow', 
                         compression='snappy')
        
        return local_path
    
    def save_as_csv(self, df: pd.DataFrame, table_name: str) -> str:
        """Save DataFrame as CSV"""
        local_path = f"/tmp/{table_name}.csv"
        
        # Convert complex types to JSON strings
        df_csv = df.copy()
        for col in df_csv.columns:
            if df_csv[col].dtype == 'object' and len(df_csv[col].dropna()) > 0:
                sample = df_csv[col].dropna().iloc[0]
                if isinstance(sample, (list, dict)):
                    df_csv[col] = df_csv[col].apply(lambda x: json.dumps(x) 
                                                   if x is not None else None)
                elif isinstance(sample, bytes):
                    import base64
                    df_csv[col] = df_csv[col].apply(lambda x: base64.b64encode(x).decode('utf-8') 
                                                   if x is not None else None)
        
        df_csv.to_csv(local_path, index=False)
        return local_path
    
    def save_as_json(self, df: pd.DataFrame, table_name: str) -> str:
        """Save DataFrame as JSON Lines"""
        local_path = f"/tmp/{table_name}.json"
        
        # Convert types for JSON serialization
        df_json = df.copy()
        for col in df_json.columns:
            if df_json[col].dtype == 'object' and len(df_json[col].dropna()) > 0:
                sample = df_json[col].dropna().iloc[0]
                if isinstance(sample, bytes):
                    import base64
                    df_json[col] = df_json[col].apply(lambda x: base64.b64encode(x).decode('utf-8') 
                                                     if x is not None else None)
            elif pd.api.types.is_datetime64_any_dtype(df_json[col]):
                df_json[col] = df_json[col].astype(str)
        
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
                        print(f"  Uploaded: s3://{self.s3_bucket}/{s3_file_key}")
        else:
            # Upload single file
            self.s3_client.upload_file(local_path, self.s3_bucket, s3_key)
            print(f"  Uploaded: s3://{self.s3_bucket}/{s3_key}")
    
    def create_athena_table_ddl(self, table_name: str, schema: Dict[str, str], 
                               format_type: str, s3_location: str, 
                               partition_columns: List[str] = None) -> str:
        """Generate CREATE TABLE DDL for Athena"""
        
        partition_cols = partition_columns or []
        regular_columns = {k: v for k, v in schema.items() if k not in partition_cols}
        
        # Build column definitions
        columns = []
        for col_name, col_type in regular_columns.items():
            columns.append(f"  `{col_name}` {col_type}")
        
        columns_def = ",\n".join(columns)
        
        # Build partitioned clause
        partition_clause = ""
        if partition_cols:
            partition_defs = []
            for col in partition_cols:
                if col in ['year', 'month', 'day']:
                    partition_defs.append(f"  `{col}` INT")
                else:
                    partition_defs.append(f"  `{col}` STRING")
            partition_clause = "PARTITIONED BY (\n" + ",\n".join(partition_defs) + "\n)"
        
        # Build CREATE TABLE statement
        if format_type == 'parquet':
            ddl = f"""CREATE EXTERNAL TABLE IF NOT EXISTS `{self.database_name}`.`{table_name}` (
{columns_def}
)"""
            if partition_clause:
                ddl += f"\n{partition_clause}"
            ddl += f"""
STORED AS PARQUET
LOCATION '{s3_location}'
TBLPROPERTIES ('parquet.compression'='SNAPPY')"""
        
        elif format_type == 'csv':
            ddl = f"""CREATE EXTERNAL TABLE IF NOT EXISTS `{self.database_name}`.`{table_name}` (
{columns_def}
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '{s3_location}'
TBLPROPERTIES ('skip.header.line.count'='1')"""
        
        elif format_type == 'json':
            ddl = f"""CREATE EXTERNAL TABLE IF NOT EXISTS `{self.database_name}`.`{table_name}` (
{columns_def}
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION '{s3_location}'"""
        
        return ddl + ";"
    
    def execute_athena_query(self, query: str) -> Dict[str, Any]:
        """Execute Athena query and wait for results"""
        print(f"\n  Executing Athena query...")
        
        response = self.athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': self.database_name},
            ResultConfiguration={'OutputLocation': self.athena_output_location}
        )
        
        query_execution_id = response['QueryExecutionId']
        
        # Wait for query to complete
        while True:
            query_status = self.athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            status = query_status['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            
            time.sleep(1)
        
        if status == 'SUCCEEDED':
            print(f"  ✓ Query executed successfully")
            return {'status': 'success', 'query_execution_id': query_execution_id}
        else:
            error_msg = query_status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
            print(f"  ✗ Query failed: {error_msg}")
            return {'status': 'failed', 'error': error_msg}
    
    def create_database(self):
        """Create Athena database if it doesn't exist"""
        print(f"\n{'='*60}")
        print(f"Creating database: {self.database_name}")
        
        query = f"CREATE DATABASE IF NOT EXISTS {self.database_name};"
        result = self.execute_athena_query(query)
        
        if result['status'] == 'success':
            print(f"✓ Database '{self.database_name}' is ready")
        
        return result
    
    def create_table_with_data(self, table_name: str, schema: Dict[str, str], 
                              partition_columns: List[str], file_format: str, 
                              size: str):
        """
        Create Athena table and populate with data
        
        Args:
            table_name: Name of the table
            schema: Dict of column_name: data_type
            partition_columns: List of partition column names
            file_format: 'csv', 'parquet', or 'json'
            size: 'small' (10K), 'medium' (100K), or 'large' (5M)
        """
        print(f"\n{'='*60}")
        print(f"Creating table: {table_name}")
        print(f"  Format: {file_format.upper()}")
        print(f"  Size: {size} ({self.SIZE_MAPPING[size]:,} rows)")
        print(f"  Partitions: {partition_columns if partition_columns else 'None'}")
        
        # Generate data
        num_rows = self.SIZE_MAPPING[size]
        df = self.generate_dataframe(schema, num_rows, partition_columns)
        
        print(f"\n  Generated {len(df):,} rows with {len(df.columns)} columns")
        
        # Save data locally
        if file_format == 'parquet':
            local_path = self.save_as_parquet(df, table_name, partition_columns)
        elif file_format == 'csv':
            local_path = self.save_as_csv(df, table_name)
        elif file_format == 'json':
            local_path = self.save_as_json(df, table_name)
        
        # Upload to S3
        s3_key = f"{self.s3_prefix}/{table_name}"
        print(f"\n  Uploading to S3...")
        self.upload_to_s3(local_path, s3_key)
        
        # Generate and execute DDL
        s3_location = f"s3://{self.s3_bucket}/{s3_key}/"
        ddl = self.create_athena_table_ddl(table_name, schema, file_format, 
                                          s3_location, partition_columns)
        
        print(f"\n  Creating Athena table...")
        print(f"  DDL:\n{ddl}\n")
        
        result = self.execute_athena_query(ddl)
        
        # If partitioned, run MSCK REPAIR TABLE
        if partition_columns and result['status'] == 'success':
            print(f"\n  Discovering partitions...")
            repair_query = f"MSCK REPAIR TABLE {self.database_name}.{table_name};"
            self.execute_athena_query(repair_query)
        
        # Clean up local files
        import shutil
        if os.path.isdir(local_path):
            shutil.rmtree(local_path)
        else:
            os.remove(local_path)
        
        print(f"\n✓ Table '{table_name}' created successfully!")
        print(f"  Location: {s3_location}")
        
        return result


def parse_schema_from_args(schema_str: str) -> Dict[str, str]:
    """
    Parse schema from command line string
    Format: "col1:TYPE1,col2:TYPE2,..."
    Example: "id:INT,name:STRING,address:STRUCT<city:STRING,zip:INT>"
    """
    schema = {}
    
    # Split by commas, but respect nested structures
    depth = 0
    current_col = ""
    
    for char in schema_str:
        if char == '<':
            depth += 1
            current_col += char
        elif char == '>':
            depth -= 1
            current_col += char
        elif char == ',' and depth == 0:
            # Column separator
            col_name, col_type = current_col.split(':', 1)
            schema[col_name.strip()] = col_type.strip()
            current_col = ""
        else:
            current_col += char
    
    # Add last column
    if current_col:
        col_name, col_type = current_col.split(':', 1)
        schema[col_name.strip()] = col_type.strip()
    
    return schema


def main():
    parser = argparse.ArgumentParser(
        description='Flexible Athena Table Generator with STRUCT Support',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Simple table with basic types
  python athena_flexible_table_generator.py \\
    --s3-bucket my-bucket \\
    --s3-prefix data/test \\
    --database test_db \\
    --athena-output s3://my-bucket/athena-results/ \\
    --table-name users \\
    --schema "id:INT,name:STRING,age:TINYINT,email:VARCHAR(100)" \\
    --format parquet \\
    --size small

  # Table with STRUCT type
  python athena_flexible_table_generator.py \\
    --s3-bucket my-bucket \\
    --s3-prefix data/test \\
    --database test_db \\
    --athena-output s3://my-bucket/athena-results/ \\
    --table-name customers \\
    --schema "id:BIGINT,name:STRING,contact:STRUCT<email:STRING,phone:STRING>,address:STRUCT<street:STRING,city:STRING,zip:INT>" \\
    --format parquet \\
    --size medium

  # Partitioned table
  python athena_flexible_table_generator.py \\
    --s3-bucket my-bucket \\
    --s3-prefix data/test \\
    --database test_db \\
    --athena-output s3://my-bucket/athena-results/ \\
    --table-name orders \\
    --schema "order_id:BIGINT,customer_id:INT,amount:DECIMAL(10,2),items:ARRAY<STRING>" \\
    --partitions "year,month" \\
    --format parquet \\
    --size large

  # Complex nested structures
  python athena_flexible_table_generator.py \\
    --s3-bucket my-bucket \\
    --s3-prefix data/test \\
    --database test_db \\
    --athena-output s3://my-bucket/athena-results/ \\
    --table-name events \\
    --schema "event_id:STRING,timestamp:TIMESTAMP,user:STRUCT<id:INT,name:STRING,profile:STRUCT<age:INT,country:STRING>>,tags:ARRAY<STRING>,metadata:MAP<STRING,STRING>" \\
    --format json \\
    --size small
        """
    )
    
    parser.add_argument('--s3-bucket', required=True, help='S3 bucket name')
    parser.add_argument('--s3-prefix', required=True, help='S3 prefix for data')
    parser.add_argument('--database', required=True, help='Athena database name')
    parser.add_argument('--athena-output', required=True, 
                       help='S3 location for Athena query results (e.g., s3://bucket/path/)')
    parser.add_argument('--table-name', required=True, help='Table name')
    parser.add_argument('--schema', required=True, 
                       help='Schema in format: "col1:TYPE1,col2:TYPE2,..." (supports STRUCT, ARRAY, MAP)')
    parser.add_argument('--partitions', default='', 
                       help='Comma-separated partition column names (e.g., "year,month")')
    parser.add_argument('--format', choices=['csv', 'parquet', 'json'], 
                       default='parquet', help='File format')
    parser.add_argument('--size', choices=['small', 'medium', 'large'], 
                       default='small', 
                       help='Data size: small (10K rows), medium (100K rows), large (5M rows)')
    
    args = parser.parse_args()
    
    # Parse schema
    schema = parse_schema_from_args(args.schema)
    
    # Parse partitions
    partition_columns = [p.strip() for p in args.partitions.split(',') 
                        if p.strip()] if args.partitions else []
    
    # Create generator
    generator = FlexibleAthenaTableGenerator(
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        database_name=args.database,
        athena_output_location=args.athena_output
    )
    
    # Create database
    generator.create_database()
    
    # Create table with data
    generator.create_table_with_data(
        table_name=args.table_name,
        schema=schema,
        partition_columns=partition_columns,
        file_format=args.format,
        size=args.size
    )
    
    print(f"\n{'='*60}")
    print(f"✅ Complete!")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()

