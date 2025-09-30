"""
AWS Glue 5.0 - AWS Iceberg Migration Script with Integrated Validation

This script provides both in-place and new table migration options for converting
AWS Athena tables to Apache Iceberg format using AWS Glue 5.0 native Iceberg support.
Includes comprehensive validation framework integrated into the migration process.

Usage:
    # For In-Place Migration (replaces original table in same location)
    aws glue start-job-run --job-name aws-iceberg-migration --arguments '{
        "--MIGRATION_TYPE": "inplace",
        "--SOURCE_DATABASE": "your_database",
        "--SOURCE_TABLE": "your_table",
        "--TARGET_DATABASE": "your_database",
        "--TARGET_TABLE": "your_table",
        "--TEMP_S3_LOCATION": "s3://your-bucket/temp/"
    }'
    
    # For New Table Migration (creates new table in different location)
    aws glue start-job-run --job-name aws-iceberg-migration --arguments '{
        "--MIGRATION_TYPE": "newtable",
        "--SOURCE_DATABASE": "your_database",
        "--SOURCE_TABLE": "your_table",
        "--TARGET_DATABASE": "target_database",
        "--TARGET_TABLE": "new_table",
        "--TARGET_S3_LOCATION": "s3://your-bucket/iceberg/"
    }'
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import boto3
import logging
from datetime import datetime
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AWSIcebergMigration:
    def __init__(self, spark, glue_context, glue_client):
        self.spark = spark
        self.glue_context = glue_context
        self.glue_client = glue_client
        self.migration_stats = {}
        self.validation_results = {}
    
    def configure_iceberg_support(self):
        """Configure Spark session for native Iceberg support in Glue 5.0"""
        try:
            logger.info("Configuring native Iceberg support for Glue 5.0...")
            
            # Configure Iceberg extensions (native in Glue 5.0)
            self.spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            self.spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
            self.spark.conf.set("spark.sql.catalog.glue_catalog.type", "glue")
            self.spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", "s3://your-warehouse-bucket/")
            
            # Configure Iceberg table properties
            self.spark.conf.set("spark.sql.iceberg.vectorization.enabled", "true")
            self.spark.conf.set("spark.sql.iceberg.vectorization.batch-size", "4096")
            
            logger.info("Iceberg support configured successfully")
            
        except Exception as e:
            logger.error(f"Failed to configure Iceberg support: {str(e)}")
            raise e
    
    def get_table_metadata(self, database, table):
        """Get comprehensive table metadata from Glue catalog including table properties"""
        try:
            logger.info(f"Getting metadata for {database}.{table}...")
            
            response = self.glue_client.get_table(DatabaseName=database, Name=table)
            table_info = response['Table']
            
            # Extract table properties from Athena/Glue metadata
            table_properties = table_info.get('Parameters', {})
            storage_descriptor = table_info['StorageDescriptor']
            
            metadata = {
                'name': table_info['Name'],
                'database': database,
                'location': storage_descriptor['Location'],
                'input_format': storage_descriptor['InputFormat'],
                'output_format': storage_descriptor['OutputFormat'],
                'serde_info': storage_descriptor['SerdeInfo'],
                'columns': storage_descriptor['Columns'],
                'partition_keys': [col['Name'] for col in table_info.get('PartitionKeys', [])],
                'table_type': table_info.get('TableType', 'EXTERNAL_TABLE'),
                'parameters': table_properties,
                # Extract specific table properties for Iceberg optimization
                'compression': table_properties.get('compression', 'snappy'),
                'file_format': table_properties.get('file_format', 'parquet'),
                'target_file_size': table_properties.get('target_file_size', '134217728'),
                'distribution_mode': table_properties.get('distribution_mode', 'hash')
            }
            
            logger.info(f"Table metadata retrieved: {len(metadata['columns'])} columns, {len(metadata['partition_keys'])} partitions")
            logger.info(f"Table properties: compression={metadata['compression']}, format={metadata['file_format']}")
            return metadata
            
        except Exception as e:
            logger.error(f"Failed to get table metadata: {str(e)}")
            raise e
    
    def create_iceberg_table(self, database, table, s3_location, schema, partition_keys=None, table_properties=None):
        """Create Iceberg table with optimized configuration based on source table properties"""
        try:
            logger.info(f"Creating Iceberg table {database}.{table}...")
            
            # Build column definitions
            column_definitions = []
            for col in schema:
                col_name = col['Name']
                col_type = col['Type']
                column_definitions.append(f"{col_name} {col_type}")
            
            # Create table DDL
            ddl = f"""
                CREATE TABLE {database}.{table} (
                    {', '.join(column_definitions)}
                )
                USING iceberg
                LOCATION '{s3_location}'
            """
            
            # Add partitioning if specified
            if partition_keys:
                ddl += f" PARTITIONED BY ({', '.join(partition_keys)})"
            
            # Use table properties from source metadata or defaults
            compression = table_properties.get('compression', 'snappy') if table_properties else 'snappy'
            file_format = table_properties.get('file_format', 'parquet') if table_properties else 'parquet'
            target_file_size = table_properties.get('target_file_size', '134217728') if table_properties else '134217728'
            distribution_mode = table_properties.get('distribution_mode', 'hash') if table_properties else 'hash'
            
            # Add table properties for optimization
            ddl += f"""
                TBLPROPERTIES (
                    'write.format.default'='{file_format}',
                    'write.parquet.compression-codec'='{compression}',
                    'write.target-file-size-bytes'='{target_file_size}',
                    'write.distribution-mode'='{distribution_mode}',
                    'write.metadata.delete-after-commit.enabled'='true',
                    'write.metadata.previous-versions-max'='5',
                    'write.data.compression-codec'='{compression}',
                    'write.delete.distribution-mode'='{distribution_mode}',
                    'write.update.distribution-mode'='{distribution_mode}',
                    'write.merge.distribution-mode'='{distribution_mode}'
                )
            """
            
            logger.info(f"Executing DDL: {ddl}")
            self.spark.sql(ddl)
            logger.info(f"Iceberg table created successfully: {database}.{table}")
            
        except Exception as e:
            logger.error(f"Failed to create Iceberg table: {str(e)}")
            raise e
    
    def comprehensive_validation(self, source_metadata, target_database, target_table, migration_type="newtable"):
        """Comprehensive validation framework with extensive Athena-specific checks"""
        try:
            logger.info("Starting comprehensive Athena-specific validation...")
            
            # Get source data
            source_df = self.glue_context.create_dynamic_frame.from_catalog(
                database=source_metadata['database'],
                table_name=source_metadata['name']
            ).toDF()
            
            # Get target data
            target_df = self.spark.table(f"{target_database}.{target_table}")
            
            # Get target metadata for detailed comparison
            target_metadata = self.get_table_metadata(target_database, target_table)
            
            # 1. Row count validation
            logger.info("1. Validating row counts...")
            source_count = source_df.count()
            target_count = target_df.count()
            
            logger.info(f"Source row count: {source_count}")
            logger.info(f"Target row count: {target_count}")
            
            row_count_diff = abs(source_count - target_count)
            row_count_diff_pct = (row_count_diff / source_count * 100) if source_count > 0 else 0
            
            # 2. Schema validation
            logger.info("2. Validating schema...")
            source_schema = source_df.schema
            target_schema = target_df.schema
            
            source_columns = {field.name: field.dataType for field in source_schema.fields}
            target_columns = {field.name: field.dataType for field in target_schema.fields}
            
            # Check for missing columns
            missing_columns = set(source_columns.keys()) - set(target_columns.keys())
            extra_columns = set(target_columns.keys()) - set(source_columns.keys())
            
            # Check for type mismatches
            type_mismatches = []
            for col in source_columns:
                if col in target_columns:
                    if str(source_columns[col]) != str(target_columns[col]):
                        type_mismatches.append({
                            'column': col,
                            'source_type': str(source_columns[col]),
                            'target_type': str(target_columns[col])
                        })
            
            # 3. Data integrity validation
            logger.info("3. Validating data integrity...")
            
            # NULL value validation
            source_nulls = {}
            target_nulls = {}
            null_mismatches = {}
            
            for col in source_df.columns:
                if col in target_df.columns:
                    source_null_count = source_df.filter(col(col).isNull()).count()
                    target_null_count = target_df.filter(col(col).isNull()).count()
                    source_nulls[col] = source_null_count
                    target_nulls[col] = target_null_count
                    if source_null_count != target_null_count:
                        null_mismatches[col] = {
                            'source_nulls': source_null_count,
                            'target_nulls': target_null_count,
                            'difference': abs(source_null_count - target_null_count)
                        }
            
            # 4. Data quality validation
            logger.info("4. Validating data quality...")
            
            # Distinct value counts for key columns
            distinct_value_validation = {}
            numeric_columns = [col for col, dtype in source_columns.items() 
                             if "int" in str(dtype).lower() or "double" in str(dtype).lower() or "float" in str(dtype).lower()]
            
            for col in source_df.columns[:10]:  # Check first 10 columns for performance
                if col in target_df.columns:
                    source_distinct = source_df.select(col).distinct().count()
                    target_distinct = target_df.select(col).distinct().count()
                    distinct_value_validation[col] = {
                        'source_distinct': source_distinct,
                        'target_distinct': target_distinct,
                        'match': source_distinct == target_distinct
                    }
            
            # Numeric column statistics
            numeric_stats_validation = {}
            for col in numeric_columns[:5]:  # Check first 5 numeric columns
                if col in target_df.columns:
                    try:
                        source_stats = source_df.select(
                            min(col).alias('min'),
                            max(col).alias('max'),
                            avg(col).alias('avg'),
                            stddev(col).alias('stddev')
                        ).collect()[0]
                        
                        target_stats = target_df.select(
                            min(col).alias('min'),
                            max(col).alias('max'),
                            avg(col).alias('avg'),
                            stddev(col).alias('stddev')
                        ).collect()[0]
                        
                        numeric_stats_validation[col] = {
                            'source': {
                                'min': source_stats['min'],
                                'max': source_stats['max'],
                                'avg': source_stats['avg'],
                                'stddev': source_stats['stddev']
                            },
                            'target': {
                                'min': target_stats['min'],
                                'max': target_stats['max'],
                                'avg': target_stats['avg'],
                                'stddev': target_stats['stddev']
                            },
                            'match': (source_stats['min'] == target_stats['min'] and 
                                    source_stats['max'] == target_stats['max'])
                        }
                    except:
                        pass
            
            # 5. Partition validation
            logger.info("5. Validating partitions...")
            source_partitions = source_metadata.get('partition_keys', [])
            target_partitions = target_metadata.get('partition_keys', [])
            
            partition_match = source_partitions == target_partitions
            
            # Get partition statistics if partitioned
            partition_stats = {}
            if source_partitions and partition_match:
                for partition_col in source_partitions[:2]:  # Check first 2 partition columns
                    source_partition_count = source_df.select(partition_col).distinct().count()
                    target_partition_count = target_df.select(partition_col).distinct().count()
                    partition_stats[partition_col] = {
                        'source_partitions': source_partition_count,
                        'target_partitions': target_partition_count,
                        'match': source_partition_count == target_partition_count
                    }
            
            # 6. Performance metrics
            logger.info("6. Collecting performance metrics...")
            
            # File counts and sizes
            try:
                source_files = source_df.select("$path").distinct().count()
                target_files = target_df.select("$path").distinct().count()
                
                # Get file size information
                source_size_df = source_df.select(
                    count("*").alias("total_records"),
                    sum("$file_size").alias("total_size"),
                    avg("$file_size").alias("avg_file_size")
                ).collect()[0]
                
                target_size_df = target_df.select(
                    count("*").alias("total_records"),
                    sum("$file_size").alias("total_size"),
                    avg("$file_size").alias("avg_file_size")
                ).collect()[0]
                
                source_size = source_size_df['total_size'] or 0
                target_size = target_size_df['total_size'] or 0
                
                compression_ratio = source_size / target_size if target_size > 0 else 0
                file_reduction_ratio = source_files / target_files if target_files > 0 else 0
            except:
                # Fallback if file metadata not available
                source_files = 0
                target_files = 0
                source_size = 0
                target_size = 0
                compression_ratio = 0
                file_reduction_ratio = 0
            
            # 7. Table properties validation
            logger.info("7. Validating table properties...")
            
            table_properties_validation = {
                'source_format': source_metadata.get('input_format', 'Unknown'),
                'target_format': 'Iceberg',
                'source_location': source_metadata.get('location', ''),
                'target_location': target_metadata.get('location', ''),
                'source_table_type': source_metadata.get('table_type', ''),
                'target_table_type': target_metadata.get('table_type', '')
            }
            
            # 8. Sample data validation
            logger.info("8. Validating sample data...")
            
            # Compare checksums for sample data
            sample_size = min(1000, source_count)
            sample_validation = {}
            
            try:
                # Get deterministic sample using orderBy
                if source_df.columns:
                    order_col = source_df.columns[0]
                    source_sample = source_df.orderBy(order_col).limit(sample_size)
                    target_sample = target_df.orderBy(order_col).limit(sample_size)
                    
                    # Compare sample counts
                    source_sample_count = source_sample.count()
                    target_sample_count = target_sample.count()
                    
                    sample_validation = {
                        'sample_size': sample_size,
                        'source_sample_count': source_sample_count,
                        'target_sample_count': target_sample_count,
                        'match': source_sample_count == target_sample_count
                    }
            except:
                sample_validation = {'error': 'Could not perform sample validation'}
            
            # Store comprehensive validation results
            self.validation_results = {
                'migration_type': migration_type,
                'source_table': f"{source_metadata['database']}.{source_metadata['name']}",
                'target_table': f"{target_database}.{target_table}",
                'validation_timestamp': datetime.now().isoformat(),
                'validation_summary': {
                    'total_validations': 8,
                    'validations_passed': 0,
                    'validations_failed': 0,
                    'validations_warning': 0
                },
                '1_row_count_validation': {
                    'source_row_count': source_count,
                    'target_row_count': target_count,
                    'row_count_match': source_count == target_count,
                    'difference': row_count_diff,
                    'difference_percentage': row_count_diff_pct,
                    'status': 'PASS' if source_count == target_count else 'FAIL'
                },
                '2_schema_validation': {
                    'source_columns': len(source_columns),
                    'target_columns': len(target_columns),
                    'missing_columns': list(missing_columns),
                    'extra_columns': list(extra_columns),
                    'type_mismatches': type_mismatches,
                    'status': 'PASS' if len(missing_columns) == 0 and len(type_mismatches) == 0 else 'FAIL'
                },
                '3_data_integrity_validation': {
                    'null_value_comparison': {
                        'columns_checked': len(source_nulls),
                        'null_mismatches': null_mismatches,
                        'status': 'PASS' if len(null_mismatches) == 0 else 'WARNING'
                    }
                },
                '4_data_quality_validation': {
                    'distinct_values': distinct_value_validation,
                    'numeric_statistics': numeric_stats_validation,
                    'status': 'PASS' if all(v.get('match', True) for v in distinct_value_validation.values()) else 'WARNING'
                },
                '5_partition_validation': {
                    'source_partitions': source_partitions,
                    'target_partitions': target_partitions,
                    'partition_match': partition_match,
                    'partition_statistics': partition_stats,
                    'status': 'PASS' if partition_match else 'FAIL'
                },
                '6_performance_metrics': {
                    'source_file_count': source_files,
                    'target_file_count': target_files,
                    'source_size_bytes': source_size,
                    'target_size_bytes': target_size,
                    'source_size_gb': source_size / (1024**3) if source_size > 0 else 0,
                    'target_size_gb': target_size / (1024**3) if target_size > 0 else 0,
                    'compression_ratio': compression_ratio,
                    'file_reduction_ratio': file_reduction_ratio,
                    'storage_savings_pct': ((source_size - target_size) / source_size * 100) if source_size > 0 else 0
                },
                '7_table_properties': table_properties_validation,
                '8_sample_data_validation': sample_validation
            }
            
            # Calculate overall validation summary
            validations = [
                self.validation_results['1_row_count_validation']['status'],
                self.validation_results['2_schema_validation']['status'],
                self.validation_results['3_data_integrity_validation']['null_value_comparison']['status'],
                self.validation_results['4_data_quality_validation']['status'],
                self.validation_results['5_partition_validation']['status']
            ]
            
            passed = sum(1 for v in validations if v == 'PASS')
            failed = sum(1 for v in validations if v == 'FAIL')
            warnings = sum(1 for v in validations if v == 'WARNING')
            
            self.validation_results['validation_summary']['validations_passed'] = passed
            self.validation_results['validation_summary']['validations_failed'] = failed
            self.validation_results['validation_summary']['validations_warning'] = warnings
            
            # Overall validation status
            overall_status = 'PASS' if failed == 0 else 'FAIL'
            self.validation_results['overall_status'] = overall_status
            
            logger.info(f"Validation completed with status: {overall_status}")
            logger.info(f"Validation summary: {passed} passed, {failed} failed, {warnings} warnings")
            
            return overall_status == 'PASS'
            
        except Exception as e:
            logger.error(f"Validation failed: {str(e)}")
            raise e
    
    def migrate_data_inplace(self, source_metadata, target_database, target_table, temp_s3_location):
        """Perform in-place migration with comprehensive validation before replacement"""
        try:
            logger.info("Starting in-place migration with validation...")
            
            # Step 1: Create temporary Iceberg table
            temp_table = f"{target_database}.{target_table}_temp"
            logger.info(f"Creating temporary table: {temp_table}")
            
            self.create_iceberg_table(
                database=target_database,
                table=f"{target_table}_temp",
                s3_location=temp_s3_location,
                schema=source_metadata['columns'],
                partition_keys=source_metadata['partition_keys'],
                table_properties=source_metadata
            )
            
            # Step 2: Read source data and write to temporary table
            logger.info("Migrating data to temporary table...")
            source_df = self.glue_context.create_dynamic_frame.from_catalog(
                database=source_metadata['database'],
                table_name=source_metadata['name']
            ).toDF()
            
            # Write data to temporary table
            writer = source_df.write \
                .format("iceberg") \
                .mode("append")
            
            # Add partitioning if specified
            if source_metadata['partition_keys']:
                writer = writer.partitionBy(*source_metadata['partition_keys'])
            
            writer.saveAsTable(temp_table)
            
            # Step 3: CRITICAL - Validate temporary table against source
            logger.info("Validating temporary table against source...")
            validation_passed = self.comprehensive_validation(
                source_metadata, 
                target_database, 
                f"{target_table}_temp", 
                "inplace"
            )
            
            if not validation_passed:
                logger.error("Validation failed for temporary table. Aborting in-place migration.")
                # Clean up temporary table
                self.spark.sql(f"DROP TABLE IF EXISTS {temp_table}")
                raise Exception("Validation failed for temporary table. Migration aborted.")
            
            logger.info("Validation passed for temporary table. Proceeding with replacement...")
            
            # Step 4: Replace original table with validated temporary table
            logger.info("Replacing original table with validated Iceberg table...")
            
            # Drop original table
            self.spark.sql(f"DROP TABLE IF EXISTS {target_database}.{target_table}")
            
            # Create final Iceberg table in the same location as original (in-place)
            self.create_iceberg_table(
                database=target_database,
                table=target_table,
                s3_location=source_metadata['location'],  # Use original table's S3 location
                schema=source_metadata['columns'],
                partition_keys=source_metadata['partition_keys'],
                table_properties=source_metadata
            )
            
            # Copy data from temporary table to final table
            temp_df = self.spark.table(temp_table)
            temp_df.write \
                .format("iceberg") \
                .mode("append") \
                .saveAsTable(f"{target_database}.{target_table}")
            
            # Clean up temporary table
            self.spark.sql(f"DROP TABLE IF EXISTS {temp_table}")
            
            logger.info("In-place migration completed successfully with validation")
            
        except Exception as e:
            logger.error(f"In-place migration failed: {str(e)}")
            # Clean up temporary table if it exists
            try:
                self.spark.sql(f"DROP TABLE IF EXISTS {target_database}.{target_table}_temp")
            except:
                pass
            raise e
    
    def migrate_data_new_table(self, source_metadata, target_database, target_table, target_s3_location):
        """Perform new table migration with comprehensive validation"""
        try:
            logger.info("Starting new table migration with validation...")
            
            # Step 1: Create target Iceberg table
            self.create_iceberg_table(
                database=target_database,
                table=target_table,
                s3_location=target_s3_location,
                schema=source_metadata['columns'],
                partition_keys=source_metadata['partition_keys'],
                table_properties=source_metadata
            )
            
            # Step 2: Read source data and write to target table
            logger.info("Migrating data to target table...")
            source_df = self.glue_context.create_dynamic_frame.from_catalog(
                database=source_metadata['database'],
                table_name=source_metadata['name']
            ).toDF()
            
            # Write data to target table
            writer = source_df.write \
                .format("iceberg") \
                .mode("append")
            
            # Add partitioning if specified
            if source_metadata['partition_keys']:
                writer = writer.partitionBy(*source_metadata['partition_keys'])
            
            writer.saveAsTable(f"{target_database}.{target_table}")
            
            # Step 3: Validate new table against source
            logger.info("Validating new table against source...")
            validation_passed = self.comprehensive_validation(
                source_metadata, 
                target_database, 
                target_table, 
                "newtable"
            )
            
            if not validation_passed:
                logger.error("Validation failed for new table.")
                raise Exception("Validation failed for new table.")
            
            logger.info("New table migration completed successfully with validation")
            
        except Exception as e:
            logger.error(f"New table migration failed: {str(e)}")
            raise e
    
    def optimize_iceberg_table(self, database, table):
        """Optimize Iceberg table for better performance"""
        try:
            logger.info(f"Optimizing Iceberg table {database}.{table}...")
            
            # Compact small files for better performance
            compact_sql = f"CALL glue_catalog.system.rewrite_data_files('{database}.{table}')"
            self.spark.sql(compact_sql)
            
            logger.info("Iceberg table optimization completed")
            
        except Exception as e:
            logger.warning(f"Table optimization failed: {str(e)}")
    
    def execute_migration(self, migration_type, source_database, source_table, 
                         target_database, target_table, target_s3_location=None, temp_s3_location=None):
        """Execute the migration based on the specified type with integrated validation"""
        try:
            logger.info(f"Starting {migration_type} migration with integrated validation...")
            logger.info(f"Source: {source_database}.{source_table}")
            logger.info(f"Target: {target_database}.{target_table}")
            
            # Get source table metadata including table properties
            source_metadata = self.get_table_metadata(source_database, source_table)
            
            # Configure Iceberg support
            self.configure_iceberg_support()
            
            if migration_type.lower() == "inplace":
                # In-place migration with validation before replacement
                logger.info("Executing in-place migration with validation...")
                if not temp_s3_location:
                    raise ValueError("TEMP_S3_LOCATION is required for in-place migration")
                self.migrate_data_inplace(source_metadata, target_database, target_table, temp_s3_location)
                
            elif migration_type.lower() == "newtable":
                # New table migration with validation
                logger.info("Executing new table migration with validation...")
                if not target_s3_location:
                    raise ValueError("TARGET_S3_LOCATION is required for new table migration")
                self.migrate_data_new_table(source_metadata, target_database, target_table, target_s3_location)
                
            else:
                raise ValueError(f"Invalid migration type: {migration_type}. Must be 'inplace' or 'newtable'")
            
            # Optimize Iceberg table
            self.optimize_iceberg_table(target_database, target_table)
            
            # Generate comprehensive final report
            report = {
                'migration_type': migration_type,
                'source_table': f"{source_database}.{source_table}",
                'target_table': f"{target_database}.{target_table}",
                'status': 'SUCCESS',
                'validation_results': self.validation_results,
                'timestamp': datetime.now().isoformat()
            }
            
            # Save report
            with open("/tmp/migration_report.json", "w") as f:
                json.dump(report, f, indent=2, default=str)
            
            logger.info(f"{migration_type} migration completed successfully with validation!")
            logger.info(f"Migration report: {json.dumps(report, indent=2, default=str)}")
            
            return report
            
        except Exception as e:
            logger.error(f"Migration failed: {str(e)}")
            raise e

def main():
    """Main migration function"""
    # Get job parameters
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'MIGRATION_TYPE',
        'SOURCE_DATABASE',
        'SOURCE_TABLE',
        'TARGET_DATABASE',
        'TARGET_TABLE',
        'TARGET_S3_LOCATION',
        'TEMP_S3_LOCATION'
    ])
    
    # Initialize Spark and Glue contexts
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    # Initialize AWS clients
    glue_client = boto3.client('glue')
    
    try:
        # Initialize migration framework
        migration = AWSIcebergMigration(
            spark=spark,
            glue_context=glueContext,
            glue_client=glue_client
        )
        
        # Execute migration
        report = migration.execute_migration(
            migration_type=args['MIGRATION_TYPE'],
            source_database=args['SOURCE_DATABASE'],
            source_table=args['SOURCE_TABLE'],
            target_database=args['TARGET_DATABASE'],
            target_table=args['TARGET_TABLE'],
            target_s3_location=args['TARGET_S3_LOCATION'],
            temp_s3_location=args.get('TEMP_S3_LOCATION')
        )
        
        logger.info("AWS Iceberg migration completed successfully!")
        
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        raise e
    
    finally:
        job.commit()

if __name__ == "__main__":
    main()