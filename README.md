# AWS Glue 5.0 - Iceberg Migration Solution

## Overview

This project provides a unified solution for migrating AWS Athena tables to Apache Iceberg format using AWS Glue 5.0's native Iceberg support. The solution includes a single migration script, comprehensive validation framework, and SQL analysis tools.

## Project Structure

```
awsiceberg2/
├── README.md                           # This comprehensive guide
├── aws_iceberg_migration.py            # Single migration script with integrated validation framework
├── pre_migration_analysis.sql          # Pre-migration analysis queries
└── post_migration_verification.sql     # Post-migration verification queries
```

## Key Features

✅ **Single Unified Script**: Handles both in-place and new table migrations with integrated validation  
✅ **Native Iceberg Support**: No external JAR files required with Glue 5.0  
✅ **Comprehensive Validation**: 8 categories of Athena-specific validation checks  
✅ **Table Properties**: Automatically fetches and applies source table properties  
✅ **SQL Analysis Tools**: Pre and post migration analysis queries  
✅ **Production Ready**: Error handling, logging, and optimization  
✅ **Performance Optimized**: In-place migration uses table rename to avoid duplicate data writes (~50% faster)  
✅ **Flexible Parameters**: Optional warehouse location and conditional S3 location requirements  

### Validation Highlights
- **8 Validation Categories**: Row counts, schema, data integrity, quality, partitions, performance, properties, samples
- **Athena-Specific Checks**: Query performance, partition pruning, Iceberg features
- **Detailed Reporting**: Comprehensive JSON reports with pass/fail/warning status
- **Automatic Rollback**: Fails migration if critical validations don't pass
- **Performance Metrics**: Compression ratios, file reduction, storage savings

### Recent Improvements (Latest Version)
- ✅ **Fixed Missing Imports**: Added explicit PySpark function imports to prevent runtime errors
- ✅ **Performance Optimization**: In-place migration now uses table rename via Glue API to avoid duplicate data writes (~50% faster)
- ✅ **Flexible Parameters**: Made `TARGET_S3_LOCATION` and `TEMP_S3_LOCATION` conditionally required based on migration type
- ✅ **Optional Warehouse**: Removed hardcoded S3 warehouse location, now optional parameter
- ✅ **Enhanced Error Handling**: Improved metadata validation with specific error messages for missing tables, storage descriptors, and S3 locations
- ✅ **Better Validation**: Added null-safety checks for all metadata field extractions  

## Migration Types

### 1. In-Place Migration
Replaces the original table with Iceberg format while maintaining the same name and database.

**Process**: Creates temporary Iceberg table in temp location → Validates against source → Replaces original table in same location only if validation passes

**Use Case**: When you want to upgrade existing tables to Iceberg without changing application references.

### 2. New Table Migration
Creates a new Iceberg table alongside the original table.

**Process**: Creates new Iceberg table → Migrates data → Validates against source

**Use Case**: When you want to test Iceberg performance or maintain both formats during transition.

## Quick Start

### 1. Prerequisites

- AWS Glue 5.0 (or later) with native Iceberg support
- AWS Athena
- Amazon S3
- Appropriate IAM permissions

### 2. Required IAM Permissions

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:CreateJob",
                "glue:StartJobRun",
                "glue:GetJobRun",
                "glue:GetTable",
                "glue:CreateTable",
                "glue:UpdateTable",
                "glue:DeleteTable",
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults",
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": "*"
        }
    ]
}
```

### 3. Job Parameters

The migration script accepts parameters via AWS Glue job arguments:

**Note**: The source S3 location is automatically detected from the Glue catalog metadata, so you don't need to specify it.

#### Required Parameters (for all migration types)
- `--MIGRATION_TYPE`: Migration type (`inplace` or `newtable`)
- `--SOURCE_DATABASE`: Source database name
- `--SOURCE_TABLE`: Source table name
- `--TARGET_DATABASE`: Target database name
- `--TARGET_TABLE`: Target table name

#### Conditionally Required Parameters
- `--TEMP_S3_LOCATION`: **Required for `inplace` migration** - Temporary table location for validation (e.g., `s3://your-bucket/temp/`)
- `--TARGET_S3_LOCATION`: **Required for `newtable` migration** - New Iceberg table location (e.g., `s3://your-bucket/iceberg/`)

#### Optional Parameters
- `--WAREHOUSE_LOCATION`: Optional S3 warehouse location for Iceberg catalog (e.g., `s3://your-bucket/warehouse/`)
  - If not provided, Glue catalog manages locations automatically

## Usage

### In-Place Migration

**Required Parameters**: All base parameters + `TEMP_S3_LOCATION`

```bash
aws glue start-job-run --job-name aws-iceberg-migration --arguments '{
    "--MIGRATION_TYPE": "inplace",
    "--SOURCE_DATABASE": "production_db",
    "--SOURCE_TABLE": "sales_data",
    "--TARGET_DATABASE": "production_db",
    "--TARGET_TABLE": "sales_data",
    "--TEMP_S3_LOCATION": "s3://data-lake/temp/sales/"
}'
```

### New Table Migration

**Required Parameters**: All base parameters + `TARGET_S3_LOCATION`

```bash
aws glue start-job-run --job-name aws-iceberg-migration --arguments '{
    "--MIGRATION_TYPE": "newtable",
    "--SOURCE_DATABASE": "production_db",
    "--SOURCE_TABLE": "sales_data",
    "--TARGET_DATABASE": "production_db",
    "--TARGET_TABLE": "sales_data_iceberg",
    "--TARGET_S3_LOCATION": "s3://data-lake/iceberg/sales/"
}'
```

### Optional: Custom Warehouse Location

You can optionally specify a custom Iceberg warehouse location:

```bash
aws glue start-job-run --job-name aws-iceberg-migration --arguments '{
    "--MIGRATION_TYPE": "newtable",
    "--SOURCE_DATABASE": "production_db",
    "--SOURCE_TABLE": "sales_data",
    "--TARGET_DATABASE": "production_db",
    "--TARGET_TABLE": "sales_data_iceberg",
    "--TARGET_S3_LOCATION": "s3://data-lake/iceberg/sales/",
    "--WAREHOUSE_LOCATION": "s3://data-lake/warehouse/"
}'
```

## Migration Process

### 1. Pre-Migration Analysis

Run the pre-migration analysis queries to understand your source table:

```sql
-- Get table metadata
DESCRIBE TABLE your_database.your_table;

-- Get row count
SELECT COUNT(*) as row_count FROM your_database.your_table;

-- Get data volume
SELECT 
    COUNT(*) as file_count,
    SUM(size) as total_size_bytes,
    SUM(size)/1024/1024/1024 as total_size_gb
FROM (
    SELECT 
        "$path" as file_path,
        CAST("$file_size" AS BIGINT) as size
    FROM your_database.your_table
);

-- Get partition information
SELECT DISTINCT partition_col1, partition_col2 
FROM your_database.your_table 
ORDER BY partition_col1, partition_col2;
```

### 2. Migration Execution

The unified script automatically:
- Configures native Iceberg support
- Creates optimized Iceberg tables
- Migrates data with validation
- Optimizes table performance
- Generates comprehensive reports

### 3. Post-Migration Verification

Run the post-migration verification queries to validate the migration:

```sql
-- 1. Verify row counts match
SELECT 
    (SELECT COUNT(*) FROM source_table) as source_count,
    (SELECT COUNT(*) FROM target_table) as target_count;

-- 2. Check schema compatibility
DESCRIBE TABLE target_database.target_table;

-- 3. Verify data integrity
SELECT COUNT(*) as null_count FROM target_table WHERE key_column IS NULL;

-- 4. Test Athena query performance
-- Compare query execution times between source and target
EXPLAIN ANALYZE SELECT * FROM source_table WHERE condition;
EXPLAIN ANALYZE SELECT * FROM target_table WHERE condition;

-- 5. Validate Iceberg-specific features
-- Time travel capability
SELECT * FROM target_table FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 00:00:00';

-- 6. Check table snapshots
SELECT * FROM target_table.snapshots;

-- 7. Verify partition pruning works
EXPLAIN SELECT * FROM target_table WHERE partition_column = 'value';

-- 8. Test complex aggregations
SELECT 
    partition_column,
    COUNT(*) as cnt,
    SUM(numeric_column) as total,
    AVG(numeric_column) as average
FROM target_table
GROUP BY partition_column;
```

## Comprehensive Athena-Specific Validation Framework

The migration script includes an extensive validation framework with 8 categories of checks specifically designed for Athena table migrations:

### 1. Row Count Validation
- **Exact Match**: Verifies source and target row counts are identical
- **Difference Calculation**: Shows absolute difference and percentage
- **Status**: FAIL if counts don't match

### 2. Schema Validation
- **Column Comparison**: Identifies missing or extra columns
- **Data Type Validation**: Detects type mismatches between source and target
- **Column Count**: Compares total number of columns
- **Status**: FAIL if columns missing or types mismatch

### 3. Data Integrity Validation
- **NULL Value Analysis**: Compares NULL counts for each column
- **Mismatch Detection**: Identifies columns with different NULL patterns
- **Column Coverage**: Validates all columns for NULL consistency
- **Status**: WARNING if NULL counts differ

### 4. Data Quality Validation
- **Distinct Value Counts**: Compares unique values for key columns
- **Numeric Statistics**: Min, max, average, standard deviation for numeric columns
- **Data Distribution**: Ensures data patterns are preserved
- **Status**: WARNING if patterns differ

### 5. Partition Validation
- **Structure Match**: Verifies partition columns are identical
- **Partition Counts**: Compares number of unique partitions per column
- **Partition Statistics**: Detailed analysis of partition distribution
- **Status**: FAIL if partition structure differs

### 6. Performance Metrics
- **File Count Comparison**: Source vs target file counts
- **Storage Analysis**: 
  - Total size in bytes and GB
  - Compression ratio achieved
  - File reduction ratio
  - Storage savings percentage
- **Performance Improvements**: Quantifies optimization benefits

### 7. Table Properties Validation
- **Format Verification**: Confirms conversion to Iceberg format
- **Location Tracking**: Documents S3 locations for both tables
- **Table Type**: Validates external table properties
- **Metadata Preservation**: Ensures critical properties maintained

### 8. Sample Data Validation
- **Deterministic Sampling**: Orders data for consistent comparison
- **Sample Size**: Up to 1000 rows for detailed verification
- **Data Consistency**: Ensures sample data matches between tables

### Validation Report Structure

```json
{
  "validation_summary": {
    "total_validations": 8,
    "validations_passed": 5,
    "validations_failed": 0,
    "validations_warning": 3
  },
  "1_row_count_validation": {
    "source_row_count": 1000000,
    "target_row_count": 1000000,
    "difference": 0,
    "difference_percentage": 0.0,
    "status": "PASS"
  },
  "2_schema_validation": {
    "source_columns": 50,
    "target_columns": 50,
    "missing_columns": [],
    "type_mismatches": [],
    "status": "PASS"
  },
  "3_data_integrity_validation": {
    "null_value_comparison": {
      "columns_checked": 50,
      "null_mismatches": {},
      "status": "PASS"
    }
  },
  "4_data_quality_validation": {
    "distinct_values": {
      "column1": {"source": 100, "target": 100, "match": true}
    },
    "numeric_statistics": {
      "amount": {
        "source": {"min": 0, "max": 1000, "avg": 500},
        "target": {"min": 0, "max": 1000, "avg": 500}
      }
    },
    "status": "PASS"
  },
  "5_partition_validation": {
    "source_partitions": ["year", "month"],
    "target_partitions": ["year", "month"],
    "partition_statistics": {
      "year": {"source": 5, "target": 5, "match": true}
    },
    "status": "PASS"
  },
  "6_performance_metrics": {
    "source_file_count": 1000,
    "target_file_count": 100,
    "source_size_gb": 100.5,
    "target_size_gb": 45.2,
    "compression_ratio": 2.22,
    "file_reduction_ratio": 10.0,
    "storage_savings_pct": 55.0
  }
}
```

### Validation Process Flow

#### In-Place Migration
1. Create temporary Iceberg table in temp location
2. Migrate data to temporary table
3. **Run all 8 validation checks against source**
4. Only proceed if validation passes
5. Replace original table with validated Iceberg table (optimized via table rename to avoid duplicate data writes)
6. Clean up temporary resources

#### New Table Migration
1. Create new Iceberg table
2. Migrate data to new table
3. **Run all 8 validation checks against source**
4. Report validation results
5. Keep both tables for comparison

### Validation Status Levels
- **PASS**: Validation successful, no issues found
- **WARNING**: Minor discrepancies that don't block migration
- **FAIL**: Critical issues that prevent migration completion

### Automatic Validation Actions
- **On FAIL**: Migration aborts, rollback initiated
- **On WARNING**: Migration continues with detailed logging
- **On PASS**: Migration proceeds to next step

### Additional Athena-Specific Validations

Beyond the automated validations, you can perform these Athena-specific checks:

#### Query Compatibility Tests
```sql
-- Test predicate pushdown
EXPLAIN SELECT * FROM iceberg_table WHERE date_column > '2024-01-01';

-- Test join performance
EXPLAIN SELECT * FROM iceberg_table t1 
JOIN other_table t2 ON t1.id = t2.id;

-- Test aggregation pushdown
EXPLAIN SELECT COUNT(*), SUM(amount) FROM iceberg_table;
```

#### Iceberg Metadata Validation
```sql
-- View table history
SELECT * FROM iceberg_table.history;

-- Check manifest files
SELECT * FROM iceberg_table.manifests;

-- Verify data files
SELECT * FROM iceberg_table.files;

-- Check table metadata
SELECT * FROM iceberg_table.metadata_log_entries;
```

#### Performance Comparison Queries
```sql
-- Compare scan performance
SET SESSION query_max_scan_size = '100GB';

-- Original table scan time
SELECT COUNT(*) FROM original_table 
WHERE date_column BETWEEN '2024-01-01' AND '2024-12-31';

-- Iceberg table scan time (should be faster)
SELECT COUNT(*) FROM iceberg_table 
WHERE date_column BETWEEN '2024-01-01' AND '2024-12-31';
```

#### Data Consistency Checks
```sql
-- Hash comparison for data validation
SELECT MD5(CAST(COLLECT_LIST(CONCAT_WS(',', *)) AS STRING)) as checksum
FROM (SELECT * FROM original_table ORDER BY primary_key) t;

-- Compare with Iceberg table
SELECT MD5(CAST(COLLECT_LIST(CONCAT_WS(',', *)) AS STRING)) as checksum
FROM (SELECT * FROM iceberg_table ORDER BY primary_key) t;
```

## Table Properties Integration

The script automatically fetches and applies table properties from the source Athena table:

### Source Table Properties
- **Compression**: Inherits compression settings from source table
- **File Format**: Maintains original file format preferences
- **Target File Size**: Uses source table's file size settings
- **Distribution Mode**: Preserves original distribution settings

### Automatic Property Detection
```python
# The script automatically extracts these properties from Glue catalog:
metadata = {
    'compression': table_properties.get('compression', 'snappy'),
    'file_format': table_properties.get('file_format', 'parquet'),
    'target_file_size': table_properties.get('target_file_size', '134217728'),
    'distribution_mode': table_properties.get('distribution_mode', 'hash')
}
```

## Native Iceberg Support in Glue 5.0

The script leverages Glue 5.0's native Iceberg support:

```python
# Automatic configuration
spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.type", "glue")

# Optional: Set custom warehouse location
# If not provided, Glue catalog manages locations automatically
if warehouse_location:
    spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", warehouse_location)
```

### Glue Job Configuration

```json
{
    "JobName": "aws-iceberg-migration",
    "Role": "arn:aws:iam::ACCOUNT:role/GlueServiceRole",
    "Command": {
        "Name": "glueetl",
        "ScriptLocation": "s3://your-bucket/scripts/aws_iceberg_migration.py",
        "PythonVersion": "3"
    },
    "DefaultArguments": {
        "--job-language": "python",
        "--job-bookmark-option": "job-bookmark-disable",
        "--datalake-formats": "iceberg"
    },
    "MaxCapacity": 10,
    "Timeout": 60,
    "WorkerType": "G.1X",
    "NumberOfWorkers": 10,
    "GlueVersion": "5.0"
}
```

## Migration Statistics

The script provides comprehensive statistics:

```json
{
    "migration_type": "inplace",
    "source_table": "production_db.sales_data",
    "target_table": "production_db.sales_data",
    "status": "SUCCESS",
    "validation_results": {
        "source_row_count": 1000000,
        "target_row_count": 1000000,
        "row_count_match": true,
        "source_columns": 15,
        "target_columns": 15,
        "missing_columns": [],
        "type_mismatches": []
    },
    "statistics": {
        "source_file_count": 100,
        "target_file_count": 50,
        "compression_ratio": 2.0,
        "migration_timestamp": "2024-01-15T10:30:00Z"
    }
}
```

## Best Practices

### Before Migration
1. **Test on Small Dataset**: Always test with a subset first
2. **Backup Data**: Create backups before in-place migration
3. **Check Dependencies**: Verify all applications and queries
4. **Run Pre-Migration Analysis**: Use the provided SQL queries

### During Migration
1. **Monitor Progress**: Watch CloudWatch logs
2. **Check Resources**: Monitor worker utilization
3. **Validate Data**: Run validation queries
4. **Test Queries**: Verify table accessibility

### After Migration
1. **Performance Testing**: Compare query performance
2. **Application Testing**: Test all dependent applications
3. **Run Post-Migration Verification**: Use the provided SQL queries
4. **Documentation**: Update documentation and procedures

## Cost Considerations

### Glue Job Costs
- **G.1X workers**: $0.44 per DPU-hour (small tables)
- **G.2X workers**: $0.88 per DPU-hour (large tables)

### S3 Storage Costs
- **Standard**: $0.023 per GB/month
- **Standard-IA**: $0.0125 per GB/month
- **Glacier**: $0.004 per GB/month

### Athena Query Costs
- $5.00 per TB of data scanned
- Use columnar formats for cost reduction
- Implement partition pruning

## Troubleshooting

### Common Issues

1. **Missing Required Parameters**
   - **Error**: `TEMP_S3_LOCATION is required for in-place migration`
   - **Solution**: Provide `--TEMP_S3_LOCATION` parameter for in-place migrations
   - **Error**: `TARGET_S3_LOCATION is required for new table migration`
   - **Solution**: Provide `--TARGET_S3_LOCATION` parameter for new table migrations

2. **Table Not Found Errors**
   - **Error**: `Table database.table does not exist in Glue catalog`
   - **Solution**: Verify table exists in Glue Data Catalog
   - Run: `aws glue get-table --database-name <db> --name <table>`

3. **Missing Storage Descriptor**
   - **Error**: `Table has no storage descriptor. Cannot migrate`
   - **Solution**: Ensure table has valid storage descriptor in Glue catalog
   - **Error**: `Table has no S3 location defined. Cannot migrate`
   - **Solution**: Verify table has valid S3 location in metadata

4. **Out of Memory Errors**
   - Increase worker count
   - Use G.2X workers
   - Optimize data processing

5. **Timeout Errors**
   - Increase job timeout
   - Optimize query performance
   - Use incremental processing

6. **Schema Mismatch**
   - Check data types
   - Handle NULL values
   - Validate column names

7. **Permission Errors**
   - Verify IAM policies
   - Check S3 bucket permissions
   - Ensure Glue service role has required permissions

### Debugging Steps

1. **Check CloudWatch Logs**
   ```bash
   aws logs describe-log-groups --log-group-name-prefix /aws-glue/jobs/
   ```

2. **Monitor Job Progress**
   ```bash
   aws glue get-job-run --job-name aws-iceberg-migration --run-id RUN_ID
   ```

3. **Validate Data**
   ```sql
   SELECT COUNT(*) FROM database.table_name;
   ```

## Rollback Procedures

### For In-Place Migration
1. **Immediate Rollback** (if migration fails during process):
   ```bash
   # Stop the Glue job
   aws glue stop-job-run --job-name aws-iceberg-migration --job-run-id JOB_RUN_ID
   
   # Restore from backup if available
   aws s3 sync s3://backup-bucket/table-backup/ s3://original-location/
   ```

2. **Post-Migration Rollback** (if issues discovered after migration):
   ```sql
   -- Drop the Iceberg table
   DROP TABLE IF EXISTS database.table_name;
   
   -- Restore original table from backup
   ```

### For New Table Migration
1. **Simple Rollback**:
   ```sql
   -- Drop the new Iceberg table
   DROP TABLE IF EXISTS database.new_table_name;
   
   -- Applications continue using original table
   ```

2. **Data Rollback** (if data was corrupted):
   ```bash
   # Remove the new table data
   aws s3 rm s3://new-table-location/ --recursive
   
   # Drop the table
   aws glue delete-table --database-name database --name new_table_name
   ```

## Performance Optimization

### Table Properties
The script automatically configures optimal table properties:

```sql
-- Optimized table properties
TBLPROPERTIES (
    'write.format.default'='parquet',
    'write.parquet.compression-codec'='snappy',
    'write.target-file-size-bytes'='134217728',
    'write.distribution-mode'='hash',
    'write.metadata.delete-after-commit.enabled'='true',
    'write.metadata.previous-versions-max'='5'
)
```

### File Optimization
- **File Size**: 128MB target file size for optimal performance
- **Compression**: Snappy compression for balanced speed and size
- **Partitioning**: Maintains existing partition structure
- **Compaction**: Automatic small file compaction

## Security Considerations

- **Encryption**: Enable encryption at rest and in transit
- **Access Control**: Use least privilege IAM policies
- **Audit**: Enable CloudTrail for audit logging
- **Network**: Use VPC endpoints for S3 access

## Support Resources

- **AWS Glue Documentation**: https://docs.aws.amazon.com/glue/
- **Apache Iceberg Documentation**: https://iceberg.apache.org/
- **AWS Support**: Create support case if needed
- **AWS Forums**: https://forums.aws.amazon.com/

## Conclusion

This unified solution provides a complete, production-ready approach for migrating AWS Athena tables to Apache Iceberg format using AWS Glue 5.0's native capabilities. The single script handles both migration types while providing comprehensive validation, optimization, and reporting features.

For questions or support, refer to the AWS documentation or create a support case.