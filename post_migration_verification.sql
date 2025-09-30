-- Post-Migration Verification Queries for AWS Athena Iceberg Tables
-- These queries help verify the successful migration to Iceberg format

-- =============================================================================
-- 1. BASIC TABLE VERIFICATION
-- =============================================================================

-- Verify table exists and is accessible
SELECT COUNT(*) as row_count FROM [DATABASE].[TABLE_NAME];

-- Check table type and format
DESCRIBE TABLE [DATABASE].[TABLE_NAME];

-- Verify table properties
SHOW CREATE TABLE [DATABASE].[TABLE_NAME];

-- =============================================================================
-- 2. ICEBERG TABLE PROPERTIES VERIFICATION
-- =============================================================================

-- Check Iceberg-specific properties
SELECT 
    table_name,
    table_type,
    table_schema,
    table_catalog
FROM information_schema.tables 
WHERE table_schema = '[DATABASE]' 
AND table_name = '[TABLE_NAME]';

-- Verify Iceberg table format
SELECT 
    table_name,
    table_type,
    table_schema,
    table_catalog,
    table_properties
FROM information_schema.tables 
WHERE table_schema = '[DATABASE]' 
AND table_name = '[TABLE_NAME]'
AND table_properties LIKE '%iceberg%';

-- =============================================================================
-- 3. SCHEMA VERIFICATION
-- =============================================================================

-- Verify column schema matches source
SELECT 
    column_name,
    ordinal_position,
    column_default,
    is_nullable,
    data_type,
    character_maximum_length,
    numeric_precision,
    numeric_scale
FROM information_schema.columns 
WHERE table_schema = '[DATABASE]' 
AND table_name = '[TABLE_NAME]'
ORDER BY ordinal_position;

-- Check for any schema changes
SELECT 
    column_name,
    data_type,
    is_nullable,
    CASE 
        WHEN data_type LIKE '%varchar%' THEN 'String'
        WHEN data_type LIKE '%int%' THEN 'Integer'
        WHEN data_type LIKE '%decimal%' THEN 'Decimal'
        WHEN data_type LIKE '%timestamp%' THEN 'Timestamp'
        WHEN data_type LIKE '%date%' THEN 'Date'
        ELSE 'Other'
    END as column_category
FROM information_schema.columns 
WHERE table_schema = '[DATABASE]' 
AND table_name = '[TABLE_NAME]'
ORDER BY ordinal_position;

-- =============================================================================
-- 4. DATA INTEGRITY VERIFICATION
-- =============================================================================

-- Verify row count matches source
SELECT COUNT(*) as total_rows FROM [DATABASE].[TABLE_NAME];

-- Check for NULL values in key columns
SELECT 
    COUNT(*) as total_rows,
    COUNT(column1) as non_null_column1,
    COUNT(column2) as non_null_column2,
    COUNT(column3) as non_null_column3,
    COUNT(*) - COUNT(column1) as null_column1,
    COUNT(*) - COUNT(column2) as null_column2,
    COUNT(*) - COUNT(column3) as null_column3
FROM [DATABASE].[TABLE_NAME];

-- Verify data distribution
SELECT 
    column1,
    COUNT(*) as frequency,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM [DATABASE].[TABLE_NAME]) as percentage
FROM [DATABASE].[TABLE_NAME]
GROUP BY column1
ORDER BY frequency DESC
LIMIT 10;

-- Check for duplicate rows
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT *) as unique_rows,
    COUNT(*) - COUNT(DISTINCT *) as duplicate_rows
FROM [DATABASE].[TABLE_NAME];

-- =============================================================================
-- 5. PARTITION VERIFICATION
-- =============================================================================

-- Verify partition structure
SELECT DISTINCT 
    partition_col1, 
    partition_col2,
    COUNT(*) as partition_count
FROM [DATABASE].[TABLE_NAME] 
GROUP BY partition_col1, partition_col2
ORDER BY partition_col1, partition_col2;

-- Check partition statistics
SELECT 
    partition_col1,
    COUNT(*) as row_count,
    COUNT(DISTINCT "$path") as file_count,
    SUM(CAST("$file_size" AS BIGINT))/1024/1024 as size_mb
FROM [DATABASE].[TABLE_NAME]
GROUP BY partition_col1
ORDER BY row_count DESC;

-- Verify partition distribution
SELECT 
    partition_col1,
    COUNT(*) as partition_count,
    MIN(partition_col2) as min_partition_col2,
    MAX(partition_col2) as max_partition_col2
FROM [DATABASE].[TABLE_NAME]
GROUP BY partition_col1
ORDER BY partition_count DESC;

-- =============================================================================
-- 6. ICEBERG-SPECIFIC VERIFICATION
-- =============================================================================

-- Check Iceberg table snapshots
SELECT 
    snapshot_id,
    committed_at,
    summary
FROM [DATABASE].[TABLE_NAME].snapshots
ORDER BY committed_at DESC;

-- Check Iceberg table history
SELECT 
    made_current_at,
    snapshot_id,
    is_current_ancestor
FROM [DATABASE].[TABLE_NAME].history
ORDER BY made_current_at DESC;

-- Check Iceberg table files
SELECT 
    content,
    file_path,
    file_format,
    record_count,
    file_size_in_bytes
FROM [DATABASE].[TABLE_NAME].files
ORDER BY file_path;

-- Check Iceberg table partitions
SELECT 
    partition,
    record_count,
    file_count
FROM [DATABASE].[TABLE_NAME].partitions
ORDER BY partition;

-- =============================================================================
-- 7. PERFORMANCE VERIFICATION
-- =============================================================================

-- Test basic query performance
SELECT 
    column1,
    COUNT(*) as count
FROM [DATABASE].[TABLE_NAME]
WHERE partition_col1 = 'specific_value'
GROUP BY column1
ORDER BY count DESC
LIMIT 10;

-- Test aggregation performance
SELECT 
    partition_col1,
    COUNT(*) as row_count,
    AVG(numeric_column) as avg_value,
    MIN(numeric_column) as min_value,
    MAX(numeric_column) as max_value
FROM [DATABASE].[TABLE_NAME]
GROUP BY partition_col1
ORDER BY row_count DESC;

-- Test join performance (if applicable)
SELECT 
    t1.column1,
    t1.column2,
    t2.column3
FROM [DATABASE].[TABLE_NAME] t1
JOIN [DATABASE].[TABLE_NAME] t2 ON t1.key_column = t2.key_column
LIMIT 100;

-- =============================================================================
-- 8. DATA QUALITY VERIFICATION
-- =============================================================================

-- Check data type consistency
SELECT 
    column_name,
    data_type,
    COUNT(*) as row_count,
    COUNT(column_name) as non_null_count,
    MIN(column_name) as min_value,
    MAX(column_name) as max_value
FROM [DATABASE].[TABLE_NAME]
WHERE data_type IN ('int', 'bigint', 'double', 'float', 'decimal')
GROUP BY column_name, data_type;

-- Check string column quality
SELECT 
    column_name,
    data_type,
    COUNT(*) as row_count,
    COUNT(column_name) as non_null_count,
    MIN(LENGTH(column_name)) as min_length,
    MAX(LENGTH(column_name)) as max_length,
    AVG(LENGTH(column_name)) as avg_length
FROM [DATABASE].[TABLE_NAME]
WHERE data_type LIKE '%varchar%' OR data_type LIKE '%string%'
GROUP BY column_name, data_type;

-- Check date/timestamp column quality
SELECT 
    column_name,
    data_type,
    COUNT(*) as row_count,
    COUNT(column_name) as non_null_count,
    MIN(column_name) as min_date,
    MAX(column_name) as max_date
FROM [DATABASE].[TABLE_NAME]
WHERE data_type LIKE '%date%' OR data_type LIKE '%timestamp%'
GROUP BY column_name, data_type;

-- =============================================================================
-- 9. STORAGE VERIFICATION
-- =============================================================================

-- Check file count and sizes
SELECT 
    COUNT(DISTINCT "$path") as file_count,
    SUM(CAST("$file_size" AS BIGINT)) as total_size_bytes,
    SUM(CAST("$file_size" AS BIGINT))/1024/1024/1024 as total_size_gb,
    AVG(CAST("$file_size" AS BIGINT)) as avg_file_size_bytes,
    MIN(CAST("$file_size" AS BIGINT)) as min_file_size_bytes,
    MAX(CAST("$file_size" AS BIGINT)) as max_file_size_bytes
FROM [DATABASE].[TABLE_NAME];

-- Check file distribution by size
SELECT 
    CASE 
        WHEN CAST("$file_size" AS BIGINT) < 1024*1024 THEN 'Small (<1MB)'
        WHEN CAST("$file_size" AS BIGINT) < 10*1024*1024 THEN 'Medium (1-10MB)'
        WHEN CAST("$file_size" AS BIGINT) < 100*1024*1024 THEN 'Large (10-100MB)'
        ELSE 'Very Large (>100MB)'
    END as file_size_category,
    COUNT(*) as file_count,
    SUM(CAST("$file_size" AS BIGINT))/1024/1024 as total_size_mb
FROM [DATABASE].[TABLE_NAME]
GROUP BY 1
ORDER BY 2 DESC;

-- =============================================================================
-- 10. MIGRATION SUCCESS VERIFICATION
-- =============================================================================

-- Overall migration success check
SELECT 
    'MIGRATION_SUCCESS' as check_type,
    CASE 
        WHEN COUNT(*) > 0 THEN 'PASS'
        ELSE 'FAIL'
    END as status,
    COUNT(*) as row_count,
    COUNT(DISTINCT "$path") as file_count,
    SUM(CAST("$file_size" AS BIGINT))/1024/1024/1024 as size_gb
FROM [DATABASE].[TABLE_NAME];

-- Check for any data loss
SELECT 
    'DATA_LOSS_CHECK' as check_type,
    CASE 
        WHEN COUNT(*) = 0 THEN 'FAIL - No data found'
        WHEN COUNT(*) < expected_count THEN 'WARNING - Possible data loss'
        ELSE 'PASS - Data count acceptable'
    END as status,
    COUNT(*) as actual_count
FROM [DATABASE].[TABLE_NAME];

-- Verify Iceberg table is queryable
SELECT 
    'ICEBERG_QUERY_TEST' as test_type,
    CASE 
        WHEN COUNT(*) > 0 THEN 'PASS'
        ELSE 'FAIL'
    END as status,
    COUNT(*) as test_row_count
FROM [DATABASE].[TABLE_NAME]
WHERE partition_col1 IS NOT NULL;

-- =============================================================================
-- 11. COMPARISON WITH SOURCE (if source table still exists)
-- =============================================================================

-- Compare row counts (replace SOURCE_TABLE with actual source table name)
-- SELECT 
--     'ROW_COUNT_COMPARISON' as check_type,
--     (SELECT COUNT(*) FROM [DATABASE].[SOURCE_TABLE]) as source_count,
--     (SELECT COUNT(*) FROM [DATABASE].[TABLE_NAME]) as target_count,
--     CASE 
--         WHEN (SELECT COUNT(*) FROM [DATABASE].[SOURCE_TABLE]) = (SELECT COUNT(*) FROM [DATABASE].[TABLE_NAME]) 
--         THEN 'PASS' 
--         ELSE 'FAIL' 
--     END as status
-- FROM [DATABASE].[TABLE_NAME]
-- LIMIT 1;

-- =============================================================================
-- 12. RECOMMENDATIONS FOR OPTIMIZATION
-- =============================================================================

-- Generate optimization recommendations
SELECT 
    CASE 
        WHEN COUNT(DISTINCT "$path") > 1000 THEN 'Consider running OPTIMIZE to compact files'
        WHEN AVG(CAST("$file_size" AS BIGINT)) < 1024*1024 THEN 'Consider running OPTIMIZE to increase file sizes'
        ELSE 'Table appears optimized'
    END as optimization_recommendation,
    COUNT(DISTINCT "$path") as file_count,
    AVG(CAST("$file_size" AS BIGINT))/1024/1024 as avg_file_size_mb
FROM [DATABASE].[TABLE_NAME];

-- Check for potential performance issues
SELECT 
    CASE 
        WHEN COUNT(DISTINCT "$path") > 10000 THEN 'HIGH_FILE_COUNT - Consider partitioning optimization'
        WHEN AVG(CAST("$file_size" AS BIGINT)) > 100*1024*1024 THEN 'LARGE_FILES - Consider file size optimization'
        ELSE 'PERFORMANCE_OK'
    END as performance_status,
    COUNT(DISTINCT "$path") as file_count,
    AVG(CAST("$file_size" AS BIGINT))/1024/1024 as avg_file_size_mb
FROM [DATABASE].[TABLE_NAME];
