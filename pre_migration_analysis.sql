-- Pre-Migration Analysis Queries for AWS Athena to Iceberg Migration
-- These queries help analyze the source table before migration

-- =============================================================================
-- 1. TABLE METADATA ANALYSIS
-- =============================================================================

-- Get basic table information
DESCRIBE TABLE [DATABASE].[TABLE_NAME];

-- Get detailed table properties
SHOW CREATE TABLE [DATABASE].[TABLE_NAME];

-- Get table statistics
SELECT 
    table_name,
    table_type,
    table_schema,
    table_catalog
FROM information_schema.tables 
WHERE table_schema = '[DATABASE]' 
AND table_name = '[TABLE_NAME]';

-- =============================================================================
-- 2. SCHEMA ANALYSIS
-- =============================================================================

-- Get column information with data types
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

-- Get column statistics
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
-- 3. DATA VOLUME ANALYSIS
-- =============================================================================

-- Get row count
SELECT COUNT(*) as total_rows FROM [DATABASE].[TABLE_NAME];

-- Get data volume information
SELECT 
    COUNT(*) as file_count,
    SUM(CAST("$file_size" AS BIGINT)) as total_size_bytes,
    SUM(CAST("$file_size" AS BIGINT))/1024/1024/1024 as total_size_gb,
    AVG(CAST("$file_size" AS BIGINT)) as avg_file_size_bytes,
    MIN(CAST("$file_size" AS BIGINT)) as min_file_size_bytes,
    MAX(CAST("$file_size" AS BIGINT)) as max_file_size_bytes
FROM (
    SELECT 
        "$path" as file_path,
        "$file_size" as file_size
    FROM [DATABASE].[TABLE_NAME]
);

-- Get file distribution by size
SELECT 
    CASE 
        WHEN CAST("$file_size" AS BIGINT) < 1024*1024 THEN 'Small (<1MB)'
        WHEN CAST("$file_size" AS BIGINT) < 10*1024*1024 THEN 'Medium (1-10MB)'
        WHEN CAST("$file_size" AS BIGINT) < 100*1024*1024 THEN 'Large (10-100MB)'
        ELSE 'Very Large (>100MB)'
    END as file_size_category,
    COUNT(*) as file_count,
    SUM(CAST("$file_size" AS BIGINT))/1024/1024 as total_size_mb
FROM (
    SELECT "$file_size" as file_size
    FROM [DATABASE].[TABLE_NAME]
)
GROUP BY 1
ORDER BY 2 DESC;

-- =============================================================================
-- 4. PARTITION ANALYSIS
-- =============================================================================

-- Get partition information
SELECT DISTINCT 
    partition_col1, 
    partition_col2,
    COUNT(*) as partition_count
FROM [DATABASE].[TABLE_NAME] 
GROUP BY partition_col1, partition_col2
ORDER BY partition_col1, partition_col2;

-- Get partition statistics
SELECT 
    partition_col1,
    COUNT(*) as row_count,
    COUNT(DISTINCT "$path") as file_count,
    SUM(CAST("$file_size" AS BIGINT))/1024/1024 as size_mb
FROM [DATABASE].[TABLE_NAME]
GROUP BY partition_col1
ORDER BY row_count DESC;

-- Get partition distribution
SELECT 
    partition_col1,
    COUNT(*) as partition_count,
    MIN(partition_col2) as min_partition_col2,
    MAX(partition_col2) as max_partition_col2
FROM [DATABASE].[TABLE_NAME]
GROUP BY partition_col1
ORDER BY partition_count DESC;

-- =============================================================================
-- 5. DATA QUALITY ANALYSIS
-- =============================================================================

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

-- Check for empty strings
SELECT 
    COUNT(*) as total_rows,
    SUM(CASE WHEN column1 = '' THEN 1 ELSE 0 END) as empty_column1,
    SUM(CASE WHEN column2 = '' THEN 1 ELSE 0 END) as empty_column2,
    SUM(CASE WHEN column3 = '' THEN 1 ELSE 0 END) as empty_column3
FROM [DATABASE].[TABLE_NAME];

-- Check data distribution for key columns
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
-- 6. DATA TYPE ANALYSIS
-- =============================================================================

-- Analyze numeric columns
SELECT 
    column_name,
    data_type,
    COUNT(*) as row_count,
    COUNT(column_name) as non_null_count,
    MIN(column_name) as min_value,
    MAX(column_name) as max_value,
    AVG(column_name) as avg_value
FROM [DATABASE].[TABLE_NAME]
WHERE data_type IN ('int', 'bigint', 'double', 'float', 'decimal')
GROUP BY column_name, data_type;

-- Analyze string columns
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

-- Analyze date/timestamp columns
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
-- 7. PERFORMANCE ANALYSIS
-- =============================================================================

-- Get query execution statistics
SELECT 
    query_id,
    query,
    execution_time,
    data_scanned,
    cost
FROM [DATABASE].[TABLE_NAME]
WHERE query_type = 'SELECT'
ORDER BY execution_time DESC
LIMIT 10;

-- Analyze table access patterns
SELECT 
    DATE_TRUNC('day', query_time) as query_date,
    COUNT(*) as query_count,
    AVG(execution_time) as avg_execution_time,
    SUM(data_scanned) as total_data_scanned
FROM [DATABASE].[TABLE_NAME]
GROUP BY DATE_TRUNC('day', query_time)
ORDER BY query_date DESC;

-- =============================================================================
-- 8. MIGRATION READINESS CHECK
-- =============================================================================

-- Check if table is suitable for Iceberg migration
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 'EMPTY_TABLE'
        WHEN COUNT(*) < 1000 THEN 'SMALL_TABLE'
        WHEN COUNT(*) < 1000000 THEN 'MEDIUM_TABLE'
        ELSE 'LARGE_TABLE'
    END as table_size_category,
    COUNT(*) as row_count,
    COUNT(DISTINCT "$path") as file_count,
    SUM(CAST("$file_size" AS BIGINT))/1024/1024/1024 as size_gb
FROM [DATABASE].[TABLE_NAME];

-- Check for complex data types that might need special handling
SELECT 
    column_name,
    data_type,
    CASE 
        WHEN data_type LIKE '%array%' THEN 'ARRAY_TYPE'
        WHEN data_type LIKE '%map%' THEN 'MAP_TYPE'
        WHEN data_type LIKE '%struct%' THEN 'STRUCT_TYPE'
        ELSE 'SIMPLE_TYPE'
    END as type_complexity
FROM information_schema.columns 
WHERE table_schema = '[DATABASE]' 
AND table_name = '[TABLE_NAME]'
AND (data_type LIKE '%array%' OR data_type LIKE '%map%' OR data_type LIKE '%struct%');

-- Check for potential migration issues
SELECT 
    'SCHEMA_ISSUES' as check_type,
    COUNT(*) as issue_count,
    STRING_AGG(column_name, ', ') as problematic_columns
FROM information_schema.columns 
WHERE table_schema = '[DATABASE]' 
AND table_name = '[TABLE_NAME]'
AND (data_type LIKE '%array%' OR data_type LIKE '%map%' OR data_type LIKE '%struct%')

UNION ALL

SELECT 
    'DATA_QUALITY_ISSUES' as check_type,
    COUNT(*) as issue_count,
    'High NULL percentage in key columns' as description
FROM [DATABASE].[TABLE_NAME]
WHERE (COUNT(column1) * 100.0 / COUNT(*) < 50) 
OR (COUNT(column2) * 100.0 / COUNT(*) < 50);

-- =============================================================================
-- 9. COST ANALYSIS
-- =============================================================================

-- Estimate migration costs
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT "$path") as file_count,
    SUM(CAST("$file_size" AS BIGINT))/1024/1024/1024 as size_gb,
    -- Estimate Glue job cost (assuming G.1X workers)
    ROUND((SUM(CAST("$file_size" AS BIGINT))/1024/1024/1024) * 0.44, 2) as estimated_glue_cost_usd,
    -- Estimate S3 storage cost
    ROUND((SUM(CAST("$file_size" AS BIGINT))/1024/1024/1024) * 0.023, 2) as estimated_s3_cost_usd_monthly
FROM [DATABASE].[TABLE_NAME];

-- =============================================================================
-- 10. RECOMMENDATIONS
-- =============================================================================

-- Generate migration recommendations
SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 'EMPTY_TABLE - Consider skipping migration'
        WHEN COUNT(*) < 1000 THEN 'SMALL_TABLE - Use G.1X workers, single worker'
        WHEN COUNT(*) < 1000000 THEN 'MEDIUM_TABLE - Use G.1X workers, 2-5 workers'
        WHEN COUNT(*) < 10000000 THEN 'LARGE_TABLE - Use G.2X workers, 5-10 workers'
        ELSE 'VERY_LARGE_TABLE - Use G.2X workers, 10+ workers, consider incremental migration'
    END as migration_recommendation,
    COUNT(*) as row_count,
    COUNT(DISTINCT "$path") as file_count,
    SUM(CAST("$file_size" AS BIGINT))/1024/1024/1024 as size_gb
FROM [DATABASE].[TABLE_NAME];
