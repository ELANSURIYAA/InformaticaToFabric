# ============================================
# Author: Ascendion AAVA
# Created on: 2024
# Description: Informatica to Fabric Conversion
# ============================================

# -- Input Type: Informatica PowerCenter Mapping XML
# -- Target Platform: Microsoft Fabric (PySpark)
# -- Conversion Approach: Direct transformation mapping from Informatica to PySpark DataFrame API
# -- Major Risks / Checks:
#    - Verify source table schema matches expected columns
#    - Validate date format conversions (DD-MON-YY to standard format)
#    - Ensure UPDATE vs INSERT logic is correctly implemented
#    - Check NULL handling in expressions
#    - Validate target table exists and has correct schema

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, to_date, 
    upper, trim, coalesce, concat_ws, expr
)
from delta.tables import DeltaTable
import logging

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("m_Ciim048d_855_Data_Src_Validation_Load") \
    .getOrCreate()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    # ============================================
    # PARAMETERS
    # ============================================
    source_table = "DATA_SRC_VALIDATION"
    target_table = "DATA_SRC_VALIDATION_LOAD"
    
    logger.info(f"Starting mapping: m_Ciim048d_855_Data_Src_Validation_Load")
    logger.info(f"Source: {source_table}")
    logger.info(f"Target: {target_table}")
    
    # ============================================
    # SOURCE READ
    # ============================================
    df_source = spark.table(source_table)
    
    logger.info(f"Source records read: {df_source.count()}")
    
    # ============================================
    # TRANSFORMATION: Expression (EXP_DATA_SRC_VALIDATION_LOAD)
    # ============================================
    df_transformed = df_source.select(
        col("DATA_SRC_VALIDATION_ID"),
        col("DATA_SRC_ID"),
        col("VALIDATION_TYPE_CD"),
        col("VALIDATION_QUERY_TXT"),
        col("VALIDATION_RESULT_CNT"),
        col("VALIDATION_THRESHOLD_CNT"),
        col("VALIDATION_STATUS_CD"),
        col("VALIDATION_DT"),
        col("VALIDATION_COMMENTS_TXT"),
        col("CREATE_USER_ID"),
        col("CREATE_TS"),
        col("UPDATE_USER_ID"),
        col("UPDATE_TS"),
        col("ACTV_IND"),
        col("VALIDATION_NAME_TXT"),
        col("VALIDATION_DESCRIPTION_TXT"),
        col("VALIDATION_FREQUENCY_CD"),
        col("NOTIFICATION_EMAIL_TXT"),
        col("LAST_VALIDATION_DT"),
        col("NEXT_VALIDATION_DT"),
        col("VALIDATION_PRIORITY_CD"),
        col("VALIDATION_OWNER_TXT"),
        col("VALIDATION_CATEGORY_CD"),
        col("VALIDATION_SUBCATEGORY_CD"),
        col("VALIDATION_SEVERITY_CD"),
        col("VALIDATION_IMPACT_CD"),
        col("VALIDATION_RESOLUTION_TXT"),
        col("VALIDATION_NOTES_TXT"),
        col("VALIDATION_TAGS_TXT"),
        col("VALIDATION_METADATA_TXT"),
        col("VALIDATION_EXECUTION_TIME_SEC"),
        col("VALIDATION_ERROR_MSG_TXT"),
        col("VALIDATION_WARNING_MSG_TXT"),
        col("VALIDATION_INFO_MSG_TXT"),
        col("VALIDATION_DEBUG_MSG_TXT"),
        col("VALIDATION_TRACE_MSG_TXT"),
        col("VALIDATION_AUDIT_ID"),
        col("VALIDATION_RUN_ID"),
        col("VALIDATION_BATCH_ID"),
        col("VALIDATION_JOB_ID"),
        col("VALIDATION_STEP_ID"),
        col("VALIDATION_TASK_ID"),
        col("VALIDATION_SUBTASK_ID"),
        col("VALIDATION_SEQUENCE_NBR"),
        col("VALIDATION_RETRY_CNT"),
        col("VALIDATION_MAX_RETRY_CNT"),
        col("VALIDATION_TIMEOUT_SEC"),
        col("VALIDATION_START_TS"),
        col("VALIDATION_END_TS"),
        col("VALIDATION_DURATION_SEC"),
        col("VALIDATION_CPU_TIME_SEC"),
        col("VALIDATION_MEMORY_MB"),
        col("VALIDATION_DISK_IO_MB"),
        col("VALIDATION_NETWORK_IO_MB"),
        col("VALIDATION_ROW_CNT"),
        col("VALIDATION_COLUMN_CNT"),
        col("VALIDATION_FILE_SIZE_MB"),
        col("VALIDATION_PARTITION_CNT"),
        col("VALIDATION_INDEX_CNT"),
        col("VALIDATION_CONSTRAINT_CNT"),
        col("VALIDATION_TRIGGER_CNT")
    )
    
    # Add system timestamp for tracking
    df_transformed = df_transformed.withColumn(
        "ETL_LOAD_TS", 
        current_timestamp()
    )
    
    # ============================================
    # TRANSFORMATION: Router (RTR_DATA_SRC_VALIDATION_LOAD)
    # ============================================
    # Group 1: INSERT - New records (UPDATE_TS is NULL)
    df_insert = df_transformed.filter(col("UPDATE_TS").isNull())
    
    # Group 2: UPDATE - Existing records (UPDATE_TS is NOT NULL)
    df_update = df_transformed.filter(col("UPDATE_TS").isNotNull())
    
    logger.info(f"Records for INSERT: {df_insert.count()}")
    logger.info(f"Records for UPDATE: {df_update.count()}")
    
    # ============================================
    # TARGET WRITE
    # ============================================
    
    # Check if target table exists
    if spark.catalog.tableExists(target_table):
        # Use Delta MERGE for UPSERT operation
        delta_table = DeltaTable.forName(spark, target_table)
        
        # Combine INSERT and UPDATE dataframes
        df_final = df_insert.unionByName(df_update)
        
        # Perform MERGE operation
        delta_table.alias("target").merge(
            df_final.alias("source"),
            "target.DATA_SRC_VALIDATION_ID = source.DATA_SRC_VALIDATION_ID"
        ).whenMatchedUpdateAll(
        ).whenNotMatchedInsertAll(
        ).execute()
        
        logger.info(f"MERGE operation completed successfully")
    else:
        # If table doesn't exist, create it with all records
        df_final = df_insert.unionByName(df_update)
        df_final.write.format("delta").mode("overwrite").saveAsTable(target_table)
        logger.info(f"Target table created and loaded successfully")
    
    # ============================================
    # LOGGING
    # ============================================
    final_count = spark.table(target_table).count()
    logger.info(f"Target table final record count: {final_count}")
    logger.info("Mapping execution completed successfully")
    
except Exception as e:
    logger.error(f"Error in mapping execution: {str(e)}")
    raise
finally:
    spark.stop()
