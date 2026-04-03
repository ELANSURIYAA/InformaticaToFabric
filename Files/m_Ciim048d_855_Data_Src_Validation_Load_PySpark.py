# ============================================
# Author: Ascendion AAVA
# Created on: 2024
# Description: Informatica to Fabric Conversion
# ============================================

# ============================================
# CONVERSION LOG
# ============================================
# Input Type: Informatica PowerCenter Mapping XML
# Target Platform: Microsoft Fabric (PySpark)
# Conversion Approach: Direct transformation mapping from Informatica to PySpark DataFrame API
# Major Risks / Checks:
#   - Verify Oracle source connectivity and credentials
#   - Validate date format conversions (DD-MON-YYYY to standard format)
#   - Ensure filter conditions are correctly applied
#   - Check target table schema compatibility
#   - Verify NULL handling and data type conversions
#   - Monitor performance for large datasets
# ============================================

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, trim, upper, to_date, current_timestamp,
    lit, coalesce, length, substring, concat
)
from pyspark.sql.types import StringType, DateType, TimestampType
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================
# SPARK SESSION INITIALIZATION
# ============================================
try:
    spark = SparkSession.builder \
        .appName("m_Ciim048d_855_Data_Src_Validation_Load") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    logger.info("Spark session created successfully")
except Exception as e:
    logger.error(f"Failed to create Spark session: {str(e)}")
    raise

# ============================================
# PARAMETERS
# ============================================
# Source parameters
SOURCE_DATABASE = "ORCL"  # Oracle database name
SOURCE_SCHEMA = "your_schema"  # Replace with actual schema
SOURCE_TABLE = "CIIM048D_855_DATA_SRC_VALIDATION"

# Target parameters
TARGET_LAKEHOUSE = "your_lakehouse"  # Replace with actual lakehouse name
TARGET_SCHEMA = "dbo"  # Default schema for Fabric
TARGET_TABLE = "CIIM048D_855_DATA_SRC_VALIDATION"

# JDBC connection parameters for Oracle source
JDBC_URL = f"jdbc:oracle:thin:@your_host:1521:{SOURCE_DATABASE}"  # Replace with actual connection details
JDBC_USER = "your_username"  # Replace with actual username
JDBC_PASSWORD = "your_password"  # Replace with actual password or use Key Vault

# ============================================
# SOURCE: SQ_CIIM048D_855_DATA_SRC_VALIDATION
# ============================================
try:
    logger.info(f"Reading source table: {SOURCE_SCHEMA}.{SOURCE_TABLE}")
    
    # Read from Oracle source
    df_source = spark.read \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", f"{SOURCE_SCHEMA}.{SOURCE_TABLE}") \
        .option("user", JDBC_USER) \
        .option("password", JDBC_PASSWORD) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .option("fetchsize", "10000") \
        .load()
    
    source_count = df_source.count()
    logger.info(f"Source records read: {source_count}")
    
except Exception as e:
    logger.error(f"Error reading source table: {str(e)}")
    raise

# ============================================
# TRANSFORMATION: EXP_CIIM048D_855_DATA_SRC_VALIDATION
# ============================================
try:
    logger.info("Applying expression transformations")
    
    df_transformed = df_source.select(
        # CTRL_ID - Pass through
        col("CTRL_ID"),
        
        # DATA_SRC_ID - Pass through
        col("DATA_SRC_ID"),
        
        # DATA_SRC_NM - Pass through
        col("DATA_SRC_NM"),
        
        # DATA_SRC_DESC - Pass through
        col("DATA_SRC_DESC"),
        
        # DATA_SRC_CTGRY_ID - Pass through
        col("DATA_SRC_CTGRY_ID"),
        
        # DATA_SRC_TYPE_ID - Pass through
        col("DATA_SRC_TYPE_ID"),
        
        # DATA_SRC_SUBTYPE_ID - Pass through
        col("DATA_SRC_SUBTYPE_ID"),
        
        # DATA_SRC_PRDCT_ID - Pass through
        col("DATA_SRC_PRDCT_ID"),
        
        # DATA_SRC_VRSN_ID - Pass through
        col("DATA_SRC_VRSN_ID"),
        
        # DATA_SRC_STTS_ID - Pass through
        col("DATA_SRC_STTS_ID"),
        
        # EFCTV_STRT_DT - Date conversion from DD-MON-YYYY to standard date format
        when(
            col("EFCTV_STRT_DT").isNotNull(),
            to_date(col("EFCTV_STRT_DT"), "dd-MMM-yyyy")
        ).otherwise(None).alias("EFCTV_STRT_DT"),
        
        # EFCTV_END_DT - Date conversion from DD-MON-YYYY to standard date format
        when(
            col("EFCTV_END_DT").isNotNull(),
            to_date(col("EFCTV_END_DT"), "dd-MMM-yyyy")
        ).otherwise(None).alias("EFCTV_END_DT"),
        
        # REC_CREAT_TS - Pass through
        col("REC_CREAT_TS"),
        
        # REC_CREAT_USER_ID - Pass through
        col("REC_CREAT_USER_ID"),
        
        # LST_UPDT_TS - Pass through
        col("LST_UPDT_TS"),
        
        # LST_UPDT_USER_ID - Pass through
        col("LST_UPDT_USER_ID"),
        
        # DATA_SRC_OWNR_ID - Pass through
        col("DATA_SRC_OWNR_ID"),
        
        # DATA_SRC_CNTCT_ID - Pass through
        col("DATA_SRC_CNTCT_ID"),
        
        # DATA_SRC_LOC_ID - Pass through
        col("DATA_SRC_LOC_ID"),
        
        # DATA_SRC_URL - Pass through
        col("DATA_SRC_URL"),
        
        # DATA_SRC_PATH - Pass through
        col("DATA_SRC_PATH"),
        
        # DATA_SRC_FILE_NM - Pass through
        col("DATA_SRC_FILE_NM"),
        
        # DATA_SRC_SCHMA_NM - Pass through
        col("DATA_SRC_SCHMA_NM"),
        
        # DATA_SRC_TBL_NM - Pass through
        col("DATA_SRC_TBL_NM"),
        
        # DATA_SRC_VIEW_NM - Pass through
        col("DATA_SRC_VIEW_NM"),
        
        # DATA_SRC_PROC_NM - Pass through
        col("DATA_SRC_PROC_NM"),
        
        # DATA_SRC_FUNC_NM - Pass through
        col("DATA_SRC_FUNC_NM"),
        
        # DATA_SRC_PKG_NM - Pass through
        col("DATA_SRC_PKG_NM"),
        
        # DATA_SRC_QUERY_TXT - Pass through
        col("DATA_SRC_QUERY_TXT"),
        
        # DATA_SRC_FILTR_TXT - Pass through
        col("DATA_SRC_FILTR_TXT"),
        
        # DATA_SRC_SORT_TXT - Pass through
        col("DATA_SRC_SORT_TXT"),
        
        # DATA_SRC_JOIN_TXT - Pass through
        col("DATA_SRC_JOIN_TXT"),
        
        # DATA_SRC_AGGT_TXT - Pass through
        col("DATA_SRC_AGGT_TXT"),
        
        # DATA_SRC_PRTN_TXT - Pass through
        col("DATA_SRC_PRTN_TXT"),
        
        # DATA_SRC_IDX_TXT - Pass through
        col("DATA_SRC_IDX_TXT"),
        
        # DATA_SRC_CNSTRNT_TXT - Pass through
        col("DATA_SRC_CNSTRNT_TXT"),
        
        # DATA_SRC_TRGR_TXT - Pass through
        col("DATA_SRC_TRGR_TXT"),
        
        # DATA_SRC_RLTNSHP_TXT - Pass through
        col("DATA_SRC_RLTNSHP_TXT"),
        
        # DATA_SRC_DPNDNCY_TXT - Pass through
        col("DATA_SRC_DPNDNCY_TXT"),
        
        # DATA_SRC_LNKGE_TXT - Pass through
        col("DATA_SRC_LNKGE_TXT"),
        
        # DATA_SRC_RFRNC_TXT - Pass through
        col("DATA_SRC_RFRNC_TXT"),
        
        # DATA_SRC_ANNTN_TXT - Pass through
        col("DATA_SRC_ANNTN_TXT"),
        
        # DATA_SRC_CMNT_TXT - Pass through
        col("DATA_SRC_CMNT_TXT"),
        
        # DATA_SRC_TAG_TXT - Pass through
        col("DATA_SRC_TAG_TXT"),
        
        # DATA_SRC_LNKG_ID - Pass through
        col("DATA_SRC_LNKG_ID"),
        
        # DATA_SRC_PRNT_ID - Pass through
        col("DATA_SRC_PRNT_ID"),
        
        # DATA_SRC_CHLD_ID - Pass through
        col("DATA_SRC_CHLD_ID"),
        
        # DATA_SRC_SIBLNG_ID - Pass through
        col("DATA_SRC_SIBLNG_ID"),
        
        # DATA_SRC_PRED_ID - Pass through
        col("DATA_SRC_PRED_ID"),
        
        # DATA_SRC_SUCC_ID - Pass through
        col("DATA_SRC_SUCC_ID"),
        
        # DATA_SRC_DPNDNT_ID - Pass through
        col("DATA_SRC_DPNDNT_ID"),
        
        # DATA_SRC_DPNDR_ID - Pass through
        col("DATA_SRC_DPNDR_ID"),
        
        # DATA_SRC_RLTD_ID - Pass through
        col("DATA_SRC_RLTD_ID"),
        
        # DATA_SRC_ASSCTD_ID - Pass through
        col("DATA_SRC_ASSCTD_ID"),
        
        # DATA_SRC_CNNCTD_ID - Pass through
        col("DATA_SRC_CNNCTD_ID"),
        
        # DATA_SRC_LNKD_ID - Pass through
        col("DATA_SRC_LNKD_ID"),
        
        # DATA_SRC_RFRD_ID - Pass through
        col("DATA_SRC_RFRD_ID"),
        
        # DATA_SRC_MNTN_IND - Pass through
        col("DATA_SRC_MNTN_IND"),
        
        # DATA_SRC_ARCHV_IND - Pass through
        col("DATA_SRC_ARCHV_IND"),
        
        # DATA_SRC_DEL_IND - Pass through
        col("DATA_SRC_DEL_IND"),
        
        # DATA_SRC_OBSLT_IND - Pass through
        col("DATA_SRC_OBSLT_IND"),
        
        # DATA_SRC_DPRCTD_IND - Pass through
        col("DATA_SRC_DPRCTD_IND"),
        
        # DATA_SRC_EXPRD_IND - Pass through
        col("DATA_SRC_EXPRD_IND"),
        
        # DATA_SRC_SUSPND_IND - Pass through
        col("DATA_SRC_SUSPND_IND"),
        
        # DATA_SRC_INACTV_IND - Pass through
        col("DATA_SRC_INACTV_IND")
    )
    
    transform_count = df_transformed.count()
    logger.info(f"Transformed records: {transform_count}")
    
except Exception as e:
    logger.error(f"Error in expression transformation: {str(e)}")
    raise

# ============================================
# TRANSFORMATION: FIL_CIIM048D_855_DATA_SRC_VALIDATION
# ============================================
try:
    logger.info("Applying filter transformation")
    
    # Filter condition: NOT ISNULL(CTRL_ID)
    df_filtered = df_transformed.filter(
        col("CTRL_ID").isNotNull()
    )
    
    filter_count = df_filtered.count()
    logger.info(f"Filtered records: {filter_count}")
    logger.info(f"Records filtered out: {transform_count - filter_count}")
    
except Exception as e:
    logger.error(f"Error in filter transformation: {str(e)}")
    raise

# ============================================
# TARGET LOAD: CIIM048D_855_DATA_SRC_VALIDATION
# ============================================
try:
    logger.info(f"Loading data to target table: {TARGET_SCHEMA}.{TARGET_TABLE}")
    
    # Write to target table in Fabric Lakehouse
    df_filtered.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{TARGET_LAKEHOUSE}.{TARGET_SCHEMA}.{TARGET_TABLE}")
    
    logger.info(f"Successfully loaded {filter_count} records to target table")
    
except Exception as e:
    logger.error(f"Error loading data to target: {str(e)}")
    raise

# ============================================
# EXECUTION SUMMARY
# ============================================
try:
    summary = f"""
    ============================================
    EXECUTION SUMMARY
    ============================================
    Mapping Name: m_Ciim048d_855_Data_Src_Validation_Load
    Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    
    Source Records Read: {source_count}
    Records After Transformation: {transform_count}
    Records After Filter: {filter_count}
    Records Filtered Out: {transform_count - filter_count}
    Records Loaded to Target: {filter_count}
    
    Status: SUCCESS
    ============================================
    """
    
    logger.info(summary)
    print(summary)
    
except Exception as e:
    logger.error(f"Error generating summary: {str(e)}")

# ============================================
# CLEANUP
# ============================================
try:
    # Stop Spark session if needed
    # spark.stop()
    logger.info("Mapping execution completed successfully")
except Exception as e:
    logger.error(f"Error during cleanup: {str(e)}")

# ============================================
# END OF SCRIPT
# ============================================
