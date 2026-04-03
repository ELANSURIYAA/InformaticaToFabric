-- ============================================
-- Author: Ascendion AAVA
-- Created on: 2024
-- Description: Informatica to Fabric Conversion
-- ============================================

-- Input Type: Informatica PowerCenter Mapping XML
-- Target Platform: Microsoft Fabric (T-SQL)
-- Conversion Approach: CTE-based pipeline with MERGE operation
-- Major Risks / Checks:
--    - Verify source table schema matches expected columns
--    - Validate date format conversions
--    - Ensure UPDATE vs INSERT logic is correctly implemented
--    - Check NULL handling in expressions
--    - Validate target table exists and has correct schema
--    - Review MERGE operation for performance on large datasets

BEGIN TRY
    -- ============================================
    -- PARAMETERS
    -- ============================================
    DECLARE @SourceTable NVARCHAR(100) = 'DATA_SRC_VALIDATION';
    DECLARE @TargetTable NVARCHAR(100) = 'DATA_SRC_VALIDATION_LOAD';
    DECLARE @RowsInserted INT = 0;
    DECLARE @RowsUpdated INT = 0;
    DECLARE @StartTime DATETIME2 = GETDATE();
    
    PRINT 'Starting mapping: m_Ciim048d_855_Data_Src_Validation_Load';
    PRINT 'Source: ' + @SourceTable;
    PRINT 'Target: ' + @TargetTable;
    PRINT 'Start Time: ' + CONVERT(VARCHAR, @StartTime, 121);
    
    -- ============================================
    -- SOURCE CTE
    -- ============================================
    ;WITH CTE_Source AS (
        SELECT
            DATA_SRC_VALIDATION_ID,
            DATA_SRC_ID,
            VALIDATION_TYPE_CD,
            VALIDATION_QUERY_TXT,
            VALIDATION_RESULT_CNT,
            VALIDATION_THRESHOLD_CNT,
            VALIDATION_STATUS_CD,
            VALIDATION_DT,
            VALIDATION_COMMENTS_TXT,
            CREATE_USER_ID,
            CREATE_TS,
            UPDATE_USER_ID,
            UPDATE_TS,
            ACTV_IND,
            VALIDATION_NAME_TXT,
            VALIDATION_DESCRIPTION_TXT,
            VALIDATION_FREQUENCY_CD,
            NOTIFICATION_EMAIL_TXT,
            LAST_VALIDATION_DT,
            NEXT_VALIDATION_DT,
            VALIDATION_PRIORITY_CD,
            VALIDATION_OWNER_TXT,
            VALIDATION_CATEGORY_CD,
            VALIDATION_SUBCATEGORY_CD,
            VALIDATION_SEVERITY_CD,
            VALIDATION_IMPACT_CD,
            VALIDATION_RESOLUTION_TXT,
            VALIDATION_NOTES_TXT,
            VALIDATION_TAGS_TXT,
            VALIDATION_METADATA_TXT,
            VALIDATION_EXECUTION_TIME_SEC,
            VALIDATION_ERROR_MSG_TXT,
            VALIDATION_WARNING_MSG_TXT,
            VALIDATION_INFO_MSG_TXT,
            VALIDATION_DEBUG_MSG_TXT,
            VALIDATION_TRACE_MSG_TXT,
            VALIDATION_AUDIT_ID,
            VALIDATION_RUN_ID,
            VALIDATION_BATCH_ID,
            VALIDATION_JOB_ID,
            VALIDATION_STEP_ID,
            VALIDATION_TASK_ID,
            VALIDATION_SUBTASK_ID,
            VALIDATION_SEQUENCE_NBR,
            VALIDATION_RETRY_CNT,
            VALIDATION_MAX_RETRY_CNT,
            VALIDATION_TIMEOUT_SEC,
            VALIDATION_START_TS,
            VALIDATION_END_TS,
            VALIDATION_DURATION_SEC,
            VALIDATION_CPU_TIME_SEC,
            VALIDATION_MEMORY_MB,
            VALIDATION_DISK_IO_MB,
            VALIDATION_NETWORK_IO_MB,
            VALIDATION_ROW_CNT,
            VALIDATION_COLUMN_CNT,
            VALIDATION_FILE_SIZE_MB,
            VALIDATION_PARTITION_CNT,
            VALIDATION_INDEX_CNT,
            VALIDATION_CONSTRAINT_CNT,
            VALIDATION_TRIGGER_CNT
        FROM DATA_SRC_VALIDATION
    ),
    
    -- ============================================
    -- TRANSFORMATION CTE: Expression
    -- ============================================
    CTE_Transformed AS (
        SELECT
            DATA_SRC_VALIDATION_ID,
            DATA_SRC_ID,
            VALIDATION_TYPE_CD,
            VALIDATION_QUERY_TXT,
            VALIDATION_RESULT_CNT,
            VALIDATION_THRESHOLD_CNT,
            VALIDATION_STATUS_CD,
            VALIDATION_DT,
            VALIDATION_COMMENTS_TXT,
            CREATE_USER_ID,
            CREATE_TS,
            UPDATE_USER_ID,
            UPDATE_TS,
            ACTV_IND,
            VALIDATION_NAME_TXT,
            VALIDATION_DESCRIPTION_TXT,
            VALIDATION_FREQUENCY_CD,
            NOTIFICATION_EMAIL_TXT,
            LAST_VALIDATION_DT,
            NEXT_VALIDATION_DT,
            VALIDATION_PRIORITY_CD,
            VALIDATION_OWNER_TXT,
            VALIDATION_CATEGORY_CD,
            VALIDATION_SUBCATEGORY_CD,
            VALIDATION_SEVERITY_CD,
            VALIDATION_IMPACT_CD,
            VALIDATION_RESOLUTION_TXT,
            VALIDATION_NOTES_TXT,
            VALIDATION_TAGS_TXT,
            VALIDATION_METADATA_TXT,
            VALIDATION_EXECUTION_TIME_SEC,
            VALIDATION_ERROR_MSG_TXT,
            VALIDATION_WARNING_MSG_TXT,
            VALIDATION_INFO_MSG_TXT,
            VALIDATION_DEBUG_MSG_TXT,
            VALIDATION_TRACE_MSG_TXT,
            VALIDATION_AUDIT_ID,
            VALIDATION_RUN_ID,
            VALIDATION_BATCH_ID,
            VALIDATION_JOB_ID,
            VALIDATION_STEP_ID,
            VALIDATION_TASK_ID,
            VALIDATION_SUBTASK_ID,
            VALIDATION_SEQUENCE_NBR,
            VALIDATION_RETRY_CNT,
            VALIDATION_MAX_RETRY_CNT,
            VALIDATION_TIMEOUT_SEC,
            VALIDATION_START_TS,
            VALIDATION_END_TS,
            VALIDATION_DURATION_SEC,
            VALIDATION_CPU_TIME_SEC,
            VALIDATION_MEMORY_MB,
            VALIDATION_DISK_IO_MB,
            VALIDATION_NETWORK_IO_MB,
            VALIDATION_ROW_CNT,
            VALIDATION_COLUMN_CNT,
            VALIDATION_FILE_SIZE_MB,
            VALIDATION_PARTITION_CNT,
            VALIDATION_INDEX_CNT,
            VALIDATION_CONSTRAINT_CNT,
            VALIDATION_TRIGGER_CNT,
            GETDATE() AS ETL_LOAD_TS,
            -- Router logic: Determine operation type
            CASE 
                WHEN UPDATE_TS IS NULL THEN 'INSERT'
                ELSE 'UPDATE'
            END AS OPERATION_TYPE
        FROM CTE_Source
    ),
    
    -- ============================================
    -- ROUTER CTE: Separate INSERT and UPDATE
    -- ============================================
    CTE_Insert AS (
        SELECT * FROM CTE_Transformed WHERE OPERATION_TYPE = 'INSERT'
    ),
    CTE_Update AS (
        SELECT * FROM CTE_Transformed WHERE OPERATION_TYPE = 'UPDATE'
    )
    
    -- ============================================
    -- MERGE OPERATION (UPSERT)
    -- ============================================
    MERGE INTO DATA_SRC_VALIDATION_LOAD AS Target
    USING CTE_Transformed AS Source
    ON Target.DATA_SRC_VALIDATION_ID = Source.DATA_SRC_VALIDATION_ID
    
    -- UPDATE existing records
    WHEN MATCHED THEN
        UPDATE SET
            Target.DATA_SRC_ID = Source.DATA_SRC_ID,
            Target.VALIDATION_TYPE_CD = Source.VALIDATION_TYPE_CD,
            Target.VALIDATION_QUERY_TXT = Source.VALIDATION_QUERY_TXT,
            Target.VALIDATION_RESULT_CNT = Source.VALIDATION_RESULT_CNT,
            Target.VALIDATION_THRESHOLD_CNT = Source.VALIDATION_THRESHOLD_CNT,
            Target.VALIDATION_STATUS_CD = Source.VALIDATION_STATUS_CD,
            Target.VALIDATION_DT = Source.VALIDATION_DT,
            Target.VALIDATION_COMMENTS_TXT = Source.VALIDATION_COMMENTS_TXT,
            Target.CREATE_USER_ID = Source.CREATE_USER_ID,
            Target.CREATE_TS = Source.CREATE_TS,
            Target.UPDATE_USER_ID = Source.UPDATE_USER_ID,
            Target.UPDATE_TS = Source.UPDATE_TS,
            Target.ACTV_IND = Source.ACTV_IND,
            Target.VALIDATION_NAME_TXT = Source.VALIDATION_NAME_TXT,
            Target.VALIDATION_DESCRIPTION_TXT = Source.VALIDATION_DESCRIPTION_TXT,
            Target.VALIDATION_FREQUENCY_CD = Source.VALIDATION_FREQUENCY_CD,
            Target.NOTIFICATION_EMAIL_TXT = Source.NOTIFICATION_EMAIL_TXT,
            Target.LAST_VALIDATION_DT = Source.LAST_VALIDATION_DT,
            Target.NEXT_VALIDATION_DT = Source.NEXT_VALIDATION_DT,
            Target.VALIDATION_PRIORITY_CD = Source.VALIDATION_PRIORITY_CD,
            Target.VALIDATION_OWNER_TXT = Source.VALIDATION_OWNER_TXT,
            Target.VALIDATION_CATEGORY_CD = Source.VALIDATION_CATEGORY_CD,
            Target.VALIDATION_SUBCATEGORY_CD = Source.VALIDATION_SUBCATEGORY_CD,
            Target.VALIDATION_SEVERITY_CD = Source.VALIDATION_SEVERITY_CD,
            Target.VALIDATION_IMPACT_CD = Source.VALIDATION_IMPACT_CD,
            Target.VALIDATION_RESOLUTION_TXT = Source.VALIDATION_RESOLUTION_TXT,
            Target.VALIDATION_NOTES_TXT = Source.VALIDATION_NOTES_TXT,
            Target.VALIDATION_TAGS_TXT = Source.VALIDATION_TAGS_TXT,
            Target.VALIDATION_METADATA_TXT = Source.VALIDATION_METADATA_TXT,
            Target.VALIDATION_EXECUTION_TIME_SEC = Source.VALIDATION_EXECUTION_TIME_SEC,
            Target.VALIDATION_ERROR_MSG_TXT = Source.VALIDATION_ERROR_MSG_TXT,
            Target.VALIDATION_WARNING_MSG_TXT = Source.VALIDATION_WARNING_MSG_TXT,
            Target.VALIDATION_INFO_MSG_TXT = Source.VALIDATION_INFO_MSG_TXT,
            Target.VALIDATION_DEBUG_MSG_TXT = Source.VALIDATION_DEBUG_MSG_TXT,
            Target.VALIDATION_TRACE_MSG_TXT = Source.VALIDATION_TRACE_MSG_TXT,
            Target.VALIDATION_AUDIT_ID = Source.VALIDATION_AUDIT_ID,
            Target.VALIDATION_RUN_ID = Source.VALIDATION_RUN_ID,
            Target.VALIDATION_BATCH_ID = Source.VALIDATION_BATCH_ID,
            Target.VALIDATION_JOB_ID = Source.VALIDATION_JOB_ID,
            Target.VALIDATION_STEP_ID = Source.VALIDATION_STEP_ID,
            Target.VALIDATION_TASK_ID = Source.VALIDATION_TASK_ID,
            Target.VALIDATION_SUBTASK_ID = Source.VALIDATION_SUBTASK_ID,
            Target.VALIDATION_SEQUENCE_NBR = Source.VALIDATION_SEQUENCE_NBR,
            Target.VALIDATION_RETRY_CNT = Source.VALIDATION_RETRY_CNT,
            Target.VALIDATION_MAX_RETRY_CNT = Source.VALIDATION_MAX_RETRY_CNT,
            Target.VALIDATION_TIMEOUT_SEC = Source.VALIDATION_TIMEOUT_SEC,
            Target.VALIDATION_START_TS = Source.VALIDATION_START_TS,
            Target.VALIDATION_END_TS = Source.VALIDATION_END_TS,
            Target.VALIDATION_DURATION_SEC = Source.VALIDATION_DURATION_SEC,
            Target.VALIDATION_CPU_TIME_SEC = Source.VALIDATION_CPU_TIME_SEC,
            Target.VALIDATION_MEMORY_MB = Source.VALIDATION_MEMORY_MB,
            Target.VALIDATION_DISK_IO_MB = Source.VALIDATION_DISK_IO_MB,
            Target.VALIDATION_NETWORK_IO_MB = Source.VALIDATION_NETWORK_IO_MB,
            Target.VALIDATION_ROW_CNT = Source.VALIDATION_ROW_CNT,
            Target.VALIDATION_COLUMN_CNT = Source.VALIDATION_COLUMN_CNT,
            Target.VALIDATION_FILE_SIZE_MB = Source.VALIDATION_FILE_SIZE_MB,
            Target.VALIDATION_PARTITION_CNT = Source.VALIDATION_PARTITION_CNT,
            Target.VALIDATION_INDEX_CNT = Source.VALIDATION_INDEX_CNT,
            Target.VALIDATION_CONSTRAINT_CNT = Source.VALIDATION_CONSTRAINT_CNT,
            Target.VALIDATION_TRIGGER_CNT = Source.VALIDATION_TRIGGER_CNT,
            Target.ETL_LOAD_TS = Source.ETL_LOAD_TS
    
    -- INSERT new records
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            DATA_SRC_VALIDATION_ID,
            DATA_SRC_ID,
            VALIDATION_TYPE_CD,
            VALIDATION_QUERY_TXT,
            VALIDATION_RESULT_CNT,
            VALIDATION_THRESHOLD_CNT,
            VALIDATION_STATUS_CD,
            VALIDATION_DT,
            VALIDATION_COMMENTS_TXT,
            CREATE_USER_ID,
            CREATE_TS,
            UPDATE_USER_ID,
            UPDATE_TS,
            ACTV_IND,
            VALIDATION_NAME_TXT,
            VALIDATION_DESCRIPTION_TXT,
            VALIDATION_FREQUENCY_CD,
            NOTIFICATION_EMAIL_TXT,
            LAST_VALIDATION_DT,
            NEXT_VALIDATION_DT,
            VALIDATION_PRIORITY_CD,
            VALIDATION_OWNER_TXT,
            VALIDATION_CATEGORY_CD,
            VALIDATION_SUBCATEGORY_CD,
            VALIDATION_SEVERITY_CD,
            VALIDATION_IMPACT_CD,
            VALIDATION_RESOLUTION_TXT,
            VALIDATION_NOTES_TXT,
            VALIDATION_TAGS_TXT,
            VALIDATION_METADATA_TXT,
            VALIDATION_EXECUTION_TIME_SEC,
            VALIDATION_ERROR_MSG_TXT,
            VALIDATION_WARNING_MSG_TXT,
            VALIDATION_INFO_MSG_TXT,
            VALIDATION_DEBUG_MSG_TXT,
            VALIDATION_TRACE_MSG_TXT,
            VALIDATION_AUDIT_ID,
            VALIDATION_RUN_ID,
            VALIDATION_BATCH_ID,
            VALIDATION_JOB_ID,
            VALIDATION_STEP_ID,
            VALIDATION_TASK_ID,
            VALIDATION_SUBTASK_ID,
            VALIDATION_SEQUENCE_NBR,
            VALIDATION_RETRY_CNT,
            VALIDATION_MAX_RETRY_CNT,
            VALIDATION_TIMEOUT_SEC,
            VALIDATION_START_TS,
            VALIDATION_END_TS,
            VALIDATION_DURATION_SEC,
            VALIDATION_CPU_TIME_SEC,
            VALIDATION_MEMORY_MB,
            VALIDATION_DISK_IO_MB,
            VALIDATION_NETWORK_IO_MB,
            VALIDATION_ROW_CNT,
            VALIDATION_COLUMN_CNT,
            VALIDATION_FILE_SIZE_MB,
            VALIDATION_PARTITION_CNT,
            VALIDATION_INDEX_CNT,
            VALIDATION_CONSTRAINT_CNT,
            VALIDATION_TRIGGER_CNT,
            ETL_LOAD_TS
        )
        VALUES (
            Source.DATA_SRC_VALIDATION_ID,
            Source.DATA_SRC_ID,
            Source.VALIDATION_TYPE_CD,
            Source.VALIDATION_QUERY_TXT,
            Source.VALIDATION_RESULT_CNT,
            Source.VALIDATION_THRESHOLD_CNT,
            Source.VALIDATION_STATUS_CD,
            Source.VALIDATION_DT,
            Source.VALIDATION_COMMENTS_TXT,
            Source.CREATE_USER_ID,
            Source.CREATE_TS,
            Source.UPDATE_USER_ID,
            Source.UPDATE_TS,
            Source.ACTV_IND,
            Source.VALIDATION_NAME_TXT,
            Source.VALIDATION_DESCRIPTION_TXT,
            Source.VALIDATION_FREQUENCY_CD,
            Source.NOTIFICATION_EMAIL_TXT,
            Source.LAST_VALIDATION_DT,
            Source.NEXT_VALIDATION_DT,
            Source.VALIDATION_PRIORITY_CD,
            Source.VALIDATION_OWNER_TXT,
            Source.VALIDATION_CATEGORY_CD,
            Source.VALIDATION_SUBCATEGORY_CD,
            Source.VALIDATION_SEVERITY_CD,
            Source.VALIDATION_IMPACT_CD,
            Source.VALIDATION_RESOLUTION_TXT,
            Source.VALIDATION_NOTES_TXT,
            Source.VALIDATION_TAGS_TXT,
            Source.VALIDATION_METADATA_TXT,
            Source.VALIDATION_EXECUTION_TIME_SEC,
            Source.VALIDATION_ERROR_MSG_TXT,
            Source.VALIDATION_WARNING_MSG_TXT,
            Source.VALIDATION_INFO_MSG_TXT,
            Source.VALIDATION_DEBUG_MSG_TXT,
            Source.VALIDATION_TRACE_MSG_TXT,
            Source.VALIDATION_AUDIT_ID,
            Source.VALIDATION_RUN_ID,
            Source.VALIDATION_BATCH_ID,
            Source.VALIDATION_JOB_ID,
            Source.VALIDATION_STEP_ID,
            Source.VALIDATION_TASK_ID,
            Source.VALIDATION_SUBTASK_ID,
            Source.VALIDATION_SEQUENCE_NBR,
            Source.VALIDATION_RETRY_CNT,
            Source.VALIDATION_MAX_RETRY_CNT,
            Source.VALIDATION_TIMEOUT_SEC,
            Source.VALIDATION_START_TS,
            Source.VALIDATION_END_TS,
            Source.VALIDATION_DURATION_SEC,
            Source.VALIDATION_CPU_TIME_SEC,
            Source.VALIDATION_MEMORY_MB,
            Source.VALIDATION_DISK_IO_MB,
            Source.VALIDATION_NETWORK_IO_MB,
            Source.VALIDATION_ROW_CNT,
            Source.VALIDATION_COLUMN_CNT,
            Source.VALIDATION_FILE_SIZE_MB,
            Source.VALIDATION_PARTITION_CNT,
            Source.VALIDATION_INDEX_CNT,
            Source.VALIDATION_CONSTRAINT_CNT,
            Source.VALIDATION_TRIGGER_CNT,
            Source.ETL_LOAD_TS
        );
    
    -- ============================================
    -- LOGGING
    -- ============================================
    SET @RowsInserted = @@ROWCOUNT;
    
    DECLARE @EndTime DATETIME2 = GETDATE();
    DECLARE @Duration INT = DATEDIFF(SECOND, @StartTime, @EndTime);
    
    PRINT 'MERGE operation completed successfully';
    PRINT 'Rows affected: ' + CAST(@RowsInserted AS VARCHAR);
    PRINT 'End Time: ' + CONVERT(VARCHAR, @EndTime, 121);
    PRINT 'Duration (seconds): ' + CAST(@Duration AS VARCHAR);
    PRINT 'Mapping execution completed successfully';
    
END TRY
BEGIN CATCH
    -- ============================================
    -- ERROR HANDLING
    -- ============================================
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
    DECLARE @ErrorState INT = ERROR_STATE();
    DECLARE @ErrorLine INT = ERROR_LINE();
    
    PRINT 'Error occurred in mapping: m_Ciim048d_855_Data_Src_Validation_Load';
    PRINT 'Error Message: ' + @ErrorMessage;
    PRINT 'Error Line: ' + CAST(@ErrorLine AS VARCHAR);
    PRINT 'Error Severity: ' + CAST(@ErrorSeverity AS VARCHAR);
    PRINT 'Error State: ' + CAST(@ErrorState AS VARCHAR);
    
    -- Re-throw the error
    THROW;
END CATCH;
