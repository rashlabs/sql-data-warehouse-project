/*
==============================================================================

Stored Procedure : Load Bronze Layer (Source -> Bronze)

==============================================================================

Script Purpose:
 This stored procedure loads data into the 'bronze' schema from external CSV files.
 It performs the following actions:
- Truncates the bronze tables before loading data.
- Uses the 'COPY' (bulk inserting) command to load data from CSV files into bronze tables.

Parameters:
None.
This stored procedure does not accept any parameters or return any values.

Usage Example: [Use to see output for bulk inserting]
 CALL bronze.load_bronze();
===============================================================================
*/

---------------------------------------------------------------------------------
-- Drop if exists
DROP PROCEDURE IF EXISTS bronze.load_bronze;

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time   TIMESTAMP;
BEGIN
    RAISE NOTICE '=====================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '=====================================';

    RAISE NOTICE '---------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '---------------------------------------';

    -- crm_cust_info
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;
    COPY bronze.crm_cust_info
    FROM '/Users/rahuls/Documents/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');
    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM end_time - start_time);

    -- crm_prd_info
    RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info
    FROM '/Users/rahuls/Documents/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

    -- crm_sales_details
    RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details
    FROM '/Users/rahuls/Documents/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

    RAISE NOTICE '---------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '---------------------------------------';

    -- erp_cust_az12
    RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12
    FROM '/Users/rahuls/Documents/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

    -- erp_loc_a101
    RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101
    FROM '/Users/rahuls/Documents/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

    -- erp_px_cat_g1v2
    RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
    FROM '/Users/rahuls/Documents/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '==============================';
        RAISE NOTICE 'ERROR OCCURRED DURING BRONZE LAYER';
        RAISE NOTICE 'ERROR MESSAGE: %', SQLERRM;
        RAISE NOTICE '==============================';
END;
$$;
---------------------------------------------------------------------------------------------
