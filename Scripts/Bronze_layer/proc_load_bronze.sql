/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

Use DataWarehouse
go
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	Declare @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	
	Begin Try
	set @batch_start_time = GETDATE();
		print '=============================================================================';
		print 'Loading bronze layer';
		print '=============================================================================';


		print '-----------------------------------------------------------------------------';
		print 'Loading CRM Tables';
		print '-----------------------------------------------------------------------------';

		SET @start_time = getdate();
		print '>> Truncating Table: [bronze].[crm_cust_info]';
		TRUNCATE TABLE [bronze].[crm_cust_info] 
		print '>> Inserting Data Into: [bronze].[crm_cust_info]';
		BULK INSERT [bronze].[crm_cust_info] 
		from 'A:\datawarehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			firstrow = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = getdate();
		print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR ) + 'seconds';
		print '------------------------'
	
		SET @start_time = getdate();
		print '>> Truncating Table: [bronze].[crm_prd_info]';
		TRUNCATE TABLE [bronze].[crm_prd_info]
		print '>> Inserting Data Into: [bronze].[crm_prd_info]';
		BULK INSERT [bronze].[crm_prd_info]
		from 'A:\datawarehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			firstrow = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = getdate();
		print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR ) + 'seconds';
		print '------------------------'
	

		SET @start_time = getdate();
		print '>> Truncating Table: [bronze].[crm_sale_details]';
		TRUNCATE TABLE [bronze].[crm_sale_details]
		print '>> Inserting Data Into: [bronze].[crm_sale_details]';
		BULK INSERT [bronze].[crm_sale_details]
		from 'A:\datawarehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			firstrow = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = getdate();
		print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR ) + 'seconds';
		print '------------------------'
	

		print '-----------------------------------------------------------------------------';
		print 'Loading ERP Tables';
		print '-----------------------------------------------------------------------------';


		SET @start_time = getdate();
		print '>> Truncating Table: [bronze].[erp_CUST_AZ12]';
		TRUNCATE TABLE [bronze].[erp_CUST_AZ12]
		print '>> Inserting Data Into: [bronze].[erp_CUST_AZ12]';
		BULK INSERT [bronze].[erp_CUST_AZ12]
		from 'A:\datawarehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.CSV'
		WITH (
			firstrow = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = getdate();
		print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR ) + 'seconds';
		print '------------------------'


	
		SET @start_time = getdate();
		print '>> Truncating Table: [bronze].[erp_LOC_A101]';
		TRUNCATE TABLE [bronze].[erp_LOC_A101]
		print '>> Inserting Data Into: [bronze].[erp_LOC_A101]';
		BULK INSERT [bronze].[erp_LOC_A101]
		from 'A:\datawarehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.CSV'
		WITH (
			firstrow = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = getdate();
		print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR ) + 'seconds';
		print '------------------------'


	
		SET @start_time = getdate();
		print '>> Truncating Table: [bronze].[erp_PX_CAT_G1V2]';
		TRUNCATE TABLE [bronze].[erp_PX_CAT_G1V2]
		print '>> Inserting Data Into: [bronze].[erp_PX_CAT_G1V2]';
		BULK INSERT [bronze].[erp_PX_CAT_G1V2]
		from 'A:\datawarehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			firstrow = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = getdate();
		print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR ) + 'seconds';
		print '------------------------'
	
	
	SET @batch_end_time = getdate();
	print'=================================================================='
	print 'Loading Bronze layer completed'
	print '>> bronze layer total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR ) + 'seconds';	
	print'=================================================================='
	End try

	Begin catch
	print('========================================================================================');
	print('Error Occured during loading Bronze Layer')
	print('Error Message' + ERROR_MESSAGE())
	print('Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR))
	print('Error Message' + CAST(ERROR_STATE() AS NVARCHAR))
	print('========================================================================================');
	End catch
	
END
