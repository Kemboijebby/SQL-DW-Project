CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @DataDirectory NVARCHAR(4000) = N'C:\path\to\SQL-DW-Project\datasets\';
    DECLARE @FilePath NVARCHAR(4000);
    DECLARE @Sql NVARCHAR(MAX);
    DECLARE @StartTime DATETIME2;
    DECLARE @EndTime DATETIME2;

    BEGIN TRY
        PRINT '============================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '============================================================';

        -- CRM: customers
        SET @StartTime = SYSDATETIME();
        SET @FilePath = @DataDirectory + N'cust_info.csv';
        SET @Sql = N'BULK INSERT bronze.crm_cust_info FROM '''
            + REPLACE(@FilePath, NCHAR(39), NCHAR(39) + NCHAR(39))
            + N''' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
        EXEC sys.sp_executesql @Sql;
        SET @EndTime = SYSDATETIME();
        PRINT CONCAT('crm_cust_info loaded in ', DATEDIFF(SECOND, @StartTime, @EndTime), ' seconds');

        -- CRM: products
        SET @StartTime = SYSDATETIME();
        SET @FilePath = @DataDirectory + N'prd_info.csv';
        SET @Sql = N'BULK INSERT bronze.crm_prd_info FROM '''
            + REPLACE(@FilePath, NCHAR(39), NCHAR(39) + NCHAR(39))
            + N''' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
        EXEC sys.sp_executesql @Sql;
        SET @EndTime = SYSDATETIME();
        PRINT CONCAT('crm_prd_info loaded in ', DATEDIFF(SECOND, @StartTime, @EndTime), ' seconds');

        -- CRM: sales
        SET @StartTime = SYSDATETIME();
        SET @FilePath = @DataDirectory + N'sales_details.csv';
        SET @Sql = N'BULK INSERT bronze.crm_sales_details FROM '''
            + REPLACE(@FilePath, NCHAR(39), NCHAR(39) + NCHAR(39))
            + N''' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
        EXEC sys.sp_executesql @Sql;
        SET @EndTime = SYSDATETIME();
        PRINT CONCAT('crm_sales_details loaded in ', DATEDIFF(SECOND, @StartTime, @EndTime), ' seconds');

        -- ERP: customer attributes
        SET @StartTime = SYSDATETIME();
        SET @FilePath = @DataDirectory + N'CUST_AZ12.csv';
        SET @Sql = N'BULK INSERT bronze.erp_cust_az12 FROM '''
            + REPLACE(@FilePath, NCHAR(39), NCHAR(39) + NCHAR(39))
            + N''' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
        EXEC sys.sp_executesql @Sql;
        SET @EndTime = SYSDATETIME();
        PRINT CONCAT('erp_cust_az12 loaded in ', DATEDIFF(SECOND, @StartTime, @EndTime), ' seconds');

        -- ERP: locations
        SET @StartTime = SYSDATETIME();
        SET @FilePath = @DataDirectory + N'LOC_A101.csv';
        SET @Sql = N'BULK INSERT bronze.erp_loc_a101 FROM '''
            + REPLACE(@FilePath, NCHAR(39), NCHAR(39) + NCHAR(39))
            + N''' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
        EXEC sys.sp_executesql @Sql;
        SET @EndTime = SYSDATETIME();
        PRINT CONCAT('erp_loc_a101 loaded in ', DATEDIFF(SECOND, @StartTime, @EndTime), ' seconds');

        -- ERP: product categories
        SET @StartTime = SYSDATETIME();
        SET @FilePath = @DataDirectory + N'PX_CAT_G1V2.csv';
        SET @Sql = N'BULK INSERT bronze.erp_px_cat_g1v2 FROM '''
            + REPLACE(@FilePath, NCHAR(39), NCHAR(39) + NCHAR(39))
            + N''' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
        EXEC sys.sp_executesql @Sql;
        SET @EndTime = SYSDATETIME();
        PRINT CONCAT('erp_px_cat_g1v2 loaded in ', DATEDIFF(SECOND, @StartTime, @EndTime), ' seconds');

        PRINT 'Bronze Layer load completed.';
    END TRY
    BEGIN CATCH
        PRINT CONCAT('Bronze Layer load failed: ', ERROR_MESSAGE());
        THROW;
    END CATCH
END;
GO
