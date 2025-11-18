--Data EDA
--Check for null or duplicates in the primery key
--Expectation: no result
USE DataWarehouse;

SELECT cst_id,
count(*) FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT (*) > 1 

--Handling the duplicates, select and rank by the created date desc
SELECT * 
FROM ( SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
)t WHERE flag_last = 1


--check for unwanted spaces i.e
--If the orinal value is not equal to the same value after trimming it means there are spaces
SELECT cst_firstname FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)


INSERT INTO silver.crm_cust_info (
	cst_id, 
	cst_key, 
	cst_firstname, 
	cst_lastname, 
	cst_material_status, 
	cst_gender,
	cst_create_date
		)
--Transformation
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname)AS cst_firstname,
TRIM(cst_lastname)AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
     WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
	 ELSE 'n/a'
END cst_material_status,
CASE WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
     WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gender,
cst_create_date
FROM (
    SELECT
 	*,
	   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
	) t
WHERE flag_last = 1; -- Select the most recent record per customer


--Data standardization & consistency
SELECT DISTINCT cst_gender
FROM silver.crm_cust_info

--lets check the data iin the silver layer now

SELECT cst_id,
count(*) FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT (*) > 1 

SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)


SELECT * FROM silver.crm_cust_info

--Table 2
--Check for data quality
USE DataWarehouse
SELECT * FROM bronze.crm_prd_info

SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') AS cat_id,--extract the first 3 characters of the prd_key as the category_id
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info

SELECT distinct id FROM bronze.erp_px_cat_g1v2
