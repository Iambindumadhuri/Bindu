MERGE INTO `tds_sales.sales_target` AS target
USING `ds_sales.sales_source` AS source
ON target.INVOICE_AND_ITEM_NUMBER = source.invoice_and_item_number
 AND target.DATE = source.DATE
 AND target.STORE_ID = SAFE_CAST(source.store_number AS INT64)  -- Casting to INT64
WHEN MATCHED THEN
 UPDATE SET
   target.INVOICE_AND_ITEM_NUMBER = source.invoice_and_item_number,
   target.DATE = source.DATE,
   target.STORE_ID = SAFE_CAST(source.store_number AS INT64),  -- Casting to INT64
   target.STORE_NM = source.Store_name,
   target.ADDRESS = source.Address,
   target.CITY = source.City,
   target.ZIPCODE = source.Zip_code,
   target.COUNTRY_NUM = source.County_number,
   target.DW_UPDATE_TS = CURRENT_TIMESTAMP(),  -- Update timestamp for the record
   target.DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP()  -- Update last update timestamp
WHEN NOT MATCHED THEN
 INSERT (
   INVOICE_AND_ITEM_NUMBER,
   DATE,
   STORE_ID,
   STORE_NM,
   ADDRESS,
   CITY,
   ZIPCODE,
   COUNTRY_NUM,
   DW_CREATE_TS,
   DW_UPDATE_TS,
   DW_LAST_UPDATE_TS
 )
 VALUES (
   source.invoice_and_item_number,
   source.DATE,
   SAFE_CAST(source.store_number AS INT64),  -- Casting to INT64
   source.Store_name,
   source.Address,
   source.City,
   source.Zip_code,
   source.County_number,
   CURRENT_TIMESTAMP(),  -- Set current timestamp for DW_CREATE_TS
   CURRENT_TIMESTAMP(),  -- Set current timestamp for DW_UPDATE_TS
   '9999-01-01'  -- Default value for DW_LAST_UPDATE_TS
 );
