-- Add 1 new table, d_dl_competitor_store_mapping
-- This table should be maintained manually , the EC team provide the raw data yearly/monthly,
-- Then we upload the data into this table
create table edw.d_qbt_competitor_store_mapping (
	shop varchar(255),
	platform varchar(255),
	store_type varchar(255)
);

update edw.d_dl_competitor_store_mapping set platform = lower(platform);

-- edw.f_qbt_sku_monitor add 2 new column
-- store_type, updated against edw.d_dl_competitor_store_mapping.[store_type]
-- rsp, updated against product.rsp
alter table edw.f_qbt_sku_monitor add column store_type varchar(255);
alter table edw.f_qbt_sku_monitor add column rsp numeric(17,6);

-- edw.f_qbt_sku_monitor add 1 new column
-- store_type, updated by edw.d_dl_competitor_store_mapping.[store_type]
alter table edw.f_qbt_store_sales add column store_type varchar(255);
