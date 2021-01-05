-- ods.r_bu_dly_jd_traffic definition

-- Drop table

-- DROP FOREIGN TABLE ods.r_bu_dly_jd_traffic;

CREATE FOREIGN TABLE ods.r_bu_dly_jd_traffic (
	store text NULL,
	pv text NULL,
	uv text NULL,
	new_fans text NULL,
	"date" text NULL,
	dl_batch_date varchar(8) NULL,
	dl_load_time timestamp NULL
)
SERVER cdp_oss_server
OPTIONS (prefix 'ODS/BU/dly_jd_traffic/', format 'csv', delimiter ',', header 'false', quote '"', escape '"', encoding 'UTF-8');


-- ods.r_bu_dly_tm_traffic definition

-- Drop table

-- DROP FOREIGN TABLE ods.r_bu_dly_tm_traffic;

CREATE FOREIGN TABLE ods.r_bu_dly_tm_traffic (
	store text NULL,
	pv text NULL,
	uv text NULL,
	new_fans text NULL,
	"date" text NULL,
	dl_batch_date varchar(8) NULL,
	dl_load_time timestamp NULL
)
SERVER cdp_oss_server
OPTIONS (prefix 'ODS/BU/dly_tm_traffic/', format 'csv', delimiter ',', header 'false', quote '"', escape '"', encoding 'UTF-8');


-- ods.r_bu_jd_b2b_gwp definition

-- Drop table

-- DROP FOREIGN TABLE ods.r_bu_jd_b2b_gwp;

CREATE FOREIGN TABLE ods.r_bu_jd_b2b_gwp (
	store_sku_id text NULL,
	lego_sku_id text NULL,
	store_sku_name text NULL,
	dl_batch_date varchar(8) NULL,
	dl_load_time timestamp NULL
)
SERVER cdp_oss_server
OPTIONS (prefix 'ODS/BU/jd_b2b_gwp/', format 'csv', delimiter ',', header 'false', quote '"', escape '"', encoding 'UTF-8');


-- ods.r_bu_jd_b2b_sku_map definition

-- Drop table

-- DROP FOREIGN TABLE ods.r_bu_jd_b2b_sku_map;

CREATE FOREIGN TABLE ods.r_bu_jd_b2b_sku_map (
	store_sku_id text NULL,
	lego_sku_id text NULL,
	store_sku_name text NULL,
	dl_batch_date varchar(8) NULL,
	dl_load_time timestamp NULL
)
SERVER cdp_oss_server
OPTIONS (prefix 'ODS/BU/jd_b2b_sku_map/', format 'csv', delimiter ',', header 'false', quote '"', escape '"', encoding 'UTF-8');


-- ods.r_bu_jd_pop_gwp definition

-- Drop table

-- DROP FOREIGN TABLE ods.r_bu_jd_pop_gwp;

CREATE FOREIGN TABLE ods.r_bu_jd_pop_gwp (
	store_sku_id text NULL,
	lego_sku_id text NULL,
	store_sku_name text NULL,
	dl_batch_date varchar(8) NULL,
	dl_load_time timestamp NULL
)
SERVER cdp_oss_server
OPTIONS (prefix 'ODS/BU/jd_pop_gwp/', format 'csv', delimiter ',', header 'false', quote '"', escape '"', encoding 'UTF-8');


-- ods.r_bu_jd_pop_sku_map definition

-- Drop table

-- DROP FOREIGN TABLE ods.r_bu_jd_pop_sku_map;

CREATE FOREIGN TABLE ods.r_bu_jd_pop_sku_map (
	store_sku_id text NULL,
	lego_sku_id text NULL,
	store_sku_name text NULL,
	dl_batch_date varchar(8) NULL,
	dl_load_time timestamp NULL
)
SERVER cdp_oss_server
OPTIONS (prefix 'ODS/BU/jd_pop_sku_map/', format 'csv', delimiter ',', header 'false', quote '"', escape '"', encoding 'UTF-8');


-- ods.r_bu_mly_jd_traffic definition

-- Drop table

-- DROP FOREIGN TABLE ods.r_bu_mly_jd_traffic;

CREATE FOREIGN TABLE ods.r_bu_mly_jd_traffic (
	store text NULL,
	uv text NULL,
	yearmonth text NULL,
	dl_batch_date varchar(8) NULL,
	dl_load_time timestamp NULL
)
SERVER cdp_oss_server
OPTIONS (prefix 'ODS/BU/mly_jd_traffic/', format 'csv', delimiter ',', header 'false', quote '"', escape '"', encoding 'UTF-8');


-- ods.r_bu_mly_tm_traffic definition

-- Drop table

-- DROP FOREIGN TABLE ods.r_bu_mly_tm_traffic;

CREATE FOREIGN TABLE ods.r_bu_mly_tm_traffic (
	store text NULL,
	uv text NULL,
	yearmonth text NULL,
	dl_batch_date varchar(8) NULL,
	dl_load_time timestamp NULL
)
SERVER cdp_oss_server
OPTIONS (prefix 'ODS/BU/mly_tm_traffic/', format 'csv', delimiter ',', header 'false', quote '"', escape '"', encoding 'UTF-8');


-- ods.r_bu_tm_gwp definition

-- Drop table

-- DROP FOREIGN TABLE ods.r_bu_tm_gwp;

CREATE FOREIGN TABLE ods.r_bu_tm_gwp (
	store_sku_id text NULL,
	lego_sku_id text NULL,
	store_sku_name text NULL,
	dl_batch_date varchar(8) NULL,
	dl_load_time timestamp NULL
)
SERVER cdp_oss_server
OPTIONS (prefix 'ODS/BU/tm_gwp/', format 'csv', delimiter ',', header 'false', quote '"', escape '"', encoding 'UTF-8');


-- ods.r_bu_tm_sku_map definition

-- Drop table

-- DROP FOREIGN TABLE ods.r_bu_tm_sku_map;

CREATE FOREIGN TABLE ods.r_bu_tm_sku_map (
	store_sku_id text NULL,
	lego_sku_id text NULL,
	store_sku_name text NULL,
	dl_batch_date varchar(8) NULL,
	dl_load_time timestamp NULL
)
SERVER cdp_oss_server
OPTIONS (prefix 'ODS/BU/tm_sku_map/', format 'csv', delimiter ',', header 'false', quote '"', escape '"', encoding 'UTF-8');


-- ods.r_cs_consumer_info definition

-- Drop table

-- DROP FOREIGN TABLE ods.r_cs_consumer_info;

CREATE FOREIGN TABLE ods.r_cs_consumer_info (
	partner text NULL,
	name_first text NULL,
	name_last text NULL,
	country text NULL,
	city text NULL,
	street text NULL,
	zip_code text NULL,
	legoid_name text NULL,
	fax text NULL,
	email text NULL,
	telephone text NULL,
	mobile text NULL,
	vip_card text NULL,
	region text NULL,
	change_date text NULL,
	deletion_mark text NULL,
	dl_batch_date varchar(8) NULL,
	dl_load_time timestamp NULL
)
SERVER cdp_oss_server
OPTIONS (prefix 'ODS/CS/consumer_info/', format 'csv', delimiter ',', header 'false', quote '"', escape '"', encoding 'UTF-8');


-- ods.r_dl_calendar definition

-- Drop table

-- DROP FOREIGN TABLE ods.r_dl_calendar;

CREATE FOREIGN TABLE ods.r_dl_calendar (
	date_id text NULL,
	"day" text NULL,
	week text NULL,
	"month" text NULL,
	"year" text NULL,
	fiscal_year text NULL,
	wk_year text NULL,
	is_chinese_holiday text NULL,
	is_holiday text NULL,
	lego_day text NULL,
	lego_week text NULL,
	lego_month text NULL,
	lego_year text NULL,
	dl_batch_date varchar(8) NULL,
	dl_load_time timestamp NULL
)
SERVER cdp_oss_server
OPTIONS (prefix 'ODS/DL/calendar/', format 'csv', delimiter ',', header 'false', quote '"', escape '"', encoding 'UTF-8');


-- ods.r_dl_city_tier definition

-- Drop table

-- DROP FOREIGN TABLE ods.r_dl_city_tier;

CREATE FOREIGN TABLE ods.r_dl_city_tier (
	province text NULL,
	muni_pref_lvl text NULL,
	city_chn text NULL,
	city_eng text NULL,
	admin_level text NULL,
	city_tier text NULL,
	"year" text NULL,
	dl_batch_date varchar(8) NULL,
	dl_load_time timestamp NULL
)
SERVER cdp_oss_server
OPTIONS (prefix 'ODS/DL/city_tier/', format 'csv', delimiter ',', header 'false', quote '"', escape '"', encoding 'UTF-8');


-- ods.r_dl_product_info definition

-- Drop table

-- DROP FOREIGN TABLE ods.r_dl_product_info;

CREATE FOREIGN TABLE ods.r_dl_product_info (
	year_version text NULL,
	lego_sku_id text NULL,
	material_id text NULL,
	"version" text NULL,
	lego_sku_name_en text NULL,
	lego_sku_name_cn text NULL,
	rsp text NULL,
	global_launch_date text NULL,
	global_delete_date text NULL,
	embargo_date text NULL,
	is_bu_d2c_launch text NULL,
	bu_d2c_launch_date text NULL,
	bu_d2c_delete_date text NULL,
	is_bu_cn_launch text NULL,
	bu_cn_launch_date text NULL,
	cn_lcs_launch_date text NULL,
	cn_ldc_launch_date text NULL,
	cn_tm_launch_date text NULL,
	cn_jd_b2b_launch_date text NULL,
	cn_jd_pop_launch_date text NULL,
	cn_tru_launch_date text NULL,
	cn_b2b2f_launch_date text NULL,
	cn_dsp_launch_date text NULL,
	cn_dls_ds_launch_date text NULL,
	cn_hamleys_launch_date text NULL,
	cn_kl_indirect_launch_date text NULL,
	cn_kidsland_online_b2c_launch_date text NULL,
	cn_wc_mini_program_launch_date text NULL,
	cn_kidswant_launch_date text NULL,
	exclusive_products text NULL,
	bu_cn_delete_date text NULL,
	product_status text NULL,
	in_and_out text NULL,
	super_segment text NULL,
	prod_cat text NULL,
	product_bu text NULL,
	theme text NULL,
	product_group text NULL,
	cn_line text NULL,
	top_theme text NULL,
	product_class text NULL,
	material_group text NULL,
	is_batteries_included text NULL,
	age_mark text NULL,
	piece_count text NULL,
	case_pack text NULL,
	lbs_size text NULL,
	net_weight_kg text NULL,
	length_mm text NULL,
	width_mm text NULL,
	height_mm text NULL,
	product_ean_code text NULL,
	carton_ean_code text NULL,
	new_list_price_pricelist_ab_china text NULL,
	new_ldc_extended_line_list_price text NULL,
	dl_batch_date varchar(8) NULL,
	dl_load_time timestamp NULL
)
SERVER cdp_oss_server
OPTIONS (prefix 'ODS/DL/product_info/', format 'csv', delimiter ',', header 'false', quote '"', escape '"', encoding 'UTF-8');


-- ods.r_jd_b2b_order_dtl definition

-- Drop table

-- DROP FOREIGN TABLE ods.r_jd_b2b_order_dtl;

CREATE FOREIGN TABLE ods.r_jd_b2b_order_dtl (
	id text NULL,
	parent_ord_id text NULL,
	son_ord_id text NULL,
	shop_id text NULL,
	openid text NULL,
	ord_start_date text NULL,
	ord_end_date text NULL,
	sku_id text NULL,
	good_name text NULL,
	goods_qtty text NULL,
	ord_amt text NULL,
	ord_status text NULL,
	create_date text NULL,
	update_date text NULL,
	dl_batch_date varchar(8) NULL,
	dl_load_time timestamp NULL
)
SERVER cdp_oss_server
OPTIONS (prefix 'ODS/JD/b2b_order_dtl/', format 'csv', delimiter ',', header 'true', quote '"', escape '"', encoding 'UTF-8');


-- ods.r_jd_b2b_order_dtl_his definition

-- Drop table

-- DROP FOREIGN TABLE ods.r_jd_b2b_order_dtl_his;

CREATE FOREIGN TABLE ods.r_jd_b2b_order_dtl_his (
	parent_order_id text NULL,
	store_sku_id text NULL,
	store_sku_name text NULL,
	order_create_time text NULL,
	order_status text NULL,
	actual_amount text NULL,
	pieces_cnt text NULL,
	gmv_unit_price text NULL,
	payment_type text NULL,
	estimated_delivery_date text NULL,
	user_risk_level text NULL,
	branches text NULL,
	delivery_center text NULL,
	datawarehouse text NULL,
	sales_assistant text NULL,
	buyer text NULL,
	dl_batch_date varchar(8) NULL,
	dl_load_time timestamp NULL
)
SERVER cdp_oss_server
OPTIONS (prefix 'ODS/JD/b2b_order_dtl_his/', format 'csv', delimiter ',', header 'true', quote '"', escape '"', encoding 'UTF-8');


-- ods.r_jd_member definition

-- Drop table

-- DROP FOREIGN TABLE ods.r_jd_member;

CREATE FOREIGN TABLE ods.r_jd_member (
	member_id text NULL,
	member_num text NULL,
	telephone text NULL,
	sex text NULL,
	birthday text NULL,
	register_date text NULL,
	openid text NULL,
	create_date text NULL,
	update_date text NULL,
	dl_batch_date varchar(8) NULL,
	dl_load_time timestamp NULL
)
SERVER cdp_oss_server
OPTIONS (prefix 'ODS/JD/member/', format 'csv', delimiter ',', header 'true', quote '"', escape '"', encoding 'UTF-8');


-- ods.r_jd_pop_consignee definition

-- Drop table

-- DROP FOREIGN TABLE ods.r_jd_pop_consignee;

CREATE FOREIGN TABLE ods.r_jd_pop_consignee (
	id text NULL,
	fullname text NULL,
	telephone text NULL,
	mobile text NULL,
	full_address text NULL,
	province text NULL,
	province_id text NULL,
	city text NULL,
	city_id text NULL,
	county text NULL,
	county_id text NULL,
	town text NULL,
	town_id text NULL,
	create_date text NULL,
	update_date text NULL,
	dl_batch_date varchar(8) NULL,
	dl_load_time timestamp NULL
)
SERVER cdp_oss_server
OPTIONS (prefix 'ODS/JD/pop_consignee/', format 'csv', delimiter ',', header 'true', quote '"', escape '"', encoding 'UTF-8');


-- ods.r_jd_pop_order definition

-- Drop table

-- DROP FOREIGN TABLE ods.r_jd_pop_order;

CREATE FOREIGN TABLE ods.r_jd_pop_order (
	id text NULL,
	openid text NULL,
	pin text NULL,
	order_total_price text NULL,
	seller_discount text NULL,
	order_payment text NULL,
	order_source text NULL,
	order_start_time text NULL,
	payment_confirm_time text NULL,
	invoice_info text NULL,
	consignee_info_id text NULL,
	order_state text NULL,
	order_state_remark text NULL,
	create_date text NULL,
	update_date text NULL,
	dl_batch_date varchar(8) NULL,
	dl_load_time timestamp NULL
)
SERVER cdp_oss_server
OPTIONS (prefix 'ODS/JD/pop_order/', format 'csv', delimiter ',', header 'true', quote '"', escape '"', encoding 'UTF-8');


-- ods.r_jd_pop_order_dtl definition

-- Drop table

-- DROP FOREIGN TABLE ods.r_jd_pop_order_dtl;

CREATE FOREIGN TABLE ods.r_jd_pop_order_dtl (
	id text NULL,
	order_search_info_id text NULL,
	sku_id text NULL,
	sku_name text NULL,
	jd_price text NULL,
	item_total text NULL,
	create_date text NULL,
	update_date text NULL,
	dl_batch_date varchar(8) NULL,
	dl_load_time timestamp NULL
)
SERVER cdp_oss_server
OPTIONS (prefix 'ODS/JD/pop_order_dtl/', format 'csv', delimiter ',', header 'true', quote '"', escape '"', encoding 'UTF-8');


-- ods.r_oms_order definition

-- Drop table

-- DROP FOREIGN TABLE ods.r_oms_order;

CREATE FOREIGN TABLE ods.r_oms_order (
	id text NULL,
	platformid text NULL,
	shopid text NULL,
	tmall_order_id text NULL,
	order_type text NULL,
	parent_order_id text NULL,
	pin text NULL,
	consignee_name text NULL,
	consignee_phone text NULL,
	consignee_mobile text NULL,
	province text NULL,
	city text NULL,
	county text NULL,
	delivery_address text NULL,
	order_total_price text NULL,
	discount text NULL,
	freight text NULL,
	actual_order_amt text NULL,
	payment_method text NULL,
	order_source text NULL,
	order_deliver_status text NULL,
	order_recall_status text NULL,
	is_exchanged text NULL,
	is_delivery_confirmed text NULL,
	payment_confirm_time text NULL,
	ship_out_time text NULL,
	delivery_confirm_time text NULL,
	order_create_time text NULL,
	order_update_time text NULL,
	dl_batch_date varchar(8) NULL,
	dl_load_time timestamp NULL
)
SERVER cdp_oss_server
OPTIONS (prefix 'ODS/OMS/order/', format 'csv', delimiter ',', header 'false', quote '"', escape '"', encoding 'UTF-8');


-- ods.r_oms_order_dtl definition

-- Drop table

-- DROP FOREIGN TABLE ods.r_oms_order_dtl;

CREATE FOREIGN TABLE ods.r_oms_order_dtl (
	id text NULL,
	parent_order_id text NULL,
	son_order_id text NULL,
	store_sku_id text NULL,
	store_sku_name text NULL,
	sap_material_id text NULL,
	lego_sku_id text NULL,
	gwp_type text NULL,
	bundle_product_sku text NULL,
	mystery_box_product_sku text NULL,
	piece_cnt text NULL,
	store_sku_rsp_price text NULL,
	lego_sku_gmv_amt text NULL,
	create_time text NULL,
	update_time text NULL,
	dl_batch_date varchar(8) NULL,
	dl_load_time timestamp NULL
)
SERVER cdp_oss_server
OPTIONS (prefix 'ODS/OMS/order_dtl/', format 'csv', delimiter ',', header 'false', quote '"', escape '"', encoding 'UTF-8');


-- ods.r_tm_order_dtl_2018_his definition

-- Drop table

-- DROP FOREIGN TABLE ods.r_tm_order_dtl_2018_his;

CREATE FOREIGN TABLE ods.r_tm_order_dtl_2018_his (
	shop_name text NULL,
	parent_order_id text NULL,
	son_order_id text NULL,
	shopper_nickname text NULL,
	order_create_time text NULL,
	order_payment_time text NULL,
	delivery_time text NULL,
	order_type text NULL,
	son_order_status text NULL,
	store_sku_id text NULL,
	sku text NULL,
	cnline text NULL,
	order_status text NULL,
	rsp text NULL,
	title text NULL,
	pieces_cnt text NULL,
	payable_amount text NULL,
	refund_status text NULL,
	refund_amount text NULL,
	actual_payment_amount text NULL,
	cosignee_province text NULL,
	cosignee_city text NULL,
	cosignee_region text NULL,
	cosignee_street text NULL,
	dl_batch_date varchar(8) NULL,
	dl_load_time timestamp NULL
)
SERVER cdp_oss_server
OPTIONS (prefix 'ODS/TM/order_dtl_2018_his/', format 'csv', delimiter ',', header 'false', quote '"', escape '"', encoding 'UTF-8');


-- ods.r_tm_order_his definition

-- Drop table

-- DROP FOREIGN TABLE ods.r_tm_order_his;

CREATE FOREIGN TABLE ods.r_tm_order_his (
	parent_order_id text NULL,
	shopper_nickname text NULL,
	shopper_alipay_account text NULL,
	payment_no text NULL,
	payment_detail text NULL,
	payable_amount text NULL,
	postage text NULL,
	paid_points text NULL,
	total_amount text NULL,
	rebated_points text NULL,
	actual_payment_amount text NULL,
	actual_paid_points text NULL,
	order_status text NULL,
	shopper_comments text NULL,
	cosignee_name text NULL,
	consinee_address text NULL,
	type_of_delivery text NULL,
	consinee_telephone text NULL,
	consinee_mobile text NULL,
	order_create_time text NULL,
	order_payment_time text NULL,
	product_title text NULL,
	product_category text NULL,
	total_pieces text NULL,
	shopid text NULL,
	shop_name text NULL,
	dl_batch_date varchar(8) NULL,
	dl_load_time timestamp NULL
)
SERVER cdp_oss_server
OPTIONS (prefix 'ODS/TM/order_his/', format 'csv', delimiter ',', header 'false', quote '"', escape '"', encoding 'UTF-8');


-- ods.r_wc_mini_member_info definition

-- Drop table

-- DROP FOREIGN TABLE ods.r_wc_mini_member_info;

CREATE FOREIGN TABLE ods.r_wc_mini_member_info (
	unionid text NULL,
	openid text NULL,
	mobile text NULL,
	register_time text NULL,
	nickname text NULL,
	is_authorized text NULL,
	dl_batch_date varchar(8) NULL,
	dl_load_time timestamp NULL
)
SERVER cdp_oss_server
OPTIONS (prefix 'ODS/WC/mini_member_info/', format 'csv', delimiter ',', header 'false', quote '"', escape '"', encoding 'UTF-8');