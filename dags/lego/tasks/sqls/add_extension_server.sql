create extension pgcrypto; 
create extension oss_fdw;
grant usage on FOREIGN DATA WRAPPER oss_fdw to etl_user;


-- DROP SERVER cdp_oss_server CASCADE
CREATE SERVER cdp_oss_server            
    FOREIGN DATA WRAPPER oss_fdw 
    OPTIONS (
        endpoint 'oss-cn-shanghai-internal.aliyuncs.com',  
        bucket 'cdp-data-rd-dev'       -- OSS BUCKET
  );
  
-- drop USER MAPPING FOR etl_user  cdp_oss_server

CREATE USER MAPPING FOR etl_user  
SERVER cdp_oss_server                        
OPTIONS ( 
  id '',         -- OSS ID
  key ''        -- OSS kEY
);

-- example
CREATE FOREIGN  TABLE cdp.ods.r_cs_consumer_info_test (
	partner text,
	name_first text,
	name_last text,
	country text,
	city text,
	street text,
	zip_code text,
	legoid_name text,
	fax text,
	email text,
	telephone text,
	mobile text,
	vip_card text,
	region text,
	change_date text,
	deletion_mark text,
	dl_batch_date varchar,
	dl_load_time timestamp
)
server cdp_oss_server                                      
    options (
        prefix 'ODS/CS/consumer_info/',       -- prefix 前缀方式匹配相关 OSS 文件
        format 'csv',                                  -- 指定按 text 格式解析
        delimiter ','                                   -- 指定字段分隔符
   );



-------------------------------------------
------   RDS  setting up ------------------
create schema dm;
create schema frn;
create user etl_user with password 'CDPetlDEV';
grant all on schema frn,dm to etl_user;
alter default privileges for user cdp_admin grant all on tables to etl_user; 

create user pt_user with password 'CDPptlDEV';
grant usage on schema dm to pt_user;
alter default privileges for user etl_user grant select on tables to pt_user; 


CREATE EXTENSION if not exists postgres_fdw;

drop SERVER gp_remote;

CREATE SERVER gp_remote
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'gp-uf6014968ma210259.gpdb.rds.aliyuncs.com',
dbname 'cdp', port '3432');
grant all on FOREIGN server gp_remote to etl_user;

DROP USER MAPPING IF EXISTS FOR etl_user SERVER gp_remote;
CREATE USER MAPPING FOR etl_user
SERVER gp_remote
OPTIONS (user 'rds_user', password 'CDPrdsDEV');

--  use etl_user  --
--update routine
drop foreign table if exists frn.a_dl_dly_sales_rpt;
IMPORT FOREIGN SCHEMA dm LIMIT TO (a_dl_dly_sales_rpt,a_dl_mly_sales_rpt,d_dl_scv,f_dl_jd_order,f_dl_jd_order_dtl)
    FROM SERVER gp_remote INTO frn;


create table if not exists dm.a_dl_dly_sales_rpt (like frn.a_dl_dly_sales_rpt);   
create table if not exists dm.a_dl_mly_sales_rpt (like frn.a_dl_mly_sales_rpt);
create table if not exists dm.d_dl_scv (like frn.d_dl_scv);   
create table if not exists dm.f_dl_jd_order (like frn.f_dl_jd_order);
create table if not exists dm.f_dl_jd_order_dtl (like frn.f_dl_jd_order_dtl);   

drop table if exists dm.a_dl_dly_sales_rpt;
create table if not exists dm.a_dl_dly_sales_rpt (like frn.a_dl_dly_sales_rpt);

delete from dm.a_dl_dly_sales_rpt;
insert into dm.a_dl_dly_sales_rpt select * from frn.a_dl_dly_sales_rpt;

delete from dm.a_dl_mly_sales_rpt;
insert into dm.a_dl_mly_sales_rpt select * from frn.a_dl_mly_sales_rpt;

insert into dm.d_dl_scv select * from frn.d_dl_scv;
insert into dm.f_dl_jd_order select * from frn.f_dl_jd_order;
insert into dm.f_dl_jd_order_dtl select * from frn.f_dl_jd_order_dtl;


IMPORT FOREIGN SCHEMA edw LIMIT TO (a_bu_dly_jd_traffic,a_bu_dly_tm_traffic,a_bu_mly_jd_traffic,a_bu_mly_tm_traffic,d_dl_calendar,d_dl_jd_member,d_dl_jd_shopper,d_dl_product_info,d_dl_tm_shopper,f_jd_b2b_order,f_jd_b2b_order_dtl,f_jd_pop_order,f_jd_pop_order_dtl,f_oms_order,f_oms_order_dtl)
FROM SERVER gp_remote INTO frn;

create table if not exists dm.a_bu_dly_jd_traffic (like frn.a_bu_dly_jd_traffic);
create table if not exists dm.a_bu_dly_tm_traffic (like frn.a_bu_dly_tm_traffic);
create table if not exists dm.a_bu_mly_jd_traffic (like frn.a_bu_mly_jd_traffic);
create table if not exists dm.a_bu_mly_tm_traffic (like frn.a_bu_mly_tm_traffic);
create table if not exists dm.d_dl_calendar (like frn.d_dl_calendar);
create table if not exists dm.d_dl_jd_member (like frn.d_dl_jd_member);
create table if not exists dm.d_dl_jd_shopper (like frn.d_dl_jd_shopper);
create table if not exists dm.d_dl_product_info (like frn.d_dl_product_info);
create table if not exists dm.d_dl_tm_shopper (like frn.d_dl_tm_shopper);
create table if not exists dm.f_jd_b2b_order (like frn.f_jd_b2b_order);
create table if not exists dm.f_jd_b2b_order_dtl (like frn.f_jd_b2b_order_dtl);
create table if not exists dm.f_jd_pop_order (like frn.f_jd_pop_order);
create table if not exists dm.f_jd_pop_order_dtl (like frn.f_jd_pop_order_dtl);
create table if not exists dm.f_oms_order (like frn.f_oms_order);
create table if not exists dm.f_oms_order_dtl (like frn.f_oms_order_dtl);

insert into dm.a_bu_dly_jd_traffic select * from  frn.a_bu_dly_jd_traffic;
insert into dm.a_bu_dly_tm_traffic select * from  frn.a_bu_dly_tm_traffic;
insert into dm.a_bu_mly_jd_traffic select * from  frn.a_bu_mly_jd_traffic;
insert into dm.a_bu_mly_tm_traffic select * from  frn.a_bu_mly_tm_traffic;
insert into dm.d_dl_calendar select * from  frn.d_dl_calendar;
insert into dm.d_dl_jd_member select * from  frn.d_dl_jd_member;
insert into dm.d_dl_jd_shopper select * from  frn.d_dl_jd_shopper;
insert into dm.d_dl_product_info select * from  frn.d_dl_product_info;
insert into dm.d_dl_tm_shopper select * from  frn.d_dl_tm_shopper;
insert into dm.f_jd_b2b_order select * from  frn.f_jd_b2b_order;
insert into dm.f_jd_b2b_order_dtl select * from  frn.f_jd_b2b_order_dtl;
insert into dm.f_jd_pop_order select * from  frn.f_jd_pop_order;
insert into dm.f_jd_pop_order_dtl select * from  frn.f_jd_pop_order_dtl;
insert into dm.f_oms_order select * from  frn.f_oms_order;
insert into dm.f_oms_order_dtl select * from  frn.f_oms_order_dtl;
--  use etl_user end --   

------   RDS  setting up end------------------
----------------------------------------------