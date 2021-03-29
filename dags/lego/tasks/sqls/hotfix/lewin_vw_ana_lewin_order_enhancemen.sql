
create view edw.vw_ana_lewin_order_enhancemen as
select 
    order_or_return,
    parent_order_id,
    member_id,
    customer,
    mobile,
    full_name,
    gender,
    register_time,
    is_multiple_paid,
    payment_time,
    lego_store_code,
    distributor_store_code,
    store_name,
    province_cn,
    city_cn,
    discount_amt,
    actual_order_amt,
    operator_id,
    order_create_time,
    t2.lego_sku_id,
    lego_sku_name,
    sku_piece,
    lego_sku_asp_price,
    lego_sku_rrp_price,
    lego_sku_gmv_amt,
    rrp_order_amt,
    is_gwp,
    gwp_type,
    ctt_ref_num,
    top_theme,
    bu_cn_launch_date,
    bu_cn_delete_date,
    product_class
from 
(
    select 
        t1.*,
        p.rsp as lego_sku_rrp_price,
        p.rsp * sku_piece as  rrp_order_amt,
        row_number() over(partition by parent_order_id, t1.lego_sku_id order by case when extract(year from payment_time) >= p.year_version::int then extract(year from payment_time) - p.year_version::int else (extract(year from payment_time) - p.year_version::int)*-1 + 100 end) as rsp_gap
        
    from (
        SELECT 
            'Y' as order_or_return,
            a.parent_order_id,
            m.member_id,
            a.distributor as customer,
            m.mobile,
            m.full_name,
            m.gender,
            m.register_time,
            a.is_multiple_paid,
            a.payment_time,
            a.lego_store_code,
            a.distributor_store_code,
            a.store_name,
            s.province_cn,
            s.city_cn,
            a.discount_amt,
            a.actual_order_amt,
            a.operator_id,
            a.order_create_time,
            b.lego_sku_id,
            b.lego_sku_name,
            b.piece_cnt as sku_piece,
            b.lego_sku_asp_price,
            b.lego_sku_gmv_amt,
            b.is_gwp,
            b.gwp_type,
            b.ctt_ref_num
        FROM edw.f_phy_store_order a
            inner join edw.f_phy_store_order_dtl b on a.parent_order_id = b.parent_order_id
            left join (
                select *  from edw.f_phy_store_member where current_row='Y'
            ) m on a.member_id = m.member_id
            left join edw.f_phy_store s on b.lego_store_code =s.lego_store_code

        union all
        SELECT 
            'N' as order_or_return,
            a.return_order_id,
            m.member_id,
            a.distributor as customer,
            m.mobile,
            m.full_name,
            m.gender,
            m.register_time,
            null as is_multiple_paid,
            a.order_create_time as ayment_time,
            a.lego_store_code,
            a.distributor_store_code,
            a.store_name,
            s.province_cn,
            s.city_cn,
            null as discount_amt,
           (a.return_amt*-1)::numeric(17,2) AS actual_order_amt,
            a.operator_id,
            a.order_create_time,
            b.lego_sku_id,
            b.lego_sku_name,
            (b.piece_cnt*-1):: numeric(15,0) AS sku_piece,
            b.lego_sku_asp_price,
            (b.lego_sku_gmv_amt*-1)::numeric(17,2),
            b.is_gwp,
            b.gwp_type,
            null as ctt_ref_num
        FROM edw.f_phy_store_return_order a
            inner join edw.f_phy_store_return_order_dtl b on a.return_order_id = b.return_order_id
            left join (
                select *  from edw.f_phy_store_member where current_row='Y'
            ) m on a.member_id = m.member_id
            left join edw.f_phy_store s on a.lego_store_code =s.lego_store_code
    )t1 left join edw.d_dl_product_info p on t1.lego_sku_id = p.lego_sku_id
) t2 left join(
    select * from (
        select lego_sku_id, top_theme, bu_cn_launch_date, bu_cn_delete_date,product_class,
            row_number() over(partition by lego_sku_id order by year_version::int desc) as rk
        from edw.d_dl_product_info
    )p1 where p1.rk = 1
)p2  on t2.lego_sku_id = p2.lego_sku_id 
 where t2.rsp_gap =1