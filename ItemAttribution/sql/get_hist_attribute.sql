select distinct oblg.itm_skey
    ,co.co_nbr||cust.cust_nbr as co_cust_nbr
    , co.co_nbr
    , co.mkt_lvl
    , oblg.oblig_dt
    , itm.itm_nbr
    , itm.itm_desc
    , itm_osd.attr_grp_nm
    , osd.catman_attr_prr_nbr
    , osd.osd_decided_val
    , osd.osd_attr_nm
    , sum(oblg.case_sold_qty) as case_sum
from edwp.sale_oblig_dtl_fact as oblg
inner join edwp.cust_ship_to_dim as cust
on cust.cust_skey = oblg.cust_skey
inner join edwp.itm_osd_dim as itm_osd
on oblg.itm_skey = itm_osd.itm_skey
inner join edwp.itm_dim as itm
on oblg.itm_skey = itm.itm_skey
inner join edwp.itm_osd_attr_dim as osd
on osd.itm_skey = itm.itm_skey
left join edwp.org_co_dim as co
on co.co_skey = cust.co_skey
where oblg.oblig_dt > CURRENT_DATE - 30
and oblg.net_prc_ext_amt > 0
and cust.curr_rec_ind = 'Y'
and co.co_nbr||cust.cust_nbr in {co_cust}
and co.mkt_lvl = {mkt_lvl}
and itm_osd.attr_grp_nm = {attr_grp_nm}
and osd.catman_attr_prr_nbr in (1,2,3,4,5)
group by 1,2,3,4,5,6,7,8,9,10,11