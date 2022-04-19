select distinct oblg.itm_skey
      ,co.co_nbr||cust.cust_nbr as co_cust_nbr
      , oblg.oblig_dt
     , itm.itm_nbr
    , itm.itm_desc
    , itm_osd.attr_grp_nm
    , sum(oblg.case_sold_qty)
from edwp.sale_oblig_dtl_fact as oblg
inner join edwp.cust_ship_to_dim as cust
on cust.cust_skey = oblg.cust_skey
inner join edwp.itm_osd_dim as itm_osd
on oblg.itm_skey = itm_osd.itm_skey
inner join edwp.itm_dim as itm
on oblg.itm_skey = itm.itm_skey
left join edwp.org_co_dim as co
on co.co_skey = cust.co_skey
where ((oblg.oblig_dt > {start_date} - 30) or (oblg.oblig_dt > {start_date} - 365 and oblg.oblig_dt < {start_date} -335))
-- where oblg.oblig_dt > {start_date} - 45
and oblg.net_prc_ext_amt > 0
and cust.curr_rec_ind = 'Y'
and co.co_nbr||cust.cust_nbr in {co_cust}
and itm_osd.attr_grp_nm = {attr_grp_nm}
group by 1,2,3,4,5,6