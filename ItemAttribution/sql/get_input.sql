select distinct
        co.co_nbr,
        co.co_nbr||cust.cust_nbr as co_cust_nbr,
        itm_osd.attr_grp_nm,
        oblg.oblig_dt as dt
from edwp.sale_oblig_dtl_fact as oblg
inner join edwp.cust_ship_to_dim as cust
on cust.cust_skey = oblg.cust_skey
inner join edwp.itm_osd_dim as itm_osd
on oblg.itm_skey = itm_osd.itm_skey
left join edwp.org_co_dim as co
on co.co_skey = cust.co_skey
where oblg.oblig_dt > current_date - 90
and oblg.net_prc_ext_amt > 0
and cust.curr_rec_ind = 'Y'
and itm_osd.bus_ctr_nm <> 'ADMINISTRATIVE'
AND cust.src_sys_cd NOT IN ('OLDBT', 'ERR', 'BT')
limit 100
