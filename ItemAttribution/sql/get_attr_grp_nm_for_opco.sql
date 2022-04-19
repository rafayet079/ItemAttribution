select distinct itm_osd.attr_grp_nm
from edwp.sale_oblig_dtl_fact as oblg
inner join edwp.cust_ship_to_dim as cust
on cust.cust_skey = oblg.cust_skey
inner join edwp.itm_osd_dim as itm_osd
on oblg.itm_skey = itm_osd.itm_skey
left join edwp.org_co_dim as co
on co.co_skey = cust.co_skey
where oblg.oblig_dt > CURRENT_DATE - 30
and oblg.net_prc_ext_amt > 0
and cust.curr_rec_ind = 'Y'
and co.co_nbr in {opco}
