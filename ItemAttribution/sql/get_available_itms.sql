select distinct itm.itm_skey as itm_skey
, itm.itm_nbr as itm_nbr
, itm.itm_desc as itm_desc
, co.co_nbr
, itm_osd.catman_attr_prr_nbr
, itm_osd.osd_decided_val
from edwp.sale_oblig_dtl_fact oblg
join edwp.itm_dim itm
    on oblg.itm_skey = itm.itm_skey
join edwp.itm_co_itm_rel as itm_rel
    on itm_rel.itm_skey = oblg.itm_skey
join edwp.org_co_dim as co
    on co.co_skey = itm_rel.co_skey
join edwp.itm_osd_attr_dim as itm_osd
    on itm_osd.itm_skey = itm_rel.itm_skey
WHERE oblg.oblig_dt > current_date - 366
AND oblg.net_prc_ext_amt > 0
AND oblg.trd_sale_ind = 'T'
and itm.curr_rec_ind = 'Y'
and itm_osd.attr_grp_nm = {attr_grp_nm}
and co.co_skey in {opco}
and itm_osd.catman_attr_prr_nbr in ('1','2','3','4','5');
