select
    osd.itm_skey
    ,osd.attr_grp_nm
    ,osd.catman_attr_prr_nbr
    ,osd.osd_attr_nm
    ,osd.osd_decided_val
from edwp.itm_osd_attr_dim as osd
where itm_skey in {itm_skey} and
      osd.curr_rec_ind = 'Y' and
      osd.catman_attr_prr_nbr in ('1','2','3','4','5')