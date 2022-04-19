select distinct
       osd.attr_grp_nm,
       osd.use_lot_nm,
       itm.itm_nbr as supc,
       osd.eq_uom_qty
from edwp.itm_osd_dim as osd
join edwp.itm_dim itm on osd.itm_skey = itm.itm_skey
where osd.use_lot_nm <> ''
    and osd.curr_rec_ind='Y'
    and supc in {supc}