select distinct co.co_nbr||cust.cust_nbr as co_cust
            , ogr.cuisine_lvl_1_desc
            , ogr.cuisine_lvl_2_desc

from edwp.oper_clstr_dim as ogr
--inner join because the scope for this query is USBL customers
JOIN edwp.oper_atom_oper_clstr_rel as usbl on ogr.oper_clstr_skey=usbl.oper_clstr_skey
   AND usbl.src_src_cd='USBL' --all USBL customers will be returned, may duplicate operator cluster ids
   AND usbl.src_del_ind='N'
   AND usbl.curr_rec_ind='Y'
-- to limit customers returned, join in the customer and sales oblig tables
JOIN edwp.org_co_dim AS co ON co.co_nbr = left(usbl.oper_atom_nkey, 3)
    AND co.bus_unit_nm='USBL'
    AND co.mkt_lvl<>''
    AND co.sts_ind='A'
JOIN edwp.cust_ship_to_dim AS cust on cust.cust_nbr=right(usbl.oper_atom_nkey, 6)
   AND cust.co_skey=co.co_skey
    AND cust.curr_rec_ind='Y'
--     AND cust.acct_typ_cd in ('TRS')  -- can match any account type
JOIN edwp.sale_oblig_dtl_fact AS oblig on oblig.cust_skey=cust.cust_skey
   and oblig.trd_sale_ind = 'T'
   AND oblig.oblig_dt >= DATEADD(year, -1, CURRENT_DATE)
WHERE ogr.curr_rec_ind='Y'
    AND ogr.src_del_ind='N'
and co.co_nbr in {opco}
and ogr.cuisine_lvl_2_desc = ''
