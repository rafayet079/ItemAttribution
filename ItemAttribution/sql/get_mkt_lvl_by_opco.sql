select distinct
        co.mkt_lvl,
        co.co_nbr,
        co.rgn_nm
from  edwp.org_co_dim as co
where co.mkt_lvl <> ''
