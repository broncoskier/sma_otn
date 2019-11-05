select
	vendor,
	wavelength_bw,
	fiber_type,
	reach_char
from modulation_reach  c
where vendor like 'ciena%' and fiber_type = 'ull' and reach_char = 'best'
;

with clients as (
SELECT  
	10 AS rate,
	slot_bw,
	clients,
	c.vendor,
	c.wavelength_bw as ic_wl_bw,
	case when slot_bw - c.wavelength_bw in (
		select 
			wavelength_bw		
		from modulation_reach mr where mr.vendor like 'ciena_ai' and fiber_type = 'ull' and reach_char = 'best') 
	then slot_bw - c.wavelength_bw else 0 end as ic_wl_bw_2
FROM generate_series(1, 500) AS clients
join trans.otn_param on technology = 'wlai' and purpose = 'intercity' 
join (
select 
	vendor,
	wavelength_bw,
	fiber_type,
	reach_char
from modulation_reach c ) c on c.vendor like 'ciena_ai' and fiber_type = 'ull' and reach_char = 'best'
)
--select * from clients;
, ic as (
select 
	c.*,	
	ceiling ( rate * clients / (ic_wl_bw + ic_wl_bw_2)) as ic_card,
	ceiling ( rate * clients / ic_wl_bw) as ic_optic
	
--	case when (rate * clients / ic_wl_bw is <=1 then )
from clients c
)
select * from ic order by 5,3;