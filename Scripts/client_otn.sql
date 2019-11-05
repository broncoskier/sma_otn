SELECT  
	10 AS rate,
	slot_bw,
	clients
	FROM generate_series(1, 1200) AS clients
join trans.otn_param on vendor = 'ciena_ai' 
and technology != 'metro'
;

drop view otn_costs cascade;

--create or replace view otn_costs as
select
	clients,
	vendor,
	technology,
	rate,
	wl_bw,
	metro_cards,
	metro_card_cost,
	client_cost,
	lines,
	line_cost,
	ic_cards,
	ic_cards_cost,
	dist_lic,
--	a.slot_bw,
	shelves,
--	lag(shelves) over (partition by vendor, rate, wl_bw, shelves order by vendor) as lag_test,
--	lead(shelves) over (partition by vendor, rate, wl_bw, shelves order by vendor) as lead_test,
--	lag(shelves) over (partition by vendor, rate, wl_bw, shelves order by shelves) as lag_shelves_test,
--	lead(shelves) over (partition by vendor, rate, wl_bw, shelves order by shelves) as lead_shelves_test,
	shelves_cost,
	sum (metro_card_cost + client_cost + line_cost + ic_cards_cost + shelves_cost + dist_lic) as otn_total_cost,
	round (sum (metro_card_cost  + client_cost + line_cost + ic_cards_cost + shelves_cost + dist_lic) / clients ,2) as otn_per_10G_cost
from (
	select
		clients,
		a.vendor,
		a.technology,
		a.rate,
		wl_bw,
		metro_cards,
		metro_card_cost,
		client_cost,
		lines,
		line_cost,
--		a.slot_bw,
		ic_cards,
		ic_cards * g.card_cost as ic_cards_cost,
		round (clients * (sum(h.cost) / (a.slot_bw / a.rate)) , 2) as dist_lic,
		shelves,
		shelves_cost
	from (
	select
		clients,
		a.vendor,
		technology,
		a.rate,
		wl_bw,
		metro_cards,
		metro_card_cost,
		coalesce(client_cost,0) as client_cost,
		lines,
		slot_bw,
		line_cost,
		ic_cards,
		shelves,
		shelves * sum(e.cost * coalesce(e.qty,1)) as shelves_cost
	from
	(
		select
			clients,
			a.vendor,
			a.technology,
			a.rate,
			wl_bw,
			a.slot_bw,
			metro_cards,
			metro_cards * c.card_cost as metro_card_cost,
			clients * j.card_cost as client_cost,
			lines,
			coalesce(lines * d.cost, 0) as line_cost,
			ic_cards,
			ceiling ((metro_cards + ic_cards) / b.slot_per_shelf) as shelves
		from (
			select
				clients,
				a.vendor,
				a.technology,
				a.rate,
				a.wl_bw,
				a.slot_bw,
				line_per_card,
				sfp_per_card,
				clients_per_sfp,
				case
				when a.technology = 'wl3n' 
				then ceiling (clients /(( 100 / a.rate) * line_per_card))
				when a.technology = 'wlai' 
				then ceiling (clients / (a.slot_bw / a.rate))
				else ceiling (clients / (sfp_per_card * clients_per_sfp))
				end
				as metro_cards,
				ceiling (clients / (a.wl_bw / a.rate)) as lines,
				ceiling (clients / (a.slot_bw / a.rate))
				as ic_cards
			from (
					select 
						clients,
						b.vendor,
						b.technology,
						a.rate,
						coalesce(wavelength_bw,100) as wl_bw,
						a.slot_bw,
						line_per_card,
						sfp_per_card,
						clients_per_sfp
					from
					(
						SELECT  
							10 AS rate,
							slot_bw,
							clients 
						FROM generate_series(1, 100) AS clients
							join trans.otn_param on technology = 'wlai' and purpose = 'intercity'
					) a
					join (
						select 
							*
						from otn_param ) b
						on 
						a.rate = b.rate
						and 
						purpose != 'intercity'
					left join (
						select
							vendor,
							wavelength_bw,
							fiber_type,
							reach_char
						from modulation_reach ) c
					on right(c.vendor,2) = right(b.technology,2) and fiber_type = 'ull' and reach_char = 'best'
				) a			
			)a
		join (
				select 
					*
				from otn_param ) b
				on 
				a.rate = b.rate
				and 
				purpose = 'intercity'
		join (
		select
			vendor,
			technology,
			rate,
			card_cost,
			part_type
--			slot_bw
		from otn_rolled_costs 
		)c
		on c.technology = a.technology 
--		and c.slot_bw = a.slot_bw 
		and c.part_type = 'card'
		left join (
		select
			vendor,
			technology,
			rate,
			card_cost,
			part_type
--			slot_bw
		from otn_rolled_costs 
		)j
		on j.technology = a.technology 
--		and c.slot_bw = a.slot_bw 
		and j.part_type = 'client'
		left join (
		select
			vendor,
			technology,
			rate,
			cost,
			part_type
		from otn_costs_all )d
		on a.technology = d.technology and d.part_type = 'line'
		group by
			clients,
			a.vendor,
			a.technology,
			a.rate,
			wl_bw,
			metro_cards,
			c.card_cost,
			j.card_cost,
			a.slot_bw,
			lines,
			ic_cards,
			d.cost,
			slot_per_shelf
	) a
	left join (
		select
			vendor,
			rate,
			cost,
			part_type,
			qty
		from otn_costs_all )e
		on e.part_type = 'shelf'
	group by
		clients,
		a.vendor,
		technology,
		a.rate,
		wl_bw,
		metro_cards,
		client_cost,
		slot_bw,
		metro_card_cost,
		lines,
		line_cost,
		ic_cards,
		shelves	
	) a
	join (
		select
			vendor,
			technology,
			rate,
			card_cost,
			part_type
		from otn_rolled_costs 
		)g
		on g.technology = 'wlai' and g.part_type = 'card'
	join (
		select
			vendor,
			technology,
			rate,
			cost,
			part_type,
			qty
		from otn_costs_all )h
		on h.technology = 'wlai' and h.part_type = 'dist_lic'
	join (
		select 
			*
		from otn_param ) b
		on 
		a.rate = b.rate
		and 
		purpose = 'intercity'	
	group by
		clients,
		a.vendor,
		a.technology,
		a.rate,
		wl_bw,
		metro_cards,
		metro_card_cost,
		client_cost,
		card_cost,
		lines,
		line_cost,
		ic_cards,
		shelves,
		a.slot_bw,
		shelves_cost
) a
group by
	clients,
	vendor,
	technology,
	rate,
	wl_bw,
	metro_cards,
	metro_card_cost,
	client_cost,
	lines,
	line_cost,
	ic_cards,
	ic_cards_cost,
	dist_lic,
--	a.slot_bw,
	shelves,
	shelves_cost
order by 
--slot_bw,
technology,
wl_bw,
clients
;

--create or replace view otn_best_costs as
select distinct
	x.clients,
	x.vendor,
	x.technology,
	x.otn_total_cost,
	x.otn_per_10G_cost
from otn_costs x
where 
	x.clients = 1
or 
	otn_per_10g_cost = (
		select min (otn_per_10g_cost)
		from otn_costs z
		where 
			z.shelves = 1 and (technology = 'wl_ai' or technology = 'drop')
	)
order by 
	x.vendor
;

select distinct
	a.clients,
	a.rate,
	x.vendor as otn_vendor,
	x.technology as otn_technology, 
--	z.otn_total_cost,
	x.otn_per_10g_cost
from (
	SELECT  
		10 AS rate,
	--	vendor,
	--	otn_per_10g_cost,
		clients 
	FROM generate_series(1, 100) AS clients
	) a
join otn_costs x
on x.clients = 600
join (
	select 
		clients,
		vendor,
		otn_total_cost
	from otn_costs) z 
	on z.clients = a.clients 
	and z.vendor = z.vendor
order by
	rate,
	x.vendor,
	a.clients
;

