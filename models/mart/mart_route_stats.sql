with rout as(
select distinct tail_number,origin,
round(avg(dep_delay),2) as avrage_dep_delay,
round(avg(actual_elapsed_time),2)as average_elapsed ,
round(avg(arr_delay),2)as average_arr_delay,
sum(cancelled)as canscelled_flights ,
sum(diverted) as no_diverted_flights,
airline,
dest,
count(*) as total_numbers_of_flight
from {{ref('prep_flights')}}
group by tail_number ,airline ,dest ,origin)
select r.*,
pa.name,
pa.country,
r.dest
from rout r  
join {{ref('prep_airports')}} pa on r.origin = pa.faa