WITH airports_reorder AS (
    SELECT faa,name,country,region,lat,lon,alt,tz
    from {{ref('staging_airports')}}
)
SELECT * FROM airports_reorder