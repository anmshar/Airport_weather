WITH hourly_data AS (
    SELECT * 
    FROM {{ref('staging_weather_hourly')}}
),
add_features AS (
    SELECT *
		, timestamp::DATE AS date -- only time (hours:minutes:seconds) as TIME data type
		, timestamp::time AS time -- only time (hours:minutes:seconds) as TIME data type
        , TO_CHAR(timestamp,'HH24:MI') as hour -- time (hours:minutes) as TEXT data type
        , TO_CHAR(timestamp, 'FMmonth') AS month_name -- month name as a text
        , TO_CHAR(timestamp, 'FMmonth') AS weekday -- weekday name as text        
        , DATE_PART('day', timestamp) AS date_day
		, DATE_PART ('month', timestamp)AS date_month
		, DATE_PART('year',timestamp) AS date_year
		, TO_CHAR(timestamp, 'IYYY-IW') AS cw -- calander week
    FROM hourly_data
),
add_more_features AS (
    SELECT *
		,(CASE 
			WHEN time BETWEEN '00:00:00' AND '05:59:59' THEN 'night'
			WHEN time BETWEEN '06:00:00' and '16:59:59' THEN 'day'
			WHEN time BETWEEN '17:00:00' And '21:59:59' THEN 'evening'
		END) AS day_part
    FROM add_features
)

SELECT *
FROM add_more_features