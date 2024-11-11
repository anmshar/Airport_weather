WITH daily_data AS (
    SELECT * 
    FROM {{ref('staging_weather_daily')}}
),
add_features AS (
    SELECT *,
        EXTRACT(DAY FROM date) AS date_day,
        EXTRACT(MONTH FROM date) AS date_month,
        EXTRACT(YEAR FROM date) AS date_year,
        EXTRACT(WEEK FROM date) AS cw,
        TO_CHAR(date, 'Month') AS month_name,
        TO_CHAR(date, 'Day') AS weekday
    FROM daily_data
),
add_more_features AS (
    SELECT *,
        (CASE 
            WHEN month_name IN ('December', 'January', 'February') THEN 'winter'
            WHEN month_name IN ('March', 'April', 'May') THEN 'spring'
            WHEN month_name IN ('June', 'July', 'August') THEN 'summer'
            WHEN month_name IN ('September', 'October', 'November') THEN 'autumn'
        END) AS season
    FROM add_features
)
SELECT *
FROM add_more_features
ORDER BY date;