WITH daily_data AS (
    SELECT * 
    FROM {ref{('staging_weather_daily')}}
),
extract_date AS (
    SELECT *,
        EXTRACT(DAY FROM date) AS date_day,
        EXTRACT(MONTH FROM date) AS date_month,
        EXTRACT(YEAR FROM date) AS date_year,
        EXTRACT(WEEK FROM date) AS cw,
        TRIM(TO_CHAR(date, 'Month')) AS month_name,
        TO_CHAR(date, 'Day') AS weekday
    FROM daily_data
),
extract_months AS (
    SELECT *,
        (CASE 
            WHEN month_name IN ('December', 'January', 'February') THEN 'winter'
            WHEN month_name IN ('March', 'April', 'May') THEN 'spring'
            WHEN month_name IN ('June', 'July', 'August') THEN 'summer'
            WHEN month_name IN ('September', 'October', 'November') THEN 'autumn'
        END) AS season
    FROM extract_date
)
SELECT *
FROM extract_months
ORDER BY date;