create database baywa;
use baywa;

-- BI/ML Use Case Specific

-- 1. Trend of daily public net electricity production in Germany for each production type

SELECT
    DATE(timestamp) AS date,
    production_type_name,
    SUM(production_value) AS total_production
FROM
    public_power_cleaned
GROUP BY
    DATE(timestamp),
    production_type_name
ORDER BY
    date, production_type_name;
    
    
-- 2.Analysis of Daily Average Price Against Net Power for Offshore and Onshore Wind

WITH common_dates AS (
    SELECT 
        DISTINCT DATE(timestamp) AS date
    FROM 
        public_power_cleaned
    INTERSECT
    SELECT 
        DISTINCT DATE(timestamp) AS date
    FROM 
        price_data_cleaned
),

daily_net_power AS (
    SELECT 
        DATE(timestamp) AS date,
        production_type_name,
        SUM(production_value) AS total_net_power
    FROM 
        public_power_cleaned
    WHERE 
        DATE(timestamp) IN (SELECT date FROM common_dates)
    AND 
        production_type_name IN ('Wind offshore', 'Wind onshore')  -- Filter for relevant types
    GROUP BY 
        DATE(timestamp), production_type_name
),

daily_price AS (
    SELECT 
        DATE(timestamp) AS date,
        AVG(price) AS average_price
    FROM 
        price_data_cleaned
    WHERE 
        DATE(timestamp) IN (SELECT date FROM common_dates)
    GROUP BY 
        DATE(timestamp)
)

SELECT 
    dp.date,
    dp.average_price,
    dnp.production_type_name,
    COALESCE(dnp.total_net_power, 0) AS total_net_power
FROM 
    daily_price dp
LEFT JOIN 
    daily_net_power dnp ON dp.date = dnp.date
ORDER BY 
    dp.date, dnp.production_type_name;




