CREATE DATABASE foodtech;
USE foodtech;
CREATE TABLE foodtech.delivery_data (
    Order_ID Int64,
    Distance_km Float64,
    Weather String,
    Traffic_Level String,
    Time_of_Day String,
    Vehicle_Type String,
    Preparation_Time_min Int64,
    Courier_Experience_yrs Int64,
    Delivery_Time_min Int64
) ENGINE = MergeTree()
ORDER BY Order_ID;
ALTER TABLE delivery_data 
MODIFY COLUMN Courier_Experience_yrs Int64;
DESCRIBE TABLE delivery_data;

SELECT * FROM delivery_data LIMIT 10;
SELECT COUNT(*) FROM delivery_data;

SELECT COUNT(*) FROM delivery_data WHERE Distance_km IS NULL;
SELECT COUNT(*) FROM delivery_data WHERE Delivery_Time_min <= 0;
SELECT COUNT(*) FROM delivery_data WHERE Order_ID IS NULL;
SELECT COUNT(*) FROM delivery_data WHERE Preparation_Time_min <= 0;
SELECT COUNT(*) FROM delivery_data WHERE Courier_Experience_yrs IS NULL;
# checking for missing value
SELECT Order_ID, COUNT(*) 
FROM delivery_data 
GROUP BY Order_ID 
HAVING COUNT(*) > 1;

SELECT COUNT(*) FROM delivery_data WHERE Delivery_Time_min < 0;
SELECT COUNT(*) FROM delivery_data WHERE Delivery_Time_min > 500;  -- Adjust as needed

SELECT 
    SUM(Weather = '') AS empty_weather,
    SUM(Traffic_Level = '') AS empty_traffic,
    SUM(Time_of_Day = '') AS empty_time_of_day,
    SUM(Vehicle_Type = '') AS empty_vehicle_type,
    SUM(Preparation_Time_min = 0) AS empty_prep_time
FROM delivery_data;

SELECT * FROM delivery_data LIMIT 100;

SELECT 
    MIN(Delivery_Time_min) AS min_time, 
    MAX(Delivery_Time_min) AS max_time, 
    AVG(Delivery_Time_min) AS avg_time, 
    median(Delivery_Time_min) AS median_time
FROM delivery_data;

SELECT * FROM delivery_data ORDER BY Delivery_Time_min ASC LIMIT 5;

SELECT Weather, AVG(Delivery_Time_min) 
FROM delivery_data 
GROUP BY Weather 
ORDER BY AVG(Delivery_Time_min) DESC;

ALTER TABLE delivery_data ADD COLUMN Delivery_Speeds Float32 MATERIALIZED (Distance_km / NULLIF(Delivery_Time_min, 1));

SELECT 
    Traffic_Level,
    AVG(Distance_km / NULLIF(Delivery_Time_min, 1)) AS avg_speed
FROM delivery_data
GROUP BY Traffic_Level
ORDER BY Traffic_Level;

SELECT 
    Vehicle_Type, 
    AVG(Delivery_Time_min) AS Avg_Delivery_Time, 
    AVG(Delivery_Speeds) AS Avg_Speed
FROM delivery_data
GROUP BY Vehicle_Type;

SELECT Traffic_Level, AVG(Delivery_Time_min) 
FROM delivery_data 
GROUP BY Traffic_Level 
ORDER BY Traffic_Level;

SELECT Time_of_Day, AVG(Delivery_Time_min) 
FROM delivery_data 
GROUP BY Time_of_Day 
ORDER BY AVG(Delivery_Time_min) DESC;

SELECT Distance_km, AVG(Delivery_Time_min) AS avg_delivery_time 
FROM delivery_data 
GROUP BY Distance_km 
ORDER BY Distance_km;

# courier_effeciency
SELECT Courier_Experience_yrs, COUNT(Order_ID) AS num_orders 
FROM delivery_data 
GROUP BY Courier_Experience_yrs 
ORDER BY num_orders DESC;


SELECT 
    (SUM(CASE WHEN Delivery_Time_min > 30 THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) AS late_deliveries 
FROM delivery_data;

SELECT 
    Weather,
    AVG((Delivery_Time_min * 100.0) / (Preparation_Time_min + Delivery_Time_min)) AS Avg_CTE
FROM delivery_data
GROUP BY Weather;

SELECT 
    Traffic_Level,
    AVG((Delivery_Time_min * 100.0) / (Preparation_Time_min + Delivery_Time_min)) AS Avg_CTE
FROM delivery_data
GROUP BY Traffic_Level;

SELECT 
    Order_ID,
    Distance_km,
    Traffic_Level,
    Weather,
    Delivery_Time_min,
    CASE 
        WHEN Delivery_Time_min > (SELECT AVG(Delivery_Time_min) FROM delivery_data) 
        THEN 'Late'
        ELSE 'On-Time'
    END AS Delivery_Status
FROM delivery_data
LIMIT 10;


SELECT 'Traffic Level' AS Metric, Traffic_Level AS Category, AVG(Delivery_Time_min) AS Avg_Delivery_Time
FROM delivery_data 
GROUP BY Traffic_Level; 
