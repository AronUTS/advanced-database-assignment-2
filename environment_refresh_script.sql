-- ============================
-- Pipeline Environment Refresh Script
-- ============================
 
-- Truncate bronze layer tables
TRUNCATE TABLE IF EXISTS bronze.sensor_raw;
TRUNCATE TABLE IF EXISTS bronze.power_raw;
TRUNCATE TABLE IF EXISTS bronze.facility_raw;
 
-- Drop all dynamic tables in silver, gold layers
SELECT 'DROP TABLE IF EXISTS SILVER."' || table_name || '";'
FROM information_schema.tables
WHERE table_schema = 'SILVER'
  AND table_type = 'BASE TABLE'
  AND table_name NOT LIKE 'DIM_%';
 
SELECT 'DROP TABLE IF EXISTS GOLD."' || table_name || '";'
FROM information_schema.tables
WHERE table_schema = 'GOLD'
  AND table_type = 'BASE TABLE'
  AND table_name NOT LIKE 'DIM_%';

-- Redeploy silver, gold layers
-- Sensor Readings (Flattened from Bronze)
CREATE OR REPLACE DYNAMIC TABLE silver.sensor_readings
  TARGET_LAG = '60 seconds'        -- Update the table every 10 seconds
  WAREHOUSE = HIPPO_WH                 -- Warehouse used for refreshing
  REFRESH_MODE = auto               -- Auto-refresh whenever underlying data changes
  INITIALIZE = on_create            -- Populate table immediately upon creation
AS
SELECT
    raw_payload:"sensor_id"::STRING   AS sensor_id,  -- Sensor providing reading
    raw_payload:"rack_id"::STRING     AS rack_id,    -- Rack associated with the sensor
    raw_payload:"type"::STRING        AS type,       -- Reading type (temperature/humidity)
    raw_payload:"unit"::STRING        AS unit,       -- Measurement unit
    raw_payload:"value"::FLOAT        AS value,      -- Recorded value
    TO_TIMESTAMP_LTZ(raw_payload:"timestamp_ms"::BIGINT / 1000) AS timestamp  -- Event timestamp
FROM bronze.sensor_raw;

-- Power Data (Flattened from Bronze)
CREATE OR REPLACE DYNAMIC TABLE silver.power_data
  TARGET_LAG = '60 seconds'        -- Update every 10 seconds
  WAREHOUSE = HIPPO_WH
  REFRESH_MODE = auto
  INITIALIZE = on_create
AS
SELECT
    raw_payload:"rack_id"::STRING     AS rack_id,       -- Rack consuming power
    raw_payload:"power_kw"::FLOAT     AS power_kw,      -- Power usage (kW)
    raw_payload:"voltage_v"::FLOAT    AS voltage_v,     -- Voltage supplied (V)
    raw_payload:"current_a"::FLOAT    AS current_a,     -- Current drawn (A)
    raw_payload:"cooling_kw"::FLOAT   AS cooling_kw,    -- Cooling system load (kW)
    TO_TIMESTAMP_LTZ(raw_payload:"timestamp_ms"::BIGINT / 1000) AS timestamp  -- Event timestamp
FROM bronze.power_raw;

-- Facility Readings (Flattened from Bronze)
CREATE OR REPLACE DYNAMIC TABLE silver.facility_readings
  TARGET_LAG = '60 seconds'        -- Update every 10 seconds
  WAREHOUSE = HIPPO_WH
  REFRESH_MODE = auto
  INITIALIZE = on_create
AS
SELECT
    raw_payload:"facility_id"::STRING        AS facility_id,        -- Facility monitored
    raw_payload:"external_temp_c"::FLOAT     AS external_temp_c,    -- Outside air temperature (°C)
    raw_payload:"external_humidity"::FLOAT   AS external_humidity,  -- Outside air humidity (%)
    raw_payload:"weather_condition"::STRING  AS weather_condition,  -- Weather descriptor
    raw_payload:"power_status"::STRING       AS power_status,       -- Power status
    TO_TIMESTAMP_LTZ(raw_payload:"timestamp_ms"::BIGINT / 1000) AS timestamp  -- Event timestamp
FROM bronze.facility_raw;


-- Rack-level performance metrics
CREATE OR REPLACE DYNAMIC TABLE gold.rack_performance
  TARGET_LAG = '60 seconds'       -- Update the table every 60 seconds
  WAREHOUSE = HIPPO_WH
  REFRESH_MODE = auto
  INITIALIZE = on_create
AS
SELECT
    r.facility_id,                                     -- Facility containing the rack
    s.rack_id,                                         -- Rack being measured
    DATE_TRUNC('hour', s.timestamp) AS time_window,    -- Hourly aggregation window
    AVG(CASE WHEN s.type = 'temperature' THEN s.value END) AS avg_temp_c,  -- Avg rack temp
    AVG(CASE WHEN s.type = 'humidity' THEN s.value END) AS avg_humidity,   -- Avg rack humidity
    AVG(p.power_kw) AS avg_power_kw,                   -- Avg power usage (kW)
    AVG(p.cooling_kw) AS avg_cooling_kw,               -- Avg cooling load (kW)
    (SUM(p.power_kw) + SUM(p.cooling_kw)) / NULLIF(SUM(p.power_kw),0) AS pue,  -- Power Usage Effectiveness
    (SUM(p.power_kw) - SUM(p.cooling_kw)) / NULLIF(SUM(p.power_kw),0) AS efficiency  -- IT load efficiency
FROM silver.sensor_readings s
JOIN silver.power_data p 
  ON s.rack_id = p.rack_id 
 AND DATE_TRUNC('hour', s.timestamp) = DATE_TRUNC('hour', p.timestamp)
JOIN gold.dim_rack r 
  ON s.rack_id = r.rack_id
GROUP BY r.facility_id, s.rack_id, DATE_TRUNC('hour', s.timestamp);

-- Facility-level aggregated summary
CREATE OR REPLACE DYNAMIC TABLE gold.facility_summary
  TARGET_LAG = '60 seconds'       -- Update the table every 60 seconds
  WAREHOUSE = HIPPO_WH
  REFRESH_MODE = auto
  INITIALIZE = on_create
AS
SELECT
    f.facility_id,                                    -- Facility being monitored
    DATE_TRUNC('hour', p.timestamp) AS time_window,   -- Hourly aggregation window
    SUM(p.power_kw) AS total_power_kw,                -- Total power consumption
    AVG(CASE WHEN s.type = 'temperature' THEN s.value END) AS avg_temp_c, -- Avg temperature
    COUNT(DISTINCT p.rack_id) AS racks_active,        -- Number of active racks
    (SUM(p.power_kw) + SUM(p.cooling_kw)) / NULLIF(SUM(p.power_kw),0) AS pue  -- Facility-level PUE
FROM silver.power_data p
JOIN gold.dim_rack r ON p.rack_id = r.rack_id
JOIN gold.dim_facility f ON r.facility_id = f.facility_id
LEFT JOIN silver.sensor_readings s 
       ON s.rack_id = r.rack_id 
      AND DATE_TRUNC('hour', p.timestamp) = DATE_TRUNC('hour', s.timestamp)
GROUP BY f.facility_id, DATE_TRUNC('hour', p.timestamp);

-- Datacenter-level efficiency overview
CREATE OR REPLACE DYNAMIC TABLE gold.datacenter_efficiency
  TARGET_LAG = '60 seconds'       -- Update the table every 60 seconds
  WAREHOUSE = HIPPO_WH
  REFRESH_MODE = auto
  INITIALIZE = on_create
AS
SELECT
    f.datacenter_id,                                 -- Datacenter identifier
    DATE_TRUNC('day', p.timestamp) AS time_window,   -- Daily aggregation window
    SUM(p.power_kw) AS total_power_kw,               -- Total power usage
    SUM(p.power_kw) - SUM(p.cooling_kw) AS it_load_kw, -- IT (useful) load
    SUM(p.cooling_kw) AS cooling_kw,                 -- Cooling load
    (SUM(p.power_kw) + SUM(p.cooling_kw)) / NULLIF(SUM(p.power_kw),0) AS pue,  -- Datacenter PUE
    (SUM(p.power_kw) - SUM(p.cooling_kw)) / NULLIF(SUM(p.power_kw),0) AS efficiency  -- Overall efficiency
FROM silver.power_data p
JOIN gold.dim_rack r ON p.rack_id = r.rack_id
JOIN gold.dim_facility f ON r.facility_id = f.facility_id
GROUP BY f.datacenter_id, DATE_TRUNC('day', p.timestamp);

-- Insert new sample data
-- Insert 80,000 individual sensor records
INSERT INTO bronze.sensor_raw (raw_payload)
SELECT f.value
FROM TABLE(FLATTEN(INPUT => bronze.generate_bronze_sensor_data(80000))) AS f;

-- Insert 80,000 individual power records
INSERT INTO bronze.power_raw (raw_payload)
SELECT f.value
FROM TABLE(FLATTEN(INPUT => bronze.generate_bronze_power_data(80000))) AS f;

-- Insert 80,000 individual facility records
INSERT INTO bronze.facility_raw (raw_payload)
SELECT f.value
FROM TABLE(FLATTEN(INPUT => bronze.generate_bronze_facility_data(80000))) AS f;
