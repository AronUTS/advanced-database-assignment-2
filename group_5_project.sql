-- ============================
-- Create Database For Project - Group 5
-- ============================
CREATE DATABASE IF NOT EXISTS GROUP_5;
USE DATABASE GROUP_5;

-- ============================
-- Create Bronze Table Statements
-- ============================

--Create schema - change this later
CREATE SCHEMA IF NOT EXISTS bronze;

-- Bronze: Sensor Temperature/Humidity
CREATE TABLE IF NOT EXISTS bronze.sensor_raw (
    raw_payload VARIANT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bronze: Power Consumption
CREATE TABLE IF NOT EXISTS bronze.power_raw (
    raw_payload VARIANT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bronze: Facility External Sensors
CREATE TABLE IF NOT EXISTS bronze.facility_raw (
    raw_payload VARIANT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- TRUNCATE TABLE IF EXISTS bronze.sensor_raw;
-- TRUNCATE TABLE IF EXISTS bronze.power_raw;
-- TRUNCATE TABLE IF EXISTS bronze.facility_raw;


-- ============================
-- Bronze UDF Generators
-- ============================

-- Sensor data generator
CREATE OR REPLACE FUNCTION generate_bronze_sensor_data(n INT DEFAULT 100)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'sensor_data_generator'
AS
$$
import random
from datetime import datetime, timedelta

def sensor_data_generator(n):
    data = []
    # Facility → Rack mapping
    facility_rack_map = {
        "F01": ["R001", "R002"],
        "F02": ["R003"],
        "F03": ["R004", "R005"]
    }

    sensor_ids = [f"S{i:03d}" for i in range(1, 11)]
    types = ["temperature", "humidity"]
    facilities = list(facility_rack_map.keys())

    for i in range(n):
        facility_id = random.choice(facilities)
        rack_id = random.choice(facility_rack_map[facility_id])
        sensor_id = sensor_ids[i % len(sensor_ids)]
        sensor_type = random.choice(types)

        # Synthetic values
        if sensor_type == "temperature":
            value = random.uniform(20, 35)
            if rack_id == "R003":
                value += 10
        else:
            value = random.uniform(30, 60)

        # Random timestamp within last 24 hours
        ts = datetime.utcnow() - timedelta(seconds=random.randint(0, 86400))

        payload = {
            "facility_id": facility_id,
            "rack_id": rack_id,
            "sensor_id": sensor_id,
            "type": sensor_type,
            "unit": "C" if sensor_type == "temperature" else "%",
            "value": round(value, 2),
            "timestamp": ts.isoformat()
        }
        data.append(payload)
    return data
$$;


-- Power data generator
CREATE OR REPLACE FUNCTION generate_bronze_power_data(n INT DEFAULT 100)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'power_data_generator'
AS
$$
import random
from datetime import datetime, timedelta

def power_data_generator(n):
    data = []
    facility_rack_map = {
        "F01": ["R001", "R002"],
        "F02": ["R003"],
        "F03": ["R004", "R005"]
    }

    for i in range(n):
        facility_id = random.choice(list(facility_rack_map.keys()))
        rack_id = random.choice(facility_rack_map[facility_id])
        power_kw = random.uniform(5, 25)
        voltage_v = 230
        current_a = round(power_kw * 1000 / voltage_v, 2)
        cooling_kw = random.uniform(2, 8)

        ts = datetime.utcnow() - timedelta(seconds=random.randint(0, 86400))

        payload = {
            "facility_id": facility_id,
            "rack_id": rack_id,
            "power_kw": round(power_kw, 2),
            "voltage_v": voltage_v,
            "current_a": current_a,
            "cooling_kw": round(cooling_kw, 2),
            "timestamp": ts.isoformat()
        }
        data.append(payload)
    return data
$$;


-- Facility data generator
CREATE OR REPLACE FUNCTION generate_bronze_facility_data(n INT DEFAULT 100)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'facility_data_generator'
AS
$$
import random
from datetime import datetime, timedelta

def facility_data_generator(n):
    data = []
    facility_ids = ["F01", "F02", "F03"]

    for i in range(n):
        facility_id = random.choice(facility_ids)
        external_temp_c = random.uniform(20, 45)
        external_humidity = random.uniform(30, 70)
        weather_condition = "Normal" if external_temp_c < 40 else "Heat Alert"
        power_status = random.choice(["Normal", "Partial Outage", "Full Outage"])

        ts = datetime.utcnow() - timedelta(seconds=random.randint(0, 86400))

        payload = {
            "facility_id": facility_id,
            "external_temp_c": round(external_temp_c, 2),
            "external_humidity": round(external_humidity, 2),
            "weather_condition": weather_condition,
            "power_status": power_status,
            "timestamp": ts.isoformat()
        }
        data.append(payload)
    return data
$$;


-- ============================
-- Populate Bronze Tables with Generated Data
-- ============================

-- Insert 20,000 individual sensor records
INSERT INTO bronze.sensor_raw (raw_payload)
SELECT f.value
FROM TABLE(FLATTEN(INPUT => generate_bronze_sensor_data(20000))) AS f;

-- Insert 20,000 individual power records
INSERT INTO bronze.power_raw (raw_payload)
SELECT f.value
FROM TABLE(FLATTEN(INPUT => generate_bronze_power_data(20000))) AS f;

-- Insert 20,000 individual facility records
INSERT INTO bronze.facility_raw (raw_payload)
SELECT f.value
FROM TABLE(FLATTEN(INPUT => generate_bronze_facility_data(20000))) AS f;

select * from bronze.facility_raw;

-- ============================
-- Example Bronze Layer Queries
-- ============================

-- Extract sensor data columns
SELECT
    raw_payload:"sensor_id"::STRING    AS sensor_id,
    raw_payload:"rack_id"::STRING      AS rack_id,
    raw_payload:"facility_id"::STRING  AS facility_id,
    raw_payload:"type"::STRING         AS type,
    raw_payload:"unit"::STRING         AS unit,
    raw_payload:"value"::FLOAT         AS value,
    timestamp
FROM bronze.sensor_raw;

-- Extract power data columns
SELECT
    raw_payload:"rack_id"::STRING      AS rack_id,
    raw_payload:"facility_id"::STRING  AS facility_id,
    raw_payload:"power_kw"::FLOAT      AS power_kw,
    raw_payload:"voltage_v"::FLOAT     AS voltage_v,
    raw_payload:"current_a"::FLOAT     AS current_a,
    raw_payload:"cooling_kw"::FLOAT    AS cooling_kw,
    timestamp
FROM bronze.power_raw;

-- Extract facility data columns
SELECT
    raw_payload:"facility_id"::STRING       AS facility_id,
    raw_payload:"external_temp_c"::FLOAT    AS external_temp_c,
    raw_payload:"external_humidity"::FLOAT  AS external_humidity,
    raw_payload:"weather_condition"::STRING AS weather_condition,
    raw_payload:"power_status"::STRING      AS power_status,
    timestamp
FROM bronze.facility_raw;


-- ==========================================
-- SILVER LAYER
-- ==========================================

-- ------------------------------
-- Fact Tables (Flattened from Bronze)
-- ------------------------------

CREATE OR REPLACE TABLE silver.sensor_readings AS
SELECT
    raw_payload:"sensor_id"::STRING  AS sensor_id,       -- Sensor providing reading
    raw_payload:"rack_id"::STRING    AS rack_id,         -- Rack associated with the sensor
    raw_payload:"type"::STRING       AS type,            -- Reading type (temperature/humidity)
    raw_payload:"unit"::STRING       AS unit,            -- Measurement unit
    raw_payload:"value"::FLOAT       AS value,           -- Recorded value
    timestamp                        AS timestamp        -- Event timestamp
FROM bronze.sensor_raw;

CREATE OR REPLACE TABLE silver.power_data AS
SELECT
    raw_payload:"rack_id"::STRING    AS rack_id,         -- Rack consuming power
    raw_payload:"power_kw"::FLOAT    AS power_kw,        -- Power usage (kW)
    raw_payload:"voltage_v"::FLOAT   AS voltage_v,       -- Voltage supplied (V)
    raw_payload:"current_a"::FLOAT   AS current_a,       -- Current drawn (A)
    raw_payload:"cooling_kw"::FLOAT  AS cooling_kw,      -- Cooling system load (kW)
    timestamp                        AS timestamp        -- Event timestamp
FROM bronze.power_raw;

CREATE OR REPLACE TABLE silver.facility_readings AS
SELECT
    raw_payload:"facility_id"::STRING        AS facility_id,       -- Facility monitored
    raw_payload:"external_temp_c"::FLOAT     AS external_temp_c,   -- Outside air temperature (°C)
    raw_payload:"external_humidity"::FLOAT   AS external_humidity, -- Outside air humidity (%)
    raw_payload:"weather_condition"::STRING  AS weather_condition, -- Weather descriptor
    raw_payload:"power_status"::STRING       AS power_status,      -- Power status
    timestamp                                AS timestamp          -- Event timestamp
FROM bronze.facility_raw;

-- ==========================================
-- GOLD LAYER
-- ==========================================

-- ------------------------------
-- Dimension Tables
-- ------------------------------

CREATE OR REPLACE TABLE gold.dim_datacenter (
    datacenter_id STRING PRIMARY KEY,     -- Unique identifier for each datacenter
    name STRING,                          -- Datacenter name
    location STRING                       -- Geographic region or city
);

CREATE OR REPLACE TABLE gold.dim_facility (
    facility_id STRING PRIMARY KEY,       -- Unique facility identifier
    datacenter_id STRING REFERENCES dim_datacenter(datacenter_id), -- Parent datacenter link
    name STRING,                          -- Facility name
    floors INT                            -- Number of floors in facility
);

CREATE OR REPLACE TABLE gold.dim_rack (
    rack_id STRING PRIMARY KEY,           -- Unique rack identifier
    facility_id STRING REFERENCES dim_facility(facility_id), -- Rack location in facility
    position STRING,                      -- Rack position or row/column indicator
    capacity_kw FLOAT                     -- Rack max supported power (kW)
);

CREATE OR REPLACE TABLE gold.dim_sensor (
    sensor_id STRING PRIMARY KEY,         -- Unique sensor identifier
    rack_id STRING REFERENCES dim_rack(rack_id), -- Associated rack
    type STRING,                          -- Type (temperature, humidity)
    unit STRING,                          -- Measurement unit (°C, %)
    calibration_date DATE                 -- Last calibration date
);

-- ------------------------------
-- Populate Dimension Tables
-- ------------------------------

-- Populate datacenters
INSERT INTO gold.dim_datacenter (datacenter_id, name, location)
VALUES 
('DC01', 'Datacenter 1', 'Sydney'),
('DC02', 'Datacenter 2', 'Melbourne');

-- Populate facilities (with datacenter link)
INSERT INTO gold.dim_facility (facility_id, datacenter_id, name, floors)
VALUES
('F01', 'DC01', 'Facility 1', 3),
('F02', 'DC01', 'Facility 2', 2),
('F03', 'DC02', 'Facility 3', 4);

-- Populate racks
INSERT INTO gold.dim_rack (rack_id, facility_id, position, capacity_kw)
VALUES
('R001', 'F01', 'A1', 10),
('R002', 'F01', 'A2', 12),
('R003', 'F02', 'B1', 15),
('R004', 'F03', 'C1', 10),
('R005', 'F03', 'C2', 12);

-- ------------------------------
-- Rack-level performance metrics
-- ------------------------------
CREATE OR REPLACE TABLE gold.rack_performance AS
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

-- ------------------------------
-- Facility-level aggregated summary
-- ------------------------------
CREATE OR REPLACE TABLE gold.facility_summary AS
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
LEFT JOIN silver_sensor_readings s 
       ON s.rack_id = r.rack_id 
      AND DATE_TRUNC('hour', p.timestamp) = DATE_TRUNC('hour', s.timestamp)
GROUP BY f.facility_id, DATE_TRUNC('hour', p.timestamp);


-- ------------------------------
-- Datacenter-level efficiency overview
-- ------------------------------
CREATE OR REPLACE TABLE gold.datacenter_efficiency AS
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
