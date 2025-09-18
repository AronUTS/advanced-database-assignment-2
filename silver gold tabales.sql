-- Silver Layer
-- Dimension Tables

CREATE TABLE dim_datacenter (
    datacenter_id STRING PRIMARY KEY,     -- Unique identifier for each datacenter
    name STRING,                          -- Human-readable datacenter name
    location STRING                       -- Geographical location (city, region)
);

CREATE TABLE dim_facility (
    facility_id STRING PRIMARY KEY,       -- Unique identifier for a facility (building)
    datacenter_id STRING REFERENCES dim_datacenter(datacenter_id), -- Parent datacenter
    name STRING,                          -- Facility/building name
    floors INT                            -- Number of floors in the facility
);

CREATE TABLE dim_rack (
    rack_id STRING PRIMARY KEY,           -- Unique identifier for a server rack
    facility_id STRING REFERENCES dim_facility(facility_id), -- Rack’s location
    position STRING,                      -- Physical position (row/column in facility)
    capacity_kw FLOAT                     -- Maximum supported power capacity (kW)
);

CREATE TABLE dim_sensor (
    sensor_id STRING PRIMARY KEY,         -- Unique identifier for each sensor
    rack_id STRING REFERENCES dim_rack(rack_id), -- Sensor’s associated rack
    type STRING,                          -- Sensor type (temperature, humidity, airflow, etc.)
    unit STRING,                          -- Measurement unit (°C, %, m/s, etc.)
    calibration_date DATE                 -- Last calibration date to ensure accuracy
);

-- Fact Tables (Time-series measurements)

CREATE TABLE silver_sensor_readings (
    sensor_id STRING REFERENCES dim_sensor(sensor_id), -- Sensor providing reading
    rack_id STRING REFERENCES dim_rack(rack_id),       -- Associated rack
    type STRING,                                       -- Type of reading (temperature, humidity)
    unit STRING,                                       -- Measurement unit
    timestamp TIMESTAMP,                               -- Time of measurement
    value FLOAT                                        -- Recorded sensor value
);

CREATE TABLE silver_power_data (
    rack_id STRING REFERENCES dim_rack(rack_id),       -- Rack consuming power
    timestamp TIMESTAMP,                               -- Time of measurement
    power_kw FLOAT,                                    -- Active power consumption (kW)
    voltage_v FLOAT,                                   -- Voltage supplied (V)
    current_a FLOAT,                                   -- Current drawn (A)
    cooling_kw FLOAT                                   -- Cooling system load (kW)
);

CREATE TABLE silver_facility_readings (
    facility_id STRING REFERENCES dim_facility(facility_id), -- Facility being monitored
    timestamp TIMESTAMP,                               -- Time of reading
    external_temp_c FLOAT,                             -- Outside air temperature (°C)
    external_humidity FLOAT,                           -- Outside air humidity (%)
    weather_condition STRING,                          -- Weather description (sunny, rainy, etc.)
    power_status STRING                                -- Facility power status (online/offline)
);



-- Gold Layer

-- Gold: Rack-level performance metrics
CREATE TABLE gold_rack_performance AS
SELECT
    r.facility_id,                                    -- Facility containing the rack
    s.rack_id,                                        -- Rack being measured
    DATE_TRUNC('hour', s.timestamp) AS time_window,   -- Aggregated by hour
    AVG(CASE WHEN s.type = 'temperature' THEN s.value END) AS avg_temp_c, -- Avg rack temp
    AVG(CASE WHEN s.type = 'humidity' THEN s.value END) AS avg_humidity, -- Avg rack humidity
    AVG(CASE WHEN s.type = 'airflow' THEN s.value END) AS avg_airflow,   -- Avg rack airflow
    AVG(p.power_kw) AS avg_power_kw,                  -- Avg power usage (kW)
    AVG(p.cooling_kw) AS avg_cooling_kw,              -- Avg cooling load (kW)
    (SUM(p.power_kw) + SUM(p.cooling_kw)) / NULLIF(SUM(p.power_kw),0) AS pue, -- Power Usage Effectiveness
    (SUM(p.power_kw) - SUM(p.cooling_kw)) / NULLIF(SUM(p.power_kw),0) AS efficiency -- IT load efficiency
FROM silver_sensor_readings s
JOIN silver_power_data p 
  ON s.rack_id = p.rack_id 
 AND DATE_TRUNC('hour', s.timestamp) = DATE_TRUNC('hour', p.timestamp)
JOIN dim_rack r ON s.rack_id = r.rack_id
GROUP BY r.facility_id, s.rack_id, DATE_TRUNC('hour', s.timestamp);

-- Gold: Facility-level aggregated summary
CREATE TABLE gold_building_summary AS
SELECT
    f.facility_id,                                   -- Facility being monitored
    DATE_TRUNC('hour', p.timestamp) AS time_window,  -- Aggregated by hour
    SUM(p.power_kw) AS total_power_kw,               -- Total power consumption
    AVG(s.value) FILTER (WHERE s.type = 'temperature') AS avg_temp_c, -- Avg building temperature
    COUNT(DISTINCT p.rack_id) AS racks_active,       -- Number of active racks
    (SUM(p.power_kw) + SUM(p.cooling_kw)) / NULLIF(SUM(p.power_kw),0) AS pue -- Facility-level PUE
FROM silver_power_data p
JOIN dim_rack r ON p.rack_id = r.rack_id
JOIN dim_facility f ON r.facility_id = f.facility_id
LEFT JOIN silver_sensor_readings s 
       ON s.rack_id = r.rack_id 
      AND DATE_TRUNC('hour', p.timestamp) = DATE_TRUNC('hour', s.timestamp)
GROUP BY f.facility_id, DATE_TRUNC('hour', p.timestamp);

-- Gold: Datacenter-level efficiency overview
CREATE TABLE gold_datacenter_efficiency AS
SELECT
    f.datacenter_id,                                 -- Datacenter identifier
    DATE_TRUNC('day', p.timestamp) AS time_window,   -- Aggregated daily
    SUM(p.power_kw) AS total_power_kw,               -- Total datacenter power usage
    SUM(p.power_kw) - SUM(p.cooling_kw) AS it_load_kw, -- IT (useful) load
    SUM(p.cooling_kw) AS cooling_kw,                 -- Cooling load
    (SUM(p.power_kw) + SUM(p.cooling_kw)) / NULLIF(SUM(p.power_kw),0) AS pue, -- Datacenter PUE
    (SUM(p.power_kw) - SUM(p.cooling_kw)) / NULLIF(SUM(p.power_kw),0) AS efficiency -- Overall efficiency
FROM silver_power_data p
JOIN dim_rack r ON p.rack_id = r.rack_id
JOIN dim_facility f ON r.facility_id = f.facility_id
GROUP BY f.datacenter_id, DATE_TRUNC('day', p.timestamp);
-- End of SQL Script