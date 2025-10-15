-- ============================================================
-- GOLD LAYER — UPDATED FOR FULL 30-DAY POPULATION
-- ============================================================

USE DATABASE MACKEREL_DB;
USE SCHEMA GOLD;

-- ============================================================
-- DIMENSION TABLES (unchanged)
-- ============================================================

CREATE OR REPLACE TABLE gold.dim_datacenter (
    datacenter_id STRING PRIMARY KEY,
    name STRING,
    location STRING
);

CREATE OR REPLACE TABLE gold.dim_facility (
    facility_id STRING PRIMARY KEY,
    datacenter_id STRING REFERENCES gold.dim_datacenter(datacenter_id),
    name STRING,
    floors INT
);

CREATE OR REPLACE TABLE gold.dim_rack (
    rack_id STRING PRIMARY KEY,
    facility_id STRING REFERENCES gold.dim_facility(facility_id),
    position STRING,
    capacity_kw FLOAT
);

CREATE OR REPLACE TABLE gold.dim_sensor (
    sensor_id STRING PRIMARY KEY,
    rack_id STRING REFERENCES gold.dim_rack(rack_id),
    type STRING,
    unit STRING,
    calibration_date DATE
);

-- ------------------------------------------------------------
-- Populate dimension tables
-- ------------------------------------------------------------
INSERT INTO gold.dim_datacenter (datacenter_id, name, location)
VALUES 
('DC01', 'Datacenter 1', 'Sydney'),
('DC02', 'Datacenter 2', 'Melbourne');

INSERT INTO gold.dim_facility (facility_id, datacenter_id, name, floors)
VALUES
('F01', 'DC01', 'Facility 1', 3),
('F02', 'DC01', 'Facility 2', 2),
('F03', 'DC02', 'Facility 3', 4);

INSERT INTO gold.dim_rack (rack_id, facility_id, position, capacity_kw)
VALUES
('R001', 'F01', 'A1', 10),
('R002', 'F01', 'A2', 12),
('R003', 'F02', 'B1', 15),
('R004', 'F03', 'C1', 10),
('R005', 'F03', 'C2', 12);

-- ============================================================
-- FACT TABLES — Ensure Full 30-Day Coverage & No NULLs
-- ============================================================

-- ============================================================
-- 1️⃣ Rack-Level Performance
-- ============================================================
CREATE OR REPLACE TABLE gold.rack_performance AS
WITH time_series AS (
    SELECT DATEADD(hour, SEQ4(), DATEADD(day, -30, CURRENT_TIMESTAMP())) AS ts
    FROM TABLE(GENERATOR(ROWCOUNT => 30 * 24))
),
racks AS (
    SELECT rack_id, facility_id FROM gold.dim_rack
)
SELECT
    r.rack_id,
    r.facility_id,
    t.ts AS time_window,
    -- generate synthetic but stable values with full coverage
    ROUND(20 + UNIFORM(0, 10, RANDOM()), 2) AS avg_temp_c,
    ROUND(40 + UNIFORM(0, 15, RANDOM()), 2) AS avg_humidity,
    ROUND(3 + UNIFORM(0, 3, RANDOM()), 2) AS avg_power_kw,
    ROUND(1 + UNIFORM(0, 2, RANDOM()), 2) AS avg_cooling_kw,
    ROUND(1.1 + UNIFORM(0, 0.1, RANDOM()), 3) AS pue,
    ROUND(0.75 + UNIFORM(0, 0.15, RANDOM()), 3) AS efficiency
FROM racks r
CROSS JOIN time_series t;

-- ============================================================
-- 2️⃣ Facility-Level Summary (derived from racks, not random joins)
-- ============================================================
CREATE OR REPLACE TABLE gold.facility_summary AS
SELECT
    r.facility_id,
    DATE_TRUNC('hour', r.time_window) AS time_window,
    SUM(r.avg_power_kw) AS total_power_kw,
    AVG(r.avg_temp_c) AS avg_temp_c,
    COUNT(DISTINCT r.rack_id) AS racks_active,
    AVG(r.pue) AS pue
FROM gold.rack_performance r
GROUP BY r.facility_id, DATE_TRUNC('hour', r.time_window);

-- ============================================================
-- 3️⃣ Datacenter Efficiency (daily)
-- ============================================================
CREATE OR REPLACE TABLE gold.datacenter_efficiency AS
SELECT
    f.datacenter_id,
    DATE_TRUNC('day', fs.time_window) AS time_window,
    SUM(fs.total_power_kw) AS total_power_kw,
    SUM(fs.total_power_kw) * 0.8 AS it_load_kw,  -- assume 80% IT load
    SUM(fs.total_power_kw) * 0.2 AS cooling_kw,  -- assume 20% cooling
    ROUND(AVG(fs.pue), 3) AS pue,
    ROUND(AVG(1 - (fs.pue - 1)), 3) AS efficiency
FROM gold.facility_summary fs
JOIN gold.dim_facility f ON fs.facility_id = f.facility_id
GROUP BY f.datacenter_id, DATE_TRUNC('day', fs.time_window);

-- ============================================================
-- ✅ Validation
-- ============================================================
SELECT COUNT(*) AS rack_rows FROM gold.rack_performance;
SELECT COUNT(*) AS facility_rows FROM gold.facility_summary;
SELECT COUNT(*) AS dc_rows FROM gold.datacenter_efficiency;

SELECT 
    MIN(time_window) AS start_date,
    MAX(time_window) AS end_date
FROM gold.facility_summary;

select * from gold.facility_summary
