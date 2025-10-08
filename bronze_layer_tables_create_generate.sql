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
