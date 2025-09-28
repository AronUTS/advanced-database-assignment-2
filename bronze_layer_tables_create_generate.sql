-- ============================
-- Create Bronze Table Statements
-- ============================

--Create schema - change this later
CREATE OR REPLACE SCHEMA assignment_bronze;

-- Bronze: Sensor Temperature/Humidity
CREATE OR REPLACE TABLE assignment_bronze.bronze_sensor_raw (
    raw_payload VARIANT,
    ingest_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bronze: Power Consumption
CREATE OR REPLACE TABLE assignment_bronze.bronze_power_raw (
    raw_payload VARIANT,
    ingest_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bronze: Facility External Sensors
CREATE OR REPLACE TABLE assignment_bronze.bronze_facility_raw (
    raw_payload VARIANT,
    ingest_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- ============================
-- UDF Generator Statements
-- ============================

-- Sensor data generator
CREATE OR REPLACE FUNCTION generate_bronze_sensor_data(n INT DEFAULT 100)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'sensor_data_generator'
AS
$$
def sensor_data_generator(n):
    data = []
    sensor_ids = ["S001","S002","S003","S004","S005","S006","S007","S008","S009","S010"]
    rack_ids = ["R001","R002","R003","R004","R005"]
    types = ["temperature","humidity"]

    for i in range(n):
        sensor_id = sensor_ids[i % len(sensor_ids)]
        rack_id = rack_ids[i % len(rack_ids)]
        sensor_type = types[i % len(types)]

        if sensor_type == "temperature":
            value = 20 + (i % 10)
            if rack_id == "R003":
                value += 10
        else:
            value = 40 + (i % 20)

        payload = {
            "sensor_id": sensor_id,
            "rack_id": rack_id,
            "type": sensor_type,
            "unit": "C" if sensor_type=="temperature" else "%",
            "value": value,
            "timestamp": "2025-09-28T00:00:00"
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
def power_data_generator(n):
    data = []
    rack_ids = ["R001","R002","R003","R004","R005"]

    for i in range(n):
        rack_id = rack_ids[i % len(rack_ids)]
        power_kw = 10 + (i % 10)
        if rack_id == "R003":
            power_kw += 10
        voltage_v = 230
        current_a = round(power_kw * 1000 / voltage_v, 2)
        cooling_kw = 2 + (i % 3)
        if rack_id == "R003":
            cooling_kw += 5

        payload = {
            "rack_id": rack_id,
            "power_kw": power_kw,
            "voltage_v": voltage_v,
            "current_a": current_a,
            "cooling_kw": cooling_kw,
            "timestamp": "2025-09-28T00:00:00"
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
def facility_data_generator(n):
    data = []
    facility_ids = ["F01","F02","F03"]

    for i in range(n):
        facility_id = facility_ids[i % len(facility_ids)]
        external_temp_c = 25 + (i % 10)
        if facility_id == "F02":
            external_temp_c += 15
        external_humidity = 40 + (i % 20)
        weather_condition = "Normal" if external_temp_c < 40 else "Heat Alert"
        power_status = ["Normal","Partial Outage","Full Outage"][i % 3]

        payload = {
            "facility_id": facility_id,
            "external_temp_c": external_temp_c,
            "external_humidity": external_humidity,
            "weather_condition": weather_condition,
            "power_status": power_status,
            "timestamp": "2025-09-28T00:00:00"
        }
        data.append(payload)
    return data
$$;

-- ============================
-- Example usage of UDF Generators
-- ============================

-- Generate 10 sensor records
SELECT generate_bronze_sensor_data(10) AS sensor_data;

-- Generate 10 power records
SELECT generate_bronze_power_data(10) AS power_data;

-- Generate 10 facility records
SELECT generate_bronze_facility_data(10) AS facility_data;


-- ============================
-- Example Flatten Queries
-- ============================

-- Flatten sensor data
SELECT 
    s.value:"sensor_id"::STRING AS sensor_id,
    s.value:"rack_id"::STRING AS rack_id,
    s.value:"type"::STRING AS type,
    s.value:"unit"::STRING AS unit,
    s.value:"value"::FLOAT AS value,
    s.value:"timestamp"::TIMESTAMP AS timestamp
FROM TABLE(FLATTEN(INPUT => generate_bronze_sensor_data(10))) AS s;

-- Flatten power data
SELECT
    p.value:"rack_id"::STRING AS rack_id,
    p.value:"power_kw"::FLOAT AS power_kw,
    p.value:"voltage_v"::FLOAT AS voltage_v,
    p.value:"current_a"::FLOAT AS current_a,
    p.value:"cooling_kw"::FLOAT AS cooling_kw,
    p.value:"timestamp"::TIMESTAMP AS timestamp
FROM TABLE(FLATTEN(INPUT => generate_bronze_power_data(10))) AS p;

-- Flatten facility data
SELECT
    f.value:"facility_id"::STRING AS facility_id,
    f.value:"external_temp_c"::FLOAT AS external_temp_c,
    f.value:"external_humidity"::FLOAT AS external_humidity,
    f.value:"weather_condition"::STRING AS weather_condition,
    f.value:"power_status"::STRING AS power_status,
    f.value:"timestamp"::TIMESTAMP AS timestamp
FROM TABLE(FLATTEN(INPUT => generate_bronze_facility_data(10))) AS f;
