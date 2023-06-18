from os import environ
from time import sleep
from sqlalchemy.exc import OperationalError
from sqlalchemy import create_engine, Table, Column, Integer, Float, MetaData, String, func, select, JSON, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.inspection import inspect

print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgreSQL successful.')

# Write the solution here

while True:
    try:
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to MySQL successful.')

# Define the table structure for device_aggregates in MySQL
metadata = MetaData()
device_aggregates = Table(
    'device_aggregates',
    metadata,
    Column('device_id', String(255)),
    Column('hour', String(50)),
    Column('max_temperature', Float),
    Column('data_points', Integer),
    Column('total_distance_in_kilometers',String(255))
)

# Create a session to interact with the MySQL database
Session = sessionmaker(bind=mysql_engine)
session = Session()

# Check if the device_aggregates table exists
inspector = inspect(mysql_engine)

if not inspector.has_table('device_aggregates'):
    metadata.create_all(mysql_engine)
    print("Created 'device_aggregates' table in MySQL.")
else:
     print("'device_aggregates' table exists in MySQL.")

# Psql query to fetch and transform data using CTE method
query1 = """
WITH temperature_data AS (
  SELECT
    device_id,
    DATE_TRUNC('hour', TO_TIMESTAMP(time::bigint)) AS hour,
    MAX(temperature) AS max_temperature
  FROM
    devices
  GROUP BY
    device_id,
    hour
), data_points_count AS (
  SELECT
    device_id,
    DATE_TRUNC('hour', TO_TIMESTAMP(time::bigint)) AS hour,
    COUNT(*) AS number_of_datapoints
  FROM
    devices
  GROUP BY
    device_id,
    hour
), location_data AS (
  SELECT
    device_id,
    DATE_TRUNC('hour', TO_TIMESTAMP(time::bigint)) AS hour,
    CAST(TRIM('"' FROM (location::json->>'latitude')) AS DECIMAL) AS latitude,
    CAST(TRIM('"' FROM (location::json->>'longitude')) AS DECIMAL) AS longitude
  FROM
    devices
)
SELECT
  t1.device_id,
  t1.hour,
  t1.max_temperature,
  t2.number_of_datapoints,
  SUM(t3.distance) AS total_distance_in_kilometers
FROM
  temperature_data t1
JOIN
  data_points_count t2 ON t1.device_id = t2.device_id
  AND t1.hour = t2.hour
JOIN (
  SELECT
    device_id,
    hour,
    (
      ACOS(
        SIN(RADIANS(latitude)) * SIN(RADIANS(lag(latitude) OVER (PARTITION BY device_id ORDER BY hour))) +
        COS(RADIANS(latitude)) * COS(RADIANS(lag(latitude) OVER (PARTITION BY device_id ORDER BY hour))) *
        COS(RADIANS(longitude - lag(longitude) OVER (PARTITION BY device_id ORDER BY hour)))
      ) * 6371
    ) AS distance
  FROM
    location_data
) AS t3 ON t1.device_id = t3.device_id
  AND t1.hour = t3.hour
GROUP BY
  t1.device_id,
  t1.hour,
  t1.max_temperature,
  t2.number_of_datapoints
ORDER BY
  t1.device_id,
  t1.hour
"""

# pull the data from PostgreSQL source : "devices" table
with psql_engine.connect() as connection:
    statement = text(query1)
    result = connection.execute(statement)
    result_set = result.fetchall()
    print("Data extracted from 'device' table in PostgreSQL source. Src_Record_Count:", len(result_set))

# Insert each row into the device_aggregates table in MySQL target
for row in result_set:
    device_id = row[0]
    hour = row[1]
    max_temperature = row[2]
    data_points = row[3]
    total_distance_in_kilometers = row[4]

    # Create a new record using the defined table structure
    record = device_aggregates.insert().values(
        device_id=device_id,
        hour=hour,
        max_temperature=max_temperature,
        data_points=data_points,
        total_distance_in_kilometers = total_distance_in_kilometers
    )

    # Add the record to the session
    session.execute(record)

# Commit the changes to the MySQL database
session.commit()
print("Data inserted into the 'device_aggregates' table in MySQL.")
sleep(10)
print('ETL Completed')