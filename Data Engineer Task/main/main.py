import asyncio
import json
from os import environ
from time import time, sleep
from faker import Faker
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy import Table, Column, Integer, String, MetaData

faker = Faker()
while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        metadata_obj = MetaData()
        devices = Table(
            'devices', metadata_obj,
            Column('device_id', String),
            Column('temperature', Integer),
            Column('location', String),
            Column('time', String),
        )
        metadata_obj.create_all(psql_engine)
        break
    except OperationalError:
        sleep(0.1)


async def store_data_point(device_id):
    ins = devices.insert()
    with psql_engine.connect() as conn:
        # Truncate the "devices" table before each run/load
        conn.execute(devices.delete())
        while True:
            data = dict(
                device_id=device_id,
                temperature=faker.random_int(10, 50),
                location=json.dumps(dict(latitude=str(faker.latitude()), longitude=str(faker.longitude()))),
                time=str(int(time()))
            )
            conn.execute(ins, data)
            print(device_id, data['time'])
            conn.commit()
            await asyncio.sleep(1.0)


loop = asyncio.get_event_loop()
duration_minutes = 1  # Set the duration for running the program in minutes
duration_seconds = duration_minutes * 60  # Convert minutes to seconds

# Create tasks for each device
tasks = [
    store_data_point(str(faker.uuid4())),
    store_data_point(str(faker.uuid4())),
    store_data_point(str(faker.uuid4()))
]

# Run the event loop for the specified duration
loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
loop.run_until_complete(asyncio.sleep(duration_seconds))

# Cancel all tasks and stop the event loop
for task in tasks:
    task.cancel()
loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
loop.close()