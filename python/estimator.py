from kafka import KafkaConsumer, KafkaProducer
import time
from Map import Map, Car
import pandas as pd
import re

import logger

logging = logger.setup('estimator')
logging.info('Started estimator.py')

# load Map
M = Map()
M.load()
import json

def init_car(cid):
    match = re.match(r'.*TU\d{2}(\d{2}).*\((.{1,2})\).*', cid)
    if not match:
        logging.error(f"wrong cid format {cid}")
        return
    rid = f'route-{match.group(2).lower()}'
    r = M.R[rid]
    car = Car(cid, r, match.group(1))
    M.add_car(car)
    logging.info(f'init new car {cid}')

def handle_gps(data):
    gps = pd.DataFrame([data], columns=['carno', 'timestamp', 'lat', 'lon', 'speed', 'direction']).iloc[0]

    if gps.carno not in M.C:
        init_car(gps.carno)
    car = M.handle_gps(gps)
    if not car: return
    # [alias, route_id, lat, lon, direction, status]
    processed_data =  [
        car.alias,
        car.R.rid[6:],
        round(float(car.pos.lat), 5),
        round(float(car.pos.lon), 5),
        round(float(car.pos.direction), 1),
        car.status
    ]
    return processed_data
    # print(car, car.status)
    # print(M.S['4'].fastest)
    # print(M.S['4a'].fastest)

if __name__  == "__main__":
    kafka_server = ['127.0.0.1:9092']
    consumer = KafkaConsumer('gps-log-topic',
                    bootstrap_servers=kafka_server,
                    # max_poll_records=1000,
                    max_poll_records=5,
                    auto_offset_reset="earliest",
                    group_id="backend"
    )
    # consumer.commit(offsets=5330)
    logging.info("Running Consumer...")
    producer = KafkaProducer(
        bootstrap_servers=kafka_server,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    logging.info("Running Producer...")

    i = 0
    while True:
        try:
            time.sleep(2)
            msg_pack = consumer.poll(timeout_ms=2000)
            # no data
            if len(msg_pack) == 0: continue
            # loop data
            for tp, messages in msg_pack.items():
                for message in messages:
                    data = json.loads(message.value)
                    processed_data = handle_gps(data)
                    if not processed_data: continue
                    producer.send('processed-gps-topic', value=processed_data)
                    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
            M.estimate()
            # logging.info(len(messages))
        except Exception as err:
            logging.exception("message")
            pass
    logging.critical("Break out of While true loop estimator:64")
    consumer.close()
    producer.flush()

