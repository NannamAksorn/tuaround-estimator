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
        # logging.error(f"wrong cid format {cid}")
        return
    rid = f'route-{match.group(2).lower()}'
    r = M.R[rid]
    car = Car(cid, r, match.group(1))
    M.add_car(car)
    logging.info(f'init new car {cid}')

def format_processed_data(car):
    # [alias, route_id, lat, lon, direction, status]
    if not car or not car.pos: return
    processed_data =  [
        car.alias,
        car.R.rid[6:],
        round(float(car.pos.lat), 5),
        round(float(car.pos.lon), 5),
        round(float(car.pos.direction), 1),
        car.status
    ]
    return processed_data

def handle_gps(data):
    gps = pd.DataFrame([data], columns=['carno', 'timestamp', 'lat', 'lon', 'speed', 'direction']).iloc[0]

    if gps.carno not in M.C:
        init_car(gps.carno)
    car = M.handle_gps(gps)
    return format_processed_data(car)

# +++++++++++++++  MAIN  +++++++++++++++++++++++++
if __name__  == "__main__":
    kafka_server = ['127.0.0.1:9092']
    consumer = KafkaConsumer('gps-log-topic',
                    bootstrap_servers=kafka_server,
                    # max_poll_records=100,
                    max_poll_records=5,
                    auto_offset_reset="earliest",
    )
    logging.info("Running Consumer...")
    producer = KafkaProducer(
        bootstrap_servers=kafka_server,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    logging.info("Running Producer...")

    while True:
        try:
            time.sleep(2)
            msg_pack = consumer.poll(timeout_ms=2000)
            # no data
            if len(msg_pack) == 0: continue
            # loop data
            car_ids = []
            for tp, messages in msg_pack.items():
                for message in messages:
                    data = json.loads(message.value)
                    car_ids.append(data[0])
                    processed_data = handle_gps(data)
                    if not processed_data: continue
                    producer.send('processed-gps-topic', value=processed_data)
            M.estimate()
            M.update_online_cars(car_ids)
            for cid in list(M.OC):
                car = M.C[cid]
                if car.status == "pne" or car.status == "ne":
                    processed_data = format_processed_data(car)
                    if not processed_data: continue
                    producer.send('processed-gps-topic', value=processed_data)
                if car.status == 'ne':
                    M.OC.pop(cid, None)

            for sid in M.S:
                stop = M.S[sid]
                if stop.isUpdated:
                    stop.isUpdated = False
                    producer.send('eta-topic', value=(sid,stop.get_fastest()))
            # break
        # sio.emit('P_4', json.dumps(M.S['4'].get_fastest()), separators=(',', ':'))
            # logging.info(len(messages))
        except Exception as err:
            logging.info("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
            logging.exception("message")
            # break
            pass
    logging.critical("Break out of While true loop estimator:64")
    producer.flush()
    consumer.close()
