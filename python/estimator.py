from kafka import KafkaConsumer
import time
from Map import Map, Car
import pandas as pd

# load Map
M = Map()
M.load()
import json

def init_car(cid):
    rid = 'route-' + cid[cid.find("(") + 1: cid.find(")")].lower()
    r = M.R[rid]
    car = Car(cid, r)
    M.add_car(car)

def handle_gps(data):
    gps = pd.DataFrame([data], columns=['carno', 'timestamp', 'lat', 'lon', 'speed', 'direction']).iloc[0]

    if gps.carno not in M.C:
        init_car(gps.carno)
    M.handle_gps(gps)
    car = M.C[gps.carno]
    print(car, car.status)
    # print(M.S['4'].fastest)
    # print(M.S['4a'].fastest)

if __name__  == "__main__":
    print("Running Consumer...")
    kafka_server = ['127.0.0.1:9092']
    consumer = KafkaConsumer('gps-log-topic',
                    bootstrap_servers=kafka_server,
                    max_poll_records=1000,
                    auto_offset_reset="earliest"
    )

    i = 0
    while True:
        try:
            time.sleep(2)
            msg_pack = consumer.poll(timeout_ms=2000)
            # no data
            if len(msg_pack) == 0: continue
            # loop data
            for tp, messages in msg_pack.items():
                print(len(messages))
                for message in messages:
                    data = json.loads(message.value)
                    handle_gps(data)
                    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))
            M.estimate()
        except Exception as err:
            print(err)
            pass

    consumer.close()
