import pymongo
import pandas as pd
from datetime import datetime


class DBhelper:
    def __init__(self):
        self.client = None
        self.db = None
        self.init()

    def init(self, client="mongodb://localhost:27017/", db="tu-around"):
        self.client = pymongo.MongoClient(client)
        self.db = self.client[db]

    def get_gps(self, carno="0TU0001 (1B)",
                date_from=datetime(2019, 11, 19, 3, 43, 0),
                date_to=datetime(2019, 11, 19, 4, 10, 0),
                ):
        col = self.db['gpslogs']
        query = {"carno": carno,
                 "acctime": {"$gte": date_from,
                             "$lt": date_to},
                 "speed": {"$gt": 0}
                 }
        selector = {"_id": 0, "lat": 1, "lon": 1, "timestamp": 1,
                    "acctime": 1, "speed": 1, "direction": 1,
                    "carstatus": 1, "carno": 1
                    }
        res = col.find(query, selector)
        gps_log = pd.DataFrame(list(res))
        return gps_log

    def get_all_gps(self):
        col = self.db['gpslogs']
        query = {"speed": {"$gte": 0}}
        selector = {"_id": 0, "lat": 1, "lon": 1, "timestamp": 1,
                    "acctime": 1, "speed": 1, "direction": 1,
                    "carstatus": 1, "carno": 1
                    }
        res = col.find(query, selector)
        gps_logs = pd.DataFrame(res)
        return gps_logs

    def get_all_car(self):
        col = self.db['gpslogs']
        agr = [{
            '$group': {'_id': '$carno'}
        }]
        res = col.aggregate(agr)
        cars = pd.DataFrame(res)
        return cars
