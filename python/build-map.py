from dbhelper import DBhelper
from Compute import get_closest_segment_np
from Map import Point, Segment, Graph, Stop, Route, Car, Map
import pandas as pd
import numpy as np
import multiprocessing as mp
import time
import logger

logging = logger.setup('build-map')
logging.info('START build-map.py')
st = time.time()
db = DBhelper()

# create Graph and build
G = Graph()
G.load()

# create MAP
M = Map()
M.set_graph(G)

#  build Route
route_no_list = ['1a', '1a_b', '1b', '2',  '3', '4', '5']
for i in route_no_list:
    route_df = pd.read_csv(f'data/route-{i}.csv')
    route = Route(f'route-{i}')
    # load route
    for index, way in route_df.iterrows():
        route.add_way(G, way['from'], way['to'], way['stop'], way['dist'], way['change'])

    # get stop
    stop_df = route_df[~route_df['stop'].isna()]
    for index, s in stop_df.iterrows():
        if s.stop in M.S:
            continue
        p = G.get_segment_by_id(s.to).pe
        stop = Stop(s.stop, p)
        logging.info(f'add STOP {stop.sid}')
        M.add_stop(stop)
    logging.info(f'add ROUTE {route.rid}')
    M.add_route(route)

# add car
# cars = db.get_all_car()
# for index, c in cars.iterrows():
#     cid = c[0]
#     rid = 'route-' + cid[cid.find("(") + 1: cid.find(")")].lower()
#     r = M.R.get(rid, route)
#     car = Car(cid, r)
#     logging.info(f'add CAR {cid} rid: {r.rid}')
#     M.add_car(car)

# save
M.save()
logging.info(f"FINISH build-map.py {time.time() - st} sec")
