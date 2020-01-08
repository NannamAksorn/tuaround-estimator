from Map import Point, Segment, Graph, Stop, Route, Car, Map
from Compute import get_closest_segment_np
from matplotlib import pyplot as plt
import pandas as pd
from datetime import datetime
import numpy as np
import geojson
from dbhelper import DBhelper
import time
import logger

logging = logger.setup('main')
logging.info('Started Main.py')

db = DBhelper()
gps_log = db.get_all_gps()

# test 3
# gps_log = db.get_gps("0TU0007 (3)",
#                      datetime(2019, 11, 19, 3, 27, 0),
#                      datetime(2019, 11, 19, 4, 0, 0))
# test 1b
# gps_log = db.get_gps()

# test 4
# gps_log = db.get_gps("0TU0008 (4)",
#                      datetime(2019, 11, 19, 0, 3, 0),
#                      datetime(2019, 11, 19, 3, 0, 0))

# test 5 changed from 2
# gps_log = db.get_gps("0TU0021 (2)",
#                      datetime(2019, 11, 21, 1, 1, 0),
#                      datetime(2019, 11, 21, 10, 0, 0))

# load Graph
# G = Graph()
# G.load()

# load Map
M = Map()
M.load()

# test gps log
st_time = time.time()
# print(M.R['route-1b'].route)

car = M.C[gps_log.iloc[0].carno]
route = car.R
for index, gps in gps_log.iloc[:].iterrows():
    # gps = gps_log.iloc[i]
    # print(gps.acctime, gps.carno, gps.timestamp)
    M.handle_gps(gps)
    M.estimate()
    logging.info(M.S['4'].fastest)
# p = Point('', gps.lon, gps.lat)
# p2 = segment.match(p)
# segment.draw()
# # print(segment.dist)
# p.draw('bo', 5)
# p2.draw('ro', 5)
# Segment('', p, p2).draw()
en_time = time.time()
logging.info(f'FINISH main.py {en_time - st_time} sec')
