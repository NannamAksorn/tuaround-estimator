from dbhelper import DBhelper
from Compute import get_closest_segment_np
from Map import Point, Segment, Graph, Stop, Route, Car
import geojson
import pandas as pd
import numpy as np
import multiprocessing as mp
import time
import logger

THREAD_SIZE = 8
output = []


# set speed MULTI PROCESSING
def process_gps_log(gps_log, segment_df, p):
    print(f'start match segment {p}')
    result = []
    for _, gps in gps_log.iterrows():
        sid = get_closest_segment_np(segment_df, gps.lon, gps.lat, gps.direction, 20)
        if sid == -1: continue
        result.append((gps.timestamp.to_pydatetime(), gps.speed, sid))
    print(f'finish match segment {p}')
    return result


def reduce(result):
    output.extend(result)


def clear_geojson():
    with open('data/road-edit2.geojson', 'r', encoding='UTF8') as f:
        with open('data/road-edit_temp.geojson', 'w', encoding='UTF8') as w:
            for line in f:
                # if "null" in line:
                #     continue
                w.write(line)
    logging.info('clean null line  in geojson')


if __name__ == '__main__':
    logging = logger.setup('build-graph')
    st = time.time()
    logging.info(f"START build-graph.py {THREAD_SIZE} threads")
    db = DBhelper()
    gps_log = db.get_all_gps()
    gps_log_count = gps_log['speed'].count()
    logging.info(f"loaded gps_log {gps_log_count} points")

    # clean null line
    clear_geojson()

    # create and build Graph
    G = Graph()
    with open('data/road-edit_temp.geojson', 'r', encoding="UTF8") as f:
        road = geojson.loads(f.read())
        G.build(road)

    # get segment df compute faster
    segment_df = G.get_segment_df()
    gps_log_list = np.array_split(gps_log, THREAD_SIZE)
    pool = mp.Pool(processes=THREAD_SIZE)
    for i in range(THREAD_SIZE):
        p = pool.apply_async(process_gps_log, args=(gps_log_list[i], segment_df, i,), callback=reduce)
    pool.close()
    pool.join()
    for acctime, speed, sid in output:
        G.speed.set_speed(sid, time=acctime, speed=speed)
    # save Graph
    G.save()

    logging.info(f"FINISH build-graph.py {time.time() - st} sec")
