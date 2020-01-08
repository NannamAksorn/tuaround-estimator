import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import pickle

from Compute import haversine_np, \
    h_dist, \
    bearing, inverse_bearing, turn_dist, bad, cross_track_dist, \
    cross_track_point, remain_dist, get_closest_segment_np


class TimeTravelSpeed:
    def __init__(self):
        self.time: dict = {}
        self.sample_period = 5

    def init_segment(self, sid):
        initial_speed = 15
        count = 1
        sample_freq = 60 // self.sample_period
        self.time[sid] = np.array((initial_speed, count) * sample_freq * 24 * 7).reshape(7, 24, sample_freq, 2)
        self.time[sid]

    def get_speed(self, sid="", time=datetime.now(), offset_sec=0):
        '''return speed in m/s'''
        d = time + timedelta(seconds=offset_sec)
        weekday = d.weekday()
        hr = d.hour
        minute = d.minute // self.sample_period
        return self.time[sid][weekday][hr][minute][0] / 3.6

    def set_speed(self, sid="", time=datetime.now(), speed=0):
        weekday = time.weekday()
        hr = time.hour
        minute = time.minute // self.sample_period
        prev_speed, count = self.time[sid][weekday][hr][minute]
        self.time[sid][weekday][hr][minute] = [(prev_speed * count + speed) / (count + 1), count + 1]


class Point:
    def __init__(self, pid, lon, lat, direction=0.0, speed=0.0):
        self.pid: str = pid
        self.lon: float = lon
        self.lat: float = lat
        self.direction: float = direction
        self.speed: float = speed

    def intersect(self, lon, lat, thres):
        if haversine_np(lon, lat, self.lon, self.lat) < thres:
            return True
        return False

    def draw(self, color="ro", markersize="1"):
        plt.plot(self.lon, self.lat, color, markersize=markersize)

    def __repr__(self):
        return f"pid: {self.pid} lon: {self.lon}, lat: {self.lat}"

    def __str__(self):
        return f"pid: {self.pid} lon: {self.lon}, lat: {self.lat}"


class Segment:
    def __init__(self, sid, ps, pe):
        self.sid: str = sid
        self.ps: Point = ps
        self.pe: Point = pe
        self.dist: float = 0
        self.bearing: float = 0
        self.bearing_backward: float = 0
        self.speed = None
        self.init()

    def init(self):
        self.dist = h_dist(self.ps, self.pe)
        self.bearing = bearing(self.ps, self.pe)
        self.bearing_backward = inverse_bearing(self.bearing)

    def draw(self, color="k-", markersize="1"):
        plt.plot([self.ps.lon, self.pe.lon], [self.ps.lat, self.pe.lat], color, markersize=markersize)
        self.ps.draw()
        self.pe.draw()

    def d2p(self, p):
        """ Closest distance to point """
        brng_SE = self.bearing
        brng_SP = bearing(self.ps, p)
        brng_ES = self.bearing_backward
        brng_EP = bearing(self.pe, p)
        dist_SP = h_dist(self.ps, p)
        td = turn_dist(p.direction, brng_SE)
        #     obtuse case
        if bad(brng_SE, brng_SP) >= 90:
            return dist_SP + td
        #     acute
        elif bad(brng_ES, brng_EP) >= 90:
            dist_EP = h_dist(self.pe, p)
            return dist_EP + td
        else:
            return cross_track_dist(dist_SP, brng_SP, brng_SE) + td

    def map_match(self, p):
        """ match point with segment"""
        p.lon, p.lat = cross_track_point(self.ps, self.pe, p)
        return p

    def get_remain_dist(self, p):
        """get remain dist from point to pe"""
        return remain_dist(self.ps, self.pe, p, self.dist)


class Graph:
    def __init__(self):
        self.points: dict = {}
        self.segments: dict = {}
        self.speed: TimeTravelSpeed = TimeTravelSpeed()
        self.segment_df = None

    def build(self, geojson):
        for feature in geojson['features']:
            #     stop
            if feature['geometry']['type'] != "LineString":
                continue
            #     way
            tid = feature['properties']['id']
            coordinates = feature['geometry']['coordinates']

            oneway = False
            # if "oneway" in feature:
            #     oneway = feature["oneway"]

            ps = self.add_point(f"p_{tid}_0",
                                coordinates[0][0],
                                coordinates[0][1]
                                )
            for i in range(len(coordinates) - 1):
                pe = self.add_point(f"p_{tid}_{i + 1}",
                                    coordinates[i + 1][0],
                                    coordinates[i + 1][1]
                                    )
                self.add_segment(f's_{tid}_f_{i + 1}', ps, pe)
                if not oneway:
                    self.add_segment(f's_{tid}_b_{i + 1}', pe, ps)
                ps = pe

    def load(self, filename="data/graph.pkl"):
        with open(filename, 'rb') as input:
            tmp_dict = pickle.load(input)
            self.points = tmp_dict.points
            self.speed = tmp_dict.speed
            self.segments = tmp_dict.segments

    def save(self, filename="data/graph.pkl"):
        with open(filename, 'wb') as output:
            pickle.dump(self, output, pickle.HIGHEST_PROTOCOL)

    def get_point_by_id(self, pid):
        return self.points[pid]

    def get_point(self, lon, lat, thres):
        for pid in self.points:
            p = self.get_point_by_id(pid)
            if p.intersect(lon, lat, thres):
                return p

    def get_segment_by_id(self, sid):
        return self.segments[sid]

    def add_point(self, pid, lon, lat):
        if pid in self.points:
            return self.get_point_by_id(pid)

        p = self.get_point(lon, lat, 5)
        if p: return p

        p = Point(pid, lon, lat)
        self.points[pid] = p
        return p

    def add_segment(self, sid, ps, pe):
        if sid in self.segments:
            return self.get_segment_by_id(sid)
        self.speed.init_segment(sid)
        s = Segment(sid, ps, pe)
        self.segments[sid] = s
        return s

    def get_segment_df(self):
        if not (self.segment_df is None):
            return self.segment_df
        temp = []
        for sid in self.segments:
            s = self.segments[sid]
            temp.append([s.ps.lon, s.ps.lat, s.pe.lon, s.pe.lat, s.bearing, s.bearing_backward, s.dist])
        temp = np.array(temp).T
        segment_df = pd.DataFrame({
            "ps_lon": temp[0],
            "ps_lat": temp[1],
            "pe_lon": temp[2],
            "pe_lat": temp[3],
            "bearing": temp[4],
            "bearing_backward": temp[5],
            "dist": temp[6],
            "sid": list(self.segments.keys())
        })
        self.segment_df = segment_df
        return segment_df


class Stop:
    def __init__(self, sid, p):
        self.sid: str = sid
        self.p: Point = p
        self.fastest: dict = {}
        self.incoming: dict = {}

    def get_fastest(self):
        res = {}
        for key, value in self.fastest.items():
            res[key[6:]] = {'i': value['cid'][5:7], 't': value['time'] // 60}
        return res

    def update(self, cid, rid, time):
        if rid not in self.fastest:
            self.fastest[rid] = {'cid': cid, 'time': time}
        elif time < self.fastest[rid]['time']:
            self.fastest[rid] = {'cid': cid, 'time': time}
        if rid not in self.incoming:
            self.incoming[rid] = {}
        elif cid not in self.incoming[rid]:
            self.incoming[rid][cid] = {}
        self.incoming[rid][cid] = {'cid': cid, 'time': time}
        # passed case
        if time == 3600 and self.fastest[rid]['cid'] == cid:
            self.fastest[rid]['time'] = 3600



class Route:
    def __init__(self, rid):
        self.rid: str = rid
        self.route: dict = {}
        self.stops: dict = {}

    def add_stop(self, stop):
        """add stop to dict set init time-to-travel to 3600 sec"""
        self.stops[stop] = 3600

    def add_way(self, graph, s_from, s_to, stop=None, dist=0, change=None):
        if (dist == 0 or pd.isna(dist)) and (s_from != '[S]'):
            dist = graph.get_segment_by_id(s_from).dist
        if not pd.isna(stop):
            self.add_stop(stop)
        self.route[s_from] = {
            'start': s_from,
            'to': s_to,
            'stop': stop,
            'dist': dist,
            'change': change
        }

    def __str__(self):
        return f'<Route> {self.rid} Count: {len(self.route)}'


class Car:
    def __init__(self, cid, route=""):
        self.R: Route = route
        self.cid: str = cid
        self.error: dict = {"out-route": 0, "stop": 0}
        self.status: str = 'init'
        self.pos: Point = None
        self.sid: str = ''
        self.change: str = ''
        self.prev_segments: list = []

    def __str__(self):
        return f'Car: {self.cid} {self.pos}'

    def __repr__(self):
        return f'Car: {self.cid} {self.pos}'

    def set_route(self, route):
        self.R = route

    def update_pos(self, new_point, sid):
        self.pos = new_point
        self.update_sid(sid)
        if new_point.speed <= 1:
            self.error['stop'] += 1
            if self.error['stop'] > 30:
                self.error['stop'] = 30
                self.status = 'st'
            else:
                self.status = 'ha'
        else:
            self.error['stop'] = 0
            self.check_route()

    def update_sid(self, sid):
        self.sid = sid
        if sid not in self.prev_segments:
            self.prev_segments.append(sid)
            if len(self.prev_segments) > 5:
                self.prev_segments.pop(0)

    def check_route(self):
        if self.sid not in self.R.route:
            self.error['out-route'] += 1
            if self.error['out-route'] > 5:
                self.error['out-route'] = 6
                self.status = 'rr'
                return 2
            else:
                self.status = 'wr'
                return 1
        else:
            self.error['out-route'] = 0
            self.status = 'ok'
            return 0

    def estimate(self, g):
        updated_stop = self.R.stops.copy()
        # update passed stop
        start, to, stop, _, change = self.R.route['[S]'].values()
        # update next stops
        time = datetime.now()
        cum_time = 0
        # first
        start, to, stop, _, change = self.R.route[self.sid].values()
        # remain dist
        dist = g.get_segment_by_id(self.sid).get_remain_dist(self.pos)
        # speed
        speed_dict = g.speed
        speed = speed_dict.get_speed(self.sid, time)

        while True:
            # t = d/v
            cum_time += dist / speed
            if not pd.isna(stop):
                if stop in updated_stop and updated_stop[stop] > cum_time:
                    updated_stop[stop] = cum_time
                else:
                    updated_stop[stop] = cum_time

            if to == "[E]":
                break
            speed = speed_dict.get_speed(to, time, cum_time)
            start, to, stop, dist, change = self.R.route[to].values()
        return updated_stop


class Map:
    def __init__(self):
        self.R: dict = {}
        self.S: dict = {}
        self.G: Graph = None
        self.C: dict = {}

    def set_graph(self, graph):
        self.G = graph

    def add_route(self, route):
        self.R[route.rid] = route

    def add_stop(self, stop):
        self.S[stop.sid] = stop

    def add_car(self, car):
        self.C[car.cid] = car

    def update_stop(self, sid, time):
        self.S[sid].update(time)

    def load(self, filename="data/map.pkl"):
        with open(filename, 'rb') as inp:
            tmp_dict = pickle.load(inp)
            self.R = tmp_dict.R
            self.S = tmp_dict.S
            self.G = tmp_dict.G
            self.C = tmp_dict.C

    def save(self, filename="data/map.pkl"):
        with open(filename, 'wb') as output:
            pickle.dump(self, output, pickle.HIGHEST_PROTOCOL)

    def handle_gps(self, gps):
        # get car
        carno = gps.carno
        if carno not in self.C:
            return
        else:
            car = self.C[carno]
        # mm and update pos
        segment_df = self.G.get_segment_df()
        sid = get_closest_segment_np(segment_df, gps.lon, gps.lat, gps.direction, 30)
        if sid == -1: return
        segment = self.G.get_segment_by_id(sid)
        match_point = segment.map_match(Point('', gps.lon, gps.lat, gps.direction, gps.speed))
        car.update_pos(match_point, sid)
        # re-route
        if car.status == 'rr':
            self.re_route(car)

    def re_route(self, car):
        match_route = (-99999, None)
        for rid in self.R:
            if rid == car.R.rid:
                continue
            R = self.R[rid]
            score = 0
            for sid in car.prev_segments:
                if sid in R.route and R.route[sid]['to'] != '[E]':
                    score += 1
                else:
                    score -= 1
            if score > match_route[0]:
                match_route = (score, R)
        if match_route[0] > 3:
            print(match_route[1].rid, car.prev_segments, match_route[0], car.cid)
            cid = car.cid
            rid = car.R.rid
            for sid in car.R.stops:
                self.S[sid].update(cid, rid, 3600)
            car.set_route(match_route[1])

    def estimate(self):
        for cid in self.C:
            car = self.C[cid]
            if car.status != 'ok':
                continue
            updated_stop = car.estimate(self.G)
            # print(updated_stop, car.R.rid)
            rid = car.R.rid
            for sid, time in updated_stop.items():
                self.S[sid].update(cid, rid, time)
        return
