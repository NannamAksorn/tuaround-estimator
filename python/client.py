import socketio
from Map import Map, Car
import pandas as pd

# load Map
M = Map()
M.load()

SOCKET_URL = 'https://service.mappico.co.th'
ROOM = "THAMMATRANS"
sio = socketio.Client()


@sio.event
def connect():
    sio.emit('room', ROOM)
    print('connection established')


@sio.on('TU-NGV')
def on_gps(data):
    gps = pd.DataFrame([data]).iloc[0]
    # print(gps)
    if gps.carno not in M.C:
        init_car(gps.carno)
    M.handle_gps(gps)
    M.estimate()
    car = M.C[gps.carno]
    print(car, car.status)
    # print(M.S['4'].fastest)
    # print(M.S['4a'].fastest)


@sio.event
def disconnect():
    print('disconnected from server')


def init_car(cid):
    rid = 'route-' + cid[cid.find("(") + 1: cid.find(")")].lower()
    r = M.R[rid]
    car = Car(cid, r)
    M.add_car(car)


if __name__ == "__main__":
    sio.connect(SOCKET_URL)
    sio.wait()
