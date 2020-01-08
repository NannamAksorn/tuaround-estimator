# from aiohttp import web
from flask import Flask
from flask_socketio import SocketIO
from Map import Map, Car
import threading
from client import M, sio as sio_client, SOCKET_URL
import json
import pandas as pd

# load Map
# M = Map()
# M.load()

app = Flask(__name__)
sio = SocketIO(app)
iconDict = {
    1: {
        "type": "image", "id": "1",
        "lat": 14.07336, "lon": 100.60212,
        "width": 15, "height": 15, "src": "@stop-target.png"
    },
    2: {
        "type": "lottie", "id": "2",
        "lat": 14.073414, "lon": 100.60212,
        "width": 50, "height": 50, "src": "@aura.json"
    }
}


@sio.on('connect')
def connect():
    print('connect')
    for key, value in iconDict.items():
        sio.emit('ICON_ADD', {"type": value['type'], "props": value})


@sio.on('disconnect')
def disconnect():
    print('disconnected')


def update_gps():
    threading.Timer(2.0, update_gps).start()
    # print('uda')
    update_dict = {}
    for cid in M.C:
        car = M.C[cid]
        if car.status != 'init':
            update_dict[car.cid[5:7]] = {
                'i': car.cid[5:7],
                'r': car.R.rid[6:],
                'a': round(float(car.pos.lat), 5),
                'o': round(float(car.pos.lon), 5),
                'd': round(float(car.pos.direction), 1),
                's': car.status
            }
    sio.emit('TU-NGV', json.dumps(update_dict, separators=(',', ':')))
    # sio.emit('TU-NGV', json.dumps(update_dict, separators=(',', ':')))
    sio.emit('P_4', json.dumps(M.S['4'].get_fastest()), separators=(',', ':'))
    # sio.emit('P_4A', json.dumps(M.S['4a'].fastest), separators=(',', ':'))
    print(M.S['4'].get_fastest(), 'P4')
    # print(M.S['4a'].fastest, 'P4a')


if __name__ == "__main__":
    update_gps()
    sio_client.connect(SOCKET_URL)
    sio.run(app, port=3000)
    sio_client.wait()
    # web.run_app(app, port=3000)
