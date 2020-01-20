from flask import Flask
import socketio
from Map import Map, Car
from client import M, sio as sio_client, SOCKET_URL
import json
import pandas as pd
import eventlet
from eventlet import wsgi
from werkzeug.middleware.proxy_fix import ProxyFix

thread = None
app = Flask(__name__)

sio = socketio.Server(async_mode="eventlet", cors_allowed_origins="*")
app.wsgi_app = socketio.WSGIApp(sio, app.wsgi_app)
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

@app.route('/')
def hello_world():
    return "Hello?"

@sio.event
def connect(sid, environ):
    global thread
    print('connect', sid)
    if thread is None:
        print('run Thread')
        sio.start_background_task(update_prediction)
        thread = sio.start_background_task(update_gps)
    for key, value in iconDict.items():
        sio.emit('ICON_ADD', {"type": value['type'], "props": value})


@sio.event
def disconnect(sid):
    print('disconnected', sid)

def update_gps():
    sio_client.connect(SOCKET_URL)
    sio_client.wait()

def update_prediction():
    while True:
        sio.sleep(2)
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
        print('emit', M.S['4'].get_fastest(), 'P4', flush=True)
        # print(M.S['4a'].fastest, 'P4a')


if __name__ == "__main__":
    wsgi.server(eventlet.listen(('', 3000)), app)
