from flask import Flask
import socket
import numpy as np
from producer import Producer
from flask import jsonify
import time

app = Flask(__name__)

@app.route('/')
def hello_world():
    return f'Hello world {socket.gethostname()}'

@app.route('/import/<int:number>')
def import_data(number):
    start = time.time_ns()
    for _ in range(number):
        producer = Producer('test-topic1')

        amplitudes = np.random.rand(10000).tolist()

        value = {"amplitudes": amplitudes, "startSnapshotTimeNano": start, "loci": 2688}

        message = producer.send_one(value)
        start = start+1000000000
    return f'Imported {number}'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)


