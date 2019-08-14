from flask import Flask
import socket
import numpy as np
from jskafka.producer import Producer
from jskafka.consumer import Consumer
from jskafka.consumer_subscribe import ConsumerSubscribe
from jskafka.consumer_thread import ConsumerThread
from flask import jsonify
import time
import datetime

app = Flask(__name__)

@app.route('/')
def hello_world():
    return f'Hello world {socket.gethostname()}'

@app.route('/import/<string:topic>/<int:number>')
def import_data(topic, number):
    start = time.time_ns()
    for _ in range(number):
        producer = Producer(topic)

        amplitudes = np.random.rand(10000).tolist()

        value = {"amplitudes": amplitudes, "startSnapshotTimeNano": start, "loci": 2688}

        message = producer.send_one(value)
        start = start+1000000000
    return f'Imported {number}'

@app.route('/get/<string:topic>/<int:partition>/<int:offset>')
def get_message(topic, partition, offset):

    dy = datetime.datetime.now()

    consumer = Consumer(topic, partition)
    message = consumer.get_message(offset)
    dx = datetime.datetime.now()

    json = message.value()

    return {"TimeUsed": str(dx - dy), "partition": message.partition(), "offset": message.offset(), "timestamp": str(message.timestamp())}

@app.route('/seek/<string:topic>/<int:partition>/<int:start_offset>/<int:slutt_offset>')
def seek_from_to_offsets(topic, partition, start_offset, slutt_offset):

    dy = datetime.datetime.now()
    consumer = Consumer(topic, partition)
    messages = consumer.seek_from_to_offsets(start_offset, slutt_offset)
    dx = datetime.datetime.now()
    return {"TimeUsed": str(dx - dy), "size": messages.__len__(), "partition": messages[0].partition(), "start_offset": messages[0].offset(), "start_timestamp": str(messages[0].timestamp()), "end_offset": messages[-1].offset(), "end_timestamp": str(messages[-1].timestamp())}

@app.route('/seek/<string:topic>/<int:start_partition>/<int:end_partition>/<int:start_offset>/<int:end_offset>')
def seek_from_to_partitions_offsets(topic, start_partition, end_partition, start_offset, end_offset):
    partitions = range(start_partition, end_partition)

    dy = datetime.datetime.now()
    consumer = ConsumerThread(topic, group_id='Group_1')

    results = consumer.seek_from_to_offsets(partitions, start_offset, end_offset)

    dx = datetime.datetime.now()
    return {"TimeUsed": str(dx - dy), "size": str(results.__len__()*results.get(min(partitions)).__len__())}

@app.route('/subscribe/<string:topic>/<int:number>')
def subscribe(topic, number):

    dy = datetime.datetime.now()

    consumer = ConsumerSubscribe(topic=topic, group_id='Group_1', auto_offset_reset='earliest')

    run = True
    messages = []
    while run:
        message = consumer.get_message()
        if message != None:
            messages.append(consumer.get_message())
            if (messages.__len__() >= number):
                run = False

    consumer.close()
    dx = datetime.datetime.now()
    return {"TimeUsed": str(dx - dy), "size": messages.__len__()}


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)


