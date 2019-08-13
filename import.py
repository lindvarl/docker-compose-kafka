import time
from producer import Producer
import numpy as np

start = time.time_ns()
while True:
    producer = Producer('test-topic1')

    amplitudes = np.random.rand(10000).tolist()

    value = {"amplitudes": amplitudes, "startSnapshotTimeNano": start, "loci": 2688}

    message = producer.send_one(value)
    start = start + 1000000000
