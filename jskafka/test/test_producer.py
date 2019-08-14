from unittest import TestCase
import datetime
import numpy as np

from jskafka.producer import Producer
import logging


class TestProducer(TestCase):
    #logging.basicConfig(level=logging.DEBUG)

    topic = 'Test'

    def test_send_one(self):

        dy = datetime.datetime.now()

        producer = Producer(self.topic)

        amplitudes = np.random.rand(10000).tolist()

        value = {"amplitudes": amplitudes, "startSnapshotTimeNano": 1550834345041000, "loci": 5000}

        message = producer.send_one(value)

    def test_send_list(self):

        dy = datetime.datetime.now()

        producer = Producer(self.topic)

        values = [
                    {"amplitudes": np.random.rand(10000).tolist(), "startSnapshotTimeNano": 1550834345040000, "loci": 5000},
                    {"amplitudes": np.random.rand(10000).tolist(), "startSnapshotTimeNano": 1550834345041000, "loci": 5000},
                    {"amplitudes": np.random.rand(10000).tolist(), "startSnapshotTimeNano": 1550834345042000, "loci": 5000},
                    {"amplitudes": np.random.rand(10000).tolist(), "startSnapshotTimeNano": 1550834345043000, "loci": 5000},
                    {"amplitudes": np.random.rand(10000).tolist(), "startSnapshotTimeNano": 1550834345044000, "loci": 5000},
                    {"amplitudes": np.random.rand(10000).tolist(), "startSnapshotTimeNano": 1550834345045000, "loci": 5000},
                    {"amplitudes": np.random.rand(10000).tolist(), "startSnapshotTimeNano": 1550834345046000, "loci": 5000},
                    {"amplitudes": np.random.rand(10000).tolist(), "startSnapshotTimeNano": 1550834345047000, "loci": 5000},
                    {"amplitudes": np.random.rand(10000).tolist(), "startSnapshotTimeNano": 1550834345048000, "loci": 5000},
                    {"amplitudes": np.random.rand(10000).tolist(), "startSnapshotTimeNano": 1550834345049000, "loci": 5000}
                ]

        message = producer.send_list(values)






