from unittest import TestCase
from datetime import datetime

from jskafka.consumer_thread import ConsumerThread
import logging

class TestConsumer(TestCase):
    #logging.basicConfig(level=logging.DEBUG)
    topic = 'Grane10k4'

    def test_get_message(self):
        partitions = [1, 3, 5]

        dy = datetime.now()

        consumer = ConsumerThread(self.topic)

        results = consumer.get_message(partitions, 50)
        self.assertIsNotNone(results)

        dx = datetime.now()
        print(f'Time used {dx - dy}')

        self.assertEqual(results.get(1).partition(), 1)
        self.assertEqual(results.get(3).partition(), 3)
        self.assertFalse('fft' in results.get(3).value())
        self.assertTrue('amplitudes' in results.get(3).value())
        self.assertEqual(results.get(5).partition(), 5)

    def test_get_message_fft(self):
        partitions = [1, 3, 5]

        dy = datetime.now()

        consumer = ConsumerThread(self.topic)

        results = consumer.get_message(partitions, 50, fft=True)
        self.assertIsNotNone(results)

        dx = datetime.now()
        print(f'Time used {dx - dy}')

        self.assertEqual(results.get(1).partition(), 1)
        self.assertEqual(results.get(3).partition(), 3)
        self.assertTrue('fft' in results.get(3).value())
        self.assertTrue('amplitudes' in results.get(3).value())
        self.assertEqual(results.get(5).partition(), 5)


    def test_seek_from_to_timestamps(self):

        #partitions = [1, 3, 5]
        partitions = [*range(0, 10), *range(50, 60), *range(100, 110), *range(200, 210), *range(300, 310)]
        start_ts = 1563281126913
        slutt_ts = 1563281136913

        dy = datetime.now()

        consumer = ConsumerThread(self.topic)

        results = consumer.seek_from_to_timestamps(partitions, start_ts, slutt_ts)

        dx = datetime.now()
        print(f'Time ueed       : {dx - dy}')

        print(f'Count partitions: {results.__len__()}')
        print(f'Start partition : {results.get(min(partitions))[0].partition()}')
        print(f'End partition   : {results.get(max(partitions) - 1)[-1].partition()}')

        print(f'Count offsets   : {results.get(min(partitions)).__len__()}')
        print(f'Start offset    : {results.get(min(partitions))[0].offset()} Timestamp: {results.get(min(partitions))[0].timestamp()[1]} Time: {datetime.fromtimestamp(results.get(min(partitions))[0].timestamp()[1] / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")}')
        print(f'End offset      : {results.get(max(partitions))[-1].offset()} Timestamp: {results.get(max(partitions))[-1].timestamp()[1]} Time: {datetime.fromtimestamp(results.get(max(partitions))[-1].timestamp()[1] / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")}')


        # partition --> offset
        self.assertTrue(results.__len__() > 0)
        self.assertEqual(results.get(3)[0].partition(), 3)
        self.assertFalse('fft' in results.get(3)[0].value())
        self.assertTrue('amplitudes' in results.get(3)[0].value())
        self.assertEqual(results.get(5)[0].partition(), 5)





    def test_seek_from_to_timestamps_fft(self):

        partitions = [1, 3, 5]
        #partitions = range(1, 10)

        start_ts = 1547819755110000  # 10
        slutt_ts = 1548063901657000  # 69

        dy = datetime.now()

        consumer = ConsumerThread(self.topic)

        results = consumer.seek_from_to_timestamps(partitions, start_ts, slutt_ts, fft=True)

        dx = datetime.now()
        print(f'Time used {dx - dy}')

        # partition --> offset
        self.assertTrue(results.__len__() > 0)
        self.assertEqual(results.get(3)[0].partition(), 3)
        self.assertTrue('fft' in results.get(3)[0].value())
        self.assertTrue('amplitudes' in results.get(3)[0].value())
        self.assertEqual(results.get(5)[0].partition(), 5)



    def test_seek_from_to_offsets(self):


        #partitions = [1, 3, 5]
        partitions = range(1, 10)

        start_offset = 10
        slutt_offset = 30

        dy = datetime.now()
        consumer = ConsumerThread(self.topic)

        results = consumer.seek_from_to_offsets(partitions, start_offset, slutt_offset)

        dx = datetime.now()
        print(f'Time used {dx - dy}')

        self.assertTrue(results.__len__() >  0)
        self.assertEqual(results.get(3)[1].partition(), 3)
        self.assertEqual(results.get(3)[2].partition(), 3)
        self.assertEqual(results.get(5)[1].partition(), 5)

        print(f'{results.get(1)[0].timestamp()[1]} : {datetime.fromtimestamp(results.get(1)[0].timestamp()[1]/1000).strftime("%Y-%m-%d %H:%M:%S.%f")}')


    def test_ja(selfs):
        a = datetime.fromtimestamp(1563281134913/1000)
        print(a.strftime('%Y-%m-%d %H:%M:%S.%f'))
