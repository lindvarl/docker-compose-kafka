from unittest import TestCase
import datetime

from jskafka.consumer import Consumer
import logging


class TestConsumer(TestCase):
    #logging.basicConfig(level=logging.DEBUG)

    topic = 'test-topic1'

    def test_get_message(self):

        partition = 1

        dy = datetime.datetime.now()

        consumer = Consumer(self.topic, partition)

        message = consumer.get_message(2)
        self.assertIsNotNone(message.value())

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertFalse('fft' in message.value())
        self.assertTrue('amplitudes' in message.value())
        self.assertTrue(message.value().get('amplitudes').__len__() > 0)
        print(message.value().get('amplitudes'))

        print(f'partition: {message.partition()}')
        print(f'offset: {message.offset()}')
        print(f'timestamp: {message.timestamp()}')
        print(message.value())

    def test_get_message_fft(self):

        partition = 1

        dy = datetime.datetime.now()

        consumer = Consumer(self.topic, partition)

        message = consumer.get_message(10, fft=True)
        self.assertIsNotNone(message.value())

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertTrue('fft' in message.value())
        self.assertTrue('amplitudes' in message.value())

        self.assertTrue(message.value().get('amplitudes').__len__() > 0)
        self.assertTrue(message.value().get('fft').__len__() > 0)

        print(f'partition: {message.partition()}')
        print(f'offset: {message.offset()}')
        print(f'timestamp: {message.timestamp()}')
        ##print(message.value())

    def test_seek_from_to_timestamps(self):
        partition = 1

        start_ts = 1551953428784  # 5
        slutt_ts = 1551953522714  # 69

        dy = datetime.datetime.now()
        consumer = Consumer(self.topic, partition)

        messages = consumer.seek_from_to_timestamps(start_ts, slutt_ts)

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertEqual(messages[0].timestamp()[1], start_ts)
        self.assertEqual(messages[-1].timestamp()[1], slutt_ts)
        self.assertFalse('fft' in messages[0].value())
        self.assertTrue('amplitudes' in messages[0].value())
        self.assertFalse('fft' in messages[-1].value())
        self.assertTrue('amplitudes' in messages[-1].value())

    def test_seek_from_to_timestamps_fft(self):
        partition = 1

        start_ts = 1551953428784  # 5
        slutt_ts = 1551953522714  # 69

        dy = datetime.datetime.now()
        consumer = Consumer(self.topic, partition)

        messages = consumer.seek_from_to_timestamps(start_ts, slutt_ts, fft=True)

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertEqual(messages[0].timestamp()[1], start_ts)
        self.assertEqual(messages[-1].timestamp()[1], slutt_ts)
        self.assertTrue('fft' in messages[0].value())
        self.assertTrue('amplitudes' in messages[0].value())
        self.assertTrue('fft' in messages[-1].value())
        self.assertTrue('amplitudes' in messages[-1].value())

    def test_seek_from_to_offsets(self):

        partition = 1

        start_offset = 10
        slutt_offset = 69

        dy = datetime.datetime.now()
        consumer = Consumer(self.topic, partition)

        messages = consumer.seek_from_to_offsets(start_offset, slutt_offset)

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertEqual(messages.__len__(), 60)
        self.assertEqual(messages[0].offset(), 10)
        self.assertEqual(messages[-1].offset(), 69)
        self.assertFalse('fft' in messages[0].value())
        self.assertTrue('amplitudes' in messages[0].value())
        self.assertFalse('fft' in messages[-1].value())
        self.assertTrue('amplitudes' in messages[-1].value())

    def test_seek_from_to_offsets_fft(self):

        partition = 1

        start_offset = 10
        slutt_offset = 69

        dy = datetime.datetime.now()
        consumer = Consumer(self.topic, partition)

        messages = consumer.seek_from_to_offsets(start_offset, slutt_offset, fft=True)

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertEqual(messages.__len__(), 60)
        self.assertEqual(messages[0].offset(), 10)
        self.assertEqual(messages[-1].offset(), 69)
        self.assertTrue('fft' in messages[0].value())
        self.assertTrue('amplitudes' in messages[0].value())
        self.assertTrue('fft' in messages[-1].value())
        self.assertTrue('amplitudes' in messages[-1].value())

    def test_get_closest_message(self):
        partition = 1

        ts = 1551953503714

        dy = datetime.datetime.now()

        consumer = Consumer(self.topic, partition)

        message = consumer.get_closest_message(ts)

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertEqual(message.offset(), 50)
        self.assertFalse('fft' in message.value())
        self.assertTrue('amplitudes' in message.value())

    def test_get_closest_message_fft(self):
        partition = 1

        ts = 1551953503714

        dy = datetime.datetime.now()

        consumer = Consumer(self.topic, partition)

        message = consumer.get_closest_message(ts, fft=True)

        dx = datetime.datetime.now()
        print(f'Time used {dx - dy}')

        self.assertEqual(message.offset(), 50)
        self.assertTrue('fft' in message.value())
        self.assertTrue('amplitudes' in message.value())


    def test_get_topics(self):

        dy = datetime.datetime.now()

        consumer = Consumer(None, None)

        topics = consumer.get_topics()

        dx = datetime.datetime.now()
        #print(f'Time used {dx - dy}')

        self.assertTrue(topics.__len__() > 0)


