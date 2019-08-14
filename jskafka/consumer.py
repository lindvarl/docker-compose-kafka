from confluent_kafka import TopicPartition
from confluent_kafka.avro import AvroConsumer
import logging.handlers
from jskafka.das_fft import DasFft
import os

class Consumer:
    log = logging.getLogger('Kafka Consumer')
    log.setLevel(logging.INFO)
    handler = logging.FileHandler('./consumer.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    log.addHandler(handler)

    def __init__(self, topic=None, partition=None, bootstrap_servers=os.environ["BOOTSTRAP_SERVER"], schema_registry_url=os.environ["SCHEMA_REGISTRY_URL"]):

        self.consumer = AvroConsumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': 'jsvgroupid',
            'schema.registry.url': schema_registry_url,
            # 'socket.keepalive.enable': True,
            # 'socket.receive.buffer.bytes': 800000
        })

        if (topic != None):
            self.topic_partition = TopicPartition(topic, partition)

    def __str__(self):
        sb = []
        for key in self.__dict__:
            sb.append("{key}='{value}'".format(key=key, value=self.__dict__[key]))

        return ', '.join(sb)

    def __repr__(self):
        return self.__str__()

    def get_message(self, offset, fft=False):
        self.log.info(f'Start : get_message({offset}, {fft})')

        self.topic_partition.offset = offset

        self.consumer.assign([self.topic_partition])

        message = self.consumer.poll(10)
        self.consumer.close()

        if fft:
            dasfft = DasFft()
            message.value()['fft'] = dasfft.amplitudes_fft(message.value()['amplitudes'])

        self.log.info(f'End : get_message({offset}, {fft})')

        return message

    def seek_from_to_timestamps(self, start_timestamp, end_timestamp, fft=False):
        self.log.info(f'Start : seek_from_to_timestamps({start_timestamp}, {end_timestamp})')

        start_offset = self.get_offset(self.topic_partition, start_timestamp)
        end_offset = self.get_offset(self.topic_partition, end_timestamp)

        return self.seek_from_to_offsets(start_offset, end_offset, fft)

    def seek_from_to_offsets(self, start_offset, end_offset, fft=False):
        self.log.info(f'Start : seek_from_to({start_offset}, {end_offset})')

        self.topic_partition.offset = start_offset

        self.consumer.assign([self.topic_partition])

        messages = []

        while True:
            message = self.consumer.poll(10)
            if fft:
                dasfft = DasFft()
                message.value()['fft'] = dasfft.amplitudes_fft(message.value()['amplitudes'])
            messages.append(message)
            if (message.offset() >= end_offset):
                self.log.info(f'End : seek_from_to({start_offset}, {end_offset})')
                return messages

    def get_closest_message(self, timestamp, fft=False):
        self.log.info(f'Start : get_closest_message({timestamp})')

        offset = self.get_offset(self.topic_partition, timestamp)
        self.topic_partition.offset = offset

        self.consumer.assign([self.topic_partition])

        message = self.consumer.poll(10)
        if fft:
            dasfft = DasFft()
            message.value()['fft'] = dasfft.amplitudes_fft(message.value()['amplitudes'])

        self.log.info(f'End : get_closest_message({timestamp})')
        return message

    def get_offset(self, topic_partition, timestamp):

        topic_partition.offset = timestamp
        offsets = self.consumer.offsets_for_times([topic_partition])
        return offsets[0].offset

    def get_topics(self):
        self.log.info(f'Start : get_topics()')
        topics = self.consumer.list_topics().topics
        self.log.info(f'Start : get_topics()')
        return topics
