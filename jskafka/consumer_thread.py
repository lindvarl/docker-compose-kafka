from confluent_kafka import TopicPartition
from confluent_kafka.avro import AvroConsumer
import logging.handlers
import concurrent.futures
from jskafka.das_fft import DasFft
import os

class ConsumerThread:
    log = logging.getLogger('Kafka ConsumerThread')
    log.setLevel(logging.INFO)
    handler = logging.FileHandler('./consumerThread.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    log.addHandler(handler)

    def __init__(self, topic, group_id, bootstrap_servers=os.environ["BOOTSTRAP_SERVER"], schema_registry_url=os.environ["SCHEMA_REGISTRY_URL"]):

        self.topic = topic
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url= schema_registry_url

    def __str__(self):
        sb = []
        for key in self.__dict__:
            sb.append("{key}='{value}'".format(key=key, value=self.__dict__[key]))

        return ', '.join(sb)

    def __repr__(self):
        return self.__str__()

    def get_message(self, partitions, offset, fft=False):
        self.log.info(f'Start : get_message({partitions}, {offset})')

        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=partitions.__sizeof__()) as executor:

            futures = {executor.submit(self.__get_message, p, offset, fft): p for p in partitions}
            for future in concurrent.futures.as_completed(futures):
                partition = futures[future]
                try:
                    data = future.result()
                except Exception as exc:
                    self.log.info(
                        f'Topic: {self.topic}, Partition: {partition}, Offset: {offset} generated an exception: {exc}')
                else:
                    results[partition] = data

        self.log.info(f'End : get_message({partitions}, {offset})')

        return results

    def seek_from_to_timestamps(self, partitions, start_timestamp, end_timestamp, fft=False):
        self.log.info(f'Start : seek_from_to_timestamps({partitions}, {start_timestamp}, {end_timestamp})')

        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=partitions.__sizeof__()) as executor:

            futures = {executor.submit(self.__seek_from_to_timestamps, p, start_timestamp, end_timestamp, fft): p for p in partitions}
            for future in concurrent.futures.as_completed(futures):
                partition = futures[future]
                try:
                    data = future.result()
                except Exception as exc:
                    self.log.info(
                        f'Topic: {self.topic}, Partition: {partition}, StartTime: {start_timestamp}, EndTime: {end_timestamp} generated an exception: {exc}')
                else:
                    results[partition] = data

        self.log.info(f'End : seek_from_to_timestamps({partitions}, {start_timestamp}, {end_timestamp})')

        return results

    def seek_from_to_offsets(self, partitions, start_offset, end_offset, fft=False):
        self.log.info(f'Start : seek_from_to_offsets({partitions}, {start_offset}, {end_offset})')

        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=partitions.__sizeof__()) as executor:

            futures = {executor.submit(self.__seek_from_to_offsets, p, start_offset, end_offset, fft): p for p in partitions}
            for future in concurrent.futures.as_completed(futures):
                partition = futures[future]
                try:
                    data = future.result()
                except Exception as exc:
                    self.log.info(
                        f'Topic: {self.topic}, Partition: {partition}, StartTime: {start_offset}, EndTime: {end_offset} generated an exception: {exc}')
                else:
                    results[partition] = data

        self.log.info(f'End : seek_from_to_offsets({partitions}, {start_offset}, {end_offset})')

        return results

    def __get_message(self, partition, offset, fft):
        self.log.info(f'Start : __get_message({partition},{offset})')

        consumer = AvroConsumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'schema.registry.url': self.schema_registry_url})

        topic_partition = TopicPartition(self.topic, partition)
        topic_partition.offset = offset
        consumer.assign([topic_partition])

        message = consumer.poll(10)

        consumer.close()

        if fft:
            dasfft = DasFft()
            message.value()['fft'] = dasfft.amplitudes_fft(message.value()['amplitudes'])



        self.log.info(f'End : __get_message({partition},{offset})')

        return message

    def __seek_from_to_timestamps(self, partition, start_timestamp, end_timestamp, fft):
        self.log.info(f'Start : __seek_from_to_timestamps({partition}, {start_timestamp}, {end_timestamp})')

        consumer = AvroConsumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'schema.registry.url': self.schema_registry_url})

        topic_partition = TopicPartition(self.topic, partition)
        start_offset = self.get_offset(consumer, topic_partition, start_timestamp)
        end_offset = self.get_offset(consumer, topic_partition, end_timestamp)

        return self.__seek_from_to_offsets(partition, start_offset, end_offset, fft)

    def __seek_from_to_offsets(self, partition, start_offset, end_offset, fft):
        self.log.info(f'Start : __seek_from_to_offsets({partition}, {start_offset}, {end_offset})')

        consumer = AvroConsumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'schema.registry.url': self.schema_registry_url})

        topic_partition = TopicPartition(self.topic, partition)
        topic_partition.offset = start_offset
        consumer.assign([topic_partition])

        messages = []

        while True:
            message = consumer.poll(10)
            if fft:
                dasfft = DasFft()
                message.value()['fft'] = dasfft.amplitudes_fft(message.value()['amplitudes'])

            messages.append(message)
            if (message.offset() >= end_offset):
                self.log.info(f'End : __seek_from_to_offsets({partition}, {start_offset}, {end_offset})')
                return messages

    def get_offset(self, consumer, topic_partition, timestamp):

        topic_partition.offset = timestamp
        offsets = consumer.offsets_for_times([topic_partition])
        return offsets[0].offset

    def get_offsets(self, topic_partition, timestamps):

        i = 0
        topic_partitions = []
        for timestamp in timestamps:
            tp = TopicPartition(topic_partition.topic, topic_partition.partition)
            tp.offset = timestamp
            topic_partitions.append(tp)

        offsets = self.consumer.offsets_for_times(topic_partitions)
        return offsets

    def get_topics(self):
        self.log.info(f'Start : get_topics()')
        topics = self.consumer.list_topics().topics
        self.log.info(f'Start : get_topics()')
        return topics
