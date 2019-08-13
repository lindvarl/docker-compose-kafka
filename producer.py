from confluent_kafka.avro import AvroProducer
import logging.handlers
from confluent_kafka import avro


class Producer:

    value_schema = avro.loads("""
        {
          "name": "Measurement",
          "type": "record",
          "namespace": "com.equinor.dasdata",
          "fields": [
            {
              "name": "amplitudes",
              "type": {
                "type": "array",
                "items": "float"
              }
            },
            {
              "name": "startSnapshotTimeNano",
              "type": "long"
            },
            {
              "name": "loci",
              "type": "int"
            }
          ]
        }
    """)

    log = logging.getLogger('Kafka Producer')
    log.setLevel(logging.INFO)
    handler = logging.FileHandler('./producer.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    log.addHandler(handler)

    def __init__(self, topic):

        self.producer = AvroProducer({
            'bootstrap.servers': 'broker:29092',
            'client.id': 'pythonClient',
            'schema.registry.url': 'http://schema-registry:8081'
        })
        self.topic = topic

    def __str__(self):
        sb = []
        for key in self.__dict__:
            sb.append("{key}='{value}'".format(key=key, value=self.__dict__[key]))

        return ', '.join(sb)

    def __repr__(self):
        return self.__str__()

    def send_one(self, value):
        self.log.info(f'Start : send_one()')

        self.producer.produce(topic=self.topic, value=value, value_schema=self.value_schema)

        self.producer.flush()

        self.log.info(f'End : send_one()')

    def send_list(self, values):
        self.log.info(f'Start : send_list()')

        for value in values:
            self.producer.produce(topic=self.topic, value=value, value_schema=self.value_schema)

        self.producer.flush()

        self.log.info(f'End : send_list()')


