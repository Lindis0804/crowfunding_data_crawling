from confluent_kafka import Producer

import json


class ProjectProducer:
    producer = None
    broker = "localhost:9092"
    topic = "geats"

    def __init__(self, broker="localhost:9092",
                 socket_timeout=100, api_version_request='false',
                 broker_version_fallback='0.9.0'
                 ):
        self.producer = Producer({
            'bootstrap.servers': broker,
            'socket.timeout.ms': socket_timeout,
            'api.version.request': api_version_request,
            'broker.version.fallback': broker_version_fallback
        })

    def delivery_report(self, err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(
                msg.topic(), msg.partition()))

    def send_msg_async(self, msg, topic="geats"):
        print("Send message asynchronously")
        self.producer.produce(
            topic,
            msg,
            callback=lambda err, original_msg=msg:
                self.delivery_report(err, original_msg),
        )
        self.producer.flush()

    def send_msg_sync(self, msg, topic='geats'):
        print("Send message synchronously")
        self.producer.produce(
            topic,
            msg,
            callback=lambda err, original_msg=msg:
                self.delivery_report(err, original_msg),
        )
        self.producer.flush()
