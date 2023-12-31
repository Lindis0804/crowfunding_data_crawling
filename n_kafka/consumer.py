from kafka import KafkaConsumer, consumer
from time import sleep
import json
import os


def save_to_json_file(file_addr, data):
    with open(file_addr, "r") as jf:
        cur_data = json.load(jf)
    if (isinstance(cur_data, list)):
        cur_data.append(data)
    elif (isinstance(cur_data, dict)):
        cur_data = data
    with open(file_addr, "w") as jf:
        json.dump(cur_data, jf)


class MessageConsumer:
    broker = ""
    topic = ""
    group_id = ""
    logger = None

    def __init__(self, broker, topic, group_id):
        self.broker = broker
        self.topic = topic
        self.group_id = group_id
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=[self.broker],
            group_id=self.group_id,
            consumer_timeout_ms=60000,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

    def activate_listener(self):
        consumer = self.consumer
        consumer.subscribe(self.topic)
        print(f"[*] Consumer is listening topic {self.topic}")
        try:
            for message in consumer:
                project = message[6]
                print(project)
                save_to_json_file("D:/bigdata/Project/crowfunding_data_crawling/data/data.json", project)
                """
                Process indiegogo project data by spark here.
                """
            consumer.commit()
        except KeyboardInterrupt:
            print("Aborted by user...")
        finally:
            consumer.close()
