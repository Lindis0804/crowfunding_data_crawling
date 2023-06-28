from fastapi import FastAPI
import uvicorn
import os
from dotenv import load_dotenv
from n_kafka.consumer import MessageConsumer
app = FastAPI()
load_dotenv()
broker = os.environ.get("kafka_broker")
topic = "project"
group_id = "consumer-1"
print("broker", broker)
consumer1 = MessageConsumer(broker=broker, topic=topic, group_id=group_id)
consumer1.activate_listener()
consumer1 = MessageConsumer(broker=broker, topic=topic, group_id=group_id)
consumer1.activate_listener()
