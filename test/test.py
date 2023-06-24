# from n_kafka.producer import ProjectProducer
# import traceback
# broker, topic = "localhost:9092", "kickstarter-project"
# print("[*] Sending to broker "+broker+", topic: "+topic)
# msg = "Oh my gosh."
# print(msg)
# projectProducer = ProjectProducer(broker=broker, topic=topic)
# try:
#     projectProducer.send_msg(msg)
#     print("[*] Send to kafka successfully.")
# except:
#     print("[*] Send to kafka fail.")
#     traceback.print_exc()
from dotenv import load_dotenv
import os
import json
load_dotenv()
obj = {
    "name": "Le Dinh Hieu",
    "MSSV": "20194280"
}
s = json.dumps(obj).encode('utf-8')
print(s)
s1 = json.loads(s.decode('utf-8'))
print(s1)
print(obj["name"])
