



import pandas as pd
import time
import random
import sys
import pika
import json
import ctypes
import threading
import argparse

def parse_args():
    p = argparse.Argumentp()
    p.add_argument('--client_id', type=str, help='Set client ID.', default='cus1')
    return p.parse_args()
args = parse_args()
topics = []
credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters('rabbit-server1',5672,'/',credentials)

class TopicRetreive(threading.Thread):
    def __init__(self, clientID):
        super(TopicRetreive, self).__init__()
        self.clientID = clientID
    def run(self):
        def callback(ch, method, properties, body):
            global topics
            topics = body.decode().split(",")
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.basic_consume(queue='topicnames_' + self.clientID, on_message_callback=callback, auto_ack=True)
        channel.start_consuming()
    def get_id(self):
       if hasattr(self, '_thread_id'):
           return self._thread_id
       for id, thread in threading._active.items():
           if thread is self:
               return id
    def raise_exception(self):
        thread_id = self.get_id()
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, ctypes.py_object(SystemExit))
ret = TopicRetreive(args.client_id)
ret.start()

if args.client_id == "cus1":
    pd_data = pd.read_csv("../data/googleplaystore.csv")
elif args.client_id == "cus2":
    pd_data = pd.read_csv("../data/googleplaystore_user_reviews.csv")

while len(topics) == 0:
    time.sleep(1)

connection = pika.BlockingConnection(parameters)
channel = connection.channel()
start = time.time()
for i in pd_data.index[:10]:
    data_row = pd_data.loc[[i],].to_dict(orient='records')[0]
    data_row["input_time"] = time.time()
    topicName = random.choice(topics)
    channel.basic_publish(exchange='', routing_key=topicName, body=json.dumps(data_row))
connection.close()
print("OK : ", time.time()-start)
ret.raise_exception()