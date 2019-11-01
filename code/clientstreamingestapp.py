
import pymongo
import json
import time
import threading
import sys
import pika


class StatRep(threading.Thread):
    def __init__(self, data, end, start):
        super(StatRep, self).__init__()
        self.end = end
        self.start = start
        self.data = data

    def run(self):
        stats_coll = db.command("collstats", table_name)
        report = {
            'clientID': client,
            'time': self.end-self.start,
            'input_time': self.data["input_time"],
            'size_coll': stats_coll['size'],
            'count_coll': stats_coll['count'],
            'avgObjSize_coll': stats_coll['avgObjSize']
        }
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.basic_publish(
            exchange='', routing_key='reporting_channel', body=json.dumps(report))
        connection.close()
        db["reports"].insert(report)


topic = sys.argv[1]
client = pymongo.MongoClient(
    "mongodb+srv://bdp2019:qwerty12345@test-bdp-e6nsj.gcp.mongodb.net/test?retryWrites=true&w=majority")
db = client.test-bdp
if "cus1" in topic:
    table_name = "googleplaystore"
    client = "cus1"
    table = db["googleplaystore"]
elif "cus2" in topic:
    table_name = "googleplay_user_reviews"
    client = "cus2"
    table = db["googleplay_user_reviews"]
credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters(
    'rabbit-server1', 5672, '/', credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue=topic)


def callback(ch, method, properties, body):
    print("message received"+body.decode(),
          file=open("clientstreamingestapp.log", "a"))
    data = json.loads(body.decode())
    start = time.time()
    table.insert(data)
    end = time.time()
    StatRep(data, end, start).start()


channel.basic_consume(queue=topic, on_message_callback=callback, auto_ack=True)
print(' [*] Waiting for messages on topic', topic,
      'press CTRL+C to exit ', file=open("clientstreamingestapp.log", "a"))
channel.start_consuming()
