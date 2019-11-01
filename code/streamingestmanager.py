import threading
import subprocess
import json
import time
import signal
import pika
import os


topics_avl = {"cus1": [], "cus2": []}
jobs = {"cus1": [], "cus2": []}
credentials = pika.PlainCredentials('guest', 'guest')
parameters = pika.ConnectionParameters(
    'rabbit-server1', '5672', '/', credentials)


class TopicPublish(threading.Thread):
    def __init__(self):
        super(TopicPublish, self).__init__()

    def run(self):
        while True:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            for item in topics_avl.keys():
                channel.queue_declare(queue='topicnames_'+item)
                channel.basic_publish(
                    exchange='', routing_key='topicnames_'+item, body=",".join(topics_avl[item]))
            connection.close()
            time.sleep(20)


class ReportReceive(threading.Thread):
    def __init__(self):
        super(ReportReceive, self).__init__()

    def run(self):
        def callback(ch, method, properties, body):
            global jobs, topics_avl
            report = json.loads(body.decode())
            if report["time"] > 10:
                clientName = report["clientID"]
                topicName = clientName+"_"+str(len(topics_avl[clientName]))
                print("Start new topic for", report["clientID"])
                a = subprocess.Popen(
                    ['python', 'clientstreamingestapp.py', topicName])
                topics_avl[clientName].append(topicName)
                jobs[clientName].append(a)
            if report["time"] < 10 and len(topics_avl[report["clientID"]]) > 1:
                for i in range(jobs):
                    if i == 0:
                        continue
                    os.killpg(os.getpgid(jobs[i].pid), signal.SIGTERM)
                jobs = [jobs[0]]
                topics_avl = [topics_avl[0]]
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue='reporting_channel')
        channel.basic_consume(queue='reporting_channel',
                              on_message_callback=callback, auto_ack=True)
        channel.start_consuming()


pub = TopicPublish()
pub.start()

rec = ReportReceive()
rec.start()

for topic in topics_avl.keys():
    topicName = topic+"_"+str(len(topics_avl[topic]))
    a = subprocess.Popen(['python', 'clientstreamingestapp.py', topicName])
    topics_avl[topic].append(topicName)
    jobs[topic].append(a)
