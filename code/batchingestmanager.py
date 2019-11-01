import pandas as pd
import os
import time
import importlib
import subprocess
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


class MyHandler(FileSystemEventHandler):
    def on_created(self, event):
        time.sleep(20)
        print('event type:', event,  'path :', event.src_path,
              file=open("batchingestmanager.log", "a"))
        f_name = event.src_path
        if "client1" in f_name:
            c_name = "cus1"
        elif "client2" in f_name:
            c_name = "cus2"
        a = subprocess.Popen(["python", "clientbatchingestapp.py", c_name])
        report = ingestion('code/client-stage-directory/')
        print('the ingestion report is:', file=open(
            "batchingestmanager.log", "a"))
        os.remove(
            "/home/fidelglock/assignment-2-771368/data/staging_dir/"+first_batch_f_name)
