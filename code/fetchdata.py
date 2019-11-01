import pandas as pd
import json
import csv
import yaml
import os
import time
import shutil
import math
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


constraints = yaml.load(open('constraints.yaml'))
print(constraints)


class CustomHandler(FileSystemEventHandler):
    def microbatch(self, f_name, f_size, c_name):
        f_num = math.ceil(os.path.getsize(f_name)/f_size)
        file_name, f_ext = f_name.split("/")[-1].split(".")
        with open(f_name) as inputfile:
            if f_ext == "csv":
                data = pd.read_csv(f_name)
            elif f_ext == "json":
                data = pd.read_json(f_name)
        for i in range(f_num):
            a = data.iloc[int(i*data.shape[0]/f_num):int((i+1)*data.shape[0]/f_num)]
            if f_ext == "csv":
                a.to_csv(
                    "/home/fidelglock/assignment-2-771368/data/staging_dir/"+file_name+"_"+str(i)+".csv")
            elif f_ext == "json":
                a.to_json(
                    "/home/fidelglock/assignment-2-771368/data/staging_dir/"+file_name+"_"+str(i)+".json")

    def on_created(self, event):
        time.sleep(20)
        print('event type:', event,  'path :', event.src_path)
        f_name = event.src_path
        if "client1" in f_name:
            c_name = "cus1"
        elif "client2" in f_name:
            c_name = "cus2"
        else:
            print("Unauthorized client access" + f_name,
                  file=open("fetchData.log", "a"))
            os.remove(f_name)
            return
        if not constraints[c_name]["filetype"] in f_name:
            print("Incorrect file extension" + f_name,
                  file=open("fetchData.log", "a"))
            os.remove(f_name)
            return
        if os.path.getsize(f_name) > constraints[c_name]["filesize"]:
            print("file size too big for the client" + f_name + " " +
                  str(os.path.getsize(f_name)), file=open("fetchData.log", "a"))
            self.microbatch(f_name, constraints[c_name]["filesize"], c_name)
            os.remove(f_name)
        else:
            print("File Ok. Staging.." + f_name + " " +
                  str(os.path.getsize(f_name)), file=open("fetchData.log", "a"))
            shutil.move(
                f_name, "/home/fidelglock/assignment-2-771368/data/staging_dir/")


event_handler = CustomHandler()
observer = Observer()
observer.schedule(
    event_handler, path='/home/ftpserver/client-input-directory/', recursive=False)
observer.start()
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    observer.stop()
observer.join()
