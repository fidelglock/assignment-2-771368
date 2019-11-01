
def ingestion(batch_path, client_id):

import pymongo
import time
import os
import bson
import pandas as pd


client = pymongo.MongoClient("mongodb+srv://bdp2019:qwerty12345@test-bdp-e6nsj.gcp.mongodb.net/test?retryWrites=true&w=majority")
db = client.test-bdp

if client_id == "cus1":
table_name = "googleplaystore"
client = "cus1"
table = db["googleplaystore"]
elif if client_id == "customer2":
table_name = "googleplaystore_user_reviews"
client = "cus2"
table = db["googleplaystore_user_reviews"]

batch = pd.read_csv(batch_path)
start = time.time()
request = table.insert(batch.to_dict(orient='records'))
end = time.time()

stats_coll = database.command("collstats", client_id)

report = {

'ingestion_size':sum([len(bson.BSON.encode(table.find_one({'_id':document_id}))) for document_id in request]),
'time':end-start,
'rows':len(request),
'rate':len(request)/batch.shape[0],
'size_coll':stats_coll['size'],
'count_coll':stats_coll['count'],
'avgObjSize_coll':stats_coll['avgObjSize']
}
print('ingestion report ready:', file=open("clientbatchingestapp.log","a"))
del batch, start, end, request, stats_coll, table, db, client, client_id

return report