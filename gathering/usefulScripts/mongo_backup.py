__author__ = 'Vasily'
import gzip
import json
import time
import sys
import pymongo
config = json.loads(open(sys.argv[1]).read())
client = pymongo.MongoClient(config["mongo"]["primary_node"])
reader = client[config["mongo"]["database"]][config["mongo"]["collection"]]
file_name = 'mongo_backup_' + config["mongo"]["database"] + "_" + config["mongo"]["collection"] +  "_" + time.strftime("_%Y-%m-%d_%H_%M_%S", time.gmtime()) + '.txt.gz'
file = gzip.open(file_name,"w")
for doc in reader.find():
    file.write(str(doc))
    file.write("\n")

file.flush()
file.close()