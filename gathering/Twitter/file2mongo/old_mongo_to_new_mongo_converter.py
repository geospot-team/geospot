import gzip
import json
import sys
from datetime import datetime

import pymongo
import simplejson


config = json.loads(open(sys.argv[1]).read())


def to_timestamp(date):
    dt = datetime.strptime(date, "%a %b %d %H:%M:%S +0000 %Y")
    return dt
    # return calendar.timegm(dt.timetuple())


old_mongo = pymongo.MongoClient(config["old"]["primary_node"])
reader = old_mongo[config["old"]["database"]][config["old"]["collection"]]

new_mongo = pymongo.MongoClient(config["new"]["primary_node"])
writer = new_mongo[config["new"]["database"]][config["new"]["collection"]]


def convert(tweet):
    tweet["geo"] = {
        "type": "Point",
        "coordinates": tweet["geo"][::-1]
    }
    tweet["created_at"] = datetime.fromtimestamp(tweet["created_at"])
    tweet["user"]["created_at"] = datetime.fromtimestamp(tweet["user"]["created_at"])
    return tweet

tweets = [convert(tweet) for tweet in reader.find()]
writer.insert(tweets)
