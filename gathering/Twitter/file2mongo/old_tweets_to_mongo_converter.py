import gzip
import json
import sys
import calendar
from datetime import datetime

import pymongo
import simplejson


config = json.loads(open(sys.argv[1]).read())

to_save_main = ["id", "text", "retweet_count", "favorite_count"]
to_save = {
    "user": ["id", "followers_count", "friends_count", "listed_count", "favourites_count"],
    "place": ["id", "url", "bounding_box"]
}

source = ["Web", "iPhone", "iPad", "Android", "instagram", "foursquare"]


def to_timestamp(date):
    dt = datetime.strptime(date, "%a %b %d %H:%M:%S +0000 %Y")
    return dt
    #return calendar.timegm(dt.timetuple())


client = pymongo.MongoClient(config["mongo"]["primary_node"])
writer = client[config["mongo"]["database"]][config["mongo"]["collection"]]
files = config["files"]


def tweet_pipe(tweet):
    content = simplejson.loads(tweet)
    content_dict = {}
    for el in to_save_main:
        content_dict[el] = content[el]
    for key in to_save:
        content_dict[key] = {}
        if content is not None:
            for el in to_save[key]:
                if content["place"] is not None:
                    content_dict[key][el] = content[key][el]

    tweet_source = content["source"]
    content_dict["source"] = "other"
    for item in source:
        if item in tweet_source:
            content_dict["source"] = item
            break

    geo = content["geo"]
    if geo != None:
        content_dict["certain_coords"] = 1
        content_dict["geo"] = {}
        content_dict["geo"]["type"] = "Point"
        content_dict["geo"]["coordinates"] = geo["coordinates"][::-1]
    else:
        content_dict["certain_coords"] = 0
        # 1st method
        bbox = content["place"]["bounding_box"]["coordinates"][0]
        content_dict["geo"] = {}
        content_dict["geo"]["type"] = "Point"
        content_dict["geo"]["coordinates"] = [(bbox[0][1] + bbox[1][1] + bbox[2][1] + bbox[3][1]) / 4,
                                              (bbox[0][0] + bbox[1][0] + bbox[2][0] + bbox[3][0]) / 4]
        # 2nd method
        # content_dict["geo"] = content["place"]["bounding_box"]["coordinates"][0]

        # 3rd method
        # content_dict["geo"] = content["place"]["bounding_box"]["coordinates"]

    content_dict["created_at"] = to_timestamp(content["created_at"])
    content_dict["user"]["created_at"] = to_timestamp(content["user"]["created_at"])
    content_dict["hashtags"] = content["entities"]["hashtags"]

    if content_dict["source"] in ["instagram", "foursquare"] and len(content["entities"]["urls"]) > 0:
        content_dict["url"] = content["entities"]["urls"][0]["expanded_url"]

    content_dict["_id"] = content_dict["id"]
    del (content_dict["id"])
    return content_dict


i = 0
batch = []

for filename in files:
    for tweet in gzip.open(filename):
        print(i)
        i = i + 1
        inserted = False
        while not inserted:
            try:
                inserted_ids = writer.save(tweet_pipe(tweet))
                inserted = True
            except Exception as exp:
                print('Unexpected error with mongo: {}\n'.format(str(exp)))



writer.ensure_index(([("geo", "2dsphere")]))