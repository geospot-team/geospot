import time
import sys
import json
from json import JSONEncoder

import pymongo

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener


config = json.loads(open(sys.argv[1]).read())

CKEY = config["twitter_auth"]["CKEY"]
CSECRET = config["twitter_auth"]["CSECRET"]
ATOKEN = config["twitter_auth"]["ATOKEN"]
ASECRET = config["twitter_auth"]["ASECRET"]

to_save_main = ["created_at", "id", "text", "source", "geo", "retweet_count", "favorite_count"]
to_save = {
    "user": ["id", "followers_count", "friends_count", "listed_count", "created_at", "favourites_count"],
    "place": ["id", "url", "bounding_box"],
    "entities": ["hashtags"]
}
to_save_entities = {
    "urls": "expanded_url",
    "user_mentions": "id",
}


class Listener(StreamListener):
    def __init__(self):
        self.client = pymongo.MongoClient(config["mongo"]["primary_node"])
        self.writer = self.client[config["mongo"]["database"]][config["mongo"]["collection"]]
        self.writer.ensure_index(([("geo", "2d")]))
        self.batch = []
        self.counter = 0
        self.batch_size = int(config["crawler_config"]["batch_size"])

    def check_batch(self):
        if len(self.batch) > self.batch_size:
            try:
                inserted_ids = self.writer.insert(self.batch, continue_on_error=True)
                self.batch = [tweet for tweet in self.batch if tweet["_id"] not in set(inserted_ids)]
            except pymongo.OperationFailure as exp:
                print('Unexpected error with mongo: {}\n Try add latter'.format(str(exp)))

    def on_data(self, data):
        try:
            content = json.loads(data)
            content_dict = {}

            for el in to_save_main:
                content_dict[el] = content[el]
            for key in to_save:
                content_dict[key] = {}
                for el in to_save[key]:
                    content_dict[key][el] = content[key][el]
            for key in to_save_entities:
                entity = content["entities"][key]
                ent_key = to_save_entities[key]
                content_dict[key] = {}
                if len(entity) != 0:
                    content_dict[key][ent_key] = entity[0][ent_key]
            content_dict["_id"] = content_dict["id"]
            del (content_dict["id"])
            self.batch.append(content_dict)
            # print(result)
            self.check_batch()
            self.counter += 1
            return True
        except BaseException as e:
            print('Failed on_data: ' + str(e))
            time.sleep(1)

    def on_error(self, status):
        print(status)
        # self.txt_file.close()


def report():
    endTime = time.time()
    print('Collected', stream_listener.counter, 'tweets in', endTime - startTime, 'seconds')


auth = OAuthHandler(CKEY, CSECRET)
auth.set_access_token(ATOKEN, ASECRET)

coords = config["crawler_config"]["bounding_box"]
coords = [float(coord) for coord in coords]

interrupt = False
while not interrupt:
    try:
        stream_listener = Listener()
        twitterStream = Stream(auth, stream_listener)
        startTime = time.time()
        twitterStream.filter(locations=coords)
    except KeyboardInterrupt:
        interrupt = True
        report()
    except BaseException as e:
        report()
        print('Unexpected error: {}\n Restart gathering'.format(str(e)))




