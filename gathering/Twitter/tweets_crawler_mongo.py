import gzip
import json
import time
import sys
from datetime import datetime

import pymongo
import simplejson
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener


config = json.loads(open(sys.argv[1]).read())

CKEY = config["twitter_auth"]["CKEY"]
CKEY = config["twitter_auth"]["CKEY"]
CSECRET = config["twitter_auth"]["CSECRET"]
ATOKEN = config["twitter_auth"]["ATOKEN"]
ASECRET = config["twitter_auth"]["ASECRET"]

to_save_main = ["id", "text", "retweet_count", "favorite_count"]
to_save = {
    "user": ["id", "followers_count", "friends_count", "listed_count", "favourites_count"],
    "place": ["id", "url", "bounding_box"]
}

source = ["Web", "iPhone", "iPad", "Android", "instagram", "foursquare"]


def to_timestamp(date):
    dt = datetime.strptime(date, "%a %b %d %H:%M:%S +0000 %Y")
    return dt
    # return calendar.timegm(dt.timetuple())


class Listener(StreamListener):
    def __init__(self):
        self.client = pymongo.MongoClient(config["mongo"]["primary_node"])
        self.writer = self.client[config["mongo"]["database"]][config["mongo"]["collection"]]
        self.writer.ensure_index(([("geo", "2dsphere")]))
        self.batch = []
        self.counter = 0
        file_name = 'tweets_recovery' + time.strftime("_%Y-%m-%d_%H_%M_%S", time.gmtime()) + '.txt.gz'
        self.recovery_file = gzip.open(file_name, 'a')
        self.batch_size = int(config["crawler_config"]["batch_size"])

    # def check_batch(self):
    #     if len(self.batch) > self.batch_size:
    #         try:
    #             self.writer.save(self.batch)
    #             # self.batch = [tweet for tweet in self.batch if tweet["_id"] not in set(inserted_ids)]
    #         except Exception as exp:
    #             print('Unexpected error with mongo: {}\nSave to file'.format(str(exp)))
    #             file_name = 'tweets_recovery' + time.strftime("_%Y-%m-%d_%H_%M_%S", time.gmtime()) + '.txt.gz'
    #             txt_file = gzip.open(file_name, 'a')
    #             for content_dict in self.batch:
    #                 txt_file.write(str(content_dict))
    #                 txt_file.write("\n")
    #             txt_file.flush()
    #             txt_file.close()
    #             self.batch = []

    def save(self, content_dict):
        try:
            self.writer.save(content_dict)
        except Exception as exp:
            print('Unexpected error with mongo: {}\n'.format(str(exp)))
            self.recovery_file.write(str(content_dict))
            self.recovery_file.write("\n")
            self.recovery_file.flush()


    def on_data(self, data):
        try:
            content = simplejson.loads(data)

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
            if geo is not None:
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
            self.save(content_dict)
            # self.batch.append(content_dict)
            # print(result)
            # self.check_batch()
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




