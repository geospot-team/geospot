import calendar
import gzip
import json
import time
import sys
from datetime import datetime

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
    # return dt
    return calendar.timegm(dt.timetuple())


class Listener(StreamListener):
    def __init__(self):
        self.counter = 0
        self.start_time = time.gmtime()
        file_name = 'tweets_' + config["city_name"] + "_" + time.strftime("_%Y-%m-%d_%H_%M_%S", self.start_time) + '.txt.gz'
        self.writer = gzip.open(file_name, 'a')
    def change_file(self):
        current_time = time.gmtime()
        if current_time.tm_wday != self.start_time.tm_wday:
            self.writer.close()
            self.start_time = current_time
            file_name = 'tweets_' + config["city_name"] + "_" + time.strftime("_%Y-%m-%d_%H_%M_%S", self.start_time) + '.txt.gz'
            self.writer = gzip.open(file_name, 'a')

    def save(self, content_dict):
        self.change_file()
        try:
            self.writer.write(str(content_dict) + "\n")
            self.writer.flush()
        except Exception as exp:
            print('Unexpected error: {}\n'.format(str(exp)))
            print('Lost tweet: {}\n'.format(str(content_dict)))


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
        # report()
        print('Unexpected error: {}\n Restart gathering'.format(str(e)))




