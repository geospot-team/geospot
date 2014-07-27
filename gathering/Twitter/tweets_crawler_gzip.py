import time
from time import gmtime, strftime
import sys
import gzip
import simplejson
from simplejson import JSONEncoder
import calendar
from datetime import datetime

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener


CKEY = 'LId7s534ocvxl6r1LiJfA'
CSECRET = '0qBx8REXNDQ5oo2IepMT2GUgLP2bMQE8iM7YFKS0NOs'
ATOKEN = '2398065811-w9oTbdj7mZoLMFpchAsi5ubNfpv5AqIsqv0O5QQ'
ASECRET = 'hiD1SReeldMDnAkjQkGXPHTDFwgPS2HbkFtaRIRZM9Aqt'

SaintPeterburg = [29.424641, 59.633739, 30.759600, 60.247791]
Moscow = [37.319260, 55.490700, 37.967609, 55.957600]

to_save_main = ["id", "text", "retweet_count", "favorite_count"]
to_save = {
    "user": ["id", "followers_count", "friends_count", "listed_count", "favourites_count"],
    "place": ["id", "url", "bounding_box"]
}
sourse = ["Web", "iPhone", "iPad", "Android", "instagram", "foursquare"]

def to_timestamp(date):
    dt = datetime.strptime(date, "%a %b %d %H:%M:%S +0000 %Y")
    return calendar.timegm(dt.timetuple())

class Listener(StreamListener):
    def __init__(self):
        name = 'tweets' + strftime("_%Y-%m-%d_%H_%M_%S", gmtime()) + '.txt.gz'
        self.txt_file = gzip.open(name, 'wb')
        self.last_date = gmtime()[:3]
        self.counter = 0

    def check_date(self):
        if self.last_date != gmtime()[:3]:
            name = 'tweets' + strftime("_%Y-%m-%d_%H_%M_%S", gmtime()) + '.txt.gz'
            self.txt_file.flush()
            self.txt_file.close()
            self.txt_file = gzip.open(name, 'wb')
            self.last_date = gmtime()[:3]

    def on_data(self, data):
        try:
            content = simplejson.loads(data)

            content_dict = {}
            for el in to_save_main:
                content_dict[el] = content[el]
            for key in to_save:
                content_dict[key] = {}
                for el in to_save[key]:
                    content_dict[key][el] = content[key][el] 

            tweet_sourse = content["source"]
            for item in sourse:
                if item in tweet_sourse:
                    content_dict["source"] = item
                    break

            geo = content["geo"]
            if geo != None:
                content_dict["geo"] = geo["coordinates"]
            else:
                # 1st method
                bbox = content["place"]["bounding_box"]["coordinates"][0]
                content_dict["geo"] = [(bbox[0][0] + bbox[1][0] + bbox[2][0] + bbox[3][0])/4, 
                                        (bbox[0][1] + bbox[1][1] + bbox[2][1] + bbox[3][1])/4]
                # 2nd method
                #content_dict["geo"] = content["place"]["bounding_box"]["coordinates"][0]

                # 3rd method
                #content_dict["geo"] = content["place"]["bounding_box"]["coordinates"]

            content_dict["created_at"] = to_timestamp(content["created_at"])
            content_dict["user"]["created_at"] = to_timestamp(content["user"]["created_at"])
            content_dict["hashtags"] = content["entities"]["hashtags"]

            if content_dict["source"] in ["instagram", "foursquare"]:
                content_dict["url"] = content["entities"]["urls"][0]["expanded_url"]

            #print content_dict
            self.txt_file.write(str(content_dict))
            self.txt_file.write("\n")
            self.txt_file.flush()
            self.counter += 1
            return True
        except BaseException as e:
            print('Failed on_data: ' + str(e))
            time.sleep(5)

    def on_error(self, status):
        print(status)

def report():
    endTime = time.time()
    print 'Collected', stream_listener.counter, 'tweets in', endTime - startTime, 'seconds'


auth = OAuthHandler(CKEY, CSECRET)
auth.set_access_token(ATOKEN, ASECRET)

if len(sys.argv) == 5:
    coords = [float(sys.argv[1]), float(sys.argv[2]), float(sys.argv[3]), float(sys.argv[4])]
elif len(sys.argv) > 2 and sys.argv[1] == 'Moscow':
    coords = Moscow
else:
    coords = SaintPeterburg

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




