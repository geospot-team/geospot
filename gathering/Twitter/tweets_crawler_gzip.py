import time
from time import gmtime, strftime
import sys
import gzip
import json
from json import JSONEncoder

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener


CKEY = 'nErsqiWgmF0b1ig0V7S81Ao30'
CSECRET = 'YzHgEDwD8FUFFVZ0GhtlCTvrZS5H3j5P6Li88hidWs7x1Qlu5l'
ATOKEN = '106777681-aOvw9xpfYwJE4OltT9nVcd5GqyfS8VDtxkGzu3X7'
ASECRET = 'BQe5s8ldhadqyzoHql6YwlPMReVd5EPdw2tfmf7rNJEsd'

SaintPeterburg = [29.424641, 59.633739, 30.759600, 60.247791]
Moscow = [37.319260, 55.490700, 37.967609, 55.957600]

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

            result = JSONEncoder().encode(content_dict)
            #print(result)

            self.check_date()
            self.txt_file.write(json.dumps(result))
            self.txt_file.write("\n")
            self.txt_file.flush()
            self.counter += 1
            return True
        except BaseException as e:
            print('Failed on_data: ' + str(e))
            time.sleep(5)

    def on_error(self, status):
        print(status)
        #self.txt_file.close()


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




