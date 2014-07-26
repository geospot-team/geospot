import sys
import calendar
from datetime import datetime, timedelta
from time import sleep
import gzip
import math
import json

import pymongo

from search import InstagramAPI


config = json.loads(open(sys.argv[1]).read())

CLIENT_ID = config["instagram_auth"]["CLIENT_ID"]
CLIENT_SECRET = config["instagram_auth"]["CLIENT_SECRET"]
ACCESS_TOKEN = config["instagram_auth"]["ACCESS_TOKEN"]
api = InstagramAPI(access_token=ACCESS_TOKEN)
to_save_main = ["created_time", "id"]


def timestamp_to_datetime(ts):
    return datetime.utcfromtimestamp(float(ts))


def datetime_to_timestamp(dt):
    return calendar.timegm(dt.timetuple())


def shift_coords(coords):
    def convert(angle):
        return 180 * angle / math.pi

    def reverse(x):
        return float(x) * math.pi / 180

    lat_left = reverse(coords[0])
    lon_left = reverse(coords[1])
    lat_right = reverse(coords[2])
    lon_right = reverse(coords[3])
    r = float(coords[6]) / 2
    dy = (lat_right - lat_left) / 2
    dx = (lon_right - lon_left) / 2

    def save(lat, lon):
        center_lat = convert(lat + dy / 2)
        center_lon = convert(lon + dx / 2)
        return [convert(lat), convert(lon), convert(lat + dy), convert(lon + dx), center_lat, center_lon, r]

    return {
        "a": save(lat_left, lon_left),
        "b": save(lat_left, lon_left + dx),
        "c": save(lat_left + dy, lon_left),
        "d": save(lat_left + dy, lon_left + dx)
    }


class InstagramCrawler(object):
    def __init__(self, start_time, end_time, map_splits):
        self.current_date = start_time
        self.client = pymongo.MongoClient(config["mongo"]["primary_node"])
        self.writer = self.client[config["mongo"]["database"]][config["mongo"]["collection"]]
        self.writer.ensure_index(([("geo", "2d")]))
        # self.batch = []
        self.counter = 0
        # self.batch_size = int(config["crawler_config"]["batch_size"])
        self.map = open(map_splits, "r")
        self.end_time = end_time


    def change_file(self):
        self.txt_file.close()
        file_name = 'media_' + self.current_date.strftime("%Y-%m-%d_%H_%M_%S") + '.txt.gz'
        self.txt_file = gzip.open(file_name, 'a')
        self.counter = 0

    def write_content(self, result):
        batch = []
        for content in result:
            content_dict = {}

            for el in to_save_main:
                content_dict[el] = content[el]
            content_dict["url"] = content["link"]
            content_dict["user_id"] = content["user"]["id"]
            # content_dict["geo"] = {
            # "type": "Point",
            # "coordinates": [content["location"]["latitude"], content["location"]["longitude"]]
            content_dict["location"] = [content["location"]["latitude"], content["location"]["longitude"]]
            content_dict["_id"] = content_dict["id"]
            del (content_dict["id"])
            # "type": "Point",
            # "coordinates": [content["location"]["latitude"], content["location"]["longitude"]]
            # }
            batch.append(content_dict)
        inserted = False
        while not inserted:
            try:
                inserted_ids = self.writer.insert(self.batch, continue_on_error=True)
                inserted = True
            except pymongo.OperationFailure as exp:
                print('Unexpected error with mongo: {}\n Try add latter'.format(str(exp)))
                sleep(1)

    def check_reply(self, result, coords):
        if len(result) == 100:
            coords_dict = shift_coords(coords)

            for key in coords_dict:
                new_coords = coords_dict[key]
                radius = new_coords[6]
                result = api.media_search(count=100, lat=new_coords[4], lng=new_coords[5],
                                          distance=int(radius * 1000),
                                          min_timestamp=min_date, max_timestamp=max_date)
                self.check_reply(result, new_coords)
        else:
            self.write_content(result)

    def search(self):
        while self.current_date > self.end_time:
            max_date = datetime_to_timestamp(self.current_date)
            self.current_date -= timedelta(days=1)
            min_date = datetime_to_timestamp(self.current_date)

            next(self.map)
            for line in self.map:
                coords = line.split(' ')
                radius = float(coords[6])
                try:
                    result = api.media_search(count=100, lat=coords[4], lng=coords[5],
                                              distance=int(radius * 1000),
                                              min_timestamp=min_date, max_timestamp=max_date)
                    self.check_reply(result, coords)
                except Exception as e:
                    print(str(e))
                    sleep(50)

            self.change_file()
            self.map.seek(0)


max_date = datetime.now() - timedelta(days=1)
min_date = max_date - timedelta(days=365)

start_time = datetime.strptime(config["crawler_config"["start_time"]], "%Y-%m-%d")
end_time = datetime.strptime(config["crawler_config"["end_time"]], "%Y-%m-%d")
if start_time > max_date:
    start_time = max_date
    end_time = min_date
if start_time <= end_time:
    end_time = start_time - timedelta(days=365)
map_splits = config["crawler_config"]["map_splits"]
crawler = InstagramCrawler(start_time, end_time, map_splits)
crawler.search()