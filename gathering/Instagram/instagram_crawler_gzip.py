import sys
import simplejson
from simplejson import JSONEncoder

import calendar
from datetime import datetime, time, timedelta
from time import sleep

import gzip
import math

from search import InstagramAPI

CLIENT_ID = 'da293d1a30da47838eab995849fc6b9d'
CLIENT_SECRET = '401cb69dbdf14441bbcc2d2257d96f58'
ACCESS_TOKEN = '1202575314.da293d1.81e8a9d8e67b4835a41199500ddbe73f'
api = InstagramAPI(access_token = ACCESS_TOKEN)

to_save_main = ["created_time", "id"]

def timestamp_to_datetime(ts):
    return datetime.utcfromtimestamp(float(ts))

def datetime_to_timestamp(dt):
    return calendar.timegm(dt.timetuple())

def shift_coords(coords):
	def convert(angle) :
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
		file_name = 'media_' + self.current_date.strftime("%Y-%m-%d_%H_%M_%S") + '.txt.gz'
		self.txt_file = gzip.open(file_name, 'wb')
		self.map = open(map_splits, "r")
		self.end_time = end_time

	def change_file(self):
		self.txt_file.close()
		file_name = 'media_' + self.current_date.strftime("%Y-%m-%d_%H_%M_%S") + '.txt.gz'
		self.txt_file = gzip.open(file_name, 'a')
		self.counter = 0

	def write_content(self, result):
		for content in result:
			content_dict = {}

			for el in to_save_main:
				content_dict[el] = content[el]
			content_dict["url"] = content["link"]
			content_dict["user_id"] = content["user"]["id"]
			content_dict["geo"] = {
				"type": "Point", 
				"coordinates": [content["location"]["latitude"], content["location"]["longitude"]]
			}
			self.txt_file.write(str(content_dict))
			self.txt_file.write('\n')
			self.txt_file.flush()

	def check_reply(self, result, coords):
		if len(result) == 100:
			coords_dict = shift_coords(coords)

			for key in coords_dict:
				new_coords = coords_dict[key]
				radius = new_coords[6]
				result = api.media_search(count = 100, lat = new_coords[4], lng = new_coords[5], 
											distance = int(radius*1000), 
											min_timestamp = min_date, max_timestamp = max_date)
				self.check_reply(result, new_coords)
		else: 	
			self.write_content(result)

	def search(self):
		while self.current_date > self.end_time:
			max_date = datetime_to_timestamp(self.current_date)
			self.current_date -= timedelta(days = 1)
			min_date = datetime_to_timestamp(self.current_date)
			
			next(self.map)
			for line in self.map:
				coords = line.split(' ')
				radius = float(coords[6])
				try:
					result = api.media_search(count = 100, lat = coords[4], lng = coords[5], 
											distance = int(radius*1000), 
											min_timestamp = min_date, max_timestamp = max_date)
					self.check_reply(result, coords)
				except Exception as e:
					print str(e)
					sleep(50)

			self.change_file()
			self.map.seek(0)


max_date = datetime.now() - timedelta(days = 1)
min_date = max_date - timedelta(days = 365)

if len(sys.argv) >= 3:
	start_time = datetime.strptime(sys.argv[1], "%Y-%m-%d")
	end_time = datetime.strptime(sys.argv[2], "%Y-%m-%d")

	if start_time > max_date:
		start_time = max_date
		end_time = min_date

	if start_time <= end_time:
		end_time = start_time - timedelta(days = 365) 
else:
	start_time = max_date
	end_time = min_date

if len(sys.argv) == 4:
	map_splits = sys.argv[3]
else:
	map_splits = "spb-0.5"

crawler = InstagramCrawler(start_time, end_time, map_splits)
crawler.search()