from search import InstagramAPI
from instagram.json_import import simplejson
import calendar
from datetime import datetime, date, time
from time import sleep
import gzip

def timestamp_to_datetime(ts):
    return datetime.utcfromtimestamp(float(ts))

def datetime_to_timestamp(dt):
    return calendar.timegm(dt.timetuple())


CLIENT_ID = 'da293d1a30da47838eab995849fc6b9d'
CLIENT_SECRET = '401cb69dbdf14441bbcc2d2257d96f58'
ACCESS_TOKEN = '1202575314.da293d1.81e8a9d8e67b4835a41199500ddbe73f'
api = InstagramAPI(access_token = ACCESS_TOKEN)


class InstagramSearcher(object):

	def __init__(self, year, month):
		self.file_name = 'media_' + str(year) + '-'+ str(month)
		self.txt_file = gzip.open(self.file_name + '.txt.gz', 'wb')
		self.year = year
		self.month = month
		self.counter = 0
		self.page = 1

	def check_count(self):
		if self.counter > 5000:
			self.txt_file.close()
			file_name = self.file_name + '_' + str(self.page) +'.txt.gz'
			self.txt_file = gzip.open(file_name, 'a')
			self.page += 1
			self.counter = 0

	def write_content(self, coords):
		for day in xrange(1, 31):
			d = date(self.year, self.month, day)
			min_date = datetime_to_timestamp(datetime.combine(d, time(0, 0)))
			max_date = datetime_to_timestamp(datetime.combine(d, time(23, 59)))
			try:
				result = api.media_search(count = 100, lat = coords[0], lng = coords[1], 
					distance = 637, min_timestamp = min_date, max_timestamp = max_date)
				for element in result:
					del element['images']
					del element['user_has_liked']
					del element['attribution']
					self.txt_file.write(simplejson.dumps(element))
					self.txt_file.write('\n')
				self.txt_file.flush()
				self.counter += 1
				self.check_count()
			except Exception as e:
				print str(e)
				sleep(50)


searcher = InstagramSearcher(2014, 6)

map_split = open("spb-0.5", "r")
next(map_split)
for line in map_split:
	coords = line.split(' ')[4:6]
	searcher.write_content(coords)

close(map_split)