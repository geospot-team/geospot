from time import sleep
from pymongo.errors import OperationFailure, PyMongoError
import Common


#connections = Common.read_connections_file("connectionsTo4sq.csv")

from pymongo import MongoClient

client = MongoClient('ec2-54-186-48-9.us-west-2.compute.amazonaws.com', 27017)
db = client['foursquare']
print db.collection_names()
#venues = db['collection_ids']
#venues_updates = db['venues_updates']
#venues.drop()
#venues_updates.drop()
#print venues.count()
#for venue in venues.find():
#    print venue
# for venue in venues_updates.find():
#     print venue

#ids = []
#for venue in venues.find({},{ '_id': 1 }):
#    ids.extend([str(venue['_id'])])

def __get_empty_updates_row(empty_period):
    d = dict()
    for name in Common.FIELDS_UPDATES:
        d[name] = empty_period
    return d


def __get_empty_period(count):
    empty_period = dict()
    for i in range(count):
        empty_period[str(i)] = 0
    return empty_period

print db.stats()
collection_ids = db['collection_ids']
print 'ids: ' + str(collection_ids.count())
print collection_ids.stats()
collection_full = db['collection_full']
print 'full: ' + str(collection_full.count())
print collection_full.stats()
collection_time_series = db['collection_time_series']
print 'time_series: ' + str(collection_time_series.count())
print collection_time_series.stats()
#collection_time_series.drop()
#for venue in collection_time_series.find():
#   print venue

i = 0
max_count = 400000
batch_size = 1000
period_size = 31
empty_period = __get_empty_period(period_size)
while i < max_count:
    insert_items = []
    for j in range(batch_size):
        insert_items.extend([{}])
        i += 1
    if (len(insert_items) != 0):
        try:
            inserted_ids = collection_time_series.insert(insert_items, continue_on_error=True)
            #print(len(inserted_ids))
            result = collection_time_series.update({'_id': {'$in': inserted_ids}},
                                                   {'$set':__get_empty_updates_row(empty_period)},
                                                   multi=True)
            # bulk = collection_time_series.initialize_unordered_bulk_op()
            # for item in insert_items:
            #     bulk.insert(item)
            # result = bulk.execute()
            print(result)
        except PyMongoError as exp:
            print('Unexpected error with mongo: {}\n Try add latter'.format(str(exp)))
            sleep(1)
    print(i)