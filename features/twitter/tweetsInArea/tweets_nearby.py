__author__ = 'Vasily'
import gzip
import json
import sys
import time
import datetime

import pymongo


start_time = time.time()

config = json.loads(open(sys.argv[1]).read())

client = pymongo.MongoClient(config["mongo"]["primary_node"])
reader = client[config["mongo"]["database"]][config["mongo"]["collection"]]

out = config["result_file"]
# out = gzip.open(out, "w")
out = open(out, "w")
# extraction config alias
R = config["search_radius"]
gps = config["gps"]

#
out.write("lat\tlon\tcounter\n")

total_tweets = reader.count()
threshold = int(total_tweets * config["robots_threshold"])
print("threshold: {}".format(threshold))
robots_threshold_reader = reader.find().sort("{\"user.followers_count\": -1}").limit(threshold).skip(threshold - 1)
robots_threshold = robots_threshold_reader[0]["user"]["followers_count"]
print("robots threshold: {}".format(robots_threshold))


def make_geo_query(lon, lat, from_date=config["start_date"], end_date=config["end_date"], min_distance=0,
                   max_distance=R):
    query = {"geo": {
         "$near": {"$geometry": {"type": "Point", "coordinates": [lon, lat]}, "$minDistance": min_distance,
                                "$maxDistance": max_distance}},
             "created_at": {"$lte":  datetime.datetime.fromtimestamp(end_date), "$gte":  datetime.datetime.fromtimestamp(from_date)},
             "user.followers_count": {"$lte": robots_threshold}
    }
    # query = "{{geo:{{" \
    # "$near : {{ $geometry: " \
    #         "{{type : \"Point\", coordinates: [ {lon},{lat}]}}, " \
    #         "$minDistance: {minDist}, $maxDistance: {maxDist}}}}}," \
    #         "created_at: {{ $lte: {to_date}, $gte : {from_date}}}," \
    #         "\"user.followers_count\":{{ $lte: {robots_threshold}}}}}".format(lon=lon, lat=lat, minDist=min_distance,
    #                                                                          maxDist=max_distance, to_date=end_date,
    #                                                                          from_date=from_date,
    #                                                                          robots_threshold=robots_threshold)
    return query


for coord in gps:
    lat = coord[0]
    lon = coord[1]
    query = make_geo_query(lon, lat)
    result = reader.find(query).count()
    out.write("{lon}\t{lat}\t{stat}\n".format(lon=lon, lat=lat, stat=result))
    out.flush()

out.close()

print("Working time: %s seconds" % (time.time() - start_time))
