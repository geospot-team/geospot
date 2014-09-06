__author__ = 'Vasily'
import json
import sys
import time
import datetime

import pandas as pd
import pymongo


start_time = time.time()

config = json.loads(open(sys.argv[1]).read())

client = pymongo.MongoClient(config["mongo"]["primary_node"])
reader = client[config["mongo"]["database"]][config["mongo"]["collection"]]

out = config["result_file"]
# out = gzip.open(out, "w")
# out = open(out, "w")
# extraction config alias
R = config["search_radius"]
gps = config["gps"]

#
# out.write("lat\tlon\tcounter\n")

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
             "created_at": {"$lte": datetime.datetime.fromtimestamp(end_date),
                            "$gte": datetime.datetime.fromtimestamp(from_date)},
             "user.followers_count": {"$lte": robots_threshold}
    }
    return query


def timestamp_filter(timestamp, config):
    return config["start"] <= timestamp.hour < config["end"]


def source_filter(source, config):
    return source in config


def extract_tweet_info(tweet):
    return pd.Series([tweet["user"]["id"], tweet["source"], tweet["created_at"]],
                     index=["user", "source", "created_at"])


def calc_features(cursor):
    start_query_time = time.time()
    data = pd.DataFrame([extract_tweet_info(tweet) for tweet in cursor])
    print("Get tweets time: %s seconds" % (time.time() - start_query_time))
    features = []
    names = []

    time_intervals = {"night": {"start": 0, "end": 6}, "morning": {"start": 6, "end": 10},
                      "day": {"start": 10, "end": 19}, "evening": {"start": 19, "end": 25},
                      "all": {"start": 0, "end": 25}}
    sources = {"Apple": {"iPhone", "iPad"}, "Android": {"Android"},
               "All": {"Web", "iPhone", "iPad", "Android", "instagram", "foursquare", "other"},
               "4sq": {"foursquare"},
               "instagram": {"instagram"},
               "Mobile": {"iPhone", "iPad", "Android"},
               "Social": {"foursquare", "instagram"}
    }

    for time_interval, time_interval_config in time_intervals.iteritems():
        df_time = pd.DataFrame()
        if data.shape[0] != 0:
            df_time = data[
                data["created_at"].apply(lambda timestamp: timestamp_filter(timestamp, time_interval_config))]
        for source, source_config in sources.iteritems():
            df = pd.DataFrame()
            if df_time.shape[0] != 0:
                df = df_time[df_time["source"].apply(lambda src: source_filter(src, source_config))]
            features.append(df.shape[0])
            names.append("{}_{}".format(time_interval, source))
            if df.shape[0] != 0:
                features.append(len(df["user"].unique()))
            else:
                features.append(0)
            names.append("unique_users_{}_{}".format(time_interval, source))
    return pd.Series(features, index=names)


def point_stats(point):
    start_query_time = time.time()
    lat = point[0]
    lon = point[1]
    query = make_geo_query(lon, lat)
    cursor = reader.find(query)
    cursor.batch_size(10000)
    features = calc_features(cursor)
    # print(features)
    print("Stat time: %s seconds\n" % (time.time() - start_query_time))
    return features


result_arr = [point_stats(point) for point in gps]
result_index = [str(point) for point in gps]
result = pd.DataFrame(result_arr, index=result_index)
result.to_csv(out,sep="\t")
print("Working time: %s seconds" % (time.time() - start_time))



