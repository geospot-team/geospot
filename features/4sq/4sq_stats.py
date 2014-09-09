import json
import sys
import time
import datetime

import pandas as pd
import pymongo

def make_geo_query(lon, lat, half_side):
    query = {"_geo": {
        "$within": {"$box": [[lon - half_side, lon + half_side], [lat - half_side, lat + half_side]]}}
    }
    return query

def get_names_with_day(names, day):
    return [name + "." + day for name in names]

def extract_names(item, names):
    extracted_features = []
    for name in names:
        sub_item = item
        sub_names = name.split('.')
        for sub_name in sub_names:
            if sub_item.has_key(sub_name):
                sub_item = sub_item[sub_name]
            else:
                sub_item = None
                break
        extracted_features.append(sub_item)
    return extracted_features

def extract_series(item, names):
    extracted_features = extract_names(item, names)
    return pd.Series(extracted_features,
                     index=names)

def calc_features(cursor, select_names, features_map):
    start_query_time = time.time()
    data = pd.DataFrame([extract_series(item, select_names) for item in cursor])
    print("Get items time: %s seconds" % (time.time() - start_query_time))
    features = [v(data) for k, v in features_map.iteritems()]
    names = [k for k, v in features_map.iteritems()]
    return pd.Series(features, index=names)

def point_stats(point, r, select_full_names, full_features_map, select_time_series_names=None, time_series_features_map=None):
    start_query_time = time.time()
    lat = point[0]
    lon = point[1]
    query = make_geo_query(lon, lat, r)

    #full
    cursor = reader_full.find(query, select_full_names)
    cursor.batch_size(10000)
    features = calc_features(cursor, select_full_names, full_features_map)

    # TODO: the same for time_series_features_map
    print("Stat time: %s seconds\n" % (time.time() - start_query_time))
    return features

#start features functions
def calc_obj_count(data_frame):
    return data_frame.shape[0]

#end features functions

#add functions to map for calculations
features_map_full = {'4sq_obj_count' : calc_obj_count}
features_map_time_series = []

#fields to ask about
select_full_names = ["createdAt"]
select_time_series_names = ["stats.checkins_count"]

start_time = time.time()
config = json.loads(open(sys.argv[1]).read())
client = pymongo.MongoClient(config["mongo"]["primary_node"])
reader_full = client[config["mongo"]["database"]][config["mongo"]["collection_full"]]
reader_time_series = client[config["mongo"]["database"]][config["mongo"]["collection_time_series"]]

out = config["result_file"]
r = config["search_radius"]*(0.1**6)
gps = config["gps"]
select_time_series_names_with_day = []

total_items = reader_full.count()
print("Total items: " + str(total_items))
result_arr = [point_stats(point, r, select_full_names, features_map_full) for point in gps]
result_index = [str(point) for point in gps]
result = pd.DataFrame(result_arr, index=result_index)
result.to_csv(out, sep="\t")
print("Working time: %s seconds" % (time.time() - start_time))



