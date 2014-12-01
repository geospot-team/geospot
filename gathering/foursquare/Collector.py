import json
import sys
import datetime

import Common
import get_venues
import mongo_backup
import search_venues


if __name__ == "__main__":
    init_file = sys.argv[1]
    config = json.loads(open(init_file).read())
    logger_queue = Common.init_threaded_logger(config)
    timestamp = datetime.datetime.today()
    collect_type = sys.argv[2]


    if collect_type == "full":
        get_venues.get_venues(config, timestamp, logger_queue)
        mongo_backup.mongo_backup(config, timestamp, logger_queue)
        search_venues.search_venues(config, timestamp, logger_queue)
    elif collect_type == "get":
        get_venues.get_venues(config, timestamp, logger_queue)
    elif collect_type == "search":
        search_venues.search_venues(config, timestamp, logger_queue)
    elif collect_type == "mongo_backup":
        mongo_backup.mongo_backup(config, timestamp, logger_queue)