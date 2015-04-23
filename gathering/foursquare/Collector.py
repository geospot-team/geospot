import json
import sys
import datetime
import subprocess

import Common

from time import sleep
import MultiProcessLogger


def run_process_sync(process, logger):
    logger.warn("Run process: " + process)
    subprocess.call(process, shell=True)
    #p = subprocess.Popen(process, shell=True)
    #sleep(10)
    #while p.poll() is not None:
    #    sleep(10)



def collect(init_file, timestamp, collect_type, logger):
    if collect_type == "full":
        run_process_sync("python get_venues.py " + init_file + " \"" + repr(timestamp) + "\" False", logger)
        run_process_sync("python mongo_backup.py " + init_file + " \"" + repr(timestamp) + "\"", logger)
        run_process_sync("python search_venues.py " + init_file + " \"" + repr(timestamp) + "\"", logger)
    elif collect_type == "get":
        run_process_sync("python get_venues.py " + init_file + " \"" + repr(timestamp) + "\" True", logger)
    elif collect_type == "get_ts_only":
        run_process_sync("python get_venues.py " + init_file + " \"" + repr(timestamp) + "\" False", logger)
    elif collect_type == "search":
        run_process_sync("python search_venues.py " + init_file + " \"" + repr(timestamp) + "\"", logger)
    elif collect_type == "mongo_backup":
        run_process_sync("python mongo_backup.py " + init_file + " \"" + repr(timestamp) + "\"", logger)

def get_collect_type(current_day):
    weekday = current_day.weekday()
    if weekday == 0: #Monday
        return "get"
    elif weekday == 6:
        return "full"
    else:
        return "get_ts_only"

def cycled_collect(init_file, logger):
    last_timestamp = None
    while True:
        timestamp = datetime.datetime.today()
        if last_timestamp is None or timestamp.date() - datetime.timedelta(days=1) >= last_timestamp.date():
            last_timestamp = timestamp
            collect(init_file, timestamp, get_collect_type(timestamp), logger)
        else:
            logger.info("Sleep: timestamp: " + str(timestamp.date()) +
                        ", target timestamp: " + str(last_timestamp.date() + datetime.timedelta(days=1)))
            sleep(60*60) #sleep for 1 hour


if __name__ == "__main__":
    init_file = sys.argv[1]
    config = json.loads(open(init_file).read())
    logger_queue = Common.init_threaded_logger(config)
    logger = MultiProcessLogger.get_logger("Main", logger_queue)
    if len(sys.argv) == 3:
        timestamp = datetime.datetime.today()
        collect_type = sys.argv[2]
        collect(init_file, timestamp, collect_type, logger)
    else:
        cycled_collect(init_file, logger)

if __name__ == "__main__test":
    init_file = sys.argv[1]
    config = json.loads(open(init_file).read())
    logger_queue = Common.init_threaded_logger(config)
    logger = MultiProcessLogger.get_logger("Main", logger_queue)
    if len(sys.argv) == 3:
        print("Started " + sys.argv[2])
        sleep(60)
        print("Ended " + sys.argv[2])
    else:
        run_process_sync("python Collector.py " + init_file + " 1", logger)
        run_process_sync("python Collector.py " + init_file + " 2", logger)
        run_process_sync("python Collector.py " + init_file + " 3", logger)