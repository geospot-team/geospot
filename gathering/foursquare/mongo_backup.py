import gzip
import json
import logging
import os
import datetime
from time import sleep

import pymongo
import sys
from yandexwebdav import Config
import Common
import MultiProcessLogger


def dump(connection_string, database_name, collection_name, timestamp, logger, prefix=''):
    client = pymongo.MongoClient(connection_string)
    #mark by the start of the next week
    week_suffix = Common.get_week_suffix(timestamp - datetime.timedelta(days=1))
    collection_name = collection_name + "_" + week_suffix
    reader = client[database_name][collection_name]
    file_name = prefix + week_suffix + '.gz'
    with gzip.open(file_name, "w") as file:
        counter = 0
        for doc in reader.find():
            if not counter % 10000:
                logger.debug("{} {} {}".format(database_name, collection_name, counter))
            file.write(str(doc))
            file.write("\n")
            counter += 1

    # if drop:
    #     logger.info("Drop {} {}".format(database_name, collection_name))
    #     client[database_name].drop_collection(collection_name)

    client.close()
    return file_name


def load_to_yandex_disk(file_name, dest_path, login, password, logger):
    conf = Config({"user": login, "password": password})
    dest_path_temp = ""
    files_dest = None
    for dest_path_dir in dest_path.split("/"):
        dest_path_temp += dest_path_dir + "/"
        try:
            files_dest = conf.list(dest_path_temp)
        except Exception:
            conf.mkdir(dest_path_temp)
    while file_name not in files_dest:
        try:
            logger.info("Uploading {} to {}".format(file_name, dest_path))
            conf.upload(file_name, dest_path + file_name)
            files_dest = conf.list(dest_path_temp)
        except:
            time_to_sleep = 10
            logger.info("Sleep for {} and retry".format(time_to_sleep))
            sleep(time_to_sleep)
            pass
    logger.info("{} Uploaded {}".format(file_name, dest_path))


def dump_and_load(config, timestamp, logger):
    connection_string = config["mongodb"]["connection_string"]
    database_name = config["mongodb"]["database_name"]
    collection_full_name = config["mongodb"]["collection_full_name"]
    collection_time_series_name = config["mongodb"]["collection_time_series_name"]
    yandex_path = config["yandex_disk"]["dest_path"]
    yandex_login = config["yandex_disk"]["login"]
    yandex_password = config["yandex_disk"]["password"]

    file_name = dump(connection_string, database_name, collection_full_name, timestamp, logger, 'full_')
    logger.info("Backup {} done.".format(file_name))
    #load_to_yandex_disk(file_name, yandex_path + "full/", yandex_login, yandex_password, logger)
    #logger.info("Remove {}".format(file_name))
    #os.remove(file_name)

    file_name = dump(connection_string, database_name, collection_time_series_name, timestamp, logger, 'ts_')
    logger.info("Backup {} done.".format(file_name))
    #load_to_yandex_disk(file_name, yandex_path + "time_series/", yandex_login, yandex_password, logger)
    #logger.info("Remove {}".format(file_name))
    #os.remove(file_name)


def mongo_backup(config, timestamp, logger_queue):
    logger = MultiProcessLogger.get_logger("Main", logger_queue)
    dump_and_load(config, timestamp, logger)


if __name__ == "__main__":
    init_file = sys.argv[1]  # 'init.json'
    config = json.loads(open(init_file).read())
    timestamp = datetime.datetime.today()
    if len(sys.argv) > 2:
        timestamp = eval(sys.argv[2])
    logger_queue = Common.init_threaded_logger(config)
    mongo_backup(config, timestamp, logger_queue)