#!/usr/bin/python
# -*- coding: utf-8 -*-
import json

import logging
import multiprocessing
import sys
from datetime import date, datetime
import foursquare
import time
import Common
import MultiProcessLogger

logger = logging.getLogger(__name__)

class GetVenues:
    def __init__(self, config, categories, timestamp, logger, ids=None,
                 mongodb_config=None, auth_keys=None):
        if(config is None):
            self.mongodb_config = mongodb_config
            self.auth_keys = auth_keys
            self.max_threads_count = 1
        else:
            self.mongodb_config = config['mongodb']
            self.auth_keys = config['auth_keys']
            self.max_threads_count = config['max_threads_count']
        self.ids = ids
        self.categories = categories
        self.timestamp = timestamp
        self.venues_counter = 0
        self.logger = logger

    def start(self):
        self.__run_in_parallel(min([len(self.auth_keys)/2, self.max_threads_count]))

    def __run(self):
        self.connection_to_4sq = Common.ConnectionTo4sq(self.auth_keys)
        self.connection_to_storage = Common.MongodbStorage(self.mongodb_config, self.timestamp, self.logger)
        i = 0
        length = len(self.ids)
        keyboard_interrupt = False
        start_time = time.time()
        while(i < length):
            try:
                id = self.ids[i]
                row = self.connection_to_4sq.get_venue(id)['venue']
                Common.addCategory(row, self.categories)
                self.connection_to_storage.write(row)
                i+=1
            except KeyboardInterrupt:
                keyboard_interrupt = True
                break
            except:
                self.logger.error('Request ' + str(self.connection_to_4sq.requests_counter) +
                                  '. Unexpected error:' + str(sys.exc_info()[0]) + str(sys.exc_info()[1]))
        self.connection_to_storage.close()
        end_time = time.time()
        if keyboard_interrupt:
            self.logger.info('Program was interrupted. It took:' + str(end_time - start_time) + 'seconds')
        else:
            self.logger.info(
                'Program finished execution successfully. It took:' + str(end_time - start_time) + 'seconds')
        return (not keyboard_interrupt)

    def __run_in_parallel(self, threads_count):
        if(threads_count == 1):
            self.__run()
        else:
            #threads_count = 1
            queue = multiprocessing.Queue()
            ch = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)

            log_queue_reader = MultiProcessLogger.LogQueueReader(queue, [ch], logging.DEBUG)
            log_queue_reader.start()

            self.connection_to_storage = Common.MongodbStorage(self.mongodb_config, self.timestamp, self.logger)
            ids = self.connection_to_storage.get_ids()#(limit=10000)
            ids = self.__chunks(ids, len(ids)/threads_count)
            args = [(self.mongodb_config, ids[i], self.categories, self.timestamp,
                     self.auth_keys[2*i:2*i+2]) for i in range(threads_count)]
            pool = multiprocessing.Pool(threads_count, secondStepGrabber_init, [queue])
            logger.info("Starting " + str(threads_count) + " processes for second step...")
            result = pool.map(secondStepGrabber, args)

    def __chunks(self, l, n):
        result = []
        for i in xrange(0, len(l), n):
            result.extend([l[i:i+n]])
        return result


def secondStepGrabber_init(queue):
    secondStepGrabber.queue = queue

def secondStepGrabber(args):
    mongodb_config = args[0]
    ids = args[1]
    categories = args[2]
    timestamp = args[3]
    auth_keys = args[4]
    logger_level = logging.DEBUG#args[5]
    queue = secondStepGrabber.queue

    logger = logging.getLogger(__name__)
    MultiProcessLogger.init_logger(logger, logger_level, queue)
    get_venues = GetVenues(None, categories, timestamp, logger, ids, mongodb_config, auth_keys)
    get_venues.start()


if __name__ == "__main__":
    config = json.loads(open('init.json').read())
    logger = logging.getLogger(__name__)
    logger_level = config['logger']['level']
    if(logger_level == 'DEBUG'):
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    timestamp = datetime.utcnow().today()
    connectionTo4sq = Common.ConnectionTo4sq(config['auth_keys'])
    categoriesDict = Common.get_categories_dict_with_full_inheritance(connectionTo4sq.get_categories())

    search_venues = GetVenues(config, categoriesDict, timestamp, logger)
    search_venues.start()