#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import multiprocessing
import sys
import traceback
import time
import Common
import MultiProcessLogger


class GetVenuesThreaded:
    def __init__(self, logger_queue, writer_queue, auth_keys, categories, ids):
        self.logger_queue = logger_queue
        self.logger = MultiProcessLogger.get_logger("Get", logger_queue)
        self.writer_queue = writer_queue
        self.auth_keys = auth_keys
        self.connection_to_4sq = Common.ConnectionTo4sq(self.auth_keys, self.logger)
        self.categories = categories
        self.ids = ids

    def run(self):
        keyboard_interrupt = False
        start_time = time.time()
        for i in range(len(self.ids)):
            try:
                self.logger.info("{} out of {}".format(i, len(self.ids)))
                id = self.ids[i]
                row = self.connection_to_4sq.get_venue(id)['venue']
                Common.addCategory(row, self.categories)
                self.writer_queue.put(("write", row))
            except KeyboardInterrupt:
                keyboard_interrupt = True
                break
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.logger.error('Unexpected error ' + str(self.connection_to_4sq.requests_counter) + ':' +
                                  str(traceback.format_exception(exc_type, exc_value, exc_traceback)))
        self.writer_queue.put(("die", None))
        end_time = time.time()
        if keyboard_interrupt:
            self.logger.info('Thread was interrupted. It took: {} seconds'.format(end_time - start_time))
        else:
            self.logger.info(
                'Thread finished execution successfully. It took: {} seconds'.format(end_time - start_time))
        return not keyboard_interrupt


class GetVenues:
    def __init__(self, config, categories):
        self.config = config
        self.auth_keys = config['auth_keys']
        self.storage_type = config['steps']['get_venues']['storage_type']
        self.max_threads_count = config['max_threads_count']
        self.categories = categories

    def start(self):
        self.__run_in_parallel(min([len(self.auth_keys) / 2, self.max_threads_count]))

    def __run_in_parallel(self, threads_count):
        logger_queue = Common.init_threaded_logger(self.config)
        logger = MultiProcessLogger.get_logger("Main", logger_queue)
        writer_queue = Common.init_threaded_writer(self.config, logger_queue, threads_count)

        logger.info("Getting ids...")
        self.connection_to_storage = Common.MongodbStorage(self.config, None, logger)
        ids = self.connection_to_storage.get_ids()#limit=10)
        logger.info("Found {} object ids.".format(len(ids)))
        ids = self.__chunks(ids, len(ids) / threads_count)
        args = [(ids[i], self.categories, self.auth_keys[2 * i:2 * i + 2]) for i in range(threads_count)]

        pool = multiprocessing.Pool(threads_count, second_step_grabber_init, [logger_queue, writer_queue])
        logger.info("Starting {} processes for second step...".format(threads_count))
        start_time = time.time()
        result = pool.map(second_step_grabber, args)
        end_time = time.time()
        logger.info('Program finished execution. It took: {} seconds'.format(end_time - start_time))

    def __chunks(self, l, n):
        result = []
        for i in xrange(0, len(l), n):
            result.extend([l[i:i + n]])
        return result


def second_step_grabber(args):
    ids = args[0]
    categories = args[1]
    auth_keys = args[2]
    logger_queue = second_step_grabber_init.logger_queue
    writer_queue = second_step_grabber_init.writer_queue

    get_venues = GetVenuesThreaded(logger_queue, writer_queue, auth_keys, categories, ids)
    get_venues.run()


def second_step_grabber_init(logger_queue, writer_queue):
    second_step_grabber_init.logger_queue = logger_queue
    second_step_grabber_init.writer_queue = writer_queue


if __name__ == "__main__":
    init_file = sys.argv[1]  # 'init.json'
    config = json.loads(open(init_file).read())
    connectionTo4sq = Common.ConnectionTo4sq(config['auth_keys'], None)
    categoriesDict = Common.get_categories_dict_with_full_inheritance(connectionTo4sq.get_categories())

    search_venues = GetVenues(config, categoriesDict)
    search_venues.start()