#!/usr/bin/python
# -*- coding: utf-8 -*-
from Queue import Empty
import json
import multiprocessing
import sys
import traceback
import time
import Common
import MultiProcessLogger


class GetVenuesThreaded:
    def __init__(self, logger_queue, writer_queue, task_queue, auth_keys, categories):
        self.logger_queue = logger_queue
        self.logger = MultiProcessLogger.get_logger("Get", logger_queue)
        self.writer_queue = writer_queue
        self.auth_keys = auth_keys
        self.connection_to_4sq = Common.ConnectionTo4sq(self.auth_keys, self.logger)
        self.categories = categories
        self.task_queue = task_queue

    def run(self):
        i = 0
        while True:
            try:
                parameter = self.task_queue.get_nowait()
                if i % 100 == 0:
                    self.logger.info("{} parameter. Left: ".format(i, self.task_queue.qsize()))
                row = self.connection_to_4sq.get_venue(parameter)['venue']
                Common.addCategory(row, self.categories)
                self.writer_queue.put(("write", row))
                i += 1
            except Empty:
                break
            except KeyboardInterrupt:
                break
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.logger.error('Unexpected error ' + str(self.connection_to_4sq.requests_counter) + ':' +
                                  str(traceback.format_exception(exc_type, exc_value, exc_traceback)))
        self.writer_queue.put(("die", None))


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
        task_queue = multiprocessing.Queue()
        logger_queue = Common.init_threaded_logger(self.config)
        logger = MultiProcessLogger.get_logger("Main", logger_queue)
        writer_queue = Common.init_threaded_writer(self.config, logger_queue, threads_count)

        logger.info("Getting ids...")
        self.connection_to_storage = Common.MongodbStorage(self.config, None, logger)
        ids = self.connection_to_storage.get_ids()#limit=10)
        logger.info("Found {} object ids.".format(len(ids)))
        for param in ids:
            task_queue.put_nowait(param)
        args = [(self.categories, self.auth_keys[2 * i:2 * i + 2]) for i in range(threads_count)]

        pool = multiprocessing.Pool(threads_count, second_step_grabber_init, [logger_queue, writer_queue, task_queue])
        logger.info("Starting {} processes for second step...".format(threads_count))
        start_time = time.time()
        try:
            result = pool.map(second_step_grabber, args)
        except KeyboardInterrupt:
            while not task_queue.empty():
                try:
                    task_queue.get_nowait()
                except Empty:
                    pass
        end_time = time.time()
        logger.info('Program finished execution. It took: {} seconds'.format(end_time - start_time))


def second_step_grabber(args):
    categories = args[0]
    auth_keys = args[1]
    logger_queue = second_step_grabber_init.logger_queue
    writer_queue = second_step_grabber_init.writer_queue
    task_queue = second_step_grabber_init.task_queue

    get_venues = GetVenuesThreaded(logger_queue, writer_queue, task_queue, auth_keys, categories)
    get_venues.run()


def second_step_grabber_init(logger_queue, writer_queue, task_queue):
    second_step_grabber_init.logger_queue = logger_queue
    second_step_grabber_init.writer_queue = writer_queue
    second_step_grabber_init.task_queue = task_queue


if __name__ == "__main__":
    init_file = sys.argv[1]  # 'init.json'
    config = json.loads(open(init_file).read())
    connectionTo4sq = Common.ConnectionTo4sq(config['auth_keys'], None)
    categoriesDict = Common.get_categories_dict_with_full_inheritance(connectionTo4sq.get_categories())

    search_venues = GetVenues(config, categoriesDict)
    search_venues.start()