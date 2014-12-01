#!/usr/bin/python
# -*- coding: utf-8 -*-
from Queue import Empty
import json
import multiprocessing
import sys
import threading
import traceback
import time
import datetime

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
                command, parameter = self.task_queue.get(timeout=2 * Common.GLOBAL_TIMEOUT)
                if command == "die":
                    break
                if i % 100 == 0:
                    self.logger.info("{} parameter. Left: {}".format(i, self.task_queue.qsize()))
                row = self.connection_to_4sq.get_venue(parameter)['venue']
                Common.addCategory(row, self.categories)
                self.writer_queue.put(("write", row), timeout=2 * Common.GLOBAL_TIMEOUT)
                i += 1
            except KeyboardInterrupt:
                break
            except Exception as e:
                self.logger.error('{} \n {}'.format(e.args, traceback.format_exc()))
        self.logger.warn('die')
        self.writer_queue.put(("die", None))


class GetIdsThreaded(threading.Thread):
    def __init__(self, queue, config, batch_size, logger_queue, threads_count, timestamp):
        threading.Thread.__init__(self)
        self.queue = queue
        self.logger_queue = logger_queue
        self.config = config
        self.daemon = True
        self.batch_size = batch_size
        self.threads_count = threads_count
        self.timestamp = timestamp
        # if logger:
        # logger.info("Getting ids...")
        # writer = Common.get_writer(self.config, logger)
        # self.ids = writer.get_ids(limit=100)
        # if logger:
        # logger.info("Found {} object ids.".format(len(self.ids)))


    def run(self):
        logger = MultiProcessLogger.get_logger("GetIds", self.logger_queue)
        writer = Common.get_writer(self.config, self.batch_size, self.timestamp, logger)
        count, ids = writer.get_ids_iter()
        logger.warn("Venues count: {}.".format(count))
        # logger.info("Getting ids...")
        # self.ids = writer.get_ids()#limit=100)
        #logger.info("Found {} object ids.".format(len(self.ids)))
        counter = 0
        try:
            for item in ids:
                try:
                    self.queue.put(("get", item['_id']), timeout=2 * Common.GLOBAL_TIMEOUT)
                except (KeyboardInterrupt, SystemExit, EOFError):
                    break
                except Exception as e:
                    logger.error('{} \n {} \n {}'.format(e.args, item['_id'], traceback.format_exc()))
                counter += 1
                if counter % self.batch_size == 0:
                    logger.info("Found {} items from {}".format(counter, count))
            for _ in range(self.threads_count):
                self.queue.put(("die", None), timeout=2 * Common.GLOBAL_TIMEOUT)
        except (KeyboardInterrupt, SystemExit, EOFError):
            pass
        except Exception as e:
            logger.error('{} \n {}'.format(e.args, traceback.format_exc()))
        finally:
            ids.close()
        logger.info('GetIdsThreaded die')


def init_threaded_get_ids(config, batch_size, logger_queue, threads_count, timestamp):
    task_queue = multiprocessing.Queue(batch_size * 2)
    task_thread = GetIdsThreaded(task_queue, config, batch_size, logger_queue, threads_count, timestamp)
    task_thread.start()

    return task_queue


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


def get_venues(config, timestamp, logger_queue):
    connectionTo4sq = Common.ConnectionTo4sq(config['auth_keys'], None)
    categories = Common.get_categories_dict_with_full_inheritance(connectionTo4sq.get_categories())
    auth_keys = config['auth_keys']
    max_threads_count = config['max_threads_count']
    batch_size = config['steps']['get_venues']['batch_size']
    auth_per_thread = config["auth_per_thread"]
    threads_count = min([len(auth_keys) / auth_per_thread, max_threads_count])

    logger = MultiProcessLogger.get_logger("Main", logger_queue)
    writer_queue = Common.init_threaded_writer(config, logger_queue, threads_count, batch_size, timestamp)
    task_queue = init_threaded_get_ids(config, batch_size, logger_queue, threads_count, timestamp)
    args = [(categories, auth_keys[auth_per_thread * i:auth_per_thread * i + auth_per_thread]) for i in
            range(threads_count)]

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
    except Exception as e:
        logger.error('{} \n {}'.format(e.args, traceback.format_exc()))
    for _ in range(threads_count):
        try:
            writer_queue.put(("die", None))
        except Empty:
            pass
    end_time = time.time()
    logger.warn('Program finished execution. It took: {} seconds'.format(end_time - start_time))


if __name__ == "__main__":
    init_file = sys.argv[1]  # 'init.json'
    config = json.loads(open(init_file).read())
    logger_queue = Common.init_threaded_logger(config)
    get_venues(config, datetime.datetime.today(), logger_queue)