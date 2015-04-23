#!/usr/bin/python
# -*- coding: utf-8 -*-
from Queue import Empty
import json
import logging
import multiprocessing
import sys
import threading
import traceback
import time
import datetime

import Common
import MultiProcessLogger
import subprocess


class GetVenuesThreaded:
    def __init__(self, logger_queue, writer_queue, task_queue):
        self.logger_queue = logger_queue
        self.writer_queue = writer_queue
        self.logger = MultiProcessLogger.get_logger("Get", logger_queue)
        self.task_queue = task_queue

    def run(self):
        self.logger.warn('Get started')
        i = 0
        while True:
            try:
                command, parameter = self.task_queue.get(timeout=10)
                if command == "die":
                    break
                self.logger.info("{} parameter. Left: {}".format(i, self.task_queue.qsize()))
                self.writer_queue.put(("write", None), timeout=10)
                time.sleep(1)
                i += 1
            except KeyboardInterrupt:
                break
            except Exception as e:
                self.logger.error('{} \n {}'.format(e.args, traceback.format_exc()))
        self.logger.warn('die')
        self.writer_queue.put(("die", None))


class GetIdsThreaded(threading.Thread):
    def __init__(self, queue, logger_queue, threads_count, timestamp):
        threading.Thread.__init__(self)
        self.queue = queue
        self.logger_queue = logger_queue
        self.daemon = True
        self.threads_count = threads_count
        self.timestamp = timestamp


    def run(self):
        logger = MultiProcessLogger.get_logger("GetIds", self.logger_queue)
        count = 10
        ids = [0]*count
        logger.warn('GetIds started')
        logger.warn("Venues count: {}.".format(count))
        counter = 0
        try:
            for item in ids:
                try:
                    self.queue.put(("get", item), timeout=2 * Common.GLOBAL_TIMEOUT)
                except (KeyboardInterrupt, SystemExit, EOFError):
                    break
                except Exception as e:
                    logger.error('{} \n {} \n {}'.format(e.args, item, traceback.format_exc()))
                counter += 1
                logger.info("Found {} items from {}".format(counter, count))
            for _ in range(self.threads_count):
                self.queue.put(("die", None), timeout=2 * Common.GLOBAL_TIMEOUT)
        except (KeyboardInterrupt, SystemExit, EOFError):
            pass
        except Exception as e:
            logger.error('{} \n {}'.format(e.args, traceback.format_exc()))
        logger.warn('GetIdsThreaded die')


def init_threaded_get_ids(logger_queue, threads_count, timestamp):
    task_queue = multiprocessing.Queue(10)
    task_thread = GetIdsThreaded(task_queue, logger_queue, threads_count, timestamp)
    task_thread.start()

    return task_queue


def second_step_grabber(args):
    logger_queue = second_step_grabber_init.logger_queue
    writer_queue = second_step_grabber_init.writer_queue
    task_queue = second_step_grabber_init.task_queue

    get_venues = GetVenuesThreaded(logger_queue, writer_queue, task_queue)
    get_venues.run()


def second_step_grabber_init(logger_queue, writer_queue, task_queue):
    second_step_grabber_init.logger_queue = logger_queue
    second_step_grabber_init.writer_queue = writer_queue
    second_step_grabber_init.task_queue = task_queue


def get_venues(timestamp, logger_queue):
    threads_count = 1

    logger = MultiProcessLogger.get_logger("Main", logger_queue)
    writer_queue = init_threaded_writer(logger_queue, threads_count)
    task_queue = init_threaded_get_ids(logger_queue, threads_count, timestamp)
    args = [i for i in range(threads_count)]

    pool = multiprocessing.Pool(threads_count, second_step_grabber_init, [logger_queue, writer_queue, task_queue])
    logger.info("Starting {} processes for second step...".format(threads_count))
    start_time = time.time()
    try:
        result = pool.map(second_step_grabber, args)
        time.sleep(10)
        pool.close()
        end_time = time.time()
        logger.warn('Program finished execution. It took: {} seconds'.format(end_time - start_time))
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

def init_threaded_writer(logger_queue, threads_count):
    writer_queue = multiprocessing.Queue(10)
    writer_threaded = WriterThreaded(writer_queue, logger_queue, threads_count)
    writer_threaded.start()

    return writer_queue

class WriterThreaded(threading.Thread):
    def __init__(self, queue, logger_queue, children_count):
        threading.Thread.__init__(self)
        self.queue = queue
        self.logger_queue = logger_queue
        self.daemon = True
        self.children_count = children_count

    def run(self):
        logger = MultiProcessLogger.get_logger("Writer", self.logger_queue)
        logger.warn('Writer started')
        while self.children_count:
            try:
                command, record = self.queue.get(timeout=10)
                if command == "write_ids":
                    time.sleep(1)
                elif command == "write":
                    time.sleep(1)
                elif command == "die":
                    time.sleep(1)
                    self.children_count -= 1
                else:
                    logger.warn("Unknown command: " + str(command))
            except (KeyboardInterrupt, SystemExit):
                raise
            except EOFError:
                break
            except Exception as e:
                logger.error('{} \n {}'.format(e.args, traceback.format_exc()))
        logger.warn('WriterThreaded die')


def collect(logger_queue, timestamp):
    get_venues(timestamp, logger_queue)


def cycled_collect(logger_queue):
    p = None
    last_timestamp = None
    while True:
        if p is not None:
            res = p.poll()
            if res is not None:
                time.sleep(5)
                continue
        timestamp = datetime.datetime.today()
        if last_timestamp is None or timestamp - datetime.timedelta(seconds=30) > last_timestamp:
            last_timestamp = timestamp
            #collect(logger_queue, timestamp)
            p = subprocess.Popen("python2.7 Test.py " + str(timestamp), shell=True)
        else:
            time.sleep(5) #sleep for 1 hour

def init_threaded_logger():
    logger_queue = multiprocessing.Queue()
    ch_c = logging.StreamHandler()
    ch_c.setFormatter(MultiProcessLogger.FORMATTER)
    ch_c.setLevel(logging.DEBUG)

    log_queue_reader = MultiProcessLogger.LogQueueReader(logger_queue, [ch_c])
    log_queue_reader.start()

    return logger_queue

if __name__ == "__main__":
    timestamp = datetime.datetime.today()
    print(isinstance(timestamp, datetime.datetime))
    print(isinstance(eval(repr(timestamp)), datetime.datetime))
    # if len(sys.argv) > 1:
    #     timestamp = sys.argv[1]
    #     logger_queue = init_threaded_logger()
    #     collect(logger_queue, timestamp)
    # else:
    #     cycled_collect(None)
