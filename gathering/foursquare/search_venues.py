from Queue import Empty
import json
import multiprocessing
import traceback
import sys
import time
import io
import datetime
import Common
import MultiProcessLogger
import mongo_backup


class SearchVenuesThreaded:
    def __init__(self, logger_queue, writer_queue, task_queue, auth_keys, categories):
        self.logger_queue = logger_queue
        self.logger = MultiProcessLogger.get_logger("Search", logger_queue)
        self.writer_queue = writer_queue
        self.task_queue = task_queue
        self.auth_keys = auth_keys
        self.connection_to_4sq = Common.ConnectionTo4sq(self.auth_keys, self.logger)
        self.categories = categories

    def run(self):
        i = 0
        while True:
            try:
                parameter = self.task_queue.get_nowait()
                self.logger.info("{} parameter. Left: {}".format(i, self.task_queue.qsize()))

                start_sub_area = time.time()
                self.search_in_area(parameter)
                end_sub_area = time.time()

                self.logger.info('Area: {}...Done. It took {} seconds. Total requests count: {}'.
                                 format(parameter,
                                        end_sub_area - start_sub_area,
                                        self.connection_to_4sq.requests_counter))
                i += 1
            except Empty:
                break
            except KeyboardInterrupt:
                break
            except Exception as e:
                self.logger.error('{} \n {}'.format(e.args, traceback.format_exc()))
        self.logger.info('die')
        self.writer_queue.put(("die", None))

    def search_in_area(self, search_parameter):
        rows = self.connection_to_4sq.search(search_parameter)['venues']
        length = len(rows)
        save = True
        if length == 50:
            save = False
            if ((search_parameter.northPoint - search_parameter.southPoint) < 0.001 and
                        (search_parameter.westPoint - search_parameter.eastPoint) < 0.001):
                self.logger.warn('Deepest params: {}'.format(search_parameter))
                save = True
            else:
                self.logger.debug('Too many objects in area {} {} of 50 limit'.format(search_parameter, length))
                new_parameters = search_parameter.split(True)
                new_parameters = [x for param in new_parameters for x in param.split(False)]
                for param in new_parameters:
                    self.search_in_area(param)
        if save:
            for row in rows:
                Common.addCategory(row, self.categories)
                self.writer_queue.put(("write_ids", row))


def firstStepGrabber(args):
    auth_keys = args[0]
    categories = args[1]
    logger_queue = first_step_grabber_init.logger_queue
    writer_queue = first_step_grabber_init.writer_queue
    task_queue = first_step_grabber_init.task_queue
    search_venues = SearchVenuesThreaded(logger_queue, writer_queue, task_queue, auth_keys, categories)
    search_venues.run()


def first_step_grabber_init(logger_queue, writer_queue, task_queue):
    first_step_grabber_init.logger_queue = logger_queue
    first_step_grabber_init.writer_queue = writer_queue
    first_step_grabber_init.task_queue = task_queue


def search_venues(config, timestamp, logger_queue):
    auth_keys = config['auth_keys']
    search_parameter = Common.SearchParameter(config['steps']['search_venues'])
    batch_size = config['steps']['search_venues']['batch_size']
    max_threads_count = config['max_threads_count']
    connectionTo4sq = Common.ConnectionTo4sq(config['auth_keys'], None)
    categories = Common.get_categories_dict_with_full_inheritance(connectionTo4sq.get_categories())
    auth_per_thread = config["auth_per_thread"]
    threads_count = min([len(auth_keys) / auth_per_thread, max_threads_count])

    start_time = time.time()
    task_queue = multiprocessing.Queue()
    logger = MultiProcessLogger.get_logger("Main", logger_queue)
    try:
        writer_queue = Common.init_threaded_writer(config, logger_queue, threads_count, batch_size, timestamp)

        split_rate = 32
        search_parameters = search_parameter.split(True, split_rate)
        search_parameters = [x for param in search_parameters for x in param.split(False, split_rate)]
        logger.warn("Search parameters count: {}.".format(len(search_parameters)))
        for param in search_parameters:
            task_queue.put_nowait(param)
        # search_parameters = self.search_parameter.split(True, threads_count)

        args = [(auth_keys[auth_per_thread * i:auth_per_thread * i + auth_per_thread], categories) for i in
                range(threads_count)]

        pool = multiprocessing.Pool(threads_count, first_step_grabber_init,
                                    [logger_queue, writer_queue, task_queue])
        logger.info("Starting {} processes for first step...".format(threads_count))
        try:
            result = pool.map(firstStepGrabber, args)
        except KeyboardInterrupt:
            while not task_queue.empty():
                try:
                    task_queue.get_nowait()
                except Empty:
                    pass
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error('{} \n {}'.format(e.args, traceback.format_exc()))

    end_time = time.time()
    logger.warn('Program finished execution. It took: {} seconds'.format(end_time - start_time))


if __name__ == "__main__":
    init_file = sys.argv[1]
    config = json.loads(open(init_file).read())
    logger_queue = Common.init_threaded_logger(config)
    search_venues(config, datetime.datetime.today(), logger_queue)