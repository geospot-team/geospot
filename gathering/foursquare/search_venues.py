from Queue import Empty
import json
import multiprocessing
import traceback
import sys
import time
import Common
import MultiProcessLogger


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
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.logger.error('Unexpected error ' + str(self.connection_to_4sq.requests_counter) + ':' +
                                  str(traceback.format_exception(exc_type, exc_value, exc_traceback)))
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


class SearchVenues:
    def __init__(self, config, categories):
        self.config = config
        self.auth_keys = config['auth_keys']
        self.search_parameter = Common.SearchParameter(config['steps']['search_venues'])
        self.max_threads_count = config['max_threads_count']
        self.categories = categories

    def start(self):
        self.__run_in_parallel(min([len(self.auth_keys) / 2, self.max_threads_count]))

    def __run_in_parallel(self, threads_count):
        task_queue = multiprocessing.Queue()
        logger_queue = Common.init_threaded_logger(self.config)
        logger = MultiProcessLogger.get_logger("Main", logger_queue)
        writer_queue = Common.init_threaded_writer(self.config, logger_queue, threads_count)

        split_rate = 32
        search_parameters = self.search_parameter.split(True, split_rate)
        search_parameters = [x for param in search_parameters for x in param.split(False, split_rate)]
        logger.info("Search parameters count: {}.".format(len(search_parameters)))
        for param in search_parameters:
            task_queue.put_nowait(param)
        #search_parameters = self.search_parameter.split(True, threads_count)

        args = [(self.auth_keys[2 * i:2 * i + 2], self.categories) for i in range(threads_count)]

        pool = multiprocessing.Pool(threads_count, first_step_grabber_init, [logger_queue, writer_queue, task_queue])
        logger.info("Starting {} processes for first step...".format(threads_count))
        start_time = time.time()
        try:
            result = pool.map(firstStepGrabber, args)
        except KeyboardInterrupt:
            while not task_queue.empty():
                try:
                    task_queue.get_nowait()
                except Empty:
                    pass
        end_time = time.time()
        logger.info('Program finished execution. It took: {} seconds'.format(end_time - start_time))


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





if __name__ == "__main__":
    # MultiProcessLogger.configure_loggers()
    init_file = sys.argv[1]  # 'init.json'
    config = json.loads(open(init_file).read())
    connectionTo4sq = Common.ConnectionTo4sq(config['auth_keys'], None)
    categories_dict = Common.get_categories_dict_with_full_inheritance(connectionTo4sq.get_categories())

    search_venues = SearchVenues(config, categories_dict)
    search_venues.start()
    # outputFile, northPoint, eastPoint, southPoint, westPoint, vStep, \
    # hStep, limit = Common.parseArgs(sys.argv)
    # searchParameters = Common.SearchParameter(northPoint, eastPoint, southPoint, westPoint, vStep, hStep, limit)
    # connectionsTo4sq = Common.read_connections_file("connectionsTo4sq.csv")
    # connection = connectionsTo4sq[0]
    # writer = Common.CSVWriter(Common.FIELDS, outputFile)
    # writer = Common.DB_Mongodb_Writer(Common.FIELDS, 'mongodb://ec2-54-186-48-9.us-west-2.compute.amazonaws.com:27017/',
    #                                   True)
    # exploreArea(writer, connection, searchParameters)
