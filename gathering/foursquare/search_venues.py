import json
import multiprocessing
from datetime import date
import random
import traceback
import foursquare
import logging
import sys
import time
import Common
import MultiProcessLogger

logger = logging.getLogger(__name__)


class SearchVenues:
    def __init__(self, config, timestamp, categories, logger,
                 mongodb_config=None, auth_keys=None, search_parameter=None):
        if(config is None):
            self.mongodb_config = mongodb_config
            self.auth_keys = auth_keys
            self.search_parameter = search_parameter
            self.max_threads_count = 1
        else:
            self.mongodb_config = config
            self.auth_keys = config['auth_keys']
            self.search_parameter = Common.SearchParameter(config['steps']['search_venues'])
            self.max_threads_count = config['max_threads_count']
        self.categories = categories
        self.venues_counter = 0
        self.logger = logger
        self.timestamp = timestamp

    def start(self):
        self.__run_in_parallel(min([len(self.auth_keys)/2, self.max_threads_count]))

    def __run(self):
        self.connection_to_4sq = Common.ConnectionTo4sq(self.auth_keys)
        self.connection_to_storage = Common.MongodbStorage(self.mongodb_config, self.timestamp, self.logger)

        parameters = self.search_parameter.split(True)
        parameters = [x for param in parameters for x in param.split(False)]
        i = 0
        length = len(parameters)
        keyboard_interrupt = False
        start_time = time.time()
        while (i < length):
            try:
                self.logger.info(str(i) + ' out of ' + str(length))
                start_sub_area = time.time()
                param = parameters[i]

                self.__search_in_area(param)

                end_sub_area = time.time()
                self.logger.debug('Area:{' + str(param) + '}...Done. It took ' + str(
                    end_sub_area - start_sub_area) + ' seconds. ' +
                                 'Total requests count:' + str(self.connection_to_4sq.requests_counter))
                i += 1
            except KeyboardInterrupt:
                keyboard_interrupt = True
                break
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self.logger.error('Unexpected error ' + str(self.connection_to_4sq.requests_counter) + ':' +
                                  str(traceback.format_exception(exc_type, exc_value, exc_traceback)))
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
            #r = random.random()
            #time.sleep(10*r)
            self.__run()
        else:
            #threads_count = 1
            queue = multiprocessing.Queue()
            ch_c = logging.StreamHandler()
            ch_c.setFormatter(MultiProcessLogger.formatter)

            ch_e = MultiProcessLogger.EmailLogHandler("deemonasd", "xxskdultsdkfzdju",
                                                    "deemonasd@gmail.com", "deemonasd@gmail.com")
            ch_e.setFormatter(MultiProcessLogger.formatter)
            ch_e.setLevel(logging.INFO)

            log_queue_reader = MultiProcessLogger.LogQueueReader(queue, [ch_c, ch_e], self.logger.level)
            log_queue_reader.start()

            search_parameters = self.search_parameter.split(True, threads_count)
            args = [(self.mongodb_config, search_parameters[i], self.auth_keys[2*i:2*i+2],
                    self.timestamp, self.categories, self.logger.level) for i in range(threads_count)]
            pool = multiprocessing.Pool(threads_count, firstStepGrabber_init, [queue])
            logger.info("Starting " + str(threads_count) + " processes for first step...")
            result = pool.map(firstStepGrabber, args)

    def __search_in_area(self, search_parameter):
        rows = self.connection_to_4sq.search(search_parameter)['venues']
        length = len(rows)
        save = True
        if (length == 50):
            save = False
            if ((search_parameter.northPoint - search_parameter.southPoint) < 0.001 and
                        (search_parameter.westPoint - search_parameter.eastPoint) < 0.001):
                self.logger.warn('Deepest params: ' + str(search_parameter))
                save = True
            else:
                logger.debug(
                    'Too many objects in area {' + str(search_parameter) + '}' + str(length) + 'of 50 limit')
                new_parameters = search_parameter.split(True)
                new_parameters = [x for param in new_parameters for x in param.split(False)]
                for param in new_parameters:
                    logger.debug('Go deeper: {' + str(param) + '}')
                    self.__search_in_area(param)
        if (save):
            for row in rows:
                Common.addCategory(row, self.categories)
                self.connection_to_storage.write_ids(row)


def firstStepGrabber( args):
    mongodb_config = args[0]
    searchParameter = args[1]
    auth_keys = args[2]
    timestamp = args[3]
    categories = args[4]
    logger_level = args[5]
    queue = firstStepGrabber_init.queue
    logger = logging.getLogger(__name__)
    MultiProcessLogger.init_logger(logger, logger_level, queue)
    search_venues = SearchVenues(None, timestamp, categories, logger,
                 mongodb_config, auth_keys, searchParameter)
    search_venues.start()

def firstStepGrabber_init(queue):
    firstStepGrabber_init.queue = queue

if __name__ == "__main__":
    #MultiProcessLogger.configure_loggers()
    init_file = sys.argv[1]#'init.json'
    config = json.loads(open(init_file).read())
    logger = logging.getLogger(__name__)
    ch = logging.StreamHandler()
    ch.setFormatter(MultiProcessLogger.formatter)
    logger.addHandler(ch)
    logger_level = config['logger']['level']
    if(logger_level == 'DEBUG'):
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    timestamp = date.today()
    connectionTo4sq = Common.ConnectionTo4sq(config['auth_keys'])
    categoriesDict = Common.get_categories_dict_with_full_inheritance(connectionTo4sq.get_categories())

    search_venues = SearchVenues(config, timestamp, categoriesDict, logger)
    search_venues.start()
    # outputFile, northPoint, eastPoint, southPoint, westPoint, vStep, \
    # hStep, limit = Common.parseArgs(sys.argv)
    # searchParameters = Common.SearchParameter(northPoint, eastPoint, southPoint, westPoint, vStep, hStep, limit)
    # connectionsTo4sq = Common.read_connections_file("connectionsTo4sq.csv")
    # connection = connectionsTo4sq[0]
    #writer = Common.CSVWriter(Common.FIELDS, outputFile)
    # writer = Common.DB_Mongodb_Writer(Common.FIELDS, 'mongodb://ec2-54-186-48-9.us-west-2.compute.amazonaws.com:27017/',
    #                                   True)
    # exploreArea(writer, connection, searchParameters)
