import csv

import getopt
import json
from datetime import date
import logging
import multiprocessing
import os
import sys
from datetime import datetime
import threading
from time import sleep
from bson import ObjectId
import foursquare
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import MultiProcessLogger


class ConnectionTo4sq:
    def __init__(self, connection_strings, logger):
        self.connection_strings = connection_strings
        self.current_client_index = -1
        self.current_client = self.__get_next_4sq_client()
        self.requests_counter = 0
        self.logger = logger

    def search(self, search_parameter):
        data = None
        while not data:
            try:
                data = self.current_client.venues.search(params={'query': '',
                                                                 'limit': '50',
                                                                 'intent': 'browse',
                                                                 'ne': search_parameter.to_str_ne(),
                                                                 'sw': search_parameter.to_str_sw(),
                                                                 'v': '20140606'})
            except foursquare.FoursquareException as e:
                self.logger.error(e)
                sleep(10)
                self.__reconnect()
        self.requests_counter += 1
        return data

    def get_venue(self, id):
        data = None
        while not data:
            try:
                data = self.current_client.venues(str(id))
            except foursquare.FoursquareException as e:
                self.logger.error(e)
                sleep(10)
                self.__reconnect()
        self.requests_counter += 1
        return data

    def get_categories(self):
        data = None
        while not data:
            try:
                data = self.current_client.venues.categories()
            except foursquare.FoursquareException as e:
                if self.logger:
                    self.logger.error(e)
                sleep(10)
                self.__reconnect()
        self.requests_counter += 1
        return data

    def __reconnect(self):
        self.current_client = self.__get_next_4sq_client()

    def __get_next_4sq_client(self):
        self.current_client_index += 1
        if self.current_client_index >= len(self.connection_strings):
            self.current_client_index = 0
        client = None
        while not client:
            try:
                #Logger.info('Too many queries to foursquare from client. Trying another one.')
                client = foursquare.Foursquare(client_id=self.connection_strings[self.current_client_index]['client_id'],
                                       client_secret=self.connection_strings[self.current_client_index]['client_secret'])
            except foursquare.FoursquareException as e:
                self.logger.error(e)
                sleep(10)
        return client


class SearchParameter:
    def __init__(self, conf, northPoint=None, eastPoint=None, southPoint=None, westPoint=None,
                 split_rate=None, limit=None):
        if(conf is None):
            self.northPoint = northPoint
            self.eastPoint = eastPoint
            self.southPoint = southPoint
            self.westPoint = westPoint
            self.split_rate = split_rate
            self.limit = limit
        else:
            self.northPoint = conf['north']
            self.eastPoint = conf['east']
            self.southPoint = conf['south']
            self.westPoint = conf['west']
            self.split_rate = conf['split_rate']
            self.limit = conf['limit']

    def __str__(self):
        return '{n:' + str(self.northPoint) + ', s:' + str(self.southPoint) + \
               ', e:' + str(self.eastPoint) + ', w:' + str(self.westPoint) + '}'

    def to_str_ne(self):
        return str(self.northPoint) + ',' + str(self.eastPoint)

    def to_str_sw(self):
        return str(self.southPoint) + ',' + str(self.westPoint)

    def split(self, horizontal, split_rate=None):
        if not split_rate:
            split_rate = self.split_rate
        new_parameters = []
        i = 0
        if horizontal:
            step = (self.northPoint - self.southPoint) / split_rate
            while i < split_rate:
                new_parameters.append(SearchParameter(None,
                                                      self.southPoint + (i + 1) * step,
                                                      self.eastPoint,
                                                      self.southPoint + i * step,
                                                      self.westPoint,
                                                      self.split_rate,
                                                      self.limit))
                i += 1
        else:
            step = (self.westPoint - self.eastPoint) / split_rate
            while (i < split_rate):
                new_parameters.append(SearchParameter(None,
                                                      self.northPoint,
                                                      self.eastPoint + i * step,
                                                      self.southPoint,
                                                      self.eastPoint + (i + 1) * step,
                                                      self.split_rate,
                                                      self.limit))
                i += 1

        return new_parameters


class MongodbStorage:
    def __init__(self, config, timestamp, logger):
        mongodb_config = config['mongodb']
        self.timestamp = timestamp
        self.logger = logger
        self.batch_size = mongodb_config['batch_size']
        self.client = MongoClient(mongodb_config['connection_string'])
        self.db = self.client[mongodb_config['database_name']]

        self.collection_ids = self.db[mongodb_config['collection_ids_name']]
        self.ids = []

        self.write_time_series = mongodb_config['write_time_series']
        self.time_series_size = mongodb_config['time_series_size']
        self.time_series_fields = mongodb_config['time_series_fields']
        self.empty_period = self.__get_empty_period(self.time_series_size)
        self.collection_time_series = self.db[mongodb_config['collection_time_series_name']]
        self.collection_time_series.ensure_index(([("_geo", "2d")]))
        self.time_series = []

        self.write_full = mongodb_config['write_full']
        self.collection_full = self.db[mongodb_config['collection_full_name']]
        self.collection_full.ensure_index(([("_geo", "2d")]))
        self.full = []

    def to_timestamp(sef, dt, epoch=datetime(1970,1,1)):
        td = dt - epoch
        # return td.total_seconds()
        return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 1e6

    def write_ids(self, row):
        d = dict()
        d['id'] = row['id']
        d['_geo'] = [row['location']['lng'], row['location']['lat']]
        d['_id'] = ObjectId(row['id'])
        d['_categoryIds'] = row['_categoryIds']
        self.ids.extend([d])

        if len(self.ids) >= self.batch_size:
            self.__execute(self.__execute_ids_update)

    def get_ids(self, category_filter=None, limit=None):
        #db = self.client['foursquare']
        #coll = db['collection_ids']
        coll = self.collection_ids

        ids = []
        if(limit is not None):
            i = 0
            for item in coll.find():
                if(i==limit):
                    break
                ids.extend([item['_id']])
                i+=1
        else:
            ids = coll.distinct('_id')
        return ids

    def write(self, row):
        row['_id'] = ObjectId(row['id'])
        row['_geo'] = [row['location']['lng'], row['location']['lat']]
        row['_timestamp'] = self.to_timestamp(self.timestamp)
        if self.write_full:
            self.full.extend([row])
        if self.write_time_series:
            self.time_series.extend([self.__filter_and_plain_row(row, self.timestamp.timetuple().tm_yday)])

        if len(self.full) >= self.batch_size:
            self.__execute(self.__execute_full_update)
        if len(self.time_series) >= self.batch_size:
            self.__execute(self.__execute_time_series_update)

    def flush(self):
        self.__execute(self.__execute_ids_update)
        self.__execute(self.__execute_full_update)
        self.__execute(self.__execute_time_series_update)

    def close(self):
        self.flush()
        self.client.close()

    def __execute(self, func):
        try:
            func()
        except PyMongoError as ex:
            self.logger.error(ex)

    def __execute_ids_update(self):
        if self.ids:
            self.logger.debug('Ids: writing {}'.format(len(self.ids)))
            #inserted_ids = self.collection_ids.insert(self.ids)
            bulk = self.collection_ids.initialize_unordered_bulk_op()
            for item in self.ids:
                if item.has_key('_id'):
                    id = item['_id']
                    del item['_id']
                else:
                    id = ObjectId(item['id'])
                bulk.find({'_id': id}).upsert().update({'$set': item})
            result = bulk.execute()
            self.logger.debug('Ids: ' + str(result))
            self.ids = []

    def __execute_full_update(self):
        if self.full:
            self.logger.debug('Full: writing {}'.format(len(self.full)))
            bulk = self.collection_full.initialize_unordered_bulk_op()
            for item in self.full:
                if item.has_key('_id'):
                    id = item['_id']
                    del item['_id']
                else:
                    id = ObjectId(item['id'])
                bulk.find({'_id': id}).upsert().update({'$set': item})
            result = bulk.execute()
            self.logger.debug('Full: ' + self.__get_str_from_result(result))
            self.full = []

    def __execute_time_series_update(self):
        if self.time_series:
            self.logger.debug('Time series: writing {}'.format(len(self.time_series)))
            ids = [ObjectId(x['id']) for x in self.time_series]
            ids_exists = set([])
            insert_items = []
            for item in self.collection_time_series.find({'_id': {'$in': ids}}, {'_ids': 1}):
                ids_exists.add(item['_id'])
            for item in self.time_series:
                if ObjectId(item['id']) not in ids_exists:
                    insert_items.extend([self.__get_empty_updates_row(item)])
            if (len(insert_items) != 0):
                inserted_ids = self.collection_time_series.insert(insert_items)
                self.logger.info('Written ' + str(len(inserted_ids)) + ' new time_series')
            bulk = self.collection_time_series.initialize_unordered_bulk_op()
            for item in self.time_series:
                if '_id' in item:
                    id = item['_id']
                    del item['_id']
                else:
                    id = ObjectId(item['id'])
                bulk.find({'_id': id}).update({'$set': item})
            result = bulk.execute()
            self.logger.debug('Time series: ' + self.__get_str_from_result(result))
            self.time_series = []

    def __filter_and_plain_row(self, row, day=None):
        d = dict()
        d['id'] = row['id']
        for name, value in row.iteritems():
            if name.startswith('_'):
                d[name] = value
        for field in self.time_series_fields:
            result = row
            sub_fields = field.split('_')
            for subField in sub_fields:
                if result.has_key(subField):
                    result = result[subField]
                else:
                    result = None
                    break
            if (result is not None):
                if (day is not None):
                    field = field + '.' + str(day - 1)
                d[field] = result
            # else:
            #     self.logger.warning('Field \'' + str(field) + '\' does not exists')
        return d

    def __get_empty_updates_row(self, row):
        d = dict()
        d['id'] = row['id']
        for name, value in row.iteritems():
            if '.' in name:
                name = name[:name.index('.')]
            if(name.startswith('_')):
                d[name] = value
            else:
                d[name] = self.empty_period
        return d

    def __get_empty_period(self, count):
        empty_period = dict()
        for i in range(count):
            empty_period[str(i)] = 0
        return empty_period

    def __get_str_from_result(self, result):
        return 'nUpserted: ' + str(result['nUpserted']) + \
               ', nMatched: ' + str(result['nMatched'])  + \
               ', nInserted: ' + str(result['nInserted'])  + \
               ', nRemoved: ' + str(result['nRemoved'])

#(limit=10000)
            #file = open('ids', 'w')
            #ids = file.read()
            #ids = json.loads(ids)
            #ids = [str(id) for id in ids]
            #ids = json.dumps(ids)
            #file.write(ids)
            #file.close()


class MongodbPartialCsvStorage(MongodbStorage):
    def __init__(self, config, timestamp, logger):
        config_toku = config['mongodb_toku']
        MongodbStorage.__init__(self, config, timestamp, logger)

    def __execute_time_series_update_csv(self):
        csvfile = ""
        csv_writer = csv.DictWriter(csvfile, self.time_series_fields)
        if (len(self.time_series) > 0):
            for item in self.time_series:
                csv_writer.writerow({self.time_series_fields[i]:item[i] for i in range(len(self.time_series_fields))})


class JsonStorage:
    def __init__(self, config, timestamp, logger):
        self.filename_full = 'venues_full_' + str(timestamp.date()) + '_' + str(os.getpid())
        self.filename_ids = 'venues_ids_' + str(timestamp.date()) + '_' + str(os.getpid())
        self.writer_full = open(self.filename_full, 'w')
        self.writer_ids = open(self.filename_ids, 'w')
        self.logger = logger
        self.batch_size = config['batch_size']
        self.counter = 0

    def write_ids(self, row):
        d = dict()
        d['id'] = row['id']
        d['_categoryIds'] = row['_categoryIds']
        self.writer_ids.write(json.dumps(d) + '/n')
        self.counter+=1

        if (self.counter >= self.batch_size):
            self.flush()

    def get_ids(self, category_filter=None, limit=None):
        file = open('ids', 'r')
        ids = []
        id = file.readline()
        while (id != ''):
            id = json.loads(id)
            ids.extend([id])
            id = file.readline()
        file.close()
        return ids

    def write(self, row):
        row['_timestamp'] = self.to_timestamp(self.timestamp)
        self.writer_ids.write(json.dumps(row) + '/n')
        self.counter+=1

        if (self.counter >= self.batch_size):
            self.flush()

    def flush(self):
        if (self.writer_full is not None):
            self.writer_full.flush()
        if (self.writer_ids is not None):
            self.writer_ids.flush()
        self.counter = 0

    def close(self):
        self.flush()
        if (self.writer_full is not None):
            self.writer_full.close()
        if (self.writer_ids is not None):
            self.writer_ids.close()


class WriterThreaded(threading.Thread):
    def __init__(self, queue, config, logger_queue, children_count):
        threading.Thread.__init__(self)
        self.queue = queue
        self.logger_queue = logger_queue
        self.config = config
        self.daemon = False
        self.children_count = children_count

    def run(self):
        logger = MultiProcessLogger.get_logger("Writer", self.logger_queue)
        writer = get_writer(self.config, logger)
        while self.children_count:
            try:
                type, record = self.queue.get()
                if type == "write_ids":
                    writer.write_ids(record)
                elif type == "write":
                    writer.write(record)
                elif type == "die":
                    self.children_count-=1
                else:
                    logger.warn("Unknown type: " + str(type))
            except (KeyboardInterrupt, SystemExit):
                writer.close()
                raise
            except EOFError:
                break
            except Exception as e:
                logger.error(e)
        writer.close()


FIELDS_UPDATES = ['rating', 'specials_count', 'hereNow_count',
                  'stats_checkinsCount', 'stats_usersCount', 'stats_tipCount', 'mayor_count',
                  'tips_count', 'photos_count', 'likes_count']


def get_writer(config, logger):
    return MongodbStorage(config, datetime.today(), logger)


def init_threaded_logger(config):
    logger_config = config["logger"]
    logger_queue = multiprocessing.Queue()
    ch_c = logging.StreamHandler()
    ch_c.setFormatter(MultiProcessLogger.FORMATTER)
    ch_c.setLevel(logger_config["console_level"])

    alerts = logger_config["email_alerts"]
    ch_e = MultiProcessLogger.EmailLogHandler(alerts["gmail_login"], alerts["password"],
                                              alerts["email_from"], alerts["email_to"])
    ch_e.setFormatter(MultiProcessLogger.FORMATTER)
    ch_e.setLevel(logger_config["email_level"])

    log_queue_reader = MultiProcessLogger.LogQueueReader(logger_queue, [ch_c, ch_e])
    log_queue_reader.start()

    return logger_queue


def init_threaded_writer(config, logger_queue, children_count):
    writer_queue = multiprocessing.Queue(config["mongodb"]["batch_size"]*2)
    writer_threaded = WriterThreaded(writer_queue, config, logger_queue, children_count)
    writer_threaded.start()

    return writer_queue


def parseInputOutputArgs(execName=sys.argv[0], argv=sys.argv[1:]):
    helpString = execName + ' -i <inputFile> -o <outputFile>'
    inputFile = ''
    outputFile = ''

    try:
        opts, args = getopt.getopt(argv, 'i:o:')
    except getopt.GetoptError:
        print(helpString)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-i':
            inputFile = arg
        elif opt == '-o':
            outputFile = arg
    if inputFile == '' or outputFile == '':
        print(helpString)
        sys.exit(2)
    return inputFile, outputFile


def parseArgs(args):
    argv = args[1:]
    helpString = args[0] + ' -o <outputFile> -n <North east corner> -e <north East corner> ' \
                           '-s <South west corner> -w <south West corner> -v <Vertical step> -h <Horizontal step> -l <limit>'
    outputFile = ''
    northPoint = 0
    eastPoint = 0
    southPoint = 0
    westPoint = 0
    vStep = 0
    hStep = 0
    limit = -1

    try:
        opts, args = getopt.getopt(argv, 'o:n:e:s:w:v:h:l:')
    except getopt.GetoptError:
        print(helpString)
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-o':
            outputFile = arg
        elif opt in "-n":
            northPoint = float(arg)
        elif opt in "-e":
            eastPoint = float(arg)
        elif opt in "-s":
            southPoint = float(arg)
        elif opt in "-w":
            westPoint = float(arg)
        elif opt in "-v":
            vStep = float(arg)
        elif opt in "-h":
            hStep = float(arg)
        elif opt in "-l":
            limit = float(arg)
    if outputFile == '' or northPoint == 0 or eastPoint == 0 or southPoint == 0 or westPoint == 0 or vStep == 0 or hStep == 0:
        print(helpString)
        sys.exit(2)
    return outputFile, northPoint, eastPoint, southPoint, westPoint, vStep, hStep, limit


def addCategory(row, categoriesDict):
    categoryIds = [item['id'] for item in row['categories']]
    row['_categoryIds'] = [get_category_path(item, categoriesDict, []) for item in categoryIds]


def get_categories_dict_with_full_inheritance(categories, parent=None, result=None):
    if not result:
        result = {}
    if isinstance(categories, dict):
        for item in categories['categories']:
            result[item['id']] = parent
            result = get_categories_dict_with_full_inheritance(item.get('categories', []), item['id'], result)
    elif isinstance(categories, list):
        for item in categories:
            result[item['id']] = parent
            result = get_categories_dict_with_full_inheritance(item.get('categories', []), item['id'], result)
    return result


def get_main_category(category_name, dict_with_full_inheritance):
    if not dict_with_full_inheritance.has_key(category_name):
        return '?'
    main_category = dict_with_full_inheritance[category_name]
    if main_category is None:
        return category_name
    else:
        return get_main_category(main_category, dict_with_full_inheritance)


def get_category_path(category_name, dict_with_full_inheritance, path):
    path.extend([category_name])
    if not dict_with_full_inheritance.has_key(category_name):
        path.extend(['?'])
        return path
    main_category = dict_with_full_inheritance[category_name]
    if main_category is None:
        return path
    else:
        return get_category_path(main_category, dict_with_full_inheritance, path)


def read_connections_file(file_path):
    csv_file = open(file_path, 'r')
    header = ['client_id', 'secret_col']
    reader = csv.DictReader(csv_file, header)
    pairs = []
    reader.next()
    while (True):
        try:
            row = reader.next()
            pairs.extend([(row['client_id'], row['secret_col'])])
        except StopIteration:
            break
    connections = []
    i = 0
    while (i < len(pairs) - 1):
        connections.extend([ConnectionTo4sq([pairs[i], pairs[i + 1]])])
        i += 2
    return connections