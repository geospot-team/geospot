import csv

import getopt
import json
import logging
import sys
from datetime import datetime
from time import sleep, time
from bson import ObjectId
import foursquare
from pymongo import MongoClient
from pymongo.errors import BulkWriteError, InvalidOperation, PyMongoError
import MultiProcessLogger


class ConnectionTo4sq:
    def __init__(self, connection_strings):
        self.connection_strings = connection_strings
        self.current_client_index = -1
        self.current_client = self.__get_next_4sq_client()
        self.requests_counter = 0

    def search(self, search_parameter):
        succeed = False
        while (not succeed):
            try:
                data = self.current_client.venues.search(params={'query': '',
                                                                 'limit': '50',
                                                                 'intent': 'browse',
                                                                 'ne': search_parameter.to_str_ne(),
                                                                 'sw': search_parameter.to_str_sw()})
                succeed = True
            except foursquare.RateLimitExceeded:
                self.__reconnect()
            except foursquare.ServerError:
                self.__reconnect()
                time.sleep(1)
        self.requests_counter += 1
        return data

    def get_venue(self, id):
        succeed = False
        while (not succeed):
            try:
                data = self.current_client.venues(id)
                succeed = True
            except foursquare.RateLimitExceeded:
                self.__reconnect()
            except foursquare.ServerError:
                self.__reconnect()
                time.sleep(1)
        self.requests_counter += 1
        return data

    def get_categories(self):
        succeed = False
        while (not succeed):
            try:
                data = self.current_client.venues.categories()
                succeed = True
            except foursquare.RateLimitExceeded:
                self.__reconnect()
            except foursquare.ServerError:
                self.__reconnect()
                time.sleep(1)
        self.requests_counter += 1
        return data

    def __reconnect(self):
        self.current_client = self.__get_next_4sq_client()

    def __get_next_4sq_client(self):
        self.current_client_index += 1
        if (self.current_client_index >= len(self.connection_strings)):
            self.current_client_index = 0
        #Logger.info('Too many queries to foursquare from client. Trying another one.')
        client = foursquare.Foursquare(client_id=self.connection_strings[self.current_client_index]['client_id'],
                                       client_secret=self.connection_strings[self.current_client_index]['client_secret'])
        return client


class SearchParameter:
    def __init__(self, conf, northPoint=None, eastPoint=None, southPoint=None, westPoint=None, vStep=None, hStep=None,
                 split_rate=None, limit=None):
        if(conf is None):
            self.northPoint = northPoint
            self.eastPoint = eastPoint
            self.southPoint = southPoint
            self.westPoint = westPoint
            self.vStep = vStep
            self.hStep = hStep
            self.split_rate = split_rate
            self.limit = limit
        else:
            self.northPoint = conf['north']
            self.eastPoint = conf['east']
            self.southPoint = conf['south']
            self.westPoint = conf['west']
            self.vStep = conf['vStep']
            self.hStep = conf['hStep']
            self.split_rate = conf['split_rate']
            self.limit = conf['limit']

    def __str__(self):
        return '{n:' + str(self.northPoint) + ', s:' + str(self.southPoint) + \
               ', e:' + str(self.eastPoint) + ', w:' + str(self.westPoint) + '}'

    def to_str_ne(self):
        return str(self.northPoint) + ',' + str(self.eastPoint)

    def to_str_sw(self):
        return str(self.southPoint) + ',' + str(self.westPoint)

    def split(self, step, horizontal):
        new_parameters = []
        i = 0
        if (horizontal):
            split_count = (self.northPoint - self.southPoint) / step
            while (i < split_count):
                new_parameters.append(SearchParameter(None,
                                                      self.southPoint + (i + 1) * step,
                                                      self.eastPoint,
                                                      self.southPoint + i * step,
                                                      self.westPoint,
                                                      self.vStep,
                                                      self.hStep,
                                                      self.split_rate,
                                                      self.limit))
                i += 1
        else:
            split_count = (self.westPoint - self.eastPoint) / step
            while (i < split_count):
                new_parameters.append(SearchParameter(None,
                                                      self.northPoint,
                                                      self.eastPoint + i * step,
                                                      self.southPoint,
                                                      self.eastPoint + (i + 1) * step,
                                                      self.vStep,
                                                      self.hStep,
                                                      self.split_rate,
                                                      self.limit))
                i += 1

        return new_parameters


class MongodbStorage:
    def __init__(self, config, timestamp, logger):
        self.connection_string = config['connection_string']
        self.timestamp = timestamp
        self.client = MongoClient(self.connection_string)
        self.db = self.client[config['database_name']]
        self.collection_ids = self.db[config['collection_ids_name']]
        self.ids = []
        self.collection_last = self.db[config['collection_last_name']]
        self.collection_last.ensure_index(([("_geo", "2d")]))
        self.last = []
        self.collection_time_series = self.db[config['collection_time_series_name']]
        self.collection_time_series.ensure_index(([("_geo", "2d")]))
        self.time_series = []
        self.collection_full = self.db[config['collection_full_name']]
        self.collection_full.ensure_index(([("_geo", "2d")]))
        self.full = []
        self.logger = logger.getChild(str(self.__class__))
        self.batch_size = config['batch_size']
        self.empty_period = self.__get_empty_period(31)

    def __to_timestamp(sef, dt, epoch=datetime(1970,1,1)):
        td = dt - epoch
        # return td.total_seconds()
        return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 1e6

    def write_ids(self, row):
        d = dict()
        d['_id'] = ObjectId(row['id'])
        d['categoryIds'] = row['categoryIds']
        self.ids.extend([d])

        if (len(self.ids) >= self.batch_size):
            self.__execute(self.__execute_ids_update)

    def get_ids(self, category_filter=None, limit=None):
        ids = []
        if(limit is not None):
            i = 0
            for item in self.collection_ids.find():
                if(i==limit):
                    break
                ids.extend([item['_id']])
                i+=1
        else:
            ids = self.collection_ids.distinct('_id')
        return ids

    def write(self, row):
        row['_id'] = ObjectId(row['id'])
        row['_geo'] = [row['location']['lat'], row['location']['lng']]
        row['_timestamp'] = self.__to_timestamp(self.timestamp)
        row['_category_ids'] = row['categoryIds']
        row['_name'] = row['name']
        self.full.extend([row])
        #self.last.extend([self.__filter_and_plain_row(row)])
        #self.time_series.extend([self.__filter_and_plain_row(row, self.timestamp.day)])
        if (len(self.full) >= self.batch_size):
            self.__execute(self.__execute_full_update)
        if (len(self.last) >= self.batch_size):
            self.__execute(self.__execute_last_update)
        if (len(self.time_series) >= self.batch_size):
            self.__execute(self.__execute_time_series_update)

    def flush(self):
        self.__execute(self.__execute_ids_update)
        self.__execute(self.__execute_last_update)
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
        if (len(self.ids) > 0):
            #inserted_ids = self.collection_ids.insert(self.ids)
            bulk = self.collection_ids.initialize_unordered_bulk_op()
            for item in self.ids:
                bulk.find({'_id': item['_id']}).upsert().update({'$set': item})
            result = bulk.execute()
            self.logger.info('Ids: ' + str(result))
            self.ids = []

    def __execute_last_update(self):
        if (len(self.last) > 0):
            bulk = self.collection_last.initialize_unordered_bulk_op()
            for item in self.last:
                bulk.find({'_id': item['_id']}).upsert().update({'$set': item})
            result = bulk.execute()
            self.logger.info('Last: ' + str(result))
            self.last = []

    def __execute_full_update(self):
        if (len(self.full) > 0):
            bulk = self.collection_full.initialize_unordered_bulk_op()
            for item in self.full:
                bulk.find({'_id': item['_id']}).upsert().update({'$set': item})
            result = bulk.execute()
            self.logger.info('Full: ' + str(result))
            self.full = []

    def __execute_time_series_update(self):
        if (len(self.time_series) > 0):
            ids = [x['_id'] for x in self.time_series]
            ids_exists = set([])
            insert_items = []
            for item in self.collection_time_series.find({'_id': {'$in': ids}}, {'_ids': 1}):
                ids_exists.add(item['_id'])
            for item in self.time_series:
                if item['_id'] not in ids_exists:
                    insert_items.extend(self.__get_empty_updates_row(item['_id'], self.empty_period))
            if (len(insert_items) != 0):
                inserted_ids = self.collection_time_series.insert(insert_items)
                self.logger.info('Written ' + str(len(inserted_ids)) + ' new time_series')
            bulk = self.collection_time_series.initialize_unordered_bulk_op()
            for item in self.time_series:
                bulk.find({'_id': item['_id']}).update({'$set': item})
            result = bulk.execute()
            self.logger.info('Time series: ' + str(result))
            self.time_series = []

    def __filter_and_plain_row(self, row, day=None):
        d = dict()
        for name, value in row.iteritems():
            if (name.startswith('_')):
                d[name] = value
        for field in FIELDS_UPDATES:
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
                    field = field + '.' + str(day)
                d[field] = result
            # else:
            #     self.logger.warning('Field \'' + str(field) + '\' does not exists')
        return d

    def __get_empty_updates_row(self, row):
        d = dict()
        for name, value in row.iteritems:
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


FIELDS_UPDATES = ['rating', 'specials_count', 'hereNow_count',
                  'stats_checkinsCount', 'stats_usersCount', 'stats_tipCount', 'mayor_count',
                  'tips_count', 'photos_count', 'likes_count']


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
    row['categoryIds'] = [get_category_path(item, categoriesDict, []) for item in categoryIds]


def get_categories_dict_with_full_inheritance(categories, parent=None, result=dict()):
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