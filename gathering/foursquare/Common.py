#!/usr/bin/python
# -*- coding: utf-8 -*-
import csv

import getopt
import json
import logging
import sys
from datetime import datetime
from bson import ObjectId
import foursquare
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import Collector
from MultiProcessLogger import DEFAULT_LEVEL
import MultiProcessLogger

#logger = logging.getLogger(__name__)

class Connection:
    def __init__(self, connection_strings):
        self.connection_strings = connection_strings
        self.current_client = -1

    def get_new_4sq_client(self):
        self.current_client += 1
        if (self.current_client >= len(self.connection_strings)):
            self.current_client = 0
        #Logger.info('Too many queries to foursquare from client. Trying another one.')
        client = foursquare.Foursquare(client_id=self.connection_strings[self.current_client][0],
                                       client_secret=self.connection_strings[self.current_client][1])
        return client


class ConnectionManager:
    CLIENT_ID_SECRET_COL_1 = [
        ('JRZZ2BAR4F5YKQ53NJ5HNNLOSY3C35FZFYPNGVS5GXMDFMCK', 'ISJRHW1U2MI5EEL1KGIJODBL0123C0KJUMLAIJQ40VLTVHGO'),
        ('MRAQNMNLQCN2WDZ5IQ5VBRN0GRG0JR42ID0ADO4ECV45SLPX', 'WQA3QM2OV2BY3JKLJ3FURY2XWQX5RNHKRENRDPWYE3HH15XH')]
    CLIENT_ID_SECRET_COL_2 = [
        ('D1JJ3OZKKVJBGNU0RTILMRTFDL2RAX0HPTBW5Z0QDRIZJW1B', 'BGKBQSMXRSMLSE4JCOWL11R1EDYNRCPJO1SMOGUEE3RAPSJO'),
        ('LLAM5AUR3OLXLBLU3GMFQLWGQHP0AUPBR32JCLATDGKCTFJY', '1XRBAJHYBOYARTGYKKGU0DQZZRNO5OANUJ1QV2D2GGCXLPXK')]
    CLIENT_ID_SECRET_COL_3 = [
        ('2D5Y0FAIVLATRSJO5G4S1WQMSJY0ULBRECCANUY2MBE4JX24', 'IGDKSM4FV3LMVRYOASQAE5QKJKSMMJGWGEJCTGBWJOCFHMVN'),
        ('PWPCNOQFDGOFXJJEEKXYKCHSNOSI50FZHNXXEIN3USYT323V', 'JTTAEXUCLVKTYS3FEPMUFRW3OUP2PT3W03V023QNPKL3N40O')]
    CLIENT_ID_SECRET_COL_4 = [
        ('5BLTXN3T1T5Z4PXMDZTR01PPWLBHAERHKXU3WIK1EKEY4LZD', 'K3KYNQ1V2I051A55EOBWYDZPJ0JGVD1YFTIGDBIUINMODT0J'),
        ('M4KPPR52RLVEHCVQ13NUM2ZVBFU2P5QWNRWCEPVEP1FEOCLS', 'I2P1VHBI3AN1UK0LDDJNPDK2TZ3FFRGW1KGUWJ3DSAXWEHMM')]
    CLIENT_ID_SECRET_COL_5 = [
        ('N4VSAGO0XKY4VZQI2JF2GTWEUXJT3SEJACF0BXVKO5JMFOC5', 'VLQ3AC4RJVBN2IFLBG5AXU4BQUTF0CB3AREU4BDGKY1M0NDV'),
        ('GDEQTXCP0EWD2J32DZMHNEP4ZBVNQGFZ50PEMGQNZL2JJJTQ', 'MXLFMJJS34BU32HMWU5KK0CYPIONTLXOGIP3EWSNDH3GGZSM')]

    def __init__(self):
        self.connection_strings = [ConnectionManager.CLIENT_ID_SECRET_COL_1, ConnectionManager.CLIENT_ID_SECRET_COL_2,
                                   ConnectionManager.CLIENT_ID_SECRET_COL_3, ConnectionManager.CLIENT_ID_SECRET_COL_4,
                                   ConnectionManager.CLIENT_ID_SECRET_COL_5]
        self.available_count = len(self.connection_strings)

    def get_connection(self):
        if (self.available_count > 0):
            self.available_count -= 1
            return Connection(self.connection_strings[len(self.connection_strings) - self.available_count - 1])
        raise Exception("No connections available")


class SearchParameter:
    def __init__(self, northPoint, eastPoint, southPoint, westPoint, vStep, hStep, limit):
        self.northPoint = northPoint
        self.eastPoint = eastPoint
        self.southPoint = southPoint
        self.westPoint = westPoint
        self.vStep = vStep
        self.hStep = hStep
        self.limit = limit

    def to_str(self):
        return 'n:' + str(self.northPoint) + ', s:' + str(self.southPoint) + \
               ', e:' + str(self.eastPoint) + ', w:' + str(self.westPoint)


class Writer:
    def __init__(self, header):
        self.header = header

    def writeRow(self, row):
        pass

    def flush(self):
        pass

    def close(self):
        pass


class Reader:
    def __init__(self, header):
        self.header = header

    def getRow(self, id):
        pass

    def getRows(self, ids):
        pass

    def getIds(self):
        pass

    def close(self):
        pass


class CSVWriter(Writer):
    def __init__(self, header, output_file_name):
        Writer.__init__(self, header)
        self.output_file_name = output_file_name
        self.csv_file = open(self.output_file_name, 'wb')
        self.writer = csv.DictWriter(self.csv_file, header)
        self.writer.writeheader()

    def writeRow(self, row):
        self.writer.writerow(self.__combine_row(row))

    def flush(self):
        self.csv_file.flush()

    def close(self):
        self.csv_file.close()

    def __combine_row(self, row):
        d = dict()
        for field in self.header:
            result = row
            if field == 'categoryIds':
                result = ';'.join(item['id'] for item in row['categories'])
            else:
                subFields = field.split('_')
                for subField in subFields:
                    if result.has_key(subField):
                        result = result[subField]
                    else:
                        result = ''
                        break
            d[field] = encodeObject(result)
        return d


class CSVReader:
    def __init__(self, header, input_file_name):
        Reader.__init__(self, header)
        self.input_file_name = input_file_name
        self.csv_file = open(self.input_file_name, 'r')
        self.reader = csv.DictReader(self.csv_file, header)

    def getRow(self, id):
        pass

    def getRows(self, ids):
        pass

    def getIds(self):
        ids = []
        while (True):
            try:
                ids.extend(decodeObject(self.reader.next()['id']))
            except StopIteration:
                break
        return ids

    def close(self):
        self.csv_file.close()


class DB_Mongodb_Writer(Writer):
    def __init__(self, header, db_connection_string, write_updates, queue=None):
        Writer.__init__(self, header)
        self.db_connection_string = db_connection_string
        self.client = MongoClient(db_connection_string)
        self.db = self.client['db_4sq']
        self.venues = self.db['venues']
        self.venues_updates = self.db['venues_updates']
        self.write_updates = write_updates
        self.bulk = self.venues.initialize_unordered_bulk_op()
        self.logger = logging.getLogger(__name__)
        MultiProcessLogger.init_logger(self.logger, queue)
        self.max_length = 1000

    def writeRow(self, row):
        to_update = self.__combine_row(row)
        to_update['update_timestamp'] = datetime.today()
        self.bulk.find({'_id': to_update['_id']}).upsert().update({'$set': to_update})
        #if (len(self.bulk._BulkOperationBuilder__bulk.ops) == self.max_length):
        #    self.flush()
            # self.venues.update({'_id': to_update['_id']}, to_update, upsert=True)
        if self.write_updates:
            updates = self.__combine_row_updates(row, datetime.today().strftime('_%d_%m_%y'))
            updates['_id'] = to_update['_id']
            to_update['update_timestamp'] = datetime.today()
            self.venues_updates.update({'_id': updates['_id']}, updates, upsert=True)

    def flush(self):
        try:
            if (len(self.bulk._BulkOperationBuilder__bulk.ops) != 0):
                result = self.bulk.execute()
                self.logger.info(result)
                self.bulk = self.venues.initialize_unordered_bulk_op()
        except BulkWriteError as bwe:
            self.logger.info(bwe)
        pass

    def close(self):
        self.client.close()

    def __combine_row(self, row):
        d = dict()
        for field in self.header:
            result = row
            if field == 'id':
                field = '_id'
                result = ObjectId(row['id'])
            elif field == 'categoryIds':
                result = ';'.join(item['id'] for item in row['categories'])
            else:
                subFields = field.split('_')
                for subField in subFields:
                    if result.has_key(subField):
                        result = result[subField]
                    else:
                        result = ''
                        break
            if (result != ''):
                d[field] = result
        return d

    def __combine_row_updates(self, row, field_suffix):
        d = dict()
        for field in FIELDS_UPDATES:
            result = row
            subFields = field.split('_')
            for subField in subFields:
                if result.has_key(subField):
                    result = result[subField]
                else:
                    result = ''
                    break
            d[field + field_suffix] = result
        return d


class DB_Mongodb_Reader(Reader):
    def __init__(self, header, db_connection_string):
        Reader.__init__(self, header)
        self.db_connection_string = db_connection_string
        self.client = MongoClient(db_connection_string)
        self.db = self.client['db_4sq']
        self.venues = self.db['venues']

    def getRow(self, id):
        pass

    def getRows(self, ids):
        pass

    def getIds(self):
        ids = self.venues.distinct('_id')
        # ids = []
        # for venue in self.venues.find({},{ '_id': 1 }):
        #     ids.extend([str(venue['_id'])])
        return ids

    def close(self):
        self.client.close()


class ReadWriterManager:
    def __init__(self):
        pass

    def getFirstStepWriter(self, queue=None):
        pass

    def getSecondStepWriter(self, queue=None):
        pass

    def getFirstStepReader(self):
        pass


class DB_Mongodb_ReadWriterManager(ReadWriterManager):
    def __init__(self, db_connection_string):
        ReadWriterManager.__init__(self)
        self.db_connection_string = db_connection_string

    def getFirstStepWriter(self, queue=None):
        return DB_Mongodb_Writer(FIELDS, self.db_connection_string, False, queue)

    def getSecondStepWriter(self, queue=None):
        return DB_Mongodb_Writer(FIELDS_EXT, self.db_connection_string, True, queue)

    def getFirstStepReader(self):
        return DB_Mongodb_Reader(FIELDS, self.db_connection_string)


class Counter:
    def __init__(self, initialValue):
        self.value = initialValue

    def increment(self):
        self.value += 1

    def get_value(self):
        return self.value


FIELDS = ['id', 'name', 'verified', 'referralId', 'hours', 'popular', 'rating',
          'categories', 'categoryIds',
          'specials_count', 'specials_items',
          'hereNow_count', 'hereNow_groups',
          'storeId', 'description', 'url', 'private', 'venuePage',
          'contact_twitter', 'contact_phone', 'contact_formattedPhone',
          'location_address', 'location_crossStreet', 'location_city', 'location_state',
          'location_postalCode', 'location_country', 'location_lat', 'location_lng',
          'location_distance',
          'stats_checkinsCount', 'stats_usersCount', 'stats_tipCount',
          'menu_url', 'menu_mobileUrl',
          'price_tier', 'price_message']

FIELDS_EXT = ['id', 'name', 'verified', 'referralId', 'url', 'hours', 'popular', 'rating',
              'categories', 'categoryIds',
              'specials_count', 'specials_items',
              'hereNow_count', 'hereNow_groups',
              'storeId', 'description', 'createdAt', 'reasons', 'private', 'venuePage',
              'contact_twitter', 'contact_phone', 'contact_formattedPhone',
              'location_address', 'location_crossStreet', 'location_city', 'location_state',
              'location_postalCode', 'location_country', 'location_lat', 'location_lng',
              'location_distance',
              'stats_checkinsCount', 'stats_usersCount', 'stats_tipCount',
              'mayor_count', 'mayor_user', 'mayor_user_id', 'mayor_user_gender',
              'tips_count', 'tips_groups',
              'tags', 'shortUrl', 'canonicalUrl', 'specialsNearby',
              'photos_count', 'photos_groups',
              'likes_count', 'likes_groups',
              'roles', 'flags', 'page',
              'menu_url', 'menu_mobileUrl',
              'price_tier', 'price_message']

FIELDS_UPDATES = ['rating', 'specials_count', 'hereNow_count',
                  'stats_checkinsCount', 'stats_usersCount', 'stats_tipCount', 'mayor_count',
                  'tips_count', 'photos_count', 'likes_count']

FIELDS_COUNT = len(FIELDS)
ROW_SEPARATOR = '\n'
COLUMN_SEPARATOR = ','
ANOTHER_SEPARATOR = ';'


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


def encodeObject(o):
    return json.dumps(o).encode('utf-8')


def decodeObject(o):
    return json.loads(o)


def addCategory(row, categoriesDict):
    categoryIds = [item['id'] for item in row['categories']]
    row['categoryIds'] = ';'.join(categoryIds)
    row['mainCategory'] = encodeObject(';'.join(get_main_category(item, categoriesDict)
                                                for item in categoryIds))
    row['fullCategories'] = encodeObject(';'.join(get_category_path(item, categoriesDict)
                                                  for item in categoryIds))


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


def get_category_path(category_name, dict_with_full_inheritance, sep='>'):
    if not dict_with_full_inheritance.has_key(category_name):
        return '?>' + category_name
    main_category = dict_with_full_inheritance[category_name]
    if main_category is None:
        return category_name
    else:
        return get_category_path(main_category, dict_with_full_inheritance) + sep + category_name


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
        connections.extend([Connection([pairs[i], pairs[i + 1]])])
        i += 2
    return connections