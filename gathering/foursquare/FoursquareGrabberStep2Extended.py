#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import sys
import foursquare
import Common
import MultiProcessLogger

logger = logging.getLogger(__name__)

def grabExtendedInfo(ids, writer, connection, queue=None):
    MultiProcessLogger.init_logger(logger, queue)
    client = connection.get_new_4sq_client()
    categoriesDict = Common.get_categories_dict_with_full_inheritance(client.venues.categories())
    counter = Common.Counter(0)
    i = 0
    length = len(ids)
    while(i < length):
        try:
            counter.increment()
            row = client.venues(ids[i])['venue']
            Common.addCategory(row, categoriesDict)
            writer.writeRow(row)
            if counter.get_value() % 1000 == 0:
                logger.debug("Done" + str(counter.get_value()) + 'rows')
                writer.flush()
            i+=1
        except foursquare.RateLimitExceeded:
            client = connection.get_new_4sq_client()
            i-=1
        except KeyboardInterrupt:
            return False
        except:
            logger.error('Unexpected error:' + str(sys.exc_info()[0]) + str(sys.exc_info()[1]))
            logger.error(str(ids[i]))
    return True

if __name__ == "__main__":
    MultiProcessLogger.configure_loggers()

    inputFile, outputFile = Common.parseInputOutputArgs() #'venues.csv', 'venues.Prepared.csv'
    connectionManager = Common.ConnectionManager()
    connection = connectionManager.get_connection()
    reader = Common.CSVReader(Common.FIELDS, inputFile)
    writer = Common.CSVWriter(Common.FIELDS_EXT, outputFile)
    ids = reader.getIds()
    grabExtendedInfo(ids, writer, connection)
    writer.close()