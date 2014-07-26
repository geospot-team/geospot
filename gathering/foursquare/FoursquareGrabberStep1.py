import foursquare
import logging
import sys
import time
import Common
import MultiProcessLogger

logger = logging.getLogger(__name__)

def splitSearchParameter(searchParameter, step, horizontal):
    newParameters = []
    i = 0
    if(horizontal):
        splitCount = (searchParameter.northPoint - searchParameter.southPoint)/step
        while(i<splitCount):
            newParameters.append(Common.SearchParameter(searchParameter.southPoint + (i+1)*step,
                                                  searchParameter.eastPoint,
                                                  searchParameter.southPoint + i*step,
                                                  searchParameter.westPoint,
                                                  searchParameter.vStep,
                                                  searchParameter.hStep,
                                                  searchParameter.limit))
            i+=1
    else:
        splitCount = (searchParameter.westPoint - searchParameter.eastPoint)/step
        while(i<splitCount):
            newParameters.append(Common.SearchParameter(searchParameter.northPoint,
                                                  searchParameter.eastPoint + i*step,
                                                  searchParameter.southPoint,
                                                  searchParameter.eastPoint + (i+1)*step,
                                                  searchParameter.vStep,
                                                  searchParameter.hStep,
                                                  searchParameter.limit))
            i+=1

    return newParameters

def point_to_str_ne(searchParameter):
    return str(searchParameter.northPoint) + ',' + str(searchParameter.eastPoint)
def point_to_str_sw(searchParameter):
    return str(searchParameter.southPoint) + ',' + str(searchParameter.westPoint)


def searchInArea(searchParameter, counter, client, categoriesDict, splitCount):
    counter.increment()
    dataCollection = client.venues.search(params={'query': '',
                                                         'limit': '50',
                                                         'intent': 'browse',
                                                         'ne': point_to_str_ne(searchParameter),
                                                         'sw': point_to_str_sw(searchParameter)})
    rows = dataCollection['venues']
    length = len(rows)
    if length == 50:
        logger.debug('Warning: too many objects in area {' + searchParameter.to_str() + '}' + str(length) + 'of 50 limit')

        rows = []
        newParameters = splitSearchParameter(searchParameter,
                                             (searchParameter.northPoint - searchParameter.southPoint)/splitCount,
                                             True)
        newParameters = [x for param in newParameters for x in splitSearchParameter(param,
                                              (param.westPoint - param.eastPoint)/splitCount,
                                              False)]
        for param in newParameters:
            logger.debug('Go deeper: {' + param.to_str() + '}')
            rows.extend(searchInArea(param, counter, client, categoriesDict, splitCount))
        length = len(rows)
        logger.debug('Total objects count in area {' + searchParameter.to_str() + '}:' + str(length))
    for row in rows:
        Common.addCategory(row, categoriesDict)
    return rows


def exploreArea(writer, connection, searchParameter, queue=None):
    MultiProcessLogger.init_logger(logger, queue)
    client = connection.get_new_4sq_client()
    categoriesDict = Common.get_categories_dict_with_full_inheritance(client.venues.categories())
    counter = Common.Counter(0)
    newParameters = splitSearchParameter(searchParameter,
                                         searchParameter.hStep,
                                         True)
    newParameters = [x for param in newParameters for x in splitSearchParameter(param, param.vStep, False)]
    i=0
    length = len(newParameters)
    countOfVenues = 0
    keyboardInterrupt = False
    startTime = time.time()
    while (i<length):
        try:
            startSubArea = time.time()
            param = newParameters[i]

            rows = searchInArea(param, counter, client, categoriesDict, 2)
            for row in rows:
                writer.writeRow(row)
            writer.flush()

            countOfVenues += len(rows)
            endSubArea = time.time()
            logger.info('Area:{' + param.to_str() + '}...Done. Found ' +
                str(len(rows)) + ' objects. It took ' + str(endSubArea - startSubArea) + ' seconds. ' +
                'Total requests count:' + str(counter.get_value()))
            i+=1
        except KeyboardInterrupt:
            keyboardInterrupt = True
            break
        except foursquare.RateLimitExceeded:
            client = connection.get_new_4sq_client()
            i-=1
        except:
            logger.error('Unexpected error:' + str(sys.exc_info()[0]) + str(sys.exc_info()[1]))
            logger.error(str(counter.get_value()))
    endTime = time.time()
    if keyboardInterrupt:
        logger.info('Program was interrupted. It took:' + str(endTime - startTime) + 'seconds')
    else:
        logger.info('Program finished execution successfully. It took:' + str(endTime - startTime) + 'seconds')
    return (not keyboardInterrupt)

if __name__ == "__main__":
    MultiProcessLogger.configure_loggers()

    outputFile, northPoint, eastPoint, southPoint, westPoint, vStep, \
        hStep, limit = Common.parseArgs(sys.argv)
    searchParameters = Common.SearchParameter(northPoint, eastPoint, southPoint, westPoint, vStep, hStep, limit)
    connectionsTo4sq = Common.read_connections_file("connectionsTo4sq.csv")
    connection = connectionsTo4sq[0]
    #writer = Common.CSVWriter(Common.FIELDS, outputFile)
    writer = Common.DB_Mongodb_Writer(Common.FIELDS, 'mongodb://ec2-54-186-48-9.us-west-2.compute.amazonaws.com:27017/',
                                      True)
    exploreArea(writer, connection, searchParameters)
