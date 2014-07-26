import logging
from multiprocessing import Pool
import multiprocessing
import sys
import Common
import FoursquareGrabberStep1
import FoursquareGrabberStep2Extended
import MultiProcessLogger

logger = logging.getLogger(__name__)


def firstStepGrabber(args):
    readWriteManager = args[0]
    searchParameter = args[1]
    connection = args[2]
    queue = firstStepGrabber.queue
    firstStepWriter = readWriteManager.getFirstStepWriter(queue)
    FoursquareGrabberStep1.exploreArea(firstStepWriter, connection, searchParameter, queue)
    firstStepWriter.close()

def firstStepGrabber_init(queue):
    firstStepGrabber.queue = queue

def secondStepGrabber(args):
    readWriteManager = args[0]
    ids = args[1]
    connection = args[2]
    queue = secondStepGrabber.queue
    secondStepWriter = readWriteManager.getSecondStepWriter(queue)
    FoursquareGrabberStep2Extended.grabExtendedInfo(ids, secondStepWriter, connection, queue)
    secondStepWriter.close()

def secondStepGrabber_init(queue):
    secondStepGrabber.queue = queue

def chunks(l, n):
    result = []
    for i in xrange(0, len(l), n):
        result.extend([l[i:i+n]])
    return result

def collectArea(readWriteManager, connectionsTo4sq, searchParameter):
    queue = multiprocessing.Queue()
    log_queue_reader = MultiProcessLogger.LogQueueReader(queue)
    log_queue_reader.start()

    threadsCount = len(connectionsTo4sq)
    searchParameters = FoursquareGrabberStep1.splitSearchParameter(searchParameter,
                       (searchParameter.northPoint - searchParameter.southPoint)/threadsCount, True)
    args = [(readWriteManager, searchParameters[i], connectionsTo4sq[i]) for i in range(threadsCount)]
    pool = Pool(threadsCount, firstStepGrabber_init, [queue])
    logger.info("Starting " + str(threadsCount) + " processes for first step...")
    result = pool.map(firstStepGrabber, args)

    firstStepReader = readWriteManager.getFirstStepReader()
    ids = firstStepReader.getIds()
    ids = chunks(ids, len(ids)/threadsCount)
    args = [(readWriteManager, ids[i], connectionsTo4sq[i], queue) for i in range(threadsCount)]
    logger.info("Starting " + str(threadsCount) + " processes for second step...")
    result = pool.map(secondStepGrabber, args)


if __name__ == "__main__":
    MultiProcessLogger.configure_loggers()

    outputFile, northPoint, eastPoint, southPoint, westPoint, vStep, \
        hStep, limit = Common.parseArgs(sys.argv)
    readWriteManager = Common.DB_Mongodb_ReadWriterManager('mongodb://ec2-54-186-48-9.us-west-2.compute.amazonaws.com:27017/')
    connectionsTo4sq = Common.read_connections_file("connectionsTo4sq.csv")
    searchParameters = Common.SearchParameter(northPoint, eastPoint, southPoint, westPoint, vStep, hStep, limit)
    collectArea(readWriteManager,connectionsTo4sq,searchParameters)