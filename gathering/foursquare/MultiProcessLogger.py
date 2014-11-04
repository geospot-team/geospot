import os
import sys
import time
import traceback
import multiprocessing, threading, logging, sys
import Collector
import Common
import search_venues
import get_venues
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

DEFAULT_LEVEL = logging.INFO

formatter = logging.Formatter("%(levelname)s: %(asctime)s - %(name)s - %(process)s - %(message)s")


class SubProcessLogHandler(logging.Handler):
    """handler used by subprocesses

    It simply puts items on a Queue for the main process to log.

    """

    def __init__(self, queue):
        logging.Handler.__init__(self)
        self.queue = queue

    def emit(self, record):
        self.queue.put(record)


class EmailLogHandler(logging.Handler):

    def __init__(self, gmail_login, password, emailFrom, emailTo):
        logging.Handler.__init__(self)
        self.gmail_login = gmail_login
        self.password = password
        self.emailFrom = emailFrom
        self.emailTo = emailTo

    def emit(self, record):
        msg = MIMEMultipart('alternative')
        msg['Subject'] = "GeoSpottingServer " + str(record.level)
        msg['From'] = self.emailFrom
        msg['To'] = self.emailTo
        text = record.msg
        part1 = MIMEText(text, 'plain')
        msg.attach(part1)
        s = smtplib.SMTP('smtp.gmail.com', 587)
        s.ehlo()
        s.starttls()
        s.ehlo()
        s.login(self.gmail_login, self.password)
        s.sendmail(self.emailFrom, self.emailTo, msg.as_string())
        s.quit()


class LogQueueReader(threading.Thread):
    """thread to write subprocesses log records to main process log

    This thread reads the records written by subprocesses and writes them to
    the handlers defined in the main process's handlers.

    """

    def __init__(self, queue, handlers = None, logger_level=logging.INFO):
        threading.Thread.__init__(self)
        self.queue = queue
        self.daemon = True
        self.handlers = handlers
        self.logger_level = logger_level

    def run(self):
        """read from the queue and write to the log handlers

        The logging documentation says logging is thread safe, so there
        shouldn't be contention between normal logging (from the main
        process) and this thread.

        Note that we're using the name of the original logger.

        """
        while True:
            try:
                record = self.queue.get()
                # get the logger for this record
                logger = logging.getLogger(record.name)
                self.__check_logger(logger)
                logger.callHandlers(record)
            except (KeyboardInterrupt, SystemExit):
                raise
            except EOFError:
                break
            except:
                traceback.print_exc(file=sys.stderr)

    def __check_logger(self, logger):
        if not logger.handlers:
            logger.setLevel(self.logger_level)
            for handler in self.handlers:
                logger.addHandler(handler)


def init_logger(logger, logger_level, queue):
    logger.setLevel(logger_level)
    if (queue != None):
        for handler in logger.handlers:
            # just a check for my sanity
            assert not isinstance(handler, SubProcessLogHandler)
            logger.removeHandler(handler)
        # add the handler
        handler = SubProcessLogHandler(queue)
        handler.setFormatter(formatter)
        logger.addHandler(handler)


def configure_loggers():
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(DEFAULT_LEVEL)
    ch.setFormatter(formatter)

    logger = logging.getLogger('__main__')
    logger.setLevel(DEFAULT_LEVEL)
    logger.addHandler(ch)

    logger = logging.getLogger(Collector.__name__)
    logger.setLevel(DEFAULT_LEVEL)
    logger.addHandler(ch)
    
    logger = logging.getLogger(search_venues.__name__)
    logger.setLevel(DEFAULT_LEVEL)
    logger.addHandler(ch)
    
    logger = logging.getLogger(get_venues.__name__)
    logger.setLevel(DEFAULT_LEVEL)
    logger.addHandler(ch)

    logger = logging.getLogger(Common.__name__)
    logger.setLevel(DEFAULT_LEVEL)
    logger.addHandler(ch)