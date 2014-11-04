import traceback
import threading
import logging
import sys
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

FORMATTER = logging.Formatter("%(levelname)s: %(asctime)s - %(name)s - %(process)s - %(message)s")

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

    def __init__(self, gmail_login, password, email_from, email_to):
        logging.Handler.__init__(self)
        self.gmail_login = gmail_login
        self.password = password
        self.emailFrom = email_from
        self.emailTo = email_to

    def emit(self, record):
        msg = MIMEMultipart('alternative')
        msg['Subject'] = "GeoSpottingServer Foursquare" + str(record.levelname)
        msg['From'] = self.emailFrom
        msg['To'] = self.emailTo
        text = str(record.msg)
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

    def __init__(self, queue, handlers = None):
        threading.Thread.__init__(self)
        self.queue = queue
        self.daemon = True
        self.handlers = handlers
        self.loggers = {}

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
                logger = self.get_logger(record.name)
                logger.callHandlers(record)
            except (KeyboardInterrupt, SystemExit):
                raise
            except EOFError:
                break
            except:
                traceback.print_exc(file=sys.stderr)

    def get_logger(self, name):
        if name in self.loggers:
            return self.loggers[name]
        logger = logging.getLogger(name)
        self.loggers[name] = logger
        for handler in logger.handlers:
            logger.removeHandler(handler)
        for handler in self.handlers:
            logger.addHandler(handler)
        return logger


def get_logger(name=None, queue=None):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    if queue:
        for handler in logger.handlers:
            # just a check for my sanity
            assert not isinstance(handler, SubProcessLogHandler)
            logger.removeHandler(handler)
        # add the handler
        handler = SubProcessLogHandler(queue)
        handler.setFormatter(FORMATTER)
        logger.addHandler(handler)
    return logger
