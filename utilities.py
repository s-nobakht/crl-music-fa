import os
from urllib.parse import urlparse
from os.path import splitext


# This class includes some utility classes and functions
class ResumeOperation(object):
    def __init__(self, fileAddress='', flushLimit=0):
        self.fileAddress = fileAddress
        self.flushLimit = flushLimit
        self.counter = 0
        self.actionCounter = 0

        if self.fileAddress:
            if os.path.exists(self.fileAddress):
                with open(self.fileAddress, 'r') as myFile:
                    fileText = myFile.read()
                if fileText:
                    self.counter = int(fileText)
                else:
                    self.counter = 0
                myFile.close()
            else:
                self.counter = 0

    def getLastNumber(self):
        return self.counter

    def setLastNumber(self, number):
        self.counter = number
        self.actionCounter += 1
        if self.flushLimit <= 0:
            self._saveToFile(self.counter)
            self.actionCounter = 0
        elif self.actionCounter >= self.flushLimit:
            self._saveToFile(self.counter)
            self.actionCounter = 0

    def resetCounter(self):
        self.counter = 0
        self.actionCounter = 0
        self._saveToFile(self.counter)

    def _saveToFile(self, number):
        # print(os.getcwd())
        f = open(self.fileAddress, 'w')
        # f.write(number)
        print(number, file=f)
        f.close()


class connection(object):
    def __init__(self, proxy='', baseURL='', requestType='normal', methodType='POST'):
        self.baseURL = baseURL
        self.proxy = proxy
        self.requestType = requestType
        self.contentType = 'application/x-www-form-urlencoded; charset=UTF-8'
        self.userAgent = 'Mozilla/5.0'
        self.methodType = 'POST'

        from requests import Session
        self.session = Session()

        # if proxy is enabled
        if self.proxy:
            proxies = {'https': self.proxy}
            self.session.proxies = proxies
            # HEAD requests ask for *just* the headers, which is all you need to grab the
            # session cookie

        self.session.head(self.baseURL)

    def setRequestType(self, reqType):
        self.requestType = reqType

    def setMethodType(self, methodType):
        self.methodType = methodType

    def refreshConnection(self):
        from requests import Session
        self.session = Session()
        self.session.head(self.baseURL)


    def getData(self, reqURL, reqData={}):
        reqHeaders = {  'User-Agent': self.userAgent,
                        'Content-Type': self.contentType,
                        'Referer': self.baseURL}
        if self.requestType == 'xhr':
            reqHeaders['X-Requested-With'] = 'XMLHttpRequest'

        if reqData:
            if self.methodType == 'POST':
                self.response = self.session.post(
                    url=reqURL,
                    data=reqData,
                    headers=reqHeaders
                )
            else:
                self.response = self.session.get(
                    url=reqURL,
                    data=reqData,
                    headers=reqHeaders
                )
        else:
            if self.methodType == 'POST':
                self.response = self.session.post(
                    url=reqURL,
                    headers=reqHeaders
                )
            else:
                self.response = self.session.get(
                    url=reqURL,
                    headers=reqHeaders
                )

        return self.response


# get extension of file in a URL
def get_ext(url):
    """Return the filename extension from url, or ''."""
    parsed = urlparse(url)
    root, ext = splitext(parsed.path)
    return ext  # or ext[1:] if you don't want the leading '.'


# this function define seperated log track and returns
# a logger object, which can be used in seperated functions and modules
def defineLogger(fileName, postFix='', logToConsole=True):
    import logging
    logger = logging.getLogger(__name__ + "_" + postFix)
    logger.setLevel(logging.INFO)
    # create a file handler
    handler = logging.FileHandler(fileName)
    handler.setLevel(logging.INFO)
    # create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    if logToConsole:
        logFormatter = logging.Formatter('%(levelname)s - %(message)s')
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(logFormatter)
        logger.addHandler(consoleHandler)
    # add the handlers to the logger
    logger.addHandler(handler)
    return logger