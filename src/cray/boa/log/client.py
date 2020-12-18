# Copyright 2019, Cray Inc. All Rights Reserved.

'''
Created on Jul 3, 2019

@author: jsl
'''

import logging.handlers
import msgpack
from socket import gethostbyname

from cray.boa.log import DEFAULT_PORT


class MsgpackHandler(logging.handlers.SocketHandler):
    def __init__(self, host=gethostbyname(''), port=DEFAULT_PORT):
        logging.handlers.SocketHandler.__init__(self,host,port)

    def makePickle(self,record):
        # Use msgpack instead of pickle, for increased safety and portability
        # between versions of python
        return msgpack.packb(record.__dict__)


if __name__ == '__main__':
    import logging
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    mph = MsgpackHandler()
    logger.addHandler(mph)
    logger.debug("hello mom")
    logger.info("goodbye-cruel-world")
    print("client finished")
    logger.info('hello')
