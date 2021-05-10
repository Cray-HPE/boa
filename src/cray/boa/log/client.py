# Copyright 2019, 2021 Hewlett Packard Enterprise Development LP
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# (MIT License)

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
