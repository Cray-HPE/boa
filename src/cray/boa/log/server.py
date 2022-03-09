#
# MIT License
#
# (C) Copyright 2019, 2021-2022 Hewlett Packard Enterprise Development LP
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
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
'''
This module allows for the instantiation and use of a multiprocess safe background
threaded service. This service aggregates logs from individual running processes
and unifies them into a standard location.

Created on Jul 3, 2019

@author: jsl
'''

import logging
import logging.handlers
import socketserver
import msgpack
import time
import threading

from socket import gethostbyname
from cray.boa.log import DEFAULT_PORT, ENCODING


class LogRecordStreamHandler(socketserver.StreamRequestHandler):
    """
    Handler for a streaming logging request.

    This basically logs the record using whatever logging policy is
    configured locally. Typically, this is the global root logger (but can handle
    any defined logger in the current defined namespace).
    """

    def handle(self):
        """
        Handle multiple requests - each expected to be a 4-byte length,
        followed by the LogRecord in pickle format. Logs the record
        according to whatever policy is configured locally.
        """
        unp = msgpack.Unpacker()
        while True:
            r = self.request.recv(1000)
            if not r:
                break
            unp.feed(r)
            for obj in unp:
                sanitized = {}
                for key, val in obj.items():
                    if isinstance(val, bytes):
                        val = str(val, ENCODING)
                    sanitized[str(key, ENCODING)] = val
                record = logging.makeLogRecord(sanitized)
                self.handleLogRecord(record)

    def handleLogRecord(self, record):
        # if a name is specified, we use the named logger rather than the one
        # implied by the record; otherwise use the record.name.
        try:
            if self.server.logname is not None:
                name = self.server.logname
            else:
                name = record.name
        except AttributeError:
            name = record.name
        logger = logging.getLogger(name)
        # N.B. EVERY record gets logged. This is because Logger.handle
        # is normally called AFTER logger-level filtering. If you want
        # to do filtering, do it at the client end to save wasting
        # cycles and network bandwidth!
        logger.handle(record)


class LogRecordSocketReceiver(socketserver.ThreadingTCPServer):
    """
    Simple TCP socket-based logging receiver suitable for small loads.
    """
    allow_reuse_address = 1

    def __init__(self,
                 host=gethostbyname(''),
                 port=DEFAULT_PORT,
                 handler=LogRecordStreamHandler):
        socketserver.ThreadingTCPServer.__init__(self, (host, port), handler)
        self.timeout = 1


def test_service():
    logging.basicConfig(format='%(levelname)-8s - %(message)s')
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    tcpserver = LogRecordSocketReceiver()
    print('About to start TCP server...')
    threading.Thread(target=tcpserver.serve_forever).start()
    print("launched")
    time.sleep(20)
    tcpserver.shutdown()
    print("service finished")


if __name__ == '__main__':
    test_service()
