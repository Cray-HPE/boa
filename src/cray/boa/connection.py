#
# MIT License
#
# (C) Copyright 2019, 2021-2022, 2024 Hewlett Packard Enterprise Development LP
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
The purpose of this module is to provide a unified way of creating or
updating a requests retry connection whenever interacting with a
microservice; these connections are exposed as a requests session
with an HTTP retry adapter attached to it.

These changes help alleviate Istio503 and temporary service outage
related calls, where at all possible.

Created on Aug 23, 2019

@author: jsl
'''

from functools import partial
import logging

from requests_retry_session import requests_retry_session as base_requests_retry_session

from cray.boa import PROTOCOL

LOGGER = logging.getLogger(__name__)


requests_retry_session = partial(base_requests_retry_session, retries=128, backoff_factor=0.01, protocol=PROTOCOL)


def wait_for_istio_proxy():
    """
    Wait for the Istio proxy to become available.
    """
    pass


if __name__ == '__main__':
    import sys
    lh = logging.StreamHandler(sys.stdout)
    lh.setLevel(logging.DEBUG)
    LOGGER.addHandler(lh)
    LOGGER.setLevel(logging.DEBUG)
    LOGGER.info("Running")
    retry_session = requests_retry_session()
    LOGGER.info(retry_session.get('https://httpstat.us/200').status_code)
    retry_session = requests_retry_session(retries=5)
    LOGGER.info(retry_session.get('https://httpstat.us/503').status_code)
