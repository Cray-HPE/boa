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
Created on Apr 29, 2019

@author: jsl
'''

import logging
from functools import wraps

LOGGER = logging.getLogger(__name__)


def call_logger(func):
    """
    This is a decorator which wraps a function and logs the function's call name
    and parameters to the logging stream at the debug level.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        if args or kwargs:
            try:
                msgbuff = ["{}.{} called with ".format(func.__module__, func.__name__)]
            except (AttributeError, TypeError):
                msgbuff = ["Called with "]
            if args:
                msgbuff.append("args: {}".format(args))
            if kwargs:
                msgbuff.append("kwargs: {}".format(kwargs))
            LOGGER.debug(' '.join(msgbuff))
        else:
            LOGGER.debug("%s.%s called.", func.__module__, func.__name__)
        return func(*args, **kwargs)
    return wrapper
