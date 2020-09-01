# Copyright 2019, Cray Inc. All Rights Reserved.

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
