# Copyright 2019, Cray Inc. All Rights Reserved.

'''
Created on Oct 29, 2019

@author: jsl
'''
from cray.boa import NontransientException


class TemplateException(NontransientException):
    """
    The format of the template is missing required fields.
    """
