#
# MIT License
#
# (C) Copyright 2019-2022 Hewlett Packard Enterprise Development LP
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
Created on February 7th, 2020

@author: jason.sollom
'''

import logging

from cray.boa import NontransientException
from ..logutil import call_logger
from cray.boa.bootimagemetadata.s3bootimagemetadata import S3BootImageMetaData

LOGGER = logging.getLogger(__name__)

class BootImageMetaDataUnknown(NontransientException):
    """
    Raised when a user requests a Provider provisioning mechanism that is not known
    by BOA.
    """

class BootImageMetaDataFactory(object):
    """
    Conditionally create new instances of the BootImageMetadata based on
    the type of the BootImageMetaData specified
    """
    def __init__(self, agent):
        self.agent = agent
    
    @call_logger
    def __call__(self):
        if self.agent.path_type:
            if self.agent.path_type == 's3':
                return S3BootImageMetaData(self.agent)
            else:
                raise BootImageMetaDataUnknown("No BootImageMetaData class for "
                                                      "type %s", self.agent.path_type)