# Copyright 2020, Cray Inc. All Rights Reserved.

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
    Raised when a user requests a provisioning mechanism that is not known
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
        if self.agent._path_type:
            if self.agent._path_type == 's3':
                return S3BootImageMetaData(self.agent)
            else:
                raise BootImageMetaDataUnknown("No BootImageMetaData class for "
                                                      "type %s", self.agent._path_type)