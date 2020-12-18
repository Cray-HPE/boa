# Copyright 2019-2020 Hewlett Packard Enterprise Development LP

'''
Created on Apr 29, 2019

@author: jsl
'''

import importlib
import logging

from . import ProviderNotImplemented
from ..logutil import call_logger

LOGGER = logging.getLogger(__name__)

class ProviderFactory(object):
    """
    Conditionally creates new instances of rootfilesystem providers based on
    a given agent instance.
    """
    def __init__(self, agent):
        self.agent = agent

    @call_logger
    def __call__(self):
        provider_name = self.agent.rootfs_provider.lower()

        if provider_name:
            # When a provisioning protocol is specified...
            provider_module = 'cray.boa.rootfs.{}'.format(provider_name)
            provider_classname = '{}Provider'.format(provider_name.upper())
        else:
            # none specified or blank
            provider_module = 'cray.boa.rootfs'
            provider_classname = 'RootfsProvider'

        # Import the Provider's provisioning model
        try:
            module = importlib.import_module(provider_module)
        except ModuleNotFoundError as mnfe:
            # This is pretty much unrecoverable at this stage of development; make note and raise
            LOGGER.error("Provider provisioning mechanism '{}' not yet implemented or not found.".format(provider_name))
            raise ProviderNotImplemented(mnfe) from mnfe

        ClassDef = getattr(module, provider_classname)
        return ClassDef(self.agent)