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