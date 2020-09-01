# Copyright 2019, Cray Inc. All Rights Reserved.

'''
Created on Apr 29, 2019

@author: jsl
'''

import importlib
import logging

from . import ProvisionerNotImplemented
from ..logutil import call_logger

LOGGER = logging.getLogger(__name__)

class ProvisionerFactory(object):
    """
    Conditionally creates new instances of rootfilesystem provisioners based on
    a given agent instance.
    """
    def __init__(self, agent):
        self.agent = agent

    @call_logger
    def __call__(self):
        provisioner_name = self.agent._rootfs_provisioner.lower()        

        if provisioner_name:
            # When a provisioning protocol is specified...
            provisioning_module = 'cray.boa.rootfs.{}'.format(provisioner_name)
            provisioning_classname = '{}Provisioner'.format(provisioner_name.upper())
        else:
            # none specified or blank
            provisioning_module = 'cray.boa.rootfs'
            provisioning_classname = 'RootfsProvisioner'

        # Import the provisioning model
        try:
            module = importlib.import_module(provisioning_module)
        except ModuleNotFoundError as mnfe:
            # This is pretty much unrecoverable at this stage of development; make note and raise
            LOGGER.error("Provisioning mechanism '{}' not yet implemented or not found.".format(provisioner_name))
            raise ProvisionerNotImplemented(mnfe) from mnfe

        ClassDef = getattr(module, provisioning_classname)
        return ClassDef(self.agent)