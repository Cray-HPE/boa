# Copyright 2019-2020 Hewlett Packard Enterprise Development LP

'''
Created on Apr 29, 2019

@author: jsl
'''

import logging
from cray.boa import NontransientException

LOGGER = logging.getLogger(__name__)


class ProviderNotImplemented(NontransientException):
    """
    Raised when a user requests a Provider Provisioning mechanism that isn't yet supported
    by BOA.
    """


class RootfsProvider(object):
    PROTOCOL = None
    DELIMITER = ':'
    """
    This class is intended to be inherited by various kinds of root Provider provisioning
    mechanisms.
    """

    def __init__(self, agent):
        """
        Given an agent, extrapolate the required boot parameter value.
        """
        self.agent = agent

    def __str__(self):
        """
        The value to add to the boot parameter.
        """
        fields = []
        if self.PROTOCOL:
            fields.append(self.PROTOCOL)

        if self.provider_field:
            fields.append(self.provider_field)

        if self.provider_field_id:
            fields.append(self.provider_field_id)

        if self.agent.rootfs_provider_passthrough:
            fields.append(self.agent.rootfs_provider_passthrough)

        if fields:
            return "root={}".format(self.DELIMITER.join(fields))
        else:
            return ''

    @property
    def provider_field(self):
        return None

    @property
    def provider_field_id(self):
        return None

    @property
    def nmd_field(self):
        """
        The value to add to the kernel boot parameters for Node Memory Dump (NMD)
        parameter.
        """
        return None

