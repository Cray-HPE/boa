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
        It follows this format.
        provider_field<DELIMITER>provider_field_id<DELIMITER>rootfs_provider_passthrough
        The fields are only present if they exist; otherwise, they are not present and
        the delimiter following that field is not present.
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

