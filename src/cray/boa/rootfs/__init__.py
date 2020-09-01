# Copyright 2019, Cray Inc. All Rights Reserved.

'''
Created on Apr 29, 2019

@author: jsl
'''

import logging
from cray.boa import NontransientException
from ..imsclient import get_image_artifacts
from ..arsclient import get_ars_download_uris

LOGGER = logging.getLogger(__name__)


class ProvisionerNotImplemented(NontransientException):
    """
    Raised when a user requests a provisioning mechanism that isn't yet supported
    by BOA.
    """


class RootfsProvisioner(object):
    PROTOCOL = None
    DELIMITER = ':'
    """
    This class is intended to be inherited by various kinds of root provisioning
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

        # Obtain and cache this value so that we don't hit resource intensive
        # operations repeatedly
        pv = self.provisioner_field
        if pv:
            fields.append(pv)

        if self.provisioner_field_id:
            fields.append(self.provisioner_field_id)

        if self.agent._rootfs_provisioner_passthrough:
            fields.append(self.agent._rootfs_provisioner_passthrough)

        if fields:
            return "root={}".format(self.DELIMITER.join(fields))
        else:
            return ''

    @property
    def provisioner_field(self):
        return None

    @property
    def provisioner_field_id(self):
        return None

    @property
    def ars_rootfs_id(self):
        return self.ars_img_id['ars_artifact_id']

    @property
    def ars_img_id(self):
        """
        Returns the rootfs image's ARS artifact ID. There is assumed to be only one.
        """
        artifacts = get_image_artifacts(self.agent._ims_image_id)
        try:
            rootfs_artifacts =  list(filter(lambda image: image['artifact_type'] == 'rootfs',
                                       artifacts))
            return rootfs_artifacts[0]
        except IndexError:
            LOGGER.error("No rootfs image associated with this image ID.")
            raise