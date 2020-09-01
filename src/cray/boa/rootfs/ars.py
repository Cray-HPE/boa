# Copyright 2019, Cray Inc. All Rights Reserved.

'''
Provisioning mechanism unique to the Artifact Repository Service; this is software
that is often installed as part of Cray CME images in both standard, enhanced
and premium offerings; the underlying dracut module to be used with this
provisioning mechanism pulls the whole of the rootfs into RAM directly from ARS,
and then uses it from memory.

Created on Jul 1, 2019

@author: jsl
'''

from requests.exceptions import HTTPError
import logging
import os

from . import RootfsProvisioner
from .. import PROTOCOL, ServiceNotReady
from ..arsclient import SERVICE_NAME
from ..connection import requests_retry_session

LOGGER = logging.getLogger(__name__)
ENDPOINT = '%s://%s' % (PROTOCOL, SERVICE_NAME)


class ARSProvisioner(RootfsProvisioner):
    PROTOCOL = 'crayars'

    @property
    def provisioner_field(self):
        return "%s:%s" %(self.ars_rootfs_id, 'api-gw-service-nmn.local')


def check_ars(session=None):
    """
    A call to check on the health of the ARS microservice.
    """
    session = session or requests_retry_session()
    uri = os.path.join(ENDPOINT, 'artifacts')
    try:
        response = session.get(uri)
        response.raise_for_status()
    except HTTPError as he:
        raise ServiceNotReady(he) from he
