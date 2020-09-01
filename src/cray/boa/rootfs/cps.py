# Copyright 2019, Cray Inc. All Rights Reserved.

'''
Provisioning mechanism unique to the ContentProjectionService; this is software
that is often installed as part of Cray CME images in both standard, enhanced
and premium offerings; the underlying implementation of CPS may be handled by
another protocol (iSCSI or DVS) depending on the product.

Created on Apr 29, 2019

@author: jsl
'''

from requests.exceptions import HTTPError
import logging
import os

from . import RootfsProvisioner
from .. import PROTOCOL, ServiceNotReady
from ..connection import requests_retry_session

LOGGER = logging.getLogger(__name__)
SERVICE_NAME = 'cray-cps'
VERSION = 'v1'
ENDPOINT = '%s://%s/%s' % (PROTOCOL, SERVICE_NAME, VERSION)


class CPSProvisioner(RootfsProvisioner):
    PROTOCOL = 'craycps'

    @property
    def provisioner_field(self):
        return self.ars_rootfs_id


def check_cps(session=None):
    """
    A call to check on the health of the CPS microservice.
    """
    session = session or requests_retry_session()
    uri = os.path.join(ENDPOINT, 'contents')
    try:
        response = session.get(uri)
        response.raise_for_status()
    except HTTPError as he:
        raise ServiceNotReady(he) from he
