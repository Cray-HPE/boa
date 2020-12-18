# Copyright 2019-2020 Hewlett Packard Enterprise Development LP

'''
Provisioning mechanism unique to the ContentProjectionService; this is software
that is often installed as part of Cray CME images in both standard, enhanced
and premium offerings; the underlying implementation of CPS may be handled by
another protocol (iSCSI or DVS) depending on the product.

Created on Feb 5th, 2020

@author: jason.sollom
'''

from requests.exceptions import HTTPError
import logging
import os

from . import RootfsProvider
from .. import PROTOCOL, ServiceNotReady
from ..connection import requests_retry_session

LOGGER = logging.getLogger(__name__)
SERVICE_NAME = 'cray-cps'
VERSION = 'v1'
ENDPOINT = '%s://%s/%s' % (PROTOCOL, SERVICE_NAME, VERSION)


class CPSS3Provider(RootfsProvider):
    PROTOCOL = 'craycps-s3'

    @property
    def provider_field(self):
        return self.agent.artifact_paths['rootfs']

    @property
    def provider_field_id(self):
        return self.agent.artifact_paths['rootfs_etag']

    @property
    def nmd_field(self):
        """
        The value to add to the kernel boot parameters for Node Memory Dump (NMD)
        parameter.
        """
        fields = []
        if self.provider_field:
            fields.append("url=%s" % self.provider_field)
        if self.provider_field_id:
            fields.append("etag=%s" % self.provider_field_id)
        if fields:
            return "nmd_data={}".format(",".join(fields))
        else:
            return ''


def check_cpss3(session=None):
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
