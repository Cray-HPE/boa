# Copyright 2019, Cray Inc. All Rights Reserved.

'''
This module performs checks against all required microservices before allowing
a BOA instance to operate on the system. If any of the preflight checks come
back as unhealthy, prevent the agent from operating on the system.

Created on Jun 6, 2019

@author: jsl
'''

import logging
import importlib
from requests.exceptions import HTTPError, ConnectionError
import os

from botocore.exceptions import ClientError, ConnectionClosedError

from . import ServiceNotReady
from .logutil import call_logger
from .arsclient import ENDPOINT as ARS_ENDPOINT
from .imsclient import ENDPOINT as IMS_ENDPOINT
from .bssclient import ENDPOINT as BSS_ENDPOINT
from .capmcclient import ENDPOINT as CAPMC_ENDPOINT
from .cfsclient import SESSIONS_ENDPOINT as CFS_ENDPOINT
from .smd import ENDPOINT as SMD_ENDPOINT
from .s3client import S3BootArtifacts, S3MissingConfiguration
from .connection import requests_retry_session
from cray.boa import NontransientException

LOGGER = logging.getLogger(__name__)
VERIFY = False

class PreflightCheck(object):
    """
    A check against all known and required services for interaction. Depending
    on the action, various microservices do not need to be fully functional.
    """
    ACTIONCHECK = {}
    ACTIONCHECK['boot'] = frozenset(['rootfs', 's3', 'bss', 'capmc', 'cfs', 'smd'])
    ACTIONCHECK['shutdown'] = frozenset(['capmc', 'cfs', 'smd'])
    ACTIONCHECK['reboot'] = ACTIONCHECK['boot']
    ACTIONCHECK['configure'] = frozenset(['cfs'])
    ACTIONCHECK['reconfigure'] = ACTIONCHECK['configure']


    def __init__(self, agent, action, cfs_required=True, rootfs_provisioner=None):
        self.agent = agent
        self.session = requests_retry_session()
        self.action = action.lower()
        self.cfs_required = cfs_required
        self.rootfs_provisioner = rootfs_provisioner
        if self.action not in self.ACTIONCHECK:
            LOGGER.warning("Unsupported action '%s' requested for preflight check; "
                           "it is not implemented.", self.action)

    def get_checks(self, requested_checks=[]):
        if not requested_checks:
            if hasattr(self, '_required_checks'):
                return self._required_checks
        self._required_checks = set()
        if self.action not in self.ACTIONCHECK:
            return self._required_checks
        for checktype in self.ACTIONCHECK[self.action]:
            try:
                if checktype == 'cfs' and not self.cfs_required:
                    continue
                if requested_checks: 
                    if checktype in requested_checks:
                        self._required_checks.add(getattr(self, 'check_%s' %(checktype)))
                else:
                    self._required_checks.add(getattr(self, 'check_%s' %(checktype)))
            except AttributeError:
                LOGGER.warning("Check type '%s' not implemented for action '%s'",
                               checktype, self.action)
        return self._required_checks


    def __call__(self, requested_checks=[]):
        """
        Do a pre-flight check.
        
        Args:
          requested_check (list): A list of the pre-flight checks that we want to do. This allows us 
                                  to be selective rather than redundantly calling some checks.
        """
        for check_function in self.get_checks(requested_checks):
            try:
                check_function()
            except ServiceNotReady as snr:
                LOGGER.warning("Service failed its preflight check: %s", snr)
                raise

    @call_logger
    def check_rootfs(self):
        if not self.rootfs_provisioner:
            return None
        # In this case, we need to dynamically import the rootfs check from
        # the individual implementers.
        module_path = 'cray.boa.rootfs.%s' % (self.rootfs_provisioner)
        try:
            module = importlib.import_module(module_path)
        except ImportError:
            LOGGER.ERROR("Preflight check for %s -- could not find module %s", 
                         self.rootfs_provisioner, module_path)
            return None
        check_function = "check_%s" % self.rootfs_provisioner
        try:
            return getattr(module, check_function)
        except AttributeError:
            LOGGER.warning("Rootfs provisioning mechanism '%s' does not implement health check.",
                           self.rootfs_provisioner)
            return None


    def check_uri(self, uri):
        try:
            response = self.session.get(uri, verify=VERIFY)
            response.raise_for_status()
        except (HTTPError, ConnectionError ) as requests_error:
            raise ServiceNotReady("Service not responsive: %s" % (requests_error)) from requests_error

    def check_ars(self):
        return self.check_uri(os.path.join(ARS_ENDPOINT, 'healthz/ready'))

    def check_bss(self):
        return self.check_uri('%s/' % (BSS_ENDPOINT)) # Responds with Hello World!

    def check_cfs(self):
        return self.check_uri(CFS_ENDPOINT)

    def check_capmc(self):
        return self.check_uri(os.path.join(CAPMC_ENDPOINT, 'get_node_rules'),)

    def check_ims(self):
        return self.check_uri(os.path.join(IMS_ENDPOINT, 'image-artifacts'),)

    def check_smd(self):
        return self.check_uri(os.path.join(SMD_ENDPOINT, 'groups'),)

    def check_s3(self):
        """
        Check that the s3 manifest.json file exists.
        """
        try:
            boot_artifacts = S3BootArtifacts(self.agent.path, self.agent.etag)
            _ = boot_artifacts.object_header
        except (ClientError, ConnectionClosedError) as error:
            raise ServiceNotReady("Service not responsive: %s" % (error)) from error
        except S3MissingConfiguration as error:
            raise NontransientException("%s", error) from error
