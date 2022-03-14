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
from .bssclient import ENDPOINT as BSS_ENDPOINT
from .capmcclient import ENDPOINT as CAPMC_ENDPOINT
from .cfsclient import SESSIONS_ENDPOINT as CFS_ENDPOINT
from .smd import ENDPOINT as SMD_ENDPOINT
from .s3client import S3Object, S3MissingConfiguration
from .connection import requests_retry_session

LOGGER = logging.getLogger(__name__)
VERIFY = False


class PreflightCheck(object):
    """
    A check against all known and required services for interaction. Depending
    on the action, various microservices do not need to be fully functional.
    """
    ACTIONCHECK = {}
    ACTIONCHECK['boot'] = frozenset(['rootfs', 's3', 'bss', 'capmc', 'smd', 'rootfs'])
    ACTIONCHECK['shutdown'] = frozenset(['capmc', 'smd'])
    ACTIONCHECK['reboot'] = ACTIONCHECK['boot']
    ACTIONCHECK['configure'] = frozenset(['cfs'])
    ACTIONCHECK['reconfigure'] = ACTIONCHECK['configure']

    def __init__(self, agent, action, rootfs_provider=None):
        self.agent = agent
        self.session = requests_retry_session()
        self.action = action.lower()
        self.rootfs_provider = rootfs_provider
        if self.action not in self.ACTIONCHECK:
            LOGGER.warning("Unsupported action '%s' requested for preflight check; "
                           "it is not implemented.", self.action)

        # Build up a set of checks to perform; this is a one-time initialization
        self.checks = set()
        for checktype in self.ACTIONCHECK[self.action]:
            try:
                self.checks.add(getattr(self, 'check_%s' % (checktype)))
            except AttributeError:
                LOGGER.warning("Check type '%s' not implemented for action '%s'",
                               checktype, self.action)
        if self.agent.cfs_enabled:
            self.checks.add(self.check_cfs)

    def __call__(self):
        """
        Do a pre-flight check.
        """
        if self.checks:
            LOGGER.info("Running preflight checks.")
        else:
            return
        for check_function in self.checks:
            try:
                check_function()
            except ServiceNotReady as snr:
                LOGGER.warning("Preflight check %s failed: %s", check_function.__name__, snr)
        LOGGER.info("Preflight checks done.")

    @call_logger
    def check_rootfs(self):
        if not self.rootfs_provider:
            return None
        # In this case, we need to dynamically import the rootfs check from
        # the individual implementers.
        module_path = 'cray.boa.rootfs.%s' % (self.rootfs_provider)
        try:
            module = importlib.import_module(module_path)
        except ImportError:
            LOGGER.ERROR("Preflight check for %s -- could not find module %s",
                         self.rootfs_provider, module_path)
            return None
        check_function = "check_%s" % self.rootfs_provider
        try:
            return getattr(module, check_function)
        except AttributeError:
            LOGGER.warning("Rootfs provider provisioning mechanism '%s' does not implement health check.",
                           self.rootfs_provider)
            return None

    def check_uri(self, uri):
        try:
            response = self.session.get(uri, verify=VERIFY)
            response.raise_for_status()
        except (HTTPError, ConnectionError) as requests_error:
            raise ServiceNotReady("Service not responsive: %s" % (requests_error)) from requests_error

    def check_bss(self):
        return self.check_uri('%s/' % (BSS_ENDPOINT))  # Responds with Hello World!

    def check_cfs(self):
        return self.check_uri(CFS_ENDPOINT)

    def check_capmc(self):
        return self.check_uri(os.path.join(CAPMC_ENDPOINT, 'get_node_rules'),)

    def check_smd(self):
        return self.check_uri(os.path.join(SMD_ENDPOINT, 'groups'),)

    def check_s3(self):
        """
        Check that the s3 manifest.json file exists.
        """
        try:
            boot_artifacts = S3Object(self.agent.path, self.agent.etag)
            _ = boot_artifacts.object_header
        except (ClientError, ConnectionClosedError, S3MissingConfiguration) as error:
            raise ServiceNotReady("Service not responsive: %s" % (error)) from error

