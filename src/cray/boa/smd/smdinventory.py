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
from collections import defaultdict
from requests.exceptions import HTTPError
import logging
import os

from ..logutil import call_logger
from . import ENDPOINT as HSM_ENDPOINT
from ..connection import requests_retry_session
from cray.boa import VERIFY

LOGGER = logging.getLogger(__name__)


class SMDInventory(object):
    """
    SMDInventory handles the generation of a hardware inventory in a similar manner to how the
    dynamic inventory is generated for CFS.  To reduce the number of calls to HSM, everything is
    cached for repeated checks, stored both as overall inventory and separate group types to allow
    use in finding BOA's base list of nodes, and lazily loaded to prevent extra calls when no limit
    is used.
    """

    def __init__(self, partition=None):
        self._partition = partition  # Can be specified to limit to roles/components query

    @property
    def groups(self):
        if not hasattr(self, '_groups'):
            data = self.get('groups')
            groups = {}
            for group in data:
                groups[group['label']] = set(group.get('members', {}).get('ids', []))
            self._groups = groups
        return self._groups

    @property
    def partitions(self):
        if not hasattr(self, '_partitions'):
            data = self.get('partitions')
            partitions = {}
            for partition in data:
                partitions[partition['name']] = set(partition.get('members', {}).get('ids', []))
            self._partitions = partitions
        return self._partitions

    @property
    def roles(self):
        if not hasattr(self, '_roles'):
            params = {}
            if self._partition:
                params['partition'] = self._partition
            data = self.get('State/Components', params=params)
            roles = defaultdict(set)
            for component in data['Components']:
                if 'Role' in component:
                    roles[component['Role']].add(component['ID'])
            self._roles = roles
        return self._roles

    @property
    def inventory(self):
        if not hasattr(self, '_inventory'):
            inventory = {}
            inventory.update(self.groups)
            inventory.update(self.partitions)
            inventory.update(self.roles)
            self._inventory = inventory
            LOGGER.info(self._inventory)
        return self._inventory

    def __contains__(self, key):
        return key in self.inventory

    def __getitem__(self, key):
        return self.inventory[key]

    @call_logger
    def get(self, path, params={}):
        url = os.path.join(HSM_ENDPOINT, path)
        if not hasattr(self, '_session'):
            self._session = requests_retry_session()
        try:
            response = self._session.get(url, params=params, verify=VERIFY)
            response.raise_for_status()
        except HTTPError as err:
            LOGGER.error("Failed to get '{}': {}".format(url, err))
            raise
        try:
            return response.json()
        except ValueError:
            LOGGER.error("Couldn't parse a JSON response: {}".format(response.text))
            raise
