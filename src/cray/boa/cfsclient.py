# Copyright 2019-2021 Hewlett Packard Enterprise Development LP
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
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# (MIT License)

from collections import defaultdict
from requests.exceptions import HTTPError
import logging
import subprocess
import time
import os
import tempfile
import uuid

from cray.boa import NontransientException
from . import PROTOCOL
from .logutil import call_logger
from .connection import requests_retry_session

LOGGER = logging.getLogger(__name__)
SERVICE_NAME = 'cray-cfs-api'
V1_ENDPOINT = "%s://%s" % (PROTOCOL, SERVICE_NAME)
V2_ENDPOINT = "%s://%s/v2" % (PROTOCOL, SERVICE_NAME)
SESSIONS_ENDPOINT = "%s/sessions" % V2_ENDPOINT
COMPONENTS_ENDPOINT = "%s/components" % V2_ENDPOINT
OPTIONS_ENDPOINT = "%s/options" % V2_ENDPOINT
OPTIONS_V1_ENDPOINT = "%s/options" % V1_ENDPOINT
CONFIGURATIONS_ENDPOINT = "%s/configurations" % V2_ENDPOINT


class CFSException(NontransientException):
    """
    An exception while dealing with CFS service.
    """


class CFSTimeout(CFSException):
    """
    When we've waited too long for CFS to complete.
    """


class CFSExhaustedRetries(CFSException):
    """
    When one or more components are unable to be attempted any further because
    they have reached the maximum number of configuration attempts.
    """


class CfsClient(object):
    """
    This is a Configuration Framework Service (CFS) Client object.  It is a
    wrapper around the CFS api calls.
    """
    PATCH_BATCH_SIZE = 1000

    def __init__(self):
        self._session = requests_retry_session()

    def clear_configuration(self, node_ids):
        self._patch_desired_config(node_ids, '')

    @call_logger
    def set_configuration(self, node_ids, configuration, enabled=False, tags={}):
        self._patch_desired_config(node_ids, configuration, enabled=enabled, tags=tags)

    @call_logger
    def _patch_desired_config(self, node_ids, desired_config, enabled=False, tags={}):
        data = []
        for node_id in node_ids:
            data.append({
                'id': node_id,
                'enabled': enabled,
                'desiredConfig': desired_config,
                'tags': tags
            })
            if len(data) >= self.PATCH_BATCH_SIZE:
                self._session.patch(COMPONENTS_ENDPOINT, json=data)
                data = []
        if data:
            response = self._session.patch(COMPONENTS_ENDPOINT, json=data)
            try:
                response.raise_for_status()
            except HTTPError as err:
                LOGGER.error("Failed asking CFS to configure nodes: %s", err)
                pass

    def get_component(self, node_id):
        url = "%s/%s" % (COMPONENTS_ENDPOINT, node_id)
        response = self._session.get(url)
        response.raise_for_status()
        return response.json()

    def get_components(self, **kwargs):
        response = self._session.get(COMPONENTS_ENDPOINT, params=kwargs)
        response.raise_for_status()
        return response.json()

    @call_logger
    def create_configuration(self, commit=None, branch=None, repo_url=None, playbook=None):
        if not repo_url:
            repo_url = self.get_default_clone_url()
        if not playbook:
            playbook = self.get_default_playbook()

        if not (commit or branch):
            raise Exception('For configuration either commit or branch must be set.')

        configurations = self.get_configurations()
        for config in configurations:
            if config.get('name').startswith('boa') and len(config.get('layers', [])) == 1:
                layer = config['layers'][0]
                if (layer.get('playbook') == playbook and
                        layer.get('cloneUrl') == repo_url and
                        (not commit or layer.get('commit') == commit) and
                        (not branch or layer.get('branch') == branch)):
                    return config.get('name')

        layer = {
            'cloneUrl': repo_url,
            'playbook': playbook
        }
        if commit:
            layer['commit'] = commit
        if branch:
            layer['branch'] = branch
        data = {
            'layers': [layer],
        }
        name = 'boa-' + str(uuid.uuid4())
        self.update_configuration(name, data)
        return name

    def get_configurations(self):
        response = self._session.get(CONFIGURATIONS_ENDPOINT)
        response.raise_for_status()
        data = response.json()
        return data

    def update_configuration(self, config_id, data):
        url = "%s/%s" % (CONFIGURATIONS_ENDPOINT, config_id)
        response = self._session.put(url, json=data)
        response.raise_for_status()

    def get_default_clone_url(self):
        response = self._session.get(OPTIONS_V1_ENDPOINT)
        response.raise_for_status()
        data = response.json()
        return data['defaultCloneUrl']

    def get_default_playbook(self):
        response = self._session.get(OPTIONS_ENDPOINT)
        response.raise_for_status()
        data = response.json()
        return data['defaultPlaybook']


def wait_for_configuration(boot_set_agent, maximum_duration=1800, check_interval=5,
                           success_threshold=1.0):
    """
    Given a set of nodes in a Boot Set agent, wait for them to all reach fully configured status.
    We will exit early if the failures exceed failure_threshold. 
    Args:
      boot_set_agent (BootSetAgent): The BootSetAgent contains a cfs client, a set of nodes,
          and appropriate status reporting/aggregating methods 
      maximum_duration: The period of time, in seconds, that we wait for components to
        become configured. When set to zero, wait indefinitely
      check_interval: The period of time between calls to CFS for component information
      success_threshold: This float value defines the percentage of nodes that must complete
        successfully in order for the configuration to be deemed complete. When the number of
        nodes have explicitly failed configuration (as indicated by those nodes reaching their
        maximum number of attempts for configuration), the function raises a configuration exception.
    State Changes:
     - Logged messages to stdout
    Raises:
     - CFSException
     - CFSExhaustedRetries
     - CFSTimeout
    Returns: None
    """
    if not maximum_duration:
        # Give them about 100 years, they may be running KNL.
        end_time = time.time() + (60 * 60 * 24 * 365 * 100)
    else:
        end_time = time.time() + maximum_duration
    check_interval = os.getenv("CFS_COMPLETION_SLEEP_INTERVAL", check_interval)
    nodes = set(boot_set_agent.nodes)
    nodes_count = len(nodes)
    allowable_failures = (1.0 - success_threshold) * nodes_count
    nodes_required_for_success = nodes_count - allowable_failures
    last_status = None
    last_status_time = time.time()
    remaining_components = nodes
    successful_components_count = 0
    failed_components_count = 0
    while time.time() < end_time:
        # GET COMPONENT INFORMATION
        # We can only request so many ids at a time or the request is too large.
        # A chunk size of 25 keeps us below the 4096 byte maximum request size when using xnames.
        # Once CFS/BOS supports tagging components with the "owner", this can be replaced
        # with querying on the session name/tag, although paging on large responses may be needed.
        seq = list(remaining_components)
        size = 25
        components_config_map = defaultdict(set)
        components = set()
        for chunk in [seq[pos:pos + size] for pos in range(0, len(seq), size)]:
            components_data = boot_set_agent.cfs_client.get_components(ids=','.join(chunk))
            components |= {component['id'] for component in components_data}
            for component in components_data:
                components_config_map[component.get(
                    'configurationStatus', 'undefined')].add(component['id'])

        # LOG COMPONENT STATUS INFORMATION
        # Report Completed Nodes' Status
        successful_components = components_config_map['configured']
        successful_components_count += len(successful_components)
        boot_set_agent.boot_set_status['configure'].move_nodes(successful_components,
                                                               'in_progress', 'succeeded')
        # Report Failed Nodes' Status
        errors = {}
        failed_components = components_config_map['failed']
        if failed_components:
            errors['CFS failed and exhausted all retries'] = list(failed_components)
        removed_components = remaining_components - components
        if removed_components:
            # Can occur if the component was removed from CFS
            errors['Status could not be retrieved from CFS'] = list(removed_components)
            failed_components |= removed_components
        for status, status_components in components_config_map.items():
            if status not in ['configured', 'failed', 'pending']:
                # Can occur if the components desired configuration was unset
                msg = 'Component entered the unhandled status "{}"'.format(status)
                errors[msg] = list(status_components)
                failed_components |= status_components
        if errors:
            boot_set_agent.boot_set_status.update_errors('configure', errors)
        failed_components_count += len(failed_components)
        boot_set_agent.boot_set_status['configure'].move_nodes(failed_components,
                                                               'in_progress', 'failed')

        # Update Boot Set Agent's failed components
        boot_set_agent.failed_nodes |= failed_components

        # CHECK EXIT CONDITIONS
        if failed_components_count > allowable_failures:
            msg = """Maximum number of nodes have failed configuration criteria threshold;
                  CFS may still be attempting to configure any remaining nodes (if any).
                  These nodes failed configuration: %s""" % (', '.join(sorted(failed_components)))
            raise CFSExhaustedRetries(msg)
        # If all components have reached a completed state, exit.
        remaining_components = components_config_map['pending']
        if not remaining_components:
            return

        # LOG RUN STATUS
        # We haven't run out of time, we haven't explicitly failed, and we haven't succeeded...
        # output some status information periodically so our users don't go crazy wondering what BOA is doing
        new_status_msgs = ['%s unconfigured nodes' % (len(remaining_components))]
        if failed_components_count:
            new_status_msgs.append('%s nodes failed configuration' % (failed_components_count))
        if successful_components_count:
            new_status_msgs.append('%s nodes completed configuration' % (successful_components_count))
        new_status_msg = ', '.join(new_status_msgs)
        if new_status_msg != last_status or time.time() > last_status_time + 15:
            LOGGER.info(new_status_msg)
            last_status_time = time.time()
            last_status = new_status_msg
        time.sleep(check_interval)

    # Here, we've found ourselves in the unenviable position where we have less than
    # 100% of the nodes configured, and our time has run out.
    if successful_components_count >= nodes_required_for_success:
        LOGGER.info("After %s seconds, %s nodes have been configured and pass the %.2f percent provided threshold.",
                    maximum_duration, successful_components_count, success_threshold)

        if len(remaining_components) > 5:
            LOGGER.info("%s nodes may still be under configuration.", len(remaining_components))
        else:
            LOGGER.info("These nodes may still have active configuration actions: %s"
                        , ', '.join(sorted(remaining_components)))
        return
    else:
        raise CFSTimeout("%s nodes failed to configure with CFS within the specified time."
                         % (nodes_count - successful_components_count))
