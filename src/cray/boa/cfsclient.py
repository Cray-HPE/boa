# © Copyright 2019-2020 Hewlett Packard Enterprise Development LP

from collections import defaultdict
from requests.exceptions import HTTPError
import logging
import subprocess
import time
import os

from cray.boa import NontransientException
from cray.boa.bosclient import report_status_by_bootset, report_errors_by_bootset
from . import PROTOCOL
from .logutil import call_logger
from .connection import requests_retry_session

LOGGER = logging.getLogger(__name__)
SERVICE_NAME = 'cray-cfs-api'
ENDPOINT = "%s://%s/apis/cfs" % (PROTOCOL, SERVICE_NAME)
SESSIONS_ENDPOINT = "%s/sessions" % (ENDPOINT)
COMPONENTS_ENDPOINT = "%s/components" % (ENDPOINT)
OPTIONS_ENDPOINT = "%s/options" % (ENDPOINT)


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

        desiredState = {
            'cloneUrl': '',
            'playbook': '',
            'commit': '',
        }
        self._patch_desired_state(node_ids, desiredState)

    @call_logger
    def set_configuration(self, node_ids, commit, repo_url=None, playbook=None, enabled=False):
        desiredState = {
            'commit': commit,
        }
        if repo_url:
            desiredState['cloneUrl'] = repo_url
        if playbook:
            desiredState['playbook'] = playbook
        self._patch_desired_state(node_ids, desiredState, enabled=enabled)

    @call_logger
    def _patch_desired_state(self, node_ids, desiredState, enabled=False):
        data = []
        for node_id in node_ids:
            data.append({
                'id': node_id,
                'enabled': enabled,
                'desiredState': desiredState
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

    def get_default_clone_url(self):
        response = self._session.get(OPTIONS_ENDPOINT)
        response.raise_for_status()
        data = response.json()
        return data['defaultCloneUrl']

    def get_default_playbook(self):
        response = self._session.get(OPTIONS_ENDPOINT)
        response.raise_for_status()
        data = response.json()
        return data['defaultPlaybook']


def wait_for_configuration(nodes,
                           session_id,
                           node_lookup_by_boot_set,
                           maximum_duration=1800, check_interval=5,
                           cfs_client=None, success_threshold=1.0):
    """
    Given a set of nodes <nodes>, wait for them to all reach fully configured status.
    We will exit early if the failure_threshold ratio of nodes is exceeded by failures.
    Args:
      nodes: An iterable of nodes that we're waiting for
      maximum_duration: The period of time, in seconds, that we wait for components to
        become configured. When set to zero, wait indefinitely
      check_interval: The period of time between calls to CFS for component information
      cfs_client: An existing, instantiated CFS client (if any).
      success_threshold: This float value defines the percentage of nodes that must complete
        successfully in order for the configuration to be deemed complete. When the number of
        nodes have explicitly failed configuration (as indicated by those nodes reaching their
        maximum number of attempts for configuration), the function raises a configuration exception.
      session_id (str): The Session's ID, used for reporting status
      node_lookup_by_boot_set (dict): Keys: Boot sets; Values: nodes; used for reporting status
    State Changes:
     - Logged messages to stdout
    Raises:
     - CFSException
     - CFSExhaustedRetries
     - CFSTimeout
    Returns: None
    """
    cfs_client = cfs_client or CfsClient()
    if not maximum_duration:
        # Give them about 100 years, they may be running KNL.
        end_time = time.time() + (60 * 60 * 24 * 365 * 100)
    else:
        end_time = time.time() + maximum_duration
    check_interval = os.getenv("CFS_COMPLETION_SLEEP_INTERVAL", check_interval)
    nodes = set(nodes)
    nodes_count = len(nodes)
    allowable_failures = (1.0 - success_threshold) * nodes_count
    nodes_required_for_success = nodes_count - allowable_failures
    last_status = None
    last_status_time = time.time()

    # Report In-progress Nodes' Status
    report_status_by_bootset(nodes, node_lookup_by_boot_set, "configure",
                             "not_started", "in_progress", session_id)

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
            components_data = cfs_client.get_components(ids=','.join(chunk))
            components |= {component['id'] for component in components_data}
            for component in components_data:
                components_config_map[component.get(
                    'configurationStatus', 'undefined')].add(component['id'])

        # LOG COMPONENT STATUS INFORMATION
        # Report Completed Nodes' Status
        successful_components = components_config_map['configured']
        successful_components_count += len(successful_components)
        report_status_by_bootset(successful_components, node_lookup_by_boot_set,
                                 "configure", "in_progress", "succeeded", session_id)
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
            report_errors_by_bootset(session_id, node_lookup_by_boot_set,
                                     phase='configure', errors=errors)
        failed_components_count += len(failed_components)
        report_status_by_bootset(failed_components, node_lookup_by_boot_set,
                                 "configure", "in_progress", "failed", session_id)

        # CHECK EXIT CONDITIONS
        if failed_components_count > allowable_failures:
            msg = """Maximum number of nodes has surpassed success criteria threshold; ",
                  there may be more failed configuration attempts in flight.
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
                        ,', '.join(sorted(remaining_components)))
        return
    else:
        raise CFSTimeout("%s nodes failed to configure with CFS within the specified time."
                         %(nodes_count - successful_components_count))


def get_commit_id(cfs_client, repo_url, branch):
    """
    Given a branch and git url, returns the commit id at the top of that branch

    Args:
      cfs_client: A client for the CFS API
      repo_url: The cloneUrl to pass to the CFS session
      branch: The branch to pass to the CFS session

    Returns:
      commit: A commit id for the given branch

    Raises:
      subprocess.CalledProcessError -- for errors encountered calling git
    """
    if not repo_url:
        try:
            repo_url = cfs_client.get_default_clone_url()
        except KeyError as e:
            msg = 'defaultCloneUrl has not been initialized'
            raise Exception(msg) from e
    repo_name = repo_url.split('/')[-1].split('.')[0]
    clone_command = 'git clone {}'.format(repo_url).split()
    checkout_command = 'git checkout {}'.format(branch).split()
    parse_command = 'git rev-parse HEAD'.split()
    try:
        subprocess.check_call(clone_command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.check_call(checkout_command, cwd=repo_name,
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        output = subprocess.check_output(parse_command, cwd=repo_name)
        commit = output.decode("utf-8").strip()
        LOGGER.info('Translated git branch {} to commit {}'.format(branch, commit))
        return commit
    except subprocess.CalledProcessError as e:
        LOGGER.error('Failed interacting with the specified cloneUrl: {}'.format(e))
        raise


if __name__ == '__main__':
    import sys
    lh = logging.StreamHandler(sys.stdout)
    lh.setLevel(logging.INFO)
    LOGGER = logging.getLogger()
    LOGGER.setLevel(logging.INFO)
    LOGGER.addHandler(lh)
    nodes = {'x3000c0s19b3n0', 'x3000c0s19b4n0', 'x3000c0s19b1n0', 'x3000c0s19b2n0'}
    from cray.boa.cfsclient import wait_for_configuration
    wait_for_configuration(nodes)
