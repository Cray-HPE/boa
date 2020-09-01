# Copyright 2020 Hewlett Packard Enterprise Development LP

import datetime
import logging
import requests

from . import PROTOCOL
from cray.boa.connection import requests_retry_session

LOGGER = logging.getLogger(__name__)
SERVICE_NAME = 'cray-bos'
API_VERSION = 'v1'
ENDPOINT = "%s://%s/%s/session" % (PROTOCOL, SERVICE_NAME, API_VERSION)


class BaseBosStatusApiException(BaseException):
    pass


class BadSession(BaseBosStatusApiException):
    """
    A Session's Status cannot be created.
    """
    pass


class BadBootSetUpdate(BaseBosStatusApiException):
    """
    A Boot Set's Status cannot be updated.
    """
    pass


class InvalidPhase(BaseBosStatusApiException):
    """
    Invalid Phase value
    """
    pass


def create_session_status(session_id, boot_sets, start_time=None, session=None):
    """
    Create a Status for the session

    Args:
      session_id (str): The ID for the BOS session
      boot_sets (list): A list of the Boot Sets (strings)
      start_time (str): Indicates the time the Session started; formatted as a
                        datetime.now string
      session: A requests connection session
    """
    endpoint = "{}/{}/status".format(ENDPOINT, session_id)
    if start_time is None:
        start_time = str(datetime.datetime.now())

    if not isinstance(boot_sets, list):
        raise BadSession("Boot sets must be a list.")

    body = {"id": session_id,
            "boot_sets": boot_sets,
            "metadata": {"start_time": start_time}
            }

    session = session or requests_retry_session()
    response = session.post(endpoint, json=body)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        LOGGER.error("Failed to create the BOS Session Status for Session"
                     " %s -- HTTP Status Code: %s -- Error: %s",
                     session_id, response.status_code, err)
        LOGGER.error(response.text)
        # It is our current policy to not exit on failure to create status.
        pass


def _generate_phase(phase_name, node_list):
    """
    Generates a phase to add to the request body.

    Args:
      phase_name (str): Name of the phase
      node_list (list): List of nodes (strings)

    Returns:
      A phase dictionary
    """
    return {
                "name": phase_name,
                "metadata": {
                    "start_time": str(datetime.datetime.now()),
                    },
                "categories": [
                    {
                        "name": "not_started",
                        "node_list": node_list
                     },
                    {
                      "name": "succeeded",
                      "node_list": []
                     },
                    {
                      "name": "failed",
                      "node_list": []
                     },
                    {
                      "name": "excluded",
                      "node_list": []
                     },
                    {
                      "name": "in_progress",
                      "node_list": []
                     }
                 ]
            }


def create_boot_set_status(session_id, boot_set_name, phases, node_list,
                           start_time=None, session=None):
    """
    Create a Status for the session

    Args:
      session_id (str): The ID for the BOS session
      boot_sets (list): A list of the Boot Sets (strings)
      start_time (str): Indicates the time the Session started; formatted as a
                        datetime.now string
      session: A requests connection session
    """

    endpoint = "{}/{}/status/{}".format(ENDPOINT, session_id, boot_set_name)
    if start_time is None:
        start_time = str(datetime.datetime.now())

    if not isinstance(phases, list):
        raise BadSession("Phases must be a list. Phases were {}".format(type(phases)))

    if not isinstance(node_list, list):
        raise BadSession("Node list must be a list. Node list was {}".format(type(node_list)))

    body = {
        "name": boot_set_name,
        "session": session_id,
        "metadata": {
            "start_time": str(datetime.datetime.now()),
            },
        "phases": []
        }

    for phase in phases:
        body['phases'].append(_generate_phase(phase, node_list))

    session = session or requests_retry_session()
    response = session.post(endpoint, json=body)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        LOGGER.error("Failed to create the BOS Boot Set Status "
                     "for Session %s: Boot Set: %s -- %s",
                     session_id, boot_set_name, err)
        LOGGER.error(response.text)
        # It is our current policy to not exit on failure to create status.
        pass


def update_boot_set_status_nodes(session_id, boot_set_name, phase, node_list,
                                 source_category,
                                 destination_category,
                                 session=None):
    """
    Update which category a node is in within a given phase.

    Args:
      session_id (str): The ID for the BOS session
      boot_set_name (str): the Boot Sets to update
      phase (str): the Phase to update
      source_category (str): The source category to take the nodes from
      destination_category (str): The destination category to place the nodes in
      node_list (list): List of node xnames (strings)
      session: A requests connection session
    """
    endpoint = "{}/{}/status/{}".format(ENDPOINT, session_id, boot_set_name)

    available_phases = ["shutdown", "boot", "configure"]
    if phase.lower() not in available_phases:
        raise InvalidPhase("Invalid phase: {} not one of {}".format(phase.lower(),
                                                                    available_phases))

    if not isinstance(node_list, list):
        raise BadBootSetUpdate("Node list must be a list.")

    body = [{
        "update_type": "NodeChangeList",
        "phase": phase,
        "data": {
            "phase": phase,
            "source": source_category,
            "destination": destination_category,
            "node_list": node_list
            }
        }]

    session = session or requests_retry_session()
    response = session.patch(endpoint, json=body)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        LOGGER.error("Failed to update the BOS Boot Set Status "
                     "for Session %s: Boot Set: %s -- %s",
                     session_id, boot_set_name, err)
        LOGGER.error(response.text)
        # It is our current policy to not exit on failure to create status.
        pass


def update_boot_set_status_metadata(session_id, boot_set_name, phase=None,
                                    start_time=None, stop_time=None,
                                    session=None):
    """
    Update the metadata for a Boot Set or a phase within a Boot Set.

    Args:
      session_id (str): The ID for the BOS session
      boot_set_name (str): the Boot Sets to update
      phase (str): the Phase to update, if None, the metadata for the Boot Set
                   itself will be updated
      start_time (str): The time the phase or Boot Set started
      stop_time (str): The time the phase or Boot Set stopped
      session: A requests connection session
    """
    endpoint = "{}/{}/status/{}".format(ENDPOINT, session_id, boot_set_name)

    if phase is None:
        phase = "boot_set"

    if phase:
        available_phases = ["shutdown", "boot", "configure", "boot_set"]
        if phase.lower() not in available_phases:
            raise InvalidPhase("Invalid phase: {} not one of {}".format(phase.lower(),
                                                                        available_phases))

    body = {
        "update_type": "GenericMetadata",
        "phase": phase,
        "data": {}
            }

    if start_time:
        body['data']['start_time'] = start_time

    if stop_time:
        body['data']['stop_time'] = stop_time

    session = session or requests_retry_session()
    response = session.patch(endpoint, json=[body])
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        LOGGER.error("Failed to update the BOS Boot Set Status "
                     "for Session %s: Boot Set: %s -- %s",
                     session_id, boot_set_name, err)
        LOGGER.error(response.text)
        # It is our current policy to not exit on failure to create status.
        pass


def update_session_status_metadata(session_id,
                                   start_time=None, stop_time=None,
                                   session=None):
    """
    Update the metadata for a Session.

    Args:
      session_id (str): The ID for the BOS session
      start_time (str): The time the Session started
      stop_time (str): The time the Session stopped
      session: A requests connection session
    """
    endpoint = "{}/{}/status".format(ENDPOINT, session_id)

    body = {}

    if start_time:
        body['start_time'] = start_time

    if stop_time:
        body['stop_time'] = stop_time

    if not body:
        # There is nothing to update, so return.
        return

    session = session or requests_retry_session()
    response = session.patch(endpoint, json=body)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        LOGGER.error("Failed to update the BOS Session Status "
                     "for Session %s-- %s",
                     session_id, err)
        LOGGER.error(response.text)
        # It is our current policy to not exit on failure to create status.
        pass


def update_boot_set_status_errors(session_id, boot_set_name, phase,
                                  errors={},
                                  session=None):
    """
    Update the errors for a phase within a Boot Set.

    Args:
      session_id (str): The ID for the BOS session
      boot_set_name (str): the Boot Sets to update
      phase (str): the Phase to update, if None the metadata for the Boot Set
                   itself will be updated
      errors (dict): Contains errors (keys) encountered by nodes (list) (values)
      session: A requests connection session

    Example:
      errors = {'Too Beautiful to live': ['x3000c0s19b3n0', 'x3000c0s19b4n0'],
                'Too Stubborn to Die':  ['x3000c0s19b2n0']}
    """
    endpoint = "{}/{}/status/{}".format(ENDPOINT, session_id, boot_set_name)

    if phase:
        available_phases = ["shutdown", "boot", "configure"]
        if phase.lower() not in available_phases:
            raise InvalidPhase("Invalid phase: {} not one of {}".format(phase.lower(),
                                                                        available_phases))

    body = {
        "update_type": "NodeErrorsList",
        "phase": phase,
        "data": errors
            }

    session = session or requests_retry_session()
    response = session.patch(endpoint, json=[body])
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        LOGGER.error("Failed to update the BOS Boot Set Status "
                     "for Session %s: Boot Set: %s -- %s",
                     session_id, boot_set_name, err)
        LOGGER.error(response.text)
        # It is our current policy to not exit on failure to create status.
        pass


def report_status_by_bootset(components, node_lookup_by_boot_set, phase, source, destination, session_id):
    """
    Send status about the configuration of components to BOS.
    This is a helper function to make it easier to report status when the nodes in components
    may be from different Boot Sets.

    Args:
      components: The IDs of the components we are reporting status about
      node_lookup_by_boot_set: Keys: Boot Sets; Values: Nodes
      phase (str): The Phase we are reporting status for
      source (str): The source category in the Configuration phase that the nodes are
                    moving out of
      destination (str): The destination category in the Configuration phase that the nodes are
                    moving in to
      session_id: The BOS Session ID
      Examples:
        Phase: 'configure'
        Source: 'not_started'
        Destination: 'succeeded'

    """
    if not components:
        return
    for boot_set in node_lookup_by_boot_set:
        components_in_bs = (set(components)
                            & set(node_lookup_by_boot_set[boot_set]))
        if not components_in_bs:
            return
        update_boot_set_status_nodes(session_id,
                                     boot_set,
                                     phase, list(components_in_bs),
                                     source, destination)


def report_errors_by_bootset(session_id, node_lookup_by_boot_set, phase, errors={}, session=None):
    """
    Update the errors for a phase within a Boot Set.
    This is a helper function to make it easier to report status when the nodes in errors
      may be from different Boot Sets.

    Args:
      session_id (str): The ID for the BOS session
      node_lookup_by_boot_set: Keys: Boot Sets; Values: Nodes
      phase (str): the Phase to update, if None the metadata for the Boot Set
                   itself will be updated
      errors (dict): Contains errors (keys) encountered by nodes (list) (values)
      session: A requests connection session
    """
    if not errors:
        return
    for boot_set in node_lookup_by_boot_set:
        components_in_bs = set(node_lookup_by_boot_set[boot_set])
        errors_in_bs = {}
        for error_msg, components in errors.items():
            components_in_error = (set(components) & components_in_bs)
            if components_in_error:
                errors_in_bs[error_msg] = list(components_in_error)
        if errors_in_bs:
            update_boot_set_status_errors(session_id, boot_set, phase, errors_in_bs)
