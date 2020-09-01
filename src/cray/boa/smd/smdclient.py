# Copyright 2019, Cray Inc. All Rights Reserved.

'''
Created on Apr 26, 2019

@author: jasons
'''

from requests.exceptions import HTTPError
import logging
import os
from json import JSONDecodeError
from collections import defaultdict

from cray.boa import PROTOCOL, VERIFY, ServiceNotReady, ServiceError, NontransientException
from ..sessiontemplate import TemplateException
from ..connection import requests_retry_session
from cray.boa.logutil import call_logger

LOGGER = logging.getLogger(__name__)
SERVICE_NAME = 'cray-smd'
ENDPOINT = "%s://%s/hsm/v1/" % (PROTOCOL, SERVICE_NAME)

@call_logger
def node_map(key, node_list, session=None):
    '''
    Return a node map based on the specified key

    Args:
        key (str): The key to use for the dictionary: xname or nid, the
                   map's value will be the non-key, either nid or xname
        node_list (list): List of nodes to filter the map on (i.e. only nodes
                          in node_list are retained all others are discarded)
                          The node_list is assumed to be in the format
                          specified for the key; eg. If the key is an xname,
                          then the node_list should all be xnames
        session (object): Allows specifying an existing Requests session to use,
                          otherwise, creates a new session object with built in
                          retry resilience.

    Returns:
        A node map (i.e. a dictionary) based on the specified key

    Raises:
        ValueError -- A. If the key is not either 'xname' or 'nid'.
                      B. If 'NodeMaps' is not a key in the JSON returned
                         by the Hardware State Manager
        requests.exceptions.HTTPError -- An HTTP error encountered while
                                         communicating with the
                                         Hardware State Manager
    '''
    session = session or requests_retry_session()
    if key.lower() not in ["xname", "nid"]:
        msg = "Invalid key value: %s; Must be xname or nid" % key
        LOGGER.error(msg)
        raise TemplateException(msg)
    if key.lower() == "nid":
        map_key = "NID"
        map_value = "ID"
    else:
        map_key = "ID"
        map_value = "NID"
    url = '%s/Defaults/NodeMaps' % (ENDPOINT)
    try:
        resp = session.get(url)
        resp.raise_for_status()
    except HTTPError as err:
        LOGGER.error("Failed while interacting with the Hardware State Manager: %s", err)
        raise ServiceError(err) from err
    try:
        data = resp.json()
    except (ValueError, JSONDecodeError) as err:
        LOGGER.error("Hardware State Manager response was not valid JSON: %s", err)
        raise ServiceError(err) from err
    try:
        node_maps = data['NodeMaps']
    except KeyError as err:
        LOGGER.error("Expected key 'NodeMaps' was not in response data: %s", err)
        raise ServiceError(err) from err

    node_dict = {}
    for node in node_maps:
        node_dict[node[map_key]] = node[map_value]

    if len(node_list):
        filtered_node_dict = { node: node_dict[node] for node in node_list }
        return filtered_node_dict
    else:
        return node_dict


def filter_nodes_by_empty(node_list):
    """
    Given a list of nodes <node_list> in xname format, filter the list of nodes
    to only those nodes that are in the empty state in the HSM database
    """
    return filter_nodes_by_state("Empty", node_list)

def filter_split(node_list):
    """
    Given a list of nodes, split them into groups:
    * enabled
    * disabled
    * empty
    """
    enabled = set(filter_nodes_by_enabled(node_list[:], True))
    disabled = set(node_list) - enabled
    empty = set(filter_nodes_by_empty(node_list))
    return list(enabled), list(disabled), list(empty)

cached_node_info = None
cached_node_set = set()

def get_bulk_nodes_info(nodes, use_cached=False, session=None):
    """
    Get the information for every node on the node list from the State Management Daemon (SMD).

    Args:
      nodes -- A collection of nodes (iterable) xname form

    Returns:
      A dictionary containing the nodes' states; If use_cached==True, this will return
      a cached value so long as the nodes in the node list match those in the cached list.

    Raises:
      HTTPError
    """
    global cached_node_info
    session = session or requests_retry_session()
    if use_cached and cached_node_info:
        node_list_set = set(nodes)
        if node_list_set <= cached_node_set:
            return cached_node_info
        else:
            LOGGER.warning("Node list contained nodes not in cached node list. "
                           "Not using cache.  Requesting fresh state instead.")
    try:
        endpoint = os.path.join(ENDPOINT, 'State/Components/Query')
        payload = {'ComponentIDs': list(nodes)}
        response = session.post(endpoint, verify=VERIFY, json=payload)
        if not response.ok:
            LOGGER.error("'%s' did not respond appropriately: %s",
                         endpoint, response.text)
            try:
                raise response.raise_for_status()
            except HTTPError as hpe:
                raise ServiceNotReady(hpe) from hpe
        cached_node_info = response.json()['Components']
        return cached_node_info
    except (HTTPError) as exception:
        LOGGER.error("Unable to determine nodes' states: %s", exception)
        return None

def filter_nodes_by_state(state, node_list, invert=False, session=None):
    """
    Check the state of nodes in the node list.  Return only those nodes that
    match the input state.  If invert is True, then return only those nodes
    NOT in the input state.

    Args:
      state (str): Node state
                   Allowable values:
                   Unknown, Empty, Populated, Off, On, Standby, Halt, Ready
      node_list (set): A list of nodes in xname format
      invert (binary): False -- Wait for all of the nodes to be in the input state
                       True -- Wait for all of the nodes to not be in the input state

    Returns:
      A set of nodes in the desired state

    Raises:
      HTTPError
    """
    session = session or requests_retry_session()
    matching = set()
    allowable_states = ["Unknown", "Empty", "Populated", "Off", "On", "Standby", "Halt", "Ready"]
    if state not in allowable_states:
        msg = "State '%s' not in allowed states: %s" % (state, ",".join(allowable_states))
        LOGGER.error(msg)
        raise NontransientException(msg)
    node_states = get_bulk_nodes_info(node_list, session=session)
    if node_states:
        if not invert:
            matching_states = list(filter(lambda node: node['State'] == state, node_states))
        else:
            matching_states = list(filter(lambda node: node['State'] != state, node_states))

        matching |= {n['ID'] for n in matching_states}
    return matching

def filter_nodes_by_enabled(node_list, enabled=True, session=None):
    """
    Check the enabled/disabled state of nodes in the node list.
    Return only those nodes that match the input state.

    Args:
      enabled (bool): True -- node is enabled (default)
                     False -- node is disabled
                     Allowable values:
                     True, False
      node_list (set): A list of nodes in xname format

    Returns:
      A set of nodes in the desired enabled or disabled state

    Raises:
      HTTPError
    """
    session = session or requests_retry_session()
    matching = set()
    if not isinstance(enabled, bool):
        msg = "enabled must be boolean."
        LOGGER.error(msg)
        raise NontransientException(msg)
    node_states = get_bulk_nodes_info(node_list, session=session)
    matching_states = list(filter(lambda node: node['Enabled'] == enabled, node_states))
    matching |= {n['ID'] for n in matching_states}
    return matching


def component_id_query(session=None, **kwargs):
    '''
    Queries SMD for all component xnames by a given <role>.
    Returns a set of xnames that correspond to <role>.
    '''
    endpoint = os.path.join(ENDPOINT, 'State/Components')
    session = session or requests_retry_session()
    response = session.get(endpoint, params=kwargs, verify=VERIFY)
    try:
        response.raise_for_status()
    except HTTPError as hpe:
        LOGGER.error("Failed to resolve component id: '%s': %s", endpoint, hpe)
        if response.status_code == 400:
            # In this case, its possible that the query terms in
            # kwargs are not valid. These are returned to the user
            # in the form of an 'detail' field in HSM response.
            try:
                raise ServiceNotReady("SMD Error: %s" % (response.json()['detail'])) from hpe
            except KeyError:
                raise ServiceNotReady("SMD Error '%s': %s" % (response.code, response.text)) from hpe
        raise
    try:
        response_json = response.json()
    except JSONDecodeError as jde:
        LOGGER.error("SMD returned a non-json response: '%s' %s", response, jde)
        raise
    components = set(node['ID'] for node in response_json["Components"])
    return components


class NodeSet(set):
    """
    A NodeSet is a Set that has more intelligent formatting for use during output.
    """
    MAXSIZE=10
    def __str__(self):
        setsize = len(self)
        if setsize > self.MAXSIZE:
            return '%s entries' % (setsize)
        else:
            return "[%s]" % (', '.join(sorted(list(self))))

def node_summary(nodes, field='State'):
    """
    Summarizes what SMD knows about a list of nodes for a given field.
    """
    nstates = defaultdict(NodeSet)
    for nstate in get_bulk_nodes_info(list(nodes)):
        nstates[nstate[field]].add(nstate['ID'])
    return nstates

def node_state_summary(nodes, field='State'):
    """
    Formats a table of node_state_summaries into a single string.
    """
    components = []
    for key, value in node_summary(nodes, field=field).items():
        components.append('%s: %s' % (key, value))
    return '\n'.join(sorted(components))
