#
# MIT License
#
# (C) Copyright 2019-2022, 2024 Hewlett Packard Enterprise Development LP
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
import logging
import time
import requests
import json
from collections import defaultdict

from cray.boa import TransientException, PROTOCOL, ServiceError
from cray.boa.logutil import call_logger
from cray.boa.connection import requests_retry_session

LOGGER = logging.getLogger(__name__)
SERVICE_NAME = 'cray-capmc'
CAPMC_VERSION = 'v1'
ENDPOINT = "%s://%s/capmc/%s" % (PROTOCOL, SERVICE_NAME, CAPMC_VERSION)


class CapmcException(TransientException):
    """
    Interaction with capmc resulted in a known failure.
    """


class CapmcTimeoutException(CapmcException):
    """
    Raised when a call to CAPMC exceeded total time to complete.
    """


class CapmcDeprecationException(ServiceError):
    """
    All or part of a request cannot be completed because it requires functionality
    that has been effectively deprecated out of a major version of capmc.
    """


def status(nodes, filtertype='show_all', session=None):
    """
    For a given iterable of nodes, represented by xnames, query CAPMC for
    the power status of all nodes. Return a dictionary of nodes that have
    been bucketed by status.
    
    Args:
      nodes (list): Nodes to get status for
      filtertype (str): Type of filter to use when sorting 
      
    Returns:
      status_dict (dict): Keys are different states; values are a literal set of nodes
      failed_nodes (set): A set of the nodes that had errors
      errors (dict): A dictionary containing the nodes (values)
                     suffering from errors (keys)
    
    Raises:
      HTTPError 
      JSONDecodeError -- error decoding the CAPMC response
    """
    endpoint = '%s/get_xname_status' % (ENDPOINT)
    status_bucket = defaultdict(set)
    session = session or requests_retry_session()
    body = {'filter': filtertype,
            'xnames': list(nodes)}

    response = session.post(endpoint, json=body)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        LOGGER.error("Failed interacting with Cray Advanced Platform Monitoring and Control "
                     "(CAPMC): %s", err)
        LOGGER.error(response.text)
        raise
    try:
        json_response = json.loads(response.text)
    except json.JSONDecodeError as jde:
        errmsg = "CAPMC returned a non-JSON response: %s %s" % (response.text, jde)
        LOGGER.error(errmsg)
        raise
    # Check for error state in the returned response and retry
    if json_response['e']:
        LOGGER.error("CAPMC responded with an error response code '%s': %s"
                     % (json_response['e'], json_response))

    failed_nodes, errors = parse_response(json_response, nodes)

    for key in ('e', 'err_msg'):
        try:
            del json_response[key]
        except KeyError:
            pass
    # For the remainder of the keys in the response, translate the status to set operation
    for key in json_response:
        status_bucket[key] |= set(json_response[key])
    return status_bucket, failed_nodes, errors


def parse_response(response, target_nodes):
    """
    Takes a CAPMC power action JSON response and process it for partial
    communication errors. This function is used in booting as well as
    shutdown, so it has been abstracted to one place in order to avoid
    duplication.

    This function has the side effect of categorizing and logging errors
    by error condition encountered.

    # Here is an example of what a partially successful shutdown looks like, since it isn't captured
    # in the documentation particularly well.
    # {"e":-1,"err_msg":"Errors encountered with 1/1 Xnames issued On","xnames":[{"xname":"x3000c0s19b3n0","e":-1,"err_msg":"NodeBMC Communication Error"}]}

    This function returns a set of nodes (in our case, almost always, xnames)
    that did not receive the requested call for action. Upstream calling
    functions may decide what to do with that information.

    Returns
      failed_nodes (set): A set of the nodes that failed
      reasons_for_failure (dict): A dictionary containing the nodes (values)
                                  suffering from errors (keys)
    """
    failed_nodes = set()
    reasons_for_failure = defaultdict(list)
    if 'e' not in response or response['e'] == 0:
        # All nodes received the requested action; happy path
        return failed_nodes, reasons_for_failure
    LOGGER.warning("CAPMC responded with e code '%s'", response['e'])
    if 'err_msg' in response:
        LOGGER.warning("err_msg: %s", response['err_msg'])
    if 'undefined' in response:
        failed_nodes |= set(response['undefined'])
    if 'xnames' in response:
        for xname_dict in response['xnames']:
            xname = xname_dict['xname']
            err_msg = xname_dict['err_msg']
            reasons_for_failure[err_msg].append(xname)
        # Report back all reasons for failure
        for err_msg, nodes in sorted(reasons_for_failure.items()):
            node_count = len(nodes)
            if node_count <= 5:
                LOGGER.warning("\t%s: %s", err_msg, ', '.join(sorted(nodes)))
            else:
                LOGGER.warning("\t%s: %s nodes", err_msg, node_count)
        # Collect all failed nodes.
        for nodes in reasons_for_failure.values():
            failed_nodes |= set(nodes)
    if response['e'] == 37:
        # CASMCMS-8274: Log when we get error 37, indicating a failure due to locked nodes.
        err_msg = response['err_msg'] if 'err_msg' in response else "CAPMC node lock error 37"

        # CAPMC does not associate it with any nodes, so we add it as a reason for failure for
        # all nodes which are not covered by other failures already
        potentially_locked_nodes = [node for node in target_nodes if node not in failed_nodes]
        reasons_for_failure[err_msg].extend(potentially_locked_nodes)
        failed_nodes |= set(potentially_locked_nodes)
    return failed_nodes, reasons_for_failure


@call_logger
def power(nodes, state, force=True, session=None, reason="BOA: Powering nodes"):
    """
    Sets a node to a power state using CAPMC; returns a set of nodes that were unable to achieve
    that state.

    It is important to note that CAPMC will respond with a 200 response, even if it fails
    to power the node to the desired state.
    
    Args:
      nodes (list): Nodes to power on
      state (string): Power state: off or on
      force (bool): Should the power off be forceful (True) or not forceful (False)
      session (Requests.session object): A Requests session instance

    Returns:
      failed (set): the nodes that failed to enter the desired power state
      boot_errors (dict): A dictionary containing the nodes (values)
                          suffering from errors (keys)
    
    Raises:
      ValueError: if state is neither 'off' nor 'on'
    """
    if not nodes:
        LOGGER.warning("power called without nodes; returning without action.")
        return set(), {}

    valid_states = ["off", "on"]
    state = state.lower()
    if state not in valid_states:
        raise ValueError("State must be one of {} not {}".format(valid_states, state))

    session = session or requests_retry_session()
    prefix, output_format = node_type(nodes)
    if output_format == 'nids':
        raise CapmcDeprecationException("CAPMC deprecated power control for nid based entries; "
                                        "please convert remaining session template references from "
                                        "nids over to xnames.")
    power_endpoint = '%s/%s_%s' % (ENDPOINT, prefix, state)

    if state == "on":
        json_response = call(power_endpoint, nodes, output_format, reason)
    elif state == "off":
        json_response = call(power_endpoint, nodes, output_format, reason, force=force)

    failed_nodes, errors = parse_response(json_response, nodes)
    return failed_nodes, errors


@call_logger
def graceful_shutdown(nodes, grace_window=300, hard_window=180, graceful_prewait=20,
                      frequency=10, session=None, reason="BOA: Staging nodes for shutdown..."):
    """
    Performs a two stage shutdown operation on the nodes in question with a
    sleep window between the calls to CAPMC. If all nodes enter poweroff state
    gracefully, we avoid forcibly powering down; otherwise, we instruct CAPMC
    that there has been a long enough wait and we pull the power plug. This
    function will wait for all <nodes> to enter a shutdown state within
    <hard_window> seconds after the call for hard shutdown.

    Args:
      nodes (list): Nodes to power off
      session (Requests.session object): A Requests session instance
      grace_window (int): Number of seconds to wait for the nodes to gracefully power down
      hard_window (int): Number of seconds to wait for the nodes to power down after a forceful
                         power down
      graceful_prewait (int):  Number of seconds to wait initially before checking the nodes'
                               power status after a graceful shutdown attempt.
      frequency (int): Number of seconds to wait before re-attempting to get status on a failure

    Returns
      failed_to_boot (set): the nodes that failed to boot
      shutdown_errors (dict): A dictionary containing the nodes (values)
                              suffering from errors (keys)
    """
    failed_nodes = set()
    errors = dict()
    if not nodes:
        LOGGER.warning("graceful_shutdown called without nodes; returning without action.")
        return failed_nodes, errors
    session = session or requests_retry_session()

    # TODO Once CASMHMS-4868 is resolved, we can change filter to show_off rather than show_all.
    # filter = 'show_off'
    filter = 'show_all'
    # We treat any node not specifically in the off state to be on.
    status_dict, failed_nodes_stat, errors_stat = status(nodes,
                                                         filtertype=filter,
                                                         session=session)

    nodes_on = (set(nodes) - status_dict['off']) - failed_nodes_stat
    failed_nodes |= failed_nodes_stat
    errors.update(errors_stat)

    for attempt in ["graceful", "forceful"]:
        if not nodes_on:
            LOGGER.info("All nodes are in state: off.")
            break

        if attempt == "graceful":
            end_time = time.time() + grace_window
            force = False
            LOGGER.info('Issuing graceful powerdown request.')
        else:
            end_time = time.time() + hard_window
            force = True
            LOGGER.info("Issuing hard poweroff request; %s nodes remain in state: on.", len(nodes_on))
        try:
            failed_nodes_tmp, errors_tmp = power(list(nodes_on), "off",
                                                 force=force, session=session, reason=reason)
            if attempt == "graceful":
                # Weed out any nodes that CAPMC said refused to turn off. Those
                # will be handled during the forceful power off.
                if "exceeded retries waiting for component to be Off" in errors_tmp:
                    nodes_failed_to_power_off = set(errors_tmp["exceeded retries waiting for component to be Off"])
                    failed_nodes_tmp = failed_nodes_tmp - nodes_failed_to_power_off
                    LOGGER.warn("CAPMC reported these nodes failed to power off. They will be forcefully powered off: {}".format(nodes_failed_to_power_off))

        except ValueError as e:
            LOGGER.critical("Error calling 'power': %s", e)
            return nodes_on, errors

        nodes_on -= failed_nodes_tmp
        failed_nodes |= failed_nodes_tmp
        errors.update(errors_tmp)

        if attempt == "graceful":
            # Give the BMC's a chance to power down before initially checking.
            time.sleep(graceful_prewait)

        while nodes_on and time.time() < end_time:
            time.sleep(frequency)
            # All nodes not explicitly OFF need to be treated as if they are
            # in a transitional state.
            try:
                status_dict, failed_nodes_stat, errors_stat = status(nodes_on,
                                                                     filtertype=filter,
                                                                     session=session)

                nodes_on = nodes_on - status_dict['off'] - failed_nodes_stat
                failed_nodes |= failed_nodes_stat
                errors.update(errors_stat)
            except (json.JSONDecodeError, requests.exceptions.HTTPError) as err:
                LOGGER.error("Received a CAPMC error while requesting node status: %s", err)

        if attempt == "forceful":
            if failed_nodes:
                msg = "CAPMC unable to issue shutdown command to %s nodes." % len(failed_nodes)
                LOGGER.error(msg)

            if nodes_on:
                num_nodes = len(nodes_on)
                msg = "%d nodes did not enter a shutdown state after %s seconds: %s" % (num_nodes,
                                                                                        hard_window,
                                                                                        sorted(nodes_on))
                LOGGER.error(msg)

    return failed_nodes, errors


@call_logger
def node_type(nodes):
    """
    Given a list of <nodes>, determine if they're in nid or xname format.
    """
    return ('node', 'nids') if list(nodes)[0].startswith('nid') else ('xname', 'xnames')


@call_logger
def call(endpoint, nodes, node_format='xnames', reason="None given", session=None, **kwargs):
    '''
    This function makes a call to the Cray Advanced Platform Monitoring and Control (CAPMC)
    Args:
        endpoint: CAPMC endpoint to interact with
        nodes: The nodes to ask CAPMC to operate on
        node_format: Either xnames or ids;  The payload needs to have the correct key
    kwargs**:
        Additional command line arguments that can be passed in by resulting calls for additional
        flexibility when interacting with capmc; these are appended in a key:value sense
        to the payload body.
    Raises:
        requests.exceptions.HTTPError -- when an HTTP error occurs
        
    Returns: The parsed JSON response from the JSON based API.
    '''
    payload = {'reason': reason,
               node_format: list(nodes)}
    session = session or requests_retry_session()
    if kwargs:
        payload.update(kwargs)
    try:
        resp = session.post(endpoint, verify=False, json=payload)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as err:
        LOGGER.error("Failed interacting with Cray Advanced Platform Monitoring and Control "
                     "(CAPMC): %s", err)
        LOGGER.error(resp.text)
        raise
    try:
        return json.loads(resp.text)
    except json.JSONDecodeError as jde:
        raise CapmcException("Non-json response from CAPMC: %s" % (resp.text)) from jde


if __name__ == '__main__':
    all_nodes = set(['x3000c0s19b1n0', 'x3000c0s19b2n0', 'x3000c0s19b3n0', 'x3000c0s19b4n0'])
    my_nodes = set(['x3000c0s19b3n0', 'x3000c0s19b4n0'])
    import sys
    lh = logging.StreamHandler(sys.stdout)
    lh.setLevel(logging.DEBUG)
    LOGGER = logging.getLogger()
    LOGGER.setLevel(logging.DEBUG)
    LOGGER.addHandler(lh)
    # shutdown(my_nodes)
    # Testcase 0; happypath
    response = {}
    nodes = []
    failed_nodes, _ = parse_response(response, nodes)
    assert len(failed_nodes) == 0
    # Testcase 1; one node with one error
    response = {"e":-1, "err_msg":"Errors encountered with 1/1 Xnames issued On", "xnames":[{"xname":"x3000c0s19b3n0", "e":-1, "err_msg":"NodeBMC Communication Error"}]}
    nodes = [ x["xname"] for x in response["xnames"] ]
    failed_nodes, _ = parse_response(response, nodes)
    assert len(failed_nodes) == 1
    # Testcase 2; two nodes with one kind of error
    response = {"e":-1, "err_msg":"Errors encountered with 2/2 Xnames issued On", "xnames":[{"xname":"x3000c0s19b3n0", "e":-1, "err_msg":"NodeBMC Communication Error"},
                                                                                          {"xname":"x3000c0s19b3n1", "e":-1, "err_msg":"NodeBMC Communication Error"}]}
    nodes = [ x["xname"] for x in response["xnames"] ]
    failed_nodes, _ = parse_response(response, nodes)
    assert len(failed_nodes) == 2
    # Testcase 3; failures > threshold, one kind of error
    response = {"e":-1, "err_msg":"Errors encountered with 7/7 Xnames issued On", "xnames":[{"xname":"x3000c0s19b3n0", "e":-1, "err_msg":"NodeBMC Communication Error"},
                                                                                          {"xname":"x3000c0s19b3n1", "e":-1, "err_msg":"NodeBMC Communication Error"},
                                                                                          {"xname":"x3000c0s19b3n2", "e":-1, "err_msg":"NodeBMC Communication Error"},
                                                                                          {"xname":"x3000c0s19b3n3", "e":-1, "err_msg":"NodeBMC Communication Error"},
                                                                                          {"xname":"x3000c0s19b3n4", "e":-1, "err_msg":"NodeBMC Communication Error"},
                                                                                          {"xname":"x3000c0s19b3n5", "e":-1, "err_msg":"NodeBMC Communication Error"},
                                                                                          {"xname":"x3000c0s19b3n6", "e":-1, "err_msg":"NodeBMC Communication Error"}]}
    nodes = [ x["xname"] for x in response["xnames"] ]
    failed_nodes, _ = parse_response(response, nodes)
    assert len(failed_nodes) == 7
    # Testcase 4: failures > threshold, multiple kinds of errors
    response = {"e":-1, "err_msg":"Errors encountered with 7/7 Xnames issued On", "xnames":[{"xname":"x3000c0s19b3n0", "e":-1, "err_msg":"NodeBMC Communication Error"},
                                                                                          {"xname":"x3000c0s19b3n1", "e":-1, "err_msg":"NodeBMC Communication Error"},
                                                                                          {"xname":"x3000c0s19b3n2", "e":-1, "err_msg":"NodeBMC Communication Error"},
                                                                                          {"xname":"x3000c0s19b3n3", "e":-1, "err_msg":"NodeBMC Communication Error"},
                                                                                          {"xname":"x3000c0s19b3n4", "e":-1, "err_msg":"NodeBMC Communication Error"},
                                                                                          {"xname":"x3000c0s19b3n5", "e":-1, "err_msg":"NodeBMC Communication Error"},
                                                                                          {"xname":"x3000c0s19b3n6", "e":-1, "err_msg":"NodeBMC went out to lunch!"}]}
    nodes = [ x["xname"] for x in response["xnames"] ]
    failed_nodes, _ = parse_response(response, nodes)
    assert len(failed_nodes) == 7
    # Testcase 5; situation normal.
    response = {'e': 0}
    nodes = []
    failed_nodes, _ = parse_response(response, nodes)
    assert len(failed_nodes) == 0
