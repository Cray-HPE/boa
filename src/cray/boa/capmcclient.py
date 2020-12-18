# Copyright 2019, Cray Inc. All Rights Reserved.

import logging
import time
import requests
import json
from collections import defaultdict

from cray.boa import TransientException, PROTOCOL
from cray.boa.logutil import call_logger
from cray.boa.connection import requests_retry_session

LOGGER = logging.getLogger(__name__)
SERVICE_NAME = 'cray-capmc'
ENDPOINT = "%s://%s/capmc" % (PROTOCOL, SERVICE_NAME)


class CapmcException(TransientException):
    """
    Interaction with capmc resulted in a known failure.
    """


class CapmcTimeoutException(CapmcException):
    """
    Raised when a call to CAPMC exceeded total time to complete.
    """


def status(nodes, filtertype='show_all', attempts=20, frequency=10, session=None):
    """
    For a given iterable of nodes, represented by xnames, query capmc for
    the power status of all nodes. Return a dictionary of nodes that have
    been bucketed by status.
    
    Args:
      nodes (list): Nodes to get status for
      filtertype (str): Type of filter to use when sorting 
      attempts (int): Number of times to attempt to get status before failing
      frequency (int): Number of seconds to wait before re-attempting to get status on a failure
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
    # Check for error state in the returned response
    if json_response['e']:
        attempts -= 1
        if attempts <= 0:
            # The query came back with an error
            raise CapmcException("CAPMC responded with an error response code '%s': %s"
                                 % (json_response['e'], json_response))
        time.sleep(frequency)
        return status(nodes, filtertype=filtertype, attempts=attempts,
                      frequency=frequency, session=session)

    for key in ('e', 'err_msg'):
        try:
            del json_response[key]
        except KeyError:
            pass
    # For the remainder of the keys in the response, translate the status to set operation
    for key in json_response:
        status_bucket[key] |= set(json_response[key])
    return status_bucket


def parse_response(response):
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
    reasons_for_failure = defaultdict(set)
    if 'e' not in response or response['e'] == 0:
        # All nodes received the requested action; happy path
        return failed_nodes, reasons_for_failure
    LOGGER.warning("CAPMC responded with e code '%s'", response['e'])
    if 'err_msg' in response:
        LOGGER.warning("err_msg: %s", response['err_msg'])
    if 'xnames' in response:
        for xname_dict in response['xnames']:
            xname = xname_dict['xname']
            err_msg = xname_dict['err_msg']
            reasons_for_failure[err_msg].add(xname)
        # Report back all reasons for failure
        for err_msg, nodes in sorted(reasons_for_failure.items()):
            node_count = len(nodes)
            if node_count <= 5:
                LOGGER.warning("\t%s: %s", err_msg, ', '.join(sorted(nodes)))
            else:
                LOGGER.warning("\t%s: %s nodes", err_msg, node_count)
        # Collect all failed nodes.
        for nodes in reasons_for_failure.values():
            failed_nodes |= nodes
    return failed_nodes, reasons_for_failure


@call_logger
def boot(nodes, frequency=10, session=None, attempts=10, reason="BOA: Staging node for boot..."):
    """
    Boots a group of nodes through capmc; returns a set of nodes that were unable to
    be issued a boot request after <attempts> attempts.

    Args:
      nodes (list): Nodes to power on
      attempts (int): Number of times to attempt to power on the nodes before failing
      frequency (int): Number of seconds to wait before re-attempting to get status on a failure
      session (Requests.session object): A Requests session instance

    Returns:
      failed_to_boot (set): the nodes that failed to boot
      boot_errors (dict): A dictionary containing the nodes (values)
                          suffering from errors (keys)
      
    """
    if not nodes:
        LOGGER.warning("boot called without nodes; returning without action.")
        return set(), {}
    session = session or requests_retry_session()
    prefix, output_format = node_type(nodes)
    boot_endpoint = '%s/%s_on' % (ENDPOINT, prefix)
    json_response = call(boot_endpoint, nodes, output_format, reason)
    failed_to_boot, boot_errors = parse_response(json_response)
    if 'e' not in json_response or json_response['e'] == 0:
        # Happy Path, return empty set
        return failed_to_boot, boot_errors
    attempts -= 1
    if not attempts:
        LOGGER.warning("Last attempt reached; falling back.")
        return failed_to_boot, boot_errors
    else:
        LOGGER.info("Reattempting call to boot.")
        time.sleep(frequency)
        nodes_to_boot = failed_to_boot - status(nodes, frequency=frequency, session=session)['on']

        return boot(list(nodes_to_boot), session=session, attempts=attempts)


@call_logger
def shutdown(nodes, force=True, frequency=10, session=None, attempts=10,
             reason="BOA: Staging nodes for shutdown..."):
    """
    Shuts a group of nodes down. This call corresponds to CAPMC's shutdown endpoint.

    It is important to note that CAPMC will respond with a 200 response, even if it fails
    to power off a node component.This function attempts the shutdown operation up to 
    <attempts> times until all nodes received the request to shutdown appropriately.
    The return value is a set of nodes (xnames) that did not successfully shutdown at
    the end of<attempt> attempts.

    Nodes that are instructed to power off that are already powered off state (due
    to a race condition, particularly between graceful and force power off) are retried
    without consuming an attempt.

    This function is designed to be recursive; each subsequent call decrements the attempt counter.
    
    Args:
      nodes (list): Nodes to power off
      attempts (int): Number of times to attempt to power off the nodes before failing
      frequency (int): Number of seconds to wait before re-attempting to get status on a failure
      session (Requests.session object): A Requests session instance
    
    Returns
      failed_to_boot (set): the nodes that failed to boot
      shutdown_errors (dict): A dictionary containing the nodes (values)
                              suffering from errors (keys)
    """
    if not nodes:
        LOGGER.warning("shutdown called without nodes; returning without action.")
        return set(), {}
    prefix, output_format = node_type(nodes)
    session = session or requests_retry_session()
    shutdown_endpoint = '%s/%s_off' % (ENDPOINT, prefix)
    LOGGER.info("Shutting down %s nodes; %s remaining attempt(s).", len(nodes), attempts)
    json_response = call(shutdown_endpoint, nodes, output_format, reason,
                         force=force)
    failed_to_shutdown, shutdown_errors = parse_response(json_response)
    if 'e' not in json_response or json_response['e'] == 0:
        # Happy Path, return empty set
        return failed_to_shutdown, shutdown_errors
    attempts -= 1
    if not attempts:
        LOGGER.warning("Last attempt reached; falling back.")
        return failed_to_shutdown, shutdown_errors
    else:
        LOGGER.info("Reattempting call to shutdown.")
        time.sleep(frequency)
        nodes_to_shutdown = failed_to_shutdown - status(nodes, frequency=frequency, session=session)['off']

        return shutdown(list(nodes_to_shutdown), force=force, session=session, attempts=attempts)


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
      attempts (int): Number of times to attempt to power off the nodes before failing
      frequency (int): Number of seconds to wait before re-attempting to get status on a failure
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
    failed_to_shutdown = set()
    shutdown_errors = dict()
    if not nodes:
        LOGGER.warning("graceful_shutdown called without nodes; returning without action.")
        return failed_to_shutdown, shutdown_errors
    session = session or requests_retry_session()
    # We treat any node not specifically in the off state to be on.
    nodes_on = set(nodes) - status(nodes, frequency=frequency, session=session)['off']

    if not nodes_on:
        LOGGER.info("All nodes already in off state.")
        return failed_to_shutdown, shutdown_errors
    LOGGER.info('Issuing graceful powerdown request.')
    _, _ = shutdown(list(nodes_on), force=False, session=session, reason=reason)

    end_time = time.time() + grace_window

    # Give the BMC's a chance to power down before initially checking.
    time.sleep(graceful_prewait)

    while nodes_on and time.time() < end_time:
        time.sleep(frequency)
        # All nodes not explicitly OFF need to be treated as if they are
        # in a transitional state.
        try:
            nodes_on = set(nodes) - status(nodes, frequency=frequency, session=session)['off']
        except CapmcException as err:
            LOGGER.error("Received a CAPMC error while requesting node status. Ignoring error: %s", err)
    # Fall through to powering nodes off with hardoff
    if nodes_on:
        LOGGER.info("Issuing hard poweroff request; %s nodes remain in on state.", len(nodes_on))
        failed_to_shutdown, shutdown_errors = shutdown(list(nodes_on), force=True, session=session)
        if failed_to_shutdown:
            msg = "CAPMC unable to issue shutdown command to %s nodes." % len(failed_to_shutdown)
            LOGGER.error(msg)
            return failed_to_shutdown, shutdown_errors

    end_time = time.time() + hard_window

    while nodes_on and time.time() < end_time:
        time.sleep(frequency)
        nodes_on = set(nodes) - status(nodes, frequency=frequency, session=session)['off']
    if nodes_on:
        num_nodes = len(nodes_on)
        shutdown_errors = {'Never went to off state': list(nodes_on)}
        msg = "%d node%s did not enter a shutdown state after %s seconds: %s" % (num_nodes,
                                                                                '' if num_nodes == 1 else 's',
                                                                                hard_window,
                                                                                sorted(nodes_on))

        LOGGER.error(msg)
        return nodes_on, shutdown_errors
    # Return emptiness because a return is expected
    return set(), dict()


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
    failed_nodes = parse_response(response)
    assert len(failed_nodes) == 0
    # Testcase 1; one node with one error
    response = {"e":-1, "err_msg":"Errors encountered with 1/1 Xnames issued On", "xnames":[{"xname":"x3000c0s19b3n0", "e":-1, "err_msg":"NodeBMC Communication Error"}]}
    failed_nodes = parse_response(response)
    assert len(failed_nodes) == 1
    # Testcase 2; two nodes with one kind of error
    response = {"e":-1, "err_msg":"Errors encountered with 2/2 Xnames issued On", "xnames":[{"xname":"x3000c0s19b3n0", "e":-1, "err_msg":"NodeBMC Communication Error"},
                                                                                          {"xname":"x3000c0s19b3n1", "e":-1, "err_msg":"NodeBMC Communication Error"}]}
    failed_nodes = parse_response(response)
    assert len(failed_nodes) == 2
    # Testcase 3; failures > threshold, one kind of error
    response = {"e":-1, "err_msg":"Errors encountered with 7/7 Xnames issued On", "xnames":[{"xname":"x3000c0s19b3n0", "e":-1, "err_msg":"NodeBMC Communication Error"},
                                                                                          {"xname":"x3000c0s19b3n1", "e":-1, "err_msg":"NodeBMC Communication Error"},
                                                                                          {"xname":"x3000c0s19b3n2", "e":-1, "err_msg":"NodeBMC Communication Error"},
                                                                                          {"xname":"x3000c0s19b3n3", "e":-1, "err_msg":"NodeBMC Communication Error"},
                                                                                          {"xname":"x3000c0s19b3n4", "e":-1, "err_msg":"NodeBMC Communication Error"},
                                                                                          {"xname":"x3000c0s19b3n5", "e":-1, "err_msg":"NodeBMC Communication Error"},
                                                                                          {"xname":"x3000c0s19b3n6", "e":-1, "err_msg":"NodeBMC Communication Error"}]}
    failed_nodes = parse_response(response)
    assert len(failed_nodes) == 7
    # Testcase 4: failures > threshold, multiple kinds of errors
    response = {"e":-1, "err_msg":"Errors encountered with 7/7 Xnames issued On", "xnames":[{"xname":"x3000c0s19b3n0", "e":-1, "err_msg":"NodeBMC Communication Error"},
                                                                                          {"xname":"x3000c0s19b3n1", "e":-1, "err_msg":"NodeBMC Communication Error"},
                                                                                          {"xname":"x3000c0s19b3n2", "e":-1, "err_msg":"NodeBMC Communication Error"},
                                                                                          {"xname":"x3000c0s19b3n3", "e":-1, "err_msg":"NodeBMC Communication Error"},
                                                                                          {"xname":"x3000c0s19b3n4", "e":-1, "err_msg":"NodeBMC Communication Error"},
                                                                                          {"xname":"x3000c0s19b3n5", "e":-1, "err_msg":"NodeBMC Communication Error"},
                                                                                          {"xname":"x3000c0s19b3n6", "e":-1, "err_msg":"NodeBMC went out to lunch!"}]}
    failed_nodes = parse_response(response)
    assert len(failed_nodes) == 7
    # Testcase 5; situation normal.
    response = {'e': 0}
    failed_nodes = parse_response(response)
    assert len(failed_nodes) == 0
