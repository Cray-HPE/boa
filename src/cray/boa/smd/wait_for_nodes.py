# Copyright 2019, Cray Inc. All Rights Reserved.

import logging
import time

from .smdclient import filter_nodes_by_state, node_state_summary
from ..connection import requests_retry_session
from cray.boa import TransientException
from cray.boa.bosclient import update_boot_set_status_nodes

LOGGER = logging.getLogger(__name__)

class NodeStateMismatch(TransientException):
    """
    While waiting for nodes to enter a specific state, one or more
    nodes did not achieve this state before timeout. This is typically
    raised at the end of a period of time when the number of nodes
    exceeds a threshold specified by the calling parent function.
    """


class NodesNotReady(TransientException):
    """
    The requested node resources are not available in the desired state.
    """


def wait_for_nodes(state, node_set, invert=False, sleep_time=60, allowed_retries=-1,
                   session=None, **status):
    """
    Waits for all nodes to be in the <state> state.

    Args:
      state (str): Wait until all the nodes are in this state or
                   not in this state if invert = True
      node_set (set): A set of nodes in xname format
      invert (binary): False -- Wait for all of the nodes to be in the input state
                       True -- Wait for all of the nodes to not be in the input state
      sleep_time (int): Number of seconds to sleep before rechecking nodes' states
      allowed_retries (int): Number of times to check that all nodes are ready;
                             if negative, no limits on retries are imposed
      status (keywords): These parameters are for reporting status. They are optional otherwise.
        boot_set (str): The Boot Set we are reporting status for
        phase (str): The Phase we are reporting status for
        source (str): The source category in the Configuration phase that the nodes are
                      moving out of
        destination (str): The destination category in the Configuration phase that the nodes are
                      moving in to
        session_id: The BOS Session ID

    Raises:
      NodesNotReady (exception): If we reach a time-out stage, then it raises a
                                  NodesNotReady exception
    """
    session = session or requests_retry_session()
    num_retries = 0
    matching_nodes = None
    summary = None
    if status:
        previously_matching_nodes = set()
    while matching_nodes != node_set:
        matching_nodes = set(filter_nodes_by_state(state, list(node_set), invert, session))
        number_not_matching = len(node_set - matching_nodes)
        # Report status
        if status:
            new_matching_nodes = set(matching_nodes) - previously_matching_nodes
            if new_matching_nodes:
                update_boot_set_status_nodes(status['session_id'], 
                                             status['boot_set'],
                                             status['phase'], 
                                             list(new_matching_nodes),
                                             status['source'], 
                                             status['destination'])
            previously_matching_nodes = set(matching_nodes)
        if (allowed_retries > 0) and (num_retries > allowed_retries):
            msg = ("Number of retries: {} exceeded allowed amount: {}; "
                   "{} nodes were {} in the state: {}".format(
                   num_retries, allowed_retries, number_not_matching, 
                   "not" if not invert else "still ",
                   state))
            LOGGER.error(msg)
            LOGGER.debug("These nodes were %s in the state: %s \n%s",  
                         "not" if not invert else "still ",
                         state,
                         "\n".join(node_set - matching_nodes))
            raise NodesNotReady(msg)
        num_retries += 1
        new_summary = node_state_summary(node_set)
        if summary != new_summary:
            # In this case, we have updated information about the system state
            # that we can relay back to the user; do so
            summary = new_summary
            LOGGER.info('\n%s', summary)
        if number_not_matching:
            LOGGER.info("Waiting %d seconds for %d node%s to %sbe in state: %s",
                        sleep_time, number_not_matching,
                        "s" if number_not_matching > 1 else "",
                        "" if not invert else "not ", state)
            time.sleep(sleep_time)

def wait_for_state(nodes, state, duration=70, interval=5, session=None, invert=False,
                   success_threshold=1.0):
    """
    Waits up to <duration> seconds for <nodes> to enter <state>, or alternatively,
    for nodes to not be in <state> when <invert> is true. Re-uses a passed in
    session and checks with the upstream service every <interval> seconds.
    The most notable difference between this function and wait_for_nodes, is that
    this function defines success criteria as a percentage of nodes, returns
    nodes that failed to reach the desired state, and does not do any direct
    status logging to the API. This function is agnostic to BOS phase information,
    so any upstream calling routine can do what they want with the nodes that
    failed to enter their desired state.
    Args:
        nodes: The set of nodes to obtain state from
        state: the string value of the state of interest
        duration: The total length of time to wait for nodes to enter or exit state
        interval: How frequently we check state (seconds)
        session: A requests session
        invert: Invert the selection critieria to NOT be equal to <state>.
    Side Effects:
        - This function logs information periodically, so as to give feedback to users
    Raises:
        - NodeStateMismatch; when duration has expired, a NodeStateMismatch is raised if our
        success threshold is not met.
    returns:
        - A set of nodes that did not reach the desired state; this is used by
        calling functions to further reduce the set of nodes to operate on as
        a threshold mechanism for partial success.
    """
    session = session or requests_retry_session()
    node_count = len(nodes)
    desired_state = "not %s" %(state) if invert else state
    node_list = list(nodes)
    node_set = set(nodes)
    end_time = time.time() + duration
    acceptable_failed_nodes = (1.0-success_threshold) * node_count
    minimum_required_success = node_count - acceptable_failed_nodes
    last_status_msg = None
    last_status_report = time.time()
    while time.time() < end_time:
        nodes_in_state = set(filter_nodes_by_state(state, node_list, invert=invert, session=session))
        if nodes_in_state == node_set:
            LOGGER.info("All nodes now in desired state (%s).", desired_state)
            return set()
        state_mismatch = node_set - nodes_in_state
        mismatch_count = len(state_mismatch)
        new_status_msg = 'Waiting on %s nodes to be %s' % (mismatch_count, desired_state)
        if new_status_msg != last_status_msg or last_status_report + 15 > time.time():
            LOGGER.info(new_status_msg)
            last_status_msg = new_status_msg
            last_status_report = time.time()
        # Wait for the interval to expire
        time.sleep(interval)
    # We're out of time! Evaluate if we have enough nodes in the desired
    # state to continue
    nodecount_in_desired_state = len(nodes_in_state)
    LOGGER.info("Wait for state period has finished; %s nodes in desired state, %s nodes are not in desired state.",
                nodecount_in_desired_state, mismatch_count)
    # Output at least a few nodes that are not ready
    if mismatch_count <= 5:
        LOGGER.warning("%s nodes failed to enter state '%s': %s",
                       mismatch_count, desired_state, ', '.join(sorted(state_mismatch)[:5]))
    else:
        LOGGER.warning("%s nodes failed to enter state '%s'; %s..." ,
                       mismatch_count, desired_state, ', '.join(sorted(state_mismatch[:5])))
    if nodecount_in_desired_state >= minimum_required_success:
        return state_mismatch
    else:
        percent_success = nodecount_in_desired_state / float(node_count)
        raise NodeStateMismatch("""
        Threshold for state change success not met; %s percent of nodes in desired state;.
        %s nodes required to pass the threshold."""
        %(percent_success, minimum_required_success))


def ready_drain(nodes, duration=70, interval=5, session=None):
    """
    Wait for nodes to exit the ready state. This is an ease of use call
    to the wait_for_state function, which is timeboxed.
    """
    session = session or requests_retry_session()
    return wait_for_state(nodes, 'Ready', duration=duration,  interval=interval, invert=True, session=session)
