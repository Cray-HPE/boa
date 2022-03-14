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
import logging
import time

from .smdclient import filter_nodes_by_state, node_state_summary
from ..connection import requests_retry_session
from cray.boa import TransientException

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


def wait_for_nodes(boot_set_agent, state, invert=False, sleep_time=60, allowed_retries=-1,
                   **status):
    """
    Waits for all nodes to be in the <state> state.

    Args:
      boot_set_agent (BootSetAgent): The BootSetAgent instance, corresponding to a
        specific boots set, that we're interacting with.
      state (str): Wait until all the nodes are in this state or
                   not in this state if invert = True
      invert (binary): False -- Wait for all of the nodes to be in the input state
                       True -- Wait for all of the nodes to not be in the input state
      sleep_time (int): Number of seconds to sleep before rechecking nodes' states
      allowed_retries (int): Number of times to check that all nodes are ready;
                             if negative, no limits on retries are imposed
      status (keywords, dict): These parameters are for reporting status. They are optional otherwise.
        boot_set (str): The Boot Set we are reporting status for
        phase (str): The Phase we are reporting status for
        source (str): The source category in the Configuration phase that the nodes are
                      moving out of
        destination (str): The destination category in the Configuration phase that the nodes are
                      moving in to

    Raises:
      NodesNotReady (exception): If we reach a time-out stage, then it raises a
                                  NodesNotReady exception
    """
    session = boot_set_agent.smd_client
    num_retries = 0
    matching_nodes = None
    node_set = set(boot_set_agent.nodes)
    summary = None
    if status:
        previously_matching_nodes = set()
    while matching_nodes != node_set:
        matching_nodes = set(filter_nodes_by_state(state, list(node_set), invert, session))
        not_matching_nodes = node_set - matching_nodes
        number_not_matching = len(not_matching_nodes)
        # Report status
        if status:
            new_matching_nodes = set(matching_nodes) - previously_matching_nodes
            if new_matching_nodes:
                boot_set_agent.boot_set_status.move_nodes(new_matching_nodes,
                                                          status['phase'],
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
            # Update the nodes which failed boot based on expended retries
            boot_set_agent.boot_set_status.move_nodes(not_matching_nodes,
                                                      status['phase'],
                                                      status['source'],
                                                      'failed')
            boot_set_agent.failed_nodes |= not_matching_nodes
            if boot_set_agent.nodes:
                # If there are nodes that have arrived in the preferred state,
                # let them continue.
                return
            else:
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
    desired_state = "not %s" % (state) if invert else state
    node_list = list(nodes)
    node_set = set(nodes)
    end_time = time.time() + duration
    acceptable_failed_nodes = (1.0 - success_threshold) * node_count
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
        % (percent_success, minimum_required_success))


def ready_drain(nodes, duration=70, interval=5, session=None):
    """
    Wait for nodes to exit the ready state. This is an ease of use call
    to the wait_for_state function, which is timeboxed.
    """
    session = session or requests_retry_session()
    return wait_for_state(nodes, 'Ready', duration=duration, interval=interval, invert=True, session=session)
