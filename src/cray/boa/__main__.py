#!/usr/bin/env python3
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
import json
import sys
import os
import logging
import threading
from queue import Queue, Empty

from . import TransientException, NontransientException, InvalidInput
from .agent import BootSetAgent
from .bosclient import SessionStatus
from .connection import wait_for_istio_proxy
from .sessiontemplate import TemplateException

LOGGER = logging.getLogger("cray.boa")
# Note; the above configures the project level logger, which is what we
# intend. If additional logging is desired, the root logger can also be
# configured. Configuring only the project level logger allows only cray.boa
# logs to populate to the event stream. Configuring the root logger allows
# for all related python libraries (requests) to also log to standard out.
# Typically, this is not desired because the requests library is a well
# understood, externally maintained package. We do not expose the ability
# to show logs from other project code bases here. To do that, simply
# uncomment the below:
# LOGGER = logging.getLogger()

VALID_OPERATIONS = ["boot", "configure", "reboot", "shutdown"]
BOOT_SESSION_FILE = "/mnt/boot_session/data.json"


class IterableQueue(Queue):

    def __iter__(self):
        while True:
            try:
                yield self.get_nowait()
            except Empty:
                return


def run():
    """
    For each boot set in the session, launch a BOA agent to execute the desired operation
    on the nodes in that boot set.
    """
    wait_for_istio_proxy()
    try:
        operation = os.environ["OPERATION"].lower()
        if operation not in VALID_OPERATIONS:
            raise NontransientException("{} is not a valid operation: {}. Canceling BOA Session.".format(operation, VALID_OPERATIONS))
        session_id = os.environ["SESSION_ID"]
        session_template_id = os.environ["SESSION_TEMPLATE_ID"]
        session_limit = os.environ["SESSION_LIMIT"]
        with open(BOOT_SESSION_FILE, "r") as stream:
            try:
                session_data = json.load(stream)
            except Exception as exc:
                LOGGER.error("Unable to read file: %s -- Error: %s",
                             BOOT_SESSION_FILE, exc)
                raise
        # Create an Agent for each Boot Set
        agents = []
        boot_sets = []
        LOGGER.debug("Starting with session: %s", session_data)
        for bs_name in session_data['boot_sets'].keys():
            boot_sets.append(bs_name)
            agent = BootSetAgent(session_id, session_template_id, bs_name, operation,
                                 session_limit, BOOT_SESSION_FILE)
            agents.append(agent)
    except KeyError as ke:
        raise TemplateException("Missing required variable: %s" % (ke)) from ke
    except InvalidInput as err:
        raise TemplateException("Template error: %s" % err) from err

    LOGGER.info("Session: %s", session_id)
    LOGGER.info("Operation: %s", operation)
    node_list = set()
    # Look up which Boot Set a node is in. Keys are nodes. Boot Sets are values.
    boot_set_lookup_by_node = {}
    node_lookup_by_boot_set = {}
    for agent in agents:
        node_lookup_by_boot_set[str(agent.boot_set)] = agent.nodes
        node_list |= agent.nodes
        for node in agent.nodes:
            boot_set_lookup_by_node[node] = agent.boot_set
    node_list = list(node_list)

    # For the duration of running the agent, keep records of state.
    with SessionStatus.CreateOrReference(session_id, boot_sets):
        exception_queue = IterableQueue()
        boa_threads = [threading.Thread(target=agent, kwargs={'queue': exception_queue})
                       for agent in agents]
        _ = [thread.start() for thread in boa_threads]
        _ = [thread.join() for thread in boa_threads]
        # When all agents are done, reraise exceptions from any of the threads
        for exception_fields in exception_queue:
            exception_type, exception_value, exception_traceback = exception_fields
            raise NontransientException("Unable to apply boot set operation: %s\n%s\n%s"
                                        % (exception_type, exception_value, exception_traceback))


if __name__ == "__main__":
    # Format logs for stdout
    _log_level = getattr(logging, os.environ.get('LOG_LEVEL', 'INFO').upper(), logging.INFO)
    _stream_handler = logging.StreamHandler()
    _stream_handler.setLevel(_log_level)
    _stream_handler.setFormatter(logging.Formatter("%(asctime)-15s - %(levelname)-7s - %(name)s - %(message)s"))
    LOGGER.addHandler(_stream_handler)
    LOGGER.setLevel(_log_level)
    LOGGER.debug("BOA starting")
    try:
        run()
    except TransientException:
        LOGGER.exception("A recoverable error has been detected; Boot Orchestration Agent is now "
                         "exiting with a non-zero response to allow for rescheduling. ")
        sys.exit(1)
    except NontransientException:
        LOGGER.exception("Fatal conditions have been detected with this run of Boot Orchestration "
                         "that are not expected to be recoverable through additional iterations. "
                         "The application is exiting with a zero status to prevent job rescheduling.")
        sys.exit(0)
    except Exception as err:
        LOGGER.exception("An unanticipated exception occurred during launch: %s; terminating attempt "
                         "with one for perceived transient error. The following stack should be "
                         "captured and filed as a bug so that the exception can be classified as "
                         "recoverable or non-recoverable.", err)
        sys.exit(1)
    LOGGER.info("BOA completed requested operation.")
