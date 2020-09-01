#!/usr/bin/env python3

# Copyright 2019-2020 Hewlett Packard Enterprise Development LP

import datetime
import json
import sys
import os
import logging
import threading
from queue import Queue, Empty

from . import TransientException, NontransientException, InvalidInput
from .agent import Agent
from .bosclient import create_session_status, create_boot_set_status
from .bosclient import update_boot_set_status_nodes, update_session_status_metadata, update_boot_set_status_metadata
from .connection import wait_for_istio_proxy
from .cfsclient import CfsClient, CFSTimeout, CFSExhaustedRetries, wait_for_configuration, get_commit_id
from .preflight import PreflightCheck
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
# LOGGER.getLogger()

VALID_OPERATIONS = ["boot", "configure", "reboot", "shutdown"]


class IterableQueue(Queue):

    def __iter__(self):
        while True:
            try:
                yield self.get_nowait()
            except Empty:
                return


def run():
    """
    For each boot set in the session, launch a BOA agents to execute the desired operation
    on the nodes in that boot set.
    If configuration is enabled, do that at the end across all nodes.
    """
    wait_for_istio_proxy()
    try:
        namespace = os.environ.get('NAMESPACE', 'services')
        operation = os.environ["OPERATION"].lower()
        if operation not in VALID_OPERATIONS:
            raise NontransientException("{} is not a valid operation: {}. Canceling BOA Session.".format(operation, VALID_OPERATIONS))
        session_id = os.environ["SESSION_ID"]
        session_template_id = os.environ["SESSION_TEMPLATE_ID"]
        session_limit = os.environ["SESSION_LIMIT"]
        boot_session_file = "/mnt/boot_session/data.json"
        with open(boot_session_file, "r") as stream:
            try:
                session_data = json.load(stream)
            except Exception as exc:
                LOGGER.error("Unable to read file: %s -- Error: %s",
                             boot_session_file, exc)
                raise

        # Deal with Boot Session information first
        partition = session_data.get('partition')
        enable_cfs = session_data['enable_cfs']
        cfs_data = session_data.get("cfs", {})
        cfs_clone_url = cfs_data.get("clone_url", session_data.get("cfs_url"))
        cfs_branch = cfs_data.get("branch", session_data.get("cfs_branch"))
        cfs_commit = cfs_data.get("commit")
        cfs_playbook = cfs_data.get("playbook")

        # Deal with Each Boot Set
        agents = []
        boot_sets = []
        for bs_name, bs_data in session_data['boot_sets'].items():
            boot_sets.append(bs_name)
            node_groups = bs_data.get('node_groups', [])
            node_list = bs_data.get('node_list', [])
            node_roles_groups = bs_data.get("node_roles_groups", [])
            kernel_parameters = bs_data.get("kernel_parameters", '')
            ims_image_id = bs_data.get("ims_image_id", '')
            path = bs_data.get("path", '')
            type = bs_data.get("type", '')
            etag = bs_data.get("etag", '')
            network = bs_data.get("network", '')
            # boot_ordinal = bs_data.get("boot_ordinal", '')
            # shutdown_ordinal = bs_data.get("shutdown_ordinal", '')
            rootfs_provider = bs_data.get("rootfs_provider", '')
            rootfs_passthrough = bs_data.get("rootfs_provider_passthrough", '')
            agent = Agent(namespace,
                          session_id,
                          session_template_id,
                          session_limit,
                          bs_name,
                          node_list,
                          node_groups,
                          node_roles_groups,
                          operation,
                          ims_image_id,
                          path,
                          type,
                          etag,
                          kernel_parameters,
                          network,
                          rootfs_provider,
                          rootfs_passthrough,
                          partition,
                          enable_cfs)
            agents.append(agent)
    except KeyError as ke:
        raise TemplateException("Missing required variable: %s" % (ke)) from ke
    except InvalidInput as err:
        raise TemplateException("Template error: %s" % err) from err

    LOGGER.info("Session: %s", session_id)
    LOGGER.info("Operation: %s", operation)
    # Because we are only doing a CFS pre-flight check, we do not need to pass in the agent parameter,
    # which is only used to check the S3 micro-service.
    pfc = PreflightCheck(None, operation, cfs_required=enable_cfs, rootfs_provisioner=None)
    pfc(['cfs'])

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

    # Record the time BOA launched
    create_session_status(session_id, boot_sets, start_time=str(datetime.datetime.now()))

    # Stage desired node configuration in CFS
    cfs_client = CfsClient()
    # BOS instructs CFS to disable CFS scheduling on nodes that are shutdown,
    # off, or will be shut off as part of a reboot. However, for nodes that
    # are simply going to be reconfigured, the CFS component should remain enabled
    # to allow for (re)-configuration.
    enabled = operation in ["configure", ]
    LOGGER.info("Setting desired CFS configuration for nodes in Session: %s", session_id)
    if not cfs_commit:
        cfs_commit = get_commit_id(cfs_client, cfs_clone_url, cfs_branch)
    cfs_client.set_configuration(node_list, cfs_commit, cfs_clone_url, cfs_playbook,
                                 enabled=enabled)

    # Launch individual agents for all non-configure operations
    if operation not in ["configure"]:
        exception_queue = IterableQueue()
        boa_threads = [threading.Thread(target=agent, kwargs={'queue': exception_queue})
                       for agent in agents]
        _ = [thread.start() for thread in boa_threads]
        _ = [thread.join() for thread in boa_threads]
        # When all agents are done, reraise exceptions from any of the threads
        for exception_fields in exception_queue:
            exception_type, exception_value, exception_traceback = exception_fields
            raise NontransientException("Unable to apply bootset operation.")

    # Wait for configuration to finish; Note: Some of these are likely already in flight
    # Create status for the configure operation because no BOA agent is launched which would
    # create the status as happens for other operations.
    # import rpdb;rpdb.set_trace()
    if enable_cfs and operation in ["configure"]:
        for bs, nodes in node_lookup_by_boot_set.items():
            create_boot_set_status(session_id, bs, ["configure"], list(nodes))

    # Configure the nodes
    if enable_cfs and operation in ["boot", "reboot", "configure"]:
        try:
            wait_for_configuration(node_list, session_id,
                                   node_lookup_by_boot_set=node_lookup_by_boot_set)
        except CFSTimeout as cfs_timeout:
            LOGGER.error(cfs_timeout)
            raise
        except CFSExhaustedRetries as cfs_exhaust:
            LOGGER.error(cfs_exhaust)
            raise
        else:
            LOGGER.info("Configuration complete.")

            # Record the end of each Boot Set.
            # This only applies when we are doing configuration.
            # If not doing configuration, the Boot Sets themselves
            # updated their own end.
            # We are updating the stop_time for both the Boot Set
            # itself as well as the 'configure' phase.
            for boot_set in boot_sets:
                update_boot_set_status_metadata(session_id,
                                                boot_set,
                                                'configure',
                                                stop_time=str(datetime.datetime.now()))
                update_boot_set_status_metadata(session_id,
                                                boot_set,
                                                None,
                                                stop_time=str(datetime.datetime.now()))

    # Record the time BOA master finished in the database
    try:
        update_session_status_metadata(session_id, stop_time=str(datetime.datetime.now()))
    except Exception as exc:
        LOGGER.error("There was an error while setting the status to Done: %s", exc)
        raise


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
