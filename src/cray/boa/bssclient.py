# Copyright 2019, Cray Inc. All Rights Reserved.
from requests.exceptions import HTTPError
import logging
import json

from . import PROTOCOL
from .logutil import call_logger
from .rootfs.factory import ProvisionerFactory
from .connection import requests_retry_session

LOGGER = logging.getLogger(__name__)
SERVICE_NAME = 'cray-bss'
ENDPOINT = "%s://%s/boot/v1" % (PROTOCOL, SERVICE_NAME)


@call_logger
def set_bss_urls(agent, node_set, kernel_params, boot_artifacts, session=None):
    '''
    Tell the Boot Script Service (BSS) which boot artifacts are associated
    with each node.

    Currently, this is biased towards 'hosts' (i.e. xnames) rather than
    NIDS.

    Args:
        agent (instance of type Agent class): The Boot Orchestration Agent instance
        node_set (list): A list of nodes to assign the boot artifacts to
        kernel_params (string): Kernel parameters to assign to the node
        boot_artifacts(list): A list of boot_artifacts
        session (requests Session instance): An existing session to use

    Returns:
        Nothing

    Raises:
        KeyError -- If the boot_artifacts does not find either the initrd 
                    or kernel keys, this error is raised.
        ValueError -- if the kernel_parameters contains an 'initrd'
        requests.exceptions.HTTPError -- An HTTP error encountered while
                                         communicating with the
                                         Hardware State Manager
    '''
    session = session or requests_retry_session()
    params = assemble_kernel_boot_parameters(agent, kernel_params, boot_artifacts)
    LOGGER.info("Params: {}".format(params))
    url = "%s/bootparameters" % (ENDPOINT)

    # Figure out which nodes already exist in BSS and which do not
    # Query payload
    payload = {"hosts": list(node_set)}
    existing_nodes_flag = True

    try:
        resp = session.get(url, json=payload, verify=False)
        resp.raise_for_status()
    except HTTPError as err:
        if err.response.status_code == 404:
            existing_nodes_flag = False
        else:
            LOGGER.error("%s" % err)
            raise

    existing_nodes = set()
    if not existing_nodes_flag:
        non_existent_nodes = node_set
    else:
        nodes = node_set
        for nlist in resp.json():
            for node in nlist['hosts']:
                existing_nodes.add(node)
        non_existent_nodes = nodes - existing_nodes

    # Assignment payload
    if existing_nodes:
        # Existing nodes
        payload = {"hosts": list(existing_nodes),
                   "params": params,
                   "kernel": boot_artifacts['kernel'],
                   "initrd": boot_artifacts['initrd']}

        try:
            resp = session.put(url, data=json.dumps(payload), verify=False)
            resp.raise_for_status()
        except HTTPError as err:
            LOGGER.error("%s" % err)
            raise

    if non_existent_nodes:
        # Existing nodes
        payload = {"hosts": list(non_existent_nodes),
                   "params": params,
                   "kernel": boot_artifacts['kernel'],
                   "initrd": boot_artifacts['initrd']}

        try:
            resp = session.post(url, data=json.dumps(payload), verify=False)
            resp.raise_for_status()
        except HTTPError as err:
            LOGGER.error("%s" % err)
            raise


@call_logger
def assemble_kernel_boot_parameters(agent, kernel_parameters, boot_artifacts):
    '''
    Assemble the kernel boot parameters that we want to set in the
    Boot Script Service (BSS). Specifically, we need to ensure that
    the 'root' parameter exists and is set correctly.

    Start with kernel boot parameters that the agent was initialized with.

    TODO: CASMCMS-2590: When we have a better definition on this, this
    function will do something.

    Returns:
        A string containing the needed kernel boot parameters

    Raises: Nothing.
    '''
    pf = ProvisionerFactory(agent)
    boot_param_pieces = [kernel_parameters]
    rootfs_parameters = str(pf())
    if rootfs_parameters:
        boot_param_pieces.append(rootfs_parameters)

    # TODO: Assemble IMS provided parameters and add them to this list
    return ' '.join(boot_param_pieces)
