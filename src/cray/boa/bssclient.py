# Copyright 2019-2020 Hewlett Packard Enterprise Development LP
from requests.exceptions import HTTPError
import logging
import json

from . import PROTOCOL
from .logutil import call_logger
from .rootfs.factory import ProviderFactory
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
    params = assemble_kernel_boot_parameters(agent, kernel_params)
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

    for node_set1 in [existing_nodes, non_existent_nodes]:
        if not node_set1:
            continue

        # Assignment payload
        payload = {"hosts": list(node_set1),
                   "params": params,
                   "kernel": boot_artifacts['kernel'],
                   "initrd": boot_artifacts['initrd']}

        try:
            resp = session.put(url, data=json.dumps(payload), verify=False)
            resp.raise_for_status()
        except HTTPError as err:
            LOGGER.error("%s" % err)
            raise


@call_logger
def assemble_kernel_boot_parameters(agent, kernel_parameters):
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
    pf = ProviderFactory(agent)
    boot_param_pieces = [kernel_parameters]
    provider = pf()
    rootfs_parameters = str(provider)
    if rootfs_parameters:
        boot_param_pieces.append(rootfs_parameters)
    nmd_parameters = provider.nmd_field
    if nmd_parameters:
        boot_param_pieces.append(nmd_parameters)
    # Add the Session ID to the kernel parameters
    boot_param_pieces.append("bos_session_id={}".format(agent.session_id))

    return ' '.join(boot_param_pieces)
