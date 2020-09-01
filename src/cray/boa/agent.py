# Copyright 2019-2020 Hewlett Packard Enterprise Development LP

import datetime
import logging
import os
import requests
import sys

from . import ServiceNotReady, NontransientException
from .sessiontemplate import TemplateException
from .bosclient import create_boot_set_status, update_boot_set_status_nodes, update_boot_set_status_errors, update_boot_set_status_metadata
from .capmcclient import boot, graceful_shutdown
from .arsclient import get_ars_download_uris
from .bssclient import set_bss_urls
from .imsclient import get_image_artifacts
from .logutil import call_logger
from .smd.smdclient import filter_split
from .smd.smdinventory import SMDInventory
from .preflight import PreflightCheck
from .smd.wait_for_nodes import wait_for_nodes, NodesNotReady, ready_drain
from .bootimagemetadata.factory import BootImageMetaDataFactory
from .s3client import TooManyArtifacts, ArtifactMissing

LOGGER = logging.getLogger(__name__)


class Agent(object):
    '''
    The Boot Orchestration Agent will handle booting and shutting down nodes.
    '''

    def __init__(self, namespace, session_id, session_template_id, session_limit,
                 boot_set_name, node_list, node_groups, node_roles_groups, operation,
                 ims_image_id, path, type_, etag, kernel_parameters, provisioning_network,
                 rootfs_provisioner, rootfs_provisioner_passthrough=None, partition=None,
                 enable_cfs=False):
        '''
        Args:
            node_list (list): The nodes we want to operate on
            node_groups: (list): A list of node groups we want to operate on
            node_roles_groups: (list): A list of SMD hardware types to operate on
            operation (str): The operation -- boot or shutdown -- that we want
                             to do to the nodes
            ims_image_id parameter (uuid): ID of the IMS image we want to boot the nodes
                                 with
            path parameter (string): ID of the image we want to boot the nodes
                                 with
            type_ parameter (string): Mime type_ identifying the path
            etag parameter (string: 'Entity tag' for the path
            kernel_parameters (str): The kernel boot parameters
            provisioning_network (str): The canonical name of the network to use for
                communicating with boot services.
            rootfs_provisioner (str): The name of the root filesystem provisioning
                mechanism for the specified path. This value informs the root=
                kernel boot parameter used by the image's initrd during boot
                operations.
            rootfs_provisioner_passthrough (str): A string containing additional
                provisioning options to append to the proc cmdline rootfs field.
            partition (str): The name of the partition to operate within. Partitions
                do not yet exist, so we default to None.
            session_id (str): Session ID of which the Boot Set is a subset
            session_template_id (str): Session Template ID; the session is created from applying the
                operation to the Session Template ID
            enable_cfs (bool): Whether CFS has been enabled or not
        '''
        self._namespace = namespace
        self._session_id = session_id
        self._session_template_id = session_template_id
        self._session_limit = session_limit
        self.boot_set = boot_set_name
        self._node_list = node_list
        self._node_groups = node_groups
        self._node_roles_groups = node_roles_groups
        self._partition = partition
        self._operation = operation
        self._ims_image_id = ims_image_id
        self.path = path
        self._path_type = type_
        self.etag = etag
        self._kernel_parameters = kernel_parameters
        self._provisioning_network = provisioning_network
        self._rootfs_provisioner = rootfs_provisioner
        self._rootfs_provisioner_passthrough = rootfs_provisioner_passthrough
        self._enable_cfs = enable_cfs

        self.inventory = SMDInventory(self._partition)

    def do_stage(self, status_val, func, *arg, **kwargs):
        LOGGER.info('%s_start' % (status_val))
        response = func(*arg, **kwargs)
        LOGGER.info('%s_finished' % (status_val))
        return response

    @property
    def ready(self):
        """
        Returns 'True' if all required microservices for the associated action
        are available. Otherwise, this returns False.

        Readiness is a function of system state and BOA action. Different actions
        require different sets of functional microservices.
        """
        try:
            pfc = PreflightCheck(self, self._operation,
                                 rootfs_provisioner=self._rootfs_provisioner,
                                 cfs_required=False)
            # CFS isn't checked here because that check should happen globally
            # for all boot agents. Because boot agents are not group aware, we
            # need to issue this check once elsewhere.
            pfc()
        except ServiceNotReady:
            return False
        return True

    @property
    def nodes(self):
        """
        Returns
          A set of nodes
        """
        if not hasattr(self, '_nodes'):
            self._nodes = set()
            # Populate from nodelist
            for group in self._node_list:
                self._nodes.add(group)
            # Populate from nodegroups
            for group_name in self._node_groups:
                if group_name not in self.inventory.groups:
                    LOGGER.warning("No hardware matching label {}".format(group_name))
                    continue
                self._nodes |= self.inventory.groups[group_name]
            # Populate from node_roles_groups
            for role_name in self._node_roles_groups:
                if role_name not in self.inventory.roles:
                    LOGGER.warning("No hardware matching role {}".format(role_name))
                    continue
                self._nodes |= self.inventory.roles[role_name]
            # Filter to nodes defined by limit
            self._apply_limit()
            if not self._nodes:
                LOGGER.warning("No nodes were found to act on.")
                return self._nodes
            # Filter down to only enabled nodes
            enabled, disabled, empty = filter_split(list(self._nodes))
            if disabled:
                num_disabled = len(disabled)
                LOGGER.info(
                    "Will not perform operation on "
                    "%s node%s that %s marked as disabled." % (num_disabled,
                                                               num_disabled != 1 and 's' or '',
                                                               num_disabled != 1 and 'are' or 'is'))
                LOGGER.debug("The following node%s cannot be operated on because %s disabled: %s"
                             % (num_disabled != 1 and 's' or '',
                                num_disabled != 1 and 'they are' or 'it is',
                                ', '.join(sorted(disabled))))
            if empty:
                num_empty = len(empty)
                LOGGER.info(
                    "Will not perform operation on "
                    "%s node%s that %s marked as empty." % (num_empty,
                                                            num_empty != 1 and 's' or '',
                                                            num_empty != 1 and 'are' or 'is'))
                LOGGER.debug("The following node%s cannot be operated on because %s empty: %s"
                             % (num_empty != 1 and 's' or '',
                                num_empty != 1 and 'they are' or 'it is',
                                ', '.join(sorted(empty))))
            self._nodes = set(enabled) - set(empty)
        return self._nodes

    def _apply_limit(self):
        if not self._session_limit:
            # No limit is defined, so all nodes are allowed
            return
        LOGGER.info('Applying limit to session: {}'.format(self._session_limit))
        limit_node_set = set()
        for limit in self._session_limit.split(','):
            if limit[0] == '&':
                limit = limit[1:]
                op = limit_node_set.intersection
            elif limit[0] == '!':
                limit = limit[1:]
                op = limit_node_set.difference
            else:
                op = limit_node_set.union

            limit_nodes = set([limit])
            if limit == 'all' or limit == '*':
                limit_nodes = self._nodes
            elif limit in self.inventory:
                limit_nodes = self.inventory[limit]
            limit_node_set = op(limit_nodes)
        self._nodes = self._nodes.intersection(limit_node_set)

    @property
    def img_artifacts(self):
        if not hasattr(self, '_img_artifacts'):
            try:
                self._img_artifacts = self.do_stage("boot_get_image_artifacts",
                                                    get_image_artifacts, self._ims_image_id)
            except requests.exceptions.HTTPError as err:
                LOGGER.error("Failed interacting with Image Management Service (IMS)", exc_info=err)
                raise ServiceNotReady(err)
            if not self._img_artifacts:
                msg = "IMS Image Record {} not found".format(self._ims_image_id)
                LOGGER.error(msg)
                raise TemplateException(msg)
        return self._img_artifacts

    @property
    def ars_uris(self):
        if not hasattr(self, '_ars_uris'):
            try:
                self._ars_uris = self.do_stage('boot_get_artifacts_urls', get_ars_download_uris, self.img_artifacts)
            except requests.exceptions.HTTPError as err:
                LOGGER.error("Failed interacting with Artifact Repository Service (ARS)", exc_info=err)
                raise ServiceNotReady(err) from err
        return self._ars_uris

    @property
    def artifact_paths(self):
        """
        Hunt down the object that contains information about all of the boot artifacts

        Returns:
          A dictionary containing paths to each of the boot artifacts; The artifact names are the
          keys and the paths are the values
          Example:
          boot_artifact['kernel'] = 's3://bucket/key'
        """
        # Use cached value
        if hasattr(self, "_boot_artifacts"):
            return self._boot_artifacts

        bimd = BootImageMetaDataFactory(self)()

        try:
            # Assemble artifacts
            # CASMCMS-4610: It would be good if the bimd had a support parameters list that
            # we could just scroll through, and that would provide the dictionary
            # instead of potentially calling unsupported functions.
            boot_artifacts = {}
            boot_artifacts['kernel'] = bimd.kernel_path
            boot_artifacts['initrd'] = bimd.initrd_path
            boot_artifacts['rootfs'] = bimd.rootfs_path
            boot_artifacts['rootfs_etag'] = bimd.rootfs_etag
            self._boot_artifacts = boot_artifacts
            return self._boot_artifacts
        except (ValueError, ArtifactMissing, TooManyArtifacts) as err:
            LOGGER.error("Obtaining boot artifacts failed: %s", err)
            raise

    def __repr__(self):
        msg = ["Boot Agent", "Image: %s" % (self._ims_image_id)]
        return ' '.join(msg)

    def __call__(self, queue):
        if not self.nodes:
            LOGGER.info("No remaining nodes available for operation '%s'.", self._operation)
            return
        try:
            if not self.ready:
                LOGGER.error("Cannot perform operation '%s'; system resources are not ready and failed precheck.",
                             self._operation)
                raise NontransientException("Agent failed preflight check.")
        except (NontransientException, Exception) as exception:
            # Any NontransientExceptions that happened as a result of calling the
            # preflight check should be aggregated onto the queue to be later
            # unpacked by the calling function. There, they are re-raised with
            # the original path.
            LOGGER.error(exception)
            queue.put(sys.exc_info())
            return

        create_boot_set_status(self._session_id, self.boot_set,
                               self._phases_per_operation(),
                               list(self.nodes))
        operation_function = getattr(self, self._operation)
        try:
            exception_occurred = False
            operation_function()
        except Exception as exception:
            # Any exceptions that happened as a result of calling this agent
            # should be aggregated onto the queue to be later unpacked by the
            # calling function. There, they are re-raised with the original path
            LOGGER.error(exception)
            queue.put(sys.exc_info())
            exception_occurred = True
        # If any of the following is true, then the Boot Set can be marked complete.
        # If we are just shutting down
        # If we are not doing configuration
        # If an exception occurred
        if self._operation == 'shutdown' or not self._enable_cfs or exception_occurred:
            update_boot_set_status_metadata(self._session_id,
                                            self.boot_set,
                                            None,
                                            stop_time=str(datetime.datetime.now()))

    @call_logger
    def _phases_per_operation(self):
        """
        Return the phases based on the operation
        """
        phases = {
            "shutdown": ["shutdown"],
            "configure": ["configure"],
            "boot": ["boot"],
            "reboot": ["shutdown", "boot"]
            }
        if self._enable_cfs:
            phases["boot"].append('configure')
            phases["reboot"].append('configure')
        return phases[self._operation]

    @call_logger
    def boot(self):
        """
        Initializes a boot.

        Raises:
          requests.exceptions.HTTPError -- If it fails to interact with IMS
          ValueError -- if the IMS image_id returns no boot artifacts
        """
        LOGGER.info("Booting the Session: %s Set: %s", self._session_id, self.boot_set)
        try:
            self.do_stage("boot_set_bss_urls", set_bss_urls, self,
                          self.nodes, self._kernel_parameters, self.artifact_paths)
        except (KeyError, ValueError, requests.exceptions.HTTPError,
                ArtifactMissing, TooManyArtifacts) as err:
            LOGGER.error("Failed interacting with Boot Script Service (BSS)", exc_info=err)
            raise ServiceNotReady(err) from err

        self.do_the_thing_and_tell_everyone('boot')

        # Wait for the nodes in question to boot
        node_sleep_time = int(os.getenv("NODE_STATE_CHECK_SLEEP_INTERVAL", 5))
        node_allowed_retries = int(os.getenv("NODE_STATE_CHECK_NUMBER_OF_RETRIES", 120))
        try:
            wait_for_nodes('Ready', self.nodes, False, node_sleep_time, node_allowed_retries,
                           session_id=self._session_id,
                           boot_set=self.boot_set,
                           phase="boot",
                           source="in_progress",
                           destination="succeeded")
        except NodesNotReady as err:
            LOGGER.error("Nodes were not ready: %s", err)
            # In this case, the nodes didn't boot within their required window; we want to treat these
            # failures as if they're not recoverable so that K8s does not re-attempt to boot nodes.
            # Otherwise, BOA will start up again and prevent users from detecting and fixing errors.
            #
            # In the future, there would be failure tolerations implemented here that would feed into
            raise NontransientException(err) from err

        # We made it past waiting for all of the nodes, so we are going to mark the Boot Set as
        # having finished this phase.
        update_boot_set_status_metadata(self._session_id,
                                        self.boot_set,
                                        "boot",
                                        stop_time=str(datetime.datetime.now()))

    @call_logger
    def do_the_thing_and_tell_everyone(self, operation):
        """
        Runs the requested operation. Further, it logs it and reports its status.
        
        Args
          operation (str): The desired operation
        
        Raises
          NontransientException -- Any errors
        """
        LOGGER.info("Performing %s on Boot Set: '%s' in Session %s", operation, self.boot_set,
                    self._session_id)
        valid_operations = ['shutdown', 'boot']
        if operation not in valid_operations:
            raise ValueError("%s is an invalid operation. Must be %s", operation,
                             valid_operations)
        func_map = {'shutdown': graceful_shutdown,
                    "boot": boot}
        args_map = {'shutdown': {'grace_window': os.environ.get('GRACEFUL_SHUTDOWN_TIMEOUT', 300),
                                'hard_window': os.environ.get('FORCEFUL_SHUTDOWN_TIMEOUT', 180),
                                'graceful_prewait': os.environ.get('GRACEFUL_SHUTDOWN_PREWAIT', 20),
                                'frequency': os.environ.get('POWER_STATUS_FREQUENCY', 10)},
                   'boot': {}}
        default_arguments = {'grace_window': 300, 'hard_window':180, 'graceful_prewait': 20,
                             'frequency': 10}
        for tag, value in default_arguments.items():
            # Each of these tags should default to integer values if they're blank; if they're not blank,
            # type cast them as integers
            if args_map['shutdown'][tag] == '':
                # The value is a string, and it's a null string
                args_map['shutdown'][tag] = value
            else:
                # The tag was specified, but it needs to be saved as an integer
                args_map['shutdown'][tag] = int(args_map['shutdown'][tag])
        func = func_map[operation]
        kwargs = args_map[operation]
        update_boot_set_status_nodes(self._session_id,
                                     self.boot_set,
                                     operation,
                                     list(self.nodes),
                                     "not_started",
                                     "in_progress")
        try:
            failed_nodes, errors = func(self.nodes, **kwargs)
        except requests.exceptions.HTTPError as err:
            LOGGER.error("Failed interacting with Cray Advanced Platform and Management Control "
                         "(CAPMC)",
                         exc_info=err)
            raise ServiceNotReady(err) from err

        succeeded_nodes = self.nodes - failed_nodes
        if operation not in ['boot']:
            update_boot_set_status_nodes(self._session_id,
                                         self.boot_set,
                                         operation,
                                         list(succeeded_nodes),
                                         "in_progress",
                                         "succeeded")
            update_boot_set_status_metadata(self._session_id,
                                            self.boot_set,
                                            operation,
                                            stop_time=str(datetime.datetime.now()))
        if failed_nodes:
            update_boot_set_status_nodes(self._session_id,
                                         self.boot_set,
                                         operation,
                                         list(failed_nodes),
                                         "in_progress",
                                         "failed")

            if errors:
                update_boot_set_status_errors(self._session_id,
                                              self.boot_set,
                                              operation,
                                              errors)
            raise NontransientException("Nodes failed to %s.", operation)

    @call_logger
    def shutdown(self):
        """
        Shuts down the nodes in the Agent's node list.
        
        Raises:
          NontransientException -- when it fails to power down nodes.
        """
        self.do_the_thing_and_tell_everyone('shutdown')

    @call_logger
    def reboot(self):
        LOGGER.info("Rebooting the Session: %s Set: %s", self._session_id, self.boot_set)
        self.shutdown()
        # CASMCMS-5183; SMD status fields need to have a chance to 'catch up' with the
        # actual state of the components; there is a delay between powering off a node
        # and nodes reaching this "ready drained" state. SMD corrects this via:
        # - Subscribes to the redfish events for power off
        # - Changes from ready after a period of 60 seconds
        # So, in the case where BOA first shuts nodes down and then reboots them, a period
        # of time must elapse for all nodes in question to exit their "ready" status.
        # For other actions, like shutdown, we don't particularly care about the SMD status field
        # of the nodes, because we know SMD will eventually catch up. For straight booting
        # a node, any number of possible actors can be acting on the actual power and booted
        # SMD status, so there is a small but very unlikely chance that ready nodes will
        # continue to be ready w/o a drain. With sufficiently large numbers of booting components,
        # this becomes a non-issue in part because the state does correct itself. Coupled with the
        # fact that boot is very rarely used, it is unlikely that this will be an issue, so we are
        # choosing to not protect against this unlikely edge case because it will needlessly
        # slow the 'boot' operation down.
        ready_drain(self.nodes)
        self.boot()
