#
# MIT License
#
# (C) Copyright 2019-2023 Hewlett Packard Enterprise Development LP
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
import os
import requests
import sys
import json
import traceback

from botocore.exceptions import ClientError
from . import ServiceNotReady, NontransientException
from .bosclient import SessionStatus, BootSetStatus, now_string
from .bosclient import SERVICE_ENDPOINT as BOS_SERVICE_ENDPOINT
from cray.boa.connection import requests_retry_session
from .capmcclient import graceful_shutdown, power, status
from .cfsclient import CfsClient, wait_for_configuration
from .bssclient import set_bss_urls
from .logutil import call_logger
from .smd.smdclient import filter_split
from .smd.smdinventory import SMDInventory
from .preflight import PreflightCheck
from .smd.wait_for_nodes import wait_for_nodes, NodesNotReady, ready_drain
from .bootimagemetadata.factory import BootImageMetaDataFactory
from .s3client import S3Object, TooManyArtifacts, ArtifactMissing
from .rootfs.factory import ProviderFactory

LOGGER = logging.getLogger(__name__)


class BootSetAgent(object):
    '''
    The Boot Orchestration Agent will handle booting and shutting down nodes.
    '''
    PHASES = {"shutdown": ["shutdown"],
              "configure": ["stage_configuration", "wait_for_configuration"],
              "boot": ["stage_configuration", "boot", "wait_for_configuration"],
              "reboot": ["stage_configuration", "shutdown", "boot", "wait_for_configuration"]}

    STATUS_FIELDS = {'shutdown': ['shutdown'],
                     'configure': ['configure'],
                     'boot': ['boot', 'configure'],
                     'reboot': ['shutdown', 'boot', 'configure']}

    def __init__(self, session_id, session_template_id, boot_set_name, operation,
                 session_limit=None, file_path=None):
        '''
        Args:
            session_id (str): Session ID of which the Boot Set is a subset
            session_template_id (str): Session Template ID; the session is created from applying the
                operation to the Session Template ID
            boot_set_name: The name of the bootset the agent will operate on.
            operation (str): The operation -- boot or shutdown -- that we want
                to do to the nodes
            session_limit (str): An optional parameter that allows a whitelisting function
                for nodes in the bootset; typically this is used as a one-off ad-hoc parameter
                for only applying changes to a small subset of nodes in the bootset.
            file_path (str): The location of a given file that contains information
                about subfields within an individual boot set.

        This creates an instance of the boot set agent; there should be one per boot set
        defined within a Session.
        '''
        self.session_id = session_id
        self.session_template_id = session_template_id
        self.session_limit = session_limit
        self.boot_set = boot_set_name
        self.operation = operation
        self.file_path = file_path

        assert operation in self.PHASES

        self._session_data = None
        self._bos_client = None
        self._capmc_client = None
        self._smd_client = None
        self._boot_artifacts = None
        self._session_status = None
        self._boot_set_status = None
        self._cfs_configuration = None
        self._cfs_client = None
        self._preflight_check = None
        self._inventory = None
        self.failed_nodes = set()

    @property
    def bos_client(self):
        if self._bos_client:
            return self._bos_client
        self._bos_client = requests_retry_session()
        return self._bos_client

    @property
    def session_template_uri(self):
        """
        The BOS session template URI that corresponds to this boot set agent.
        """
        return os.path.join(BOS_SERVICE_ENDPOINT, 'sessiontemplate', self.session_template_id)

    @property
    def session_uri(self):
        return os.path.join(BOS_SERVICE_ENDPOINT, 'session', self.session_id)

    @property
    def session_data(self):
        """
        Session data can come from multiple sources, depending on
        how <self> is defined. If it is defined in a local file, read it.
        Otherwise, if we're reading from the BOS API, it can be obtained
        from the corresponding session template.

        This information is cached upon first use; if the file or the
        API values are changed, there will be a mismatch of information.
        From the agent's perspective, this structure is immutable.

        Most importantly, _session_data contains references to BootSet data,
        for which this class is most concerned with.
        """
        if self._session_data:
            return self._session_data
        if self.file_path:
            with open(self.file_path, "r") as stream:
                try:
                    self._session_data = json.loads(stream.read())
                except Exception as exc:
                    LOGGER.error("Unable to read file: %s -- Error: %s",
                                 self.file_path, exc)
                    raise
            return self._session_data
        # There was no file path, so get it from the API
        response = self.bos_client.get(self.session_template_uri)
        try:
            response.raise_for_status()
        except requests.HTTPError as hpe:
            LOGGER.info("Unable to acquire session_data from BOS: %s", hpe)
            raise
        self._session_data = json.loads(response.text)
        return self._session_data

    @property
    def partition(self):
        """
        Currently unused, since Shasta has no concept of partitions yet.
        """
        return self.session_data.get('partition', None)

    @property
    def enable_cfs(self):
        """
        Likely soon to be deprecated when CFS information is truly per bootset.

        Not to be confused with cfs_enabled, the enable_cfs flag is a
        boolean value specified at the session template level. This doesn't
        currently do much and should likely be deprecated when cfs configuration
        options are migrated to a per boot set implementation. We expose it here
        as a call out that the field needs deprecation, but we'll expose it
        in the meantime.
        """
        return self.session_data.get('enable_cfs', True)

    @property
    def _cfs_data(self):
        """
        A session template level field, containing a dictionary, that pertains
        to how CFS should handle a given session.
        """
        return self.session_data.get('cfs', {})

    @property
    def cfs_configuration(self):
        """
        A reference to CFS V2 'configuration' entry. With V2 of CFS, we expect
        to always interact with CFS using a cfs_configuration field. If we weren't
        given one in the sessiontemplate/configmap data, then we create one from
        the v2 fields.
        """
        if self._cfs_configuration:
            return self._cfs_configuration
        if 'configuration' in self._cfs_data:
            return self._cfs_data['configuration']
        self._cfs_configuration = self.cfs_client.create_configuration(commit=self.cfs_commit,
                                                                       branch=self.cfs_branch,
                                                                       repo_url=self.cfs_clone_url,
                                                                       playbook=self.cfs_playbook)
        return self._cfs_configuration

    @property
    def cfs_clone_url(self):
        """
        The URL to use (if any) that corresponds to this boot agent
        """
        return self._cfs_data.get('clone_url', None)

    @property
    def cfs_branch(self):
        """
        cfs_branch (str): The VCS branch to use;
        """
        return self._cfs_data.get('branch', None)

    @property
    def cfs_playbook(self):
        return self._cfs_data.get('playbook', None)

    @property
    def cfs_commit(self):
        """
        cfs_commit (str): The VCS commit id to use;
        """
        return self._cfs_data.get('commit', None)

    @property
    def cfs_enabled(self):
        """
        CFS is enabled if the appropriate fields have been provided to the Boot Agent;
        otherwise it is disabled. This prevents us from having a specific 'enabled'
        or disabled flag for a given boot set. If the information is provided, it
        will be used to configure self.nodes.

        self.cfs_playbook is allowed to be None because CFS has a concept of a default
        playbook if it is otherwise unspecified.
        """
        return all([self.cfs_configuration, self.enable_cfs])

    @property
    def boot_set_data(self):
        """
        The object structure from a session template that corresponds to
        this boot set agent.
        """
        return self.session_data['boot_sets'][self.boot_set]

    @property
    def node_list(self):
        """
        The nodes we want to operate on. A static list of nodes, provided
        by the user, as a target for actions by this agent.
        Contributes to the definition of self.nodes.
        """
        return self.boot_set_data.get('node_list', [])

    @property
    def node_groups(self):
        """
        node_groups: (list): A list of node groups we want to operate on.
        Contributes to the definition of self.nodes.
        """
        return self.boot_set_data.get('node_groups', [])

    @property
    def node_roles_groups(self):
        """
        A list of SMD hardware types to operate on.
        Contributes to the definition of self.nodes.
        """
        return self.boot_set_data.get('node_roles_groups', [])

    @property
    def path(self):
        """
        path parameter (string): ID of the image we want to boot the nodes
        with. Corresponds to an IMS Image.
        """
        return self.boot_set_data.get('path', None)

    @property
    def path_type(self):
        """
        Mime type (string) identifying the path
        """
        return self.boot_set_data.get('type', None)

    @property
    def etag(self):
        """
        etag parameter (string: 'Entity tag' for the path
        """
        return self.boot_set_data.get('etag', None)

    @property
    def session_template_kernel_parameters(self):
        """
        session_template_kernel_parameters (str): The kernel boot parameters
                                                  from the BOS Session Template
        """
        return self.boot_set_data.get('kernel_parameters', None)

    @property
    def rootfs_provider(self):
        """
        The name of the root filesystem provisioning mechanism for the
        specified path. This value informs the root=kernel boot parameter
        used by the image's initrd during boot operations.
        """
        return self.boot_set_data.get('rootfs_provider', None)

    @property
    def rootfs_provider_passthrough(self):
        """
        rootfs_provider_passthrough (str): A string containing additional
        provisioning options to append to the proc cmdline rootfs field.
        """
        return self.boot_set_data.get('rootfs_provider_passthrough', None)

    def __repr__(self):
        return "BOA Agent (Session %s Boot Set: %s)" % (self.session_id, self.boot_set)

    @call_logger
    def assemble_kernel_boot_parameters(self):
        '''
        Assemble the kernel boot parameters that we want to set in the
        Boot Script Service (BSS).

        Append the kernel boot parameters together in this order.

        1. Parameters from the image itself.
        2. Parameters from the BOS Session template
        3. rootfs parameters
        4. Node Memory Dump (NMD) parameters

        Warning: We need to ensure that the 'root' parameter exists and is set correctly.
        If any of the parameter locations are empty, they are simply not used.

        TODO: CASMCMS-2590: When we have a better definition on this, this
        function will do something.

        Returns:
            A string containing the needed kernel boot parameters

        Raises:
            ClientError -- An S3 client error
        '''

        boot_param_pieces = []

        # Parameters from the image itself if the parameters exist.
        if (self.artifact_info.get('boot_parameters') is not None and
            self.artifact_info.get('boot_parameters_etag') is not None):
            LOGGER.info("++ _get_s3_download_url %s with etag %s.",
                        self.artifact_info['boot_parameters'],
                        self.artifact_info['boot_parameters_etag'])

            try:
                s3_obj = S3Object(self.artifact_info['boot_parameters'],
                                  self.artifact_info['boot_parameters_etag'])
                image_kernel_parameters_object = s3_obj.object

                image_kernel_parameters_raw = image_kernel_parameters_object['Body'].read().decode('utf-8')
                image_kernel_parameters = image_kernel_parameters_raw.split()
                if image_kernel_parameters:
                    boot_param_pieces.extend(image_kernel_parameters)
            except ClientError as error:
                LOGGER.error("Unable to read file {}. Thus, no kernel boot parameters obtained "
                             "from image".format(self.artifact_info['boot_parameters']))
                LOGGER.debug(error)
                pass

        # Parameters from the BOS Session template if the parameters exist.
        if self.session_template_kernel_parameters:
            boot_param_pieces.append(self.session_template_kernel_parameters)

        # Append special parameters for the rootfs and Node Memory Dump
        pf = ProviderFactory(self)
        provider = pf()
        rootfs_parameters = str(provider)
        if rootfs_parameters:
            boot_param_pieces.append(rootfs_parameters)
        nmd_parameters = provider.nmd_field
        if nmd_parameters:
            boot_param_pieces.append(nmd_parameters)

        # Add the Session ID to the kernel parameters
        boot_param_pieces.append("bos_session_id={}".format(self.session_id))

        return ' '.join(boot_param_pieces)

    def do_stage(self, status_val, func, *arg, **kwargs):
        """
        Args:
          status_val (str): Name of the stage we are running; for logging purposes only
          func (func): Name of the function to invoke
          arg: Array of positional arguments to pass to 'func'
          kwargs: Dictionary of arguments to pass to 'func'
        """
        LOGGER.info('%s_start' % (status_val))
        response = func(*arg, **kwargs)
        LOGGER.info('%s_finished' % (status_val))
        return response

    @property
    def nodes(self):
        """
        Returns
          A set of nodes
        """
        if not hasattr(self, '_nodes'):
            self._nodes = set()
            # Populate from nodelist
            for node_name in self.node_list:
                self._nodes.add(node_name)
            # Populate from nodegroups
            for group_name in self.node_groups:
                if group_name not in self.inventory.groups:
                    LOGGER.warning("No hardware matching label {}".format(group_name))
                    continue
                self._nodes |= self.inventory.groups[group_name]
            # Populate from node_roles_groups
            for role_name in self.node_roles_groups:
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
        return self._nodes - self.failed_nodes

    def _apply_limit(self):
        if not self.session_limit:
            # No limit is defined, so all nodes are allowed
            return self._nodes
        LOGGER.info('Applying limit to session: {}'.format(self.session_limit))
        limit_node_set = set()
        for limit in self.session_limit.split(','):
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
        return self._nodes

    @property
    def artifact_info(self):
        """
        Hunt down the object that contains information about all of the boot artifacts
        Populate the needed information about paths and etags.

        Returns:
          A dictionary containing paths to each of the boot artifacts; The artifact names are the
          keys and the paths are the values
          Example:
          artifact_info['kernel'] = 's3://bucket/key'
        """
        # Use cached value if previously discovered
        if self._boot_artifacts:
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
            boot_artifacts['boot_parameters'] = bimd.boot_parameters_path
            boot_artifacts['boot_parameters_etag'] = bimd.boot_parameters_etag
            self._boot_artifacts = boot_artifacts
            return self._boot_artifacts
        except (ValueError, ArtifactMissing, TooManyArtifacts) as err:
            LOGGER.error("Obtaining boot artifacts failed: %s", err)
            raise

    @property
    def cfs_client(self):
        if self._cfs_client:
            return self._cfs_client
        self._cfs_client = CfsClient()
        return self._cfs_client

    @property
    def capmc_client(self):
        if self._capmc_client:
            return self._capmc_client
        self._capmc_client = requests_retry_session()
        return self._capmc_client

    @property
    def smd_client(self):
        if self._smd_client:
            return self._smd_client
        self._smd_client = requests_retry_session()
        return self._smd_client

    @property
    def session_status(self):
        """
        A record handler to the session status object associated with this boot set
        """
        if self._session_status:
            return self._session_status
        self._session_status = SessionStatus.byref(self.session_id)
        return self._session_status

    @property
    def status_fields(self):
        """
        The status fields unique to this bootset.
        """
        return self.STATUS_FIELDS[self.operation]

    @property
    def boot_set_status(self):
        """
        A record handler specific to an individual bootset, by which this agent
        is directly responsible for reporting node phase changes to.
        """
        if self._boot_set_status:
            return self._boot_set_status
        # Boot Set Statuses need to be re-entrant safe; that is,
        # we assume any existing records with our same name have been
        # safely (and sanely) created before us. We need to resume
        # using these.
        self._boot_set_status = BootSetStatus.CreateOrReference(self.session_status, self.boot_set, self.status_fields,
                                                                self.nodes)
        # Re-entrant protection: when phases are already created as a result of a previous BOA operation,
        # all nodes need to move from their existing category to the 'not_started' category.
        for phase in self._boot_set_status:
            phase.move_to_not_started(self.nodes)
        return self._boot_set_status

    @property
    def inventory(self):
        """
        An SMD Inventory for our current partition.
        """
        if self._inventory:
            return self._inventory
        self._inventory = SMDInventory(self.partition)
        return self._inventory

    @property
    def preflight_check(self):
        if self._preflight_check:
            return self._preflight_check
        self._preflight_check = PreflightCheck(self, self.operation, rootfs_provider=self.rootfs_provider)
        return self._preflight_check

    def __call__(self, queue=None):
        """
        Instruct the boot set agent to run. Under normal execution scenarios,
        preflight checks are issued and then a series of staging operations are executed, as
        defined by the operation. Each operation has one or more phases associated with it.

        BOA executes multiple agents concurrently, one per boot set defined
        in the session template. When operating in this mode, any exception or error that is
        encountered is appended to a Queue object for later upstream processing.
        """
        _ = self.preflight_check()

        def failed_node_error():
            if not self.failed_nodes:
                # Nothing to see here. Move along.
                return
            LOGGER.error("These nodes failed to {}. {}".format(self.operation, self.failed_nodes))
            LOGGER.error("You can attempt to {0} these nodes by issuing the command:\n"
                         "cray bos v1 session create --template-name {1} --operation {0} --limit {2}".format(
                         self.operation, self.session_template_id, ','.join(self.failed_nodes)))

        with self.boot_set_status:
            # Initialize each phase unconditionally as not_started
            if not self.nodes:
                LOGGER.info("No remaining nodes available for operation '%s'.", self.operation)
                return
            for phase_operation in self.phase_operations:
                try:
                    phase_operation()
                except Exception:
                    # Any exceptions that happened as a result of calling this agent
                    # should be aggregated onto the queue to be later unpacked by the
                    # calling function.
                    if queue:
                        queue.put(sys.exc_info())
                    LOGGER.error(traceback.format_exc())

                    # Log failed nodes, so an admin can re-run them.
                    failed_node_error()

                    return

        # Log failed nodes, so an admin can re-run them.
        failed_node_error()
        LOGGER.info('%r finished.', self)

    @property
    def phases(self):
        """
        Return the phases based on the operation in question.
        """
        return self.PHASES[self.operation]

    @property
    def phase_operations(self):
        """
        Every requested operation corresponds to a set of functions that must be
        called in order before the BootSetAgent is considered finished. This property
        is a list of functions unique to the requested operation.

        example:
            reboot operation corresponds to functions:
                self.stage_configuration
                self.shutdown
                self.boot
                self.wait_for_configuration

        These functions are expected to be executed in that order during the __call__ routine.
        Implemented phases may make contextual decisions about how to operate given the phase,
        as well as information stored within the BootSetAgent.
        """
        return [getattr(self, phase) for phase in self.phases]

    # Below defined are functions that are referenced by self.phase_operations;
    # they should not reference or chain call each other, because the order and
    # chaining of behavior is already defined and honored within __call__.

    def stage_configuration(self):
        """
        Sets values unique to this particular boot set in CFS; updates the 'configure' phase.
        """
        if self.cfs_enabled:
            LOGGER.info("Setting desired CFS configuration for nodes in Session: %s", self.session_id)
            # When we're reconfiguring, we don't want to lock the components;
            # instead, we let CFS immediately start configuring.
            enabled = self.operation in ['configure']
            self.cfs_client.set_configuration(self.nodes, self.cfs_configuration, enabled=enabled,
                                              tags={'bos_session': self.session_id})
            self.boot_set_status['configure'].move_nodes(self.nodes, 'not_started', 'in_progress')
        else:
            LOGGER.info("CFS disabled for %r", self)

    def wait_for_configuration(self):
        """
        Blocks and waits for CFS to finish provisioning nodes.
        """
        if not self.cfs_enabled:
            LOGGER.info("No action required of CFS; continuing...")
            return
        LOGGER.info("Waiting on completion of configuration...")
        wait_for_configuration(self)

    def _handle_environment_variables(self, args_dict):
        """
        Massages the environment variables into a usable form.
        It weeds out empty environment variables and uses the default
        values instead from the args_dict.

        Input:
          args_dict (dict): Key/value where the value is a tuple containing
                            the environment variable and a default value.

        Returns:
          A dictionary containing lower-cased environment variable keys and
          their values. The keys are based on the args_dict input.
        """
        args = {}
        for key, value in args_dict.items():
            environ_val, default_val = value
            if not environ_val or environ_val.strip() == '':
                args[key] = default_val
            else:
                if environ_val.isdigit():
                    args[key] = int(environ_val)
                else:
                    args[key] = environ_val
        # Turn the retry string into a boolean.
        if 'retry' in args:
            args['retry'] = (args['retry'].lower() == 'true')

        return args

    @call_logger
    def boot(self):
        """
        Initializes a boot.

        Raises:
          ServiceNotReady -- If it fails to interact with BSS
          NontransientException -- If nodes were not ready.
        """
        LOGGER.info("%r Booting...", self)
        self.boot_set_status['boot'].move_nodes(self.nodes, 'not_started', 'in_progress')
        try:
            self.do_stage("boot_set_bss_urls", set_bss_urls, self,
                          self.nodes,
                          self.assemble_kernel_boot_parameters(),
                          self.artifact_info)
        except (KeyError, ValueError, requests.exceptions.HTTPError,
                ArtifactMissing, TooManyArtifacts) as err:
            LOGGER.error("Failed interacting with Boot Script Service (BSS)", exc_info=err)
            raise ServiceNotReady(err) from err
        self.boot_set_status.update_metadata("boot", start_time=now_string())

        errors = {}
        # Eliminate nodes that are on.
        status_dict, failed_nodes, errors_stat = status(self.nodes)
        self.failed_nodes |= failed_nodes
        errors.update(errors_stat)
        nodes_on = status_dict['on']
        if nodes_on:
            LOGGER.warn("{} nodes were already ON. They will not be booted. ".format(nodes_on))
        nodes_off = set(self.nodes) - nodes_on

        if not nodes_off:
            LOGGER.warn("No nodes to boot.")
            if errors:
                self.boot_set_status.update_errors('boot', errors=errors)
                # There were no nodes to boot, so we are going to mark the Boot Set as
                # having finished this phase.
                self.boot_set_status.update_metadata("boot", stop_time=now_string())
            return

        failed_nodes, errors_pow = power(nodes_off, "on", reason="Session ID: {}".format(self.session_id))
        self.failed_nodes |= failed_nodes
        for new_phase, finished_nodes in zip(['succeeded', 'failed'], [self.nodes, failed_nodes]):
            if finished_nodes:
                self.boot_set_status['boot'].move_nodes(finished_nodes, 'in_progress', new_phase)

        errors.update(errors_pow)
        if errors:
            self.boot_set_status.update_errors('boot', errors=errors)
        if not self.nodes:
            # If every node failed to boot, then stop here. Otherwise, the booted nodes get
            # to soldier on.
            raise NontransientException("Nodes failed to boot.")
        # Wait for the nodes in question to boot
        arg_dict = {'sleep_time': (os.getenv("NODE_STATE_CHECK_SLEEP_INTERVAL"), 5),
                    'allowed_retries': (os.getenv("NODE_STATE_CHECK_NUMBER_OF_RETRIES"), 120)}
        args = self._handle_environment_variables(arg_dict)

        try:
            # Note: wait_for_nodes updates the status of
            wait_for_nodes(boot_set_agent=self,
                           state='Ready',
                           invert=False,
                          # **status based parameters which allow wait_for_nodes to
                          # dynamically alter the status
                           phase="boot",
                           source="in_progress",
                           destination="succeeded",
                           **args)
        except NodesNotReady as err:

            LOGGER.error("Nodes were not ready: %s", err)
            # In this case, the nodes didn't boot within their required window; we want to treat these
            # failures as if they're not recoverable so that K8s does not re-attempt to boot nodes.
            # Otherwise, BOA will start up again and prevent users from detecting and fixing errors.

            # In the future, there would be failure tolerations implemented here that would feed into
            raise NontransientException(err) from err
        finally:
            # We made it past waiting for all of the nodes, so we are going to mark the Boot Set as
            # having finished this phase.
            self.boot_set_status.update_metadata("boot", stop_time=now_string())

    @call_logger
    def shutdown(self):
        """
        Shuts down the nodes in the Agent's node list. If this is part of
        the reboot operation, we additionally wait for nodes to exit from the ready state.

        Raises:
          NontransientException -- when it fails to power down nodes.
        """
        LOGGER.info("Shutting down %r", self)
        self.boot_set_status['shutdown'].move_nodes(self.nodes, 'not_started', 'in_progress')
        arg_dict = {'grace_window': (os.environ.get('GRACEFUL_SHUTDOWN_TIMEOUT'), 300),
                    'hard_window': (os.environ.get('FORCEFUL_SHUTDOWN_TIMEOUT'), 180),
                    'graceful_prewait': (os.environ.get('GRACEFUL_SHUTDOWN_PREWAIT'), 20),
                    'frequency': (os.environ.get('POWER_STATUS_FREQUENCY'), 10)}

        args = self._handle_environment_variables(arg_dict)
        failed_nodes, errors = graceful_shutdown(self.nodes,
                                                 reason="Session ID: {}".format(self.session_id),
                                                 **args)
        completed_nodes = set(self.nodes) - failed_nodes
        self.failed_nodes |= failed_nodes
        for new_category, finished_nodes in zip(['succeeded', 'failed'], [completed_nodes, failed_nodes]):
            if finished_nodes:
                self.boot_set_status['shutdown'].move_nodes(finished_nodes, 'in_progress', new_category)
        if errors:
            self.boot_set_status.update_errors('shutdown',
                                               errors=errors)
            LOGGER.error("Errors occurred while shutting down. Check BOS Status. These nodes failed to "
                         "shutdown: {}".format(failed_nodes))
        if not self.nodes:
            # If every node failed to power down, then stop here. Otherwise, the surviving nodes get
            # to soldier on.
            raise NontransientException("Nodes failed to shutdown")
        if self.operation == 'reboot':
            ready_drain(self.nodes)

