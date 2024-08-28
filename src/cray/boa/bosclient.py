#
# MIT License
#
# (C) Copyright 2020-2022 Hewlett Packard Enterprise Development LP
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
import datetime
import logging
import requests
from functools import wraps

from . import PROTOCOL
from cray.boa.connection import requests_retry_session

LOGGER = logging.getLogger(__name__)
SERVICE_NAME = 'cray-bos'
API_VERSION = 'v1'
SERVICE_ENDPOINT = "%s://%s/%s" % (PROTOCOL, SERVICE_NAME, API_VERSION)
SESSION_ENDPOINT = "%s/session" % (SERVICE_ENDPOINT)

# The current stance is that BOA must be able to continue even during the
# absence of BOS. Unfortunately, this can lead to cascading issues, where
# failure to initially create a record can lead to failure to update that
# record later on. Normally, we would not tolerate these failures...
# As a result, we code these kinds of update failures conditional upon
# a single boolean switch, 'LOSSY'; when its ok to have BOS interactions
# fail, LOSSY is true. When LOSSY is False, we treat BOS like any other API
# and failure to communicate with it results in retries and manual correction.
LOSSY = True


def raise_or_log(func):
    """
    Decorates a function to allow for lossy behavior, during a loss of information,
    the exception is logged.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except BaseBosStatusApiException as bbsae:
            if LOSSY:
                LOGGER.warning(str(bbsae))
            else:
                raise

    return wrapper


class BaseBosStatusApiException(BaseException):
    pass


class BadSession(BaseBosStatusApiException):
    """
    A Session's Status cannot be created.
    """


class StatusAlreadyExists(BaseBosStatusApiException):
    """
    If you attempt to create a Boot Set Status that has already been initialized.
    """


class BadBootStatusUpdate(BaseBosStatusApiException):
    """
    A BootStatus cannot be updated.
    """


class BadBootSetUpdate(BaseBosStatusApiException):
    """
    A Boot Set's Status cannot be updated.
    """


class InvalidPhase(BaseBosStatusApiException):
    """
    Invalid Phase value
    """


def now_string():
    """
    Returns a timestring for the current moment.
    """
    return str(datetime.datetime.now())


class SessionStatus(object):
    """
    A class binding for the creation or introspection of a BOS Session status
    entry.

    The purpose of this class is to hold as little state as possible to better
    handle service restarts, or restarts. As such, most properties that are
    defined in this class read directly from the API endpoint to obtain their
    state. Similarly, changes to properties via  write operation directly
    write state back into the BOS API.
    """
    ENDPOINT = SESSION_ENDPOINT

    @raise_or_log
    def __init__(self, session_id, boot_sets):
        """
        Creates a new BOS Session status entry by initializes appropriate
        records with the BOS API. Every SessionStatus instance must be initialized
        once using this routine for associated properties to exist.

        Failure to create a new SessionStatus instance must be accepted by
        calling routines; failure to initialize a new status results in
        a BaseBosStatusApiException raise.
        """
        self._client = None
        self.session_id = session_id
        if not isinstance(boot_sets, list):
            raise BadSession("Boot sets must be a list.")
        body = {"id": session_id,
                "boot_sets": boot_sets,
                "metadata": {"start_time": now_string()}
                }
        response = self.client.post(self.endpoint, json=body)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            if response.status_code == 409:
                raise StatusAlreadyExists("Status has been previously initialized.") from err
            raise BaseBosStatusApiException(err) from err

    @classmethod
    def byref(cls, session_id):
        """
        Creates a binding to an already initialized SessionStatus object; this
        does not change any existing records via the BOS API, it just references
        an existing SessionStatus instance that was created before.
        """
        self = super().__new__(cls)
        self.session_id = session_id
        self._client = None
        return self

    @classmethod
    def CreateOrReference(cls, session_id, boot_sets):
        """
        Because BOA can restart in order to recover from an outage, a SessionStatus
        record may already exist in BOS. When this happens, we need to resume operation
        and reference the existing record. This classmethod serves as a way to obtain
        a binding to that record, either conditionally creating it, or
        returning a binding to an existing initialized record.
        """
        try:
            return cls(session_id, boot_sets)
        except StatusAlreadyExists:
            return cls.byref(session_id)

    def __repr__(self):
        return "Session Status '%s'" % (self.session_id)

    @property
    def endpoint(self):
        """
        The endpoint that pertains to a given status object.
        """
        return  "{}/{}/status".format(self.ENDPOINT, self.session_id)

    @property
    def client(self):
        """
        Creates an HTTP client that can be used when interacting with BOS during
        creation or updates to metadata.

        Important: Even though the __main__ routine is likely to initialize a
        SessionStatus, individual Agents will have their own record/copy of this
        class instance; each boot set initialized this way (ByRef) will have a unique
        client. This is done on purpose, so that each bootset can operate completely
        independently and asynchronously of their respective counterparts.
        """
        if hasattr(self, '_client') and not self._client:
            # We either don't have one, or its None
            self._client = requests_retry_session()
        return self._client

    @raise_or_log
    def update_metadata(self, start_time=None, stop_time=None):
        """
        Update the metadata for a Session.

          start_time (str): The time the Session started
          stop_time (str): The time the Session stopped
        """

        body = {}
        if start_time:
            body['start_time'] = start_time
        if stop_time:
            body['stop_time'] = stop_time
        if not body:
            # There is nothing to update, so return.
            return
        response = self.client.patch(self.endpoint, json=body)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            raise BadBootStatusUpdate("Failed to update %r: %s" % (self, err)) from err

    def move_nodes(self, components, node_lookup_by_boot_set, phase, source, destination):
        """
        Send status about the configuration of components to BOS.
        This is a helper function to make it easier to report status when the nodes in components
        may be from different Boot Sets. Components may be handled by multiple
        different boot_sets.

        Args:
          components: The IDs of the components we are reporting status about
          node_lookup_by_boot_set: A dictionary mapping Keys: Boot Sets (string, by name) to nodes (a list of string xnames)
          phase (str): The Phase we are reporting status for
          source (str): The source category in the Configuration phase that the nodes are
                        moving out of
          destination (str): The destination category in the Configuration phase that the nodes are
                        moving in to
          Examples:
            Phase: 'configure'
            Source: 'not_started'
            Destination: 'succeeded'

        """
        if not components:
            return
        for boot_set in node_lookup_by_boot_set:
            components_in_bs = (set(components)
                                & set(node_lookup_by_boot_set[boot_set]))
            if not components_in_bs:
                return
            boot_set_status = BootSetStatus.byref(self, boot_set)
            boot_set_status.move_nodes(components, phase, source, destination)

    def update_errors(self, node_lookup_by_boot_set, phase, errors={}):
        """
        Update the errors for a phase within a Boot Set.
        This is a helper function to make it easier to report status when the nodes in errors
          may be from different Boot Sets.

        Args:
          node_lookup_by_boot_set: Keys: Boot Sets; Values: Nodes
          phase (str): the Phase to update, if None the metadata for the Boot Set
                       itself will be updated
          errors (dict): Contains errors (keys) encountered by nodes (list) (values)
          session: A requests connection session
        """
        if not errors:
            return
        for boot_set in node_lookup_by_boot_set:
            components_in_bs = set(node_lookup_by_boot_set[boot_set])
            errors_in_bs = {}
            for error_msg, components in errors.items():
                components_in_error = (set(components) & components_in_bs)
                if components_in_error:
                    errors_in_bs[error_msg] = list(components_in_error)
            if errors_in_bs:
                boot_set_status = BootSetStatus.byref(self, boot_set)
                boot_set_status.update_errors(phase, errors)

    def __enter__(self):
        """
        Initializes a created start time with a record of start.
        """
        ns = now_string()
        self.update_metadata(start_time=ns)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Marks a sessionstatus record as having completed.
        """
        ns = now_string()
        self.update_metadata(stop_time=ns)


class BootSetStatus(object):
    """
    A BootSetStatus is a collection of information about the lifecycle of actions
    that have been applied as a result of a bootset, as a smaller part of individual
    actions within the context of a SessionStatus.

    Like a SessionStatus, very little state is held "in memory" by a BootSetStatus,
    but instead references values stored and retrieved from BOS API.
    """
    # These phase names are whitelisted by the BOS API
    AVAILABLE_PHASES = ["shutdown", "boot", 'configure']

    @raise_or_log
    def __init__(self, session_status, name, phases, node_list):
        """
        Create a Status for the session. Each bootset that is part of the
        session must track a set of nodes through its various phases.
    Args:
      session_status(SessionStatus): An instance of Session Status
      name (str): The name of this bootset within session_status
      phases (list): The list of phases that correspond to this boot set.
      node_list: The list of nodes to be maintained by this boot set.

    Note: It is possible for a BootSetStatus to already exist, if the boot orchestration
    agent has restarted. It is the responsibility of the calling code to address
    this possibility. In those cases, the best way to resume operation is to obtain
    a record binding by using the byref classmethod. The implementation dictates that
    phases are created along side the BootSetStatus, so new phases cannot be added
    after the first record creation attempt.
    """
        self.session_status = session_status
        self.name = name
        self._phases = {}
        start_time = now_string()
        if not isinstance(phases, list):
            raise BadSession("Phases must be a list. Phases were {}".format(type(phases)))
        node_list = list(node_list)
        if not isinstance(node_list, list):
            raise BadSession("Node list must be a list. Node list was {}".format(type(node_list)))
        body = {
            "name": name,
            "session": self.session_status.session_id,
            "metadata": {
                "start_time": start_time,
                },
            "phases": []
            }
        for phase in phases:
            if phase.lower() not in self.AVAILABLE_PHASES:
                raise TypeError("Phase '%s' is unsupported." % (phase))
            body['phases'].append(PhaseStatus.generate_phase(phase, node_list, start_time))
            self._phases[phase] = PhaseStatus(self, phase)
        response = self.client.post(self.endpoint, json=body)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            if response.status_code == 409:
                raise StatusAlreadyExists("Attempted to create a bootsetstatus that already exists") from err
            LOGGER.error("Failed to create the %r: %s", self, err)
            LOGGER.error(response.text)
            raise

    @classmethod
    def byref(cls, session_status, name):
        """
        Creates a binding to an already initialized SessionStatus object; this
        does not change any existing records via the BOS API, it just references
        an existing SessionStatus instance that was created before.
        """
        self = super().__new__(cls)
        self.session_status = session_status
        self.name = name
        return self

    @classmethod
    def CreateOrReference(cls, session_status, name, phases, node_list):
        """
        Because BOA can restart in order to recover from an outage, a BootSetStatus
        record may already exist in BOS. When this happens, we need to resume operation
        and reference the existing BootSet. This classmethod serves as a way to obtain
        a binding to that bootset status object, either conditionally creating it, or
        returning a binding to an existing initialized record.
        """
        try:
            return cls(session_status, name, phases, node_list)
        except StatusAlreadyExists:
            return cls.byref(session_status, name)

    def __repr__(self):
        return "%r Boot Set: %s" % (self.session_status, self.name)

    @property
    def endpoint(self):
        """
        A boot set status endpoint is an extension of the concept of SessionStatus endpoint.
        """
        return "{}/{}".format(self.session_status.endpoint, self.name)

    @property
    def client(self):
        """
        A symbolic reference to the base session status client. No need to create
        additional instances if not necessary.
        """
        return self.session_status.client

    def __getitem__(self, phase_name):
        """
        Represents the defined phases within a BootSetStatus instance; this returns
        Bindings to the previously defined phases within the status, which are actually
        attributes. We expose these individual phases with class bindings so that we can
        more easily and cleanly implement these with python's callback classes. Each phase
        then, should be capable of "cleaning up" and moving nodes to an appropriate category
        within a phase.

        Repeatedly accessing the same PhaseStatus through an instance will yield the previously
        instantiated instance.

        Note: It is possible to "get" a phase that doesn't exist; introspection on such an
        item will fail.
        """
        return self._phases[phase_name]

    def __iter__(self):
        """
        Return an iterator object which iterates through the phases within a boot set
        """
        return iter(list(self._phases.values()))

    @raise_or_log
    def move_nodes(self, components, phase, source, destination):
        """
        For a given phase, move all nodes/components from the <source> category
        to the <destination> category.
        """
        self[phase].move_nodes(components, source, destination)

    @raise_or_log
    def update_metadata(self, phase='boot_set', start_time=None, stop_time=None):
        """
        Update the metadata for a Boot Set or a phase within a Boot Set.

        Args:
          phase (str): the Phase to update, if None, the metadata for the Boot Set
                       itself will be updated
          start_time (str): The time the phase or Boot Set started
          stop_time (str): The time the phase or Boot Set stopped
        """
        if phase:
            available_phases = self.AVAILABLE_PHASES[:]
            available_phases.append('boot_set')
            if phase.lower() not in available_phases:
                raise InvalidPhase("Invalid phase: '{}' not one of {}".format(phase.lower(),
                                                                            available_phases))
        body = {"update_type": "GenericMetadata",
                "phase": phase,
                "data": {}}
        if start_time:
            body['data']['start_time'] = start_time
        if stop_time:
            body['data']['stop_time'] = stop_time
        response = self.client.patch(self.endpoint, json=[body])
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            raise BadBootSetUpdate("Failed to update %r: %s" % (self, err)) from err

    @raise_or_log
    def update_errors(self, phase='boot_set', errors=None):
        """
        Update the errors for a phase within a Boot Set.

        Args:
          phase (str or None): the Phase to update, if 'boot_set' is the phase;
              if None; default to 'boot_set'.
          errors (dict): Contains errors (keys) encountered by nodes (list) (values)
        Example:
          errors = {'Too Beautiful to live': ['x3000c0s19b3n0', 'x3000c0s19b4n0'],
                    'Too Stubborn to Die':  ['x3000c0s19b2n0']}
        """
        if not errors:
            errors = {}
        if not phase:
            phase = 'boot_set'
        available_phases = self.AVAILABLE_PHASES
        available_phases.append('boot_set')
        if phase.lower() not in available_phases:
            raise InvalidPhase("Invalid phase: {} not one of {}".format(phase.lower(),
                                                                        available_phases))
        body = {
            "update_type": "NodeErrorsList",
            "phase": phase,
            "data": errors
                }
        response = self.client.patch(self.endpoint, json=[body])
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            raise BadBootSetUpdate("Failed to update %r: %s" % (self, err)) from err

    def __enter__(self):
        """
        Initializes a created start time with a record of start.
        """
        ns = now_string()
        self.update_metadata(start_time=ns)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Marks a sessionstatus record as having completed.
        """
        ns = now_string()
        self.update_metadata(stop_time=ns)


class PhaseStatus(object):
    """
    Phases are part of a boot set status; they are unique from other status records
    because they are initialized as part of a BootSetStatus as part of the BootSetStatus
    property. They are mutable as properties from a BootSetStatus afterwards though.
    """
    ALL_CATEGORIES = frozenset(['not_started', 'succeeded', 'failed', 'excluded', 'in_progress'])
    ALL_NOT_STARTED = ALL_CATEGORIES - set(['not_started'])

    @staticmethod
    def generate_phase(phase_name, node_list, start_time):
        """
        Generates a phase to add to the request body during the creation of a BootSetStatus.

        Args:
          phase_name (str): Name of the phase
          node_list (list): List of nodes (strings)

        Returns:
          A phase dictionary
        """
        return {
                    "name": phase_name,
                    "metadata": {
                        "start_time": start_time,
                        },
                    "categories": [
                        {
                            "name": "not_started",
                            "node_list": node_list
                         },
                        {
                          "name": "succeeded",
                          "node_list": []
                         },
                        {
                          "name": "failed",
                          "node_list": []
                         },
                        {
                          "name": "excluded",
                          "node_list": []
                         },
                        {
                          "name": "in_progress",
                          "node_list": []
                         }
                     ]
                }

    def __init__(self, boot_set_status, name):
        """
        Initializes a record handler within a BootSetStatus. Invoking this does
        NOT create a new record within the BOS API.
        """
        self.boot_set_status = boot_set_status
        self.name = name.lower()
        if self.name not in self.boot_set_status.AVAILABLE_PHASES:
            raise InvalidPhase("Invalid phase: {} not one of {}".format(self.name,
                                                                        self.boot_set_status.AVAILABLE_PHASES))

    def move_to_not_started(self, nodes):
        """
        Moves all nodes to the category 'not_started' regardless of where they
        currently are.

        This is useful for initializing, or re-initializing individual phases for
        BOA re-entry behavior.
        """
        for source_category in self.ALL_NOT_STARTED:
            self.move_nodes(nodes, source_category, 'not_started')

    def __repr__(self):
        return "%r Phase: %s" % (self.boot_set_status, self.name)

    @property
    def client(self):
        """
        Since we're really operating on an attribute of a boot_set_status, just use
        our parent client.
        """
        return self.boot_set_status.client

    @raise_or_log
    def move_nodes(self, nodes, source_category, destination_category):
        """
        Update which category a node is in within this phase.

        Args:
          node_list (list or set): iterable of node xnames (strings)
          source_category (str): The source category to take the nodes from
          destination_category (str): The destination category to place the nodes in
        """
        if not isinstance(nodes, (list, set)):
            raise BadBootSetUpdate("Node list must be a list.")
        body = [{
            "update_type": "NodeChangeList",
            "phase": self.name,
            "data": {
                "phase": self.name,
                "source": source_category,
                "destination": destination_category,
                "node_list": list(nodes)
                }
            }]
        response = self.client.patch(self.boot_set_status.endpoint, json=body)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            raise BadBootSetUpdate("Failed to update %r: %s" % (self, err)) from err

    @raise_or_log
    def update_metadata(self, start_time=None, stop_time=None):
        """
        Update the metadata for a Boot Set for <self> phase within a Boot Set.

        Args:
          start_time (str): The time the phase or Boot Set started
          stop_time (str): The time the phase or Boot Set stopped
        """
        body = {"update_type": "GenericMetadata",
                "phase": self.name,
                "data": {}}
        if start_time:
            body['data']['start_time'] = start_time
        if stop_time:
            body['data']['stop_time'] = stop_time
        response = self.client.patch(self.boot_set_status.endpoint, json=[body])
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            raise BadBootSetUpdate("Failed to update %r: %s" % (self, err)) from err

