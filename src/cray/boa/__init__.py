# Copyright 2019-2020 Hewlett Packard Enterprise Development LP

import os

PROTOCOL = "http"
API_GW_DNSNAME = "api-gw-service-nmn.local"
API_GW = "%s://%s/apis/" % (PROTOCOL, API_GW_DNSNAME)
API_GW_SECURE = "%ss://%s/apis/" % (PROTOCOL, API_GW_DNSNAME)


class BOAException(Exception):
    """
    This is the base exception for all custom exceptions that can be raised from
    this application.
    """


class InvalidInput(BOAException):
    """
    There are invalid inputs to the BOA Agent.
    """


class TransientException(BOAException):
    """
    Transient Exceptions are exceptions that could recover over time as a function
    of services going temporarily offline. The expectation is that any
    Exception that is transient in nature can be re-attempted at a later point
    after required interfaces recover.
    """


class NontransientException(BOAException):
    """
    Nontransient Exceptions are exceptions that are generally expected to fail
    each and every time for a given boot orchestration. During the course of
    excecution, any component that raises a nontransient exception will percolate
    to the top level of the application stack. The application will exit 0, to
    prevent Kubernetes from re-deploying the pod.
    """


class ServiceNotReady(TransientException):
    """
    Raised when a service is not ready for interaction; this is used most
    frequently during preflight checks. For clarification purposes, this
    exception is still viable if a service is responding to requests, but
    has not reached the run or state level necessary to honor the request
    in question.
    """


class ServiceError(NontransientException):
    """
    The service in question responded in a way that indicates the request made
    is not viable and it is not likely that the service will become viable given
    additional time or attempts without operator intervention.
    """

class ArtifactMissing(NontransientException):
    """
    A boot artifact could not be located.
    """

class TooManyArtifacts(NontransientException):
    """
    One and only one artifact was expected to be found. More than one artifact
    was found.
    """

def in_cluster():
    """
    Performs a check to determine if this software is running inside of a cluster.
    """
    return "KUBERNETES_SERVICE_HOST" in os.environ

if in_cluster():
    PROTOCOL = "http"
    VERIFY = False
else:
    PROTOCOL = "https"
    VERIFY = True
