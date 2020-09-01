# Copyright 2019, Cray Inc. All Rights Reserved.
'''
Created on Apr 26, 2019

@author: jasons
'''

from requests.exceptions import HTTPError
from json import JSONDecodeError
import logging

from . import PROTOCOL
from .logutil import call_logger
from cray.boa.connection import requests_retry_session

LOGGER = logging.getLogger(__name__)
SERVICE_NAME = 'cray-ars'
ENDPOINT = "%s://%s" % (PROTOCOL, SERVICE_NAME)

@call_logger
def get_ars_download_uris(image_artifacts, session=None):
    '''
    Ask the Artifact Repository Service (ARS) for the download URI for each
    image artifact.

    Args:
        image_artifacts(dict): A dictionary containing the image artifacts
    Returns:
        dict: A dict containing the boot artifacts;
              key: artifact type
              value: ARS download URIs
    Raises:
        requests.exceptions.HTTPError -- An HTTP error encountered while
                                         communicating with the
                                         Hardware State Manager
    '''
    session = session or requests_retry_session()
    boot_artifacts = {}
    for artifact in image_artifacts:
        url = "%s/artifacts/%s" % (ENDPOINT, artifact['ars_artifact_id'])
        try:
            #resp = session.get(url).json()
            response = session.get(url)
            response.raise_for_status()
        except HTTPError as hpe:
            LOGGER.error("Unable to get ARS download URI: %s" % (hpe))
            raise
        try:
            resp = response.json()
        except JSONDecodeError as jde:
            LOGGER.error("Non-JSON response from ARS: %s" % (jde))
            raise
        boot_artifacts[artifact['artifact_type']] = resp['uri']
    return boot_artifacts

