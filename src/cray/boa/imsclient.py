# Copyright 2019, Cray Inc. All Rights Reserved.

'''
Created on Apr 26, 2019

@author: jasons
'''

from requests.exceptions import HTTPError
import logging

from . import PROTOCOL
from .logutil import call_logger
from .connection import requests_retry_session

LOGGER = logging.getLogger(__name__)
SERVICE_NAME = 'cray-ims'
ENDPOINT = "%s://%s/" % (PROTOCOL, SERVICE_NAME)

@call_logger
def get_image_artifacts(image_id, session=None):
    '''
    Talk with the Image Management Service (IMS) to get the image artifacts
    associated with the image we want to boot.  This image is identified by
    its IMS image id.
    These image artifacts will contain the Artifact Repository Service (ARS)
    artifact IDs.

    Returns:
        list: List of image artifacts as returned by IMS
    '''
    session = session or requests_retry_session()
    url = "%s/image-artifacts" % (ENDPOINT)
    try:
        resp = session.get(url)
        resp.raise_for_status()
    except HTTPError as err:
        LOGGER.error("Failed listing image artifacts: %s", err)
        raise
    image_artifacts = []
    for artifact in resp.json():
        if image_id == artifact['ims_image_id']:
            image_artifacts.append(artifact)
    return image_artifacts

