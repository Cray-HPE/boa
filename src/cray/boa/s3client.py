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
'''
Created on February 5th, 2020

@author: jasons
'''

import json
import logging
import os

import boto3
from botocore.exceptions import ClientError
from botocore.config import Config as BotoConfig
from urllib.parse import urlparse

from . import TooManyArtifacts, ArtifactMissing, NontransientException

LOGGER = logging.getLogger(__name__)


class S3MissingConfiguration(NontransientException):
    """
    We were missing configuration information need to contact S3.
    """


class S3Url(object):
    """
    https://stackoverflow.com/questions/42641315/s3-urls-get-bucket-name-and-path/42641363
    """

    def __init__(self, url):
        self._parsed = urlparse(url, allow_fragments=False)

    @property
    def bucket(self):
        return self._parsed.netloc

    @property
    def key(self):
        if self._parsed.query:
            return self._parsed.path.lstrip('/') + '?' + self._parsed.query
        else:
            return self._parsed.path.lstrip('/')

    @property
    def url(self):
        return self._parsed.geturl()


def s3_client(connection_timeout=60, read_timeout=60):
    """
    Return an s3 client

    Args:
      connection_timeout -- Number of seconds to wait to time out the connection
                            Default: 60 seconds
      read_timeout -- Number of seconds to wait to time out a read
                            Default: 60 seconds
    Returns:
      Returns an s3 client object
    Raises:
      S3MissingConfiguration -- it cannot contact S3 because it did not have the proper
                                credentials or configuration
    """
    try:
        s3_access_key = os.environ['S3_ACCESS_KEY']
        s3_secret_key = os.environ['S3_SECRET_KEY']
        s3_protocol = os.environ['S3_PROTOCOL']
        s3_gateway = os.environ['S3_GATEWAY']
    except KeyError as error:
        LOGGER.error("Missing needed S3 configuration: %s", error)
        raise S3MissingConfiguration(error) from error

    s3 = boto3.client('s3',
                      endpoint_url=s3_protocol + "://" + s3_gateway,
                      aws_access_key_id=s3_access_key,
                      aws_secret_access_key=s3_secret_key,
                      use_ssl=False,
                      verify=False,
                      config=BotoConfig(
                          connect_timeout=connection_timeout,
                          read_timeout=read_timeout))
    return s3


class S3Object:
    """
    A generic S3 object. It provides a way to download the object.
    """

    def __init__(self, path, etag=None):
        """
        Args:
          path (string): S3 path to the S3 object
          etag (string): S3 entity tag
          """
        self.path = path
        self.etag = etag
        self.s3url = S3Url(self.path)

    @property
    def object_header(self):
        """
        Get the S3 object's header metadata.


        Return:
          The S3 object headers

        Raises:
          ClientError
        """

        try:
            s3 = s3_client()
            s3_obj = s3.head_object(
                        Bucket=self.s3url.bucket,
                        Key=self.s3url.key
                    )
        except ClientError as error:
            LOGGER.error("s3 object %s was not found.", self.path)
            LOGGER.debug(error)
            raise

        if self.etag and self.etag != s3_obj["ETag"].strip('\"'):
            LOGGER.warning("s3 object %s was found, but has an etag '%s' that does "
                               "not match what BOS has '%s'.", self.path, s3_obj["ETag"],
                               self.etag)
        return s3_obj

    @property
    def object(self):
        """
        The S3 object itself.  If the object was not found, log it and return an error.

        Args:
          path -- path to the S3 key
          etag -- Entity tag

        Return:
          S3 Object

        Raises:
          boto3.exceptions.ClientError -- when it cannot read from S3
        """

        s3 = s3_client()

        LOGGER.info("++ _get_s3_download_url %s with etag %s.", self.path, self.etag)
        try:
            return s3.get_object(Bucket=self.s3url.bucket, Key=self.s3url.key)
        except ClientError as error:
            LOGGER.error("Unable to download object {}.".format(self.path))
            LOGGER.debug(error)
            raise


class S3BootArtifacts(S3Object):

    def __init__(self, path, etag=None):
        """
        Args:
          path (string): S3 path to the S3 object
          etag (string): S3 entity tag
          """
        S3Object.__init__(self, path, etag)
        self._manifest_json = None

    @property
    def manifest_json(self):
        """
        Read a manifest.json file from S3. If the object was not found, log it and return an error.

        Args:
          path -- path to the S3 key
          etag -- Entity tag

        Return:
          Manifest file in JSON format

        Raises:
          boto3.exceptions.ClientError -- when it cannot read from S3
        """

        if self._manifest_json:
            return self._manifest_json

        try:
            s3_manifest_obj = self.object
            s3_manifest_data = s3_manifest_obj['Body'].read().decode('utf-8')
        except (ClientError, NoSuchKey) as error:
            LOGGER.error("Unable to read manifest file {}.".format(self.path))
            LOGGER.debug(error)
            raise

        # Cache the manifest.json file
        self._manifest_json = json.loads(s3_manifest_data)
        return self._manifest_json

    def _get_artifact(self, artifact_type):
        """
        Get the artifact_type artifact object out of the manifest.

        The artifact object looks like this
        {
            "link": {
              "path": "s3://boot-artifacts/F6C1CC79-9A5B-42B6-AD3F-E7EFCF22CAE8/rootfs",
              "etag": "foo",
              "type": "s3"
            },
            "type": "application/vnd.cray.image.rootfs.squashfs",
            "md5": "cccccckvnfdikecvecdngnljnnhvdlvbkueckgbkelee"
        }

        Return:
          Artifact object

        Raises:
          ValueError -- Manifest file is corrupt or invalid
          ArtifactMissing -- The requested artifact is missing
          TooManyArtifacts -- There is more than one artifact when only one was expected
        """
        try:
            artifacts = [artifact for artifact in self.manifest_json['artifacts'] if
                                 artifact['type'] == artifact_type]
        except ValueError as value_error:
            LOGGER.info("Received ValueError while processing manifest file.")
            LOGGER.debug(value_error)
            raise
        if not artifacts:
            msg = "No %s artifact could be found in the image manifest." % artifact_type
            LOGGER.info(msg)
            raise ArtifactMissing(msg)
        if len(artifacts) > 1:
            msg = "Multiple %s artifacts found in the manifest." % artifact_type
            LOGGER.info(msg)
            raise TooManyArtifacts(msg)
        return artifacts[0]

    @property
    def initrd(self):
        """
        Get the initrd artifact object out of the manifest.

        Return:
          initrd object
        """
        return self._get_artifact('application/vnd.cray.image.initrd')

    @property
    def kernel(self):
        """
        Get the kernel artifact object out of the manifest.

        Return:
          Kernel object
        """
        return self._get_artifact('application/vnd.cray.image.kernel')

    @property
    def boot_parameters(self):
        """
        Get the kernel artifact object out of the manifest, if one exists.

        Return:
           boot parameters object if one exists, else None
        """
        try:
            bp = self._get_artifact('application/vnd.cray.image.parameters.boot')
        except ArtifactMissing:
            bp = None

        return bp

    @property
    def rootfs(self):
        """
        Get the rootfs artifact object out of the manifest.

        Return:
          rootfs object
        """
        return self._get_artifact('application/vnd.cray.image.rootfs.squashfs')
