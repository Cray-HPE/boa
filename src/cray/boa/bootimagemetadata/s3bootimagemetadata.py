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
Created on February 7th, 2020

@author: jason.sollom
'''

import logging

from botocore.exceptions import ClientError

from . import BootImageMetaData, BootImageMetaDataBadRead
from ..s3client import S3BootArtifacts, S3MissingConfiguration

LOGGER = logging.getLogger(__name__)


class S3BootImageMetaData(BootImageMetaData):

    def __init__(self, agent):
        """
        Create an S3 BootImage by downloading the manifest
        """
        super().__init__(agent)
        self.boot_artifacts = S3BootArtifacts(self._agent.path, self._agent.etag)

    @property
    def metadata(self):
        """
        Get the initial object metadata. This metadata may contain information
        about the other boot objects -- kernel, initrd, rootfs, kernel parameters.

        Raises:
          BootImageMetaDataBadRead -- it cannot read the manifest
        """
        try:
            return self.boot_artifacts.manifest_json
        except (ClientError, S3MissingConfiguration) as error:
            LOGGER.error("Unable to read %s -- Error: %s", self._agent.path, error)
            raise BootImageMetaDataBadRead(error)

    @property
    def kernel(self):
        """
        Get the kernel object
        As an example, the object looks like this
        {'link': {'etag': 'dcaa006fdd460586e62f9ec44e7f61cf',
                               'path': 's3://boot-images/1fb58f4e-ad23-489b-89b7-95868fca7ee6/boot_parameters',
                               'type': 's3'},
                      'md5': 'dcaa006fdd460586e62f9ec44e7f61cf',
                      'type': 'application/vnd.cray.image.parameters.boot'}
        """
        return self.boot_artifacts.kernel

    @property
    def initrd(self):
        """
        Get the initrd object
        As an example, the object looks like this
        {'link': {'etag': 'be2927a765c88558370ee1c5edf1c50c-3',
                      'path': 's3://boot-images/1fb58f4e-ad23-489b-89b7-95868fca7ee6/initrd',
                      'type': 's3'},
             'md5': 'aa69151d7fe8dcb66d74cbc05ef3e7cc',
             'type': 'application/vnd.cray.image.initrd'}
        """
        return self.boot_artifacts.initrd

    @property
    def boot_parameters(self):
        """
        Get the boot parameters object
        As an example, the object looks like this
        {'link': {'etag': 'dcaa006fdd460586e62f9ec44e7f61cf',
                               'path': 's3://boot-images/1fb58f4e-ad23-489b-89b7-95868fca7ee6/boot_parameters',
                               'type': 's3'},
                      'md5': 'dcaa006fdd460586e62f9ec44e7f61cf',
                      'type': 'application/vnd.cray.image.parameters.boot'}
        """
        return self.boot_artifacts.boot_parameters

    @property
    def rootfs(self):
        """
        Get the rootfs object
        As an example, the object looks like this
        {'link': {'etag': 'f04af5f34635ae7c507322985e60c00c-131',
                      'path': 's3://boot-images/1fb58f4e-ad23-489b-89b7-95868fca7ee6/rootfs',
                      'type': 's3'},
             'md5': 'e7d60fdcc8a2617b872a12fcf76f9d53',
             'type': 'application/vnd.cray.image.rootfs.squashfs'}
        """
        return self.boot_artifacts.rootfs

    @property
    def kernel_path(self):
        """
        Get the S3 path to the kernel
        """
        return self.kernel['link']['path']

    @property
    def initrd_path(self):
        """
        Get the S3 path to the initrd
        """
        return self.initrd['link']['path']

    @property
    def rootfs_path(self):
        """
        Get the S3 path to the rootfs
        """
        return self.rootfs['link']['path']

    @property
    def rootfs_etag(self):
        """
        Get the S3 etag to the rootfs
        """
        return self.rootfs['link']['etag']

    @property
    def boot_parameters_path(self):
        """
        Get the S3 path to the boot parameters
        This attribute may not exist.

        Returns:
          The S3 path to the boot parameters file, if it exists
          else None
        """
        bp = self.boot_parameters
        if bp:
            return bp['link']['path']
        return None

    @property
    def boot_parameters_etag(self):
        """
        Get the S3 path to the boot parameter's etag
        This attribute may not exist.
        Returns:
          The S3 path to the boot parameters etag file,
          if it exists else None
        """
        bp = self.boot_parameters
        if bp:
            return bp['link']['etag']
        return None
