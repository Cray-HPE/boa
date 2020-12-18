# Copyright 2020, Cray Inc. All Rights Reserved.

'''
Created on February 7th, 2020

@author: jason.sollom
'''


class BootImageMetaData(object):
    def __init__(self, agent):
        """
        Base class for BootImage Metadata object
        """
        self._agent = agent
    
    @property
    def metadata(self):
        """
        Get the initial object metadata. This metadata may contain information
        about the other boot objects -- kernel, initrd, rootfs, kernel parameters.
        """
        return None
    
    @property
    def kernel(self):
        """ 
        Get the kernel
        """
        return None

    @property
    def initrd(self):
        """ 
        Get the initrd
        """
        return None

    @property
    def boot_parameters(self):
        """ 
        Get the boot parameters
        """
        return None

    @property
    def rootfs(self):
        """ 
        Get the kernel
        """
        return None

class BootImageMetaDataBadRead(Exception):
    """
    The metadata for the boot image could not be read/retrieved.
    """
    pass

