# Copyright 2019-2020 Cray Inc. All Rights Reserved.

'''
Created on Apr 26, 2019

@author: jasons
'''

from cray.boa import PROTOCOL
SERVICE_NAME = 'cray-smd'
ENDPOINT = "%s://%s/hsm/v1/" % (PROTOCOL, SERVICE_NAME)


