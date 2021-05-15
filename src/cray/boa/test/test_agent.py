# Copyright 2020-2021 Hewlett Packard Enterprise Development LP
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
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# (MIT License)

'''
Created on Sep 8, 2020

@author: jsl
'''
import unittest
import tempfile
import logging
import json
import os
from mock import patch, MagicMock

from cray.boa.agent import BootSetAgent

LOGGER = logging.getLogger(__name__)


class TestAgent(unittest.TestCase):

    def setUp(self):
        # Write a file representing a session
        self.file_path = tempfile.NamedTemporaryFile(delete=True).name
        obj = {"boot_sets": {
                "nid1": {
                    "etag": "1ad2687fa9320a7358f117934527c29b",
                    "kernel_parameters": "console=ttyS0,115200 bad_page=panic crashkernel=256M hugepagelist=2m-2g intel_iommu=off intel_pstate=disable iommu=pt ip=dhcp numa_interleave_omit=headless numa_zonelist_order=node oops=panic pageblock_order=14 pcie_ports=native printk.synchronous=y rd.neednet=1 rd.retry=10 rd.shell k8s_gw=api-gw-service-nmn.local quiet turbo_boost_limit=999 biosdevname=0", "name": "nid3", "network": "nmn",
                    "node_list": ["x3000c0s19b3n0"],
                    "path": "s3://boot-images/73ad471b-5cb1-4f55-9a73-c1c145058800/manifest.json",
                    "rootfs_provider": "cpss3",
                    "rootfs_provider_passthrough": "dvs:api-gw-service-nmn.local:300:eth0",
                    "type": "s3"},
                "Computes": {
                    "etag": "1ad2687fa9320a7358f117934527c29b",
                    "kernel_parameters": "console=ttyS0,115200 bad_page=panic crashkernel=256M hugepagelist=2m-2g intel_iommu=off intel_pstate=disable iommu=pt ip=dhcp numa_interleave_omit=headless numa_zonelist_order=node oops=panic pageblock_order=14 pcie_ports=native printk.synchronous=y rd.neednet=1 rd.retry=10 rd.shell k8s_gw=api-gw-service-nmn.local quiet turbo_boost_limit=999 biosdevname=0", "name": "nid3", "network": "nmn",
                    "path": "s3://boot-images/73ad471b-5cb1-4f55-9a73-c1c145058800/manifest.json",
                    "rootfs_provider": "cpss3",
                    "rootfs_provider_passthrough": "dvs:api-gw-service-nmn.local:300:eth0",
                    "type": "s3",
                    "node_roles_groups": ["Computes"]},
                "RandyBitCoinMiner": {
                    "etag": "1ad2687fa9320a7358f117934527c29b",
                    "kernel_parameters": "console=ttyS0,115200 bad_page=panic crashkernel=256M hugepagelist=2m-2g intel_iommu=off intel_pstate=disable iommu=pt ip=dhcp numa_interleave_omit=headless numa_zonelist_order=node oops=panic pageblock_order=14 pcie_ports=native printk.synchronous=y rd.neednet=1 rd.retry=10 rd.shell k8s_gw=api-gw-service-nmn.local quiet turbo_boost_limit=999 biosdevname=0", "name": "nid3", "network": "nmn",
                    "path": "s3://boot-images/73ad471b-5cb1-4f55-9a73-c1c145058800/manifest.json",
                    "rootfs_provider": "cpss3",
                    "rootfs_provider_passthrough": "dvs:api-gw-service-nmn.local:300:eth0",
                    "type": "s3",
                    "node_groups": ["ThisOne", "ThatOne", "TheOtherOne"]},
                },
                "cfs": {"branch": "master", "clone_url": "https://api-gw-service-nmn.local/vcs/cray/config-management.git"},
                "description": "BOS session template for booting compute nodes, generated by the installation",
                "enable_cfs": True,
                "name": "unittest_sessiontemplate_%s" % (self.id)}
        with open(self.file_path, 'w') as template_file:
            template_file.write(json.dumps(obj))

    def tearDown(self):
        os.unlink(self.file_path)

    def test_creation(self):
        agent = BootSetAgent("session_%s" % (self.id), "template_%s" % (self.id),
                             boot_set_name="Computes", operation="reboot", file_path=self.file_path)
        single_agent = BootSetAgent("session_%s" % (self.id), "template_%s" % (self.id),
                             boot_set_name="nid1", operation="reboot", file_path=self.file_path)
        randy_agent = BootSetAgent("session_%s" % (self.id), "template_%s" % (self.id),
                             boot_set_name="RandyBitCoinMiner", operation="configure", file_path=self.file_path)
        # Test that nominal properties resolve to fields from defined bootset.
        self.assertTrue(agent.session_template_uri is not None, "Need a URI for BOS corresponding to this instance.")
        self.assertEqual(agent._session_data, None, "In the beginning, there should be no value here.")
        self.assertTrue(agent.session_data is not None, "We can read a session template from a provided file.")
        self.assertTrue(agent.boot_set_data is not None, "We were able to read session data unique to our bootset.")
        # self.assertEqual(agent.cfs_clone_url, "https://api-gw-service-nmn.local/vcs/cray/config-management.git")
        # self.assertEqual(agent.cfs_branch, "master")
        # self.assertTrue(agent.enable_cfs, "Turned on.")
        self.assertEqual(single_agent.node_list, ['x3000c0s19b3n0'])
        self.assertTrue(len(randy_agent.node_groups), 3)
        self.assertEqual(agent.node_roles_groups, ["Computes"])
        self.assertTrue(agent.path is not None)
        self.assertEqual(agent.path_type, 's3')
        self.assertTrue(agent.etag is not None)
        self.assertEqual(agent.session_template_kernel_parameters, single_agent.session_template_kernel_parameters)
        self.assertEqual(agent.rootfs_provider_passthrough, randy_agent.rootfs_provider_passthrough)
        self.assertTrue('%r' % (agent), "This is callable and resolves to %r" % (agent))

    @patch("cray.boa.agent.CfsClient.create_configuration", MagicMock(return_value='12345'))
    def test_cfs_enabled(self):
        agent = BootSetAgent("session_%s" % (self.id), "template_%s" % (self.id),
                     boot_set_name="Computes", operation="reboot", file_path=self.file_path)
        self.assertTrue(agent.cfs_enabled, "We have all the right fields.")
        self.assertTrue(agent.cfs_configuration == '12345', 'not none, but: %s' % (agent.cfs_configuration))


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
