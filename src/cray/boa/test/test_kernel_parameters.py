# Copyright 2019-2020 Hewlett Packard Enterprise Development LP

import importlib
import pytest

from cray.boa.rootfs.factory import ProviderFactory
from cray.boa.agent import BootSetAgent


class TestKernelParameters(object):

    @pytest.fixture(params=['cpss3'])
    def provider_name(self, request):
        return request.param

    @pytest.fixture(params=['s3://boot-images/73ad471b-5cb1-4f55-9a73-c1c145058800/rootfs'])
    def root_fs_path(self, request):
        return request.param

    @pytest.fixture(params=['Easy-as-123-and-ABC'])
    def root_fs_id(self, request):
        return request.param

    @pytest.fixture
    def agent(self, provider_name, root_fs_path, root_fs_id):
#        ag = BootSetAgent('services', '123', 'cle-1.3.0', '',
#                          'computes', 'x3000c0s19b1n0', '', '', 'boot',
#                          's3://boot-images/73ad471b-5cb1-4f55-9a73-c1c145058800/manifest.json', 's3', '',
#                          'kernel=parameters', 'nmn', provider_name)
        print("Running agent fixture")
        ag = BootSetAgent('123', 'cle-1.3.0', 'computes', 'boot', file_path='/this/does/not/exist')
        ag._session_data = {
            'boot_sets':{
                'computes': {
                    "node_list": ["x3000c0s19b1n0"],
                    "path": "s3://boot-images/73ad471b-5cb1-4f55-9a73-c1c145058800/manifest.json",
                    "type": "s3",
                    "rootfs_provider": provider_name
                    }}}
        ag._boot_artifacts = {}
        ag._boot_artifacts['rootfs'] = root_fs_path
        ag._boot_artifacts['rootfs_etag'] = root_fs_id
        return ag

    def testFactoryOutput(self, agent, provider_name):
        """ 
        Test the output from the ProviderFactory class
        """
 
        pf = ProviderFactory(agent)
        provider_class = pf()
        provider_module = 'cray.boa.rootfs.{}'.format(provider_name)
        provider_classname = '{}Provider'.format(provider_name.upper())
        module = importlib.import_module(provider_module)
        ClassDef = getattr(module, provider_classname)
        assert type(provider_class) == type(ClassDef(agent))

    def testNMDParameter(self, agent, root_fs_path, root_fs_id):
        """
        Test that Node Memory Dump (NMD) parameter is as expected.
        """
        pf = ProviderFactory(agent)
        provider_class = pf()
        nmd_parameter = provider_class.nmd_field
        assert "nmd_data=url={},etag={}".format(root_fs_path, root_fs_id) == nmd_parameter
 
    def testRootFSParameter(self, agent, root_fs_path, root_fs_id, provider_name):
        """
        Test that Rootfs kernel parameter is as expected.
        """
 
        pf = ProviderFactory(agent)
        provider_class = pf()
        root_parameter = str(provider_class)
        assert "root={}".format(":".join([provider_class.PROTOCOL, root_fs_path, root_fs_id])) == root_parameter

