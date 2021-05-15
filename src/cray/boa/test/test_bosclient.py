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

import pytest
from requests import Session
from mock import patch, MagicMock

from cray.boa.bosclient import now_string, SessionStatus, BootSetStatus, PhaseStatus


@pytest.fixture(params=[('session_id', ['bs_one', 'bs_two', 'Computes'])])
def session_status(request):
    ss = SessionStatus.byref(request.param[0])
    ss._client = MagicMock(spec=Session)
    return ss


@pytest.fixture(params=[['session_id', 'Computes', ['boot'], ['node_one', 'node_two']]])
def boot_set_status(request):
    ss = SessionStatus.byref(request.param[0])
    ss._client = MagicMock(spec=Session)
    return BootSetStatus(ss, *request.param[1:])


@pytest.fixture(params=[['session_id', ['Computes'], ['boot'],
                        ['node_one', 'node_two']],
                        ['session_id', ['Computes'], ['configure'],
                        ['node_one', 'node_two']]])
def phase_status(request):
    ss = SessionStatus.byref(request.param[0])
    ss._client = MagicMock(spec=Session)
    bss = BootSetStatus(ss, request.param[1][0], request.param[2],
                        request.param[3])
    ps = PhaseStatus(bss, request.param[2][0])
    return ps


class TestSessionStatus(object):
    """
    Tests aimed at vetting basic SessionStatus routines.
    """

    @patch('cray.boa.bosclient.requests_retry_session',
           MagicMock(spec=Session))
    def test_basic_use(self):
        """
        Creates a SessionStatus object with the __init__ routine, then
        examines some of the defined @properties.
        """
        ss = SessionStatus('session_id', boot_sets=['foo', 'bar', 'baz'])
        assert '%r' % (ss) is not None
        assert isinstance(ss.endpoint, str)
        assert ss.endpoint.startswith('http')
        assert ss.update_metadata(start_time=now_string()) is None


class TestBootSetStatus(object):

    def test_bss_creation(self, session_status):
        _ = BootSetStatus(session_status, 'bs_one',
                          ['boot'], ['node_one', 'node_two'])

    def test_endpoint(self, boot_set_status):
        assert isinstance(boot_set_status.endpoint, str)

    def test_move_nodes(self, boot_set_status):
        assert boot_set_status.move_nodes(['node_one'], 'boot', 'not_started', 'in_progress') is None

    def test_update_metadata(self, boot_set_status):
        assert boot_set_status.update_metadata(start_time=now_string()) is None
        assert boot_set_status.update_metadata(stop_time=now_string()) is None
        assert boot_set_status.update_metadata(phase='boot', stop_time=now_string()) is None

    def test_update_errors(self, boot_set_status):
        assert boot_set_status.update_errors(None, {'Too Beautiful to live': ['node_one'],
                                                    'Too Stubborn to Die':  ['node_two']}) is None
        assert boot_set_status.update_errors('boot', {'Too Beautiful to live': ['node_one'],
                                                      'Too Stubborn to Die':  ['node_two']}) is None

    def test_bss_context(self, boot_set_status):
        with boot_set_status as bss:
            bss.move_nodes(['node_one'], 'boot', 'in_progress', 'succeeded')


class TestPhaseStatus(object):

    def test_generate_phase(self):
        """
        Doesn't do much but ensures the static method can be called
        and returns a dictionary.
        """
        now = now_string()
        value = PhaseStatus.generate_phase('phase_name', ['node_list'], start_time=now)
        assert isinstance(value, dict), "Must return a viable dictionary."

    def test_creation(self, boot_set_status):
        phase = PhaseStatus(boot_set_status, 'boot')
        reprstr = '%r' % (phase)
        assert isinstance(reprstr, str), "String identifier created."

    def test_move_nodes(self, phase_status):
        phase_status.move_nodes('in_progress', 'failed', ['node_one'])

