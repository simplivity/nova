# Copyright 2015 SimpliVity Corp.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import paramiko
import socket

from nova.i18n import _LE

from nova.openstack.common import log as logging
from nova.virt.simplivity.libvirt import exception as svt_exception
from nova.openstack.common import processutils

LOG = logging.getLogger(__name__)
CONNECTION_TIMEOUT = 60


class Connection(object):
    """Object to represent connection to virtual controller"""
    def __init__(self, host, username, password, port=22, keyfile=None):
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.keyfile = keyfile  # TODO: Support a key file

        # Establish ssh connection
        self.ssh = self._ssh_connect()

    def _ssh_connect(self):
        """Method to connect to remote system using ssh protocol"""
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self.host,
                        username=self.username,
                        password=self.password,
                        port=self.port,
                        key_filename=self.keyfile,
                        timeout=CONNECTION_TIMEOUT)

            LOG.debug("SSH connection with %s established.", self.host)
            return ssh
        except(paramiko.BadHostKeyException,
               paramiko.AuthenticationException,
               paramiko.SSHException,
               socket.error):
            LOG.exception(_LE('Failed to connect to virtual controller'))
            raise svt_exception.SVTConnectionFailed()

    def get_ssh(self):
        """Method to establish ssh connection"""
        if (self.ssh is None or
                self.ssh.get_transport() is None or
                not self.ssh.get_transport().is_active()):
            LOG.debug("Re-establishing connection to %s.", self.host)
            self.ssh = self._ssh_connect()

        return self.ssh

    def ssh_execute(self, cmd, check_exit_code=True):
        """Method to execute remote command"""
        LOG.debug('Executing remote shell: %s', cmd)
        self.ssh = self.get_ssh()

        stdin_stream, stdout_stream, stderr_stream = self.ssh.exec_command(cmd)
        channel = stdout_stream.channel

        stdout = stdout_stream.read()
        stderr = stderr_stream.read()
        stdin_stream.close()

        exit_status = channel.recv_exit_status()
        if exit_status != -1:
            LOG.debug('Exit status: %s', exit_status)
            if check_exit_code and exit_status != 0:
                raise processutils.ProcessExecutionError(
                    exit_code=exit_status, stdout=stdout, stderr=stderr,
                    cmd=cmd)

        return (stdout, stderr)
