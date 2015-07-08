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

from nova.i18n import _LE, _LW
from nova.openstack.common import log as logging
from nova.openstack.common import processutils
from nova.virt.simplivity.vmwareapi import exception as svt_exception

LOG = logging.getLogger(__name__)
CONNECTION_TIMEOUT = 600  # 10 minute timeout


class SvtConnection(object):
    """Object to represent session to virtual controller."""
    def __init__(self, host, username, password, port=22, keyfile=None,
                 vmware_username=None, vmware_password=None):
        # Virtual controller credentials
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.keyfile = keyfile  # TODO(thangp): Support a key file

        # vCenter credentials
        self.vmware_username = vmware_username
        self.vmware_password = vmware_password

        # Establish ssh connection
        self.ssh = self._ssh_connect()

    def _ssh_connect(self):
        """Method to connect to remote system using ssh protocol."""
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self.host,
                        username=self.username,
                        password=self.password,
                        port=self.port,
                        key_filename=self.keyfile,
                        timeout=CONNECTION_TIMEOUT)

            LOG.debug("svt: SSH connection with %s established." % self.host)
            return ssh
        except(paramiko.BadHostKeyException,
               paramiko.AuthenticationException,
               paramiko.SSHException,
               socket.error):
            LOG.exception(_LE('Failed to connect to virtual controller'))
            raise svt_exception.SvtConnectionFailed()

    def get_ssh(self):
        """Method to establish ssh connection."""
        if (self.ssh is None or
                self.ssh.get_transport() is None or
                not self.ssh.get_transport().is_active()):
            LOG.debug("svt: Re-establishing connection to %s" % self.host)
            self.ssh = self._ssh_connect()

        return self.ssh

    def ssh_execute(self, cmd, check_exit_code=True):
        """Method to execute remote command."""
        LOG.debug("svt: Executing remote shell - %s", cmd)
        self.ssh = self.get_ssh()

        stdin_stream, stdout_stream, stderr_stream = self.ssh.exec_command(cmd)
        channel = stdout_stream.channel

        stdout = stdout_stream.read()
        stderr = stderr_stream.read()
        stdin_stream.close()

        exit_status = channel.recv_exit_status()
        if exit_status != -1:
            LOG.debug("svt: Exit status - %s", exit_status)
            if check_exit_code and exit_status != 0:
                raise processutils.ProcessExecutionError(
                        exit_code=exit_status, stdout=stdout, stderr=stderr,
                        cmd=cmd)

        return (stdout, stderr)


class SvtOperations(object):
    """Object to represent actions performed by the virtual controller."""
    def __init__(self, connection):
        self.connection = connection

    def _svt_session_cmd(self):
        """Returns the command to start a session to vCenter."""
        cmd = ("source /var/tmp/build/bin/appsetup; "
               "svt-session-start --username %(username)s "
               "--password %(password)s &> /dev/null" %
               {"username": self.connection.vmware_username,
                "password": self.connection.vmware_password})
        return cmd

    def backup_delete(self, instance_id, datastore_name, backup_name=None):
        """Delete a previously saved backup."""
        if backup_name:
            delete_cmd = ("svt-backup-delete --datastore %s --vm %s "
                          "--backup %s --force" % (datastore_name,
                                                   instance_id, backup_name))
        else:
            delete_cmd = ("svt-backup-delete --datastore %s --vm %s "
                          "--force" % (datastore_name, instance_id))
        cmd = self._svt_session_cmd() + "; " + delete_cmd

        try:
            stdout, stderr = self.connection.ssh_execute(cmd)
        except processutils.ProcessExecutionError as e:
            # Return code 60 means no backups exists of the given name or none
            # exists. Return code 10 means no VM found in datastore.
            if e.exit_code == 10 or e.exit_code == 11 or e.exit_code == 60:
                LOG.warn(_LW("No backup exists to be deleted"))
            else:
                LOG.error(_LE("Deleting backup failed with error - "
                              "%(stderr)s and return code - %(exit_code)s"),
                          {'stderr': e.stderr, 'exit_code': e.exit_code})
                raise svt_exception.SvtBackupDeleteFailed()

    def backup_restore(self, instance_id, datastore_name, backup_instance_id,
                       backup_name, src_datacenter=None,
                       dest_datacenter=None, dest_datastore_name=None):
        """Restore an instance from a SimpliVity backup."""
        if (src_datacenter is not None and dest_datacenter is not None and
            dest_datastore_name is not None):
            restore_cmd = ("svt-backup-restore --datastore %s --vm %s "
                           "--source %s --destination %s --home %s "
                           "--backup %s --name %s" % (datastore_name,
                           backup_instance_id, src_datacenter,
                           dest_datacenter, dest_datastore_name,
                           backup_name, instance_id))
        else:
            restore_cmd = ("svt-backup-restore --datastore %s --vm %s "
                           "--backup %s --name %s" % (datastore_name,
                           backup_instance_id, backup_name, instance_id))

        cmd = self._svt_session_cmd() + "; " + restore_cmd

        try:
            stdout, stderr = self.connection.ssh_execute(cmd)
        except processutils.ProcessExecutionError as e:
            LOG.error(_LE("Restoring instance failed with error: %s") %
                      e.stderr)
            raise svt_exception.SvtRestoreFailed()

    def vm_backup(self, datastore_name, instance_id, backup_name):
        """Save the state of a VM at a point in time."""
        backup_cmd = ("svt-vm-backup --datastore %s --vm %s --name %s"
                      % (datastore_name, instance_id, backup_name))
        cmd = self._svt_session_cmd() + "; " + backup_cmd

        try:
            stdout, stderr = self.connection.ssh_execute(cmd)
        except processutils.ProcessExecutionError as e:
            LOG.error(_LE("Backing up instance failed with error: %s") %
                      e.stderr)
            raise svt_exception.SvtBackupFailed()

    def vm_restore(self, datastore_name, instance_id, backup_name):
        """Restore a VM to a backup."""
        backup_cmd = ("svt-vm-restore --datastore %s --vm %s --backup %s "
                      "--force" % (datastore_name, instance_id, backup_name))
        cmd = self._svt_session_cmd() + "; " + backup_cmd

        try:
            stdout, stderr = self.connection.ssh_execute(cmd)
        except processutils.ProcessExecutionError as e:
            LOG.error(_LE("Restoring instance to backup failed with "
                          "error: %s") % e.stderr)
            raise svt_exception.SvtRestoreFailed()
