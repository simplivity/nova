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

"""Volume drivers for libvirt."""

import os
import hashlib

from oslo.config import cfg
from nova.i18n import _

from nova import utils
from nova.openstack.common import log as logging
from nova.openstack.common import processutils
from nova.virt.libvirt import volume as driver

LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class LibvirtSvtVolumeDriver(driver.LibvirtBaseVolumeDriver):
    """
    Class implements libvirt part of volume driver for SimpliVity volumes.
    """

    def __init__(self, connection):
        """Create back-end to nfs"""
        super(LibvirtSvtVolumeDriver, self).__init__(connection,
                                                     is_block_dev=False)

    def connect_volume(self, connection_info, disk_info):
        """Connect the volume. Returns xml for libvirt."""
        LOG.debug('SVT: connect_volume')

        conf = super(LibvirtSvtVolumeDriver,
                     self).connect_volume(connection_info, disk_info)
        options = connection_info['data'].get('options')

        # This will mount under /opt/stack/data/nova/mnt/
        # (default in nova.conf)
        path = self._ensure_mounted(connection_info['data']['export'], options)

        # Find volume svt_container subdirectory
        LOG.debug('SVT: connection_info=%s', str(connection_info))
        svt_container = "_volumes"
        if 'svt_container' in connection_info:
            svt_container = connection_info['svt_container']

        # svtfs_mount/container/volume_uuid
        path = os.path.join(path, svt_container,
                            connection_info['data']['name'])
        LOG.debug('SVT: path=%s', str(path))

        conf.source_type = 'file'
        conf.source_path = path
        conf.driver_format = connection_info['data'].get('format', 'raw')
        return conf

    def _ensure_mounted(self, nfs_export, options=None):
        """Ensure the nfs share is mounted"""
        LOG.debug('SVT: _ensure_mounted')

        # This will mount under /opt/stack/data/nova/mnt/
        # (default in nova.conf)
        mount_path = os.path.join(CONF.simplivity.nfs_mount_point_base,
                                  self.get_hash_str(nfs_export))
        LOG.debug('SVT: mount_path=%s', str(mount_path))

        out, err = utils.execute('mount', '-l', '-t', 'nfs,nfs4',
                                 run_as_root=True)
        LOG.debug('SVT: mount output=%s', str(out))

        # Only mount if it is not already
        if mount_path not in out:
            self._mount_nfs(mount_path, nfs_export, options, ensure=True)

        return mount_path

    def _mount_nfs(self, mount_path, nfs_share, options=None, ensure=False):
        """Mount nfs share to mount path"""
        LOG.debug('SVT: _mount_nfs')

        if not os.path.exists(mount_path):
            utils.execute('mkdir', '-p', mount_path)

        # Construct the NFS mount command
        # mount -t nfs localhost:/mnt/svtfs/0/<guid>
        #              /opt/stack/data/nova/mnt/<uid>
        nfs_cmd = ['mount', '-t', 'nfs']
        if CONF.simplivity.nfs_mount_options is not None:
            nfs_cmd.extend(['-o', CONF.simplivity.nfs_mount_options])
        if options is not None:
            nfs_cmd.extend(options.split(' '))
        nfs_cmd.extend([nfs_share, mount_path])

        # nfs_share = omni.cube.io:/mnt/svtfs/0/<uid>
        # mount_path = /opt/stack/data/nova/mnt/<uid>
        try:
            utils.execute(*nfs_cmd, run_as_root=True)
        except processutils.ProcessExecutionError as exc:
            if ensure and 'already mounted' in exc.message:
                LOG.warn(_("%s is already mounted"), nfs_share)
            elif ensure and 'Connection timed out' in exc.message:
                # SVT: Having problems when the NFS share is already mounted
                # Getting "Exit code: 32/Connection timed out" on
                LOG.warn(_("Connection timed out mounting %s"), nfs_share)
            else:
                raise

    @staticmethod
    def get_hash_str(base_str):
        """Returns string that represents hash of base_str (in hex format)"""
        return hashlib.md5(base_str).hexdigest()
