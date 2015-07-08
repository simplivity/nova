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

import os
import hashlib
import functools
import json

from lxml import etree
from oslo_config import cfg

from nova import block_device
from nova import conductor
from nova import exception
from nova.i18n import _
from nova.i18n import _LE
from nova.i18n import _LI
from nova.i18n import _LW
from nova.image import glance
from nova.api.metadata import base as instance_metadata
from nova.openstack.common import excutils
from nova.openstack.common import importutils
from nova.openstack.common import log as logging
from nova.openstack.common import loopingcall
from nova.openstack.common import processutils
from nova.openstack.common import fileutils
from nova.openstack.common import jsonutils
from nova.openstack.common import units
from nova.compute import power_state
from nova.compute import task_states
from nova.compute import flavors
from nova.compute import utils as compute_utils
from nova import paths
from nova.pci import pci_manager
from nova import utils
from nova.virt import configdrive
from nova.virt import driver
from nova.virt.disk import api as disk
from nova.virt import images
from nova.virt.libvirt import blockinfo
from nova.virt.libvirt import config as vconfig
from nova.virt.libvirt import driver as libvirt_driver
from nova.virt.libvirt import imagecache
from nova.virt.libvirt import utils as libvirt_utils
from nova.virt.simplivity.libvirt import imagebackend as svt_imagebackend
from nova.virt.simplivity.libvirt import utils as svt_utils
from nova.virt.simplivity.vmwareapi import virtual_controller as vc

libvirt = None

LOG = logging.getLogger(__name__)

# Taken from libvirt.driver
VIR_DOMAIN_NOSTATE = 0
VIR_DOMAIN_RUNNING = 1
VIR_DOMAIN_BLOCKED = 2
VIR_DOMAIN_PAUSED = 3
VIR_DOMAIN_SHUTDOWN = 4
VIR_DOMAIN_SHUTOFF = 5
VIR_DOMAIN_CRASHED = 6
VIR_DOMAIN_PMSUSPENDED = 7
LIBVIRT_POWER_STATE = {
    VIR_DOMAIN_NOSTATE: power_state.NOSTATE,
    VIR_DOMAIN_RUNNING: power_state.RUNNING,
    VIR_DOMAIN_BLOCKED: power_state.RUNNING,
    VIR_DOMAIN_PAUSED: power_state.PAUSED,
    VIR_DOMAIN_SHUTDOWN: power_state.SHUTDOWN,
    VIR_DOMAIN_SHUTOFF: power_state.SHUTDOWN,
    VIR_DOMAIN_CRASHED: power_state.CRASHED,
    VIR_DOMAIN_PMSUSPENDED: power_state.SUSPENDED,
}

MIN_LIBVIRT_VERSION = (0, 9, 11)

# When the above version matches/exceeds this version
# delete it & corresponding code using it
MIN_LIBVIRT_DEVICE_CALLBACK_VERSION = (1, 1, 1)
# Live snapshot requirements
REQ_HYPERVISOR_LIVESNAPSHOT = "QEMU"
MIN_LIBVIRT_LIVESNAPSHOT_VERSION = (1, 3, 0)
MIN_QEMU_LIVESNAPSHOT_VERSION = (1, 3, 0)
# Block size tuning requirements
MIN_LIBVIRT_BLOCKIO_VERSION = (0, 10, 2)
# BlockJobInfo management requirement
MIN_LIBVIRT_BLOCKJOBINFO_VERSION = (1, 1, 1)
# Relative block commit (feature is detected,
# this version is only used for messaging)
MIN_LIBVIRT_BLOCKCOMMIT_RELATIVE_VERSION = (1, 2, 7)
# libvirt discard feature
MIN_LIBVIRT_DISCARD_VERSION = (1, 0, 6)
MIN_QEMU_DISCARD_VERSION = (1, 6, 0)
REQ_HYPERVISOR_DISCARD = "QEMU"
# libvirt numa topology support
MIN_LIBVIRT_NUMA_TOPOLOGY_VERSION = (1, 0, 4)

# Set defaults for options found under /etc/nova/nova.conf
default_opts = [
    cfg.StrOpt('libvirt_type',
               default='kvm',  # Only KVM supported
               help='Libvirt domain type (valid options are: '
                    'kvm, lxc, qemu, uml, xen)'),
]

svt_opts = [
    cfg.ListOpt('svt_volume_drivers',
                default=['svt=nova.virt.simplivity.libvirt.volume.LibvirtSvtVolumeDriver'],
                help='SimpliVity handler for volumes.'),
    cfg.StrOpt('nfs_mount_point_base',
               default=paths.state_path_def('mnt'),
               help='Compute node mount point for NFS export'),
    cfg.StrOpt('svt_datacenter_name',
               default=None,
               help='Name of SimpliVity datacenter to use for instances'),
    cfg.StrOpt('svt_datastore_name',
               default=None,
               help='Name of SimpliVity datastore to use for instances'),
    cfg.StrOpt('svt_image_store',
               default='_base',
               help='Directory within the datastore where images are stored'),
    cfg.StrOpt('svt_vc_host',
               default='omni.cube.io',
               help='Hostname or ipv4 address of virtual controller'),
    cfg.StrOpt('svt_vc_username',
               default='root',
               help='Username to log into virtual controller'),
    cfg.StrOpt('svt_vc_password',
               default=None,
               help=('Password associated with the username to log into '
                     'virtual controller')),
    cfg.StrOpt('svt_shares_config',
               default='/etc/nova/shares.conf',
               help='File with the list of available NFS shares'),
    cfg.FloatOpt('svt_used_ratio',
                 default=0.90,
                 help=('Percent of ACTUAL usage of the underlying datastore '
                       'before no new VMs can be allocated to the '
                       'datastore.')),
    cfg.FloatOpt('svt_oversub_ratio',
                 default=1.0,
                 help=('This will compare the allocated to available space '
                       'on the datastore.  If the ratio exceeds this number, '
                       'the destination will no longer be valid.')),
    cfg.StrOpt('svt_mount_options',
               default='vers=3,noac',
               help=('Mount options passed to the nfs client. See section '
                     'of the nfs man page for details.')),
    ]

vmwareapi_opts = [
    cfg.StrOpt('host_ip',
               help='Hostname or IP address for connection to VMware VC '
                    'host.'),
    cfg.IntOpt('host_port',
               default=443,
               help='Port for connection to VMware VC host.'),
    cfg.StrOpt('host_username',
               help='Username for connection to VMware VC host.'),
    cfg.StrOpt('host_password',
               help='Password for connection to VMware VC host.',
               secret=True),
    cfg.MultiStrOpt('cluster_name',
                    help='Name of a VMware Cluster ComputeResource.'),
    cfg.StrOpt('datastore_regex',
               help='Regex to match the name of a datastore.')
    ]

CONF = cfg.CONF
CONF.register_opts(default_opts)  # Register default options
CONF.register_opts(svt_opts, group='simplivity')  # Register SimpliVity options
CONF.register_opts(vmwareapi_opts, 'vmware')
CONF.import_opt('compute_driver', 'nova.compute.manager')

"""
Setup instructions:

1. Edit /etc/nova/nova.conf:
  [DEFAULT]
  use_cow_images=False
  compute_driver = nova.virt.simplivity.libvirt.SvtDriver

  [simplivity]
  svt_shares_config=/etc/nova/shares.conf
  svt_datastore_name=svtds
  svt_vc_username=root
  svt_vc_password=password

  [libvirt]
  images_type=raw
  # Note that hw_machine_type depends on what is supported under
  # "virsh capabilities"
  hw_machine_type = x86_64=pc-i440fx-1.7

2. Append virtual controller IP and hostname to /etc/hosts
  # echo  "<ip> omni.cube.io" >> /etc/hosts

3. Append datastore(s) to /etc/nova/shares.conf
  # echo omni.cube.io:/mnt/svtfs/0/<guid> >> /etc/nova/shares.conf

4. Restart nova-compute.
"""

VOLUME_CONTAINER = "_volumes"  # Default container for volumes
DEFAULT_CACHE_MODE = "writethrough"


class SvtDriver(libvirt_driver.LibvirtDriver):

    def __init__(self, virtapi, read_only=False):
        super(SvtDriver, self).__init__(virtapi)

        # Use O_DIRECT
        self._disk_cachemode = DEFAULT_CACHE_MODE

        # Conductor API needed to update instance block devices
        self._conductor_api = conductor.API()

        global libvirt
        if libvirt is None:
            libvirt = importutils.import_module('libvirt')

        # Establish connection to virtual controller
        # To be used later to invoke remote shell commands
        self.svt_connection = self._get_vc_connection()

        # Override image backend to use simplivity's version
        self.image_backend = svt_imagebackend.Backend(CONF.use_cow_images)

        # Override the existing volume_drivers, so SimpliVity volume driver
        # can be detected
        self.volume_drivers = driver.driver_dict_from_config(
            CONF.simplivity.svt_volume_drivers, self)

        # Find NFS shares (/etc/nova/shares.conf) and mount to compute node
        self.shares = {}  # key = address. value = mount options
        self._mounted_shares = {}  # key = mount point. value = address.
        try:
            self._check_config()  # Check for config option and file
            self._ensure_shares_mounted()  # Mount shares found in shares.conf
        except Exception as e:
            LOG.warning(_LW('Exception during NFS setup with error: %s'), e)

        # CONF.instances_path used by libvirt.utils.get_instance_path
        # instances_path is where instances are stored on disk

        # Override CONF.instance_path to use one in /etc/cinder/shares.conf
        instances_path = self._find_instances_path()
        if instances_path is not None:
            # If no instances_path is found, use nova default
            # Only one datastore/share is supported at this time
            LOG.debug('svt: Setting instances_path to %s', instances_path)
            CONF.set_override('instances_path', instances_path)

    def _get_vc_connection(self):
        """
        Returns an object representing a connection to the virtual controller.
        """
        return vc.SvtConnection(CONF.simplivity.svt_vc_host,
                                CONF.simplivity.svt_vc_username,
                                CONF.simplivity.svt_vc_password,
                                vmware_username=CONF.vmware.host_username,
                                vmware_password=CONF.vmware.host_password)

    def _check_config(self):
        """
        Setup NFS share for CONF.instances_path, where instances are stored
        on disk.
        """
        LOG.debug('svt: _check_config')

        config = CONF.simplivity.svt_shares_config
        LOG.debug('svt: CONF.svt_shares_config=%s', str(config))

        # Check if the option is in nova.conf and that the svt_shares_config
        # file exists
        if not config:
            msg = (_("No config file specified (%s)") % 'svt_shares_config')
            raise exception.NovaException(msg)
        if not os.path.exists(config):
            msg = (_("Config file at %(config)s doesn't exist") %
                   {'config': config})
            raise exception.NovaException(msg)

    def _ensure_shares_mounted(self):
        """
        Look for remote shares in the flags and try to mount them locally.
        """
        self.shares = {}

        # _mounted_shares: key = mount point. value = address.
        self._mounted_shares = {}
        # Populates self.shares
        self._load_svt_config(CONF.simplivity.svt_shares_config)

        for share in self.shares.keys():  # key = address : value = options
            try:
                mount_path = self._ensure_share_mounted(share)
                self._mounted_shares[mount_path] = share
            except Exception as exc:
                LOG.warning(_LW('Exception during mounting %s'), exc)

        LOG.debug('svt: Available shares %s', str(self._mounted_shares))

    def _ensure_share_mounted(self, nfs_export):
        LOG.debug('svt: _ensure_share_mounted %s', str(nfs_export))

        # This will mount under /opt/stack/data/nova/mnt/
        # (default in nova.conf)
        mount_path = os.path.join(CONF.simplivity.nfs_mount_point_base,
                                  self.get_hash_str(nfs_export))
        LOG.debug('svt: mount_path=%s', str(mount_path))

        # List active mount points on node
        out, err = utils.execute('mount', '-l', '-t', 'nfs,nfs4',
                                 run_as_root=True)
        LOG.debug('svt: mount=%s', str(out))

        # Only mount if it is not already
        if mount_path not in out:
            options = CONF.simplivity.svt_mount_options
            self._mount_nfs(mount_path, nfs_export, options=options,
                            ensure=True)

        return mount_path

    @staticmethod
    def get_hash_str(base_str):
        """Returns string that represents hash of base_str (in hex format)."""
        # Generates a hash that is unique for a given share and always the same
        # hash for that given share
        return hashlib.md5(base_str).hexdigest()

    def _mount_nfs(self, mount_path, nfs_share, options=None, ensure=False):
        """Mount nfs export to mount path"""
        LOG.debug('svt: _mount_nfs')

        utils.execute('mkdir', '-p', mount_path)

        # Construct the NFS mount command
        # mount -t nfs localhost:/mnt/svtfs/0/<uid>
        #   /opt/stack/data/nova/mnt/<uid>
        nfs_cmd = ['mount', '-t', 'nfs']
        if options is not None:
            nfs_cmd.extend(['-o', options])
        nfs_cmd.extend([nfs_share, mount_path])

        # nfs_share = localhost:/mnt/svtfs/0/<uid>
        # mount_path = /opt/stack/data/nova/mnt/<uid>
        try:
            utils.execute(*nfs_cmd, run_as_root=True)
        except processutils.ProcessExecutionError as exc:
            if ensure and 'already mounted' in exc.message:
                LOG.warn(_LW("%s is already mounted"), nfs_share)
            elif ensure and 'Connection timed out' in exc.message:
                # Having problems when the NFS share is already mounted
                # Getting "Exit code: 32/Connection timed out" on
                LOG.warn(_LW("Connection timed out mounting %s"), nfs_share)
            else:
                raise

    def _is_share_eligible(self, nfs_share):
        """
        Verifies NFS share is eligible to host virtual machines.

        First validation step: ratio of actual space (used_space / total_space)
        is less than 'nfs_used_ratio'.

        Second validation step: apparent space allocated (differs from actual
        space used when using sparse files) and compares the apparent available
        space (total_available * nfs_oversub_ratio) to ensure enough space is
        available for the new volume.

        :param nfs_share: nfs share
        """
        LOG.debug('svt: _is_share_eligible')

        # The method is nearly identical to NfsDriver._is_share_eligible.
        # We had to change the following variables: svt_used_ratio &
        #    svt_oversub_ratio
        # We may want to further refine the validation here later
        used_ratio = CONF.simplivity.svt_used_ratio
        oversub_ratio = CONF.simplivity.svt_oversub_ratio

        total_size, total_available, total_allocated = \
            self._get_capacity_info(nfs_share)
        used = (total_size - total_available) / total_size
        if used > used_ratio:
            # NOTE(morganfainberg): We check the used_ratio first since
            # with oversubscription it is possible to not have the actual
            # available space but be within our oversubscription limit
            # therefore allowing this share to still be selected as a valid
            # target.
            LOG.debug('%s is above svt_used_ratio', nfs_share)
            return False
        if total_allocated / total_size >= oversub_ratio:
            LOG.debug('%s reserved space is above svt_oversub_ratio',
                      nfs_share)
            return False
        return True

    def _connect_volume(self, connection_info, disk_info):
        driver_type = connection_info.get('driver_volume_type')
        if driver_type not in self.volume_drivers:
            raise exception.VolumeDriverNotFound(driver_type=driver_type)
        driver = self.volume_drivers[driver_type]
        return driver.connect_volume(connection_info, disk_info)

    def _disconnect_volume(self, connection_info, disk_dev):
        driver_type = connection_info.get('driver_volume_type')
        if driver_type not in self.volume_drivers:
            raise exception.VolumeDriverNotFound(driver_type=driver_type)
        driver = self.volume_drivers[driver_type]
        return driver.disconnect_volume(connection_info, disk_dev)

    def spawn(self, context, instance, image_meta, injected_files,
              admin_password, network_info=None, block_device_info=None):
        if self._is_simplivity_image(image_meta):
            # Spawn instance from SimpliVity backup
            self._spawn_from_backup(context, instance, image_meta,
                                   injected_files, admin_password,
                                   network_info, block_device_info)
        else:
            # Spawn instance from Glance image
            # Usual method for spawning new instances
            self._spawn_from_image(context, instance, image_meta,
                                  injected_files, admin_password,
                                  network_info, block_device_info)

    @staticmethod
    def _is_simplivity_image(image):
        """Check if an image is a SimpliVity generated image"""
        LOG.debug('svt: _is_simplivity_image')

        # Extract the relevant info to construct the new instance
        if (image is not None and
            image.get('properties', {}).get('svt_backup_name') is not None):
            # Must have svt_backup_name property to be called a SimpliVity
            # image
            return True

        return False

    def _spawn_from_image(self, context, instance, image_meta, injected_files,
              admin_password, network_info, block_device_info):
        """Spawn instance from Glance image"""
        LOG.debug('svt: _spawn_from_image')

        disk_info = blockinfo.get_disk_info(CONF.libvirt_type,
                                            instance,
                                            block_device_info,
                                            image_meta)

        LOG.debug('svt: disk_info=%s. block_device_info=%s.' %
                  (disk_info, block_device_info))

        # Find if there are persistent volumes
        block_device_mapping = driver.block_device_info_get_mapping(
            block_device_info)

        # Go through all the volumes in mapping (if any)
        for device in block_device_mapping:
            connection_info = device['connection_info']

            # Look for the volume metadata (svt_container)
            vol_id = connection_info['serial']
            vol = self._volume_api.get(context, vol_id)
            if 'svt_container' in vol['volume_metadata']:
                # Add extra key to handle subdirectory
                connection_info['svt_container'] = \
                    vol['volume_metadata']['svt_container']
            else:
                # Set default container
                connection_info['svt_container'] = VOLUME_CONTAINER

            LOG.debug('svt: connection_info=%s', str(connection_info))

        # Copy over base image and use it as root disk using zero-copy
        self._create_image(context, instance,
                           disk_info['mapping'],
                           network_info=network_info,
                           block_device_info=block_device_info,
                           files=injected_files,
                           admin_pass=admin_password)

        # Create libvirt xml for instance
        xml = self._get_guest_xml(context, instance, network_info,
                                  disk_info, image_meta,
                                  block_device_info=block_device_info,
                                  write_to_disk=True)

        self._create_domain_and_network(context, xml, instance, network_info,
                                        block_device_info, reboot=True,
                                        vifs_already_plugged=True)

        # Register instance with SimpliVity virtual controller
        svt_utils.register_instance(self.svt_connection, instance)
        LOG.debug("Instance is running")

        def _wait_for_boot():
            """Called at an interval until the VM is running."""
            state = self.get_info(instance)['state']

            if state == power_state.RUNNING:
                LOG.info(_LI("Instance spawned successfully."),
                         instance=instance)
                raise loopingcall.LoopingCallDone()

        # Poll libvirt until instance is running
        timer = loopingcall.FixedIntervalLoopingCall(_wait_for_boot)
        timer.start(interval=0.5).wait()

    def _spawn_from_backup(self, context, instance, image_meta, injected_files,
              admin_password, network_info, block_device_info):
        """Spawn instance from SimpliVity backup image"""
        LOG.debug('svt: _spawn_from_backup')

        disk_info = blockinfo.get_disk_info(CONF.libvirt_type,
                                            instance,
                                            block_device_info,
                                            image_meta)

        # Boot from backup image
        # Find volumes associated with backup image (if any)
        restored_volumes = self._create_image_from_backup(context, instance,
            disk_info['mapping'], network_info=network_info,
            block_device_info=block_device_info, files=injected_files,
            admin_pass=admin_password, image_meta=image_meta)

        # Create libvirt xml for instance
        xml = self._get_guest_xml(context, instance, network_info,
                                  disk_info, image_meta,
                                  block_device_info=block_device_info,
                                  write_to_disk=True)

        self._create_domain_and_network(context, xml, instance, network_info,
                                        block_device_info, reboot=True,
                                        vifs_already_plugged=True)

        # Register instance with SimpliVity virtual controller
        svt_utils.register_instance(self.svt_connection, instance)

        # Attach volume (if any) to instance in order of mount points
        # Otherwise, devices might not be mounted in order until after reboot
        for mountpoint in sorted(restored_volumes.iterkeys()):
            mountpoint_path = os.path.join('/dev', mountpoint)
            volume_id = restored_volumes[mountpoint]
            self._attach_restored_volume(context, volume_id,
                                         mountpoint_path, instance)

        LOG.debug("Instance is running")

        def _wait_for_boot():
            """Called at an interval until the VM is running."""
            state = self.get_info(instance)['state']

            if state == power_state.RUNNING:
                LOG.info(_LI("Instance restored successfully."),
                         instance=instance)
                raise loopingcall.LoopingCallDone()

        timer = loopingcall.FixedIntervalLoopingCall(_wait_for_boot)
        timer.start(interval=0.5).wait()

    def _attach_restored_volume(self, context, volume_id, mountpoint,
                                instance):
        """Attach restored volume to instance"""
        # Taken from nova.compute.manager
        context = context.elevated()
        LOG.debug("svt: Attaching volume %s as %s" % (volume_id, mountpoint))

        # Create connection_info dict, since it is needed by _volume_api
        try:
            connector = self.get_volume_connector(instance)
            connection_info = self._volume_api.initialize_connection(context,
                                    volume_id, connector)
        except Exception:
            with excutils.save_and_reraise_exception():
                LOG.exception(_LE("Failed to connect to volume %(volume_id)s "
                                "while attaching at %(mountpoint)s"),
                              {'volume_id': volume_id,
                               'mountpoint': mountpoint},
                              context=context, instance=instance)
                self._volume_api.unreserve_volume(context, volume_id)

        if 'serial' not in connection_info:
            connection_info['serial'] = volume_id

        # Attach volume to instance
        self.attach_volume(context, connection_info, instance, mountpoint)
        self._volume_api.attach(context, volume_id,
                                instance.uuid, mountpoint)
        values = {
            'instance_uuid': instance.uuid,
            'connection_info': jsonutils.dumps(connection_info),
            'device_name': mountpoint,
            'delete_on_termination': True,  # Delete volume on delete
            'virtual_name': None,
            'snapshot_id': None,
            'volume_id': volume_id,
            'volume_size': None,
            'no_device': None}
        self._conductor_api.block_device_mapping_update_or_create(context,
                                                                  values)

    def _get_attached_volumes_from_xml(self, xml_doc):
        """Returns a dict of attached volumes from the XML definition"""
        device_info = vconfig.LibvirtConfigGuest()
        device_info.parse_dom(xml_doc)
        attached_volumes = {}
        for device in device_info.devices:
            if (device.root_name != 'disk'):
                continue

            # Cinder volumes should have both a serial and target device name
            if (device.serial is None or device.target_dev is None):
                continue

            # Append attached volume to dict as device: volume_name
            #     device.serial = volume UUID
            volume_name = os.path.basename(device.source_path)
            attached_volumes.update({device.target_dev: volume_name})

        return attached_volumes

    def _get_attached_volumes_from_instance(self, instance):
        """Get a list of attached volumes for a given instance"""
        # Find the instance domain
        try:
            virt_dom = self._lookup_by_name(instance['name'])
        except exception.InstanceNotFound:
            raise exception.InstanceNotRunning(instance_id=instance.uuid)

        # Find volumes attached to instance (if any) to snapshot
        xml = virt_dom.XMLDesc(0)
        xml_doc = etree.fromstring(xml)
        return self._get_attached_volumes_from_xml(xml_doc)

    def snapshot(self, context, instance, image_id, update_task_state):
        """Create snapshot from a VM instance"""
        LOG.debug('svt: snapshot')

        # Find the instance domain
        try:
            virt_dom = self._lookup_by_name(instance['name'])
        except exception.InstanceNotFound:
            raise exception.InstanceNotRunning(instance_id=instance.uuid)

        # Find volumes attached to instance (if any) to snapshot
        xml = virt_dom.XMLDesc(0)
        xml_doc = etree.fromstring(xml)
        attached_volumes = self._get_attached_volumes_from_xml(xml_doc)
        LOG.debug('svt: attached_volumes=%s', attached_volumes)

        # Info about instance being snapshot
        base_image_ref = instance.image_ref
        base = compute_utils.get_image_metadata(
            context, self._image_api, base_image_ref, instance)
        instance_type = flavors.extract_flavor(instance)
        file_size = instance_type['root_gb'] * units.Gi  # Size in bytes
        snapshot = self._image_api.get(context, image_id)

        LOG.debug('svt: vcpus=%s. memory_mb=%s. root_gb=%s. ephemeral_gb=%s. '
                  'swap=%s.' % (instance_type['vcpus'],
                                instance_type['memory_mb'],
                                instance_type['root_gb'],
                                instance_type['ephemeral_gb'],
                                instance_type['swap']))
        LOG.info(_LI("Instance type %s" % instance_type), instance=instance)

        # We need to add a fake location in order to have Glance accept
        # the image
        metadata = {'is_public': False,
                    'status': 'active',
                    'name': snapshot['name'],
                    "size": file_size,
                    # The location must be legitimate.
                    # Glance will try to make a request to verify it.
                    'location': 'http://localhost',
                    'min_disk': int(instance_type['root_gb']),
                    'min_ram': int(instance_type['memory_mb']),
                    'properties': {
                       'kernel_id': instance.kernel_id,
                       'image_state': 'available',
                       'owner_id': instance.project_id,
                       'ramdisk_id': instance.ramdisk_id,
                       'os_type': instance.os_type,
                       'svt_backup_name': snapshot['name'],
                       'svt_instance_uuid': instance.uuid,
                       'svt_ephemeral_gb': instance_type['ephemeral_gb'],
                       'svt_swap': instance_type['swap'],
                       'svt_attached_volumes': attached_volumes
                       }
                    }

        # Find instance disk format
        disk_path = libvirt_utils.find_disk(virt_dom)
        source_format = libvirt_utils.get_disk_type(disk_path)
        image_format = CONF.libvirt.snapshot_image_format or source_format

        # NOTE(bfilippov): save lvm and rbd as raw
        if image_format == 'lvm' or image_format == 'rbd':
            image_format = 'raw'

        # NOTE(vish): glance forces ami disk format to be ami
        if base.get('disk_format') == 'ami':
            metadata['disk_format'] = 'ami'
        else:
            metadata['disk_format'] = image_format

        metadata['container_format'] = base.get('container_format', 'bare')

        # Get current state of the instance
        state = LIBVIRT_POWER_STATE[virt_dom.info()[0]]

        if (self._has_min_version(MIN_LIBVIRT_LIVESNAPSHOT_VERSION,
                                  MIN_QEMU_LIVESNAPSHOT_VERSION,
                                  REQ_HYPERVISOR_LIVESNAPSHOT)
            and source_format not in ('lvm', 'rbd')
            and not CONF.ephemeral_storage_encryption.enabled):
            live_snapshot = True

            # Abort is an idempotent operation, so make sure any block
            # jobs which may have failed are ended. This operation also
            # confims the running instance, as opposed to the system as a
            # whole, has a new enough version of the hypervisor (bug 1193146).
            try:
                virt_dom.blockJobAbort(disk_path, 0)
            except libvirt.libvirtError as ex:
                error_code = ex.get_error_code()
                if error_code == libvirt.VIR_ERR_CONFIG_UNSUPPORTED:
                    live_snapshot = False
                else:
                    pass
        else:
            live_snapshot = False

        LOG.debug('svt: live_snapshot=%s', live_snapshot)
        # NOTE(rmk): We cannot perform live snapshots when a managedSave
        #            file is present, so we will use the cold/legacy method
        #            for instances which are shutdown.
        if state == power_state.SHUTDOWN:
            live_snapshot = False

        # NOTE(dkang): managedSave does not work for LXC
        if CONF.libvirt_type != 'lxc' and not live_snapshot:
            if state == power_state.RUNNING or state == power_state.PAUSED:
                self._detach_pci_devices(virt_dom,
                    pci_manager.get_instance_pci_devs(instance))
                self._detach_sriov_ports(instance, virt_dom)

                # NOTE(thangp): Save and destroy a running guest domain,
                # so it can be restarted from the same state at a later time
                virt_dom.managedSave(0)

        # Takes a snapshot of the disk
        if live_snapshot:
            LOG.info(_LI("Beginning live snapshot process"), instance=instance)
        else:
            LOG.info(_LI("Beginning cold snapshot process"), instance=instance)

        update_task_state(task_state=task_states.IMAGE_PENDING_UPLOAD)

        # Save the state of the instance
        svt_utils.vm_backup(self.svt_connection, instance.uuid,
                            snapshot['name'])

        # The libvirt driver calls _live_snapshot, which creates temporary
        # mirror of the root disk creates a new image of it. We do not have to
        # go through this process since we do the same via incrementing the
        # reference count.

        new_dom = None
        if CONF.libvirt.virt_type != 'lxc' and not live_snapshot:
            # NOTE(thangp): Restarted instance from the same state
            if state == power_state.RUNNING:
                new_dom = self._create_domain(domain=virt_dom)
            elif state == power_state.PAUSED:
                new_dom = self._create_domain(domain=virt_dom,
                        launch_flags=libvirt.VIR_DOMAIN_START_PAUSED)
            if new_dom is not None:
                self._attach_pci_devices(new_dom,
                    pci_manager.get_instance_pci_devs(instance))
                self._attach_sriov_ports(context, instance, new_dom)

        # Save image (placeholder) in Glance
        update_task_state(task_state=task_states.IMAGE_UPLOADING,
                          expected_state=task_states.IMAGE_PENDING_UPLOAD)
        self._image_api.update(context, image_id, metadata)
        LOG.info(_LI("Snapshot image upload complete"), instance=instance)
        LOG.info(_LI("Image metadata %s" % metadata), instance=instance)

    def attach_volume(self, context, connection_info, instance, mountpoint,
                      disk_bus=None, device_type=None, encryption=None):
        # You can only attach a volume by Horizon or CLI:
        #     $ nova volume-attach <server> <volume> [<device>]
        LOG.debug('svt: attach_volume')

        instance_id = instance.uuid
        instance_name = instance.name
        virt_dom = self._lookup_by_name(instance_name)
        disk_dev = mountpoint.rpartition("/")[2]
        bdm = {
            'device_name': disk_dev,
            'disk_bus': disk_bus,
            'device_type': device_type}

        # Note(cfb): If the volume has a custom block size, check that
        #            that we are using QEMU/KVM and libvirt >= 0.10.2. The
        #            presence of a block size is considered mandatory by
        #            cinder so we fail if we can't honor the request.
        data = {}
        if ('data' in connection_info):
            data = connection_info['data']
        if ('logical_block_size' in data or 'physical_block_size' in data):
            if ((CONF.libvirt.virt_type != "kvm" and
                 CONF.libvirt.virt_type != "qemu")):
                msg = _("Volume sets block size, but the current "
                        "libvirt hypervisor '%s' does not support custom "
                        "block size") % CONF.libvirt.virt_type
                raise exception.InvalidHypervisorType(msg)

            if not self._has_min_version(MIN_LIBVIRT_BLOCKIO_VERSION):
                ver = ".".join([str(x) for x in MIN_LIBVIRT_BLOCKIO_VERSION])
                msg = _("Volume sets block size, but libvirt '%s' or later is "
                        "required.") % ver
                raise exception.Invalid(msg)

        # Query for the volume metadata and update the connection_info
        # svt_container is used by connect_volume to find and attach the volume
        volume_id = connection_info['serial']
        volume_metadata = self._volume_api.get_volume_metadata(context,
                                volume_id)

        svt_container = "_volumes"
        if 'svt_container' in volume_metadata:
            svt_container = volume_metadata['svt_container']

            # Pass in svt_container to connection_info
            connection_info['svt_container'] = svt_container

        # Move volume into instance container
        nfs_share = connection_info['data']['export']
        volume_name = connection_info['data']['name']
        (host_address, share_path) = nfs_share.split(':')

        src_path = os.path.join(share_path, svt_container, volume_name)
        tgt_path = os.path.join(share_path, instance_id, volume_name)
        if src_path != tgt_path:  # Do not copy in place
            svt_utils.move_file(self.svt_connection, src_path, tgt_path)

        # Update volume metadata to point to correct container
        connection_info['svt_container'] = instance_id
        metadata = {'svt_container': instance_id}
        self._volume_api.update_volume_metadata(context, volume_id, metadata)

        # Connect volume to libvirt xml
        disk_info = blockinfo.get_info_from_bdm(CONF.libvirt.virt_type, bdm)
        conf = self._connect_volume(connection_info, disk_info)
        self._set_cache_mode(conf)

        try:
            # NOTE(vish): We can always affect config because our
            #             domains are persistent, but we should only
            #             affect live if the domain is running.
            flags = libvirt.VIR_DOMAIN_AFFECT_CONFIG
            state = LIBVIRT_POWER_STATE[virt_dom.info()[0]]
            if state in (power_state.RUNNING, power_state.PAUSED):
                flags |= libvirt.VIR_DOMAIN_AFFECT_LIVE

            # cache device_path in connection_info -- required by encryptors
            if 'data' in connection_info:
                connection_info['data']['device_path'] = conf.source_path

            if encryption:
                encryptor = self._get_volume_encryptor(connection_info,
                                                       encryption)
                encryptor.attach_volume(context, **encryption)

            virt_dom.attachDeviceFlags(conf.to_xml(), flags)
        except Exception as ex:
            LOG.exception(_('Failed to attach volume at mountpoint: %s'),
                          mountpoint, instance=instance)
            if isinstance(ex, libvirt.libvirtError):
                errcode = ex.get_error_code()
                if errcode == libvirt.VIR_ERR_OPERATION_FAILED:
                    self._disconnect_volume(connection_info, disk_dev)
                    raise exception.DeviceIsBusy(device=disk_dev)

            with excutils.save_and_reraise_exception():
                self._disconnect_volume(connection_info, disk_dev)

    def destroy(self, context, instance, network_info, block_device_info=None,
                destroy_disks=True, migrate_data=None):
        LOG.debug('svt: destroy')

        self._destroy(instance)

        # If destroy_disks = false, no files are deleted and the container
        # will continue to exist
        self.cleanup(context, instance, network_info, block_device_info,
                     destroy_disks, migrate_data)

        # Delete any glance images associated with the instance
        self._delete_instance_backups(context, instance)

    def _delete_instance_backups(self, context, instance):
        """
        Delete any backups associated with an instance. Once an instance is
        deleted, any backups associated with it are automatically removed by
        SimpliVity.
        """
        LOG.debug('svt: _delete_instance_backups')
        if instance.get('image_ref') is None:
            return

        # Use image service to find out more info on image
        (image_service, image_id) = glance.get_remote_image_service(context,
                                            instance.image_ref)

        # Search for image where its properties contain the instance uuid
        filters = {'properties': {'svt_instance_uuid': instance.uuid}}
        images = image_service.detail(context, filters=filters)

        # Delete glance image if it is a SimpliVity image and is associated
        # with the instance being deleted
        for image in images:
            LOG.debug('svt: Deleting SimpliVity backup image: %s', image['id'])
            image_service.delete(context, image['id'])

    def _get_connection_info(self, block_device_info, vol_name):
        # Find if there are persistent volumes
        block_device_mapping = driver.block_device_info_get_mapping(
                                        block_device_info)

        # Go through all the volumes in mapping (if any)
        for device in block_device_mapping:
            connection_info = device['connection_info']

            # Find connection for given volume ID
            if (connection_info.get('data') is not None and
                    vol_name == connection_info['data']['name']):
                return connection_info

        return None

    def _hard_reboot(self, context, instance, network_info,
                     block_device_info=None):
        """
        Reboot a virtual machine, given an instance reference.

        Performs a Libvirt reset (if supported) on the domain.

        If Libvirt reset is unavailable this method actually destroys and
        re-creates the domain to ensure the reboot happens, as the guest
        OS cannot ignore this action.

        If xml is set, it uses the passed in xml in place of the xml from the
        existing domain.
        """
        LOG.debug('svt: _hard_reboot')

        self._destroy(instance)

        # Get the system metadata from the instance
        system_meta = utils.instance_sys_meta(instance)

        # Convert the system metadata to image metadata
        image_meta = utils.get_image_from_system_metadata(system_meta)
        if not image_meta:
            image_ref = instance.get('image_ref')
            image_meta = compute_utils.get_image_metadata(context,
                self._image_api, image_ref, instance)

        block_device_mapping = driver.block_device_info_get_mapping(
            block_device_info)

        # Go through all the volumes in mapping
        for device in block_device_mapping:
            connection_info = device['connection_info']
            vol_id = connection_info['serial']

            # Look for the volume metadata (svt_container)
            vol = self._volume_api.get(context, vol_id)
            if 'svt_container' in vol['volume_metadata']:
                # Add extra key to handle subdirectory
                connection_info['svt_container'] = \
                    vol['volume_metadata']['svt_container']

        disk_info = blockinfo.get_disk_info(CONF.libvirt_type,
                                            instance, block_device_info)
        xml = self._get_guest_xml(context, instance, network_info, disk_info,
                                  image_meta=image_meta,
                                  block_device_info=block_device_info,
                                  write_to_disk=True)

        disk_info_json = self._get_instance_disk_info(instance.name, xml,
                                                      block_device_info)
        instance_dir = svt_utils.get_instance_path(instance)
        self._create_images_and_backing(context, instance, instance_dir,
                                        disk_info_json)

        # Initialize all the necessary networking, block devices and
        # start the instance
        self._create_domain_and_network(context, xml, instance, network_info,
                                        block_device_info, reboot=True,
                                        vifs_already_plugged=True)

        self._prepare_pci_devices_for_use(
            pci_manager.get_instance_pci_devs(instance, 'all'))

        def _wait_for_reboot():
            """Called at an interval until the VM is running again."""
            state = self.get_info(instance)['state']

            if state == power_state.RUNNING:
                LOG.info(_LI("Instance rebooted successfully."),
                         instance=instance)
                raise loopingcall.LoopingCallDone()

        timer = loopingcall.FixedIntervalLoopingCall(_wait_for_reboot)
        timer.start(interval=0.5).wait()

    def get_guest_disk_config(self, instance, name, disk_mapping, inst_type,
                              image_type=None):
        if CONF.libvirt.hw_disk_discard:
            if not self._has_min_version(MIN_LIBVIRT_DISCARD_VERSION,
                                     MIN_QEMU_DISCARD_VERSION,
                                     REQ_HYPERVISOR_DISCARD):
                msg = (_('Volume sets discard option, but libvirt %(libvirt)s'
                         ' or later is required, qemu %(qemu)s'
                         ' or later is required.') %
                      {'libvirt': MIN_LIBVIRT_DISCARD_VERSION,
                       'qemu': MIN_QEMU_DISCARD_VERSION})
                raise exception.Invalid(msg)

        image = self.image_backend.image(instance, name, image_type)
        disk_info = disk_mapping[name]
        return image.libvirt_info(disk_info['bus'],
                                  disk_info['dev'],
                                  disk_info['type'],
                                  "none",  # DIRECT_IO supported by svt
                                  inst_type['extra_specs'],
                                  self.get_hypervisor_version())

    def _find_instances_path(self):
        """
        Find an NFS share to use as the CONF.instances_path, where instances
        are stored on disk.
        """
        if not self._mounted_shares:
            msg = _LW("No NFS shares available")
            LOG.warn(msg)
            raise exception.NovaException(msg)

        target_share = None

        # _mounted_shares: key = mount point. value = address.
        for nfs_share in self._mounted_shares.values():
            total_size, total_available, total_allocated = \
                self._get_capacity_info(nfs_share)

            if self._is_share_eligible(nfs_share):
                target_share = os.path.join(
                    CONF.simplivity.nfs_mount_point_base,
                    self.get_hash_str(nfs_share))
                break

        if target_share is None:
            msg = _LW("No suitable NFS shares found")
            LOG.warn(msg)
            raise exception.NovaException(msg)

        LOG.debug('svt: _find_instances_share found %s', target_share)
        return target_share

    def _get_capacity_info(self, nfs_share):
        """
        Calculate available space on the NFS share.

        :param nfs_share, e.g. 172.18.194.100:/var/nfs
        """
        mount_point = os.path.join(CONF.simplivity.nfs_mount_point_base,
                                   self.get_hash_str(nfs_share))

        total_size = 0
        total_available = 0
        total_allocated = 0
        if not os.path.exists(mount_point):
            return total_size, total_available, total_allocated

        stat, _ = utils.execute('stat', '-f', '-c', '%S %b %a', mount_point,
                              run_as_root=True)
        block_size, blocks_total, blocks_avail = map(float, stat.split())
        total_available = block_size * blocks_avail
        total_size = block_size * blocks_total

        du, _ = utils.execute('du', '-sb', '--apparent-size', '--exclude',
                              '*snapshot*', mount_point, run_as_root=True)
        total_allocated = float(du.split()[0])

        #  Sizes returned in bytes
        return total_size, total_available, total_allocated

    def _load_svt_config(self, share_file):
        """
        Load the svt shares contained in the config file
        """
        self.shares = {}

        for share in self._read_config_file(share_file):
            # A configuration line may be either:
            #  host:/vol_name
            # or
            #  host:/vol_name -o options=123,rw --other
            if not share.strip():
                # Skip blank or whitespace-only lines
                continue
            if share.startswith('#'):
                continue

            share_info = share.split(' ', 1)
            # Results in share_info =
            #    [ 'address:/vol', '-o options=123,rw --other' ]
            share_address = share_info[0].strip().decode('unicode_escape')
            share_opts = share_info[1].strip() if len(share_info) > 1 else None

            self.shares[share_address] = share_opts

        LOG.debug("svt: Shares loaded %s", self.shares)

    def _read_config_file(self, config_file):
        # Returns list of lines in file
        with open(config_file) as f:
            return f.readlines()

    def _create_image(self, context, instance,
                      disk_mapping, suffix='',
                      disk_images=None, network_info=None,
                      block_device_info=None, files=None,
                      admin_pass=None, inject_files=True):
        """Create instance from Glance image"""
        LOG.debug('svt: _create_image')

        booted_from_volume = self._is_booted_from_volume(
            instance, disk_mapping)

        def image(fname, image_type=CONF.libvirt.images_type):
            return self.image_backend.image(instance, fname + suffix,
                                            image_type)

        def raw(fname):
            return image(fname, image_type='raw')

        # Ensure directories exist and are writable
        fileutils.ensure_tree(svt_utils.get_instance_path(instance))
        LOG.info(_('Creating image'), instance=instance)

        # NOTE(dprince): for rescue console.log may already exist... chown it.
        self._chown_console_log_for_instance(instance)

        # NOTE(yaguang): For evacuate disk.config already exist in shared
        # storage, chown it.
        self._chown_disk_config_for_instance(instance)

        # NOTE(vish): No need add the suffix to console.log
        svt_utils.write_to_file(self._get_console_log_path(instance), '', 7)

        # Exported share on virtual controller (i.e. address:share)
        nfs_share = self._mounted_shares[CONF.instances_path]
        if not nfs_share:
            raise exception.SVTShareNotFound()

        if not disk_images:
            disk_images = {'image_id': instance.image_ref,
                           'kernel_id': instance.kernel_id,
                           'ramdisk_id': instance.ramdisk_id}

        if disk_images['kernel_id']:
            fname = imagecache.get_cache_fname(disk_images, 'kernel_id')
            raw('kernel').cache(fetch_func=svt_utils.fetch_image,
                                context=context,
                                filename=fname,
                                image_id=disk_images['kernel_id'],
                                user_id=instance.user_id,
                                project_id=instance.project_id,
                                instance_id=instance.uuid,
                                nfs_share=nfs_share)
            if disk_images['ramdisk_id']:
                fname = imagecache.get_cache_fname(disk_images, 'ramdisk_id')
                raw('ramdisk').cache(fetch_func=svt_utils.fetch_image,
                                     context=context,
                                     filename=fname,
                                     image_id=disk_images['ramdisk_id'],
                                     user_id=instance.user_id,
                                     project_id=instance.project_id,
                                     instance_id=instance.uuid,
                                     nfs_share=nfs_share)

        inst_type = flavors.extract_flavor(instance)

        # NOTE(ndipanov): Even if disk_mapping was passed in, which
        # currently happens only on rescue - we still don't want to
        # create a base image.
        if not booted_from_volume:
            LOG.debug('svt: Not booted from volume')

            # root_fname = SHA1 hash of a image ID
            root_fname = str(disk_images['image_id'])
            size = instance.root_gb * units.Gi

            if size == 0 or suffix == '.rescue':
                size = None

            # Copy the image into the VM container.
            # fetch_func called only if file does not exist in _base.
            # Otherwise, the image is copied over from _base.

            # image('disk').path = Path to VM's root disk
            LOG.debug('svt: image.path=%s', image('disk').path)

            # Use SimpliVity image backend
            backend = image('disk')
            backend.cache(fetch_func=svt_utils.fetch_image,
                          context=context,
                          filename=root_fname,  # Image file name to copy
                          size=size,
                          image_id=disk_images['image_id'],
                          user_id=instance.user_id,
                          project_id=instance.project_id,
                          instance_id=instance.uuid,
                          nfs_share=nfs_share)
            # instance_id & nfs_share: Extra parameters for us to know about
            # the NFS share

        # Lookup the filesystem type if required
        os_type_with_default = disk.get_fs_type_for_os_type(
            instance.os_type)

        LOG.debug('svt: block_device_info=%s. disk_mapping=%s' %
                  (block_device_info, disk_mapping))
        ephemeral_gb = instance.ephemeral_gb
        if 'disk.local' in disk_mapping:
            LOG.debug('svt: Creating disk.local')

            # disk.local an an image. Nova will create a raw image to be used
            # as the ephemeral disk.
            disk_image = image('disk.local')
            fn = functools.partial(self._create_ephemeral,
                                   fs_label='ephemeral0',
                                   os_type=instance.os_type)
            fname = "ephemeral_%s_%s" % (ephemeral_gb, os_type_with_default)
            size = ephemeral_gb * units.Gi
            disk_image.cache(fetch_func=fn,
                             filename=fname,
                             size=size,
                             ephemeral_size=ephemeral_gb)

        for idx, eph in enumerate(driver.block_device_info_get_ephemerals(
                block_device_info)):
            LOG.debug('svt: Creating ephemeral%d', idx)

            disk_image = image(blockinfo.get_eph_disk(idx))

            specified_fs = eph.get('guest_format')
            if specified_fs and not self.is_supported_fs_format(specified_fs):
                msg = _("%s format is not supported") % specified_fs
                raise exception.InvalidBDMFormat(details=msg)

            # Create more ephermal disks as needed, only if ephermals is in
            # block_device_info dict
            fn = functools.partial(self._create_ephemeral,
                                   fs_label='ephemeral%d' % idx,
                                   os_type=instance.os_type,
                                   is_block_dev=disk_image.is_block_dev)
            size = eph['size'] * units.Gi
            fname = "ephemeral_%s_%s" % (eph['size'], os_type_with_default)
            disk_image.cache(fetch_func=fn,
                             context=context,
                             filename=fname,
                             size=size,
                             ephemeral_size=eph['size'],
                             specified_fs=specified_fs)

        if 'disk.swap' in disk_mapping:
            LOG.debug('svt: Creating disk.swap')

            mapping = disk_mapping['disk.swap']
            swap_mb = 0

            swap = driver.block_device_info_get_swap(block_device_info)
            if driver.swap_is_usable(swap):
                swap_mb = swap['swap_size']
            elif (inst_type['swap'] > 0 and
                  not block_device.volume_in_mapping(
                    mapping['dev'], block_device_info)):
                swap_mb = inst_type['swap']

            if swap_mb > 0:
                size = swap_mb * units.Mi
                image('disk.swap').cache(fetch_func=self._create_swap,
                                         context=context,
                                         filename="swap_%s" % swap_mb,
                                         size=size,
                                         swap_mb=swap_mb)

        # Config drive
        if configdrive.required_by(instance):
            LOG.info(_LI('Using config drive'), instance=instance)
            extra_md = {}
            if admin_pass:
                extra_md['admin_pass'] = admin_pass

            inst_md = instance_metadata.InstanceMetadata(instance,
                content=files, extra_md=extra_md, network_info=network_info)
            with configdrive.ConfigDriveBuilder(instance_md=inst_md) as cdb:
                configdrive_path = self._get_disk_config_path(instance, suffix)
                LOG.info(_LI('Creating config drive at %(path)s'),
                         {'path': configdrive_path}, instance=instance)

                try:
                    cdb.make_drive(configdrive_path)
                except processutils.ProcessExecutionError as e:
                    with excutils.save_and_reraise_exception():
                        LOG.error(_LE('Creating config drive failed '
                                      'with error: %s'), e, instance=instance)

        # File injection only if needed
        elif inject_files and CONF.libvirt.inject_partition != -2:
            if booted_from_volume:
                LOG.warn(_LW('File injection into a boot from volume '
                             'instance is not supported'), instance=instance)
            self._inject_data(
                instance, network_info, admin_pass, files, suffix)

        if CONF.libvirt.virt_type == 'uml':
            libvirt_utils.chown(image('disk').path, 'root')

    def _create_image_from_backup(self, context, instance,
                      disk_mapping, suffix='',
                      disk_images=None, network_info=None,
                      block_device_info=None, files=None,
                      admin_pass=None, image_meta=None, inject_files=True):
        """Create an instance from a backup"""
        LOG.debug('svt: _create_image_from_backup')

        # Find info on image
        backup_instance_uuid = None
        backup_name = None
        attached_volumes = None
        backup_ephemeral_gb = 0
        backup_swap_mb = 0
        is_vmware = False
        src_datacenter = None
        src_datastore = None
        if (image_meta and image_meta.get('properties') is not None):
            meta_properties = image_meta.get('properties', {})

            # Is the image VMware based?  We need to determine this in order
            # to properly restore it on KVM.
            if (meta_properties.get('vmware_image_version') is not None and
                meta_properties.get('vmware_adaptertype') is not None):
                is_vmware = True
                src_datacenter = meta_properties.get('svt_datacenter_name')
                src_datastore = meta_properties.get('svt_datastore_name')

            backup_instance_uuid = meta_properties.get('svt_instance_uuid')
            backup_name = meta_properties.get('svt_backup_name')

            min_ram = image_meta.get('min_disk')
            min_disk = image_meta.get('min_ram')

            backup_ephemeral_gb = meta_properties.get('svt_ephemeral_gb', 0)
            backup_swap_mb = meta_properties.get('svt_swap', 0)

            # Find any volumes that were attached
            # attached_volumes = dict where {device: volume_name}
            attached_volumes = str(meta_properties.get('svt_attached_volumes'))

            LOG.debug('svt: backup_instance_uuid=%s. backup_name=%s. '
                      'min_ram=%s. min_disk=%s. attached_volumes=%s.',
                      (backup_instance_uuid, backup_name, min_ram, min_disk,
                       attached_volumes))

        # If we are missing vital info on the backup, quit
        if backup_instance_uuid is None and backup_name is None:
            raise exception.SVTBackupInfoNotFound()

        # Are we booting from a cinder volume?
        booted_from_volume = (
            (not bool(instance.get('image_ref')))
            or 'disk' not in disk_mapping
        )

        if booted_from_volume:
            LOG.error(_LE('Booting a SimpliVity backup from volume is not '
                          'supported'), instance=instance)
            raise exception.SVTOperationNotSupported()

        def image(fname, image_type=CONF.libvirt.images_type):
            return self.image_backend.image(instance, fname + suffix,
                                            image_type)

        def raw(fname):
            return image(fname, image_type='raw')

        # Restore instance from a backup
        if is_vmware:
            LOG.info(_LI('Creating image from a backup (ESX)'),
                     instance=instance)
            dest_datacenter = CONF.simplivity.svt_datacenter_name
            dest_datastore_name = CONF.simplivity.svt_datastore_name
            svt_utils.backup_restore(self.svt_connection,
                                     instance.uuid,
                                     src_datastore,
                                     backup_instance_uuid, backup_name,
                                     src_datacenter=src_datacenter,
                                     dest_datacenter=dest_datacenter,
                                     dest_datastore_name=dest_datastore_name)
        else:
            LOG.info(_LI('Creating image from a backup (KVM)'),
                     instance=instance)
            datastore = CONF.simplivity.svt_datastore_name
            svt_utils.backup_restore(self.svt_connection, instance.uuid,
                                     datastore, backup_instance_uuid,
                                     backup_name)

        def _wait_for_restore():
            container = svt_utils.get_instance_path(instance)

            # Directories under the mount may not appear until you do a list
            os.listdir(CONF.instances_path)
            LOG.debug('svt: Waiting for container %s to be restored',
                      container)

            if os.path.exists(container):
                LOG.info(_LI("Instance restored successfully."),
                         instance=instance)
                raise loopingcall.LoopingCallDone()

        # Wait until the container exists
        timer = loopingcall.FixedIntervalLoopingCall(_wait_for_restore)
        timer.start(interval=1.0).wait()

        # Restore volumes (if any)
        # Keys and values must be enclosed in double quotes or
        # it cannot be decoded
        volumes = json.loads(attached_volumes.replace("'", '"'))
        restored_volumes = {}  # key = device. value = volume uuid.
        if volumes is not None:
            restored_volumes = self._restore_volumes(context, instance,
                                                     volumes, suffix=suffix)

        # Update disk mapping.  Make sure there are no collisions in device
        # names.  See source code in blockinfo.find_disk_dev_for_disk_bus.
        if restored_volumes:
            LOG.debug('svt: Checking for device name collisions')
            LOG.debug('svt: disk_mapping=%s', disk_mapping)

            for device in disk_mapping:
                # dict where keys are bus, type, and dev
                disk_info = disk_mapping[device]
                disk_dev = disk_info.get('dev')
                disk_bus = disk_info.get('bus')

                # If a volume is already mapped to the device name, find an
                # alternate device name
                if disk_dev in restored_volumes:
                    LOG.debug('svt: %s already exists', disk_dev)

                    # Update mapping
                    disk_info['dev'] = self._get_device_name(disk_bus,
                                            disk_mapping, restored_volumes)
                    LOG.debug('svt: Changing device name %s to %s' %
                              (disk_dev, disk_info['dev']))

        # Ensure directories exist and are writable
        fileutils.ensure_tree(svt_utils.get_instance_path(instance))

        # NOTE(dprince): for rescue console.log may already exist... chown it.
        self._chown_console_log_for_instance(instance)

        # NOTE(yaguang): For evacuate disk.config already exist in shared
        # storage, chown it.
        self._chown_disk_config_for_instance(instance)

        # NOTE(vish): No need add the suffix to console.log
        svt_utils.write_to_file(self._get_console_log_path(instance), '', 7)

        if not disk_images:
            disk_images = {'image_id': instance.image_ref,
                           'kernel_id': instance.kernel_id,
                           'ramdisk_id': instance.ramdisk_id}

        if disk_images['kernel_id']:
            fname = imagecache.get_cache_fname(disk_images, 'kernel_id')
            raw('kernel').cache(fetch_func=svt_utils.fetch_image,
                                context=context,
                                filename=fname,
                                image_id=disk_images['kernel_id'],
                                user_id=instance.user_id,
                                project_id=instance.project_id)
            if disk_images['ramdisk_id']:
                fname = imagecache.get_cache_fname(disk_images, 'ramdisk_id')
                raw('ramdisk').cache(fetch_func=svt_utils.fetch_image,
                                     context=context,
                                     filename=fname,
                                     image_id=disk_images['ramdisk_id'],
                                     user_id=instance.user_id,
                                     project_id=instance.project_id)

        inst_type = flavors.extract_flavor(instance)

        # NOTE(ndipanov): Even if disk_mapping was passed in, which
        # currently happens only on rescue - we still don't want to
        # create a base image.
        if not booted_from_volume:
            size = instance.root_gb * units.Gi

            # Exported share on virtual controller (i.e. address:share)
            nfs_share = self._mounted_shares[CONF.instances_path]
            if not nfs_share:
                raise exception.SVTShareNotFound()

            if size == 0 or suffix == '.rescue':
                size = None

            # Resize root disk if necessary
            disk_path = os.path.join(svt_utils.get_instance_path(instance),
                                     'disk')
            if not os.path.exists(disk_path):
                # Root disk needs to be renamed.  Root disk name may not be
                # instance-uuid-flat.vmdk!
                vmdk_disk_path = os.path.join(
                    svt_utils.get_instance_path(instance),
                    backup_instance_uuid + '-flat.vmdk')
                utils.execute('mv', vmdk_disk_path, disk_path)

            self._resize_image(disk_path, size)

        # Lookup the filesystem type if required
        os_type_with_default = instance.os_type
        if not os_type_with_default:
            os_type_with_default = 'default'

        disk_bus = blockinfo.get_disk_bus_for_device_type(CONF.libvirt_type,
                                                          image_meta, "disk")

        # Inject disk.local and disk.swap into disk_mapping if they are in
        # the backup
        LOG.debug('svt: backup_ephemeral_gb=%s. backup_swap_mb=%s.' %
                  (backup_ephemeral_gb, backup_swap_mb))

        if int(backup_ephemeral_gb) > 0 and 'disk.local' not in disk_mapping:
            disk_local_info = blockinfo.get_next_disk_info(disk_mapping,
                                                           disk_bus)
            disk_mapping.update({'disk.local': disk_local_info})

        if int(backup_swap_mb) > 0 and 'disk.swap' not in disk_mapping:
            disk_swap_info = blockinfo.get_next_disk_info(disk_mapping,
                                                          disk_bus)
            disk_mapping.update({'disk.swap': disk_swap_info})

        LOG.debug('svt: block_device_info=%s. disk_mapping=%s.' %
                  (block_device_info, disk_mapping))

        # Use larger of ephermeral disk, backup or flavor
        ephemeral_gb = instance.ephemeral_gb
        if int(backup_ephemeral_gb) > int(instance.ephemeral_gb):
            ephemeral_gb = backup_ephemeral_gb
            instance.ephemeral_gb = backup_ephemeral_gb

        if 'disk.local' in disk_mapping:
            LOG.debug('svt: Creating disk.local')

            # disk.local an an image. Nova will create a raw image to be used
            # as the ephemeral disk.
            fn = functools.partial(self._create_ephemeral,
                                   fs_label='ephemeral0',
                                   os_type=instance.os_type)
            fname = "ephemeral_%s_%s" % (ephemeral_gb, os_type_with_default)
            size = ephemeral_gb * units.Gi

            # If disk.local does not exist, the fetch_func will be called
            image('disk.local').cache(fetch_func=fn,
                                      filename=fname,
                                      size=size,
                                      ephemeral_size=ephemeral_gb)

            # Resize disk.local if necessary
            disk_local_path = os.path.join(
                svt_utils.get_instance_path(instance), 'disk.local')
            self._resize_image(disk_local_path, size)

        for idx, eph in enumerate(driver.block_device_info_get_ephemerals(
                block_device_info)):
            LOG.debug('svt: Creating ephemeral%d', idx)

            # Create more ephermal disks as needed, only if ephermals is in
            # block_device_info dict
            fn = functools.partial(self._create_ephemeral,
                                   fs_label='ephemeral%d' % idx,
                                   os_type=instance.os_type)
            size = eph['size'] * units.Gi
            fname = "ephemeral_%s_%s" % (eph['size'], os_type_with_default)

            image(blockinfo.get_eph_disk(idx)).cache(
                fetch_func=fn,
                filename=fname,
                size=size,
                ephemeral_size=eph['size'])

        # We do not care about restoring swap disk
        if 'disk.swap' in disk_mapping:
            LOG.debug('svt: Creating disk.swap')

            # Delete existing disk.swap
            disk_swap_path = os.path.join(
                svt_utils.get_instance_path(instance), 'disk.swap')
            utils.execute('rm', '-rf', disk_swap_path)

            mapping = disk_mapping['disk.swap']
            swap_mb = 0

            swap = driver.block_device_info_get_swap(block_device_info)
            if driver.swap_is_usable(swap):
                swap_mb = swap['swap_size']
            elif (inst_type['swap'] > 0 and
                  not block_device.volume_in_mapping(mapping['dev'],
                                                     block_device_info)):
                swap_mb = inst_type['swap']

            # Use larger of swap disk, backup or flavor
            if int(backup_swap_mb) > swap_mb:
                swap_mb = backup_swap_mb

            if swap_mb > 0:
                size = swap_mb * units.Mi

                # If disk.swap does not exist, the fetch_func will be called
                image('disk.swap').cache(fetch_func=self._create_swap,
                                         filename="swap_%s" % swap_mb,
                                         size=size,
                                         swap_mb=swap_mb)

        # Config drive
        if configdrive.required_by(instance):
            LOG.info(_LI('Using config drive'), instance=instance)
            extra_md = {}
            if admin_pass:
                extra_md['admin_pass'] = admin_pass

            inst_md = instance_metadata.InstanceMetadata(instance,
                content=files, extra_md=extra_md, network_info=network_info)
            with configdrive.ConfigDriveBuilder(instance_md=inst_md) as cdb:
                configdrive_path = self._get_disk_config_path(instance, suffix)
                LOG.info(_LI('Creating config drive at %(path)s'),
                         {'path': configdrive_path}, instance=instance)

                try:
                    cdb.make_drive(configdrive_path)
                except processutils.ProcessExecutionError as e:
                    with excutils.save_and_reraise_exception():
                        LOG.error(_LE('Creating config drive failed '
                                      'with error: %s'), e, instance=instance)

        # File injection only if needed
        elif inject_files and CONF.libvirt.inject_partition != -2:
            if booted_from_volume:
                LOG.warn(_LW('File injection into a boot from volume '
                             'instance is not supported'), instance=instance)
            self._inject_data(
                instance, network_info, admin_pass, files, suffix)

        if CONF.libvirt.virt_type == 'uml':
            libvirt_utils.chown(image('disk').path, 'root')

        # Handle any attached volumes at a later stage
        return restored_volumes

    def _restore_volumes(self, context, instance, volumes, suffix='',
                         volume_avail_zone=None):
        """Restore any volumes associated with a restored instance backup."""

        # Syntactic nicety
        def basepath(fname='', suffix=suffix):
            return os.path.join(svt_utils.get_instance_path(instance),
                                fname + suffix)

        LOG.debug('svt: _restore_volumes')

        restored_volumes = {}  # key = device. value = volume uuid.
        for device in volumes:
            volume_path = basepath(fname=volumes[device])
            if not os.path.exists(volume_path):
                LOG.warn(_LW("%s does not exist"), volume_path)
                raise exception.VolumeNotFound()

            volume_info = images.qemu_img_info(volume_path)

            volume_size = volume_info.virtual_size  # Size in bytes
            volume_size_gb = volume_size / 1073741824  # 1024^3
            volume_name = instance['hostname'] + '-volume'
            volume_desc = ""

            # Set volume metadata so it could be restored properly
            volume_metadata = {'svt_container': instance.uuid,
                               'svt_existing_volume_name': volumes[device],
                               'svt_device_name': device}

            LOG.debug('svt: device=%s. volume_path=%s. volume_size_gb=%s' %
                      (device, volume_path, volume_size_gb))

            # Restore volume into OpenStack
            restored_volume = self._volume_api.create(
                context, volume_size_gb, volume_name, volume_desc,
                metadata=volume_metadata, availability_zone=volume_avail_zone)

            # Track the mount point and volume ID
            restored_volumes.update({device: restored_volume['id']})

        return restored_volumes

    def migrate_disk_and_power_off(self, context, instance, dest,
                                   flavor, network_info,
                                   block_device_info=None,
                                   timeout=0, retry_interval=0):
        """
        This method will rename the existing instance container to
        <uuid>_resize. It will then copy the disks
        (disk, disk.local, disk.swap) inside <uuid>_resize to the original
        instance container.
        """

        LOG.debug("Starting migrate_disk_and_power_off", instance=instance)

        # Checks if the migration needs a disk resize down.
        for kind in ('root_gb', 'ephemeral_gb'):
            if flavor[kind] < instance[kind]:
                reason = _("Unable to resize disk down.")
                raise exception.InstanceFaultRollback(
                    exception.ResizeError(reason=reason))

        disk_info_text = self.get_instance_disk_info(instance['name'],
                block_device_info=block_device_info)
        disk_info = jsonutils.loads(disk_info_text)

        # NOTE(dgenin): Migration is not implemented for LVM backed instances.
        if (CONF.libvirt.images_type == 'lvm' and
                not self._is_booted_from_volume(instance, disk_info_text)):
            reason = "Migration is not supported for LVM backed instances"
            raise exception.MigrationPreCheckError(reason)

        # Copy disks to destination
        # Rename instance directory to _resize at first for using
        # shared storage for instance dir (eg. NFS).
        inst_base = libvirt_utils.get_instance_path(instance)
        inst_base_resize = inst_base + "_resize"
        shared_storage = self._is_storage_shared_with(dest, inst_base)

        # Try to create the directory on the remote compute node.
        # If this fails we pass the exception up the stack so we can catch
        # failures here earlier
        if not shared_storage:
            utils.execute('ssh', dest, 'mkdir', '-p', inst_base)

        self.power_off(instance, timeout, retry_interval)

        block_device_mapping = driver.block_device_info_get_mapping(
            block_device_info)
        volumes = {}  # key = volume id. value = volume name.
        for vol in block_device_mapping:
            connection_info = vol['connection_info']
            disk_dev = vol['mount_device'].rpartition("/")[2]
            self._disconnect_volume(connection_info, disk_dev)

            # Append volume name to list so it could be copied over later
            if connection_info.get('data') is not None:
                volume_id = connection_info['serial']
                volume_name = connection_info['data']['name']
                volumes.update({volume_id: volume_name})

        try:
            utils.execute('mv', inst_base, inst_base_resize)
            # If we are migrating the instance with shared storage then
            # create the directory.  If it is a remote node the directory
            # has already been created.
            if shared_storage:
                dest = None
                utils.execute('mkdir', '-p', inst_base)

            active_flavor = flavors.extract_flavor(instance)
            for info in disk_info:
                LOG.debug('svt: info=%s. shared_storage=%s.' %
                          (info, shared_storage))

                # Assume inst_base == dirname(info['path'])
                img_path = info['path']
                fname = os.path.basename(img_path)
                from_path = os.path.join(inst_base_resize, fname)

                if (fname == 'disk.swap' and
                    active_flavor.get('swap', 0) != flavor.get('swap', 0)):
                    # To properly resize the swap partition, it must be
                    # re-created with the proper size.  This is acceptable
                    # because when an OS is shut down, the contents of the
                    # swap space are just garbage, the OS doesn't bother about
                    # what is in it.

                    # We will not copy over the swap disk here, and rely on
                    # finish_migration/_create_image to re-create it for us.
                    continue

                if info['type'] == 'qcow2' and info['backing_file']:
                    tmp_path = from_path + "_rbase"
                    # merge backing file
                    utils.execute('qemu-img', 'convert', '-f', 'qcow2',
                                  '-O', 'qcow2', from_path, tmp_path)

                    if shared_storage:
                        utils.execute('mv', tmp_path, img_path)
                    else:
                        libvirt_utils.copy_image(tmp_path, img_path, host=dest)
                        utils.execute('rm', '-f', tmp_path)

                else:  # raw or qcow2 with no backing file
                    libvirt_utils.copy_image(from_path, img_path, host=dest)

            # Copy over any volumes in the resize container to the original
            # container
            LOG.debug('svt: volumes=%s', volumes)
            for volume_id in volumes:
                volume_name = volumes[volume_id]
                from_path = os.path.join(inst_base_resize, volume_name)
                img_path = os.path.join(inst_base, volume_name)

                libvirt_utils.copy_image(from_path, img_path, host=dest)

                # Make sure the svt_container is still pointing to the correct
                # container
                instance_id = instance.uuid
                metadata = {'svt_container': instance_id}
                self._volume_api.update_volume_metadata(context.elevated(),
                                                        volume_id,
                                                        metadata)
        except Exception:
            with excutils.save_and_reraise_exception():
                self._cleanup_remote_migration(dest, inst_base,
                                               inst_base_resize,
                                               shared_storage)

        return disk_info_text

    def _wait_for_running(self, instance):
        state = self.get_info(instance)['state']

        if state == power_state.RUNNING:
            LOG.info(_LI("Instance running successfully."), instance=instance)
            raise loopingcall.LoopingCallDone()

    def finish_migration(self, context, migration, instance, disk_info,
                         network_info, image_meta, resize_instance,
                         block_device_info=None, power_on=True):
        LOG.debug("Starting finish_migration", instance=instance)

        # Resize disks.  Only "disk" and "disk.local" are necessary.
        LOG.debug('svt: disk_info=%s', disk_info)
        disk_info = jsonutils.loads(disk_info)
        for info in disk_info:
            size = self._disk_size_from_instance(instance, info)
            if resize_instance:
                self._disk_resize(info, size)
            if info['type'] == 'raw' and CONF.use_cow_images:
                self._disk_raw_to_qcow2(info['path'])

        disk_info = blockinfo.get_disk_info(CONF.libvirt.virt_type,
                                            instance,
                                            block_device_info,
                                            image_meta)

        # Set svt_container metadata is in block_device_mapping
        # It gets lost between migrate_disk_and_power_off and finish_migration
        block_device_mapping = driver.block_device_info_get_mapping(
            block_device_info)
        for vol in block_device_mapping:
            connection_info = vol['connection_info']
            volume_id = connection_info['serial']
            volume_metadata = self._volume_api.get_volume_metadata(context,
                                    volume_id)

            if 'svt_container' in volume_metadata:
                connection_info['svt_container'] = \
                    volume_metadata['svt_container']

        # Assume _create_image do nothing if a target file exists.
        self._create_image(context, instance,
                           disk_mapping=disk_info['mapping'],
                           network_info=network_info,
                           block_device_info=None, inject_files=False)
        xml = self._get_guest_xml(context, instance, network_info, disk_info,
                                  block_device_info=block_device_info,
                                  write_to_disk=True)
        self._create_domain_and_network(context, xml, instance, network_info,
                                        block_device_info, power_on,
                                        vifs_already_plugged=True)

        # Register instance with SimpliVity virtual controller
        svt_utils.register_instance(self.svt_connection, instance)

        if power_on:
            timer = loopingcall.FixedIntervalLoopingCall(
                                                    self._wait_for_running,
                                                    instance)
            timer.start(interval=0.5).wait()

    def finish_revert_migration(self, context, instance, network_info,
                                block_device_info=None, power_on=True):
        LOG.debug("Starting finish_revert_migration")

        inst_base = svt_utils.get_instance_path(instance)
        inst_base_resize = inst_base + "_resize"

        # NOTE(danms): if we're recovering from a failed migration,
        # make sure we don't have a left-over same-host base directory
        # that would conflict. Also, don't fail on the rename if the
        # failure happened early.
        if os.path.exists(inst_base_resize):
            self._cleanup_failed_migration(inst_base)
            utils.execute('mv', inst_base_resize, inst_base)

        disk_info = blockinfo.get_disk_info(CONF.libvirt.virt_type,
                                            instance,
                                            block_device_info)

        # Set svt_container metadata is in block_device_mapping
        # It gets lost between migrate_disk_and_power_off and
        # finish_revert_migration
        block_device_mapping = driver.block_device_info_get_mapping(
            block_device_info)
        for vol in block_device_mapping:
            connection_info = vol['connection_info']
            volume_name = connection_info['data']['name']

            volume_path = os.path.join(inst_base, volume_name)
            if os.path.exists(volume_path):
                connection_info['svt_container'] = instance.uuid

        xml = self._get_guest_xml(context, instance, network_info, disk_info,
                                  block_device_info=block_device_info)
        self._create_domain_and_network(context, xml, instance, network_info,
                                        block_device_info, power_on)

        if power_on:
            timer = loopingcall.FixedIntervalLoopingCall(
                                                    self._wait_for_running,
                                                    instance)
            timer.start(interval=0.5).wait()

    @staticmethod
    def _get_device_name(disk_bus, disk_mapping, volumes):
        max_dev = blockinfo.get_dev_count_for_disk_bus(disk_bus)
        devs = range(max_dev)  # 4 for ide, 26 otherwise

        dev_prefix = blockinfo.get_dev_prefix_for_disk_bus(disk_bus)
        for idx in devs:
            disk_device = dev_prefix + chr(ord('a') + idx)
            if (not blockinfo.has_disk_dev(disk_mapping, disk_device) and
                    disk_device not in volumes):
                return disk_device

        # We exhausted all possible device names, give up
        raise exception.NovaException(_("No free disk device names for "
                                        "prefix '%s'"), dev_prefix)

    @staticmethod
    def _resize_image(image_path, image_size):
        """Resize a given image to the desired size"""

        # Resize image if necessary (size in bytes)
        if image_size > 0 and disk.get_disk_size(image_path) < image_size:
            LOG.debug('svt: Resizing disk %s to %s' % (image_path, image_size))
            disk.extend(image_path, image_size)

    def rebuild(self, context, instance, image_meta, injected_files,
                admin_password, bdms, detach_block_devices,
                attach_block_devices, network_info=None,
                recreate=False, block_device_info=None,
                preserve_ephemeral=False):
        # Power off instance before restoring
        power_back_on = False
        state = self.get_info(instance)['state']
        if state == power_state.RUNNING or state == power_state.SUSPENDED:
            self._destroy(instance)
            power_back_on = True

        instance.task_state = task_states.REBUILD_SPAWNING
        instance.save(expected_task_state=[task_states.REBUILDING])

        meta_properties = image_meta.get('properties')
        backup_name = meta_properties.get('svt_backup_name')
        attached_volumes_json = str(meta_properties.get(
                                    'svt_attached_volumes', "[]"))
        attached_volumes = json.loads(attached_volumes_json.replace("'", '"'))

        # Detach any existing volumes that are not part of the backup
        bdm_devices = [bdm.device_name for bdm in bdms]
        attached_volume_devices = ['/dev/' + device_name
                                   for device_name in attached_volumes.keys()]

        # Detach any existing volume that is not part of the backup
        LOG.debug("svt: bdm_devices=%s", bdm_devices)
        LOG.debug("svt: attached_volume_devices=%s", attached_volume_devices)
        for bdm in bdms:
            if (bdm.is_volume and
                bdm.device_name not in attached_volume_devices):

                LOG.debug("svt: Detaching volume %s", bdm.device_name)
                connector = self.get_volume_connector(instance)
                connection_info = self._volume_api.initialize_connection(
                    context, bdm.volume_id, connector)

                self.detach_volume(connection_info, instance, bdm.device_name)
                self._volume_api.detach(context, bdm.volume_id)
                bdm.destroy()
                break

        # Restore instance from backup
        svt_utils.vm_restore(self.svt_connection, instance.uuid, backup_name)

        if power_back_on:
            self._hard_reboot(context, instance, network_info,
                              block_device_info)
