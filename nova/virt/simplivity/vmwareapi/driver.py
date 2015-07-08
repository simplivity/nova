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

from oslo.config import cfg

from nova.compute import flavors
from nova.compute import task_states
from nova import conductor
from nova import exception
from nova.i18n import _, _LI, _LE
from nova import image
from nova.openstack.common import excutils
from nova.openstack.common import jsonutils
from nova.openstack.common import log as logging
from nova.openstack.common import units
from nova import utils
from nova.virt import configdrive
from nova.virt import driver
from nova.virt.simplivity.vmwareapi import exception as svt_exception
from nova.virt.simplivity.vmwareapi import utils as svt_utils
from nova.virt.simplivity.vmwareapi import virtual_controller as vc
from nova.virt.vmwareapi import driver as vmwareapi_driver
from nova.virt.vmwareapi import ds_util
from nova.virt.vmwareapi import error_util
from nova.virt.vmwareapi import vif as vmware_vif
from nova.virt.vmwareapi import vim_util
from nova.virt.vmwareapi import vm_util
from nova import volume


LOG = logging.getLogger(__name__)

"""
NOTE(thangp): 1 virtual controller = 1 cluster = 1 nova-compute
It is important to set "use_linked_clone = False", so that the instance
is wholly contained in the directory.

Setup instructions:

1. Edit /etc/nova/nova.conf:
  [DEFAULT]
  use_cow_images = False
  compute_driver = nova.virt.simplivity.vmwareapi.SvtVMwareDriver
  multi_instance_display_name_template = %(name)s%(count)s

  [simplivity]
  vc_host = 10.131.3.155
  vc_username = svtbuild
  vc_password = svtpasswd

  [vmware]
  use_linked_clone = False
  cluster_name = Boston
  host_username = administrator
  host_password = svtpasswd
  host_ip = 10.131.50.25

2. Restart nova-compute (as non-root)
"""
simplivity_opts = [
    cfg.StrOpt('vc_datacenter',
               default=None,
               help='Name of SimpliVity datacenter to use for instances'),
    cfg.StrOpt('vc_host',
               default=None,
               help='Hostname or ipv4 address of the virtual controller for '
                    'the given cluster'),
    cfg.StrOpt('vc_username',
               default='svtcli',
               help='Username to log into the virtual controller'),
    cfg.StrOpt('vc_password',
               default=None,
               help=('Password associated with the username to log into '
                     'the virtual controller')),
    ]

CONF = cfg.CONF
CONF.register_opts(simplivity_opts, 'simplivity')

DEFAULT_DISK_TYPE = "eagerZeroedThick"
DEFAULT_ADAPTER_TYPE = "lsiLogic"


class SvtVMwareDriver(vmwareapi_driver.VMwareVCDriver):
    """The vCenter host connection object."""

    def __init__(self, virtapi, scheme="https"):
        LOG.debug("svt: Initializing SimpliVity VMware driver")
        # To access the user and password:
        #     user = CONF.simplivity.vc_username
        #     passwd = CONF.simplivity.vc_password

        super(SvtVMwareDriver, self).__init__(virtapi)

        # Reference to image, volume, and conductor APIs
        self._image_api = image.API()
        self._volume_api = volume.API()
        self._conductor_api = conductor.API()

        # Establish connection to virtual controller
        self.vc_connection = self._get_vc_connection()
        self.vc_ops = vc.SvtOperations(self.vc_connection)

    def _get_vc_connection(self):
        """Returns an object representing a connection to the virtual
        controller.
        """
        return vc.SvtConnection(CONF.simplivity.vc_host,
                                CONF.simplivity.vc_username,
                                CONF.simplivity.vc_password,
                                vmware_username=CONF.vmware.host_username,
                                vmware_password=CONF.vmware.host_password)

    def _get_vm_and_vmdk_attribs(self, instance):
        """Get the root vmdk file name that the VM is pointing to."""
        vm_ref = vm_util.get_vm_ref(self._session, instance)
        hw_devices = self._session._call_method(vim_util,
                    "get_dynamic_property", vm_ref,
                    "VirtualMachine", "config.hardware.device")
        (vmdk_file_path_before_snapshot, adapter_type,
         disk_type) = vm_util.get_vmdk_path_and_adapter_type(
                                    hw_devices, uuid=instance.uuid)
        if not vmdk_file_path_before_snapshot:
            LOG.debug("svt: No root disk defined")
            raise error_util.NoRootDiskDefined()

        datastore_name = ds_util.DatastorePath.parse(
                vmdk_file_path_before_snapshot).datastore
        os_type = self._session._call_method(vim_util,
                "get_dynamic_property", vm_ref, "VirtualMachine",
                "summary.config.guestId")
        return (vm_ref, vmdk_file_path_before_snapshot, adapter_type,
                disk_type, datastore_name, os_type)

    def _get_volume_info(self, context, volume_uuid, properties=None):
        """Get a set of properties for a given volume."""
        volume_info = {}
        properties = properties or []
        try:
            """
            A volume could return the following info:
                {'status': 'in-use',
                 'instance_uuid': 'e2c4765d-0a14-499f-8221-ed6a9b42420f',
                 'display_name': 'volume1',
                 'attach_time': '',
                 'availability_zone': 'nova',
                 'bootable': False,
                 'attach_status': 'attached',
                 'display_description': '',
                 'volume_type_id': 'None',
                 'volume_metadata': {'readonly': 'False',
                                     'attached_mode': 'rw'},
                 'snapshot_id': None,
                 'mountpoint': '/dev/sdb',
                 'id': '91aff3dc-907a-43b8-9f8e-576cba83d2ee',
                 'size': 5
                }
            """
            volume = self._volume_api.get(context, volume_uuid)
            for property in properties:
                volume_info.update({property: volume[property]})
        except Exception:
            LOG.debug("svt: Could not find volume %s", volume_uuid)

        return volume_info

    def _get_attached_volumes(self, context, vm_ref, instance):
        """Get a list of attached volumes for a given instance."""
        attached_volumes = []

        node_vmops = self._get_vmops_for_compute_node(instance['node'])
        hardware_devices = node_vmops._session._call_method(vim_util,
                "get_dynamic_property", vm_ref, "VirtualMachine",
                "config.hardware.device")
        if hardware_devices.__class__.__name__ == "ArrayOfVirtualDevice":
            hardware_devices = hardware_devices.VirtualDevice

        for device in hardware_devices:
            if (device.__class__.__name__ == "VirtualDisk" and
                device.backing.__class__.__name__ ==
                "VirtualDiskFlatVer2BackingInfo"):

                vmdk_file_path = device.backing.fileName
                vmdk_file_name = os.path.basename(vmdk_file_path)

                # Volume vmdk disks are prefixed with "volume-"
                if not vmdk_file_name.startswith("volume-"):
                    continue

                # Extract the volume UUID in "volume-<volume_uuid>.vmdk"
                volume_uuid = vmdk_file_name[7:-5]
                # Save the volume ID, mount point, and metadata
                properties = ['id', 'mountpoint', 'volume_metadata', 'size',
                              'availability_zone']
                volume_info = self._get_volume_info(context, volume_uuid,
                                                    properties)
                attached_volumes.append(volume_info)

        LOG.debug("svt: attached_volumes=%s", attached_volumes)
        return attached_volumes

    def snapshot(self, context, instance, image_id, update_task_state):
        """Create snapshot from a running VM instance."""
        snapshot = self._image_api.get(context, image_id)

        """
        Query the vmdk file type.
        For example, the following properties can be returned.
            vmdk_file_path=[svtds] <instance-uuid>/<instance-uuid>.vmdk
            adapter_type=ide,
            disk_type=thin,
            datastore_name=svtds,
            os_type=otherGuest
        """
        (vm_ref, vmdk_file_path, adapter_type, disk_type, datastore_name,
         os_type) = self._get_vm_and_vmdk_attribs(instance)

        # Save network info so network interface could be detached from
        # cloned instance
        vifs = []
        network_info = instance.info_cache.network_info
        for vif in network_info:
            vifs.append({"id": vif['id'], "address": vif['address']})

        # Find any volumes attached to the instance and save any info
        # so they can be restored later on
        attached_volumes = self._get_attached_volumes(context, vm_ref,
                                                      instance)

        instance_type = flavors.extract_flavor(instance)
        file_size = instance_type['root_gb'] * units.Gi  # Size in bytes

        # We need to add a fake location in order to have glance accept
        # the image
        location = "http://localhost"

        metadata = {"is_public": False,
                    "status": "active",
                    "name": snapshot['name'],
                    "container_format": "bare",
                    "size": file_size,
                    "location": location,
                    "min_disk": int(instance_type['root_gb']),
                    "min_ram": int(instance_type['memory_mb']),
                    "properties": {
                       "image_state": "available",
                       # These are properties saved by the VMware driver, so
                       # we want to comply with their convention
                       "vmware_adaptertype": adapter_type,
                       "vmware_disktype": disk_type,
                       "vmware_ostype": os_type,
                       "vmware_image_version": 1,
                       "owner_id": instance['project_id'],
                       # Specific properties that need to be saved in order
                       # for it to be restored later
                       "svt_datacenter_name": CONF.simplivity.vc_datacenter,
                       "svt_datastore_name": datastore_name,
                       "svt_backup_name": snapshot['name'],
                       "svt_instance_uuid": instance['uuid'],
                       "svt_network_info": jsonutils.dumps(vifs),
                       "svt_ephemeral_gb": instance_type['ephemeral_gb'],
                       "svt_swap": instance_type['swap'],
                       "svt_attached_volumes": jsonutils.dumps(
                                attached_volumes)
                       }
                    }

        LOG.info(_LI("Beginning snapshot process"), instance=instance)
        update_task_state(task_state=task_states.IMAGE_PENDING_UPLOAD)

        # Execute svt-vm-backup to save the state of the instance
        # Exceptions are handled and raised within vm_backup
        self.vc_ops.vm_backup(datastore_name, instance['uuid'],
                              snapshot['name'])

        # Save image (placeholder) in glance
        update_task_state(task_state=task_states.IMAGE_UPLOADING,
                          expected_state=task_states.IMAGE_PENDING_UPLOAD)
        self._image_api.update(context, image_id, metadata)
        LOG.info(_LI("Snapshot image upload complete"), instance=instance)

    def spawn(self, context, instance, image_meta, injected_files,
              admin_password, network_info=None, block_device_info=None):
        """Create an instance."""
        if svt_utils._is_simplivity_image(image_meta):
            # Spawn instance from SimpliVity backup
            self._spawn_from_backup(context, instance, image_meta,
                                    injected_files, admin_password,
                                    network_info, block_device_info)
        else:
            node_vmops = self._get_vmops_for_compute_node(instance['node'])
            node_vmops.spawn(context, instance, image_meta, injected_files,
                  admin_password, network_info, block_device_info)

    def _spawn_from_backup(self, context, instance, image_meta,
                           injected_files, admin_password, network_info,
                           block_device_info, instance_name=None,
                           power_on=True):
        """Spawn instance from SimpliVity backup image."""

        node_vmops = self._get_vmops_for_compute_node(instance['node'])
        client_factory = node_vmops._session._get_vim().client.factory

        bdm_root = False  # Is booted from volume
        if block_device_info:
            msg = "Block device information present: %s" % block_device_info

            # block_device_info can contain an auth_password so we have to
            # scrub the message before logging it
            LOG.debug(logging.mask_password(msg), instance=instance)
            block_device_mapping = driver.block_device_info_get_mapping(
                    block_device_info)
            if block_device_mapping:
                bdm_root = True

        (dc_info, datastore) = svt_utils.get_datacenter_and_datastore(
            node_vmops)

        # Image properties
        root_gb_in_kb = instance.root_gb * units.Mi  # Size in bytes
        (file_type, is_iso) = svt_utils._get_disk_format(image_meta)
        (vmdk_file_size_in_kb, os_type, adapter_type, disk_type, vif_model,
            image_linked_clone) = svt_utils.get_image_properties(context,
                                                                 instance,
                                                                 root_gb_in_kb)

        # linked_clone should always be false in a SimpliVity cluster
        # so that the instance is wholly contained in the directory
        linked_clone = svt_utils.decide_linked_clone(
            image_linked_clone, CONF.vmware.use_linked_clone)
        if linked_clone:
            LOG.error(_LE("SimpliVity does not support use_linked_clone, "
                          "it must be disabled."))
            raise svt_exception.SvtOperationNotSupported()

        # vif_infos is an array of vif_dict
        vif_infos = vmware_vif.get_vif_info(node_vmops._session,
            node_vmops._cluster, utils.is_neutron(), vif_model, network_info)

        # Get the instance name. In some cases this may differ from the UUID,
        # e.g. when the spawn of a rescue instance takes place.
        if not instance_name:
            instance_name = instance.uuid

        # Image info to use when instance is restored
        meta_properties = image_meta.get('properties')
        src_instance_uuid = meta_properties.get('svt_instance_uuid')
        backup_name = meta_properties.get('svt_backup_name')
        vifs = jsonutils.loads(meta_properties.get('svt_network_info', "[]"))
        attached_volumes = jsonutils.loads(
            meta_properties.get('svt_attached_volumes', "[]"))

        # In case svt_network_info is empty, find the network_info from the
        # source instance
        vifs = vifs or svt_utils.get_instance_network_info(context,
                                                           src_instance_uuid)

        if not src_instance_uuid or not backup_name:
            LOG.error(_LE("Missing svt_instance_uuid or svt_backup_name in "
                          "image metadata properties"))
            raise svt_exception.SvtBackupInfoNotFound()

        # Restore instance from backup
        self.vc_ops.backup_restore(instance.uuid, datastore.name,
                                   src_instance_uuid, backup_name)

        # svt-backup-restore will create the instance and attach the root
        # disk, so just retrieve the instance reference
        vm_ref = vm_util.get_vm_ref(self._session, instance)

        # Cache the vm_ref. This saves a remote call to the vCenter. This uses
        # the instance_name. This covers all use cases including rescue and
        # resize.
        vm_util.vm_ref_cache_update(instance_name, vm_ref)

        hardware_devices = node_vmops._session._call_method(vim_util,
                "get_dynamic_property", vm_ref, "VirtualMachine",
                "config.hardware.device")
        if hardware_devices.__class__.__name__ == "ArrayOfVirtualDevice":
                hardware_devices = hardware_devices.VirtualDevice

        # Detach old network interfaces and cdrom
        svt_utils.detach_instance_devices(client_factory, node_vmops, vm_ref,
                                          instance, hardware_devices)
        svt_utils.detach_instance_networks(client_factory, node_vmops, vm_ref,
                                           instance, hardware_devices, vifs)

        # Create a new spec so that selected flavor is applied
        detach_config_spec = vm_util.get_vm_create_spec(
            client_factory, instance, instance_name, datastore.name,
            vif_infos, os_type)
        vm_util.reconfigure_vm(node_vmops._session, vm_ref, detach_config_spec)

        # Set the machine.id parameter of the instance to inject
        # the NIC configuration inside the VM
        if CONF.flat_injected:
            node_vmops._set_machine_id(client_factory, instance, network_info)

        # Set the VNC configuration of the instance, VNC port starts from 5900
        if CONF.vnc_enabled:
            node_vmops._get_and_set_vnc_config(client_factory, instance)

        if not bdm_root:
            # Cached vmdk root image on datastore
            upload_name = instance.image_ref
            upload_folder = '%s/%s' % (node_vmops._base_folder, upload_name)
            uploaded_file_path = str(datastore.build_path(
                upload_folder, "%s.%s" % (upload_name, file_type)))

            session_vim = node_vmops._session._get_vim()
            cookies = session_vim.client.options.transport.cookiejar

            # Disks are restored with the original names, i.e. the source
            # instance UUID
            root_vmdk_path = ds_util.DatastorePath(datastore.name,
                instance_name, "%s.vmdk" % src_instance_uuid)

            # Resize the copy to the appropriate size. No need for cleanup up
            # here, as _extend_virtual_disk already does it
            if root_gb_in_kb > vmdk_file_size_in_kb:
                node_vmops._extend_virtual_disk(instance,
                      root_gb_in_kb, root_vmdk_path,
                      dc_info.ref)

            if is_iso:
                node_vmops._attach_cdrom_to_vm(vm_ref, instance,
                                               datastore.ref,
                                               uploaded_file_path)

            # Prepare config drive and attach to instance
            if configdrive.required_by(instance):
                uploaded_iso_path = node_vmops._create_config_drive(instance,
                    injected_files, admin_password, datastore.name,
                    dc_info.name, instance.uuid, cookies)
                uploaded_iso_path = ds_util.DatastorePath(datastore.name,
                                                          uploaded_iso_path)
                node_vmops._attach_cdrom_to_vm(vm_ref, instance, datastore.ref,
                    str(uploaded_iso_path))

        else:
            # Boot instance from volume, attach the root disk to the VM
            for root_disk in block_device_mapping:
                connection_info = root_disk['connection_info']
                node_vmops._volumeops.attach_root_volume(connection_info,
                     instance, node_vmops._default_root_device, datastore.ref)

        # Attach any connected volumes
        if attached_volumes:
            self._restored_volumes(context, vm_ref, instance, node_vmops,
                                   attached_volumes)

        # Power on virtual machine
        if power_on:
            vm_util.power_on_instance(node_vmops._session, instance,
                                      vm_ref=vm_ref)

    def _restored_volumes(self, context, vm_ref, instance, node_vmops,
                          attached_volumes):
        """Restore backed up volume to instance."""
        # Taken from nova.compute.manager
        LOG.debug("svt: Restoring %d volumes", len(attached_volumes))

        """
        # attached_volumes is an array of dicts:
            {'mountpoint': '/dev/sdb',
             'id': '5e645ca7-3393-453f-9c3f-cf3df6e3c6ea',
             'volume_metadata': {'readonly': 'False',
                                 'attached_mode': 'rw'},
             'availability_zone': 'nova',
             'size': 5}
        """
        for volume_info in attached_volumes:
            # Volume name: <instance_uuid>-volume
            name = instance.uuid + "-volume"
            description = ""
            size = volume_info['size']
            az = volume_info.get('availability_zone')

            # NOTE(thangp): Restore volume in the same availability zone
            if az is None:
                LOG.error(_LE("Missing availability_zone for volume in "
                              "image metadata properties"))
                raise svt_exception.SvtRestoreFailed()

            # Save volume metadata, so it could be used to restore the volume
            metadata = volume_info['volume_metadata']
            metadata['original_volume_id'] = volume_info['id']

            LOG.debug("svt: Creating new volume %s", name)
            new_volume = self._volume_api.create(context, size, name,
                description, availability_zone=az, metadata=metadata)

            # Restored volumes reside in instance backing. Re-attach them to
            # the instance but update the name.

            # Create connection_info dict, since it is needed by _volume_api
            mountpoint = volume_info['mountpoint']
            new_volume_id = new_volume['id']
            try:
                connector = self.get_volume_connector(instance)
                connection_info = self._volume_api.initialize_connection(
                    context, new_volume_id, connector)
            except Exception:
                with excutils.save_and_reraise_exception():
                    LOG.exception(_("Failed to connect to volume "
                                    "%(volume_id)s while attaching at "
                                    "%(mountpoint)s"),
                                  {'volume_id': new_volume_id,
                                   'mountpoint': mountpoint},
                                  context=context, instance=instance)
                    self._volume_api.unreserve_volume(context, new_volume_id)

            self.attach_volume(context, connection_info, instance, mountpoint)
            self._volume_api.attach(context, new_volume_id,
                                    instance.uuid, mountpoint)
            values = {
                'instance_uuid': instance.uuid,
                'connection_info': jsonutils.dumps(connection_info),
                'device_name': mountpoint,
                'delete_on_termination': True,  # Delete volume on delete
                'virtual_name': None,
                'snapshot_id': None,
                'volume_id': new_volume_id,
                'volume_size': size,
                'no_device': None}
            self._conductor_api.block_device_mapping_update_or_create(context,
                                                                      values)

    def _delete_instance_backups(self, context, instance):
        """Delete any backups associated with the instance. Once an
        instance is deleted, any backups associated with it should be
        automatically removed by SimpliVity.
        """
        if instance.get('image_ref') is None:
            return

        # Delete all SimpliVity backups associated with the instance
        node_vmops = self._get_vmops_for_compute_node(instance['node'])
        datastore = ds_util.get_datastore(node_vmops._session,
            node_vmops._cluster, datastore_regex=node_vmops._datastore_regex)
        self.vc_ops.backup_delete(instance.uuid, datastore.name)

        # Search for image where its properties contain the instance uuid
        filters = {'properties': {'svt_instance_uuid': instance.uuid}}
        images = self._image_api.get_all(context, filters=filters) or []

        # Delete glance image if it is a SimpliVity image and is associated
        # with the instance being deleted
        for svt_image in images:
            LOG.info(_LI("Deleting SimpliVity backup image %s"),
                     svt_image['id'])
            self._image_api.delete(context, svt_image['id'])

    def destroy(self, context, instance, network_info, block_device_info=None,
                destroy_disks=True, migrate_data=None):
        """Destroy instance."""

        # Find any backups of the instance and delete before beginning
        # to delete the instance
        self._delete_instance_backups(context, instance)

        # Destroy gets triggered when Resource Claim in resource_tracker
        # is not successful. When resource claim is not successful,
        # node is not set in instance. Perform destroy only if node is set
        if not instance['node']:
            return

        node_vmops = self._get_vmops_for_compute_node(instance['node'])
        node_vmops.destroy(instance, destroy_disks)

    def _attach_volume_vmdk(self, context, connection_info, instance,
                            mountpoint):
        """Attach vmdk volume storage to instance."""
        node_volumeops = self._get_volumeops_for_compute_node(
            instance['node'])
        node_vmops = self._get_vmops_for_compute_node(instance['node'])
        (dc_info, datastore) = svt_utils.get_datacenter_and_datastore(
            node_vmops)

        instance_name = instance['name']
        vm_ref = vm_util.get_vm_ref(self._session, instance)

        """
        data could contain the following:
            {'volume': 'vm-784',
             'access_mode': 'rw',
             'qos_specs': None,
             'volume_id': 'd50d8795-9aff-4bff-b04f-db460b71030a'}
        """
        data = connection_info['data']
        volume = self._volume_api.get(context, data['volume_id'])
        volume_size_kb = volume['size'] * units.Mi
        metadata = volume['volume_metadata']
        original_volume_id = metadata.get('original_volume_id')

        def _get_vmdk_base_volume_device(volume_ref):
            # Get the vmdk file name that the VM is pointing to
            hardware_devices = node_volumeops._session._call_method(vim_util,
                    "get_dynamic_property", volume_ref,
                    "VirtualMachine", "config.hardware.device")
            return vm_util.get_vmdk_volume_disk(hardware_devices)

        # Get volume details from volume ref
        volume_ref = vim_util.get_moref(data['volume'], 'VirtualMachine')
        volume_device = _get_vmdk_base_volume_device(volume_ref)
        volume_name = "volume-" + data['volume_id']
        volume_vmdk_path = None

        # vmdk disk type should always be: eagerZeroedThick or preallocated
        disk_type = DEFAULT_DISK_TYPE
        adapter_type = DEFAULT_ADAPTER_TYPE

        # vmdk disk will always stay within the instance backing on attach
        volume_vmdk_path = ds_util.DatastorePath(datastore.name,
                instance.uuid, "%s.vmdk" % volume_name)
        src_vmdk_path = None
        if not volume_device and not original_volume_id:
            # If there is no disk in the volume backing, create one where the
            # name is volume-<volume_id>.vmdk
            vm_util.create_virtual_disk(self._session, dc_info.ref,
                    adapter_type, disk_type, volume_vmdk_path, volume_size_kb)
        else:
            # The vmdk disk should be inside the volume backing
            src_vmdk_path = ds_util.DatastorePath(datastore.name,
                volume_name, "%s.vmdk" % volume_name)

            # The original volume just needs to be renamed
            if original_volume_id:
                original_volume_name = "volume-" + original_volume_id
                src_vmdk_path = ds_util.DatastorePath(datastore.name,
                        instance.uuid, "%s.vmdk" % original_volume_name)

            # Move volume vmdk disk within volume backing to instance backing
            # so it can be snapshot if necessary
            LOG.debug("svt: src_vmdk_path=%s volume_vmdk_path=%s" %
                      (src_vmdk_path, volume_vmdk_path))
            svt_utils.move_virtual_disk(self._session, dc_info.ref,
                                        src_vmdk_path, volume_vmdk_path)

            # Delete volume metadata "original_volume_id" so volume does
            # not get renamed again. Only do this if the move was successful.
            if original_volume_id:
                self._volume_api.delete_volume_metadata(context,
                    data['volume_id'], ['original_volume_id'])

        # Attach the disk to virtual machine instance
        node_volumeops.attach_disk_to_vm(vm_ref, instance, adapter_type,
                disk_type, vmdk_path=volume_vmdk_path)

        # Store the uuid of the volume_device
        node_volumeops._update_volume_details(vm_ref, instance,
                                              data['volume_id'])

        LOG.info(_("Mountpoint %(mountpoint)s attached to "
                   "instance %(instance_name)s"),
                 {'mountpoint': mountpoint, 'instance_name': instance_name},
                 instance=instance)

    def attach_volume(self, context, connection_info, instance, mountpoint,
                      disk_bus=None, device_type=None, encryption=None):
        """Attach volume storage to VM instance."""
        LOG.debug("svt: attach_volume")

        # Every disk must be entirely contained in the directory in order
        # to be snapshot.

        # When a volume is attached to an instance, a reconfigure operation
        # is performed on the instance to add the volume's VMDK to it.
        self._attach_volume_vmdk(context, connection_info, instance,
                                 mountpoint)

    def _detach_volume_vmdk(self, connection_info, instance, mountpoint):
        """Detach volume storage to instance."""
        node_volumeops = self._get_volumeops_for_compute_node(instance['node'])
        node_vmops = self._get_vmops_for_compute_node(instance['node'])
        (dc_info, datastore) = svt_utils.get_datacenter_and_datastore(
            node_vmops)

        instance_name = instance['name']
        vm_ref = vm_util.get_vm_ref(node_volumeops._session, instance)

        # Detach volume from instance
        data = connection_info['data']

        def _get_vmdk_backed_disk_device(vm_ref, connection_info_data):
            # Get the vmdk file name that the VM is pointing to
            hardware_devices = node_volumeops._session._call_method(vim_util,
                    "get_dynamic_property", vm_ref, "VirtualMachine",
                    "config.hardware.device")

            # Get disk uuid
            disk_uuid = node_volumeops._get_volume_uuid(vm_ref,
                    connection_info_data['volume_id'])
            device = vm_util.get_vmdk_backed_disk_device(hardware_devices,
                                                         disk_uuid)

            # It is acceptable that no device is found
            return device

        device = _get_vmdk_backed_disk_device(vm_ref, data)
        if device:
            # Get the volume ref (shadow VM)
            volume_ref = node_volumeops._get_volume_ref(data['volume'])

            node_volumeops.detach_disk_from_vm(vm_ref, instance, device)
            LOG.info(_("Mountpoint %(mountpoint)s detached from "
                       "instance %(instance_name)s"),
                     {'mountpoint': mountpoint,
                      'instance_name': instance_name},
                     instance=instance)

            # Move volume vmdk disk within instance backing to volume backing
            disk_type = DEFAULT_DISK_TYPE
            adapter_type = DEFAULT_ADAPTER_TYPE

            volume_name = "volume-" + data['volume_id']
            src_vmdk_path = ds_util.DatastorePath(datastore.name,
                    instance.uuid, "%s.vmdk" % volume_name)
            dest_vmdk_path = ds_util.DatastorePath(datastore.name,
                    volume_name, "%s.vmdk" % volume_name)
            svt_utils.move_virtual_disk(self._session, dc_info.ref,
                                        src_vmdk_path, dest_vmdk_path)

            # Volumes need to be re-attached to the shadow VM (volume_ref)
            # on detach, so they can be later re-used
            node_volumeops.attach_disk_to_vm(volume_ref, instance,
                adapter_type, disk_type, vmdk_path=dest_vmdk_path)

            # Store the uuid of the volume_device
            node_volumeops._update_volume_details(volume_ref, instance,
                                                  data['volume_id'])
        else:
            LOG.error(_LE("Could not find volume %s") % data['volume_id'])
            raise exception.NotFound()

    def detach_volume(self, connection_info, instance, mountpoint,
                      encryption=None):
        """Detach volume storage to instance."""
        self._detach_volume_vmdk(connection_info, instance, mountpoint)

    def rebuild(self, context, instance, image_meta, injected_files,
                admin_password, bdms, detach_block_devices,
                attach_block_devices, network_info=None,
                recreate=False, block_device_info=None,
                preserve_ephemeral=False):
        node_vmops = self._get_vmops_for_compute_node(instance['node'])

        # Power off instance before restoring
        state = vm_util.get_vm_state_from_name(self._session, instance.uuid)
        active_states = ['poweredon', 'suspended']
        power_back_on = False
        if state.lower() in active_states:
            node_vmops.power_off(instance)
            power_back_on = True

        # Update instance task state
        instance.task_state = task_states.REBUILD_SPAWNING
        instance.save(expected_task_state=[task_states.REBUILDING])

        (dc_info, datastore) = svt_utils.get_datacenter_and_datastore(
            node_vmops)

        # Image info to use when instance is restored
        meta_properties = image_meta.get('properties')
        backup_name = meta_properties.get('svt_backup_name')
        attached_volumes = jsonutils.loads(
            meta_properties.get('svt_attached_volumes', "[]"))

        # Detach any existing volumes that are not part of the backup
        bdm_volume_ids = [bdm.volume_id for bdm in bdms]
        attached_volume_ids = [volume_info['id']
                               for volume_info in attached_volumes]

        # Detach any existing volume that is not part of the backup
        for bdm in bdms:
            if bdm.is_volume and bdm.volume_id not in attached_volume_ids:
                LOG.debug("svt: Detaching volume %s", bdm.volume_id)
                connector = self.get_volume_connector(instance)
                connection_info = self._volume_api.initialize_connection(
                    context, bdm.volume_id, connector)

                # NOTE(thangp): mountpoint is not used for anything other than
                # logging, so it is ok to use a static one
                self.detach_volume(connection_info, instance, 'vda')
                self._volume_api.detach(context, bdm.volume_id)
                bdm.destroy()
                break

        # Restore instance from backup
        self.vc_ops.vm_restore(datastore.name, instance.uuid, backup_name)

        # Create and attach volumes that are part of the backup but not
        # currently attached
        vm_ref = vm_util.get_vm_ref(self._session, instance)
        for volume_info in attached_volumes:
            if volume_info['id'] not in bdm_volume_ids:
                self._restored_volumes(context, vm_ref, instance, node_vmops,
                                       [volume_info])

        if power_back_on:
            node_vmops.power_on(instance)
