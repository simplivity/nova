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

from nova import exception
from nova.i18n import _LE
from nova import image
from nova import objects
from nova.openstack.common import log as logging
from nova.openstack.common import strutils
from nova.virt.vmwareapi import constants
from nova.virt.vmwareapi import ds_util
from nova.virt.vmwareapi import vif as vmware_vif
from nova.virt.vmwareapi import vm_util

LOG = logging.getLogger(__name__)
IMAGE_API = image.API()


def _is_simplivity_image(image):
    """Check if an image is a SimpliVity generated image."""

    # Extract the relevant info to construct the new instance
    is_simplivity_image = False
    if (image is not None and
            image.get('properties') and
            image['properties'].get('svt_backup_name') is not None):
        # Must have svt_backup_name property to be called a SimpliVity image
        is_simplivity_image = True

    return is_simplivity_image


def _get_disk_format(image_meta):
    disk_format = image_meta.get('disk_format')
    if disk_format not in ['iso', 'vmdk', None]:
        raise exception.InvalidDiskFormat(disk_format=disk_format)
    return (disk_format, disk_format == 'iso')


def get_vmdk_size_and_properties(context, image, instance):
    """Get size of the vmdk file that is to be downloaded for attach in spawn.
    Need this to create the dummy virtual disk for the meta-data file. The
    geometry of the disk created depends on the size.
    """

    LOG.debug("Getting image size for the image %s", image,
              instance=instance)
    meta_data = IMAGE_API.get(context, image)
    size, properties = meta_data["size"], meta_data["properties"]
    LOG.debug("Got image size of %(size)s for the image %(image)s",
              {'size': size, 'image': image}, instance=instance)
    return size, properties


def decide_linked_clone(image_linked_clone, global_linked_clone):
    """Explicit decision logic: whether to use linked clone on a vmdk.

    This is *override* logic not boolean logic.

    1. let the image over-ride if set at all
    2. default to the global setting

    In math terms, I need to allow:
    glance image to override global config.

    That is g vs c. "g" for glance. "c" for Config.

    So, I need  g=True vs c=False to be True.
    And, I need g=False vs c=True to be False.
    And, I need g=None vs c=True to be True.

    Some images maybe independently best tuned for use_linked_clone=True
    saving datastorage space. Alternatively a whole OpenStack install may
    be tuned to performance use_linked_clone=False but a single image
    in this environment may be best configured to save storage space and
    set use_linked_clone=True only for itself.

    The point is: let each layer of control override the layer beneath it.

    rationale:
    For technical discussion on the clone strategies and their trade-offs
    see: https://www.vmware.com/support/ws5/doc/ws_clone_typeofclone.html

    :param image_linked_clone: boolean or string or None
    :param global_linked_clone: boolean or string or None
    :return: Boolean
    """

    value = None

    # Consider the values in order of override.
    if image_linked_clone is not None:
        value = image_linked_clone
    else:
        # this will never be not-set by this point.
        value = global_linked_clone

    return strutils.bool_from_string(value)


def get_image_properties(context, instance, root_size):
    """Get the size of the flat vmdk file that is there in the storage
    repository.
    """

    image_ref = instance.image_ref
    if image_ref:
        image_info = get_vmdk_size_and_properties(context, image_ref,
                                                  instance)
    else:
        # In case the image may be booted from a volume
        image_info = (root_size, {})

    image_size, image_properties = image_info
    vmdk_file_size_in_kb = int(image_size) / 1024
    os_type = image_properties.get("vmware_ostype",
                                   constants.DEFAULT_OS_TYPE)
    adapter_type = image_properties.get("vmware_adaptertype",
                                        constants.DEFAULT_ADAPTER_TYPE)
    disk_type = image_properties.get("vmware_disktype",
                                     constants.DEFAULT_DISK_TYPE)
    # Get the network card type from the image properties
    vif_model = image_properties.get("hw_vif_model",
                                     constants.DEFAULT_VIF_MODEL)

    # Fetch the image_linked_clone data here. It is retrieved
    # with the above network based API call.
    image_linked_clone = image_properties.get("vmware_linked_clone")

    return (vmdk_file_size_in_kb, os_type, adapter_type, disk_type,
        vif_model, image_linked_clone)


def get_instance_network_info(context, instance_uuid):
    """Returns the network_info of an instance by its UUID."""
    instance = objects.Instance.get_by_uuid(context, instance_uuid)
    network_info = instance.info_cache.network_info
    vifs = []
    for vif in network_info:
        vifs.append({"id": vif['id'], "address": vif['address']})

    return vifs


def detach_instance_devices(client_factory, node_vmops, vm_ref,
                            instance, hardware_devices):
    """For each virtual device attached to the source instance, i.e.
    cdrom, detach the virtual device.
    """

    # For every device that you want to change, you have to create
    # a VirtualDeviceConfigSpec and append it as an array to
    # VirtualMachineConfigSpec.deviceChange
    detach_config_spec = client_factory.create(
            "ns0:VirtualMachineConfigSpec")
    device_config_specs = []

    detach_device_classes = ["VirtualCdrom"]
    for device in hardware_devices:
        if device.__class__.__name__ in detach_device_classes:
            virtual_device_config_spec = client_factory.create(
                    "ns0:VirtualDeviceConfigSpec")
            virtual_device_config_spec.operation = "remove"
            virtual_device_config_spec.device = device
            device_config_specs.append(virtual_device_config_spec)
        # Remove volume devices so they can be restored properly later
        elif device.__class__.__name__ == "VirtualDisk":
            virtual_device_config_spec = client_factory.create(
                    "ns0:VirtualDeviceConfigSpec")

            vmdk_file_path = device.backing.fileName
            vmdk_file_name = os.path.basename(vmdk_file_path)

            # Volume vmdk disks are prefixed with "volume-"
            if vmdk_file_name.startswith("volume-"):
                virtual_device_config_spec.operation = "remove"
                virtual_device_config_spec.device = device
                device_config_specs.append(virtual_device_config_spec)

    detach_config_spec.deviceChange = device_config_specs

    try:
        vm_util.reconfigure_vm(node_vmops._session, vm_ref,
                               detach_config_spec)
    except Exception as e:
        LOG.error(_LE("Detaching virtual device failed. "
                      "Exception: %s"), e, instance=instance)


def detach_instance_networks(client_factory, node_vmops, vm_ref, instance,
                             hardware_devices, vifs):
    """For each network interface attached to the source instance,
    delete the interface so that a new interface (with a regenerated
    MAC) can be attached.
    """
    for vif in vifs:
        port_index = vm_util.get_vm_detach_port_index(
            node_vmops._session, vm_ref, vif['id'])
        if port_index is None:
            LOG.debug("svt: No device with interface-id %s exists "
                      "on VM", vif['id'])
            continue

        device = vmware_vif.get_network_device(hardware_devices,
                                               vif['address'])
        if device is None:
            LOG.debug("svt: No device with MAC address %s exists on "
                      "the VM", vif['address'])
            continue

        detach_config_spec = vm_util.get_network_detach_config_spec(
            client_factory, device, port_index)
        LOG.debug("svt: Reconfiguring VM to detach interface",
                  instance=instance)
        try:
            vm_util.reconfigure_vm(node_vmops._session, vm_ref,
                                   detach_config_spec)
        except Exception as e:
            LOG.error(_LE("Detaching network adapter failed. "
                          "Exception: %s"), e, instance=instance)
            raise exception.InterfaceDetachFailed(
                    instance_uuid=instance['uuid'])


def get_datacenter_and_datastore(node_vmops):
    datastore = ds_util.get_datastore(node_vmops._session,
        node_vmops._cluster, datastore_regex=node_vmops._datastore_regex)
    dc_info = node_vmops.get_datacenter_ref_and_name(datastore.ref)

    # Cannot continue if there is no datastore or datacenter available
    if not datastore or not dc_info:
        LOG.error(_LE("Could not find datastore or datacenter"))
        raise exception.NotFound()

    return (dc_info, datastore)


def move_virtual_disk(session, dc_ref, source, dest):
    LOG.debug("svt: Moving virtual disk %(source)s to %(dest)s",
              {'source': source, 'dest': dest})
    vim = session._get_vim()
    vmdk_copy_task = session._call_method(
            vim,
            "MoveVirtualDisk_Task",
            vim.service_content.virtualDiskManager,
            sourceName=source,
            sourceDatacenter=dc_ref,
            destName=dest,
            force=True)
    session._wait_for_task(vmdk_copy_task)
    LOG.debug("svt: Moved virtual disk %(source)s to %(dest)s",
              {'source': source, 'dest': dest})
