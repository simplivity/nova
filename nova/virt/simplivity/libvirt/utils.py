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
from nova.i18n import _
from nova.i18n import _LE

from nova import exception
from nova import utils
from nova.image import glance
from nova.virt import images
from nova.openstack.common import fileutils
from nova.openstack.common import processutils
from nova.openstack.common import log as logging
from nova.virt.simplivity.libvirt import exception as svt_exception

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

SRC_APPSETUP = "export SVTCLI_TESTMODE=1; source /var/tmp/build/bin/appsetup"


def execute(*args, **kwargs):
    return utils.execute(*args, **kwargs)


def fetch_image(context, target, image_id, user_id, project_id, max_size=0):
    """Grab image"""
    fetch_to_raw(context, image_id, target, user_id, project_id,
                 max_size=max_size)


def fetch_to_raw(context, image_href, path, user_id, project_id, max_size=0):
    path_tmp = "%s.part" % path  # Temporary path where image is stored
    fetch(context, image_href, path_tmp, user_id, project_id,
          max_size=max_size)

    with fileutils.remove_path_on_error(path_tmp):
        data = images.qemu_img_info(path_tmp)

        fmt = data.file_format
        if fmt is None:
            raise exception.ImageUnacceptable(
                reason=_("'qemu-img info' parsing failed."),
                image_id=image_href)

        backing_file = data.backing_file
        if backing_file is not None:
            raise exception.ImageUnacceptable(image_id=image_href,
                reason=(_("fmt=%(fmt)s backed by: %(backing_file)s") %
                        {'fmt': fmt, 'backing_file': backing_file}))

        # We can't generally shrink incoming images, so disallow
        # images > size of the flavor we're booting.  Checking here avoids
        # an immediate DoS where we convert large qcow images to raw
        # (which may compress well but not be sparse).
        disk_size = data.virtual_size
        if max_size and max_size < disk_size:
            LOG.error(_('%(base)s virtual size %(disk_size)s larger than'
                        ' flavor root disk size %(size)s') %
                      {'base': path,
                       'disk_size': disk_size,
                       'size': max_size})
            raise exception.InstanceTypeDiskTooSmall()

        LOG.debug('Svt: fmt=%s. force_raw_images=%s.' %
                  (fmt, CONF.force_raw_images))
        if fmt != "raw" and CONF.force_raw_images:
            staged = "%s.converted" % path
            LOG.debug("%s was %s, converting to raw" % (image_href, fmt))
            with fileutils.remove_path_on_error(staged):
                images.convert_image(path_tmp, staged, 'raw')
                os.unlink(path_tmp)

                data = images.qemu_img_info(staged)
                if data.file_format != "raw":
                    raise exception.ImageUnacceptable(image_id=image_href,
                        reason=_("Converted to raw, but format is now %s") %
                        data.file_format)

                os.rename(staged, path)
        else:
            LOG.debug('Svt: Renamed %s to %s' % (path_tmp, path))
            os.rename(path_tmp, path)


def fetch(context, image_href, path, _user_id, _project_id, max_size=0):
    (image_service, image_id) = glance.get_remote_image_service(context,
                                                                image_href)

    # Query info about image, i.e. location, type, metadata, and properties
    LOG.debug('Svt: image_id=%s. dst_path=%s.' % (image_id, path))
    with fileutils.remove_path_on_error(path):
        image_service.download(context, image_id, dst_path=path)


def get_instance_path(instance, forceold=False, relative=False):
    """Determine the correct path for instance storage.

    This method determines the directory name for instance storage, while
    handling the fact that we changed the naming style to something more
    unique in the grizzly release.

    :param instance: the instance we want a path for
    :param forceold: force the use of the pre-grizzly format
    :param relative: if True, just the relative path is returned

    :returns: a path to store information about that instance
    """
    pre_grizzly_name = os.path.join(CONF.instances_path, instance['name'])
    if forceold or os.path.exists(pre_grizzly_name):
        if relative:
            return instance['name']
        return pre_grizzly_name

    if relative:
        return instance['uuid']
    return os.path.join(CONF.instances_path, instance['uuid'])


def write_to_file(path, contents, umask=None):
    """Write the given contents to a file

    :param path: Destination file
    :param contents: Desired contents of the file
    :param umask: Umask to set when creating this file (will be reset)
    """
    if umask:
        saved_umask = os.umask(umask)

    try:
        with open(path, 'w') as f:
            f.write(contents)
    finally:
        if umask:
            os.umask(saved_umask)


def zero_copy_file(connection, remote_src, remote_tgt):
    """Use SimpliVity zero-copy to copy file"""
    LOG.debug('Svt: zero_copy_file')

    # Do not try to copy to yourself
    if remote_src == remote_tgt:
        return

    try:
        cmd = _('cp %s %s' % (remote_src, remote_tgt))
        stdout, stderr = connection.ssh_execute(cmd)
    except processutils.ProcessExecutionError as e:
        LOG.error('Copying file failed with error: %s', e.stderr)
        raise svt_exception.SVTZeroCopyFailed()


def backup_delete(connection, instance_id, backup_name):
    """
    Delete a previously saved backup

    @param connection: Represents an SSHClient object to the virtual controller
    @param instance_id: The id of the new instance
    @param backup_name: The backup being copied
    """
    try:
        cmd = _('%s && svt-backup-delete --datastore %s --vm %s --backup %s'
                % (SRC_APPSETUP, CONF.simplivity.svt_datastore_name,
                   instance_id, backup_name))
        stdout, stderr = connection.ssh_execute(cmd)
    except processutils.ProcessExecutionError as e:
        LOG.error('Deleting backup failed with error: %s', e.stderr)
        raise svt_exception.SVTBackupDeleteFailed()


def register_instance(connection, instance):
    """
    Register instance with SimpliVity virtual controller

    @param connection: Represents an SSHClient object to the virtual controller
    @param instance: Represents an instance object
    """
    try:
        cmd = _('%s && csp-vm-associate --uuid %s --datastore %s'
                ' --container %s' %
                (SRC_APPSETUP, instance['uuid'],
                 CONF.simplivity.svt_datastore_name,
                 instance['uuid']))
        stdout, stderr = connection.ssh_execute(cmd)
    except processutils.ProcessExecutionError as e:
        LOG.error('Registering instance failed with error: %s', e.stderr)
        raise svt_exception.SVTVMAssociateFailed()


def backup_restore(connection, instance_id, datastore_name,
                   backup_instance_id, backup_name, src_datacenter=None,
                   dest_datacenter=None, dest_datastore_name=None):
    """Restore an instance from a SimpliVity backup."""
    if (src_datacenter is not None and dest_datacenter is not None and
        dest_datastore_name is not None):
        # Restore a backup from a different datastore
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

    cmd = SRC_APPSETUP + " && " + restore_cmd

    try:
        stdout, stderr = connection.ssh_execute(cmd)
    except processutils.ProcessExecutionError as e:
        LOG.error(_LE("Restoring instance failed with error: %s") %
                  e.stderr)
        raise svt_exception.SVTRestoreFailed()


def vm_backup(connection, instance_id, backup_name):
    """
    Save the state of a VM at a point in time

    @param connection: Represents an SSHClient object to the virtual controller
    @param instance_id: Instance id on which to perform the backup
    @param backup_name: The name for the backup
    """
    try:
        cmd = _('%s && svt-vm-backup --datastore %s --vm %s --name %s'
                % (SRC_APPSETUP, CONF.simplivity.svt_datastore_name,
                   instance_id, backup_name))
        stdout, stderr = connection.ssh_execute(cmd)
    except processutils.ProcessExecutionError as e:
        LOG.error('Backing up instance failed with error: %s', e.stderr)
        raise svt_exception.SVTBackupFailed()


def vm_restore(connection, instance_id, backup_name):
    """
    Restore the state of a VM at a point in time

    @param connection: Represents an SSHClient object to the virtual controller
    @param instance_id: Instance id on which to perform the restore
    @param backup_name: The name for the backup
    """
    try:
        cmd = _('%s && svt-vm-restore --datastore %s --vm %s --backup %s '
                '--force' % (SRC_APPSETUP, CONF.simplivity.svt_datastore_name,
                             instance_id, backup_name))
        stdout, stderr = connection.ssh_execute(cmd)
    except processutils.ProcessExecutionError as e:
        LOG.error('Restoring up instance failed with error: %s', e.stderr)
        raise svt_exception.SVTRestoreFailed()


def move_file(connection, remote_src, remote_tgt):
    """Move a file to a given path"""
    # Do not try to move to yourself
    if remote_src == remote_tgt:
        return

    try:
        """
        Copy file over and delete it later
        """
        cmd = _('cp %s %s' % (remote_src, remote_tgt))
        stdout, stderr = connection.ssh_execute(cmd)

        cmd = _('rm -rf %s' % remote_src)
        stdout, stderr = connection.ssh_execute(cmd)
    except processutils.ProcessExecutionError as e:
        LOG.error('Moving file failed with error: %s', e.stderr)
        raise svt_exception.SVTMoveFailed()


def create_image(disk_format, path, size):
    """Create a disk image

    :param disk_format: Disk image format (as known by qemu-img)
    :param path: Desired location of the disk image
    :param size: Desired size of disk image. May be given as an int or
                 a string. If given as an int, it will be interpreted
                 as bytes. If it's a string, it should consist of a number
                 with an optional suffix ('K' for Kibibytes,
                 M for Mebibytes, 'G' for Gibibytes, 'T' for Tebibytes).
                 If no suffix is given, it will be interpreted as bytes.
    """
    execute('qemu-img', 'create', '-f', disk_format, path, size)


def create_cow_image(backing_file, path, size=None):
    """Create COW image

    Creates a COW image with the given backing file

    :param backing_file: Existing image on which to base the COW image
    :param path: Desired location of the COW image
    """
    base_cmd = ['qemu-img', 'create', '-f', 'qcow2']
    cow_opts = []
    if backing_file:
        cow_opts += ['backing_file=%s' % backing_file]
        base_details = images.qemu_img_info(backing_file)
    else:
        base_details = None
    if base_details and base_details.cluster_size is not None:
        cow_opts += ['cluster_size=%s' % base_details.cluster_size]
    # For now don't inherit this due the following discussion...
    # See: http://www.gossamer-threads.com/lists/openstack/dev/10592
    # if 'preallocation' in base_details:
    #     cow_opts += ['preallocation=%s' % base_details['preallocation']]
    if base_details and base_details.encryption:
        cow_opts += ['encryption=%s' % base_details.encryption]
    if size is not None:
        cow_opts += ['size=%s' % size]
    if cow_opts:
        # Format as a comma separated list
        csv_opts = ",".join(cow_opts)
        cow_opts = ['-o', csv_opts]
    cmd = base_cmd + cow_opts + [path]
    execute(*cmd)
