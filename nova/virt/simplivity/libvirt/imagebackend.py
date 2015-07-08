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

import abc
import os

from oslo.config import cfg
from nova.i18n import _, _LE

from nova import exception
from nova import utils
from nova.openstack.common import fileutils
from nova.openstack.common import log as logging
from nova.virt import images
from nova.virt.disk import api as disk
from nova.virt.libvirt import config as vconfig
from nova.virt.libvirt import utils as libvirt_utils
from nova.virt.simplivity.libvirt import utils as svt_utils
from nova.virt.simplivity.libvirt import common as svt_common

try:
    import rbd
except ImportError:
    rbd = None

CONF = cfg.CONF
CONF.import_opt('preallocate_images', 'nova.virt.driver')

LOG = logging.getLogger(__name__)

BASE_DIR_NAME = "_base"


class Image(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, source_type, driver_format, is_block_dev=False):
        """Image initialization.

        :source_type: file
        :driver_format: raw or qcow2
        :is_block_dev:
        """
        LOG.debug('svt: __init__')
        self.source_type = source_type
        self.driver_format = driver_format
        self.is_block_dev = is_block_dev
        self.preallocate = False

        # NOTE(mikal): We need a lock directory which is shared along with
        # instance files, to cover the scenario where multiple compute nodes
        # are trying to create a base file at the same time
        self.lock_path = os.path.join(CONF.instances_path, 'locks')

    @abc.abstractmethod
    def create_image(self, prepare_template, base, size, *args, **kwargs):
        """Create image from template.

        Contains specific behavior for each image type.

        :prepare_template: function, that creates template.
        Should accept `target` argument.
        :base: Template name
        :size: Size of created image in bytes
        """
        pass

    def libvirt_info(self, disk_bus, disk_dev, device_type, cache_mode,
            extra_specs, hypervisor_version):
        """Get `LibvirtConfigGuestDisk` filled for this image.

        :disk_dev: Disk bus device name
        :disk_bus: Disk bus type
        :device_type: Device type for this image.
        :cache_mode: Caching mode for this image
        :extra_specs: Instance type extra specs dict.
        """
        LOG.debug('svt: libvirt_info')
        info = vconfig.LibvirtConfigGuestDisk()
        info.source_type = self.source_type
        info.source_device = device_type
        info.target_bus = disk_bus
        info.target_dev = disk_dev
        info.driver_cache = cache_mode
        info.driver_format = self.driver_format
        driver_name = libvirt_utils.pick_disk_driver_name(hypervisor_version,
                                                          self.is_block_dev)
        info.driver_name = driver_name
        info.source_path = self.path

        tune_items = ['disk_read_bytes_sec', 'disk_read_iops_sec',
            'disk_write_bytes_sec', 'disk_write_iops_sec',
            'disk_total_bytes_sec', 'disk_total_iops_sec']
        # Note(yaguang): Currently, the only tuning available is Block I/O
        # throttling for qemu.
        if self.source_type in ['file', 'block']:
            for key, value in extra_specs.iteritems():
                scope = key.split(':')
                if len(scope) > 1 and scope[0] == 'quota':
                    if scope[1] in tune_items:
                        setattr(info, scope[1], value)
        return info

    def check_image_exists(self):
        return os.path.exists(self.path)

    def cache(self, fetch_func, filename, size=None, *args, **kwargs):
        """Creates image from template.

        Ensures that neither template nor image exists.
        Ensures that base directory exists.
        Synchronizes on template fetching.

        :fetch_func: Function that creates the base image
                     Should accept `target` argument.
        :filename: Name of the file in the image directory
        :size: Size of created image in bytes (optional)
        """
        @utils.synchronized(filename, external=True, lock_path=self.lock_path)
        def call_if_not_exists(target, *args, **kwargs):
            # In order for the fetch_func to be called, we must remove
            # instance_id and nfs_share from kwargs instead of modifying
            # the fetch_func.

            # instance_id and nfs_share may not exist if we are creating an
            # ephemeral disk
            if kwargs.get('instance_id') is not None:
                kwargs.pop('instance_id')
            if kwargs.get('nfs_share') is not None:
                kwargs.pop('nfs_share')

            if not os.path.exists(target):
                fetch_func(target=target, *args, **kwargs)
            elif CONF.libvirt.images_type == "lvm" and \
                    'ephemeral_size' in kwargs:
                fetch_func(target=target, *args, **kwargs)

        LOG.debug('svt: cache')

        # Path to image store directory
        base_dir = os.path.join(CONF.instances_path,
                                CONF.simplivity.svt_image_store)
        if (CONF.libvirt.images_type == "default"
                or CONF.libvirt.images_type == "qcow2"):
            base_dir = os.path.join(CONF.instances_path, BASE_DIR_NAME)

        if not os.path.exists(base_dir):
            fileutils.ensure_tree(base_dir)
        base = os.path.join(base_dir, filename)

        # base = Path to image in _base
        # filename = Image name
        # self.path = Path to VM root disk
        LOG.debug('svt: path=%s. filename=%s. base=%s.' %
                  (self.path, filename, base))

        if not self.check_image_exists() or not os.path.exists(base):
            self.create_image(call_if_not_exists, base, size,
                              *args, **kwargs)

        if (size and self.preallocate and self._can_fallocate() and
                os.access(self.path, os.W_OK)):
            utils.execute('fallocate', '-n', '-l', size, self.path)

    def _can_fallocate(self):
        """Check once per class, whether fallocate(1) is available,
           and that the instances directory supports fallocate(2).
        """
        LOG.debug('svt: _can_fallocate')
        can_fallocate = getattr(self.__class__, 'can_fallocate', None)
        if can_fallocate is None:
            _out, err = utils.trycmd('fallocate', '-n', '-l', '1',
                                     self.path + '.fallocate_test')
            fileutils.delete_if_exists(self.path + '.fallocate_test')
            can_fallocate = not err
            self.__class__.can_fallocate = can_fallocate
            if not can_fallocate:
                LOG.error('Unable to preallocate_images=%s at path: %s',
                          (CONF.preallocate_images, self.path))
        return can_fallocate

    @staticmethod
    def verify_base_size(base, size, base_size=0):
        """Check that the base image is not larger than size.
           Since images can't be generally shrunk, enforce this
           constraint taking account of virtual image size.
        """

        # Note(pbrady): The size and min_disk parameters of a glance
        #  image are checked against the instance size before the image
        #  is even downloaded from glance, but currently min_disk is
        #  adjustable and doesn't currently account for virtual disk size,
        #  so we need this extra check here.
        # NOTE(cfb): Having a flavor that sets the root size to 0 and having
        #  nova effectively ignore that size and use the size of the
        #  image is considered a feature at this time, not a bug.

        LOG.debug('svt: verify_base_size')
        if size is None:
            return

        if size and not base_size:
            base_size = disk.get_disk_size(base)

        if size < base_size:
            LOG.error(_LE('%(base)s virtual size %(base_size)s larger than'
                          ' flavor root disk size %(size)s') %
                      {'base': base,
                       'base_size': base_size,
                       'size': size})
            raise exception.InstanceTypeDiskTooSmall()

    def snapshot_create(self):
        raise NotImplementedError()

    def snapshot_extract(self, target, out_format):
        raise NotImplementedError()

    def snapshot_delete(self):
        raise NotImplementedError()


class Raw(Image):
    def __init__(self, instance=None, disk_name=None, path=None,
                 snapshot_name=None):
        LOG.debug('svt: __init__')
        super(Raw, self).__init__("file", "raw", is_block_dev=False)

        # Path to VM root disk
        self.path = (path or
                     os.path.join(libvirt_utils.get_instance_path(instance),
                                  disk_name))
        self.disk_name = disk_name
        self.snapshot_name = snapshot_name
        self.preallocate = CONF.preallocate_images != 'none'
        self.correct_format()

    def correct_format(self):
        LOG.debug('svt: correct_format')
        if os.path.exists(self.path):
            data = images.qemu_img_info(self.path)
            self.driver_format = data.file_format or 'raw'

    def create_image(self, prepare_template, base, size, *args, **kwargs):
        @utils.synchronized(base, external=True, lock_path=self.lock_path)
        def copy_raw_image(base, target, size):
            # Copy base image to VM container
            libvirt_utils.copy_image(base, target)
            if size:
                # class Raw is misnamed, format may not be 'raw' in all cases
                use_cow = self.driver_format == 'qcow2'
                disk.extend(target, size, use_cow=use_cow)

        def zero_copy_image(base, target, remote_src, remote_tgt,
                            size):
            LOG.debug('svt: zero_copy_image')

            # Establish connection to virtual controller
            svt_connection = svt_common.Connection(
                CONF.simplivity.svt_vc_host, CONF.simplivity.svt_vc_username,
                CONF.simplivity.svt_vc_password)

            LOG.debug('svt: target=%s. remote_src=%s. remote_tgt=%s.' %
                      (target, remote_src, remote_tgt))
            svt_utils.zero_copy_file(svt_connection, remote_src, remote_tgt)
            if size:
                # Extend disk size (if needed)
                use_cow = self.driver_format == 'qcow2'
                disk.extend(target, size, use_cow=use_cow)

        """
        Copy base image to the VM container
            base = Path to image under _base
            self.path = Path to VM's root disk
        """
        LOG.debug('svt: create_image')

        LOG.debug('svt: base=%s. self.path=%s.' % (base, self.path))
        generating = 'image_id' not in kwargs
        if generating:
            # Generating image in place
            prepare_template(target=self.path, *args, **kwargs)
        else:
            prepare_template(target=base, max_size=size, *args, **kwargs)

            self.verify_base_size(base, size)
            if not os.path.exists(self.path):
                with fileutils.remove_path_on_error(self.path):
                    nfs_share = kwargs['nfs_share']
                    (address, remote_share) = nfs_share.split(':')
                    LOG.debug('svt: address=%s. remote_share=%s' %
                              (address, remote_share))

                    # Generate path to image and instance in virtual controller
                    remote_image = os.path.join(
                        remote_share, CONF.simplivity.svt_image_store,
                        kwargs['image_id'])
                    remote_instance = os.path.join(remote_share,
                                                   kwargs['instance_id'],
                                                   self.disk_name)

                    zero_copy_image(base, self.path, remote_image,
                                    remote_instance, size)
        self.correct_format()

    def snapshot_create(self):
        pass

    def snapshot_extract(self, target, out_format):
        images.convert_image(self.path, target, out_format)

    def snapshot_delete(self):
        pass


class Qcow2(Image):
    def __init__(self, instance=None, disk_name=None, path=None,
                 snapshot_name=None):
        super(Qcow2, self).__init__("file", "qcow2", is_block_dev=False)

        # Path to VM root disk
        self.path = (path or
                     os.path.join(libvirt_utils.get_instance_path(instance),
                                  disk_name))
        self.disk_name = disk_name
        self.snapshot_name = snapshot_name
        self.preallocate = CONF.preallocate_images != 'none'

    def create_image(self, prepare_template, base, size, *args, **kwargs):
        @utils.synchronized(base, external=True, lock_path=self.lock_path)
        def copy_qcow2_image(base, target, size):
            # TODO(pbrady): Consider copying the cow image here
            # with preallocation=metadata set for performance reasons.
            # This would be keyed on a 'preallocate_images' setting.
            libvirt_utils.create_cow_image(base, target)
            if size:
                disk.extend(target, size, use_cow=True)

        # Download the unmodified base image unless we already have a copy.
        if not os.path.exists(base):
            prepare_template(target=base, max_size=size, *args, **kwargs)
        else:
            self.verify_base_size(base, size)

        legacy_backing_size = None
        legacy_base = base

        # Determine whether an existing qcow2 disk uses a legacy backing by
        # actually looking at the image itself and parsing the output of the
        # backing file it expects to be using.
        if os.path.exists(self.path):
            backing_path = libvirt_utils.get_disk_backing_file(self.path)
            if backing_path is not None:
                backing_file = os.path.basename(backing_path)
                backing_parts = backing_file.rpartition('_')
                if backing_file != backing_parts[-1] and \
                        backing_parts[-1].isdigit():
                    legacy_backing_size = int(backing_parts[-1])
                    legacy_base += '_%d' % legacy_backing_size
                    legacy_backing_size *= 1024 * 1024 * 1024

        # Create the legacy backing file if necessary.
        if legacy_backing_size:
            if not os.path.exists(legacy_base):
                with fileutils.remove_path_on_error(legacy_base):
                    libvirt_utils.copy_image(base, legacy_base)
                    disk.extend(legacy_base, legacy_backing_size, use_cow=True)

        if not os.path.exists(self.path):
            with fileutils.remove_path_on_error(self.path):
                copy_qcow2_image(base, self.path, size)

    def snapshot_create(self):
        libvirt_utils.create_snapshot(self.path, self.snapshot_name)

    def snapshot_extract(self, target, out_format):
        libvirt_utils.extract_snapshot(self.path, 'qcow2',
                                       self.snapshot_name, target,
                                       out_format)

    def snapshot_delete(self):
        libvirt_utils.delete_snapshot(self.path, self.snapshot_name)


class Backend(object):
    def __init__(self, use_cow):
        self.BACKEND = {
            'raw': Raw,
            'qcow2': Qcow2,
            'default': Qcow2 if use_cow else Raw
        }

    def backend(self, image_type=None):
        if not image_type:
            image_type = CONF.libvirt.images_type
        image = self.BACKEND.get(image_type)
        if not image:
            raise RuntimeError(_('Unknown image_type=%s') % image_type)
        return image

    def image(self, instance, disk_name, image_type=None):
        """Constructs image for selected backend

        :instance: Instance name.
        :name: Image name.
        :image_type: Image type.
        Optional, is CONF.libvirt.images_type by default.
        """
        backend = self.backend(image_type)
        return backend(instance=instance, disk_name=disk_name)

    def snapshot(self, disk_path, snapshot_name, image_type=None):
        """Returns snapshot for given image

        :path: path to image
        :snapshot_name: snapshot name
        :image_type: type of image
        """
        backend = self.backend(image_type)
        return backend(path=disk_path, snapshot_name=snapshot_name)
