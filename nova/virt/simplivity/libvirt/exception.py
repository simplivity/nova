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

from nova.i18n import _

from nova import exception


class SVTOperationNotSupported(exception.NovaException):
    msg_fmt = _('Requested operation is not supported')


class SVTConnectionFailed(exception.NovaException):
    msg_fmt = _('Failed to establish connection with virtual controller')


class SVTShareNotFound(exception.NovaException):
    msg_fmt = _('Could not find NFS exported share')


class SVTVMAssociateFailed(exception.NovaException):
    msg_fmt = _('Failed to associate a container with a virtual machine')


class SVTZeroCopyFailed(exception.NovaException):
    msg_fmt = _('Failed to copy file')


class SVTMoveFailed(exception.NovaException):
    msg_fmt = _('Failed to move file')


class SVTBackupInfoNotFound(exception.NovaException):
    msg_fmt = _('Could not find backup info')


class SVTRestoreFailed(exception.NovaException):
    msg_fmt = _('Failed to restore instance from backup')


class SVTBackupFailed(exception.NovaException):
    msg_fmt = _('Failed to backup instance')


class SVTBackupDeleteFailed(exception.NovaException):
    msg_fmt = _('Failed to delete instance backup')
