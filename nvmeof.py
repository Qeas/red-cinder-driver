# Copyright 2022 DDN, Inc. All rights reserved.
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

from oslo_log import log as logging
from oslo_utils import units

from cinder import coordination
from cinder import interface
from cinder import objects
from cinder.volume import driver
from cinder.volume.drivers.red import api
from cinder.volume.drivers.red import options

LOG = logging.getLogger(__name__)


@interface.volumedriver
class RedNvmeOFDriver(driver.VolumeDriver):
    """Executes volume driver commands on Red cluster
    Version history:

    .. code-block:: none

        1.0.0 - Initial driver version.
    """

    VERSION = '1.0.0'

    vendor_name = 'Red'
    product_name = 'Red'
    storage_protocol = 'NVMeOF'
    driver_volume_type = 'nvmeof'

    def __init__(self, *args, **kwargs):
        super(RedNvmeOFDriver, self).__init__(*args, **kwargs)
        if not self.configuration:
            code = 'ENODATA'
            message = (_('%(product_name)s %(storage_protocol)s '
                         'backend configuration not found')
                       % {'product_name': self.product_name,
                          'storage_protocol': self.storage_protocol})
            raise jsonrpc.NefException(code=code, message=message)

        self.configuration.append_config_values(options.RED_NVMEOF_OPTS)
        self.ctxt = None
        self.backend_name = self._get_backend_name()
        self.driver = self.__class__.__name__
        self.red_rest_address = self.configuration.red_rest_address
        self.red_data_address = self.configuration.red_data_address
        self.red_cluster = self.configuration.red_cluster
        self.red_tenant = self.configuration.red_tenant
        self.red_subtenant = self.configuration.red_subtenant
        self.red_dataset = self.configuration.red_dataset
        self.block_size = self.configuration.red_bdev_blocksize
        self.red_instance_id = self.configuration.red_instance_id
        self.red_nvmeof_addrfam = self.configuration.red_nvmeof_addrfam
        self.red_nvmeof_transport = self.configuration.red_nvmeof_transport
        self.red_data_port = self.configuration.red_data_port
        self.path = '{cluster}/{tenant}/{subtenant}/{dataset}'.format(
            cluster=self.red_cluster, tenant=self.red_tenant,
            subtenant=self.red_subtenant, dataset=self.red_dataset)

    @staticmethod
    def get_driver_options():
        return options.RED_NVMEOF_OPTS

    def do_setup(self, ctxt):
        self.ctxt = ctxt
        retries = 0
        while not self._do_setup():
            retries += 1
            self.red.delay(retries)

    def _do_setup(self):
        try:
            self.red = api.RedProxy(
                self.driver_volume_type,
                self.red_cluster,
                self.red_tenant,
                self.red_subtenant,
                self.red_dataset,
                self.backend_name,
                self.configuration)
        except api.RedException as error:
            LOG.error('Failed to initialize RESTful API for backend '
                      '%(backend_name)s on host %(host)s: %(error)s',
                      {'backend_name': self.backend_name,
                       'host': self.host, 'error': error})
            return False
        return True

    def check_for_setup_error(self):
        """Check setup."""
        if self.block_size % 512 != 0:
            raise api.RedException(code='EBADARG', message='red_block_size'
                ' must be a multiple of 512')

    def get_volume_stats(self, refresh=False):
        """Get volume stats.

        If 'refresh' is True, run update the stats first.
        """
        if refresh or not self._stats:
            self._update_volume_stats()
        return self._stats

    def _update_volume_stats(self):
        """Retrieve stats info for Red cluster."""
        provisioned_capacity_gb = total_volumes = 0
        volumes = objects.VolumeList.get_all_by_host(self.ctxt, self.host)
        for volume in volumes:
            provisioned_capacity_gb += volume['size']
            total_volumes += 1
        max_over_subscription_ratio = (
            self.configuration.safe_get('max_over_subscription_ratio'))
        reserved_percentage = (
            self.configuration.safe_get('reserved_percentage'))
        if reserved_percentage is None:
            reserved_percentage = 0
        location_info = '%(driver)s:%(host)s:%(path)s' % {
            'driver': self.driver,
            'host': self.red_data_address,
            'path': self.path
        }
        display_name = 'Capabilities of %(product)s %(protocol)s driver' % {
            'product': self.product_name,
            'protocol': self.storage_protocol
        }
        dataset_info = self.red.datasets.get(self.red_dataset)
        if len(dataset_info) < 1:
            raise api.RedException(
                code='ENOENT',
                message='Could not get dataset %s stats' % self.red_dataset
            )
        total = dataset_info[0]['dataset']['quota'] // units.Gi
        usage = dataset_info[0]['dataset'].get('usage')
        used =  usage // units.Gi if usage else 0
        stats = {
            'backend_state': 'up',
            'driver_version': self.VERSION,
            'vendor_name': self.vendor_name,
            'storage_protocol': self.storage_protocol,
            'volume_backend_name': self.backend_name,
            'location_info': location_info,
            'display_name': display_name,
            'red_cluster': self.red_cluster,
            'red_tenant': self.red_tenant,
            'red_subtenant': self.red_subtenant,
            'red_dataset': self.red_dataset,
            'multiattach': False,
            'QoS_support': False,
            'consistencygroup_support': False,
            'consistent_group_snapshot_enabled': False,
            'online_extend_support': False,
            'sparse_copy_volume': False,
            'thin_provisioning_support': True,
            'thick_provisioning_support': False,
            'total_capacity_gb': total,
            'allocated_capacity_gb': used,
            'free_capacity_gb': total - used,
            'total_volumes': total_volumes,
            'provisioned_capacity_gb': provisioned_capacity_gb,
        }
        self._stats = stats

    def create_volume(self, volume):
        """Creates a volume.

        :param volume: volume reference
        :returns: model update dict for volume reference
        """
        volume_size = volume['size'] * units.Gi
        nblocks = int(volume_size / self.block_size)
        payload = {}
        payload['name'] = volume['name']
        payload['block_size'] = self.block_size
        payload['nblocks'] = nblocks
        self.red.bdevs.create(payload)

    def delete_volume(self, volume):
        """Deletes a volume.

        :param volume: volume reference
        """
        LOG.debug('Delete volume %s' % volume['name'])
        self.red.bdevs.delete(volume['name'])

    def create_export(self, ctxt, volume, connector):
        """Driver entry point to get the export info for a new volume."""
        pass

    def ensure_export(self, ctxt, volume):
        """Driver entry point to get the export info for an existing volume."""
        pass

    def remove_export(self, ctxt, volume):
        """Driver entry point to remove an export for a volume."""
        pass

    @coordination.synchronized('{self.red.lock}-{volume[id]}')
    def initialize_connection(self, volume, connector):
        """Allow connection to connector and return connection info.

        :param volume: volume reference
        :param connector: connector reference
        :returns: dictionary of connection information
        """
        multipath = connector.get('multipath', False)
        LOG.debug('Initialize connection for volume %(volume)s and '
                  'multipath: %(multipath)s',
                  {'volume': volume['name'], 'multipath': multipath})
        response = self.red.bdevs.get(volume['name'])
        if len(response) > 0:
            bdev_info = response[0]
        else:
            raise api.RedException(code='ENOENT', message='bdev %s'
                ' not found on RedCluster' % volume['name'])

        # If bdev not exposed, xattrs is empty -> expose bdev first
        if not self.parse_bdev_info(bdev_info):
            nvmf = {
                'transport': self.red_nvmeof_transport,
                'addrfam': self.red_nvmeof_addrfam,
                'ipaddrs': self.red_data_address,
                'port': self.red_data_port,
            }
            bdev_info = self.red.bdevs.expose(volume['name'], nvmf)

        vol_uuid = bdev_info.get('uuid')
        bdev_info = self.parse_bdev_info(bdev_info, multipath)
        if not bdev_info:
            raise api.RedException(
                code='ENOENT', message='no RED_INTERNAL_NVMEOF in xattrs')
        conn_info = {
            'driver_volume_type': self.driver_volume_type,
            'data': {
                'transport_type': self.red_nvmeof_transport,
                'nqn': bdev_info['nqn'],
                'target_portal': bdev_info['ipaddress'],
                'target_port': self.red_data_port,
                'ns_id': str(bdev_info.get('namespaceId')),
                'vol_uuid': vol_uuid,
            }
        }
        return conn_info

    def parse_bdev_info(self, bdev_info, multipath=False):
        xattrs = bdev_info.get('xattrs')
        if not xattrs:
            return None
        nvmeof = xattrs.get('RED_INTERNAL_NVMEOF')

        # TODO: multipath
        return nvmeof[0] if nvmeof else None

    @coordination.synchronized('{self.red.lock}-{volume[id]}')
    def terminate_connection(self, volume, connector, **kwargs):
        """Terminate a connection to a volume.

        :param volume: a volume object
        :param connector: a connector object
        :returns: dictionary of connection information
        """
        try:
            self.red.bdevs.unexpose(volume['name'])
        except api.RedException as error:
            LOG.warning('Error when trying to unexpose the volume %s', error)
        conn_info = {'driver_volume_type': self.driver_volume_type, 'data': {}}
        return conn_info

    def _get_backend_name(self):
        backend_name = self.configuration.safe_get('volume_backend_name')
        if not backend_name:
            LOG.error('Failed to get configured volume backend name')
            backend_name = '%(product)s_%(protocol)s' % {
                'product': self.product_name,
                'protocol': self.storage_protocol
            }
        return backend_name

    def create_consistencygroup(self, ctxt, group):
        """Creates a consistency group.

        :param ctxt: the context of the caller.
        :param group: the dictionary of the consistency group to be created.
        :returns: group_model_update
        """
        group_model_update = {}
        return group_model_update

    def create_group(self, ctxt, group):
        """Creates a group.

        :param ctxt: the context of the caller.
        :param group: the group object.
        :returns: model_update
        """
        return self.create_consistencygroup(ctxt, group)

    def delete_consistencygroup(self, ctxt, group, volumes):
        """Deletes a consistency group.

        :param ctxt: the context of the caller.
        :param group: the dictionary of the consistency group to be deleted.
        :param volumes: a list of volume dictionaries in the group.
        :returns: group_model_update, volumes_model_update
        """
        group_model_update = {}
        volumes_model_update = []
        for volume in volumes:
            self.delete_volume(volume)
        return group_model_update, volumes_model_update

    def delete_group(self, ctxt, group, volumes):
        """Deletes a group.

        :param ctxt: the context of the caller.
        :param group: the group object.
        :param volumes: a list of volume objects in the group.
        :returns: model_update, volumes_model_update
        """
        return self.delete_consistencygroup(ctxt, group, volumes)

    def update_consistencygroup(self, ctxt, group, add_volumes=None,
                                remove_volumes=None):
        """Updates a consistency group.

        :param ctxt: the context of the caller.
        :param group: the dictionary of the consistency group to be updated.
        :param add_volumes: a list of volume dictionaries to be added.
        :param remove_volumes: a list of volume dictionaries to be removed.
        :returns: group_model_update, add_volumes_update, remove_volumes_update
        """
        group_model_update = {}
        add_volumes_update = []
        remove_volumes_update = []
        return group_model_update, add_volumes_update, remove_volumes_update

    def update_group(self, ctxt, group, add_volumes=None,
                     remove_volumes=None):
        """Updates a group.

        :param ctxt: the context of the caller.
        :param group: the group object.
        :param add_volumes: a list of volume objects to be added.
        :param remove_volumes: a list of volume objects to be removed.
        :returns: model_update, add_volumes_update, remove_volumes_update
        """
        return self.update_consistencygroup(ctxt, group, add_volumes,
                                            remove_volumes)

    def extend_volume(self, volume, new_size):
        """Extend an existing volume.

        :param volume: volume reference
        :param new_size: volume new size in GB
        """
        volume_size = new_size * units.Gi
        nblocks = int(volume_size / self.block_size)
        payload = {}
        payload['bdevNblocks'] = nblocks
        payload['bdevCapacity'] = volume_size
        self.red.bdevs.resize(volume['name'], payload)
