
from oslo_config import cfg

from cinder.volume import configuration as conf


RED_CONNECTION_OPTS = [
    cfg.StrOpt('red_rest_protocol',
               default='https',
               choices=['http', 'https'],
               help='Red RESTful API interface protocol.'),
    cfg.IntOpt('red_rest_port',
               default=443,
               help='Red RESTful API interface port.'),
    cfg.StrOpt('red_user',
               required=True,
               default='storage_admin_001',
               help='User name to connect to Red RESTful API '
                    'interface.'),
    cfg.StrOpt('red_password',
               required=True,
               secret=True,
               help='User password to connect to Red RESTful API '
                    'interface.'),
    cfg.ListOpt('red_rest_address',
                default=[],
                help='One or more comma delimited IP addresses for management '
                     'communication with Red RESTful API interface.'),
    cfg.ListOpt('red_data_address',
                help='One or more comma delimited IP addresses for data IO.'),
    cfg.IntOpt('red_data_port',
               default=4420,
               help='Red data interface port.'),
    cfg.FloatOpt('red_rest_connect_timeout',
                 default=30,
                 help='Specifies the time limit (in seconds), within '
                      'which the connection to Red RESTful '
                      'API interface must be established.'),
    cfg.FloatOpt('red_rest_read_timeout',
                 default=300,
                 help='Specifies the time limit (in seconds), '
                      'within which Red RESTful API '
                      'interface must send a response.'),
    cfg.FloatOpt('red_rest_backoff_factor',
                 default=1,
                 help='Specifies the backoff factor to apply between '
                      'connection attempts to Red RESTful '
                      'API interface.'),
    cfg.IntOpt('red_rest_retry_count',
               default=5,
               help='Specifies the number of times to repeat Red '
                    'RESTful API calls in case of connection errors '
                    'or Red appliance retryable errors.'),
    cfg.IntOpt('red_instance_id',
               default=1,
               help='Red cluster instance ID for exposing bdevs on.'),
    cfg.StrOpt('red_nvmeof_addrfam',
               default='ipv4',
               help='Address family (IPv4, IPv6 or IB) to be used for '
                    'NVME-overFabric device.'),
    cfg.StrOpt('red_nvmeof_transport',
               default='tcp',
               help='Transport protocol to be used for '
                    'NVME-overFabric device.'),
]

RED_DATASET_OPTS = [
    cfg.IntOpt('red_bdev_blocksize',
                default=4096,
                help='Specifies the block size of a volume in bytes.'
                     'Must be a multiple of 512'),
    cfg.StrOpt('red_cluster',
               default='cinder',
               help='Red cluster where the volumes will be created.'),
    cfg.StrOpt('red_tenant',
               default='cinder',
               help='Red tenant where the volumes will be created.'),
    cfg.StrOpt('red_subtenant',
               default='cinder',
               help='Red subtenant where the volumes will be created.'),
    cfg.StrOpt('red_dataset',
               default='cinder',
               help='Red dataset where the volumes will be created.')
]

RED_NVMEOF_OPTS = (
    RED_CONNECTION_OPTS +
    RED_DATASET_OPTS
)

CONF = cfg.CONF
CONF.register_opts(RED_NVMEOF_OPTS, group=conf.SHARED_CONF_GROUP)
