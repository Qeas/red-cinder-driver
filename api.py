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

import hashlib
import json
import posixpath

from eventlet import greenthread
from oslo_log import log as logging
from pkg_resources import parse_version
import requests
import six

from cinder import exception
from cinder.i18n import _

LOG = logging.getLogger(__name__)

ASYNC_WAIT = 0.25
HTTPS_PORT = 8443
HTTP_PORT = 8080
HTTPS = 'https'
HTTP = 'http'
AUTO = 'auto'
NVMEOF = 'nvmeof'


class RedException(exception.VolumeDriverException):
    def __init__(self, data=None, **kwargs):
        defaults = {
            'name': 'redError',
            'code': 'EBADMSG',
            'source': 'CinderDriver',
            'detail': 'Unknown error'
        }
        if isinstance(data, dict):
            for key in defaults:
                if key in kwargs:
                    continue
                if key in data:
                    kwargs[key] = data[key]
                else:
                    kwargs[key] = defaults[key]
        elif isinstance(data, six.string_types):
            if 'detail' not in kwargs:
                kwargs['detail'] = data
        for key in defaults:
            if key not in kwargs:
                kwargs[key] = defaults[key]
        message = ('%(detail)s (source: %(source)s, '
                   'name: %(name)s, code: %(code)s)') % kwargs
        self.code = kwargs['code']
        del kwargs['detail']
        super(RedException, self).__init__(message)


class RedRequest(object):
    def __init__(self, proxy, method):
        self.proxy = proxy
        self.method = method
        self.attempts = proxy.retries + 1
        self.payload = None
        self.error = None
        self.path = None
        self.time = 0
        self.wait = 0
        self.data = []
        self.stat = {}
        self.hooks = {
            'response': self.hook
        }
        self.kwargs = {
            'hooks': self.hooks,
            'timeout': self.proxy.timeout
        }

    def __call__(self, path, payload=None):
        info = '%(method)s %(url)s %(payload)s' % {
            'method': self.method,
            'url': self.proxy.url(path),
            'payload': payload
        }
        LOG.debug('Start request: %(info)s', {'info': info})
        self.path = path
        self.payload = payload
        for attempt in range(self.attempts):
            if self.error:
                self.delay(attempt)
                if not self.find_host():
                    continue
                LOG.debug('Retry request %(info)s after %(attempt)s '
                          'failed attempts, maximum retry attempts '
                          '%(attempts)s, reason: %(error)s',
                          {'info': info, 'attempt': attempt,
                           'attempts': self.attempts,
                           'error': self.error})
            self.data = []
            try:
                response = self.request(self.method, self.path, self.payload)
            except Exception as error:
                if isinstance(error, RedException):
                    self.error = error
                else:
                    code = 'EAGAIN'
                    message = six.text_type(error)
                    self.error = RedException(message, code=code)
                LOG.error('Failed request %(info)s: %(error)s',
                          {'info': info, 'error': self.error})
                continue
            count = sum(self.stat.values())
            LOG.debug('Finish request %(info)s, '
                      'response time: %(time)s seconds, '
                      'wait time: %(wait)s seconds, '
                      'requests count: %(count)s, '
                      'requests statistics: %(stat)s, '
                      'response content: %(content)s',
                      {'info': info, 'time': self.time,
                       'wait': self.wait, 'count': count,
                       'stat': self.stat,
                       'content': response.content})
            content = None
            if response.content:
                content = json.loads(response.content)
            if not response.ok:
                LOG.error('Failed request %(info)s, '
                          'response content: %(content)s',
                          {'info': info, 'content': content})
                self.error = RedException(content)
                continue
            if response.status_code == requests.codes.created and 'data' in content:
                return content['data']
            if isinstance(content, dict) and 'data' in content:
                return self.data
            return content
        LOG.error('Failed request %(info)s, '
                  'reached maximum retry attempts: '
                  '%(attempts)s, reason: %(error)s',
                  {'info': info, 'attempts': self.attempts,
                   'error': self.error})
        raise self.error

    def request(self, method, path, payload):
        if self.method not in ['get', 'delete', 'put', 'post']:
            code = 'EINVAL'
            message = (_('Request method %(method)s not supported')
                       % {'method': self.method})
            raise RedException(code=code, message=message)
        if not path:
            code = 'EINVAL'
            message = _('Request path is required')
            raise RedException(code=code, message=message)
        url = self.proxy.url(path)
        kwargs = dict(self.kwargs)
        if payload:
            if not isinstance(payload, dict):
                code = 'EINVAL'
                message = _('Request payload must be a dictionary')
                raise RedException(code=code, message=message)
            if method in ['get', 'delete']:
                kwargs['params'] = payload
            elif method in ['put', 'post']:
                kwargs['data'] = json.dumps(payload)
        return self.proxy.session.request(method, url, **kwargs)

    def hook(self, response, **kwargs):
        info = (_('session request %(method)s %(url)s %(body)s '
                  'and session response %(code)s %(content)s')
                % {'method': response.request.method,
                   'url': response.request.url,
                   'body': response.request.body,
                   'code': response.status_code,
                   'content': response.content})
        LOG.debug('Start request hook on %(info)s', {'info': info})
        if response.status_code not in self.stat:
            self.stat[response.status_code] = 0
        self.stat[response.status_code] += 1
        self.time += response.elapsed.total_seconds()
        attempt = self.stat[response.status_code]
        limit = len(self.proxy.hosts) * self.attempts
        if response.ok and not response.content:
            return response
        try:
            content = json.loads(response.content)
        except (TypeError, ValueError) as error:
            code = 'EINVAL'
            message = (_('Failed request hook on %(info)s: '
                         'JSON parser error: %(error)s')
                       % {'info': info, 'error': error})
            raise RedException(code=code, message=message)
        if response.ok and content is None:
            return response
        if not isinstance(content, dict):
            code = 'EINVAL'
            message = (_('Failed request hook on %(info)s: '
                         'no valid content found')
                       % {'info': info})
            raise RedException(code=code, message=message)
        if attempt > limit and not response.ok:
            return response
        method = 'get'
        if response.status_code == requests.codes.unauthorized:
            if not self.auth():
                raise RedException(content)
            request = response.request.copy()
            request.headers.update(self.proxy.session.headers)
            return self.proxy.session.send(request, **kwargs)
        elif response.status_code == requests.codes.not_found:
            if content.get('title') == 'Delete Bdev' and (
                'Failed to lookup' in content.get('detail')):
                code = requests.codes.not_found
                message = content.get('detail')
                raise RedException(code='ENOENT', message=message)
            if not self.check_host():
                raise RedException(content)
            return response
        elif response.status_code == requests.codes.server_error:
            if 'code' in content and content['code'] == 'EBUSY':
                raise RedException(content)
            return response
        elif response.status_code == requests.codes.accepted:
            path, payload = self.parse(content, 'monitor')
            if not path:
                code = 'ENODATA'
                message = (_('Failed request hook on %(info)s: '
                             'monitor path not found')
                           % {'info': info})
                raise RedException(code=code, message=message)
            self.delay(attempt, sync=False)
            return self.request(method, path, payload)
        elif response.status_code == requests.codes.ok:
            if 'data' not in content or not content['data']:
                LOG.debug('Finish request hook on %(info)s: '
                          'non-paginated content',
                          {'info': info})
                return response
            data = content['data']
            count = len(data)
            LOG.debug('Continue request hook on %(info)s: '
                      'add %(count)s data items to response',
                      {'info': info, 'count': count})
            if 'token' not in data:
                self.data.append(data)
            path, payload = self.parse(content, 'next')
            if not path:
                LOG.debug('Finish request hook on %(info)s: '
                          'no next page found',
                          {'info': info})
                return response
            if self.payload:
                payload.update(self.payload)
            LOG.debug('Continue request hook with new request '
                      '%(method)s %(path)s %(payload)s',
                      {'method': method, 'path': path,
                       'payload': payload})
            return self.request(method, path, payload)

        LOG.debug('Finish request hook on %(info)s',
                  {'info': info})
        return response

    def auth(self):
        method = 'get'
        path = '/auth_user'
        self.proxy.session.headers['user_id'] = self.proxy.username
        self.proxy.session.headers['password'] = self.proxy.password
        self.proxy.delete_bearer()
        response = self.request(method, path, None)
        content = json.loads(response.content)
        if 'token' in content.get('data'):
            token = content['data']['token']
            if token:
                self.proxy.update_token(token)
                return True
        return False

    def check_host(self):
        method = 'get'
        payload = {
            'path': self.proxy.cluster,
        }
        LOG.info('Attempt to find cluster %(cluster)s on host %(host)s',
                 {'cluster': self.proxy.cluster,
                  'host': self.proxy.host})
        try:
            response = self.request(method, self.proxy.root, payload)
        except Exception:
            return False
        content = json.loads(response.content)
        if 'data' not in content or not content['data']:
            LOG.error('cluster %(cluster)s not found on host %(host)s',
                      {'cluster': self.proxy.cluster,
                       'host': self.proxy.host})
            return False
        LOG.info('Found cluster %(cluster)s on host %(host)s',
                 {'cluster': self.proxy.cluster,
                  'host': self.proxy.host})
        return True

    def find_host(self):
        for host in self.proxy.hosts:
            self.proxy.update_host(host)
            if self.check_host():
                self.proxy.update_lock()
                return True
        return False

    def delay(self, attempt, sync=True):
        self.wait += self.proxy.delay(attempt, sync)

    @staticmethod
    def parse(content, name):
        if 'links' in content:
            links = content['links']
            if isinstance(links, list):
                for link in links:
                    if (isinstance(link, dict) and
                            'href' in link and
                            'rel' in link and
                            link['rel'] == name):
                        url = six.moves.urllib.parse.urlparse(link['href'])
                        payload = six.moves.urllib.parse.parse_qs(url.query)
                        return url.path, payload
        return None, None


class RedCollections(object):

    def __init__(self, proxy):
        self.proxy = proxy
        self.namespace = 'red'
        self.prefix = 'instance'
        self.root = '/collections'
        self.subj = 'collection'
        self.properties = []

    def path(self, name):
        quoted_name = six.moves.urllib.parse.quote_plus(name)
        return posixpath.join(self.root, quoted_name)

    def key(self, name):
        return '%s:%s_%s' % (self.namespace, self.prefix, name)

    def get(self, name, payload=None):
        LOG.debug('Get properties of %(subj)s %(name)s: %(payload)s',
                  {'subj': self.subj, 'name': name, 'payload': payload})
        path = self.path(name)
        return self.proxy.get(path, payload)

    def set(self, name, payload=None):
        LOG.debug('Modify properties of %(subj)s %(name)s: %(payload)s',
                  {'subj': self.subj, 'name': name, 'payload': payload})
        path = self.path(name)
        return self.proxy.put(path, payload)

    def list(self, payload=None):
        LOG.debug('List of %(subj)ss: %(payload)s',
                  {'subj': self.subj, 'payload': payload})
        return self.proxy.get(self.root, payload)

    def create(self, payload=None):
        LOG.debug('Create %(subj)s: %(payload)s',
                  {'subj': self.subj, 'payload': payload})
        try:
            return self.proxy.post(self.root, payload)
        except RedException as error:
            if error.code != 'EEXIST':
                raise

    def delete(self, name, payload=None):
        LOG.debug('Delete %(subj)s %(name)s: %(payload)s',
                  {'subj': self.subj, 'name': name, 'payload': payload})
        path = self.path(name)
        try:
            return self.proxy.delete(path, payload)
        except RedException as error:
            if error.code != 'ENOENT':
                raise

    def expose(self, name, payload={"nvmfs":None}):
        LOG.debug('Expose %(subj)s %(name)s: %(payload)s',
                  {'subj': self.subj, 'name': name, 'payload': payload})
        path = '%s/expose' % self.path(name)
        try:
            return self.proxy.put(path, payload)
        except RedException as error:
            if error.code != 'EEXIST':
                raise

    def unexpose(self, name, payload={}):
        LOG.debug('Unexpose %(subj)s %(name)s: %(payload)s',
                  {'subj': self.subj, 'name': name, 'payload': payload})
        path = '%s/unexpose' % self.path(name)
        payload = {"instanceId": None, "subsystemName": ""}
        try:
            return self.proxy.post(path, payload)
        except RedException as error:
            if error.code != 'EEXIST':
                raise

    def resize(self, name, payload={}):
        LOG.debug('Resize %(subj)s %(name)s: %(payload)s',
                  {'subj': self.subj, 'name': name, 'payload': payload})
        path = '%s/resize' % self.path(name)
        try:
            return self.proxy.put(path, payload)
        except RedException as error:
            if error.code != 'EEXIST':
                raise


class RedDatasets(RedCollections):
    def __init__(self, proxy):
        super(RedDatasets, self).__init__(proxy)
        self.root = ('/clusters/{cluster}/tenants/{tenant}/subtenants/' +
                     '{subtenant}/datasets').format(
            cluster=proxy.cluster, tenant=proxy.tenant,
            subtenant=proxy.subtenant)
        self.subj = 'dataset'


class RedBdevs(RedCollections):
    def __init__(self, proxy):
        super(RedBdevs, self).__init__(proxy)
        self.root = ('/clusters/{cluster}/tenants/{tenant}/subtenants/' +
                     '{subtenant}/datasets/{dataset}/bdevs').format(
            cluster=proxy.cluster, tenant=proxy.tenant,
            subtenant=proxy.subtenant, dataset=proxy.dataset)
        self.subj = 'bdev'
        self.properties += [
            {
                'name': self.key('blocksize'),
                'api': 'volumeBlockSize',
                'cfg': 'red_blocksize',
                'title': 'Block size',
                'change': _('Volume block size cannot be changed after '
                            'the volume has been created.'),
                'description': _('Specifies the block size of the volume.'),
                'type': 'integer',
                'enum': [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536,
                         131072],
                'default': 32768
            },
            {
                'name': self.key('thin_provisioning'),
                'api': 'sparseVolume',
                'cfg': 'red_sparsed_volumes',
                'title': 'Thin provisioning',
                'inherit': _('Provisioning type cannot be inherit.'),
                'description': _('Controls if a volume is created sparse '
                                 '(with no space reservation).'),
                'type': 'boolean',
                'default': True
            }
        ]


class RedProxy(object):
    def __init__(
            self, proto, cluster, tenant, subtenant, dataset, backend, conf):
        self.cluster = cluster
        self.tenant = tenant
        self.subtenant = subtenant
        self.dataset = dataset
        self.bdevs = RedBdevs(self)
        self.datasets = RedDatasets(self)
        self.version = None
        self.lock = None
        self.auto = False
        self.tokens = {}
        self.headers = {
            'Content-Type': 'application/json',
            'X-XSS-Protection': '1'
        }
        self.scheme = conf.red_rest_protocol
        self.port = conf.red_rest_port
        self.username = conf.red_user
        self.password = conf.red_password
        self.backend = backend
        self.hosts = []
        if conf.red_rest_address:
            for host in conf.red_rest_address:
                if host:
                    self.hosts.append(host)
        elif conf.red_data_address:
            self.hosts.append(conf.red_data_address)
        else:
            code = 'EINVAL'
            message = (_('Red Rest API address is not defined, '
                         'please check the Cinder configuration file for '
                         'Red backend and red_rest_address, '
                         'red_data_address options'))
            raise RedException(code=code, message=message)
        self.host = self.hosts[0]
        self.retries = conf.red_rest_retry_count
        self.backoff = conf.red_rest_backoff_factor
        self.timeout = (conf.red_rest_connect_timeout,
                        conf.red_rest_read_timeout)
        self.session = requests.Session()
        self.session.verify = conf.driver_ssl_cert_verify
        if self.session.verify and conf.driver_ssl_cert_path:
            self.session.verify = conf.driver_ssl_cert_path
        self.session.headers.update(self.headers)
        if not conf.driver_ssl_cert_verify:
            requests.packages.urllib3.disable_warnings()
        self.update_lock()

    def __getattr__(self, name):
        return RedRequest(self, name)

    def delete_bearer(self):
        if 'Authorization' in self.session.headers:
            del self.session.headers['Authorization']

    def update_bearer(self, token):
        bearer = 'Bearer %s' % token
        self.session.headers['Authorization'] = bearer

    def update_token(self, token):
        self.tokens[self.host] = token
        self.update_bearer(token)

    def update_host(self, host):
        self.host = host
        if host in self.tokens:
            token = self.tokens[host]
            self.update_bearer(token)

    def version_less(self, version):
        if not self.version:
            return True
        return parse_version(self.version) < parse_version(version)

    def update_lock(self):
        software = {}
        settings = {}
        compound = []
        clusters = []
        guids = []
        guid = None
        try:
            software = self.software.version()
        except Exception:
            pass
        if software and 'version' in software:
            version = software['version']
            if version:
                compound.append(version)
        if software and 'build' in software:
            build = software['build']
            if build:
                compound.append(build)
        if compound:
            self.version = '.'.join(map(str, compound))
            LOG.info('Software version for group %(backend)s: %(version)s',
                     {'backend': self.backend, 'version': self.version})
        else:
            self.version = None
        try:
            settings = self.settings.get('system.guid')
        except Exception:
            pass
        if settings and 'value' in settings:
            guid = settings['value']
        payload = {'fields': 'nodes'}
        try:
            clusters = self.rsf.list(payload)
        except Exception:
            pass
        for cluster in clusters:
            if 'nodes' not in cluster:
                continue
            nodes = cluster['nodes']
            for node in nodes:
                if 'machineId' in node:
                    hostid = node['machineId']
                    if hostid and hostid != '-':
                        guids.append(hostid)
        if guid and guids and guid in guids:
            guid = ':'.join(map(str, sorted(guids)))
        if not guid:
            guid = ':'.join(map(str, sorted(self.hosts)))
        lock = '%s:%s' % (guid, self.cluster)
        if isinstance(lock, six.text_type):
            lock = lock.encode('utf-8')
        self.lock = hashlib.md5(lock).hexdigest()
        LOG.info('Coordination lock for group %(backend)s: %(lock)s',
                 {'backend': self.backend, 'lock': self.lock})

    def url(self, path=None):
        if not path:
            path = ''
        netloc = '%s:%d/redapi/v1' % (self.host, self.port)
        components = (self.scheme, netloc, path, None, None)
        url = six.moves.urllib.parse.urlunsplit(components)
        return url

    def delay(self, attempt, sync=True):
        backoff = self.backoff
        if not sync:
            backoff = ASYNC_WAIT
        if self.retries > 0:
            attempt %= self.retries
            if attempt == 0:
                attempt = self.retries
        interval = float(backoff * (2 ** (attempt - 1)))
        LOG.debug('Waiting for %(interval)s seconds',
                  {'interval': interval})
        greenthread.sleep(interval)
        return interval
