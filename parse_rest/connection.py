#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.

from six.moves.urllib.request import Request, urlopen
from six.moves.urllib.error import HTTPError
from six.moves.urllib.parse import urlencode, urlparse

import json
from parse_rest import core
API_ROOT = ''

# Connection can sometimes hang forever on SSL handshake
CONNECTION_TIMEOUT = 60
CONNECTION = {}


def register(api_root, app_id, rest_key, **kw):
    global CONNECTION
    CONNECTION = {'api_root': api_root, 'app_id': app_id, 'rest_key': rest_key}
    CONNECTION.update(**kw)


class SessionToken:
    def __init__(self, token):
        global CONNECTION
        self.token = token

    def __enter__(self):
        CONNECTION.update({'session_token': self.token})

    def __exit__(self, type, value, traceback):
        del CONNECTION['session_token']


class MasterKey:
    def __init__(self, master_key):
        global CONNECTION
        self.master_key = master_key

    def __enter__(self):
        return CONNECTION.update({'master_key': self.master_key})

    def __exit__(self, type, value, traceback):
        del CONNECTION['master_key']


def master_key_required(func):
    '''decorator describing methods that require the master key'''
    def ret(obj, *args, **kw):
        conn = CONNECTION
        if not (conn and conn.get('master_key')):
            message = '%s requires the master key' % func.__name__
            raise core.ParseError(message)
        func(obj, *args, **kw)
    return ret

# Using this as "default=" argument solve the problem with Datetime object not being JSON serializable
def date_handler(obj):
    return obj.isoformat() if hasattr(obj, 'isoformat') else obj


class ParseBase(object):

    @classmethod
    def get_url(cls, api_root, **kw):
        if cls.__name__ == 'ParseBatcher':
            url = api_root + '/batch'
        elif cls.__name__ == 'Function':
            url = api_root +  '/functions'
        elif cls.__name__ == 'Job':
            url = api_root + '/jobs'
        elif cls.__name__ == 'Config':
            url = api_root + '/config' 
        elif cls.__name__ == 'User':
            url = api_root + '/users'
            url += '/' + kw.pop('objectId') if kw.get('objectId') else ''
        elif cls.__name__ == 'Role':
            url = api_root + '/roles'
        elif cls.__name__ == 'Installation':
            url = api_root + '/installations'
            url += '/' + kw.pop('installation_id') if kw.get('installation_id') else ''
        elif cls.__name__ == 'Push':
            url = api_root + '/push'
        elif cls.__name__ == 'File':
            url = api_root + '/files'
            url += '/' + kw.pop('name') if kw.get('name') else ''
        elif cls.__name__ == 'ParseResource':
            url = api_root + '/' + kw.pop('uri')
        else: 
            url = '/'.join([api_root, 'classes', cls.__name__])
            url += '/' + kw.pop('objectId') if kw.get('objectId') else ''
        return url, kw

    @classmethod
    def execute(cls, http_verb, extra_headers=None, batch=False, _body=None, **kw):
        """
        if batch == False, execute a command with the given parameters and
        return the response JSON.
        If batch == True, return the dictionary that would be used in a batch
        command.
        """
        api_root = CONNECTION['api_root']
        url, kw = cls.get_url(api_root, **kw)
        if batch:
            urlsplitter = urlparse(api_root).netloc
            ret = {"method": http_verb, "path": url.split(urlsplitter, 1)[1]}
            if kw:
                ret["body"] = kw
            return ret

        if not (CONNECTION.get('app_id') and CONNECTION.get('rest_key')):
            raise core.ParseError('Missing connection credentials')

        if _body is None:
            data = kw and json.dumps(kw, default=date_handler) or "{}"
        else:
            data = _body
        if http_verb == 'GET' and data:
            url += '?%s' % urlencode(kw)
            data = None
        else:
            if cls.__name__ == 'File':
                data = data
            else:
                data = data.encode('utf-8')

        headers = {
            'Content-type': 'application/json',
            'X-Parse-Application-Id': CONNECTION['app_id'],
            'X-Parse-REST-API-Key': CONNECTION['rest_key']
        }
        headers.update(extra_headers or {})

        if cls.__name__ == 'File':
            request = Request(url.encode('utf-8'), data, headers)
        else:
            request = Request(url, data, headers)

        if CONNECTION.get('session_token'):
            request.add_header('X-Parse-Session-Token', CONNECTION['session_token'])
        elif CONNECTION.get('master_key'):
            request.add_header('X-Parse-Master-Key', CONNECTION['master_key'])
        
        request.get_method = lambda: http_verb
        
        try:
            response = urlopen(request, timeout=CONNECTION_TIMEOUT)
        except HTTPError as e:
            exc = {
                400: core.ResourceRequestBadRequest,
                401: core.ResourceRequestLoginRequired,
                403: core.ResourceRequestForbidden,
                404: core.ResourceRequestNotFound
                }.get(e.code, core.ParseError)
            raise exc(e.read())

        return json.loads(response.read().decode('utf-8'))

    @classmethod
    def GET(cls, **kw):
        return cls.execute('GET', **kw)

    @classmethod
    def POST(cls, **kw):
        return cls.execute('POST', **kw)

    @classmethod
    def PUT(cls, **kw):
        return cls.execute('PUT', **kw)

    @classmethod
    def DELETE(cls, **kw):
        return cls.execute('DELETE', **kw)

    @classmethod
    def drop(cls):
        return cls.POST("%s/schemas/%s" % (API_ROOT, cls.__name__),
                        _method="DELETE", _ClientVersion="browser")


class ParseBatcher(ParseBase):
    """Batch together create, update or delete operations"""

    def batch(self, methods):
        """
        Given a list of create, update or delete methods to call, call all
        of them in a single batch operation.
        """
        methods = list(methods) # methods can be iterator
        if not methods:
            #accepts also empty list (or generator) - it allows call batch directly with query result (eventually empty)
            return
        queries, callbacks = list(zip(*[m(batch=True) for m in methods]))
        # perform all the operations in one batch
        responses = self.execute("POST", requests=queries)
        # perform the callbacks with the response data (updating the existing
        # objets, etc)

        batched_errors = []
        for callback, response in zip(callbacks, responses):
            if "success" in response:
                callback(response["success"])
            else:
                batched_errors.append(response["error"])

        if batched_errors:
            raise core.ParseBatchError(batched_errors)

    def batch_save(self, objects):
        """save a list of objects in one operation"""
        self.batch(o.save for o in objects)

    def batch_delete(self, objects):
        """delete a list of objects in one operation"""
        self.batch(o.delete for o in objects)
