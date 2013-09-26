#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright 2013 Matt Martz
# All Rights Reserved.
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

import gevent
from gevent import monkey
monkey.patch_all()
from gevent.pool import Pool
from gevent.queue import Queue

import sys
import json
import os
import argparse
import requests
import functools
import time
import threading

__version__ = '0.2.1'


def handle_args():
    desc = ('Gevent-based, multithreaded tool for interacting with OpenStack '
            'Swift and Rackspace Cloud Files')
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--version', action='version',
                        version=__version__)
    parser.add_argument('-c', '--container', required=True,
                        help='The name container to operate on')
    parser.add_argument('-r', '--region', required=False,
                        default=os.getenv('OS_REGION_NAME', 'DFW'),
                        help='Region where the specified container exists. '
                             'Defaults to OS_REGION_NAME environment variable '
                             'with a fallback to DFW')
    parser.add_argument('--internal', required=False, default=False,
                        action='store_true',
                        help='Use the internalURL (ServiceNet) for '
                             'communication and operations')
    parser.add_argument('-t', '--threads', required=False, type=int,
                        default=10,
                        help='Number of concurrent threads used for '
                             'deletion. Default 10')
    parser.add_argument('-u', '--username', required=False,
                        default=os.getenv('OS_USERNAME'),
                        help='Username to authenticate with. Defaults to '
                             'OS_USERNAME environment variable')
    parser.add_argument('-p', '--password', required=False,
                        default=os.getenv('OS_PASSWORD'),
                        help='API Key or password to authenticate with. '
                             'Defaults to OS_PASSWORD environment variable')
    parser.add_argument('-i', '--identity', required=False,
                        default=os.getenv('OS_AUTH_SYSTEM', 'rackspace'),
                        choices=('rackspace', 'keystone'),
                        help='Identitiy type to auth with. Defaults to '
                             'OS_AUTH_SYSTEM environment variable with a '
                             'fallback to rackspace')
    rs_auth_url = 'https://identity.api.rackspacecloud.com/v2.0'
    auth_url = os.getenv('OS_AUTH_URL', rs_auth_url)
    parser.add_argument('-a', '--auth-url', required=False,
                        default=auth_url,
                        help='Auth URL to use. Defaults to OS_AUTH_URL '
                             'environment variable with a fallback to '
                             '%s' % rs_auth_url)
    parser.add_argument('-v', '--verbose', required=False, action='count',
                        help='Enable verbosity. Supply multiple times for '
                             'additional verbosity. 1) Show Thread '
                             'Start/Finish, 2) Show Object Name.')

    subparsers = parser.add_subparsers()

    delete = subparsers.add_parser('delete',
                                   help='Delete files from specified '
                                        'container')
    delete.set_defaults(action='delete')

    upload = subparsers.add_parser('upload',
                                   help='Upload files to specified container')
    upload.set_defaults(action='upload')
    upload.add_argument('directory', help='The directory to upload')

    download = subparsers.add_parser('download',
                                     help='Download files to specified '
                                          'directory from the specified '
                                          'container')
    download.set_defaults(action='download')
    download.add_argument('directory',
                          help='The directory to download files to')

    args = parser.parse_args()
    return args


class AuthenticationError(Exception):
    pass


class Posthaste(object):
    def __init__(self, args):
        self._args = args
        self._authenticate(args)
        self._num_auths = 0
        self.semaphore = threading.Semaphore()
        self._queue = Queue()

    def requires_auth(self, f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            while True:
                try:
                    f(*args, **kwargs)
                except AuthenticationError:
                    with self.semaphore:
                        print ('Thread session died; attempting '
                               're-authentication.')
                        self._authenticate()
                        self._num_auths += 1
                        time.sleep(1)
                if self._num_auths > self._args.threads + 10:
                    sys.stderr.write('Exceeded limit of %s authentication '
                                     'attempts; aborting.\n' %
                                     self._args.threads + 10)
                    gevent.hub.get_hub().parent.throw(SystemExit())
                else:
                    break
            return f
        return wrapped

    def _authenticate(self, args=None):
        if not args:
            args = self._args
        auth_url = os.path.join(args.auth_url, 'tokens')

        if args.identity == 'rackspace':
            auth_data = {
                'auth': {
                    'RAX-KSKEY:apiKeyCredentials': {
                        'username': args.username,
                        'apiKey': args.password
                    }
                }
            }
        elif args.identity == 'keystone':
            auth_data = {
                'auth': {
                    'passwordCredentials': {
                        'username': args.username,
                        'password': args.password
                    }
                }
            }
        else:
            raise SystemExit('Unsupported identity/OS_AUTH_SYSTEM provided')

        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }

        r = requests.post(auth_url, data=json.dumps(auth_data),
                          headers=headers)

        if r.status_code != 200:
            raise SystemExit(json.dumps(r.json(), indent=4))

        auth_response = r.json()
        token = auth_response['access']['token']['id']
        service_catalog = auth_response['access']['serviceCatalog']

        if args.internal:
            url_type = 'internalURL'
        else:
            url_type = 'publicURL'

        endpoint = None
        for service in service_catalog:
            if (service['type'] == 'object-store' and
                    service['name'] in ['cloudFiles', 'swift']):
                for ep in service['endpoints']:
                    if ep['region'] == args.region:
                        endpoint = ep[url_type]
                        break
                break
        if not endpoint:
            raise SystemExit('Endpoint not found')

        self.token = token
        self.endpoint = endpoint

    def get_files(self, directory, verbose, sized_sort=True):
        def _walker(arg, dirname, fnames):
            for fname in fnames:
                full_path = os.path.join(dirname, fname)
                if os.path.isdir(full_path):
                    continue
                obj_name = os.path.relpath(full_path, directory)
                obj_size = os.stat(full_path).st_size
                files.append({
                    'path': full_path,
                    'name': obj_name,
                    'size': obj_size
                })
        if verbose:
            sys.stdout.write('Scanning the filesystem for files...')
            sys.stdout.flush()
        files = []
        os.path.walk(directory, _walker, None)
        if verbose:
            print 'Done!'
        if sized_sort:
            files.sort(key=lambda d: d['size'], reverse=True)

        if verbose:
            sys.stdout.write('Queueing files...')
            sys.stdout.flush()
        for file in files:
            self._queue.put_nowait(file)
        if verbose:
            print 'Done!'

    def get_objects(self, container, verbose):
        if verbose:
            sys.stdout.write('Querying API for objects...')
            sys.stdout.flush()
        headers = {
            'Accept': 'application/json',
            'X-Auth-Token': self.token
        }

        all_objects = []
        r = requests.get('%s/%s?format=json' % (self.endpoint, container),
                         headers=headers)

        if r.status_code != 200:
            raise SystemExit(json.dumps(json.loads(r.text), indent=4))

        objects = r.json()
        all_objects.extend(objects)
        while len(objects):
            r = requests.get('%s/%s?format=json&marker=%s' %
                             (self.endpoint, container, objects[-1]['name']),
                             headers=headers)

            if r.status_code != 200:
                raise SystemExit(json.dumps(json.loads(r.text), indent=4))

            objects = r.json()
            all_objects.extend(objects)

        for obj in all_objects:
            self._queue.put_nowait(obj['name'])

        if verbose:
            print 'Done!'

    def handle_delete(self, container, threads, verbose):
        @self.requires_auth
        def _delete(i, files, errors):
            if verbose:
                print 'Thread %3s: starting' % i
            while True:
                try:
                    f = files.get_nowait()
                except gevent.queue.Empty:
                    if verbose:
                        if verbose > 1:
                            print 'Thread %3s: queue empty' % i
                        print 'Thread %3s: exiting' % i
                    raise gevent.GreenletExit
                else:
                    if verbose > 1:
                        print 'Thread %3s: deleting %s' % (i, f)
                    try:
                        r = s.delete('%s/%s/%s' %
                                     (self.endpoint, container, f),
                                     headers={'X-Auth-Token': self.token})
                    except:
                        e = sys.exc_info()[1]
                        errors.append({
                            'name': f,
                            'container': container,
                            'exception': str(e)
                        })
                    else:
                        if r.status_code == 401:
                            raise AuthenticationError
                        if r.status_code != 204:
                            errors.append({
                                'name': f,
                                'container': container,
                                'status_code': r.status_code,
                                'headers': r.headers,
                                'response': json.loads(r.text)
                            })
                    finally:
                        if verbose > 1:
                            print ('Thread %3s: delete complete for %s'
                                   % (i, f))

        s = requests.Session()

        pool = Pool(size=threads)
        errors = []
        for i in xrange(threads):
            pool.spawn(_delete, i, self._queue, errors)
        pool.join()
        return errors

    def handle_upload(self, directory, container, threads, verbose):
        @self.requires_auth
        def _upload(thread, queue, errors):
            if verbose:
                print 'Thread %s: start' % thread
            while True:
                try:
                    file = queue.get_nowait()
                except gevent.queue.Empty:
                    if verbose:
                        if verbose > 1:
                            print 'Thread %3s: queue empty' % thread
                        print 'Thread %3s: exiting' % thread
                    raise gevent.GreenletExit()
                else:
                    with open(file['path'], 'rb') as f:
                        body = f.read()
                    if verbose > 1:
                        print 'Thread %3s: uploading %s' % (thread,
                                                            file['name'])
                    try:
                        r = s.put('%s/%s/%s' %
                                  (self.endpoint,  container,  file['name']),
                                  data=body, headers={
                                      'X-Auth-Token': self.token
                                  })
                    except:
                        e = sys.exc_info()[1]
                        errors.append({
                            'name': file['name'],
                            'container': container,
                            'exception': str(e)
                        })
                    else:
                        if r.status_code == 401:
                            raise AuthenticationError
                        if r.status_code != 201:
                            errors.append({
                                'name': file['name'],
                                'container': container,
                                'status_code': r.status_code,
                                'headers': r.headers,
                                'response': json.loads(r.text)
                            })
                    finally:
                        if verbose > 1:
                            print ('Thread %3s: upload complete for %s'
                                   % (thread, file['name']))

        s = requests.Session()

        pool = Pool(size=threads)
        errors = []
        for i in xrange(threads):
            pool.spawn(_upload, i, self._queue, errors)
        pool.join()
        return errors

    def handle_download(self, directory, container, threads, verbose):
        @self.requires_auth
        def _download(i, files, directory, errors):
            if verbose:
                print 'Starting thread %s' % i
            while True:
                try:
                    filename = files.get_nowait()
                except gevent.queue.Empty:
                    if verbose:
                        if verbose > 1:
                            print 'Thread %3s: queue empty' % i
                        print 'Thread %3s: exiting' % i
                    raise gevent.GreenletExit
                else:
                    directory = os.path.abspath(directory)
                    if verbose > 1:
                        print 'Thread %3s: downloadng %s' % (i, filename)
                    try:
                        path = os.path.join(directory, filename)
                        try:
                            os.makedirs(os.path.dirname(path), 0755)
                        except OSError as e:
                            if e.errno != 17:
                                raise
                        with open(path, 'wb+') as f:
                            r = s.get('%s/%s/%s' % (self.endpoint,
                                                    container,
                                                    filename),
                                      headers={
                                          'X-Auth-Token': self.token
                                      }, stream=True)
                            if r.status_code == 401:
                                raise AuthenticationError
                            for block in r.iter_content(4096):
                                if not block:
                                    break
                                f.write(block)
                    except:
                        e = sys.exc_info()[1]
                        errors.append({
                            'name': filename,
                            'container': container,
                            'exception': str(e)
                        })
                    else:
                        if r.status_code != 200:
                            errors.append({
                                'name': filename,
                                'container': container,
                                'status_code': r.status_code,
                                'headers': r.headers,
                                'response': json.loads(r.text)
                            })
                finally:
                    if verbose > 1:
                        print ('Thread %3s: download complete for %s'
                               % (i, filename))

        s = requests.Session()

        pool = Pool(size=threads)
        errors = []
        for i in xrange(threads):
            pool.spawn(_download, i, self._queue, directory, errors)
        pool.join()
        return errors


def shell():
    args = handle_args()
    posthaste = Posthaste(args)
    if args.action == 'upload':
        posthaste.get_files(args.directory, args.verbose)
        errors = posthaste.handle_upload(args.directory, args.container,
                                         args.threads, args.verbose)
    elif args.action == 'download':
        posthaste.get_objects(args.container, args.verbose)
        errors = posthaste.handle_download(args.directory, args.container,
                                           args.threads, args.verbose)
    elif args.action == 'delete':
        posthaste.get_objects(args.container, args.verbose)
        errors = posthaste.handle_delete(args.container, args.threads,
                                         args.verbose)

    if errors:
        print '\nErrors:'
        print json.dumps(errors, indent=4)
    else:
        print '\nCompleted Successfully'


if __name__ == '__main__':
    try:
        shell()
    except:
        e = sys.exc_info()[1]
        if isinstance(e, SystemExit):
            raise
        else:
            raise SystemExit(e)

# vim:set ts=4 sw=4 expandtab:
