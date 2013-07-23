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
from gevent.pool import Pool
monkey.patch_all()

import sys
import json
import os
import collections
import argparse
import requests


def handle_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--container', required=True,
                        help='The name container to operate on')
    parser.add_argument('-r', '--region', required=True, default='DFW',
                        choices=('DFW', 'ORD', 'LON'),
                        help='Region where the specified container exists. '
                             'Default DFW')
    parser.add_argument('-t', '--threads', required=False, type=int,
                        default=10,
                        help='Number of concurrent threads used for '
                             'deletion. Default 10')
    parser.add_argument('-u', '--username', required=False,
                        default=os.getenv('OS_USERNAME'),
                        help='Username to authenticate with. Default '
                             'OS_USERNAME environment variable')
    parser.add_argument('-p', '--password', required=False,
                        default=os.getenv('OS_PASSWORD'),
                        help='API Key or password to authenticate with. '
                             'Default OS_PASSWORD environment variable')
    parser.add_argument('-i', '--identity', required=False,
                        default='rackspace', choices=('rackspace', 'keystone'),
                        help='Identitiy type to auth with. Default rackspace')
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
    download.add_argument('directory', help='The directory to upload')

    args = parser.parse_args()
    return args


def authenticate(args):
    auth_url = 'https://identity.api.rackspacecloud.com/v2.0/tokens'

    if args.identity == 'rackspace':
        auth_data = {
            'auth': {
                'RAX-KSKEY:apiKeyCredentials': {
                    'username': args.username,
                    'apiKey': args.password
                }
            }
        }
    else:
        auth_data = {
            'auth': {
                'passwordCredentials': {
                    'username': args.username,
                    'password': args.password
                }
            }
        }
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

    endpoint = None
    for service in service_catalog:
        if service['name'] == 'cloudFiles':
            for ep in service['endpoints']:
                if ep['region'] == args.region:
                    endpoint = ep['publicURL']
                    break
            break
    if not endpoint:
        raise SystemExit('Endpoint not found')

    return endpoint, token


def get_files(directory, sized_sort=True):
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

    files = []
    os.path.walk(directory, _walker, None)
    if sized_sort:
        files.sort(key=lambda d: d['size'], reverse=True)

    return files


def get_objects(endpoint, container, token):
    headers = {
        'Accept': 'application/json',
        'X-Auth-Token': token
    }

    all_objects = []
    r = requests.get('%s/%s?format=json' % (endpoint, container),
                     headers=headers)

    if r.status_code != 200:
        raise SystemExit(json.dumps(json.loads(r.text), indent=4))

    objects = r.json()
    all_objects.extend(objects)
    while len(objects):
        r = requests.get('%s/%s?format=json&marker=%s' %
                         (endpoint, container, objects[-1]['name']),
                         headers=headers)

        if r.status_code != 200:
            raise SystemExit(json.dumps(json.loads(r.text), indent=4))

        objects = r.json()
        all_objects.extend(objects)
    return all_objects


def handle_delete(endpoint, container, objects, token, threads, verbose):
    def _delete(i, files, errors):
        if verbose:
            print 'Starting thread %s' % i
        s = requests.Session()
        for f in files:
            if verbose > 1:
                print 'Deleting %s' % f
            try:
                r = s.delete('%s/%s/%s' % (endpoint, container, f),
                             headers={'X-Auth-Token': token})
            except:
                e = sys.exc_info()[0]
                errors.append({
                    'name': f,
                    'container': container,
                    'exception': str(e)
                })
            else:
                if r.status_code != 204:
                    errors.append({
                        'name': f,
                        'container': container,
                        'status_code': r.status_code,
                        'headers': r.headers,
                        'response': json.loads(r.text)
                    })
        if verbose:
            print 'Completed thread %s' % i

    files = collections.defaultdict(list)
    thread_mark = threads
    files_per_thread = len(objects) / threads / 3
    i = 0
    for o in objects:
        files[i].append(o['name'])
        i += 1
        if len(files[thread_mark - 1]) == files_per_thread:
            thread_mark += threads
            files_per_thread = files_per_thread / 2
            i = 0
        if i == thread_mark:
            i = 0

    pool = Pool(size=threads)
    errors = []
    for i, file_chunk in files.iteritems():
        pool.spawn(_delete, i, file_chunk, errors)
    pool.join()
    return errors


def handle_upload(directory, endpoint, container, files, token, threads,
                  verbose):
    def _upload(i, files, errors):
        if verbose:
            print 'Starting thread %s' % i
        s = requests.Session()
        for fobj in files:
            with open(fobj['path'], 'rb') as f:
                body = f.read()
            if verbose > 1:
                print 'Uploading %s' % fobj['name']
            try:
                r = s.put('%s/%s/%s' %
                          (endpoint, container, fobj['name']), data=body,
                          headers={'X-Auth-Token': token})
            except:
                e = sys.exc_info()[0]
                errors.append({
                    'name': fobj['name'],
                    'container': container,
                    'exception': str(e)
                })
            else:
                if r.status_code != 201:
                    errors.append({
                        'name': fobj['name'],
                        'container': container,
                        'status_code': r.status_code,
                        'headers': r.headers,
                        'response': json.loads(r.text)
                    })
        if verbose:
            print 'Completed thread %s' % i

    file_chunks = collections.defaultdict(list)
    thread_mark = threads
    files_per_thread = len(files) / threads / 3
    i = 0
    for f in files:
        file_chunks[i].append(f)
        i += 1
        if len(file_chunks[thread_mark - 1]) == files_per_thread:
            thread_mark += threads
            files_per_thread = files_per_thread / 2
            i = 0
        if i == thread_mark:
            i = 0

    pool = Pool(size=threads)

    errors = []
    for i, file_chunk in file_chunks.iteritems():
        pool.spawn(_upload, i, file_chunk, errors)
    pool.join()
    return errors


def handle_download(directory, endpoint, container, objects, token, threads,
                    verbose):
    def _download(i, files, directory, errors):
        if verbose:
            print 'Starting thread %s' % i
        s = requests.Session()
        directory = os.path.abspath(directory)
        for filename in files:
            if verbose > 1:
                print 'Deleting %s' % filename
            try:
                path = os.path.join(directory, filename)
                try:
                    os.makedirs(os.path.dirname(path), 0755)
                except OSError as e:
                    if e.errno != 17:
                        raise
                with open(path, 'wb+') as f:
                    r = s.get('%s/%s/%s' % (endpoint, container, filename),
                              headers={'X-Auth-Token': token}, stream=True)
                    for block in r.iter_content(4096):
                        if not block:
                            break
                        f.write(block)
            except:
                e = sys.exc_info()[0]
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
        if verbose:
            print 'Completed thread %s' % i

    files = collections.defaultdict(list)
    thread_mark = threads
    files_per_thread = len(objects) / threads / 3
    i = 0
    for o in objects:
        files[i].append(o['name'])
        i += 1
        if len(files[thread_mark - 1]) == files_per_thread:
            thread_mark += threads
            files_per_thread = files_per_thread / 2
            i = 0
        if i == thread_mark:
            i = 0

    pool = Pool(size=threads)
    errors = []
    for i, file_chunk in files.iteritems():
        pool.spawn(_download, i, file_chunk, directory, errors)
    pool.join()
    return errors


def shell():
    args = handle_args()
    endpoint, token = authenticate(args)
    if args.action == 'upload':
        files = get_files(args.directory)
        errors = handle_upload(args.directory, endpoint, args.container,
                               files, token, args.threads, args.verbose)
    elif args.action == 'download':
        objects = get_objects(endpoint, args.container, token)
        errors = handle_download(args.directory, endpoint, args.container,
                                 objects, token, args.threads, args.verbose)
    elif args.action == 'delete':
        objects = get_objects(endpoint, args.container, token)
        errors = handle_delete(endpoint, args.container, objects, token,
                               args.threads, args.verbose)

    if errors:
        print '\nErrors:'
        print json.dumps(errors, indent=4)
    else:
            print '\nCompleted Successfully'


if __name__ == '__main__':
    try:
        shell()
    except:
        e = sys.exc_info()[0]
        raise SystemExit(e)

# vim:set ts=4 sw=4 expandtab:
