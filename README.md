# posthaste

OpenStack Swift and Rackspace Cloud Files threaded-operation utility for uploading, downloading, and deleting objects.

## Requirements

posthaste currently requires [gevent](http://www.gevent.org/), which in turn requires [greenlet](https://pypi.python.org/pypi/greenlet) and [libevent](http://libevent.org/). 

The "new" Gevent (as of this writing, 1.0 RC 2) alleviates the libevent dependency and thus simplifies the process of using this tool.  More information can be found on [Github](https://github.com/surfly/gevent#installing-from-github).

## Usage

    usage: python posthaste.py [-h] -c CONTAINER [-r REGION] [--internal]
                               [-t THREADS] [-u USERNAME] [-p PASSWORD]
                               [-i {rackspace,keystone}] [-a AUTH_URL] [-v]
                               {delete,upload,download} ...
    
    Gevent-based, multithreaded tool for interacting with OpenStack Swift and
    Rackspace Cloud Files
    
    positional arguments:
      {delete,upload,download}
        delete              Delete files from specified container
        upload              Upload files to specified container
        download            Download files to specified directory from the
                            specified container
    
    optional arguments:
      -h, --help            show this help message and exit
      -c CONTAINER, --container CONTAINER
                            The name container to operate on
      -r REGION, --region REGION
                            Region where the specified container exists. Defaults
                            to OS_REGION_NAME environment variable with a fallback
                            to DFW
      --internal            Use the internalURL (ServiceNet) for communication and
                            operations
      -t THREADS, --threads THREADS
                            Number of concurrent threads used for deletion.
                            Default 10
      -u USERNAME, --username USERNAME
                            Username to authenticate with. Defaults to OS_USERNAME
                            environment variable
      -p PASSWORD, --password PASSWORD
                            API Key or password to authenticate with. Defaults to
                            OS_PASSWORD environment variable
      -i {rackspace,keystone}, --identity {rackspace,keystone}
                            Identitiy type to auth with. Defaults to
                            OS_AUTH_SYSTEM environment variable with a fallback to
                            rackspace
      -a AUTH_URL, --auth-url AUTH_URL
                            Auth URL to use. Defaults to OS_AUTH_URL environment
                            variable with a fallback to
                            https://identity.api.rackspacecloud.com/v2.0
      -v, --verbose         Enable verbosity. Supply multiple times for additional
                            verbosity. 1) Show Thread Start/Finish, 2) Show Object
                            Name.

## Examples

```shell
python posthaste.py -c example -r DFW -u $OS_USERNAME -p $OS_PASSWORD -t 100 upload /path/to/some/dir/
```

```shell
python posthaste.py -c example -r DFW -u $OS_USERNAME -p $OS_PASSWORD -t 100 download /path/to/some/dir/
```

```shell
python posthaste.py -c example -r DFW -u $OS_USERNAME -p $OS_PASSWORD -t 100 delete
```
