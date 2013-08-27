# posthaste

OpenStack Swift threaded operation utility for Uploading, Downloading and Deleting

## Requirements

posthaste currently requires [gevent](http://www.gevent.org/), which in turn requires [greenlet](https://pypi.python.org/pypi/greenlet) and [libevent](http://libevent.org/). 

The "new" Gevent (as of this writing, 1.0 RC 2) alleviates the libevent dependency and thus simplifies the process of using tool.  More information can be found on [Github](https://github.com/surfly/gevent#installing-from-github).

## Usage

    usage: posthaste.py [-h] -c CONTAINER -r {DFW,ORD,LON} [--internal]
                        [-t THREADS] [-u USERNAME] [-p PASSWORD]
                        [-i {rackspace,keystone}] [-v]
                        {delete,upload,download} ...

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
      -r {DFW,ORD,LON}, --region {DFW,ORD,LON}
                            Region where the specified container exists. Default
                            DFW
      --internal            Use the internalURL (ServiceNet) for communication and
                            operations
      -t THREADS, --threads THREADS
                            Number of concurrent threads used for deletion.
                            Default 10
      -u USERNAME, --username USERNAME
                            Username to authenticate with. Default OS_USERNAME
                            environment variable
      -p PASSWORD, --password PASSWORD
                            API Key or password to authenticate with. Default
                            OS_PASSWORD environment variable
      -i {rackspace,keystone}, --identity {rackspace,keystone}
                            Identitiy type to auth with. Default rackspace
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
