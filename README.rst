posthaste
=========

OpenStack Swift threaded operation utility for Uploading, Downloading
and Deleting

.. image:: https://pypip.in/v/posthaste/badge.png
        :target: https://crate.io/packages/posthaste
.. image:: https://pypip.in/d/posthaste/badge.png
        :target: https://crate.io/packages/posthaste

Requirements
------------

posthaste currently requires `gevent <http://www.gevent.org/>`_, which
in turn requires `greenlet <https://pypi.python.org/pypi/greenlet>`_.

Usage
-----

::

    usage: posthaste [-h] [--version] -c CONTAINER [-r REGION] [--internal]
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
      --version             show program's version number and exit
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
      -H, --headers <file_name_regex>,<header_name>:<header_value>
                            Set headers returned by RackSpace when serving files matching
                            a specified regular expression.

Installation
------------

All instructions below utilize a Python virtual environment.  It is recommended that you do utilize individual virtual environments for any python module that has the potential of installing many dependencies that could affect other applications or your Operating System.

Red Hat / CentOS / Fedora
~~~~~~~~~~~~~~~~~~~~~~~~~

**Note:** This will require at least Red Hat / CentOS 6 or newer, due to the dependency on python 2.6. You can get python 2.6 or newer on older OSes using 3rd part repositories or utilizing `pythonz <http://saghul.github.io/pythonz/>`_.

.. code-block:: bash

    sudo yum -y install gcc python-devel python-pip python-virtualenv python-argparse
    virtualenv ~/posthaste
    cd ~/posthaste
    . bin/activate
    pip install posthaste

Ubuntu / Debian
~~~~~~~~~~~~~~~

.. code-block:: bash

    sudo apt-get -y install gcc python-dev python-pip python-virtualenv
    virtualenv ~/posthaste
    cd ~/posthaste
    . bin/activate
    pip install posthaste


Testing
~~~~~~~

.. code-block:: bash

    cd ~/posthaste
    . bin/activate
    mkdir -p files
    for num in {1..1000}; do dd if=/dev/urandom of=files/file${num} bs=1k count=4; done
    posthaste -c testcontainer -r ORD -t 100 -u <your_USERNAME_here> -p <your_API-KEY_here> -vv upload files/
    posthaste -c testcontainer -r ORD -t 100 -u <your_USERNAME_here> -p <your_API-KEY_here> -vv delete


Examples
--------

.. code-block:: bash

    posthaste -c example -r DFW -u $OS_USERNAME -p $OS_PASSWORD -t 100 upload /path/to/some/dir/

.. code-block:: bash

    posthaste -c example -r DFW -u $OS_USERNAME -p $OS_PASSWORD -t 100 download /path/to/some/dir/

.. code-block:: bash

    posthaste -c example -r DFW -u $OS_USERNAME -p $OS_PASSWORD -t 100 delete

Grand access to webfonts across different domains:
::
    posthaste -c example -r DFW -u $OS_USERNAME -p $OS_PASSWORD -t 100 upload /path/to/some/dir/ -H ".*\.(eot|otf|woff|ttf)$,Access-Control-Allow-Origin:*"
