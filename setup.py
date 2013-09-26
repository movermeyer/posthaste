import setuptools

setuptools.setup(
    name='posthaste',
    version='0.2.1',
    description=('Gevent-based, multithreaded tool for interacting with '
                 'OpenStack Swift and Rackspace Cloud Files'),
    long_description=open('README.rst').read(),
    keywords='rackspace openstack cloud cloudfiles swift',
    author='Matt Martz',
    author_email='matt@sivel.net',
    url='https://github.com/rackerlabs/posthaste',
    license='Apache License, Version 2.0',
    py_modules=['posthaste'],
    install_requires=[
        'gevent>=0.13',
        'requests>=1.2'
    ],
    entry_points={
        'console_scripts': [
            'posthaste=posthaste:shell'
        ]
    },
    classifiers=[
        'Programming Language :: Python',
        'Environment :: Console',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent'
    ]
)

# vim:set ts=4 sw=4 expandtab:
