#!/usr/bin/env python
import os
try:
    from setuptools import setup, Extension
except ImportError:
    from distutils.core import setup, Extension

from staws.version import get_version


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='stormtracks_aws',
    version=get_version(),
    description='Run stormtracks on AWS',
    long_description=read('README.rst'),
    author='Mark Muetzelfeldt',
    author_email='markmuetz@gmail.com',
    maintainer='Mark Muetzelfeldt',
    maintainer_email='markmuetz@gmail.com',

    packages=['staws',],
    scripts=[
        'staws/st_master.py',
        ],

    install_requires=[
        'argh',
        'argcomplete',
        'Fabric==1.10.1',
        'boto==2.38.0',
        'filechunkio==1.6',
        'ipython==3.1.0',
        'paramiko==1.15.2',
        'pycrypto==2.6.1',
        'wsgiref==0.1.2',
        ],
    url='https://github.com/markmuetz/stormtracks_aws',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: C',
        'Topic :: Scientific/Engineering :: Atmospheric Science',
        ],
    keywords=['tropical cyclone track detection'],
    )
