"""
To be used either directly from command line using e.g.:

`fab set_hosts update_stormtracks`

or any functions can be imported then run using:

::

    execute(update_stormtracks, host=host)

key file (\*.pem) must be placed in file aws_credentials/st_worker1.pem (rel to .).
"""
from __future__ import print_function

import os
import sys
import csv
from subprocess import call
from time import sleep

from boto.ec2 import connect_to_region
from fabric.api import env, run, cd, settings, sudo, put, execute, task, prefix
from fabric.api import parallel, hide
from fabric.contrib.files import upload_template
from fabric.context_managers import quiet

from aws_helpers import get_ec2_ip_addresses

REGION = 'eu-central-1'


@task
def set_hosts(tag="group", value="st_worker", region=REGION):
    """
    Gets all hosts with given tag/value in region and uses to set fabric hosts.
    Can be used to run all commands with e.g.
    `fab set_hosts update_stormtracks`
    or
    `fab set_hosts:tag=mygroup,value=myvalue update_stormtracks_aws`
    """
    key = "tag:{0}".format(tag)
    env.hosts = get_ec2_ip_addresses(region, key, value)
    env.user = "ubuntu"
    env.key_filename = ["aws_credentials/st_worker1.pem"]


@task
def update_stormtracks():
    """
    Updates stormtracks git repo.
    """
    with cd('Projects/stormtracks'):
        run('git fetch')
        run('git merge origin/master')


@task
def update_stormtracks_aws():
    """
    Updates stormtracks_aws git repo.
    """
    with cd('Projects/stormtracks_aws'):
        run('git fetch')
        run('git merge origin/master')


@task
def st_worker_run(start_year, end_year):
    """
    Configures worker to run with given years by copying settings then starting worker.
    Uses settings template to say which years to run analysis on.
    """
    upload_template('st_worker_files/st_worker_settings.tpl.py',
                    'Projects/stormtracks_aws/st_worker_files/st_worker_settings.py',
                    {'start_year': str(start_year), 'end_year': str(int(end_year) + 1)})

    put('st_worker_files/dotstormtracks.bz2', 'dotstormtracks.bz2')
    run('tar xvf dotstormtracks.bz2')
    put('st_worker_files/stormtracks_settings.py', '.stormtracks/stormtracks_settings.py')

    sudo('supervisorctl start st_worker_run')


@task
def st_worker_status():
    """
    Gets the current status of the worker.
    """
    cmd = 'tail -n1 /home/ubuntu/stormtracks_data/logs/st_worker_status.log'
    status = run(cmd)
    return status


@task
def full_setup():
    """
    Installs stormtracks and all dependencies on target computer.
    """
    basic_setup()
    install_stormtracks()
    install_supervisor()
    notify()


@task
def basic_setup():
    """
    Updates Ubuntu software repositories and installs some useful tools.
    Adds some dotfiles to make ssh'ing into server more pleasant
    """
    sudo('apt-get update')
    sudo('apt-get install -y mercurial git')
    sudo('apt-get install -y python-pip')
    sudo('pip install virtualenv')
    run('hg clone https://bitbucket.org/markmuetz/dotfiles')
    with cd('dotfiles'):
        for f in [".bashrc",  ".gitconfig",  ".hgrc",  ".inputrc",  ".profile",
                  ".screenrc",  ".vim",  ".vimrc", ".zshrc"]:
            run('cp -r {0} ..'.format(f))


@task
def install_stormtracks():
    """
    Installs everything needed for stormtracks and stormtracks_aws.
    Includes all deps for building all Python packages (e.g. NetCDF4, scipy).
    """
    # OS level installs.
    sudo('apt-get install -y git build-essential libhdf5-dev libgeos-dev libproj-dev'
         'libfreetype6-dev python-dev libblas-dev liblapack-dev gfortran libnetcdf-dev')
    sudo('apt-get install -y python-pip')
    sudo('pip install virtualenv')

    # Fix broken symlink in Ubuntu.
    sudo('ln -s /usr/lib/libgeos-3.4.2.so /usr/lib/libgeos.so')

    # Project installs.
    run('mkdir Projects')
    with cd('Projects'):
        # Git repos.
        run('git clone https://github.com/markmuetz/stormtracks')
        run('git clone https://github.com/markmuetz/stormtracks_aws')

        # Setup virtualenv.
        with cd('stormtracks'):
            run('virtualenv st_env')

        # Use virtualenv to install python deps.
        with prefix('source stormtracks/st_env/bin/activate'):
            run('pip install -r stormtracks/requirements_a.txt')
            run('pip install -r stormtracks/requirements_b.txt'
                '--allow-external basemap --allow-unverified basemap')
            run('pip install -e stormtracks')
            run('pip install -r stormtracks_aws/requirements.txt')

    # Copy across credentials.
    run('mkdir Projects/stormtracks_aws/aws_credentials/')
    put('aws_credentials/st_worker1.pem',
        'Projects/stormtracks_aws/aws_credentials/st_worker1.pem',
        mode=0400)
    put('aws_credentials/credentials.csv',
        'Projects/stormtracks_aws/aws_credentials/credentials.csv',
        mode=0400)


@task
def install_supervisor(update=False):
    """
    Sets up supervisord config and adds to upstart init.
    Starts supervisord running.
    """
    # supervisor configuration file.
    put('st_worker_files/supervisord.conf', 'supervisord.conf')
    sudo('cp supervisord.conf /etc/supervisord.conf')

    # Upstart configuration file (MUST NOT be named supervisord.conf)
    put('st_worker_files/supervisor.conf', 'supervisor.conf')
    sudo('cp supervisor.conf /etc/init/supervisor.conf')

    if update:
        sudo('service supervisor restart')
    else:
        # Install (OS level) and start supervisord
        sudo('pip install supervisor')
        sudo('service supervisor start')


@task
def file_exists(filename):
    with quiet():
        have_build_dir = run("test -e {0}".format(filename)).succeeded
        return have_build_dir


@task
def supervisorctl(cmd, program):
    sudo('supervisorctl {0} {1}'.format(cmd, program))


@task
def monitor_directory_space(path='stormtracks_data/data/', poll_time=10):
    """
    Calculated rate of growth of files in directory
    """
    with settings(hide('warnings', 'running', 'stdout', 'stderr'),
                  warn_only=True), cd(path):
        sse, size = None, None
        prev_sse, prev_size = None, None

        while True:
            output = run('date +"%s.%N" && du -s')
            sse_str, size_str = output.split('\r\n')
            prev_sse = sse
            prev_size = size
            sse = float(sse_str)
            size = long(size_str.split('\t')[0])

            if prev_sse is not None:
                elapsed_time = sse - prev_sse
                delta_size = size - prev_size
                print("{0:2.1f}MB/s".format((delta_size / elapsed_time) / 2**10))
            sleep(poll_time)
    return sse, size


@task
def mount_vol():
    run('mkdir PERSISTENT_DATA')
    sudo('mount /dev/xvdf PERSISTENT_DATA')


def beep():
    call(['paplay', '/usr/share/sounds/LinuxMint/stereo/dialog-information.ogg'])


def notify():
    call(['paplay', '/usr/share/sounds/LinuxMint/stereo/desktop-logout.ogg'])
