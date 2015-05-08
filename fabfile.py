import os
import sys
import csv
from subprocess import call
from time import sleep

from boto.ec2 import connect_to_region
from fabric.api import env, run, cd, settings, sudo, put, execute, task
from fabric.api import parallel
from fabric.contrib.files import upload_template
from fabric.context_managers import quiet

REGION = 'eu-central-1'

env.user = "ubuntu"
env.key_filename = ["aws_credentials/st_worker1.pem"]


@task
def set_hosts(tag="group", value="st_worker", region=REGION):
    key = "tag:{0}".format(tag)
    env.hosts = get_ec2_ip_addresses(region, key, value)
    print(env.hosts)


@task
def update_stormtracks():
    with cd('Projects/stormtracks'):
        run('git fetch')
        run('git merge origin/master')

@task
def update_stormtracks_aws():
    with cd('Projects/stormtracks_aws'):
        run('git fetch')
        run('git merge origin/master')


@task
def st_worker_run(start_year, end_year):
    upload_template('st_years.tpl.py', 'st_years.py', 
            {'start_year': str(start_year), 'end_year': str(end_year + 1)})

    put('st_worker_files/dotstormtracks.bz2', 'dotstormtracks.bz2')
    run('tar xvf dotstormtracks.bz2')
    put('st_worker_files/stormtracks_settings.py', '.stormtracks/stormtracks_settings.py')

    sudo('supervisorctl start st_worker_run')


@task
def st_worker_status():
    cmd = 'tail -n1 /home/ubuntu/stormtracks_data/logs/st_worker_status.log'
    status = run(cmd)
    return status


@task
def full_setup():
    basic_setup()
    install_stormtracks()
    install_supervisor()


@task
def basic_setup():
    sudo('apt-get update')
    sudo('apt-get install -y mercurial git')
    sudo('apt-get install -y python-pip')
    sudo('pip install virtualenv')
    run('hg clone https://bitbucket.org/markmuetz/dotfiles')
    with cd('dotfiles'):
        for f in [".bashrc",  ".gitconfig",  ".hgrc",  ".inputrc",  ".profile",  ".screenrc",  ".vim",  ".vimrc", ".zshrc"]:
            run('cp -r {0} ..'.format(f))


@task
def install_stormtracks():
    sudo('apt-get install -y git build-essential libhdf5-dev libgeos-dev libproj-dev libfreetype6-dev python-dev libblas-dev liblapack-dev gfortran libnetcdf-dev')
    sudo('apt-get install -y python-pip')
    sudo('pip install virtualenv')
    sudo('ln -s /usr/lib/libgeos-3.4.2.so /usr/lib/libgeos.so')
    run('mkdir Projects')
    with cd('Projects'):
        run('git clone https://github.com/markmuetz/stormtracks')
        with cd('stormtracks'):
            run('virtualenv st_env')

        with prefix('source stormtracks/st_env/bin/activate'):
            run('pip install -r stormtracks/requirements_a.txt')
            run('pip install -r stormtracks/requirements_b.txt --allow-external basemap --allow-unverified basemap')
            run('pip install -e stormtracks')
        run('git clone https://github.com/markmuetz/stormtracks_aws')
    put('aws_credentials/st_worker1.pem', 'Projects/stormtracks_aws/aws_credentials/st_worker1.pem',
        mode=0400)
    put('aws_credentials/credentials.csv', 'Projects/stormtracks_aws/aws_credentials/credentials.csv',
        mode=0400)


@task
def install_supervisor(update=False):
    put('st_worker_files/supervisord.conf', 'supervisord.conf')
    put('st_worker_files/supervisor.conf', 'supervisor.conf')
    sudo('cp supervisord.conf /etc/supervisord.conf')
    sudo('cp supervisor.conf /etc/init/supervisor.conf')
    sudo('pip install supervisor')
    if update:
        sudo('service supervisor restart')
    else:
        sudo('service supervisor start')


@task
def file_exists():
    with quiet():
        have_build_dir = run("test -e /tmp/build").succeeded
        return have_build_dir


@task
def supervisorctl(cmd, program):
    sudo('supervisorctl {0} {1}'.format(cmd, program))


@task
def mount_vol():
    run('mkdir PERSISTENT_DATA')
    sudo('mount /dev/xvdf PERSISTENT_DATA')
