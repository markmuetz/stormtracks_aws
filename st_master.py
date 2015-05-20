#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
"""
Main executable for interacting with st_workers.
Should be run on local (master or command) computer.
Uses aws_helpers to interact with AWS services, e.g. creating new EC2 instances.
Uses functions in fabfile to run remote commands on EC2 instances.
"""
from __future__ import print_function

import logging
from time import sleep
from argparse import ArgumentParser
import multiprocessing as mp

import argcomplete
from fabric.api import execute, env
from fabric.network import disconnect_all
import commandify as cmdify

import fabfile
import aws_helpers
from aws_helpers import AwsInteractionError
from st_utils import setup_logging
import amis


if __name__ == '__main__':
    log = setup_logging(name='st_master', filename='logs/st_master.log')


@cmdify.main_command
def main_command(args):
    pass


@cmdify.command
def create_instances(conn, args):
    aws_helpers.create_instances(conn, args)


@cmdify.command
def create_image(conn, args):
    instance = aws_helpers.find_instance(conn, args.instance_id)
    image = aws_helpers.create_image(conn, instance.id, args.image_nametag, args)


@cmdify.command
def list_instances(conn, args):
    aws_helpers.list_instances(conn, args)


@cmdify.command
def terminate_instances(conn, args):
    aws_helpers.terminate_instances(conn, args)


@cmdify.command
def find_instance(conn, args):
    aws_helpers.find_instance(conn, args.instance_id)


@cmdify.command
def run_fabric_commands(conn, args):
    instance = aws_helpers.find_instance(conn, args.instance_id)
    host = instance.ip_address
    execute_fabric_commands(args, host)


@cmdify.command
def setup_st_worker_image(conn, args):
    """
    Either creates an image from scratch, starting with a blank Ubuntu image
    and performing a full setup on it, or uses an existing instance to create
    an image.
    """
    if args.create_image_from_scratch:
        images = conn.get_all_images(filters={'tag:name': args.image_nametag})
        if len(images) != 0:
            raise AwsInteractionError('Image with nametag {0} already exists! Delete and try'
                                      'again.'.format(args.image_nametag))

        if args.num_instances != 1:
            raise AwsInteractionError('Should only be one instance for setup_st_worker_image')

        log.info('Creating instance')
        # Use Ubuntu AMI:
        args.image_id = amis.UBUNTU_1404_AMD64_AMI
        instances = aws_helpers.create_instances(conn, args)
        if len(instances) != 1:
            raise AwsInteractionError('Should only have created one'
                                      'instance for setup_st_worker_image')

        instance = instances[0]
        host = instance.ip_address

        log.info('Sleeping for 1m to allow instance to get ready')
        sleep(60)

        log.info('Perform full setup')
        execute(fabfile.full_setup, host=host)
    else:
        instance = aws_helpers.find_instance(conn, args.instance_id)

    log.info('Creating image')
    image = aws_helpers.create_image(conn, instance.id, args.image_nametag, args)

    aws_helpers.terminate_instances(conn, args)

    log.info("Success! Run 'python aws_interaction.py run_analysis'")


@cmdify.command
def match_instances_to_years(instances, years):
    min_years_per_instance = len(years) // len(instances)
    extra_years = len(years) - len(instances) * min_years_per_instance
    instance_to_years_map = {}
    year_index = 0
    for instance in instances:
        years_per_instance = min_years_per_instance
        if extra_years:
            extra_years -= 1
            years_per_instance += 1

        instance_to_years_map[instance] = []
        for i in range(year_index, year_index + years_per_instance):
            instance_to_years_map[instance].append(years[i])

        year_index += years_per_instance

    return instance_to_years_map


@cmdify.command(start_year={'flag': '-s'}, 
                end_year={'flag': '-e'},
                create_new_instances={'flag': '-d'})
def run_analysis(conn, args, create_new_instances=True, start_year=2005, end_year=2005,
                 terminate=True, monitor=True):
    """
    Runs a full analysis.
    Creates EC2 instances as necessary, allows them time to start up. Then executes
    remote commands on them, getting them to download then analyse the given years,
    monitoring their progress. Once they have finished, terminate all running instances.
    """
    log.info('Running analysis: {0}-{1}'.format(args.start_year, args.end_year))
    if not args.allow_multiple_instances and args.num_instances != 1:
        raise AwsInteractionError('Should only be one instance for run_analysis')

    if create_new_instances:
        log.info('Creating instance from image')
        images = conn.get_all_images(filters={'tag:name': args.image_nametag})
        if len(images) != 1:
            raise AwsInteractionError('Should be exactly one image')
        image = images[0]

        args.image_id = image.id

        instances = aws_helpers.create_instances(conn, args)

        if len(instances) != args.num_instances:
            raise AwsInteractionError('Should have created exactly {0} instance(s) for run_analysis\n'
                                      'Created {1}'.format(args.num_instances, len(instances)))
        log.info('Sleeping for 60s to allow instance(s) to get ready')
        sleep(60)
    else:
        log.info('Using existing instances')
        key = "tag:{0}".format(args.tag)
        instances = aws_helpers.get_instances(conn, filters={key: args.tag_value}, running=True)
        log.info('Using instance(s): {0}'.format(', '.join([i.id for i in instances])))

    years = range(args.start_year, args.end_year + 1)
    instance_to_years_map = match_instances_to_years(instances, years)
    log.debug(instance_to_years_map)

    instance_procs = []
    for instance in instances:
        host = instance.ip_address
        log.info('Running on host:{0}, instance_id: {1}'.format(host, instance.id))

        proc = mp.Process(name=host, target=execute_fabric_commands,
                          kwargs={
                              'args': args,
                              'host': host,
                              'years': instance_to_years_map[instance],
                              'monitor': monitor})
        instance_procs.append((instance, proc))

        log.info('Executing fabric commands')
        proc.start()

    while instance_procs:
        finished_instance_procs = []
        for instance, proc in instance_procs:
            proc.join(timeout=0.01)
            if not proc.is_alive():
                log.info("proc {0} finished".format(proc))
                finished_instance_procs.append((instance, proc))
        for instance_proc in finished_instance_procs:
            instance_procs.remove(instance_proc)
            instance, proc = instance_proc
            if monitor and terminate:
                # Don't need to monitor to make sure it's finished.
                print('Terminating instance {0}'.format(instance.id))
                instance.terminate()

        if instance_procs:
            sleep(10)

    log.info('Done')

    if monitor:
        fabfile.notify()


def execute_fabric_commands(args, host, years, monitor):
    """
    Executes remote functions to run analysis on a given year for a given host.
    Monitors their output to see when they are finished (blocking).
    """
    process_log = setup_logging(name='st_master'.format(host),
                                filename='logs/st_master_{0}.log'.format(host),
                                use_console=False)

    process_log.info('Updating stormtracks')
    execute(fabfile.update_stormtracks, host=host)
    execute(fabfile.update_stormtracks_aws, host=host)

    process_log.info('Updating supervisor')
    execute(fabfile.install_supervisor, update=True, host=host)

    process_log.info('Starting anaysis')
    execute(fabfile.st_worker_run, years=years, host=host)

    while not execute(fabfile.log_exists, host=host)[host]:
        process_log.info('Sleeping for 10s to allow creation of logfile')
        sleep(10)
    process_log.info('Logfile created')

    # Must be done after st_worker has started running.
    process_log.info('Logging mem usage')
    execute(fabfile.log_vital_stats, host=host)

    if monitor:
        # Blocks until finished.
        st_worker_status_monitor(process_log, args, host)

        process_log.info('Retrieving logs')
        execute(fabfile.retrieve_logs, host=host)


# @cmdify.command
def st_worker_status_monitor(process_log, args, host):
    """
    Monitor the status of an st_worker, looking for when they have finished their analysis.
    """
    status = execute(fabfile.st_worker_status, host=host)[host]
    process_log.info(status)
    minutes = 0
    while status[:14] != 'analysed years':
        try:
            supervisor_status_str = execute(fabfile.supervisorctl, 
                                            cmd='status', program='st_worker_run', host=host)[host]
            name, supervisor_status =  supervisor_status_str.split()[:2]
            if supervisor_status != 'RUNNING':
                process_log.error('st_worker_run no longer running: {0}'.format(supervisor_status))
                fabfile.beep()
        except Exception as e:
            process_log.error('Problem running supervisorctl'.format(e))
            fabfile.beep()
            raise e

        process_log.info('{0}: {1}, waited for {2}m'.format(host, status, minutes))
        minutes += 1
        sleep(60)
        status = execute(fabfile.st_worker_status, host=host)[host]

    process_log.info('Run full analysis')


@cmdify.command
def st_status(conn, args):
    """
    Gets analysis status of all instances
    """
    key = "tag:{0}".format(args.tag)
    instances = aws_helpers.get_instances(conn, filters={key: args.tag_value}, running=True)
    for instance in instances:
        host = instance.ip_address
        execute(analysis_status, wait=True, host=host)


@cmdify.command
def attach_mount(conn, args):
    """
    Experimental: Attaches a specific mount to an instance.
    """
    instance = aws_helpers.find_instance(conn, args.instance_id)
    conn.attach_volume('vol-dd64eb93', instance.instance_id, '/dev/sdf')
    sleep(5)
    execute(mount_vol, host=instance.ip_address)


def main():
    env.user = "ubuntu"
    env.key_filename = ["aws_credentials/st_worker1.pem"]

    conn = aws_helpers.create_ec2_connection('eu-central-1')

    parser = cmdify.CommandifyArgumentParser(provide_args={'conn': conn},
                                             suppress_warnings=['default_true'])

    parser.add_argument('--instance-id')
    parser.add_argument('--image-id', default=amis.ST_WORKER_IMAGE_CURRENT)
    parser.add_argument('--image-nametag', default=amis.ST_WORKER_IMAGE_NAMETAG)
    parser.add_argument('--create-image-from-scratch', default=False, action='store_true')
    parser.add_argument('-i', '--num-instances', type=int, default=1)
    parser.add_argument('-t', '--tag', default='group')
    parser.add_argument('-v', '--tag-value', default='st_worker')
    parser.add_argument('-n', '--num-ensemble-members', type=int, default=56)
    parser.add_argument('-r', '--region', default='eu-central-1')
    parser.add_argument('-a', '--allow-multiple-instances', default=False, action='store_true')
    parser.add_argument('-d', '--dry-run', default=False, action='store_true')
    parser.add_argument('--instance-type', default='t2.medium')

    parser.setup_arguments()
    argcomplete.autocomplete(parser)
    args = parser.parse_args()
    try:
        parser.dispatch_commands()
    except AwsInteractionError, e:
        log.error(e)
        parser.error(e)
    finally:
        disconnect_all()


if __name__ == '__main__':
    main()
