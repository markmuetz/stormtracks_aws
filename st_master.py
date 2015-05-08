#!/usr/bin/env python
from __future__ import print_function

import logging
from time import sleep
from argparse import ArgumentParser

from fabric.api import execute
from fabric.network import disconnect_all

import fabfile
import aws_helpers
from st_utils import setup_logging
import amis

log = setup_logging(name='st_master', filename='logs/st_master.log')

def main(args):
    log.info('Action: {0}'.format(args.action))

    conn = aws_helpers.create_ec2_connection(args.region)
    if args.action == 'create_instances':
        aws_helpers.create_instances(conn, args)
    if args.action == 'create_image':
        instance = aws_helpers.find_instance(conn, args.instance_id)
        image = aws_helpers.create_image(conn, instance.id, args.image_nametag, args)
    elif args.action == 'terminate_instances':
        aws_helpers.terminate_instances(conn, args)
    elif args.action == 'list_instances':
        aws_helpers.list_instances(conn, args)
    elif args.action == 'st_status':
        st_status(conn, args)
    elif args.action == 'attach_mount':
        attach_mount(conn, args)
    elif args.action == 'setup_st_worker_image':
        setup_st_worker_image(conn, args)
    elif args.action == 'find_instance':
        aws_helpers.find_instance(conn, args.instance_id)
    elif args.action == 'run_analysis':
        run_analysis(conn, args)
    elif args.action == 'execute_fabric_commands':
        instance = aws_helpers.find_instance(conn, args.instance_id)
        host = instance.ip_address
        execute_fabric_commands(args, host)
    elif args.action == 'st_worker_status_monitor':
        instance = aws_helpers.find_instance(conn, args.instance_id)
        host = instance.ip_address
        st_worker_status_monitor(args, host)
    else:
        raise aws_helpers.AwsInteractionError('Unkown action: {0}'.format(args.action))


def st_status(conn, args):
    key = "tag:{0}".format(args.tag)
    instances = aws_helpers.get_instances(conn, filters={key: args.tag_value}, running=True)
    for instance in instances:
        host = instance.ip_address
        execute(analysis_status, wait=True, host=host)


def attach_mount(conn, args):
    instance = aws_helpers.find_instance(conn, args.instance_id)
    conn.attach_volume('vol-dd64eb93', instance.instance_id, '/dev/sdf')
    sleep(5)
    execute(mount_vol, host=instance.ip_address)


def setup_st_worker_image(conn, args):
    if False:
        images = conn.get_all_images(filters={'tag:name': args.image_nametag})
        if len(images) != 0:
            raise aws_helpers.AwsInteractionError('Image with nametag {0} already exists! Delete and try'
                    'again.'.format(args.image_nametag))

        if args.num_instances != 1:
            raise aws_helpers.AwsInteractionError('Should only be one instance for setup_st_worker_image')

        log.info('Creating instance')
        # Use Ubuntu AMI:
        args.image_id = amis.UBUNTU_1404_AMD64_AMI
        instances = aws_helpers.create_instances(conn, args)
        if len(instances) != 1:
            raise aws_helpers.AwsInteractionError('Should only have created one instance for setup_st_worker_image')

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


def run_analysis(conn, args):
    log.info('Running analysis: {0}-{1}'.format(args.start_year, args.end_year))
    if args.num_instances != 1:
        raise aws_helpers.AwsInteractionError('Should only be one instance for run_analysis')

    log.info('Creating instance from image')
    images = conn.get_all_images(filters={'tag:name': args.image_nametag})
    if len(images) != 1:
        raise aws_helpers.AwsInteractionError('Should be exactly one image')
    image = images[0]

    args.image_id = image.id
    instances = aws_helpers.create_instances(conn, args)
    if len(instances) != 1:
        raise aws_helpers.AwsInteractionError('Should only have created one instance for run_analysis')

    instance = instances[0]
    host = instance.ip_address
    log.info('Running on host:{0}, instance_id: {1}'.format(host, instance.id))

    log.info('Sleeping for 60s to allow instance to get ready')
    sleep(60)

    log.info('Executing fabric commands')
    execute_fabric_commands(args, host)

    log.info('Terminating all instances')
    aws_helpers.terminate_instances(conn, args)

    log.info('Done')
    fabfile.notify()


def execute_fabric_commands(args, host):
    log.info('Updating stormtracks')
    execute(fabfile.update_stormtracks, host=host)
    execute(fabfile.update_stormtracks_aws, host=host)

    log.info('Starting anaysis')
    execute(fabfile.st_worker_run, start_year=args.start_year, end_year=args.end_year, host=host)

    log.info('Sleeping for 20s to allow creation of logfile')

    # TODO: Poll for file creation.
    sleep(20)
    st_worker_status_monitor(args, host)


def st_worker_status_monitor(args, host):
    status = execute(fabfile.st_worker_status, host=host)
    log.info(status[host])
    minutes = 0
    while status[host][:14] != 'analysed years':
        log.info('{0}: Waited for {1}m'.format(status[host], minutes))
        minutes += 1
        sleep(60)
        status = execute(fabfile.st_worker_status, host=host)

    log.info('Run full analysis')


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('action')
    parser.add_argument('--instance-id')
    parser.add_argument('--image-id', default=amis.ST_WORKER_IMAGE_CURRENT)
    parser.add_argument('--image-nametag', default=amis.ST_WORKER_IMAGE_NAMETAG)
    parser.add_argument('-i', '--num-instances', type=int, default=1)
    parser.add_argument('-t', '--tag', default='group')
    parser.add_argument('-v', '--tag-value', default='st_worker')
    parser.add_argument('-s', '--start-year', type=int, default=2005)
    parser.add_argument('-e', '--end-year', type=int, default=2005)
    parser.add_argument('-n', '--num-ensemble-members', type=int, default=56)
    parser.add_argument('-r', '--region', default='eu-central-1')
    parser.add_argument('-a', '--allow-multiple-instances', default=False, action='store_true')
    parser.add_argument('--instance-type', default='t2.medium')
    args = parser.parse_args()

    return parser, args


if __name__ == '__main__':
    parser, args = parse_args()
    try:
        main(args)
    except aws_helpers.AwsInteractionError, e:
        log.error(e)
        parser.error(e)
    finally:
        disconnect_all()
