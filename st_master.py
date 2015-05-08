from __future__ import print_function

import logging
from time import sleep
from argparse import ArgumentParser
import datetime as dt

from fabric.network import disconnect_all
from fabfile import full_setup, update_stormtracks, st_worker_run, st_worker_status

from aws_helpers import create_ec2_connection
from st_utils import setup_logging

log = setup_logging(name='st_master', filename='logs/st_master.log')

# ami-accff2b1 is Ubuntu Trusty 14.04 AMD64 AMI
UBUNTU_1404_AMD64_AMI = 'ami-accff2b1'

class AwsInteractionError(Exception):
    pass


def main(args):
    log.info('Action: {0}'.format(args.action))

    conn = create_ec2_connection(args.region)
    if args.action == 'create_instances':
        create_instances(conn, args)
    if args.action == 'create_image':
        create_image(conn, args.instance_id, args)
    elif args.action == 'terminate_instances':
        terminate_instances(conn, args)
    elif args.action == 'list_instances':
        list_instances(conn, args)
    elif args.action == 'st_status':
        st_status(conn, args)
    elif args.action == 'run_2005':
        run_2005(conn, args)
    elif args.action == 'attach_mount':
        attach_mount(conn, args)
    elif args.action == 'setup_st_worker_image':
        setup_st_worker_image(conn, args)
    elif args.action == 'find_instance':
        find_instance(conn, args.instance_id)
    elif args.action == 'run_analysis':
        run_analysis(conn, args)
    elif args.action == 'execute_fabric_commands':
        instance = find_instance(conn, args.instance_id)
        host = instance.ip_address
        execute_fabric_commands(args, host)
    else:
        raise AwsInteractionError('Unkown action: {0}'.format(args.action))


def st_status(conn, args):
    key = "tag:{0}".format(args.tag)
    instances = get_instances(conn, filters={key: args.tag_value}, running=True)
    for instance in instances:
        host = instance.ip_address
        execute(analysis_status, wait=True, host=host)


def attach_mount(conn, args):
    instance = find_instance(conn, args.instance_id)
    conn.attach_volume('vol-dd64eb93', instance.instance_id, '/dev/sdf')
    sleep(5)
    execute(mount_vol, host=instance.ip_address)


def setup_st_worker_image(conn, args):
    image_nametag = 'st_worker_image_1'
    if False:
        images = conn.get_all_images(filters={'tag:name': image_nametag})
        if len(images) != 0:
            raise AwsInteractionError('Image with nametag {0} already exists! Delete and try'
                    'again.'.format(image_nametag))

        if args.num_instances != 1:
            raise AwsInteractionError('Should only be one instance for setup_st_worker_image')

        log.info('Creating instance')
        # Use Ubuntu AMI:
        args.image_id = UBUNTU_1404_AMD64_AMI
        instances = create_instances(conn, args)
        if len(instances) != 1:
            raise AwsInteractionError('Should only have created one instance for setup_st_worker_image')

        instance = instances[0]
        host = instance.ip_address

        log.info('Sleeping for 1m to allow instance to get ready')
        sleep(60)

        log.info('Perform full setup')
        execute(full_setup, host=host)
    else:
        instance = find_instance(conn, args.instance_id)

    log.info('Creating image')
    image = create_image(conn, instance.instance_id, image_nametag, args)

    terminate_instances(conn, args)

    log.info("Success! Run 'python aws_interaction.py run_analysis'")


def run_analysis(conn, args):
    log.info('Running analysis: {0}-{1}'.format(args.start_year, args.end_year))
    image_nametag = 'st_worker_image_1'
    if args.num_instances != 1:
        raise AwsInteractionError('Should only be one instance for run_analysis')

    log.info('Creating instance from image')
    images = conn.get_all_images(filters={'tag:name': image_nametag})
    if len(images) != 1:
        raise AwsInteractionError('Should be exactly one image')
    image = images[0]

    args.image_id = image.instance_id
    instances = create_instances(conn, args)
    if len(instances) != 1:
        raise AwsInteractionError('Should only have created one instance for run_analysis')

    instance = instances[0]
    host = instance.ip_address
    log.info('Running on host:{0}, instance_id: {1}'.format(host, instance.instance_id))

    log.info('Sleeping for 60s to allow instance to get ready')
    sleep(60)

    log.info('Executing fabric commands')
    execute_fabric_commands(args, host)

    log.info('Terminating all instances')
    terminate_instances(conn, args)

    log.info('Done')


def execute_fabric_commands(args, host):
    log.info('Updating stormtracks')
    execute(update_stormtracks, host=host)

    log.info('Starting anaysis')
    execute(st_worker_run, start_year=args.start_year, end_year=args.end_year, host=host)

    log.info('Sleeping for 20s to allow creation of logfile')

    # TODO: Poll for file creation.
    sleep(20)
    status = execute(st_worker_status, host=host)
    log.info(status[host])
    minutes = 0
    while wait and status[host][:8] != 'analysed':
        log.info('{0}: Waited for {1}m'.format(status[host], minutes))
        minutes += 1
        sleep(60)
        status = execute(st_worker_status, host=host)

    if wait:
        log.info('Run full analysis')


def list_instances(conn, args, running=True):
    key = "tag:{0}".format(args.tag)
    instances = get_instances(conn, filters={key: args.tag_value}, running=running)
    for i, instance in enumerate(instances):
        log.info('Instance {0}'.format(i))
        log.info('    inst id   : {0}'.format(instance.instance_id))
        log.info('    state     : {0}'.format(instance.update()))
        log.info('    IP address: {0}'.format(instance.ip_address))
        log.info('    conn cmd  : "ssh -i st_worker1.pem ubuntu@{0}"'.format(instance.ip_address))


def terminate_instances(conn, args):
    key = "tag:{0}".format(args.tag)
    instances = get_instances(conn, filters={key: args.tag_value})
    for instance in instances:
        log.info('Terminating instance: {0}'.format(instance.instance_id))
        instance.terminate()

    all_instances_terminated = False
    while not all_instances_terminated:
        sleep(1)
        all_instances_terminated = True
        terminated_count = 0
        for instance in instances:
            if instance.update() != 'terminated':
                all_instances_terminated = False
                log.debug(instance.state)
            else:
                terminated_count += 1

        log.info('Instances terminated: ({0}/{1})'.format(terminated_count, len(instances))) 
    log.info('All instances terminated') 


def find_instance(conn, instance_id, running=True):
    instances = get_instances(conn, filters={'instance-id': instance_id}, running=running)
    if len(instances) != 1:
        log.error('Found {0} instances'.format(len(instances)))
        raise AwsInteractionError('Filtering on instance ID should only return one instance')
    return instances[0]


def get_instances(conn, filters, running=True):
    ret_instances = []
    instances = conn.get_only_instances(filters=filters)
    for instance in instances:
        if running:
            if instance.update() == 'running':
                log.debug("Instance running {0}".format(instance.public_dns_name))
                ret_instances.append(instance)
        else:
            log.debug("Instance {0} {1}".format(instance.update(), instance.public_dns_name))
            ret_instances.append(instance)
                
    return ret_instances


def create_image(conn, instance_id, image_nametag, args):
    log.info('Creating image from instance: {0}'.format(instance_id))
    instance = find_instance(conn, instance_id)
    name = dt.datetime.strftime(dt.datetime.now(), 'st_worker_ubuntu-14-04_%Y-%m-%d-%H-%M')
    image_id = instance.create_image(name, description='stormtracks worker, stormtracks installed')
    image = conn.get_all_images(filters={'image-id': image_id})[0]
    log.info('Waiting for instance to become available')
    while image.update() != 'available':
        log.info(image.state)
        sleep(10)
    log.info('Image successfully created, id: {0}'.format(image_id))

    log.info('Waiting for instance to reboot')
    while instance.update() != 'running':
        log.info(instance.state)
        sleep(1)
    log.info('Instance has rebooted')

    image.add_tag(args.tag, args.tag_value)
    image.add_tag('name', image_nametag)

    return image


def create_instances(conn, args):
    if not args.allow_multiple_instances:
        key = "tag:{0}".format(args.tag)
        instances = get_instances(conn, filters={key: args.tag_value})
        if len(instances) != 0:
            raise AwsInteractionError('Already running instance and allow-multiple-instances=False')

    log.info('Creating {0} instances of type {1} with tag/value: {2}/{3}'.format(args.num_instances,
                                                                                 args.instance_type,
                                                                                 args.tag,
                                                                                 args.tag_value))
    log.info('Using image id: {0}'.format(args.image_id))

    # Create block device mapping.
    bdm = boto.ec2.blockdevicemapping.BlockDeviceMapping()
    dev_sda1 = boto.ec2.blockdevicemapping.EBSBlockDeviceType(delete_on_termination=True)
    dev_sda1.size = 18 # size in Gigabytes
    bdm['/dev/sda1'] = dev_sda1 

    reservations = conn.run_instances(args.image_id, 
                                      min_count=args.num_instances,
                                      max_count=args.num_instances,
                                      key_name='st_worker1', 
                                      instance_type=args.instance_type, 
                                      security_groups=['st_worker_security'],
                                      block_device_map=bdm)

    if len(reservations.instances) != args.num_instances:
        raise Exception('Not enough instances created ({0}/{1})'.\
            format(len(reservations.instances), args.args.num_instances))

    for instance in reservations.instances:
        instance.add_tag(args.tag, args.tag_value)

    all_instances_running = False
    running_instances = []
    while not all_instances_running:
        sleep(5)
        all_instances_running = True
        running_count = 0
        for instance in reservations.instances:
            if instance.update() != 'running':
                all_instances_running = False
                log.debug(instance.state)
            else:
                running_count += 1
                if instance not in running_instances:
                    running_instances.append(instance)
        log.info('Instances running: ({0}/{1})'.format(running_count, args.num_instances)) 

    # conn.attach_volume('vol-dd64eb93', instance.id, '/dev/sdf')

    log.info('Created {0} instances'.format(len(running_instances)))
    return running_instances


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('action')
    parser.add_argument('--instance-id')
    parser.add_argument('--image-id')
    parser.add_argument('-i', '--num-instances', type=int, default=1)
    parser.add_argument('-t', '--tag', default='group')
    parser.add_argument('-v', '--tag-value', default='st_worker')
    parser.add_argument('-s', '--start-year', type=int, default=2005)
    parser.add_argument('-e', '--end-year', type=int, default=2005)
    parser.add_argument('-n', '--num-ensemble-members', type=int, default=56)
    parser.add_argument('-r', '--region', default='eu-central-1')
    parser.add_argument('-a', '--allow-multiple-instances', default=False, action='store_true')
    parser.add_argument('--instance-type', default='t2.small')
    args = parser.parse_args()

    return parser, args


if __name__ == '__main__':
    parser, args = parse_args()
    try:
        main(args)
    except AwsInteractionError, e:
        log.error(e)
        parser.error(e)
    finally:
        disconnect_all()
