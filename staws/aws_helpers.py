"""
Collection of utilities for interacting with AWS services.
Uses `boto` to interact with AWS.

Mainly uses the EC2 compute and S3 storage services.
Credentials must be placed in file aws_credentials/credentials.csv (rel to .)
"""
from __future__ import print_function

import os
import sys
import csv
import logging
import math
from time import sleep
import datetime as dt

import boto
import boto.ec2
import boto.sns
from filechunkio import FileChunkIO

log = logging.getLogger('st_master.aws')


class AwsInteractionError(Exception):
    pass


def _get_credentials():
    """
    Reads and returns credentials as:
    username, aws_access_key_id, aws_secret_access_key
    """
    reader = csv.reader(open('aws_credentials/credentials.csv', 'r'))
    reader.next()  # Skip headers
    return reader.next()


def create_ec2_connection(region):
    """
    Creates EC2 connection to the given region using credentials
    """
    log.debug("Connecting to {0}".format(region))
    regions = [r.name for r in boto.ec2.regions()]
    if region not in regions:
        raise Exception('Unkown region {0}\n{1}'.format(region, '\n'.join(regions)))

    username, aws_access_key_id, aws_secret_access_key = _get_credentials()

    conn = boto.ec2.connect_to_region(
        region_name=region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    if conn is not None:
        log.debug("Connection with AWS established")
    else:
        raise Exception("Connection not created")

    return conn


def get_ec2_ip_addresses(region, key, value="*"):
    """
    Returns IP adresses of all servers with a given key/value
    """
    ip_addresses = []
    conn = create_ec2_connection(region)
    instances = conn.get_only_instances(filters={key: value})
    for instance in instances:
        if instance.update() == 'running':
            log.debug("{0} is running".format(instance.ip_address))
            ip_addresses.append(str(instance.ip_address))
    return ip_addresses


def create_instances(conn, args):
    """
    Creates instance(s) using args.
    """
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
    dev_sda1.size = 18  # size in Gigabytes
    bdm['/dev/sda1'] = dev_sda1

    reservations = conn.run_instances(args.image_id,
                                      min_count=args.num_instances,
                                      max_count=args.num_instances,
                                      key_name='st_worker1',
                                      instance_type=args.instance_type,
                                      security_groups=['st_worker_security'],
                                      block_device_map=bdm)

    if len(reservations.instances) != args.num_instances:
        raise Exception('Not enough instances created ({0}/{1})'.
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


def list_instances(conn, args, running=True):
    """
    Lists all running instances.
    """
    print(args)
    key = "tag:{0}".format(args.tag)
    instances = get_instances(conn, filters={key: args.tag_value}, running=running)
    log.debug('key: {0}, value: {1}'.format(key, args.tag_value))
    for i, instance in enumerate(instances):
        log.info('Instance {0}'.format(i))
        log.info('    inst id   : {0}'.format(instance.id))
        log.info('    state     : {0}'.format(instance.update()))
        log.info('    IP address: {0}'.format(instance.ip_address))
        log.info('    conn cmd  : "ssh -i aws_credentials/st_worker1.pem ubuntu@{0}"'.
                 format(instance.ip_address))


def terminate_instance(conn, args, instance):
    """
    Terminates given instance.
    """
    log.info('Terminating instance: {0}'.format(instance.id))
    instance.terminate()
    while not instance.update() != 'terminated':
        sleep(1)
        log.debug(instance.state)
    log.info('Instance terminated')


def terminate_instances(conn, args):
    """
    Terminates all running instances with given tag/value.
    """
    key = "tag:{0}".format(args.tag)
    instances = get_instances(conn, filters={key: args.tag_value})
    for instance in instances:
        log.info('Terminating instance: {0}'.format(instance.id))
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
    """
    Find a specific instance based on its ID.
    """
    instances = get_instances(conn, filters={'instance-id': instance_id}, running=running)
    if len(instances) != 1:
        log.error('Found {0} instances'.format(len(instances)))
        raise AwsInteractionError('Filtering on instance ID should only return one instance')
    return instances[0]


def get_instances(conn, filters, running=True):
    """
    Get all instances subject to filters and wheter or not they are running.
    """
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
    """
    Creates an AMI image from the given instance ID.

    Can take a while and will force reboot of instance.
    """
    log.info('Creating image from instance: {0}'.format(instance_id))
    instance = find_instance(conn, instance_id)
    name = dt.datetime.strftime(dt.datetime.now(), 'st_worker_ubuntu-14-04_%Y-%m-%d-%H-%M')
    image_id = instance.create_image(name, description='stormtracks worker, stormtracks installed')
    image = conn.get_all_images(filters={'image-id': image_id})[0]
    log.info('Waiting for image to become available')
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


def create_s3_connection():
    username, aws_access_key_id, aws_secret_access_key = _get_credentials()
    conn = boto.connect_s3(aws_access_key_id=aws_access_key_id,
                           aws_secret_access_key=aws_secret_access_key)
    return conn


def list_files(bucket_name='stormtracks_data'):
    conn = create_s3_connection()
    b = conn.get_bucket(bucket_name)
    for key in b.list():
        print(key.key)


def get_all_files(bucket_name='stormtracks_data', 
                  directory='/home/markmuetz/stormtracks_data/output/prod_release_1'):
    conn = create_s3_connection()
    b = conn.get_bucket(bucket_name)
    for key in b.list():
        get_file(key, directory)


def get_file_from_name(filename, 
                       bucket_name='stormtracks_data',
                       directory='/home/markmuetz/stormtracks_data/output/prod_release_1'):
    conn = create_s3_connection()
    b = conn.get_bucket(bucket_name)
    key = b.get_key(filename)
    get_file(key, directory)


def get_file(key, directory='/home/markmuetz/stormtracks_data/output/prod_release_1'):
    filename = os.path.join(directory, key.key)
    if os.path.exists(filename):
        print('File {0} exists, skipping'.format(key.key))
        return

    downloaded = False
    tries = 0
    while not downloaded:
        print('Downloading: {0}'.format(key.key))
        tries += 1
        try:
            key.get_contents_to_filename(filename)
            downloaded = True
        except Exception as e:
            print('PROBLEM DOWNLOADING FILE:')
            print(e)
            if tries <= 3:
                print('Try again.')
                continue
            else:
                if os.path.exists(filename):
                    print('DELETING FILE')
                    os.remove(filename)
                print('Giving up.')
                break
        except KeyboardInterrupt:
            print('KeyboardInterrupt')
            if os.path.exists(filename):
                print('DELETING FILE')
                os.remove(filename)
            sys.exit(1)


def upload_large_file(filename):
    """
    Uploads a large file to AWS S3.
    """
    conn = create_s3_connection()
    b = conn.get_bucket('stormtracks_data')

    # Get file info
    source_path = filename
    source_size = os.stat(source_path).st_size

    # Create a multipart upload request
    mp = b.initiate_multipart_upload(os.path.basename(source_path))

    # Use a chunk size of 5 MiB
    # Smallest size possible:
    chunk_size = 5242880
    chunk_count = int(math.ceil(source_size / float(chunk_size)))

    # Send the file parts, using FileChunkIO to create a file-like object
    # that points to a certain byte range within the original file. We
    # set bytes to never exceed the original file size.
    for i in range(chunk_count):
        log.debug('Uploading chunk {0}'.format(i + 1))
        offset = chunk_size * i
        bytes = min(chunk_size, source_size - offset)
        with FileChunkIO(source_path, 'r', offset=offset, bytes=bytes) as fp:
            mp.upload_part_from_file(fp, part_num=i + 1)

    # Finish the upload
    mp.complete_upload()


def publish_message():
    """
    Experimental: publishes message to SNS.
    """
    conn = create_sns_connection('eu-central-1')
    conn.publish(topic='arn:aws:sns:eu-central-1:787530111813:st',
                 subject='sns test1',
                 message='sns test1')


def create_sns_connection(region):
    print("Connecting to {0}".format(region))
    username, aws_access_key_id, aws_secret_access_key = _get_credentials()

    conn = boto.sns.connect_to_region(
        region_name=region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    if conn is not None:
        print("Connection with AWS established")
    else:
        raise Exception("Connection not created")

    return conn
