from __future__ import print_function

import csv
import logging

import boto
import boto.ec2
import boto.sns
from filechunkio import FileChunkIO

log = logging.getLogger('st_runner.aws')


def _get_credentials():
    reader = csv.reader(open('aws_credentials/credentials.csv', 'r'))
    reader.next() # Skip headers
    return reader.next()


def get_ec2_ip_addresses(region, key, value ="*"):
    ip_addresses   = []
    conn   = _create_ec2_connection(region)
    reservations = conn.get_only_instances(filters = {key : value})
    for instance in instances:
        if instance.update() == 'running':
            print("{0} is running".format(instance.ip_address))
            ip_addresses.append(str(instance.ip_address))
    return ip_addresses


def create_ec2_connection(region):
    log.debug("Connecting to {0}".format(region))
    regions = [r.name for r in boto.ec2.regions()]
    if region not in regions:
        raise Exception('Unkown region {0}\n{1}'.format(region, '\n'.join(regions)))

    username, aws_access_key_id, aws_secret_access_key = _get_credentials()

    conn = boto.ec2.connect_to_region(
        region_name = region,
        aws_access_key_id = aws_access_key_id,
        aws_secret_access_key = aws_secret_access_key
    )

    if conn != None:
        log.debug("Connection with AWS established")
    else:
        raise Exception("Connection not created")

    return conn


def upload_large_file(filename):
    username, aws_access_key_id, aws_secret_access_key = _get_credentials()
    conn = boto.connect_s3(aws_access_key_id=aws_access_key_id,
                           aws_secret_access_key=aws_secret_access_key)
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
        with FileChunkIO(source_path, 'r', offset=offset,
                             bytes=bytes) as fp:
            mp.upload_part_from_file(fp, part_num=i + 1)

    # Finish the upload
    mp.complete_upload()


def publish_message():
    conn = create_sns_connection('eu-central-1')
    conn.publish(topic='arn:aws:sns:eu-central-1:787530111813:st',
                 subject='sns test1',
                 message='sns test1')


def create_sns_connection(region):
    print("Connecting to {0}".format(region))
    reader = csv.reader(open('credentials.csv', 'r'))
    reader.next()
    username, aws_access_key_id, aws_secret_access_key = reader.next()

    conn = boto.sns.connect_to_region(
        region_name = region,
        aws_access_key_id = aws_access_key_id,
        aws_secret_access_key = aws_secret_access_key
    )

    if conn != None:
        print("Connection with AWS established")
    else:
        raise Exception("Connection not created")

    return conn
