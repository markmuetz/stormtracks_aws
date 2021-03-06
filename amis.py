"""
Useful AMI Images for launching EC2 instances.
"""
# ami-accff2b1 is Ubuntu Trusty 14.04 AMD64 AMI:
UBUNTU_1404_AMD64_AMI = 'ami-accff2b1'
# First image built from 'ami-accff2b1' after doing full_setup:
ST_WORKER_IMAGE_1 = 'ami-4acff057'
# Built after modifying ST_WORKER_IMAGE_1 - changing file structure.
ST_WORKER_IMAGE_2 = 'ami-06ad921b'
# Built after modifying ST_WORKER_IMAGE_2 - deleting 2003's data
ST_WORKER_IMAGE_3 = 'ami-38704e25'
# Convenient for using as default.
ST_WORKER_IMAGE_CURRENT = ST_WORKER_IMAGE_3
ST_WORKER_IMAGE_NAMETAG = 'st_worker_image_3'
