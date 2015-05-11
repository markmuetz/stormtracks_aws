import sys
sys.path.insert(0, '..')
import os

import aws_helpers
from aws_helpers import AwsInteractionError
os.chdir('..')

REGION = 'eu-central-1'

class TestAwsConnections:
    def test_1_credentials(self):
        """Check that can find credentials"""
        credentials = aws_helpers._get_credentials()

    def test_2_ec2_connection(self):
        """Check can create ec2 connection"""
        conn = aws_helpers.create_ec2_connection(REGION)
