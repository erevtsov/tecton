import os

import dagster_aws.s3 as s3


def get_s3_resource():
    """
    Returns an S3 resource configured with AWS credentials from environment variables.
    """
    # Ensure that the required environment variables are set
    if 'AWS_ACCESS_KEY_ID' not in os.environ or 'AWS_SECRET_ACCESS_KEY' not in os.environ:
        raise OSError('AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set in the environment variables.')

    # Create and return the S3 resource
    return s3.S3Resource(
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
    )
