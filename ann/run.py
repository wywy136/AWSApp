# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import sys
import time
import json
import driver
import boto3
import os
from botocore.client import Config
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

sys.path.insert(2, os.path.realpath(os.path.pardir))
import util.helpers

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('ann_config.ini')

sfn_client = boto3.client('stepfunctions', region_name=config['aws']['AwsRegionName'])
sqs_resource = boto3.resource('sqs', region_name=config['aws']['AwsRegionName'])

"""A rudimentary timer for coarse-grained profiling
"""
class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")


if __name__ == '__main__':
    # Call the AnnTools pipeline
    if len(sys.argv) > 1:
        with Timer():
            driver.run(sys.argv[1], 'vcf')

        complete_time = int(time.time())
            
        # Get parameters from subprocess
        file_path = sys.argv[1]
        key = sys.argv[2]
        key_prefix = '/'.join(key.split("/")[:-1])  # ywang27/userX
        job_id = sys.argv[3]
        email = sys.argv[4]
        user_id = sys.argv[5]

        segments = file_path.split('/')
        file_name = segments[-1][:-4]

        bucket_name = config["s3"]["AwsS3ResultsBucket"]

        # Get connection to S3 bucket
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.upload_file
        s3 = boto3.resource('s3', region_name=config["aws"]["AwsRegionName"], config=Config(signature_version='s3v4'))

        # Get current working directory
        cwd = os.getcwd()
        file_name_local = f"{cwd}/jobs/{job_id}/{file_name}.annot.vcf"
        log_file_name_local = f"{cwd}/jobs/{job_id}/{file_name}.vcf.count.log"

        # Upload the annotation result file to S3 bucket
        key_result_file = f"{key_prefix}/{job_id}~{file_name}.annot.vcf"
        try:
            s3.meta.client.upload_file(file_name_local, bucket_name, key_result_file)
        except ClientError as e:
            raise Exception(f'Unable to upload file: {e}')

        # Upload the log file to S3 results bucket
        key_log_file = f"{key_prefix}/{job_id}~{file_name}.vcf.count.log"
        try:
            s3.meta.client.upload_file(log_file_name_local, bucket_name, key_log_file)
        except ClientError as e:
            raise Exception(f'Unable to upload file: {e}')

        # Clean up (delete) local job files
        dl_command = f"rm -rf {cwd}/jobs/{job_id}"
        os.system(dl_command)

        # Updates the job item in DynamoDB table
        dynamodb = boto3.resource('dynamodb', region_name=config["aws"]["AwsRegionName"])
        try:
            table = dynamodb.Table(config['dydb']['AwsDynamoDBAnnotationsName'])
        except ClientError as e:
            raise Exception(f'Unable to connect to DynamoDB table: {e}')

        # Conditional update: job_status=RUNNING
        try:
            table.update_item(
                Key={
                    "job_id": job_id
                },
                UpdateExpression="SET job_status=:js, s3_results_bucket=:rs, s3_key_result_file=:ksf, s3_key_log_file=:klf, complete_time=:ct",
                ExpressionAttributeValues={
                    ":rs": bucket_name,
                    ":ksf": key_result_file,
                    ":klf": key_log_file,
                    ":ct": complete_time,
                    ":js": "COMPLETED"
                },
                ConditionExpression=Attr("job_status").eq("RUNNING")
            )
        except ClientError as e:
            raise Exception(f'Unable to update the table: {e}')

        # Publish notification to ywang27_a12_job_results topic
        sns_client = boto3.client('sns', region_name=config['aws']['AwsRegionName'])

        # Get SNS topic arn
        sns_topic_arn_results = config["sns"]["AwsSNSResultsName"]

        # Make data
        data = {
            "job_id": job_id,
            "complete_time": complete_time,
            "url": f"{config['ses']['AwsSESUrl']}{job_id}",
            "email": email,
            "key_result": key_result_file,
            "user_id": user_id
        }

        # Publish a message to the topic
        # https://docs.aws.amazon.com/sns/latest/dg/sns-publishing.html
        try:
            sns_response = sns_client.publish(
                TopicArn=sns_topic_arn_results,
                Message=json.dumps({'default': json.dumps(data)}),
                MessageStructure='json'
            )
        except ClientError as e:
            raise Exception(f'Unable to publish the SNS message: {e}')

        # =========== Invoke step function ============
        # The step function will wait 5 min and then publish a message to archive SNS topic
        # Start step function
        try:
            response = sfn_client.start_execution(
                stateMachineArn=config['sfn']['AwsArchiveName'],
                name=job_id,
                input=json.dumps(data),
            )
        except ClientError as e:
            raise Exception(f"Cannot start the step function execution: {e}")
        print("Step function execution started. After 5 min the result file will be archived if the user is free.")  

    else:
        print("A valid .vcf file must be provided as input to this program.")
