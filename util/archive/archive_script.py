# archive_script.py
#
# Archive free user data
#
# Copyright (C) 2011-2021 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import boto3
import time
import os
import sys
import json
import psycopg2
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('archive_script_config.ini')

s3_resource = boto3.resource('s3', region_name=config["aws"]["AwsRegionName"])
s3_client = boto3.client('s3', region_name=config["aws"]["AwsRegionName"])

glacier_client = boto3.client('glacier', region_name=config["aws"]["AwsRegionName"])

# Connect to DynamoDB
dynamodb_resource = boto3.resource('dynamodb', region_name=config["aws"]["AwsRegionName"])
try:
  table = dynamodb_resource.Table(config['dynamodb']['AwsDynamoDBAnnName'])
except ClientError as e:
  raise Exception(f'Unable to connect to DynamoDB table: {e}')

'''Capstone - Exercise 7
Archive free user results files
'''
def handle_archive_queue(sqs=None):
 
  # Read a message from the queue (5 min after the completion of job)
  messages = sqs.receive_messages(WaitTimeSeconds=20)
  print(f"Read {len(messages)} messages from queue.")

  for message in messages:
    # If message read, extract job parameters from the message body as before
    # get message body
    mbody: str = message.body
    mbody: dict = json.loads(mbody)
    mbody_messages = json.loads(mbody["Message"])["body"]

    # =========== Get user's newest role from DynamoDB ============
    user_id = mbody_messages["user_id"]
    profile = helpers.get_user_profile(id=user_id)
    role = profile[4]

    # Premium user
    if role == "premium_user":
      print("Premium user. Will not archive the result file.")

    # Free user
    else:
      print("Free user. Result file is being archived now.")
      # Get object from S3
      bucket_name = config["s3"]["AwsSQSResultsName"]
      obj = s3_resource.Object(bucket_name, mbody_messages["key_result"])

      # Upload archive to glacier
      # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.upload_archive
      try:
        archive_response = glacier_client.upload_archive(
          vaultName=config["glacier"]["AwsGlacierName"],
          body=obj.get()['Body'].read()
        )
      except ClientError as e:
        raise Exception(f"Cannot archive the object: {e}")

      # Get archive id
      archive_id = archive_response["archiveId"]
      print("An object was archived. Archive id: ")
      print(archive_id)

      # Update table
      job_id = mbody_messages["job_id"]
      table.update_item(
          Key={
              "job_id": job_id
          },
          UpdateExpression="SET result_archive_id=:rai, archived=:ar",
          ExpressionAttributeValues={
              ":rai": archive_id,
              ":ar": True
          },
      )
      print("Archive id is stored into dynamodb.")

      # Delete the object from S3
      # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_object
      response = s3_client.delete_object(
        Bucket=bucket_name,
        Key=mbody_messages["key_result"],
      )
      print("Object deleted from S3.")

    # Delete message
    message.delete()


if __name__ == '__main__':  
  # Get connection to SQS 
  sqs = boto3.resource('sqs', region_name=config["aws"]["AwsRegionName"])
  # Queue name
  queue_name = config["sqs"]["AwsSQSArchiveName"]
  queue = sqs.get_queue_by_name(QueueName=queue_name)

  # Poll queue for new results and process them
  while True:
    handle_archive_queue(sqs=queue)

### EOF
