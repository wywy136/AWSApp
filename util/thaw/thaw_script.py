# thaw_script.py
#
# Thaws upgraded (premium) user data
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
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('thaw_script_config.ini')

# Connect to DynamoDB
dynamodb_resource = boto3.resource('dynamodb', region_name=config["aws"]["AwsRegionName"])
try:
  table = dynamodb_resource.Table(config['dynamodb']['AwsDynamoDBAnnName'])
except ClientError as e:
  raise Exception(f'Unable to connect to DynamoDB table: {e}')

# Connect to Glacier
glacier_client = boto3.client('glacier', region_name=config["aws"]["AwsRegionName"])

'''Capstone - Exercise 9
Initiate thawing of archived objects from Glacier
'''
def handle_thaw_queue(sqs=None):
  
  # Read messages from the queue
  messages = sqs.receive_messages(WaitTimeSeconds=20)
  print(f"Read {len(messages)} messages from queue.")

  for message in messages:
    mbody: str = message.body
    mbody: dict = json.loads(mbody)
    mbody_messages = json.loads(mbody["Message"])
    print(mbody_messages)

    user_id = mbody_messages["user_id"]
    print(f"Thaw request got for user {user_id}")

    # Query the table with the user_id
    # Using global secondary index created before
    try:
      # use global secondary index
      query_results = table.query(
        IndexName=config['dynamodb']['AwsDynamoDBAnnSecondaryIndex'],
        KeyConditionExpression=Key('user_id').eq(user_id)
      )
    except Exception as e:
      print(f"Failed to query data from table: {e}")
      continue

    items = query_results.get("Items", [])
    print(f"Queried in total {len(items)} items associated with user: {user_id}")
    unrestored = 0
    for item in items:
      if "result_archive_id" in item:
        unrestored += 1
    print(f"# of archived job(s): {unrestored} . Now thawing.")

    for item in items:
      # If the result file has been archived
      if "result_archive_id" not in item:
        continue
      else:
        archive_id = item["result_archive_id"]
        job_id = item["job_id"]
        # Initiate an archive-retrieval job
        # When the job finished, publish a message to SNS topic
        # First try expedited retrieval
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.initiate_job
        thaw_job_id = None
        try:
          response = glacier_client.initiate_job(
            vaultName=config["glacier"]["AwsGlacierName"],
            jobParameters={
              "Type": "archive-retrieval",
              "ArchiveId": archive_id,
              "Tier": "Expedited",
              "SNSTopic": config["sns"]["AwsSNSRestoreTopic"],
              "Description": job_id
            }
          )
          thaw_job_id = response["jobId"]
        except ClientError as e:
          # Failed expedited retrieval
          if e.response['Error']['Code'] == 'InsufficientCapacityException':
            print("Expedited retrieval request fails. Attempting standard retrieval.")
            # Try standard retrieval
            try:
              response = glacier_client.initiate_job(
                vaultName=config["glacier"]["AwsGlacierName"],
                jobParameters={
                  "Type": "archive-retrieval",
                  "ArchiveId": archive_id,
                  "Tier": "Standard",
                  "SNSTopic": config["sns"]["AwsSNSRestoreTopic"],
                  "Description": job_id
                }
              )
              thaw_job_id = response["jobId"]
            except ClientError as e:
              raise Exception(f"Cannot initiate the retrieval job: {e}")
          else:
            raise Exception(f"Cannot initiate the retrieval job: {e}")

        print(f"Retrieval job initiated for job_id: {job_id}")
        print(f"Retrieval job id: {thaw_job_id}")

    # Delete message
    message.delete()


if __name__ == '__main__':  

  # Get handles to resources; and create resources if they don't exist
  sqs = boto3.resource('sqs', region_name=config["aws"]["AwsRegionName"])
  # Queue name
  queue_name = config["sqs"]["AwsSQSThawName"]
  queue = sqs.get_queue_by_name(QueueName=queue_name)

  # Poll queue for new results and process them
  while True:
    handle_thaw_queue(sqs=queue)

### EOF