# annotator_webhook.py
#
# NOTE: This file lives on the AnnTools instance
# Modified to run as a web server that can be called by SNS to process jobs
# Run using: python annotator_webhook.py
#
# Copyright (C) 2011-2022 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import requests
from flask import Flask, jsonify, request, redirect

import json
import os
import subprocess

import boto3
from botocore.client import Config
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

app = Flask(__name__)
environment = 'ann_config.Config'
app.config.from_object(environment)

# Function to generate sqs policy
def allow_sns_to_write_to_sqs(topicarn, queuearn):
    policy_document = """{{
  "Version":"2012-10-17",
  "Statement":[
    {{
      "Sid":"MyPolicy",
      "Effect":"Allow",
      "Principal" : {{"AWS" : "*"}},
      "Action":"SQS:SendMessage",
      "Resource": "{}",
      "Condition":{{
        "ArnEquals":{{
          "aws:SourceArn": "{}"
        }}
      }}
    }}
  ]
}}""".format(queuearn, topicarn)

    return policy_document

# Connect to SQS and get the message queue
sqs = boto3.resource('sqs', region_name=app.config.get("AWS_REGION_NAME"))

# queue name
queue_name = app.config.get("AWS_SQS_REQUESTS_NAME")
# Check if requests queue exists, otherwise create it
try:
  queue = sqs.get_queue_by_name(QueueName=queue_name)
except ClientError as e:
  # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs.html
  # Create a sqs queue
  sqs_client = boto3.client('sqs', region_name=app.config.get("AWS_REGION_NAME"))
  queue = sqs_client.create_queue(
    QueueName=queue_name, 
    Attributes={'ReceiveMessageWaitTimeSeconds': str(app.config.get("AWS_SQS_WAIT_TIME"))}
  )

  # Get connection to sns
  sns_client = boto3.client('sns', region_name=app.config.get("AWS_REGION_NAME"))
  sns_topic_arn_requests = app.config.get("AWS_SNS_REQUESTS_NAME")

  # Get queue arn
  queue_url = sqs_client.get_queue_url(QueueName=app.config.get("AWS_SQS_REQUESTS_NAME"))['QueueUrl']
  sqs_queue_attrs = sqs_client.get_queue_attributes(
    QueueUrl=queue_url,
    AttributeNames=['All']
  )['Attributes']
  sqs_queue_arn = sqs_queue_attrs['QueueArn']

  # Subscribe
  sns_client.subscribe(
    TopicArn=sns_topic_arn_requests,
    Protocol='sqs',
    Endpoint=sqs_queue_arn
  )

  policy_json = allow_sns_to_write_to_sqs(sns_topic_arn_requests, sqs_queue_arn)
  sqs_client.set_queue_attributes(
    QueueUrl = queue_url,
    Attributes = {
      'Policy' : policy_json
    }
  )

  sqs = boto3.resource('sqs', region_name=app.config.get("AWS_REGION_NAME"))
  queue = sqs.get_queue_by_name(QueueName=queue_name)
'''
A13 - Replace polling with webhook in annotator

Receives request from SNS; queries job queue and processes message.
Reads request messages from SQS and runs AnnTools as a subprocess.
Updates the annotations database with the status of the request.
'''
@app.route('/process-job-request', methods=['GET', 'POST'])
def annotate():

  if (request.method == 'GET'):
    return jsonify({
      "code": 405, 
      "error": "Expecting SNS POST request."
    }), 405

  # Check message type
  message_type = request.headers.get("x-amz-sns-message-type")

  # Confirm SNS topic subscription confirmation
  if message_type == "SubscriptionConfirmation":
    print("Subscription confirmation request received.")
    # Get request body
    request_body = json.loads(request.data)

    # Get url
    confirm_url = request_body["SubscribeURL"]

    # Send a get request to the url to confirm subscription
    requests.get(confirm_url)
    print("Subscription confirmed.")
  
  # Process job request notification
  else:
    messages = queue.receive_messages(WaitTimeSeconds=20)
    print(f"Read {len(messages)} messages from queue.")

    for message in messages:
      # If message read, extract job parameters from the message body as before
      # get message body
      mbody: str = message.body
      mbody: dict = json.loads(mbody)
      mbody_messages = json.loads(mbody["Message"])
      
      # extract params from body
      job_id = mbody_messages["job_id"]
      user_id = mbody_messages["user_id"]
      bucket_name = mbody_messages['s3_inputs_bucket']
      file_name = mbody_messages['input_file_name']
      s3_key_input_file = mbody_messages['s3_key_input_file']
      email = mbody_messages['email']
      # role = mbody_messages['role']

      # Include below the same code you used in prior homework
      # Get the input file S3 object and copy it to a local file
      # Use a local directory structure that makes it easy to organize
      # multiple running annotation jobs

      if not os.path.isdir("jobs"):
        os.mkdir("jobs")

      create_folder = f'mkdir ./jobs/{job_id}'
      os.system(create_folder)

      # Download input file from s3
      # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.download_file
      s3 = boto3.resource('s3', region_name=app.config.get("AWS_REGION_NAME"), config=Config(signature_version='s3v4'))
      try:
        s3.meta.client.download_file(bucket_name, s3_key_input_file, file_name)
      except:
        raise Exception(f'Fail to download the file {file_name}. Please check the file is under {path}.')

      # move file to /jobs/<job_id>/
      mv_command = f"mv {file_name} ./jobs/{job_id}"
      os.system(mv_command)

      # Launch annotation job as a background process
      target_path = f"./jobs/{job_id}/{file_name}"
      try:
        subprocess.Popen(["python","run.py", target_path, s3_key_input_file, job_id, email, user_id])
      except:
        raise Exception('Failure to launch the annotator job. Please try again.')

      # Update the “job_status” key in the annotations table to “RUNNING”.
      # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.update_item
      # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LegacyConditionalParameters.AttributeUpdates.html
      dynamodb = boto3.resource('dynamodb', region_name=app.config.get("AWS_REGION_NAME"))
      try:
        table = dynamodb.Table(app.config.get("AWS_DYNAMODB_ANNOTATIONS_TABLE"))
      except ClientError as e:
        raise Exception(f'Unable to connect to DynamoDB table: {e}')

      # Conditinal update: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.update_item
      # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/dynamodb.html#boto3.dynamodb.conditions.Attr
      # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/dynamodb.html#ref-dynamodb-conditions
      try:
        table.update_item(
          Key={
              "job_id": job_id
          },
          UpdateExpression="SET job_status=:jobStatus",
          ExpressionAttributeValues={
              ":jobStatus": "RUNNING"
          },
          ConditionExpression=Attr("job_status").eq("PENDING")
        )
      except:
        raise Exception('Cannot update Dynamodb. Please double check.')

      # Delete the message from the queue, if job was successfully submitted
      # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Message.delete
      message.delete()

  return jsonify({
    "code": 200, 
    "message": "Annotation job request processed."
  }), 200

app.run('0.0.0.0', debug=True)

### EOF