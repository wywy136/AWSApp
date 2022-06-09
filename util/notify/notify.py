# notify.py
#
# Notify users of job completion
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

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser
config = ConfigParser(os.environ)
config.read('notify_config.ini')

'''Capstone - Exercise 3(d)
Reads result messages from SQS and sends notification emails.
'''
def handle_results_queue(sqs=None):

  # Read messages from the queue
  messages = sqs.receive_messages(WaitTimeSeconds=20)
  print(f"Read {len(messages)} messages from queue.")

  for message in messages:
      # If message read, extract job parameters from the message body as before
      # get message body
      mbody: str = message.body
      mbody: dict = json.loads(mbody)
      mbody_messages = json.loads(mbody["Message"])

      job_id = mbody_messages["job_id"]
      complete_time = mbody_messages["complete_time"]
      url = mbody_messages["url"]
      email = mbody_messages["email"]

      # Process message
      try:
        ses_response = helpers.send_email_ses(
          recipients=email,
          sender=config["gas"]["MailDefaultSender"],
          subject=f"Results available for job {job_id}",
          body=f"Your annotation job completed at {complete_time}. Click here to view job details and results: {url}."
        )
      except ClientError as e:
        raise Exception(f'Unable to send email: {e}')

      # Delete message
      message.delete()
  

if __name__ == '__main__':
  
  # Get handles to resources; and create resources if they don't exist
  # Get connection to SQS
  sqs = boto3.resource('sqs', region_name=config["aws"]["AwsRegionName"])
  queue_name = config["sqs"]["AwsSQSResultsName"]
  try:
      queue = sqs.get_queue_by_name(QueueName=queue_name)
  except:
      raise Exception(f'Failure to get queue {queue_name}. Please try again.')

  # Poll queue for new results and process them
  while True:
    handle_results_queue(sqs=queue)

### EOF
