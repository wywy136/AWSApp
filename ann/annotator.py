import uuid
import subprocess
import os
import shutil
import boto3
from botocore.client import Config
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
import json

# Get configuration
from configparser import ConfigParser


if __name__ == "__main__":
    config = ConfigParser(os.environ)
    config.read('ann_config.ini')

    # Connect to SQS and get the message queue
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#queue
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs.html
    # sqs resource
    sqs = boto3.resource('sqs', region_name=config["aws"]["AwsRegionName"])

    # queue name
    queue_name = config["sqs"]["AwsSQSRequestsName"]
    try:
        queue = sqs.get_queue_by_name(QueueName=queue_name)
    except:
        raise Exception(f'Failure to get queue {queue_name}. Please try again.')

    # Poll the message queue in a loop 
    while True:
      
        # Attempt to read a message from the queue
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Queue.receive_messages
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
            s3 = boto3.resource('s3', region_name=config["aws"]["AwsRegionName"], config=Config(signature_version='s3v4'))
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
                subprocess.Popen(["python","run.py", target_path, s3_key_input_file, job_id, email])
            except:
                raise Exception('Failure to launch the annotator job. Please try again.')

            # Update the “job_status” key in the annotations table to “RUNNING”.
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.update_item
            # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LegacyConditionalParameters.AttributeUpdates.html
            dynamodb = boto3.resource('dynamodb', region_name=config["aws"]["AwsRegionName"])
            try:
                table = dynamodb.Table('ywang27_annotations')
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
