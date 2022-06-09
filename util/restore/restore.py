# restore.py
#
# Restores thawed data, saving objects to S3 results bucket
# NOTE: This code is for an AWS Lambda function
#
# Copyright (C) 2011-2021 Vas Vasiliadis
# University of Chicago
##

import json
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
import boto3


def lambda_handler(event, context):
    
    AwsRegionName = "us-east-1"
    AwsDynamoDBAnnName = "ywang27_annotations"
    AwsGlacierName = "ucmpcs"
    
    print(event)
    
    # Extract info from event
    message = json.loads(event['Records'][0]['Sns']['Message'])
    thaw_job_id = message['JobId']
    job_id = message['JobDescription']
    # thaw_job_id = event["thaw_job_id"]
    # job_id = event["job_id"]
    
    # Connect to DynaboDB
    dynamodb = boto3.resource('dynamodb', region_name=AwsRegionName)
    table = dynamodb.Table(AwsDynamoDBAnnName)
    
    try:
        query_results = table.query(
            KeyConditionExpression=Key('job_id').eq(job_id)
        )
    except ClientError as e:
        raise Exception(f"Cannot query dynamodb table: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Cannot query dynamodb table: {e}")
        }
    
    # Get job details
    items = query_results.get("Items", [])
    for item in items:
        # Filter out restored jobs
        if "result_archive_id" not in item.keys():
            print(f"Job {job_id} has been restored. ")
            return {
                'statusCode': 200
            }
            
        result_bucket = item["s3_results_bucket"]
        result_key = item["s3_key_result_file"]
        
        # Connect to Glacier
        glacier_client = boto3.client('glacier', region_name=AwsRegionName)
        
        # Get the thawed object
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.get_job_output
        result_file_obj = None
        try:
            result_file_obj = glacier_client.get_job_output(
                vaultName=AwsGlacierName,
                jobId=thaw_job_id
            )
        except ClientError as e:
            raise Exception(f"Cannot get thawed object from vault {AwsGlacierName}: {e}")
            return {
                'statusCode': 500,
                'body': json.dumps(f"Cannot get thawed object from vault {AwsGlacierName}: {e}")
            }
        print("Got thawed object.")
        
        # Connect to DynamoDB
        s3_recourse = boto3.resource('s3', region_name=AwsRegionName)
        
        # Copy the object to S3 specified location
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Object.put
        try:
            s3_recourse.Object(
                result_bucket, 
                result_key
            ).put(Body=result_file_obj['body'].read())
        except ClientError as e:
            raise Exception(f"Cannot copy object to S3 bucket: {e}")
            return {
                'statusCode': 500,
                'body': json.dumps(f"Cannot copy object to S3 bucket: {e}")
            }
        print(f"Copied object to {result_key}.")
        
        # Remove archive id for restored jobs
        try:
            table.update_item(
                Key={
                    "job_id": job_id
                },
                UpdateExpression="REMOVE result_archive_id",
            )
            table.update_item(
                Key={
                  "job_id": job_id
                },
                UpdateExpression="SET archived=:ar",
                ExpressionAttributeValues={
                  ":ar": False
                },
            )
        except ClientError as e:
            raise Exception(f"Cannot remove field 'result_archive_id' from table : {e}")
            return {
                'statusCode': 500,
                'body': json.dumps(f"Cannot remove field 'result_archive_id' from table : {e}")
            }
        print(f"Updated table with job id: {job_id}")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
