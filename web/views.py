# views.py
#
# Copyright (C) 2011-2022 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Yu Wang <ywang27@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime, timedelta

import boto3
from botocore.client import Config
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template, 
  request, session, url_for, send_file)
from app import app, db
from decorators import authenticated, is_premium

from auth import get_profile

"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Open a connection to the S3 service
  s3 = boto3.client('s3', 
    region_name=app.config['AWS_REGION_NAME'], 
    config=Config(signature_version='s3v4'))

  # Get bucket name and user id
  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + "/job"

  # Define policy conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name,
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f'Unable to generate presigned URL for upload: {e}')
    return abort(500)

  # Render the upload form which will parse/submit the presigned POST
  print(f"========{session['role']}=========")
  return render_template('annotate.html',
    s3_post=presigned_post,
    role=session['role'])


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

  region = app.config['AWS_REGION_NAME']

  # Parse redirect URL query parameters for S3 object info
  bucket_name = request.args.get('bucket')
  s3_key = request.args.get('key')

  # Get job_id and filename
  segments = s3_key.split('/')
  path = '/'.join(segments[0:2])  # ywang27/userX
  [job_id, file_name] = segments[2].split('~')

  # Get submit time
  submit_time = int(time.time())

  # ============ Working with annotation DynamoDB ==============
  # Get connection to Dynamodb
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table    
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  try:
    table = dynamodb.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])
  except ClientError as e:
    app.logger.error(f'Cannot get table from Dynamodb: {e}')
    return render_template('error.html',
      title="Internal Error",
      alert_level='warning',
      message=f'Cannot get table from Dynamodb: {e}'
    )

  # Create a job item and persist it to the annotations database
  data = { "job_id": job_id,
           "user_id": session['primary_identity'],
           "input_file_name": file_name,
           "s3_inputs_bucket": app.config['AWS_S3_INPUTS_BUCKET'],
           "s3_key_input_file": s3_key,
           "submit_time": submit_time,
           "job_status": "PENDING",
           "archived": False
         }
  # Put the data into dynamodb
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.put_item
  try:
      table.put_item(Item=data)
  except ClientError as e:
    app.logger.error(f'Cannot put an item into Dynamodb: {e}')
    return render_template('error.html',
      title="Internal Error",
      alert_level='warning',
      message=f'Cannot put an item into Dynamodb: {e}'
    )

  # ============ Working with SNS and SQS =============
  # Publishes a notification message to the SNS topic
  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Client.publish
  # Create SNS client
  sns_client = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])

  # Get SNS topic arn
  sns_topic_arn = app.config["AWS_SNS_JOB_REQUEST_TOPIC"]

  # Add user's email to data
  user_id = session.get('primary_identity')
  profile = get_profile(identity_id=user_id)
  email = profile.email
  # print(email)

  data['email'] = email

  # Publish a message to the topic
  # https://docs.aws.amazon.com/sns/latest/dg/sns-publishing.html
  try:
      sns_response = sns_client.publish(
          TopicArn=sns_topic_arn,
          Message=json.dumps({'default': json.dumps(data)}),
          MessageStructure='json'
      )
  except ClientError as e:
    app.logger.error(f'Unable to publish the SNS message: {e}')
    return render_template('error.html',
      title="Internal Error",
      alert_level='warning',
      message=f'Unable to publish the SNS message: {e}'
    )

  return render_template('annotate_confirm.html', job_id=job_id)


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
  # Define a timedelta
  epoch = datetime(1900, 1, 1)

  # Get authentic user id
  user_id = session['primary_identity']

  # Get connection to Dynamodb
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  try:
    table = dynamodb.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])
  except ClientError as e:
    app.logger.error(f'Cannot get table from Dynamodb: {e}')
    return render_template('error.html',
      title="Internal Error",
      alert_level='warning',
      message=f'Cannot get table from Dynamodb: {e}'
    )
  
  # Reading items from gas-inputs bucket
  # Get connection to S3
  s3 = boto3.client('s3', 
    region_name=app.config['AWS_REGION_NAME'], 
    config=Config(signature_version='s3v4')
  )

  # Prefix with authentic user
  prefix_authentic = app.config['AWS_S3_KEY_PREFIX'] + user_id

  # Get items
  try:
    objects = s3.list_objects(
      Bucket=app.config['AWS_S3_INPUTS_BUCKET'], 
      Prefix=prefix_authentic
    )
  except ClientError as e:
    app.logger.error(f'Cannot list objects from S3: {e}')
    return render_template('error.html',
      title="Internal Error",
      alert_level='warning',
      message=f'Cannot list objects from S3: {e}'
    )

  annotation_info = []
  if "Contents" in objects:
    for obj in objects["Contents"]:
      # Get job_id
      file_key = obj["Key"]
      job_id = file_key.split('/')[2].split('~')[0]
      
      # Get required info by querying DynamoDB
      try:
        query_results = table.query(
          KeyConditionExpression=Key("job_id").eq(job_id)
        )
      except ClientError as e:
        app.logger.error(f'Cannot query Dynamodb: {e}')
        return render_template('error.html',
          title="Internal Error",
          alert_level='warning',
          message=f'Cannot query Dynamodb: {e}'
        )

      # Not matching item
      if len(query_results['Items']) == 0:
        continue 

      # Found matching item
      item = query_results['Items'][0]
      # print(item)

      # Process time format
      request_time = time.localtime(float(item['submit_time']))
      month = str(request_time.tm_mon)
      if len(month) < 2:
        month = '0' + month

      day = str(request_time.tm_mday)
      if len(day) < 2:
        day = '0' + day

      hour = str(request_time.tm_hour)
      if len(hour) < 2:
        hour = '0' + hour

      minute = str(request_time.tm_min)
      if len(minute) < 2:
        minute = '0' + minute

      request_time = f"{request_time.tm_year}-{month}-{day} {hour}:{minute}"

      vcf_file_name = item['input_file_name']
      status = item['job_status']

      annotation_info.append({
        "job_id": job_id,
        "request_time": request_time,
        "vcf_file_name": vcf_file_name,
        "status": status
      })
    
  return render_template('annotations.html', annotations=annotation_info)


"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
  # Get job info from DynamoDB using job id
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  try:
    table = dynamodb.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])
  except ClientError as e:
    app.logger.error(f'Cannot get table from Dynamodb: {e}')
    return render_template('error.html',
      title="Internal Error",
      alert_level='warning',
      message=f'Cannot get table from Dynamodb: {e}'
    )

  # Get required info by querying DynamoDB
  try:
    query_results = table.query(
      KeyConditionExpression=Key("job_id").eq(id)
    )
  except ClientError as e:
    app.logger.error(f'Cannot query Dynamodb: {e}')
    return render_template('error.html',
      title="Internal Error",
      alert_level='warning',
      message=f'Cannot query Dynamodb: {e}'
    )

  # Found matching item
  item = query_results['Items'][0]

  # Check authentication
  user_id = session['primary_identity']

  # If not authenticated
  if user_id != item['user_id']:
    app.logger.error(f'Not authenticated to access the annotation job.')
    return abort(403)
  
  else:
    # Process time format - request
    request_time = time.localtime(float(item['submit_time']))
    month = str(request_time.tm_mon)
    if len(month) < 2:
      month = '0' + month

    day = str(request_time.tm_mday)
    if len(day) < 2:
      day = '0' + day

    hour = str(request_time.tm_hour)
    if len(hour) < 2:
      hour = '0' + hour

    minute = str(request_time.tm_min)
    if len(minute) < 2:
      minute = '0' + minute

    request_time = f"{request_time.tm_year}-{month}-{day} {hour}:{minute}"

    # Process time format - complete
    if "complete_time" in item:
      complete_time = time.localtime(float(item['complete_time']))
      month = str(complete_time.tm_mon)
      if len(month) < 2:
        month = '0' + month

      day = str(complete_time.tm_mday)
      if len(day) < 2:
        day = '0' + day

      hour = str(complete_time.tm_hour)
      if len(hour) < 2:
        hour = '0' + hour

      minute = str(complete_time.tm_min)
      if len(minute) < 2:
        minute = '0' + minute

      complete_time = f"{complete_time.tm_year}-{month}-{day} {hour}:{minute}"
    # Unfinished job: no complete_time
    else:
      complete_time = None

    vcf_file_name = item['input_file_name']
    status = item['job_status']

    # Generate presigned_url for downloading
    s3 = boto3.client('s3', 
    region_name=app.config['AWS_REGION_NAME'], 
    config=Config(signature_version='s3v4'))

    # Annotation File key
    file_name_prefix = vcf_file_name.split('.')[0]
    key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + id + f'~{file_name_prefix}.annot.vcf'

    # Get presigned url to download file
    # https://allwin-raju-12.medium.com/boto3-and-python-upload-download-generate-pre-signed-urls-and-delete-files-from-the-bucket-87b959f7bbaf
    bucket_name = app.config['AWS_S3_RESULTS_BUCKET']

    # Whether the file has been archived
    archived = item["archived"]

    # Whether the file is downloadable
    downloadable = False
    presigned_url = ''
    # not archived or restored after archive
    if "result_archive_id" not in item:
      downloadable = True
      try:
        presigned_url = s3.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                   'Bucket': bucket_name,
                   'Key': key_name,
                   'ResponseContentDisposition': 'attachment'
                   },
            ExpiresIn=180
        )
      except ClientError as e:
        app.logger.error(f'Cannot generate presigned url for downloading: {e}')
        return render_template('error.html',
          title="Internal Error",
          alert_level='warning',
          message=f'Cannot generate presigned url for downloading: {e}'
        )

    # Whther the user is premium
    is_premium = False
    if session['role'] == "premium_user":
      is_premium = True

    # Annotation info to be displayed
    annotation_info = {
      "job_id": id,
      "request_time": request_time,
      "complete_time": complete_time,
      "vcf_file_name": vcf_file_name,
      "status": status,
      "archived": archived,
      "downloadable": downloadable,
      "is_premium": is_premium
    }

    return render_template('annotation.html', annotations=annotation_info, download_url=presigned_url)


"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
  # Read log file from S3 bucket
  # Get user id
  user_id = session['primary_identity']

  # Get file key
  # Read from DynamoDB for file name
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  try:
    table = dynamodb.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])
  except ClientError as e:
    app.logger.error(f'Cannot get table from Dynamodb: {e}')
    return render_template('error.html',
      title="Internal Error",
      alert_level='warning',
      message=f'Cannot get table from Dynamodb: {e}'
    )

  # Get required info by querying DynamoDB
  query_results = table.query(
    KeyConditionExpression=Key("job_id").eq(id)
  )

  # Get required info by querying DynamoDB
  query_results = table.query(
    KeyConditionExpression=Key("job_id").eq(id)
  )

  item = query_results['Items'][0]
  vcf_file_name = item['input_file_name']

  # Get prefix
  file_name_prefix = vcf_file_name.split('.')[0]

  # Get file key
  log_file_key = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + id + f'~{file_name_prefix}.vcf.count.log'
  
  # Get connection to s3
  s3 = boto3.resource('s3')

  # Get object from S3
  bucket_name = app.config['AWS_S3_RESULTS_BUCKET']
  try:
    obj = s3.Object(bucket_name, log_file_key)
  except ClientError as e:
    app.logger.error(f'Cannot get the log file: {e}')
    return render_template('error.html',
      title="Internal Error",
      alert_level='warning',
      message=f'Cannot get the log file: {e}'
    )

  log_content = obj.get()['Body'].read().decode('utf-8')

  # Replace '\n' with <br> in HTML
  log_content = log_content.replace('\n', '<br>')

  return render_template('view_log.html', log=log_content, job_id=id)


"""Subscription management handler
"""
import stripe
from auth import update_profile

@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    return render_template('subscribe.html')
    
  elif (request.method == 'POST'):
    if 'stripe_token' not in request.form.keys():
      app.logger.error("Cannot get stripe token.")

      return render_template('error.html', 
        title='Redirect Error', 
        alert_level='warning',
        message="Cannot get stripe token. Please try again."
      ), 404

    # Get stripe token
    stripe_token = request.form['stripe_token']

    # Get user's info
    user_id = session.get('primary_identity')

    # Create a new customer
    # https://stripe.com/docs/api/customers/create?lang=python
    stripe.api_key = app.config['STRIPE_SECRET_KEY']
    try:
      customer = stripe.Customer.create(
        card=stripe_token,
        name=session.get('name'),
        email=session.get('email'),
        description=f"New customer created. User id: {user_id}"
      )
    except Exception as e:
      app.logger.error(f"Failed to create customer: {e}")
      return render_template('error.html', 
        title='Server Error', 
        alert_level='warning',
        message=f"Cannot create a customer: {e}" 
      ), 500

    # Create a subscription for the customer
    # https://stripe.com/docs/api/subscriptions/create?lang=python
    customer_id = customer["id"]
    try:
      stripe.Subscription.create(
        customer = customer_id,
        items = [
          {"price": app.config['STRIPE_PRICE_ID']},
        ]
      )
    except Exception as e:
      app.logger.error(f"Failed to create subscription, got: {e}")
      return render_template('error.html', 
        title='Server Error', 
        alert_level='warning',
        message=f"Cannot create a subscrption: {e}" 
      ), 500

    # update user role in accounts database
    try:
      update_profile(
        identity_id=session['primary_identity'], 
        role="premium_user"
      )
    except Exception as e:
      app.logger.error(f"Failed to update profile for user {session['primary_identity']}: {e}")
      return render_template('error.html', 
        title='Internal Error', 
        alert_level='warning',
        message=f"Failed to update profile for user {session['primary_identity']}: {e}"
      ), 500
    
    # update role in the session
    session['role'] = "premium_user"

    # Request restoration of the user's data from Glacier
    # ...add code here to initiate restoration of archived user data
    # ...and make sure you handle files not yet archived!

    # Publish message for a thaw job with user id
    sns = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])
    data = {"user_id": user_id}

    try:
      sns.publish(
        TopicArn=app.config['AWS_SNS_THAW_TOPIC'], 
        Message=json.dumps({'default': json.dumps(data)}),
        MessageStructure='json'
      )
    except ClientError as e:
      app.logger.error(f"Cannot publish message to SNS: {e}")
      return render_template('error.html', 
        title='Internal Error', 
        alert_level='warning',
        message=f"Cannot publish message to SNS: {e}"
      ), 500

    # Display confirmation page
    return render_template('subscribe_confirm.html', stripe_id=customer_id) 


"""Set premium_user role
"""
@app.route('/make-me-premium', methods=['GET'])
@authenticated
def make_me_premium():
  # Hacky way to set the user's role to a premium user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="premium_user"
  )

  return redirect(url_for('profile'))


"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )

  return redirect(url_for('profile'))



"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500

### EOF
