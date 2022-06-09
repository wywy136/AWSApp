# GAS Framework
An enhanced web framework (based on [Flask](https://flask.palletsprojects.com/)) for use in the capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](https://getbootstrap.com/docs/3.3/).

Directory contents are as follows:
* `/web` - The GAS web app files
* `/ann` - Annotator files
* `/util` - Utility scripts/apps for notifications, archival, and restoration
* `/aws` - AWS user data files

## A16

For this assignment, I have carefully checked my code and application according to the instructions. I would be very appreciate if you could please contact me at ywang27@uchicago.edu for any problem accessing the application.

In order to complete this assignment, the newly added AWS services and their functionalities are
- An SNS topic with name `ywang27_a16_thaw_jobs`, to record any upgrading behaviour from user and initiate potential thaw jobs.
- An SQS queue with name `ywang27_a16_thaw_jobs`, subscribed to the SNS topic `ywang27_a16_thaw_jobs`, to process potential thaw jobs.
- An SNS topic with name `ywang27_a16_restore_jobs`, to record a completion of thawing jobs and start a potential restore job.
- A Lambda Function with name `ywang27_restore`, subscribed to the SNS topic `ywang27_a16_restore_jobs`, to process a potential restore job.

### Thawing an Object

The only point to start a thawing job is when a free user upgrade to a permium user. After updating the user database, the web will send a message in topic `ywang27_a16_thaw_jobs` with the user id. 

In the backend, the thaw script with continuous polling the queue `ywang27_a16_thaw_jobs` processes any thaw job. The script will query the annotation table using global secondary index to return all the jobs related to the user. For every job, if the job is now archived, the script will initiate a thawing job (first try expedited, then standard). Otherwise the job is either submitted when the user is premium or has been restored previouly, and will simply bypass this job.

Another topic `ywang27_a16_restore_jobs` is passed to a thawing job so that when the thawing job is finished, a message will be published to notify the following restoring process. This message contains the GAS job id and the thawing job id.

### Restoring an Object

The restoration is processed by lambda function `ywang27_restore`, subscribed to the SNS topic `ywang27_a16_restore_jobs`. When a restoring job comes, the function first query the annotation table to get the up-to-date archive status of the job. It is possible that a job has already been restored in the 4-hour window in a previous user upgrading behaviour. 

If the job is not restored yet (by checking the field "result_archive_id" does not exist in the query result), the function will first get the file object from glacier and then copy the object to the S3 result bucket using the result file key contained in the query result. After the result file is restored, the table will delete the field "result_archive_id" and change the field "archived" to `False` to update the status.

### Displaying on Job Details Page

There is a couple of status for a result file to be displayed on the job details page. Here is how I handled them
- Firstly check the complete time of the job. If the job is not completed yet, show "not completed". Otherwise:
- Check whether the file is archived. If the file is not archived (either submitted by a permium user or restored by a free user upgrading to a premium user), show "download" with the presigned link. Otherwise:
- Check whether the user is currently a premium user or not. This could be accessed via `session['role']` . If the user is not a premium user, show "file archived; upgrade to premium user" with the link to subscription. Otherwise:
- Check whether the result file is downloadable. A file is downloadable when the queried result for a job does not contain "result_archive_id" field which means the file has already been restored. This is equivalent to check the "archived" field of a job. If the file is downloadable, show "download" with the presigned link. Otherwise, the job is still not restored and show "file is being restored; please check back later".
