# aws-lambda-datapipeline-for-dynamodb-to-s3
aws-lambda-datapipeline-for-dynamodb-to-s3

This Lambda function creates data pipeline dynamically to backup dynamodb table to s3 location.

The function take the following parameters:
'BackupBucket', 'DDBReadThroughputRatio', 'LogBucket', 'TableName', 'TerminateAfterMin'

and then creates the pipeline, activate the job in async.

The job then starts its task same as the "Export dynamodb table to S3" that is creating EMR cluster, installing hadoop, pig and backup data into S3 given location.
It terminates the created EMR as soon as job finishes, else it will terminate it if the EMR cluster creation time passes  TerminateAfterMin.
It puts the log into LogBucket.
And also I added DDBReadThroughputRatio, so that people can modify the throughput from their end according to their requirement.

The function requires a role for lambda where lambda should have full permission on datapipeline.
