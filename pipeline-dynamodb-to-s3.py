#Author: Ahmad.Iftekhar

from __future__ import print_function
import boto3
import json
from time import gmtime, strftime

vPipelineObjects=''
vParameterObjects=''
vParameterValues=''



def lambda_handler(event, context):
  dt = strftime("%Y%m%d_%H%M%S", gmtime())
  #vTableName = 'testtab'
  #vS3OutLocation='rumman-dynamodb-backup'
  #vLogLocation = 'rumman-log'
  #vTerminateAfterMin = 60
  vRequiredParameters=['BackupBucket', 'DDBReadThroughputRatio', 'LogBucket', 'TableName', 'TerminateAfterMin']
  #vRequiredParameters.sort() #already sorted
  eventKeys = event.keys()
  eventKeys.sort()
  if eventKeys != vRequiredParameters:
    print ('This lambda function takes followings parameters: i) TableName, ii) BackupBucket, iii) LogBucket, iv) DDBReadThroughputRatio, v) TerminateAfterMin')
  
  else:   

      vTableName = event['TableName']
      vS3OutLocation = event['BackupBucket']
      vLogLocation = event['LogBucket']
      vTerminateAfterMin = event['TerminateAfterMin']
      vDDBReadThroughputRatio = event['DDBReadThroughputRatio']
      
      client = boto3.client('datapipeline')
      vPipelineName = 'lambda_dynamodb_to_s3_' + vTableName
      vDataPipeline = client.create_pipeline(
        name=vPipelineName,
        uniqueId=vPipelineName + '_' + dt,
        description='Pipeline created by lambda function to copy dynamodb table to s3',
        tags=[
          {
            'key': 'CreatedBy',
            'value': 'lambda'
          },
        ]
      )
      vPipelineId = vDataPipeline['pipelineId']
      print ('Pipeline create with PipelineId = ' + vPipelineId )

      f_pipeline_definition(vTableName,vS3OutLocation,vLogLocation,vTerminateAfterMin,vDDBReadThroughputRatio)
      global vPipelineObjects
      global vParameterObjects
      global vParameterValues
      #print ('vPipelineObjects  = ' + json.dumps(vPipelineObjects) )
      #print ('vParameterObjects = ' + json.dumps(vParameterObjects) )
      #print ('vParameterValues  = ' + json.dumps(vParameterValues) )

      
      client.put_pipeline_definition(pipelineId=vPipelineId,
        pipelineObjects=vPipelineObjects,
        parameterObjects = vParameterObjects,
        parameterValues=vParameterValues)

      client.activate_pipeline(pipelineId=vPipelineId)



def f_pipeline_definition(vTableName,vS3OutLocation,vLogLocation,vTerminateAfterMin,vDDBReadThroughputRatio):
  global vPipelineObjects
  vPipelineObjects = [
  {
    "fields": [
    {
    "stringValue": "CASCADE",
    "key": "failureAndRerunMode"
    },
    {
    "refValue": "DefaultSchedule",
    "key": "schedule"
    },
    {
    "stringValue": "DataPipelineDefaultResourceRole",
    "key": "resourceRole"
    },
    {
    "stringValue": "DataPipelineDefaultRole",
    "key": "role"
    },
    {
    "stringValue": "s3://"+ vLogLocation +"/",
    "key": "pipelineLogUri"
    },
    {
    "stringValue": "cron",
    "key": "scheduleType"
    }
    ],
  "id": "Default",
  "name": "Default"
  },
  {
  "fields": [
    {
    "stringValue": "1",
    "key": "occurrences"
    },
    {
    "stringValue": "1 Day",
    "key": "period"
    },
    {
    "stringValue": "Schedule",
    "key": "type"
    },
    {
    "stringValue": "FIRST_ACTIVATION_DATE_TIME",
    "key": "startAt"
    }
  ],
  "id": "DefaultSchedule",
  "name": "RunOnce"
  },
  {
  "fields": [
    {
    "refValue": "S3BackupLocation",
    "key": "output"
    },
    {
    "refValue": "DDBSourceTable",
    "key": "input"
    },
    {
    "stringValue": "2",
    "key": "maximumRetries"
    },
    {
    "stringValue": "s3://dynamodb-emr-#{myDDBRegion}/emr-ddb-storage-handler/2.1.0/emr-ddb-2.1.0.jar,org.apache.hadoop.dynamodb.tools.DynamoDbExport,#{output.directoryPath},#{input.tableName},#{input.readThroughputPercent}",
    "key": "step"
    },
    {
    "refValue": "EmrClusterForBackup",
    "key": "runsOn"
    },
    {
    "stringValue": "EmrActivity",
    "key": "type"
    },
    {
    "stringValue": "true",
    "key": "resizeClusterBeforeRunning"
    }
    ],
  "id": "TableBackupActivity",
  "name": "TableBackupActivity"
  },
  {
  "fields": [
    {
    "stringValue": "#{myOutputS3Loc}/#{myDDBTableName}/#{format(@actualStartTime, 'YYYY-MM-dd-HH-mm-ss')}",
    "key": "directoryPath"
    },
    {
    "stringValue": "S3DataNode",
    "key": "type"
    }
    ],
  "id": "S3BackupLocation",
  "name": "S3BackupLocation"
  },
  {
    "fields": [
      {
      "stringValue": "#{myDDBReadThroughputRatio}",
      "key": "readThroughputPercent"
      },
      {
      "stringValue": "DynamoDBDataNode",
      "key": "type"
      },
      {  
      "stringValue": "#{myDDBTableName}",
      "key": "tableName"
      }
    ],
    "id": "DDBSourceTable",
    "name": "DDBSourceTable"
  },
  {
  "fields": [
      {
      "stringValue": "s3://#{myDDBRegion}.elasticmapreduce/bootstrap-actions/configure-hadoop, --yarn-key-value,yarn.nodemanager.resource.memory-mb=11520,--yarn-key-value,yarn.scheduler.maximum-allocation-mb=11520,--yarn-key-value,yarn.scheduler.minimum-allocation-mb=1440,--yarn-key-value,yarn.app.mapreduce.am.resource.mb=2880,--mapred-key-value,mapreduce.map.memory.mb=5760,--mapred-key-value,mapreduce.map.java.opts=-Xmx4608M,--mapred-key-value,mapreduce.reduce.memory.mb=2880,--mapred-key-value,mapreduce.reduce.java.opts=-Xmx2304m,--mapred-key-value,mapreduce.map.speculative=false",
      "key": "bootstrapAction"
      },
      {
      "stringValue": "1",
      "key": "coreInstanceCount"
      },
      {
      "stringValue": "m3.xlarge",
      "key": "coreInstanceType"
      },
      {
      "stringValue": "3.8.0",
      "key": "amiVersion"
      },
      {
      "stringValue": "m3.xlarge",
      "key": "masterInstanceType"
      },
      {
      "stringValue": "#{myDDBRegion}",
      "key": "region"
      },
      {
      "stringValue": "EmrCluster",
      "key": "type"
      },
      {
      "stringValue":  "" +   str(vTerminateAfterMin) +  " Minute",
      "key": "terminateAfter"
      }
    ],
    "id": "EmrClusterForBackup",
    "name": "EmrClusterForBackup"
  }
  ]

  global vParameterValues
  vParameterValues = [
      {
      "stringValue": "us-east-1",
      "id": "myDDBRegion"
      },
      {
      "stringValue":  vTableName,
      "id": "myDDBTableName"
      },
      {
      "stringValue": vDDBReadThroughputRatio,
      "id": "myDDBReadThroughputRatio"
      }, 
      {
      "stringValue": "s3://" + vS3OutLocation + "/",
      "id": "myOutputS3Loc"
      }
  ]	
  global vParameterObjects
  vParameterObjects =  [
    {
      "attributes": [
      {
      "stringValue": "Source DynamoDB table name",
      "key": "description"
      },
      {
      "stringValue": "String",
      "key": "type"
      }
      ],
    "id": "myDDBTableName"
    },
    {
      "attributes": [
        {
        "stringValue": "Output S3 folder",
        "key": "description"
        },
        {
        "stringValue": "AWS::S3::ObjectKey",
        "key": "type"
        }
        ],
    "id": "myOutputS3Loc"
    },
    {
      "attributes": [
        {
        "stringValue": "0.25",
        "key": "default"
        },
        {
        "stringValue": "Enter value between 0.1-1.0",
        "key": "watermark"
        },
        {
        "stringValue": "DynamoDB read throughput ratio",
        "key": "description"
        },
        {
        "stringValue": "Double",
        "key": "type"
        }
      ],
    "id": "myDDBReadThroughputRatio"
  },
  {
     "attributes": [
       {
       "stringValue": "us-east-1",
       "key": "default"
       },
       {
       "stringValue": "us-east-1",
       "key": "watermark"
       },
       {
       "stringValue": "Region of the DynamoDB table",
       "key": "description"
       },
       {
       "stringValue": "String",
       "key": "type"
       }
     ],
     "id": "myDDBRegion"
  }
  ]  



