import csv
import sys
import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import time
from utils import get_db_config,PME_Logging 

def csv_to_json(event, context):
    t = time.localtime()
    env = get_db_config('PME_Config')
    cwLog = PME_Logging() 
    cwLog.awsGroup = env['Log_Location']['S']
    cwLog.awsStream = str(t.tm_yday) + "_" + str(t.tm_year) + "_MakeJson"
    cwLog.debugLevel = env['Log_Level']['S']
  
    watched_bucket = env['File_Drop_Bucket']['S']
    awsbucket = env['AWS_Bucket']['S'] 
    
    fileInfo = event['Records'][0]['s3'] 
    bucket = fileInfo['bucket']['name']
    file = fileInfo['object']['key']

    s3 = boto3.client('s3',region_name='us-west-2')

    identifier = event['Records'][0]['s3']['object']['key'].split('*')
    cwLog.domain = identifier[0]
    cwLog.rn = identifier[1]
    
    cwLog.messageLevel = 'INFO'
    cwLog.message = f'*************STARTING REQUEST*************'
    cwLog.log_to_CloudWatch()

    cwLog.message = f"Event ->  {event}" 
    cwLog.log_to_CloudWatch()
    
    # create a dictionary
    json_template = []
 
    session = boto3.session.Session()
    s3_client = session.client('s3')
    try:  
        waiter = s3_client.get_waiter('object_exists')
        waiter.wait(Bucket=bucket, Key = file,
                  WaiterConfig={
                     'Delay': 20, 'MaxAttempts': 10})
                
        cwLog.message = 'Object exists: ' + bucket + '/'+ file
        cwLog.log_to_CloudWatch()

        s3FileName = file[:file.rfind(".")] + ".json"
        cwLog.message = f'Writing to file -> {bucket} / {s3FileName}'
        cwLog.log_to_CloudWatch() 

        csv.field_size_limit(sys.maxsize)
        obj = s3.get_object(Bucket=bucket, Key=file) 
        data = obj['Body'].read().decode('utf-8').splitlines()
        records = csv.DictReader(data)
        
        # Convert each row into a dictionary
        for rows in records:
    	    json_template.append(rows)

        s3response = s3.put_object(
             Body=bytes(json.dumps(json_template, default=str).encode()),
             Bucket=bucket,
             Key=s3FileName)
             
        if s3response['ResponseMetadata']['HTTPStatusCode'] != 200:
            cwLog.messageLevel = 'ERROR'
            cwLog.message = "JSON write failed!"
            cwLog.log_to_CloudWatch()
            data = {'status': 'Fail','message':f'Unable to write to -> {s3FileName}'}
            return data
            
        s3response = s3.copy_object(Bucket=awsbucket,CopySource= bucket + '/'+ file ,Key= "Processed/" + file)
    
        s3response = s3.delete_object(
            Bucket=bucket,
            Key=file)  

        cwLog.messageLevel = 'INFO'
        cwLog.message = context.aws_request_id + " Success!!.  Converted " + str(len(json_template)) + " to json" 
        cwLog.log_to_CloudWatch()
 
        data = {'status': 'Success','message':f'All Good'}
        return data 
        
    except ClientError as e:
    	cwLog.messageLevel = 'ERROR'
    	cwLog.message = "boto3 client error in use_waiters_check_object_exists: " + e.__str__()
    	cwLog.log_to_CloudWatch()
    	data = {'status': 'Fail','message':f'Unable to find file -> {e.__str__()}'}
    	return data
    except Exception as e:
        cwLog.messageLevel = 'ERROR'
        cwLog.message = "Unexpected error in use_waiters_check_object_exists: " + e.__str__()
        cwLog.log_to_CloudWatch()
        cwLog.message = "Couldn't get object '%s' from bucket '%s'.",file,watched_bucket
        cwLog.log_to_CloudWatch()
        data = {'status': 'Fail','message':f'Could not get object {file} from bucket {watched_bucket}.'}
        return data
    
    except:
        cwLog.messageLevel = 'ERROR'
        cwLog.message = "Couldn't get object '%s' from bucket '%s'.",file,bucket
        cwLog.log_to_CloudWatch()
        data = {'status': 'Fail','message':f'Could not get object {file} from bucket {watched_bucket}.'}
        return data