import sys
from datetime import timedelta, datetime
import boto3 
from botocore.exceptions import ClientError
import dateutil
import dateutil.parser
import json
import time
import copy

from utils import get_db_config,PME_Logging,get_s3_file
from sodapy import Socrata

def etl_to_soda_handler(event, context):
    def _isodate(datestr):
        return dateutil.parser.parse(datestr)
        
    t = time.localtime()
    env = get_db_config('PME_Config')
    cwLog = PME_Logging() 
    cwLog.awsGroup = env['Log_Location']['S']
    cwLog.awsStream = str(t.tm_yday) + "_" + str(t.tm_year) + "_Socrata"
    cwLog.debugLevel = env['Log_Level']['S']
  
    watched_bucket = env['File_Drop_Bucket']['S']
    awsbucket = env['AWS_Bucket']['S'] 
	
    cwLog.domain = event['object'][0]
    cwLog.rn = event['input']['pme_id'][0]
    cwLog.messageLevel = 'INFO'
    cwLog.message = f'*************STARTING REQUEST*************'
    cwLog.log_to_CloudWatch()

    log_info = copy.copy(event)
    del log_info['SecretString'] 
    cwLog.message = f"Event ->  {log_info}"
    cwLog.log_to_CloudWatch()
 
    today = datetime.today()
    jobstart = today

    try:
        apptoken = event['SecretString']['SecretString']   
        socrata_user = apptoken['user'] 
        socrata_pwd = apptoken['Password'] 
        socrata_token = apptoken['SecurityToken']  
    
        awsbucket = event['body']["Bucket"]
     
        socrata_domain = event['body']['body']['socratadomain']
        updatetype = event['body']['body']['updateType'].lower() 
        fxf = event['body']['body']['fxf']
    
        s3 = boto3.resource('s3',
        region_name='us-west-2' 
         )  
        payload = event['Payload']['Payload']  
        cwLog.message = f'Payload -> {payload}'
        cwLog.log_to_CloudWatch()
    
        session = boto3.session.Session()
        s3_client = session.client('s3')
 
        waiter = s3_client.get_waiter('object_exists')
        waiter.wait(Bucket=awsbucket, Key = payload['S3_File'],
                  WaiterConfig={
                     'Delay': 10, 'MaxAttempts': 20})
                     
        cwLog.message = 'Object exists: ' + awsbucket + '/'+ payload['S3_File']
        cwLog.log_to_CloudWatch()

    except ClientError as e:
        cwLog.messageLevel = 'ERROR'
        cwLog.message = "boto3 client error in use_waiters_check_object_exists: " + e.__str__()
        cwLog.log_to_CloudWatch()
        return {"ErrorState":"Fail", "message": cwLog.message}
    except Exception as e:
        cwLog.messageLevel = 'ERROR'
        cwLog.message = "Unexpected error in use_waiters_check_object_exists: " + e.__str__()
        cwLog.log_to_CloudWatch()
        return {"ErrorState":"Fail", "message": cwLog.message}

    r = get_s3_file(s3,awsbucket, payload['S3_File']) 

    #Upsert Into Socrata
    cwLog.message = 'Update Type ' + updatetype
    cwLog.log_to_CloudWatch()

    cutoff = int(env['Upsert_Row_Batch']['N'])
    totalrows = 0
    insert_start = datetime.today() 
    try: 
        socrata_client = Socrata(socrata_domain, socrata_token, username=socrata_user, password=socrata_pwd,timeout=10000)
        if len(r) < cutoff:
            if updatetype == 'upsert':
                socrata_request = socrata_client.upsert(fxf,r)
            elif updatetype == 'replace':
                socrata_request = socrata_client.replace(fxf,r) 
            if (socrata_request['Errors'] == 0):
                insert_time = datetime.today() - insert_start
                cwLog.messageLevel = 'WARN'
                cwLog.message = context.aws_request_id + " SUCCESS!! Dataset " + fxf + " Created " + str(socrata_request['Rows Created']) + " Updated " + str(socrata_request['Rows Updated'])
                cwLog.log_to_CloudWatch()
            else:
                cwLog.messageLevel = 'ERROR'
                cwLog.message = f"Socrata Update {updatetype} failed"
                cwLog.log_to_CloudWatch()
            totalrows = socrata_request['Rows Created'] + socrata_request['Rows Updated']
        else:           
            partialdataset = []
            partialrow = {}
            rowcount = len(r)
            interval = int(rowcount/cutoff) + 1
            if updatetype == 'upsert':
                init = 0
                updatelength = cutoff
                for soccount in range(0,interval):
                    updatelength = cutoff * (soccount + 1)
                    if updatelength >= rowcount:
                        updatelength = rowcount
                    for inc in range(init,updatelength):
                        partialrow=r[inc]
                        partialdataset.append(partialrow.copy())
                        partialrow.clear()
                      
                    socrata_request = socrata_client.upsert(fxf,partialdataset) 
                    if (socrata_request['Errors'] == 0):
                        cwLog.messageLevel = 'WARN'
                        cwLog.message = context.aws_request_id + " SUCCESS!! Dataset " + fxf + " Created " + str(socrata_request['Rows Created']) + " Rows Deleted " + str(socrata_request['Rows Deleted']) + " Rows Updated " + str(socrata_request['Rows Updated'])
                        cwLog.log_to_CloudWatch()
                    else:
                        cwLog.messageLevel = 'ERROR'
                        cwLog.message = f"Socrata Update {updatetype} failed"
                        cwLog.log_to_CloudWatch()
                    totalrows = totalrows + socrata_request['Rows Created'] + socrata_request['Rows Updated']
                    init = updatelength
                insert_time = datetime.today() - insert_start
            elif updatetype == 'replace':
                init = 0
                updatelength = cutoff
                trunc_response = requests.put(f"https://{socrata_domain}/resource/{fxf}.json", json=[], auth=(socrata_username, socrata_password))
                for soccount in range(0,interval):
                    updatelength = cutoff * (soccount + 1)
                    if updatelength >= rowcount:
                        updatelength = rowcount
                    for inc in range(init,updatelength):
                        partialrow=r[inc]
                        partialdataset.append(partialrow.copy())
                        partialrow.clear() 
                    socrata_request = socrata_client.upsert(fxf,partialdataset) 
                    if (socrata_request['Errors'] == 0):
                        cwLog.messageLevel = 'WARN'
                        cwLog.message = context.aws_request_id + " SUCCESS!! Dataset " + fxf + " Created " + str(socrata_request['Rows Created']) + " Rows Deleted " + str(socrata_request['Rows Deleted']) + " Rows Updated " + str(socrata_request['Rows Updated'])
                        cwLog.log_to_CloudWatch()
                    else: 
                        cwLog.messageLevel = 'ERROR'
                        cwLog.message = f"Socrata Update {updatetype} failed"
                        cwLog.log_to_CloudWatch()
                    totalrows = totalrows + socrata_request['Rows Created'] + socrata_request['Rows Updated']
                    init = updatelength
                    partialdataset.clear() 
                insert_time = datetime.today() - insert_start
    except Exception as err:
        cwLog.messageLevel = 'ERROR'
        cwLog.message = f"Unexpected {err=}, {type(err)=} Rolling Back"
        cwLog.log_to_CloudWatch()
        return {"ErrorState":"Fail", "message": cwLog.message} 

    jobend = datetime.today()
    delta = jobend - jobstart
    data = [{'num_rows':totalrows,'insertion_time_secs':float(insert_time.total_seconds()),'payload_size_bytes':int(sys.getsizeof(r))}]
    cwLog.messageLevel = 'INFO'
    cwLog.message = f'** Complete!!  Job ran for {delta.total_seconds()} seconds **'
    cwLog.log_to_CloudWatch()

    return {"ErrorState":"Success","Data":data}
