import json
import pyodbc
import boto3
import decimal
from datetime import datetime
import datetime as dt
import copy
 
from sql_util import get_sql, get_db_config, PME_Logging
import time

def get_sql_data(event, context):
 
    t = time.localtime()
    env = get_db_config('PME_Config') 
    cwLog = PME_Logging()
    cwLog.awsGroup = env['Log_Location']['S']
    cwLog.awsStream = str(t.tm_yday) + "_" + str(t.tm_year) + "_GetSQLData "
    cwLog.debugLevel = env['Log_Level']['S']
 
    watched_bucket = env['File_Drop_Bucket']['S']
    awsbucket = env['AWS_Bucket']['S'] 

    cwLog.domain = event['targetDomain']
    cwLog.rn = event['Dataset'] 
    cwLog.messageLevel = 'WARN'
    cwLog.message = '--> REQUEST <--'
    cwLog.log_to_CloudWatch() 

    log_info = copy.copy(event)
    del log_info['SecretString'] 
    cwLog.message = f"Event ->  {log_info}" 
    cwLog.log_to_CloudWatch()

    db_host = None
    database = None
    
    for c_domain in event['customerDomains']['Items'][0]['customerDomains']['L']:
        if c_domain['M']['name']['S'] == event['targetDomain']:
            database = c_domain['M']['database']['S']
            db_host = c_domain['M']['host']['S']
            
    if db_host == None:
        cwLog.messageLevel = 'ERROR'
        cwLog.message = 'Database information not in Config file'
        cwLog.log_to_CloudWatch()
        data = {'status': 'Fail','message':'Database information not in Config file'}
        return data
 
    apptoken = json.loads(event['SecretString']['SecretString'] )

    db_user = apptoken['mssql_user']
    db_pwd = apptoken['mssql_password']

    s3 = boto3.client('s3',
    region_name='us-west-2' 
     )

    #establishing the connection
    conn_str = f'Driver={{ODBC Driver 17 for SQL Server}};Server={db_host};Database={database};UID={db_user};PWD={db_pwd}'
    cwLog.message = f"Connection String -> {conn_str}"
    cwLog.log_to_CloudWatch()
 
    conn = pyodbc.connect(conn_str)
    info = event['config']['Item']

    sql = get_sql(s3,info,awsbucket, conn, cwLog)
    q = sql[0] + " FROM [" + info['database']['M']['schema']['S'] + "].[" + info['database']['M']['table']['S']+ "]"# WHERE [LastModified] > DateADD(mi, -" + str(info['Cadence']['N'] + ", Current_TimeStamp)"

    cursor = conn.cursor()
    cwLog.message = f'Connected to DB -> {db_host} Running Query -> {q}'
    cwLog.log_to_CloudWatch()

    try:
        cursor.execute(q)
 
        r = [dict((cursor.description[i][0], value) \
                   for i, value in enumerate(row)) for row in cursor.fetchall()]
             
        for i, data in enumerate(r):
            for n in data:
     
                if isinstance(data[n], decimal.Decimal):
                    data[n] = float(data[n])
                    r[i] = data
                elif isinstance(data[n], dt.date):
                    data[n] = data[n].isoformat() 
                    r[i] = data
                elif isinstance(data[n], bool):
                    data[n] = str(data[n])
                    r[i] = data 
      
        if len(r) == 0: 
            cwLog.messageLevel = 'WARN'
            cwLog.message = context.aws_request_id + " Rows returned from " + info['database']['M']['table']['S'] + " -> 0"  
            cwLog.log_to_CloudWatch()
            data = {'status': 'Success','message':'No data retrieved'}
            return data
        else: 
            cwLog.messageLevel = 'WARN'
            cwLog.message =  context.aws_request_id + " Rows returned from " + info['database']['M']['table']['S'] + " -> " + str(len(r)) 
            cwLog.log_to_CloudWatch() 
      
        s3FileName = f"mssql_files/{event['targetDomain']}*{info['PME_ResourceName']['S']}.json" 
        cwLog.messageLevel = 'INFO'
        cwLog.message = "SQL Results sent to -> " + s3FileName + " in Bucket " + awsbucket 
        cwLog.log_to_CloudWatch()
        s3response = s3.put_object(
             Body=bytes(json.dumps(r, default=str).encode()),
             Bucket=awsbucket,
             Key=s3FileName
        )  
        if s3response['ResponseMetadata']['HTTPStatusCode'] != 200:
            cwLog.messageLevel = 'ERROR'
            cwLog.message = "JSON write failed!"
            cwLog.log_to_CloudWatch()
            data = {'status': 'Fail','message':'Unable to write to ' + event['targetDomain'] + ' ' + info['PME_ResourceName']['S'] + '.json'}
            return data

    except Exception as e: 
        cwLog.messageLevel = 'ERROR'
        cwLog.message = f"Something went wrong -> {e}"
        cwLog.log_to_CloudWatch()
        data = {'status': 'Fail','message':f'Something went wrong -> {e}'}
        return data
 
    data = {'status': 'Success','message':f'All Good'}
    return data