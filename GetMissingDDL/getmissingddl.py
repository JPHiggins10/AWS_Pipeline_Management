import json
import pyodbc
import boto3
import decimal
from datetime import datetime
import datetime as dt
import time
import copy

from utils import get_db_config, PME_Logging, get_table_info, validate_fieldname, camel_case_split, get_domain_server_info   
from re import sub

def getschema(event, context):
    t = time.localtime()
    env = get_db_config('PME_Config') 
    cwLog = PME_Logging()
    cwLog.awsGroup = env['Log_Location']['S']
    cwLog.awsStream = str(t.tm_yday) + "_" + str(t.tm_year) + "_GetSchema "
    cwLog.debugLevel = env['Log_Level']['S']
  
    watched_bucket = env['File_Drop_Bucket']['S']
    awsbucket = env['AWS_Bucket']['S'] 
 
    cwLog.domain = event['targetDomain'] 
    cwLog.rn = event['Dataset']
    cwLog.messageLevel = 'INFO'
    cwLog.message = '*************STARTING REQUEST*************'
    cwLog.log_to_CloudWatch()

    log_info = copy.copy(event)
    del log_info['SecretString'] 
    cwLog.message = f"Event ->  {log_info}"
    cwLog.log_to_CloudWatch()

    dbInfo = get_domain_server_info(env['customerDomains']['L'], log_info['targetDomain'])
    db_host = dbInfo['host']
    database = dbInfo['database']
 
    apptoken = json.loads(event['SecretString']['SecretString'] )
 
    db_user = apptoken['mssql_user']
    db_pwd = apptoken['mssql_password']

    s3 = boto3.client('s3',
    region_name='us-west-2' 
     )
 
    #establishing the connection
    conn_str = f'Driver={{ODBC Driver 17 for SQL Server}};Server={db_host};Database={database};UID={db_user};PWD={db_pwd};TrustServerCertificate=yes;'
    cwLog.message = f"Connection String -> {conn_str}"
    cwLog.log_to_CloudWatch()

    try:
        conn = pyodbc.connect(conn_str)
        dataset = event['Dataset'] 
        """if dataset['requireDDL']['BOOL'] == True:
            data = {'status': 'Success','message':f'No DDL required'}
            return data""" 
        
        tableInfo = get_table_info('PME_Config',dataset)
        tableName = tableInfo['database']['M']['table']['S']
        tableSchema = tableInfo['database']['M']['schema']['S']
        cursor = conn.cursor()   
        cwLog.messageLevel = 'INFO' 
        cwLog.message = f'Getting DB Schema for -> ' + tableName
        cwLog.log_to_CloudWatch()
        cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '{tableName}' and TABLE_SCHEMA = '{tableSchema}'" )  
        rs = [dict((cursor.description[i][0], value) \
                   for i, value in enumerate(row)) for row in cursor.fetchall()]
                   
        sql_text = "SELECT "

        for schema in rs:
            s = validate_fieldname("_".join(camel_case_split(schema["COLUMN_NAME"])))
            sql_text = sql_text + " " + schema['COLUMN_NAME'] + " AS " + s + ","
    
        sql_text = sub('\,$', '', sql_text) 
        s3.put_object(Body=sql_text, Bucket=awsbucket, Key='sql_definition/' + dataset + '.sql')

    except pyodbc.Error as ex:
        sqlstate = ex.args[1]
        cwLog.messageLevel = 'ERROR'
        cwLog.message = sqlstate 
        cwLog.log_to_CloudWatch()
        data = {'status': 'Fail','message': sqlstate}
        return data
    
    except Exception as e:
        cwLog.messageLevel = 'ERROR'
        cwLog.message = f"Something went wrong -> {e}"
        cwLog.log_to_CloudWatch()
        data = {'status': 'Fail','message':f'Something went wrong -> {e}'}
        return data
     
        cwLog.messageLevel = 'WARN' 
        cwLog.message = f' {context.aws_request_id} DB Schema for -> {tableName} Gathered'
        cwLog.log_to_CloudWatch()

    data = {'status': 'Success','message':f'All Good'}
    return data
    