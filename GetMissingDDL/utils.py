import json
import boto3
import botocore
import time
import os 
from re import finditer

def camel_case_split(identifier) -> list:
    matches = finditer('.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', identifier)
    return [m.group(0) for m in matches]

def validate_fieldname(fieldname: str) -> str:
    return fieldname.replace("-", "_").lower()
    
def get_db_config(tblname):
    db = boto3.client("dynamodb")

    response = db.get_item(TableName=tblname,
                                   Key={'PME_Schema':{'S': 'Config'},
                                       'PME_ResourceName':{'S': os.getenv("domainConfig")} 
                                   })
    return(response['Item']) 

def get_table_info(tblname, key):
    db = boto3.client("dynamodb")

    response = db.get_item(
    TableName=tblname,
                                   Key={'PME_Schema':{'S': 'Dataset'},
                                       'PME_ResourceName':{'S': key}
                                  }
)
    return(response['Item'])
    
def get_domain_server_info(config_cd, domain):

    print(config_cd)
    serverInfo = {}
    for cD in config_cd:
        if cD['M']['name']['S'] == domain:
            serverInfo['host'] = cD['M']['host']['S']
            serverInfo['database'] = cD['M']['database']['S']
    
    return (serverInfo)

class PME_Logging:

    messageLevel = 0
    debugLevel = 0
    message = ''
    domain = ''
    rn = ''
    awsStream = ''
    awsGroup = ''

    def log_to_CloudWatch(self):

        cw = boto3.client('logs',
        region_name='us-west-2' 
            )
        if self.messageLevel == 'INFO':
            ml = 0
        elif self.messageLevel == 'WARN':
            ml = 5
        elif self.messageLevel == 'ERROR':
            ml = 10
        else:
            ml =  -1

        if self.debugLevel == 'INFO':
            cl = 0
        elif self.debugLevel == 'WARN':
            cl = 5
        elif self.debugLevel == 'ERROR':
            cl = 10
 
        if ml < cl:
            return
        cwMessage = f'[{self.messageLevel}] {self.domain} {self.rn} -> {self.message}'
        try:

            response = cw.put_log_events(
                logGroupName = self.awsGroup,
                logStreamName = self.awsStream,
            logEvents=[
                {
                    'timestamp': int(round(time.time() * 1000)),
                    'message': cwMessage
                },
            ],
            sequenceToken='1'
            )
        except botocore.exceptions.ClientError as e:
                try:
                    if e.response["Error"]["Code"] == 'ResourceNotFoundException':
                        response = cw.create_log_stream(
                            logGroupName = self.awsGroup,
                            logStreamName = self.awsStream
                        )
                        self.message = cwMessage
                        self.messageLevel = 'INFO'
                        self.log_to_CloudWatch()
                    else:   
                        print("Something else went wrong")
                        raise
                except botocore.exceptions.ClientError as e:
                    if e.response["Error"]["Code"] == 'ResourceAlreadyExistsException': 
                        return  
 
        return response
