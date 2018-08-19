from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import psycopg2

#import requests
dynamodb_client = boto3.resource('dynamodb')
dynamodb_client = boto3.resource('redshift')

hashkey = 'IP'
secondKey = 'dateLastPageView'

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

def lambda_handler(event, context):

    print(context)
    table = dynamodb_client.Table('SessionMemoireV3')
    response = table.query(

          KeyConditionExpression=Key(hashkey).eq(str(event["adresse_ip_user"]))  & Key(secondKey).between(str(int(event["date_action"])-1200), str(event["date_action"]))

    )
    if(response["Count"] >=1):
        responseDel = table.delete_item(
            Key={
                hashkey: response['Items'][0][hashkey]
                
            }
        
        )
        response = table.put_item(
            Item={
                'dateLastPageView': str(event["date_action"]),
                'CountPageView': response['Items'][0]['CountPageView']+1,
                'IP': str(event["adresse_ip_user"]),
                'dateBegSession': response['Items'][0]['dateBegSession']

            }
        )


    else:
        response = table.put_item(
            Item={
                'dateLastPageView': str(event["date_action"]),
                'CountPageView': 1,
                'IP': str(event["adresse_ip_user"]),
                'dateBegSession': event["date_action"]

            }
        )
    #    print("todo")
    return response
    #return event["adresse_ip_user"]
