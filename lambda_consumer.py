import boto3
import json
import time
import base64
import os

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')
table = dynamodb.Table('RealTimeTable')

def lambda_handler(event, context):
    print("Event Received:", event)
    
    for record in event['Records']:
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        data = json.loads(payload)
        print("Decoded Payload:", data)

        # Save to DynamoDB
        try:
            table.put_item(Item=data)
            print("DynamoDB insert successful")
        except Exception as e:
            print("DynamoDB ERROR:", str(e))

        # Save to S3
        try:
            s3.put_object(
                Bucket=os.environ['S3_BUCKET_NAME'],
                Key=f"data/{int(time.time())}.json",
                Body=json.dumps(data)
            )
            print("S3 upload successful")
        except Exception as e:
            print("S3 ERROR:", str(e))

    return {
        'statusCode': 200,
        'body': json.dumps('Success')
    }
