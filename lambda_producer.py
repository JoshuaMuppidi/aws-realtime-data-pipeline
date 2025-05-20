import boto3
import json
import os

kinesis = boto3.client('kinesis')

def lambda_handler(event, context):
    data = json.loads(event['body'])
    kinesis.put_record(
        StreamName=os.environ['KINESIS_STREAM_NAME'],
        Data=json.dumps(data),
        PartitionKey="partition-1"
    )
    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Data sent to Kinesis!"})
    }
