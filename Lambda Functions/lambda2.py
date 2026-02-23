import json
import os
import boto3

sqs = boto3.client('sqs')

HIGH_Q = os.environ['HIGH_PRIORITY_QUEUE_URL']
NORMAL_Q = os.environ['NORMAL_QUEUE_URL']

def _read_from(queue_url, label):
    resp = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=5,
        VisibilityTimeout=2,
        WaitTimeSeconds=1,
        MessageAttributeNames=['All']
    )
    messages = []
    for m in resp.get('Messages', []):
        body = json.loads(m['Body'])
        messages.append({
            "queue": label,
            "sentiment": body.get("sentiment"),
            "scores": body.get("sentiment_scores"),
            "preview": body.get("preview"),
            "timestamp": body.get("timestamp"),
            "s3_bucket": body.get("s3_bucket"),
            "s3_key": body.get("s3_key")
        })
        # demo mode: do NOT delete messages, so learners can see them in console too
        # sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])
    return messages

def lambda_handler(event, context):
    high_msgs = _read_from(HIGH_Q, "HighPriorityQueue")
    normal_msgs = _read_from(NORMAL_Q, "NormalQueue")
    combined = high_msgs + normal_msgs

    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(combined)
    }
