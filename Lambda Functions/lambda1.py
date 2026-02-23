import json
import boto3
import os
from urllib.parse import unquote_plus
from datetime import datetime, timezone

# Initialize AWS service clients (outside handler for warm starts)
# - S3: read uploaded message (.txt)
# - Comprehend: 🔎 DetectSentiment (the AI step)
# - SQS: send a compact JSON payload to the appropriate queue
s3 = boto3.client('s3')
comprehend = boto3.client('comprehend')
sqs = boto3.client('sqs')

# Required env vars
HIGH_PRIORITY_QUEUE_URL = os.environ['HIGH_PRIORITY_QUEUE_URL']
NORMAL_QUEUE_URL = os.environ['NORMAL_QUEUE_URL']

# Optional: set to "true" to move files to processed/ after success
MOVE_TO_PROCESSED = os.environ.get('MOVE_TO_PROCESSED', 'false').lower() == 'true'
PROCESSED_PREFIX = os.environ.get('PROCESSED_PREFIX', 'processed/')
MAX_BYTES = 5000  # Comprehend synchronous DetectSentiment limit

def _truncate_utf8_bytes(text: str, max_bytes: int) -> str:
    data = text.encode('utf-8')
    if len(data) <= max_bytes:
        return text
    return data[:max_bytes].decode('utf-8', errors='ignore')

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _route_queue(sentiment: str):
    if sentiment == 'NEGATIVE':
        return HIGH_PRIORITY_QUEUE_URL, "HighPriorityQueue", "🔴 HIGH PRIORITY"
    return NORMAL_QUEUE_URL, "NormalQueue", "🟢 NORMAL PRIORITY"

def lambda_handler(event, context):
    results = []

    try:
        records = event.get('Records', [])
        if not records:
            return {'statusCode': 400, 'body': json.dumps({'error': 'No Records in event'})}

        for rec in records:
            try:
                s3ev = rec['s3']
                bucket = s3ev['bucket']['name']
                key = unquote_plus(s3ev['object']['key'])

                print("=" * 64)
                print(f"NEW MESSAGE  bucket={bucket} key={key}")

                # Read file
                obj = s3.get_object(Bucket=bucket, Key=key)
                body_bytes = obj['Body'].read()
                try:
                    text = body_bytes.decode('utf-8')
                except UnicodeDecodeError:
                    # Basic guard for non-text uploads
                    print("Non-UTF8 content, skipping.")
                    results.append({'file': key, 'status': 'skipped_non_utf8'})
                    continue

                if not text.strip():
                    print("Empty file, skipping.")
                    results.append({'file': key, 'status': 'empty_file'})
                    continue

                # Truncate by bytes for Comprehend
                text_for_analysis = _truncate_utf8_bytes(text, MAX_BYTES)

                # --- Amazon Comprehend (WHERE AI HAPPENS) ---
# Sync API limit is 5,000 BYTES (not chars), so we truncated safely above.
# Returns label (POSITIVE/NEGATIVE/NEUTRAL/MIXED) + per-class scores.
                resp = comprehend.detect_sentiment(Text=text_for_analysis, LanguageCode='en')
                sentiment = resp['Sentiment']
                scores = resp['SentimentScore']

                print(f"Detected sentiment={sentiment} scores={scores}")

                # Build SQS payload (no full text; include small preview + S3 pointer)
                preview = text[:300]
                message_body = {
                    's3_bucket': bucket,
                    's3_key': key,
                    'sentiment': sentiment,
                    'sentiment_scores': {
                        'positive': float(scores['Positive']),
                        'negative': float(scores['Negative']),
                        'neutral': float(scores['Neutral']),
                        'mixed': float(scores['Mixed'])
                    },
                    'preview': preview,
                    'timestamp': _now_iso()
                }

                queue_url, queue_name, priority_label = _route_queue(sentiment)
                sqs_resp = sqs.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps(message_body),
                    MessageAttributes={
                        'Sentiment': {'StringValue': sentiment, 'DataType': 'String'},
                        'S3Key': {'StringValue': key, 'DataType': 'String'}
                    }
                )

                msg_id = sqs_resp['MessageId']
                print(f"Sent to {queue_name} ({priority_label})  message_id={msg_id}")

                # (Optional) move to processed/
                if MOVE_TO_PROCESSED:
                    dest_key = key if '/' not in key else '/'.join(key.split('/')[1:])
                    dest_key = f"{PROCESSED_PREFIX}{dest_key}"
                    s3.copy_object(
                        Bucket=bucket,
                        CopySource={'Bucket': bucket, 'Key': key},
                        Key=dest_key
                    )
                    s3.delete_object(Bucket=bucket, Key=key)
                    print(f"Moved to {dest_key}")

                results.append({
                    'file': key,
                    'queue': queue_name,
                    'priority': priority_label,
                    'sentiment': sentiment,
                    'message_id': msg_id
                })

            except s3.exceptions.NoSuchKey:
                print(f"NoSuchKey for {key}")
                results.append({'file': key, 'error': 'NoSuchKey'})
            except Exception as e:
                print(f"Error processing {rec}: {e}")
                results.append({'file': rec.get('s3', {}).get('object', {}).get('key', 'unknown'),
                                'error': str(e)})

        status = 207 if any('error' in r for r in results) else 200
        return {'statusCode': status, 'body': json.dumps({'results': results})}

    except Exception as e:
        print(f"Fatal handler error: {e}")
        return {'statusCode': 500, 'body': json.dumps({'error': 'handler_error', 'details': str(e)})}
