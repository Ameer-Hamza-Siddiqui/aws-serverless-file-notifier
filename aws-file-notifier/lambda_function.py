import json
import os
import time
from urllib.parse import unquote_plus

import boto3

s3 = boto3.client("s3")
sns = boto3.client("sns")
logs = boto3.client("logs")


def write_log(status, **details):
    logs.put_log_events(
        logGroupName=os.environ["LOG_GROUP"],
        logStreamName=os.environ["LOG_STREAM"],
        logEvents=[{
            "timestamp": int(time.time() * 1000),
            "message": json.dumps({"status": status, **details})
        }]
    )


def get_files(body):
    # S3 sends a test event when notifications are configured.
    if body.get("Event") == "s3:TestEvent":
        return []

    # Manually sent SQS message.
    if "bucket" in body and "key" in body:
        return [(body["bucket"], body["key"])]

    # Automatic S3 upload notifications.
    if "Records" in body:
        files = []
        for item in body["Records"]:
            if item.get("eventSource") != "aws:s3":
                raise ValueError("Unsupported event source")
            files.append((
                item["s3"]["bucket"]["name"],
                unquote_plus(item["s3"]["object"]["key"])
            ))
        return files

    raise ValueError("Invalid message format")


def lambda_handler(event, context):
    failures = []

    for record in event["Records"]:
        message_id = record["messageId"]

        try:
            body = json.loads(record["body"])
            files = get_files(body)

            for bucket, key in files:
                if (
                    bucket != os.environ["ALLOWED_BUCKET"]
                    or key != os.environ["ALLOWED_KEY"]
                ):
                    raise ValueError("Object not allowed")

                metadata = s3.head_object(Bucket=bucket, Key=key)

                details = {
                    "File name": key,
                    "Bucket": bucket,
                    "Size (bytes)": metadata["ContentLength"],
                    "Content type": metadata.get("ContentType", "unknown"),
                    "Last modified": metadata["LastModified"].isoformat(),
                    "ETag": metadata["ETag"].strip('"'),
                    "Storage class": metadata.get("StorageClass", "STANDARD")
                }

                sns.publish(
                    TopicArn=os.environ["TOPIC_ARN"],
                    Subject="S3 File Upload Notification",
                    Message="\n".join(
                        f"{name}: {value}"
                        for name, value in details.items()
                    )
                )

                write_log(
                    "EMAIL_PUBLISHED",
                    message_id=message_id,
                    file=key,
                    size_bytes=metadata["ContentLength"]
                )

            if not files:
                write_log("S3_TEST_EVENT_IGNORED")

        except Exception as error:
            try:
                write_log(
                    "PROCESSING_FAILED",
                    message_id=message_id,
                    error_type=type(error).__name__
                )
            except Exception:
                pass

            failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": failures}



/////////////////////////
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "lambda:CreateFunction",
        "lambda:GetFunction",
        "lambda:GetFunctionConfiguration",
        "lambda:DeleteFunction"
      ],
      "Resource": "arn:aws:lambda:ap-southeast-1:340003870627:function:devops-file-notifier"
    }
  ]
}


///////////////////////////////
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "lambda:CreateEventSourceMapping",
      "Resource": "*",
      "Condition": {
        "ArnEquals": {
          "lambda:FunctionArn": "arn:aws:lambda:ap-southeast-1:340003870627:function:devops-file-notifier"
        },
        "StringEquals": {
          "aws:RequestedRegion": "ap-southeast-1"
        }
      }
    }
  ]
}