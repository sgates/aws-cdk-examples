# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import os
import json
import logging
import uuid
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

patch_all()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")


def log_event(level, message, **kwargs):
    """Log structured JSON events"""
    log_entry = {
        "level": level,
        "message": message,
        **kwargs
    }
    logger.info(json.dumps(log_entry))


def handler(event, context):
    request_id = context.request_id
    trace_id = os.environ.get('_X_AMZN_TRACE_ID', 'N/A')
    table = os.environ.get("TABLE_NAME")
    
    try:
        log_event("INFO", "Processing request",
                  request_id=request_id,
                  trace_id=trace_id,
                  table_name=table)
        
        if event.get("body"):
            item = json.loads(event["body"])
            log_event("INFO", "Received payload",
                      request_id=request_id,
                      payload=item)
            
            dynamodb_client.put_item(
                TableName=table,
                Item={
                    "year": {"N": str(item["year"])},
                    "title": {"S": str(item["title"])},
                    "id": {"S": str(item["id"])}
                },
            )
        else:
            log_event("INFO", "No payload provided, using default data",
                      request_id=request_id)
            
            dynamodb_client.put_item(
                TableName=table,
                Item={
                    "year": {"N": "2012"},
                    "title": {"S": "The Amazing Spider-Man 2"},
                    "id": {"S": str(uuid.uuid4())},
                },
            )
        
        log_event("INFO", "Successfully inserted data",
                  request_id=request_id)
        
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": "Successfully inserted data!"}),
        }
    
    except json.JSONDecodeError as e:
        log_event("ERROR", "Invalid JSON in request body",
                  request_id=request_id,
                  error=str(e),
                  error_type="JSONDecodeError")
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Invalid JSON format"}),
        }
    
    except KeyError as e:
        log_event("ERROR", "Missing required field in payload",
                  request_id=request_id,
                  error=str(e),
                  error_type="KeyError")
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Missing required field"}),
        }
    
    except Exception as e:
        log_event("ERROR", "Failed to process request",
                  request_id=request_id,
                  trace_id=trace_id,
                  error=str(e),
                  error_type=type(e).__name__)
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Internal server error"}),
        }
