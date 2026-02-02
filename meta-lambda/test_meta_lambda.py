import os
import json
import time

# Set Args BEFORE loading Args below
os.environ['AWS_PROFILE'] = 'ingest'
os.environ['DEV'] = 'False'
os.environ['LOCAL_RUN'] = 'False'
os.environ['LOG_LEVEL'] = 'DEBUG'
os.environ['DB_HOST'] = '127.0.0.1'
os.environ['DB_PORT'] = '3306'
os.environ['AWS_REGION'] = 'us-west-2'
# Set any required environment variables
os.environ['INTERNAL_BUCKET_NAME'] = "synoptic-ingest-singapore-sp014"

# Must load these AFTER setting Args above
from meta_lambda_handler import main
from args import args

# Dummy Lambda context object
class Context:
    def __init__(self):
        self.function_name = "test"
        self.memory_limit_in_mb = 128
        self.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789012:function:test"
        self.aws_request_id = "test-id"

# Simulated Lambda event
event = {}

# Run the function, look for logs in ../dev folder
response = main(event, Context())