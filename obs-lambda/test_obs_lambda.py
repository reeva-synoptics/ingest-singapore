import os
import json
import time

# Set Args BEFORE loading Args below
os.environ['AWS_PROFILE'] = 'ingest'
os.environ['DEV'] = 'True'
os.environ['LOCAL_RUN'] = 'True'
os.environ['LOG_LEVEL'] = 'DEBUG'

# Set any required environment variables
os.environ['INTERNAL_BUCKET_NAME'] = ""

# Must load these AFTER setting Args above
from obs_lambda_handler import main
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