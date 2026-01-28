import os
import json
import time

# Set Args BEFORE loading Args below
os.environ['AWS_PROFILE'] = 'ingest'
os.environ['DEV'] = 'True'
os.environ['LOCAL_RUN'] = 'True'
os.environ['LOG_LEVEL'] = 'DEBUG'

# Set any required environment variables
os.environ['INTERNAL_BUCKET_NAME'] = "ingest-singapore"

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
print("=" * 80)
print("Starting Singapore Ingest Test")
print("=" * 80)

response = main(event, Context())

print("=" * 80)
print("Test completed. Check ../dev folder for logs and output files:")
print("  - ingest_singapore_obs.log (full log)")
print("  - grouped_obs.txt (parsed observations)")
print("  - station_meta.json (station metadata if available)")
print("=" * 80)