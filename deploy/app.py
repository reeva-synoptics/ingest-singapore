#!/usr/bin/env python3
import aws_cdk as cdk

from stacks.obs_lambda_stack import {INGEST_NAME}ObsLambda
from stacks.meta_lambda_stack import {INGEST_NAME}MetaLambda

# Define env
env = {
    'account': "905418025696",
    'region': "us-west-2",
}

# Define the new S3 bucket name here
ingest_s3_bucket_name = ""
ingest_s3_bucket_arn = f"arn:aws:s3:::{ingest_s3_bucket_name}"

# The cache bucket below is used as a temporary cache for incoming raw data
cache_s3_bucket_name = "synoptic-ingest-provider-data-cache-a4fb6"
cache_s3_bucket_arn = f"arn:aws:s3:::{cache_s3_bucket_name}"

config = {'vpc_id': "vpc-09a07e46ba606169b",
          "vpc_name": "ingest-uw2-vpc",
          "vpc_subnet_id": ["subnet-07287ffd7252971f0", "subnet-069ceb1375036964b"],
          "ingest_s3_bucket_arn": [f"{ingest_s3_bucket_arn}/*", ingest_s3_bucket_arn],
          "ingest_bucket_path_prefix": ingest_s3_bucket_name,
          "cache_s3_bucket_arn": [f"{cache_s3_bucket_arn}/*", cache_s3_bucket_arn],
          "cache_bucket_path_prefix": cache_s3_bucket_name}



obs_lambda_environment = {
    'DEV': 'true',
    'LOCAL_RUN': 'true',
    'LOG_LEVEL': 'INFO',
    'POE_SOCKET_ADDRESS': "10.0.0.114", #"10.0.0.114" #"mesonet-v2.entry.int.synopticdata.net",
    'POE_SOCKET_PORT': '8095',
    'POE_CHUNK_SIZE': '2000',
    'FORCE_IO_DUMP': 'true',
    'INTERNAL_BUCKET_NAME': ingest_s3_bucket_name,
    'CACHE_BUCKET_NAME': cache_s3_bucket_name
}

meta_lambda_environment = {
    'DEV': 'true',
    'LOCAL_RUN': 'true',
    'LOG_LEVEL': 'DEBUG',
    'METAMGR_SOCKET_ADDRESS': "10.14.159.245",
    'METAMGR_SOCKET_PORT': '8888',
    'ENDPOINT': 'stations',
    'FORCE_IO_DUMP': 'true',
    'INTERNAL_BUCKET_NAME': ingest_s3_bucket_name,
    'FIRST_RUN': 'false'
}

app = cdk.App()

{INGEST_NAME}ObsLambda(app, id="{INGEST_NAME}ObsLambda", lambda_env=obs_lambda_environment, config=config, env=env)
{INGEST_NAME}MetaLambda(app, id="{INGEST_NAME}MetaLambda", lambda_env=meta_lambda_environment, config=config, env=env)

# DEPLOYMENT EXAMPLES
#cdk deploy {INGEST_NAME}ObsLambda --profile ingest
#cdk deploy {INGEST_NAME}MetaLambda --profile ingest
#BUILDX_NO_DEFAULT_ATTESTATIONS=1 cdk deploy {INGEST_NAME}ObsLambda --profile ingest
#BUILDX_NO_DEFAULT_ATTESTATIONS=1 cdk deploy {INGEST_NAME}MetaLambda --profile ingest
app.synth()