# Local Testing Guide - Observation Lambda

Full end-to-end testing from your local computer without VPN access. This must be fully tested locally before deploying to AWS Lambda.

## Prerequisites

- Python 3.9+ with required dependencies installed
- AWS CLI configured with `ingest` profile using AWS SSO
- Access to AWS SSM for port forwarding

## Authentication Setup

Before running any tests, authenticate with AWS SSO:
```bash
aws sso login --profile ingest
```

Note: SSO tokens expire periodically - re-run this command if you encounter authentication errors.

## Setup Development Directory

Create the development directory structure:

```bash
mkdir -p ../dev
```

## Local Testing Workflow

### 1. Basic Local Test (No Remote Services)

First, test the core functionality locally without connecting to remote services:

```bash
python test_obs_handler.py
```

The test script (`test_obs_handler.py`) should look like:

```python
import os
import json
import time

# Set environment variables BEFORE importing handler
os.environ['AWS_PROFILE'] = 'ingest'
os.environ['DEV'] = 'True'
os.environ['LOCAL_RUN'] = 'True'
os.environ['LOG_LEVEL'] = 'DEBUG'
os.environ['INTERNAL_BUCKET_NAME'] = "dummy-ingest-bucket"

# Import AFTER setting environment variables
from obs_handler import main
from args import args

# Run the function - logs output to ../dev folder
response = main()
```

**What this does:**
- Tests core `obs_handler` functionality
- Runs in local development mode with debug logging
- Outputs logs and results to `../dev` folder
- Allows quick verification before connecting to remote services

### 2. Setup AWS SSM Tunnels for Full Testing

To test with actual POE and MetaManager services, set up port forwarding tunnels. Run these commands in separate terminal windows and keep them running:

#### Tunnel for POE/DBInsert Service
```bash
aws ssm start-session \
  --target i-095f2bbb20b7d81fc \
  --document-name AWS-StartPortForwardingSessionToRemoteHost \
  --parameters '{"host":["10.0.0.114"],"portNumber":["8095"],"localPortNumber":["18095"]}'
```

#### Tunnel for MetaManager Service
```bash
aws ssm start-session \
  --target i-095f2bbb20b7d81fc \
  --document-name AWS-StartPortForwardingSessionToRemoteHost \
  --parameters '{"host":["10.14.159.245"],"portNumber":["8888"],"localPortNumber":["18888"]}'
```

### 3. Run Full Local Test with Remote Services

Update your test script to connect to remote services:

```python
import os
import json
import time

# Set environment variables for full testing
os.environ['AWS_PROFILE'] = 'ingest'
os.environ['DEV'] = 'true'
os.environ['LOCAL_RUN'] = 'false'
os.environ['LOG_LEVEL'] = 'DEBUG'
os.environ['POE_SOCKET_ADDRESS'] = 'localhost'
os.environ['POE_SOCKET_PORT'] = '18095'
os.environ['METAMGR_SOCKET_ADDRESS'] = 'localhost'
os.environ['METAMGR_SOCKET_PORT'] = '18888'
os.environ['POE_CHUNK_SIZE'] = '2000'
os.environ['FORCE_IO_DUMP'] = 'true'
os.environ['OUTPUT_DIR'] = '../dev'
os.environ['INTERNAL_BUCKET_NAME'] = 'dummy-ingest-bucket'
os.environ['AWS_REGION'] = 'us-west-2'

# Import AFTER setting environment variables
from obs_handler import main
from args import args

# Run the function
response = main()
```

Execute the test:

```bash
python test_obs_handler.py
```

### 4. Validate Results

**CHECK VALIDATION LOGS AND ENSURE EVERYTHING PASSES**

Review outputs in `../dev/`:
- `grouped_obs.txt` - Parsed observation data
- `*.log` - Detailed execution logs
- `*_payload.json` - POE payloads for debugging

Verify:
- All validation checks pass
- Data appears correctly in dev POE/database
- Deduplication is working as expected
- No errors in CloudWatch-style logs

## Environment Variables Reference

| Variable | Description | Example Value |
|----------|-------------|---------------|
| `DEV` | Development mode flag | `true`/`false` |
| `LOCAL_RUN` | Local execution flag | `true`/`false` |
| `LOG_LEVEL` | Logging verbosity | `DEBUG`/`INFO`/`WARNING`/`ERROR` |
| `POE_SOCKET_ADDRESS` | POE service address | `localhost` |
| `POE_SOCKET_PORT` | POE service port | `18095` |
| `METAMGR_SOCKET_ADDRESS` | MetaManager address | `localhost` |
| `METAMGR_SOCKET_PORT` | MetaManager port | `18888` |
| `POE_CHUNK_SIZE` | Processing chunk size | `2000` |
| `FORCE_IO_DUMP` | Force I/O debugging | `true`/`false` |
| `INTERNAL_BUCKET_NAME` | S3 bucket name | `dummy-ingest-bucket` |
| `AWS_REGION` | AWS region | `us-west-2` |

## Configuration Constants

Update these constants in `obs_handler.py` before testing:

- `INGEST_NAME` - Name of your ingest source
- `STID_PREFIX` - Station ID prefix for your network
- `LOG_LEVEL` - Set to `DEBUG` for testing, `WARNING` for production

## Troubleshooting

### Common Issues

1. **Import errors or missing dependencies**:
   - Ensure all required Python packages are installed
   - Check that you're in the correct directory

2. **AWS authentication errors**:
   - Run `aws sso login --profile ingest` to refresh token
   - Verify your ingest profile is configured correctly

3. **Connection failures to POE/MetaManager**:
   - Verify AWS SSM tunnels are running in separate terminals
   - Check that tunnel ports (18095, 18888) are not in use
   - Test connectivity: `curl http://localhost:18095/health`

4. **Missing output files**:
   - Ensure `../dev/` directory exists
   - Check file permissions

5. **Validation failures**:
   - Review debug logs in `../dev/*.log`
   - Check that stations exist in MetaManager
   - Verify data format matches expected schema

### Debug Commands

```bash
# Verify AWS authentication
aws sts get-caller-identity --profile ingest

# Test service connectivity
curl http://localhost:18095/health  # POE service
curl http://localhost:18888/health  # MetaManager service

# Check SSM tunnel status
netstat -an | grep 18095
netstat -an | grep 18888
```

## Next Steps

After successful local testing:
1. Set `LOG_LEVEL` to `INFO` or `WARNING` for production
2. Deploy to AWS using CDK (see main README)
3. Monitor CloudWatch logs for production behavior
4. Verify data flow in production environment

## Notes

- Keep AWS SSM tunnel sessions running while testing with remote services
- Development output is written to `../dev/` directory
- Always test full validation pipeline before deployment
- SSO tokens expire - re-authenticate if you see credential errors