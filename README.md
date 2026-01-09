# CDK Ingest Lambda Template

This repository provides a standardized template for ingest pipeline services using AWS Lambda functions. The template supports both metadata and observation ingestion workflows with comprehensive local testing capabilities.

## Repository Structure

```
cdk-ingest-lambda-template/
├── deploy/              # AWS CDK infrastructure code
├── meta-lambda/         # Metadata ingestion Lambda function
├── obs-lambda/          # Observation ingestion Lambda function  
├── sql/                 # Any SQL code required for this ingest
```

## Overview

The template consists of two main Lambda functions:

- **meta-lambda**: Handles station metadata collection, processing, and upload to MetaManager
- **obs-lambda**: Retrieves, parses, validates, and submits observation data to POE

Both functions support full end-to-end testing from your local environment without requiring VPN access, using AWS SSM port forwarding for secure remote service connections.

## Quick Start

### Prerequisites

- AWS CLI configured with `ingest` profile using AWS SSO (`aws sso login --profile ingest`)
- Python 3.9+ with required dependencies
- Access to AWS SSM for port forwarding
- AWS CDK installed (`npm install -g aws-cdk`)

### Authentication Setup

Before running any tests or deployments, authenticate with AWS SSO:
```bash
aws sso login --profile ingest
```

Note: SSO tokens expire periodically, so you may need to re-run this command if you encounter authentication errors.

### 1. Local Testing (Recommended First)

Before deployment, test the core functionality locally:

#### Test Metadata Handler
```bash
cd meta-lambda/
python test_meta_handler.py
```

#### Test Observation Handler  
```bash
cd obs-lambda/
python test_obs_handler.py
```

### 2. Setup Development Environment

Create development output directory:
```bash
mkdir -p dev
```

## Development Workflow

### Stage 1: Metadata Ingest (Station Setup)

**Goal**: Prepare and upload station metadata to dev environment for validation.

#### 1. Configure Constants
Update constants in `meta-lambda/meta_handler.py`:
- `INGEST_NAME`
- `MNET_ID`, `MNET_SHORTNAME`  
- `STID_PREFIX`
- `ELEVATION_UNIT` ("METERS" or "FEET")
- `RESTRICTED_DATA_STATUS`, `RESTRICTED_METADATA_STATUS`

#### 2. Setup Database Tunnel
Ensure you're authenticated, then open AWS SSM tunnel to database (keep running in separate terminal):
```bash
aws sso login --profile ingest

aws ssm start-session \
  --target i-095f2bbb20b7d81fc \
  --document-name AWS-StartPortForwardingSessionToRemoteHost \
  --parameters '{"host":["mothmetadata.read.db.int.synopticdata.net"],"portNumber":["3306"],"localPortNumber":["3306"]}'
```

#### 3. Run Metadata Handler Locally
```bash
cd meta-lambda/
python meta_handler.py
```

#### 4. Verify Results
- Check dev database for station entries
- Review `../dev/stations_metadata.json`
- Confirm accurate lat/lon/elevation formatting

#### 5. Add generated station SQL to Dev/Prod databases
- Using the sql file that was generated, execute that script to add the stations to first dev database then prod

### Stage 2: Observation Ingest

**Goal**: Retrieve, parse, validate, and submit observation data to POE.

#### 1. Configure Constants  
Update constants in `obs-lambda/obs_handler.py`:
- `INGEST_NAME`, `STID_PREFIX`
- Set appropriate `LOG_LEVEL` (DEBUG for testing, WARNING for production)

#### 2. Setup Service Tunnels
Ensure you're authenticated, then open tunnels to development services (separate terminals):

```bash
aws sso login --profile ingest
```

**POE/DBInsert Service:**
```bash
aws ssm start-session \
  --target i-095f2bbb20b7d81fc \
  --document-name AWS-StartPortForwardingSessionToRemoteHost \
  --parameters '{"host":["10.0.0.114"],"portNumber":["8095"],"localPortNumber":["18095"]}'
```

**MetaManager Service:**
```bash
aws ssm start-session \
  --target i-095f2bbb20b7d81fc \
  --document-name AWS-StartPortForwardingSessionToRemoteHost \
  --parameters '{"host":["10.14.159.245"],"portNumber":["8888"],"localPortNumber":["18888"]}'
```

#### 3. Run Observation Handler Locally
```bash
cd obs-lambda/
python obs_handler.py
```

#### 4. Validate Results
- **Check validation logs and ensure everything passes**
- Review `../dev/grouped_obs.txt`
- Confirm data appears in dev POE/database
- Verify deduplication is working correctly

## Production Deployment

After successful dev testing, deploy to AWS using CDK:

### 1. Configure CDK Files

Replace `{INGEST_NAME}` with your actual ingest name (e.g., "taiwan", "metar", etc.) in these files:

#### Update `deploy/app.py`
Replace all instances of `{INGEST_NAME}` with your ingest name:
```python
from stacks.obs_stack import TaiwanObsLambda  # Replace Taiwan with your ingest name
from stacks.meta_stack import TaiwanMetaLambda  # Replace Taiwan with your ingest name

# Update bucket name
ingest_s3_bucket_name = "synoptic-ingest-taiwan-t9g4r"  # Replace taiwan with your ingest name

# Update stack instances
TaiwanObsLambda(app, "TaiwanObsLambda", ...)  # Replace Taiwan with your ingest name
TaiwanMetaLambda(app, "TaiwanMetaLambda", ...)  # Replace Taiwan with your ingest name
```

#### Update `deploy/stacks/obs_stack.py`
Replace `{INGEST_NAME}` with your ingest name in the class definition:
```python
class TaiwanObsLambda(Stack):  # Replace Taiwan with your ingest name
```

#### Update `deploy/stacks/meta_stack.py`
Replace `{INGEST_NAME}` with your ingest name in the class definition:
```python
class TaiwanMetaLambda(Stack):  # Replace Taiwan with your ingest name
```

### 2. Deploy to AWS

Navigate to the deploy directory and deploy the stacks:

```bash
cd deploy/

# Ensure you're authenticated
aws sso login --profile ingest

# Deploy metadata stack first example
cdk deploy TaiwanMetaLambda --profile ingest

# Deploy observation stack example
cdk deploy TaiwanObsLambda --profile ingest
```

### 3. Post-Deployment Verification

After deployment:
1. **Monitor CloudWatch logs**: Check for processing activity and errors
2. **Verify data flow**: Confirm data reaches production POE/databases
3. **Update configuration**: Set logger level to `INFO`, disable dev flags if needed
4. **Update documentation**: Record final configuration in Confluence

## Environment Variables

### Common Variables
| Variable | Description | Example |
|----------|-------------|---------|
| `DEV` | Development mode | `true`/`false` |
| `LOCAL_RUN` | Local execution flag | `true`/`false` |
| `LOG_LEVEL` | Logging verbosity | `DEBUG`/`INFO`/`WARNING`/`ERROR` |
| `INTERNAL_BUCKET_NAME` | S3 bucket name | `your-ingest-bucket` |
| `AWS_REGION` | AWS region | `us-west-2` |

### Observation-Specific Variables
| Variable | Description | Example |
|----------|-------------|---------|
| `POE_SOCKET_ADDRESS` | POE service address | `localhost` |
| `POE_SOCKET_PORT` | POE service port | `18095` |
| `METAMGR_SOCKET_ADDRESS` | MetaManager address | `localhost` |
| `METAMGR_SOCKET_PORT` | MetaManager port | `18888` |
| `POE_CHUNK_SIZE` | Processing chunk size | `2000` |
| `FORCE_IO_DUMP` | Enable I/O debugging | `true`/`false` |

## Logging Guidelines

Use appropriate log levels for different types of messages:

- **ERROR**: Actual errors/failures requiring attention
- **WARNING**: Concerning but non-fatal issues  
- **INFO**: Normal operational information (use sparingly)
- **DEBUG**: Detailed troubleshooting info (use instead of print statements)

## Key Files and Outputs

### Development Outputs
- `dev/stations_metadata.json` - Station metadata for review
- `dev/grouped_obs.txt` - Parsed observation data
- `dev/*.log` - Detailed execution logs
- `dev/*_payload.json` - POE payloads for debugging

### Important Notes

- **Never overwrite existing STIDs in MetaManager once deployed**
- **Keep AWS SSM tunnels active during local testing**
- **Test full metadata + observation pipeline in dev before production**
- **Clean up `.sql` files from metadata ingest after review**
- **SSO tokens expire periodically - re-authenticate if you encounter AWS credential errors**

## Troubleshooting

### Common Issues

1. **Local script fails to connect to services**: 
   - Verify AWS SSM tunnels are active
   - Ensure you've run `aws sso login --profile ingest`

2. **AWS authentication errors**: 
   - Run `aws sso login --profile ingest` to refresh your SSO token
   - Verify your ingest profile is configured for AWS SSO
   - Check that your SSO session hasn't expired

3. **Database connection failures**: 
   - Ensure database tunnel is running on correct port (3306)
   - Verify localhost connectivity

4. **Missing output files**: Check permissions in `dev/` directory

5. **CDK deployment failures**:
   - Ensure you're authenticated with `aws sso login --profile ingest`
   - Verify all `{INGEST_NAME}` placeholders have been replaced
   - Check CloudFormation console for detailed error messages

### Debug Commands

```bash
# Ensure SSO authentication is current
aws sso login --profile ingest

# Verify AWS credentials
aws sts get-caller-identity --profile ingest

# Test service connectivity
curl http://localhost:18095/health  # POE service
curl http://localhost:18888/health  # MetaManager service

# Check CDK status
cdk list --profile ingest
cdk diff TaiwanObsLambda --profile ingest
```

## Support

For issues or questions:
- Review development output files in `dev/` directory
- Check CloudWatch logs for error details AFTER deployment
- Consult individual Lambda function files for specific guidance
- Validate environment variables and tunnel configurations