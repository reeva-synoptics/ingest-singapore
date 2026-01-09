# Ingest Pipeline Checklist (Metadata + Observations)

This document outlines the steps required to adapt, test, and deploy a new data ingest using the provided codebase. It is broken into two stages: **Metadata ingest** and **Observation ingest**.

## METADATA INGEST (STATION SETUP)

**Goal:** Prepare and upload station metadata to the dev environment for validation and use for the observation ingest.

### 1. Gather and Clean Station Metadata
- Source metadata from the provider (CSV, API, JSON, etc.)
- Format metadata as a dictionary keyed by "SYNOPTIC_STID", with required fields:
  - "SYNOPTIC_STID", "NAME", "LAT", "LON", "ELEVATION"
  - Optional fields: "OTID"
- Watch for non-latin characters!

### 2. Set Script Constants
Update the following constants in `metadata_ingest.py`:
- "INGEST_NAME"
- "MNET_ID", "MNET_SHORTNAME"
- "STID_PREFIX"
- "ELEVATION_UNIT" (must be "METERS" or "FEET")
- "RESTRICTED_DATA_STATUS", "RESTRICTED_METADATA_STATUS"

### 3. Run Metadata Ingest in Dev Mode
Run using the CLI or a test lambda driver:
```
python metadata_ingest.py --dev --firstrun
```
This step:
- Validates metadata formatting
- Uploads metadata to dev MetaManager
- Uploads "stations_metadata.json" to S3

### 4. Verify in Dev Database
- Check the dev Metamoth Table for expected station entries
- Confirm accurate lat/lon/elevation/STID formatting
- Ensure no unintended overwrites occurred

## OBSERVATION INGEST

**Goal:** Retrieve, parse, validate, and submit observation data to POE (dev first!), with staged testing from local dev through to production deployment.

### 1. Retrieve and Parse Raw Provider Data
Before any dev testing:
- **Fetch Raw Data**
  - Use "requests", S3 listings, FTP, or other provider APIs
  - Target a raw format (e.g., BUFR, JSON, CSV)
- **Write a Parsing Function**
  - Convert raw data into a "grouped_obs_set" dictionary keyed by "station|timestamp"
  - Ensure values follow your standardized vargem/unit format
- **Write a Caching Function**
  - Implement logic to retain raw raw obs in the cache bucket

### 2. Set Script Constants
Update `obs_lambda_handler.py`:
- "INGEST_NAME", "STID_PREFIX"
- Logging is read in by the LOG_LEVEL arg, please note the following rules for log levels;

#### Proper log level usage:
- ERROR - For actual errors/failures that need attention
- WARNING - For concerning but non-fatal issues
- INFO - For normal operational information (like successful operations). Use this sparingly, as this is what the default level is for args.
- DEBUG - For detailed troubleshooting info...feel free to use this judiciously. Use this instead of print statements


### 3. Test Locally First
Mock or point to real raw data and run:
```
python test_lambda.py --dev
```
This tests:
- Successful parsing to "grouped_obs"
- Variable/date/station validations
- Saving of local test outputs:
  - "../dev/grouped_obs.txt"
  - "../dev/station_meta.json"

### 4. Run in Dev via Lambda
- Deploy the ingest Lambda to AWS, point at dev POE
- Trigger manually via the Test button in lambda
- Check CloudWatch logs for successful processing

### 5. Validate Output in Dev Database
- Confirm data is arriving in the dev POE / dev DBinsert / dev moth
- Use STID + timestamp filters to verify that deduplication is working
- Check variable units, timestamps, etc.

## PRODUCTION DEPLOYMENT

**After dev testing passes, promote to prod**

### 1. Update Constants and Logging
- Set logger level to "WARNING"
- Remove or disable "--dev" references
- Validate "args.dev" is False before any live POE submission

### 2. Deploy Observation Lambda in Production
- Push Lambda code
- Monitor logs for data submission activity

### 3. Final Verification
- Confirm data is flowing into POE and production databases
- Monitor for unexpected ingestion patterns, duplicates, or parsing failures
- Run sense entries on Cron
- Update Confluence Documentation
- View in Viewer (sometimes takes a day before the data shows up here)

## Additional Notes

- Never overwrite existing STIDs in MetaManager once deployed.
- Always test full ingest (metadata + obs) in dev before switching to production.
- The "grouped_obs.txt" and "stations_metadata.json" files are your main checkpoints for debugging and review.
- Check ".sql" files from the metadata ingest — they contain schema-related updates and should be cleaned up after review.