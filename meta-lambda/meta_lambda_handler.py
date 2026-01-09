import csv
import re
import requests
from io import StringIO
from datetime import datetime, timedelta
from ingestlib import aws
import os
import sys
import json
import logging
import time
from ingestlib import station_lookup, parse
import math
import posixpath
########################################################################################################################
# OVERVIEW
########################################################################################################################

# This script handles metadata processing for new data ingests. The goal is to get metadata into metamoth. Key considerations:
#
# Metadata Sources:
# - Provider metadata endpoint (preferred): Allows pre-compilation/validation of STIDs before observation ingestion
# - Observation ingest script: Metadata extracted during observation processing
#
# STID Management:
# - Critical to maintain unique STIDs
# - Once created, STIDs must never be rewritten
# - Careful handling required when creating STIDs during observation processing to avoid POE receiving STIDs that don't exist in metamoth.
#
# Process Flow:
# 1. Define Constants (Elevation Unit, MNET ID, etc.)
# 1. Collect raw metadata (source-dependent)
# 2. Validate/ensure unique STIDs that DON'T get overwritten/edited
# 3. Parse station details (lat, lon, elevation, other_id)
# 4. Insert into database via:
#    - Metamanager (preferred method)
#    - Station lookup (backup method)
#
# Output:
# - Creates SQL for metadata database insertion
# - Updates stations_metadata.json

########################################################################################################################
# DEFINE CONSTANTS
########################################################################################################################
INGEST_NAME = '' #TODO Update Ingest Name
M_TO_FEET = 3.28084
ELEVATION_UNIT = '' # ELEVATION UNIT OF THIS INGESTS METADATA MUST BE EITHER 'METERS' OR 'FEET'. METAMOTH CURRENTLY STORES ELEVATION IN FEET, SO WE WILL CONVERT IF IT'S IN METERS. 
MNET_ID = # CREATE NEW MNET_ID FOR THIS INGEST
MNET_SHORTNAME = #
RESTRICTED_DATA_STATUS = # TRUE OR FALSE, IS THE DATA RESTRICTED?
RESTRICTED_METADATA_STATUS = # TRUE OR FALSE, IS THE METADATA RESTRICTED?
STID_PREFIX = #

########################################################################################################################
# DEFINE LOGS
########################################################################################################################
logger = logging.getLogger(f"{INGEST_NAME}_ingest")

def setup_logging():
    from args import args
    logger.handlers.clear()
    logger.setLevel(getattr(logging, args.log_level))
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    if args.local_run:
        dev_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../dev'))
        os.makedirs(dev_dir, exist_ok=True)
        handler = logging.FileHandler(os.path.join(dev_dir, f'{INGEST_NAME}.log'), mode='w')
    else:
        handler = logging.StreamHandler()
    
    handler.setFormatter(formatter)
    logger.addHandler(handler)
########################################################################################################################
# DEFINE ETL/PARSING FUNCTIONS
########################################################################################################################

def generate_metadata_payload(station_meta, payload_type, source_info=None):
    """
    Generates the metadata payload for ingestlib station lookup

    Args:
        station_meta (dict): A dictionary containing station metadata.
        payload_type (str): Type of payload ('station_lookup' or 'metamanager').
        source_info (dict): Optional source information for the metamanager payload.

    Returns:
        dict or str: Parsed metadata payload based on the payload type.
    """
    if payload_type not in {"station_lookup", "metamanager"}:
        raise ValueError("Invalid payload_type. Must be 'station_lookup' or 'metamanager'.")

    metadata = []
    
    for station_id, row in station_meta.items():
        try:
            # Extract required fields from the row
            stid = row.get('SYNOPTIC_STID', None)
            name = row.get('NAME', None)
            lat = row.get('LAT', None)
            lon = row.get('LON', None)
            otid = row.get('OTID', None)
            elevation = row.get('ELEVATION', None)

            # Check lat/lon validity
            if lat is None or lon is None:
                continue
            lat = float(lat)
            lon = float(lon)
            if not (-90 <= lat <= 90 and -180 <= lon <= 180) or (lat == 0 and lon == 0):
                logger.debug(f"Skipping station {station_id} due to invalid lat/lon: {lat}, {lon}")
                continue

            # Check Elevation
            if elevation is not None:
                elevation = float(elevation)
                
                if ELEVATION_UNIT == 'METERS':
                    elevation *= M_TO_FEET
                elif ELEVATION_UNIT != 'FEET':
                    raise ValueError("Invalid ELEVATION_UNIT, must be 'METERS' or 'FEET'")
                
                if math.isnan(elevation):
                    elevation = None

            if stid and name:
                station = {
                    "STID": stid,
                    "NAME": name.replace("'", "''"),
                    "LATITUDE": lat,
                    "LONGITUDE": lon,
                    "OTHER_ID": otid,
                    "MNET_ID": MNET_ID,
                    "ELEVATION": None if elevation is None else round(elevation, 3),
                    "RESTRICTED_DATA": row.get('RESTRICTED_DATA', RESTRICTED_DATA_STATUS),
                    "RESTRICTED_METADATA": row.get('RESTRICTED_METADATA', RESTRICTED_METADATA_STATUS)
                }
                metadata.append(station)
            else:
                logger.debug(f"Skipping station {station_id} due to missing required fields: STID or NAME.")
        except ValueError as e:
            logger.debug(f"Skipping station {station_id} due to error: {e}")
    
    if payload_type == "station_lookup":
        payload = {
            "MNET_ID": MNET_ID,
            "STNS": metadata
        }
    else:
        default_source = {
            "name": "Administration Console",
            "environment": str(MNET_ID)
        }
        payload = {
            "source": source_info if source_info else default_source,
            "metadata": metadata
        }
    
    return json.dumps(payload, indent=4) if payload_type == "metamanager" else payload


def update_stations(url: str, headers: dict, payload: str) -> requests.Response:
    """
    Sends a PUT request to update station data.

    Args:
        url (str): The URL to send the request to.
        headers (dict): The headers to include in the request.
        payload (str): The payload data in JSON format.

    Returns:
        requests.Response: The response from the server.
    """
    response = requests.request("PUT", url, headers=headers, data=payload)
    return response

# Function to save data to a JSON file
def save_to_json(data, filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)

########################################################################################################################
# MAIN FUNCTION
########################################################################################################################
def main(event, context):
    from args import args
    # set up local logging to save in ../dev folder
    setup_logging()
    logger.debug(f"ARGS LOADED from: {__file__}")
    logger.debug(f"ENV at load time: DEV={os.getenv('DEV')} LOCAL_RUN={os.getenv('LOCAL_RUN')} LOG_LEVEL={os.getenv('LOG_LEVEL')}")
    logger.debug(vars(args))
    
    start_runtime = time.time()
    try:
        # Declare S3 Paths for Metadata Storage
        s3_bucket_name = os.environ.get('INTERNAL_BUCKET_NAME')
        if not s3_bucket_name:
            raise ValueError("Missing INTERNAL_BUCKET_NAME env var.")

        s3_meta_work_dir = "metadata"
        s3_station_meta_file = posixpath.join(s3_meta_work_dir, f"{INGEST_NAME}_stations_metadata.json")
        metamgr_url = f"http://{args.metamgr_socket_address}:{args.metamgr_socket_port}/{args.endpoint}"

        # Declare Local Paths
        work_dir = '/tmp/tmp/'
        os.makedirs(work_dir, exist_ok=True)
        station_meta_file = os.path.join(work_dir, f"{INGEST_NAME}_stations_metadata.json")

        # Load Existing Stations and Payload Files
        try:
            aws.S3.download_file(bucket=s3_bucket_name, object_key=s3_station_meta_file, local_directory=work_dir)
            with open(station_meta_file, 'r', encoding='utf-8') as json_file:
                existing_stations = json.load(json_file)
            logger.info(f"Loaded {len(existing_stations)} existing stations")
        except FileNotFoundError:
            logger.info("No existing station metadata found")
        except Exception as e:
            logger.warning(f"Failed to load existing station metadata: {e}")
        ########################################################################################################################
        # Fetch Metadata
        ########################################################################################################################
        # --------------- 1. SECRET MANAGEMENT (if applicable) ---------------
        # Retrieve and parse API credentials from AWS Secrets Manager
        secret = aws.SecretsManager.get(secret_name="")
        try:
            secret_dict = json.loads(secret)  # Parse string to dictionary
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing secret as JSON: {e}")
            raise
        
        api_key = secret_dict['api_key']

        # --------------- 2. RAW METADATA COLLECTION (if not collected already by obs lambda) ---------------
        # Fetch initial metadata as raw_meta variable, ideally this is from a metadata specific endpoint, although it's possible this doesn't exist...
        # raw_meta = #TODO obtian the raw metadata from the provider

        # --------------- 3. METADATA PROCESSING ---------------
        # Process data into a format that can be prepared for station lookup or metamanager payload, store as station_meta. Should be a Dictionary
        # NEVER overwrite existing Station Meta if STIDs have been defined!
        # you'll need to parse into this names, yes this is case sensitive
            #'SYNOPTIC_STID'
            # NAME
            # LAT
            # LON
            # OTID
            # ELEVATION
        # station_meta = #TODO parse this into a version ready for generate_metadata_payload(), it's possible this is just a remapping step. Never change synoptic stids after defined.

        # --------------- 4. STATION LOOKUP PAYLOAD CREATION ---------------
        station_lookup_payload = generate_metadata_payload(station_meta=station_meta, payload_type='station_lookup')

        # SAVE TO LOCAL DEV (if local_run)
        if args.local_run:
            dev_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../dev'))
            os.makedirs(dev_dir, exist_ok=True)

            # Save station_meta locally
            station_meta_dev_path = os.path.join(dev_dir, f'{INGEST_NAME}_station_meta.json')
            with open(station_meta_dev_path, 'w') as f:
                json.dump(station_meta, f, indent=4)

            # Save station_lookup_payload locally
            lookup_payload_dev_path = os.path.join(dev_dir, f'{INGEST_NAME}_station_lookup_payload.json')
            with open(lookup_payload_dev_path, 'w') as f:
                json.dump(station_lookup_payload, f, indent=4)

            logger.debug(f"[DEV] Saved station_meta to {station_meta_dev_path}")
            logger.debug(f"[DEV] Saved station_lookup_payload to {lookup_payload_dev_path}")
            logger.debug(f"[DEV] Station count: {len(station_meta)}")

        if not args.local_run:
            try:
                logger.debug('production station lookup proceeding')
                station_lookup.load_metamgr(station_lookup_payload, mode='prod', output_location=work_dir)
                logger.debug('past station lookup')
            except Exception as e:
                logger.exception(f"Station lookup failed: {e}")
                raise

            # --------------- 5. DATA PERSISTENCE ---------------

            # Save and upload station_meta
            save_to_json(data=station_meta, filename=station_meta_file)
            aws.S3.upload_file(
                local_file_path=station_meta_file,
                bucket=s3_bucket_name,
                s3_key=s3_station_meta_file
            )

            # Save and upload station_lookup_payload
            station_lookup_file = os.path.join(work_dir, f'{INGEST_NAME}_station_lookup_payload.json')
            save_to_json(data=station_lookup_payload, filename=station_lookup_file)
            s3_lookup_key = os.path.join(s3_meta_work_dir, f'{INGEST_NAME}_station_lookup_payload.json')
            aws.S3.upload_file(
                local_file_path=station_lookup_file,
                bucket=s3_bucket_name,
                s3_key=s3_lookup_key
            )

            # Clean up old SQL files
            deleted_files_count = aws.S3.delete_files(
                bucket=s3_bucket_name,
                prefix=s3_meta_work_dir,
                endswith=".sql"
            )
            logger.debug(f"Deleted {deleted_files_count} SQL files from the bucket {s3_bucket_name}")
            for file_name in os.listdir(work_dir):
                if file_name.endswith(".sql"):
                    # Get the full path of the SQL file
                    sql_updates = os.path.join(work_dir, file_name)
                    
                    # Get the path portion of the s3_key (without the file name)
                    s3_key_path = os.path.dirname(s3_station_meta_file)
                    
                    # Manually join the S3 path and the new SQL file name
                    s3_sql = f"{s3_key_path}/{os.path.basename(sql_updates)}"
                    
                    # Upload the SQL file to S3
                    aws.S3.upload_file(local_file_path=sql_updates, 
                                    bucket=s3_bucket_name, 
                                    s3_key=s3_sql)

        total_runtime = time.time() - start_runtime
        logger.info(msg=json.dumps({'completion': 1, 'time': total_runtime}))
    except:
        logger.error(msg=json.dumps({'completion': 0, 'time': total_runtime}))