import csv
import re
import requests
from io import StringIO
from datetime import datetime, timedelta
from ingestlib import aws
import sys, os
import ssl
import certifi
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from enum import IntEnum
import boto3
import json
import logging
import time
import concurrent.futures
from ingestlib import station_lookup, parse, core
import math
import posixpath
import re, unicodedata
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
INGEST_NAME = "singapore"
M_TO_FEET = 3.28084
ELEVATION_UNIT = 'METERS' # ELEVATION UNIT OF THIS INGESTS METADATA MUST BE EITHER 'METERS' OR 'FEET'. METAMOTH CURRENTLY STORES ELEVATION IN FEET, SO WE WILL CONVERT IF IT'S IN METERS. 
MNET_ID = 340 # CREATE NEW MNET_ID FOR THIS INGEST
MNET_SHORTNAME = "Singapore" #TODO add the mnet shortname
RESTRICTED_DATA_STATUS = False # True or False, IS THE DATA RESTRICTED?
RESTRICTED_METADATA_STATUS = False # True or False, IS THE METADATA RESTRICTED?
STID_PREFIX = "SNEA" #TODO add the stid prefix

########################################################################################################################
# DEFINE LOGS
########################################################################################################################
logger = logging.getLogger(f"{INGEST_NAME}_ingest")

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


            # Clean name with ascii characters, NOTE that we are NOT converting single apostrophe's to double apostrophe's
            # station_lookup.load_metamgr does this already. Duplicating the apostrophe's is unnecessary
            if name:
                clean_name = core.ascii_sanitize(name) if not name.isascii() else name
            else:
                clean_name = None

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
                    "NAME": clean_name,
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


def save_to_json(data, filename):
    """Save data to a JSON file."""
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w') as f:
        json.dump(data, f, indent=4)


def fetch_singapore_station_metadata(api_key):
    """
    Fetch station metadata from Singapore data.gov.sg API.
    
    This function queries the Singapore API to extract station metadata including
    station IDs, names, and geographic coordinates.
    
    Args:
        api_key (str): API key for Singapore data.gov.sg API
        
    Returns:
        dict: Station metadata in the format:
        {
            "station_id": {
                "SYNOPTIC_STID": str,
                "NAME": str,
                "LAT": float,
                "LON": float,
                "OTID": str,
                "ELEVATION": float or None,
                "RESTRICTED_DATA": bool,
                "RESTRICTED_METADATA": bool
            }
        }
    """
    station_meta = {}
    
    try:
        # Base URL for Singapore data.gov.sg API
        base_url = "https://api-open.data.gov.sg/v2/real-time/api"
        headers = {"X-Api-Key": api_key}
        
        # List of observation endpoints that contain station metadata
        # These endpoints return data with station information embedded
        endpoints = [
            "air-temperature",
            "relative-humidity",
            "wind-speed",
            "wind-direction",
            "rainfall"
        ]
        
        logger.debug(f"Fetching station metadata from {len(endpoints)} endpoints")
        
        # Fetch from each endpoint to collect station information
        for endpoint in endpoints:
            try:
                url = f"{base_url}/{endpoint}"
                logger.debug(f"Fetching metadata from: {url}")
                
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                # Parse station information from the response
                if "data" in data and "stations" in data["data"]:
                    stations = data["data"]["stations"]
                    
                    for station in stations:
                        station_id = station.get("id")
                        
                        if not station_id:
                            logger.debug(f"Skipping station without ID from {endpoint}")
                            continue
                        
                        # Skip if we've already processed this station
                        if station_id in station_meta:
                            logger.debug(f"Station {station_id} already in metadata, skipping")
                            continue
                        
                        try:
                            # Extract station information
                            name = station.get("name", f"Station {station_id}")
                            location = station.get("location", {})
                            lat = location.get("latitude")
                            lon = location.get("longitude")
                            device_id = station.get("deviceId", station_id)
                            
                            # Validate coordinates
                            if lat is None or lon is None:
                                logger.debug(f"Skipping station {station_id}: missing coordinates")
                                continue
                            
                            try:
                                lat = float(lat)
                                lon = float(lon)
                            except (ValueError, TypeError):
                                logger.debug(f"Skipping station {station_id}: invalid coordinate format")
                                continue
                            
                            # Validate coordinate ranges
                            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                                logger.debug(f"Skipping station {station_id}: coordinates out of range ({lat}, {lon})")
                                continue
                            
                            if lat == 0 and lon == 0:
                                logger.debug(f"Skipping station {station_id}: null island coordinates")
                                continue
                            
                            # Create STID (unique station identifier)
                            synoptic_stid = f"{STID_PREFIX}{station_id}"
                            
                            # Build station metadata entry
                            station_meta[station_id] = {
                                "SYNOPTIC_STID": synoptic_stid,
                                "NAME": name,
                                "LAT": lat,
                                "LON": lon,
                                "OTID": device_id,
                                "ELEVATION": None,  # Not provided by Singapore data.gov.sg API
                                "RESTRICTED_DATA": RESTRICTED_DATA_STATUS,
                                "RESTRICTED_METADATA": RESTRICTED_METADATA_STATUS
                            }
                            
                            logger.debug(f"Added station {station_id}: {name} ({lat}, {lon})")
                        
                        except Exception as e:
                            logger.debug(f"Error processing station {station_id}: {e}")
                            continue
                
                else:
                    logger.debug(f"No stations data in {endpoint} response")
            
            except requests.exceptions.RequestException as e:
                logger.warning(f"Failed to fetch from {endpoint}: {e}")
                continue
            except Exception as e:
                logger.warning(f"Error processing {endpoint}: {e}")
                continue
        
        logger.info(f"Successfully fetched metadata for {len(station_meta)} stations")
        return station_meta
    
    except Exception as e:
        logger.error(f"Error fetching station metadata: {e}")
        return {}

########################################################################################################################
# MAIN FUNCTION
########################################################################################################################
def main(event,context):
    from args import args

    # ----- choose dirs once -----
    if args.local_run or args.dev:
        log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../dev"))
        work_dir = "../dev/"
        s3_work_dir = "metadata/"
    else:
        log_dir = "/tmp/tmp/"
        work_dir = "/tmp/tmp/"
        s3_work_dir = "metadata/"

    # ----- logging (stdout + one file) -----
    log_file = core.setup_logging(
        logger, INGEST_NAME,
        log_level=getattr(args, "log_level", "INFO"),
        write_logs=True,
        log_dir=log_dir,
        filename=f"{INGEST_NAME}_meta.log"
    )

    core.setup_signal_handler(logger, args)

    logger.debug(f"ARGS LOADED from: {__file__}")
    logger.debug(f"ENV at load time: DEV={os.getenv('DEV')} LOCAL_RUN={os.getenv('LOCAL_RUN')} LOG_LEVEL={os.getenv('LOG_LEVEL')}")
    logger.debug(vars(args))
    
    start_runtime = time.time()
    try:
        # ========================================================================
        # S3 Configuration
        # ========================================================================
        # Bucket: INTERNAL_BUCKET_NAME = "ingest-singapore" (from environment)
        # S3 Keys:
        #   - Metadata file: metadata/singapore_stations_metadata.json
        #   - Metadata prefix: metadata/
        #   - SQL files: metadata/*.sql (auto-cleaned)
        # ========================================================================
        
        # Declare S3 Paths for Metadata Storage
        s3_bucket_name = os.environ.get('INTERNAL_BUCKET_NAME')
        if not s3_bucket_name:
            raise ValueError("Missing INTERNAL_BUCKET_NAME env var.")

        s3_meta_work_dir = "metadata"
        # S3 Key: metadata/singapore_stations_metadata.json
        s3_station_meta_file = posixpath.join(s3_meta_work_dir, f"{INGEST_NAME}_stations_metadata.json")

        # Declare Local Paths
        work_dir = '/tmp/tmp/'
        os.makedirs(work_dir, exist_ok=True)
        station_meta_file = os.path.join(work_dir, f"{INGEST_NAME}_stations_metadata.json")

        # Load Existing Stations and Payload Files
        existing_stations = {}
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
        secret = aws.SecretsManager.get(secret_name="ingest/singapore")
        try:
            secret_dict = json.loads(secret)  # Parse string to dictionary
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing secret as JSON: {e}")
            raise
        api_key = secret_dict['api_key']
        logger.debug("API key retrieved from AWS Secrets Manager")
        
        # --------------- 2. RAW METADATA COLLECTION (if not collected already by obs lambda) ---------------
        # Fetch initial metadata as raw_meta variable, ideally this is from a metadata specific endpoint, although it's possible this doesn't exist...
        # Fetch station metadata from Singapore data.gov.sg API
        updated_meta = fetch_singapore_station_metadata(api_key=api_key)
        
        if not updated_meta:
            logger.warning("No station metadata fetched from Singapore API")

        # SAVE TO LOCAL DEV (if local_run)
        if args.local_run:
            dev_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../dev'))
            os.makedirs(dev_dir, exist_ok=True)
            
            # Save station_meta to dev directory
            station_meta_dev_path = os.path.join(dev_dir, f'{INGEST_NAME}_station_meta.json')
            with open(station_meta_dev_path, 'w') as f:
                json.dump(updated_meta, f, indent=4)
            
            logger.debug(f"[DEV] Saved station_meta to {station_meta_dev_path}")
            logger.debug(f"[DEV] Station count: {len(updated_meta)}")

        # --------------- 3. METADATA PROCESSING ---------------
        # Process data into a format that can be prepared for station lookup or metamanager payload, store as station_meta. Should be a Dictionary
        station_lookup_payload = generate_metadata_payload(station_meta=updated_meta, payload_type='station_lookup', source_info=None)

        # --------------- 4. STATION LOOKUP PROCESSING ---------------
        # Generate and execute station lookup via ingestlib
        
        if not args.local_run:
            logger.debug('production station lookup proceeding')
            
            try:
                station_lookup.load_metamgr(station_lookup_payload, mode='prod', logstream=logger, output_location=work_dir)
                logger.debug('station lookup completed successfully')
            except Exception as e:
                logger.exception(f"Station lookup failed: {e}")
                raise
            
            # --------------- 5. DATA PERSISTENCE ---------------
            # Save updated metadata
            save_to_json(data=updated_meta, filename=station_meta_file)
            aws.S3.upload_file(local_file_path=station_meta_file, 
                            bucket=s3_bucket_name, 
                            s3_key=s3_station_meta_file)
            logger.info(f"Saved {len(updated_meta)} stations to {s3_station_meta_file}")

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
    except Exception as e:
        total_runtime = time.time() - start_runtime
        logger.exception(f"Unhandled exception: {e}")
        logger.error(msg=json.dumps({'completion': 0, 'time': total_runtime}))
   