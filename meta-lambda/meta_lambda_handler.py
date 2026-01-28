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
STID_PREFIX = "SMI" #TODO add the stid prefix

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
        # Declare S3 Paths for Metadata Storage
        s3_bucket_name = os.environ.get('INTERNAL_BUCKET_NAME')
        if not s3_bucket_name:
            raise ValueError("Missing INTERNAL_BUCKET_NAME env var.")

        s3_meta_work_dir = "metadata"
        s3_station_meta_file = posixpath.join(s3_meta_work_dir, f"{INGEST_NAME}_stations_metadata.json")

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
        # --------------- 2. RAW METADATA COLLECTION (if not collected already by obs lambda) ---------------
        # Fetch initial metadata as raw_meta variable, ideally this is from a metadata specific endpoint, although it's possible this doesn't exist...
        
        # Load raw data from dev folder (cached from obs handler)
        raw_data_file = os.path.join(work_dir, "20260120_081905.json")
        raw_data = {}
        
        if os.path.exists(raw_data_file):
            try:
                with open(raw_data_file, 'r', encoding='utf-8') as f:
                    raw_data = json.load(f)
                logger.info(f"Loaded raw metadata from {raw_data_file}")
            except Exception as e:
                logger.warning(f"Failed to load raw data from {raw_data_file}: {e}")
                raw_data = {}
        else:
            logger.warning(f"Raw data file not found at {raw_data_file}, using empty dataset")
        
        # --------------- 3. METADATA PROCESSING ---------------
        station_meta = {}
        
        # Extract station metadata from raw API data
        if raw_data:
            for dataset_name, dataset_payload in raw_data.items():
                if not dataset_payload or "data" not in dataset_payload:
                    continue
                
                stations = dataset_payload.get("data", {}).get("stations", [])
                
                for station in stations:
                    station_id = station.get("id")
                    if not station_id:
                        continue
                    
                    # Create or update station metadata entry
                    if station_id not in station_meta:
                        location = station.get("location", {})
                        station_meta[station_id] = {
                            "SYNOPTIC_STID": f"{STID_PREFIX}_{station_id}",  # Combine prefix with station ID
                            "NAME": station.get("name", f"Station {station_id}"),
                            "LAT": location.get("latitude"),
                            "LON": location.get("longitude"),
                            "OTID": station.get("deviceId", station_id),
                            "ELEVATION": None,  # Not provided by data.gov.sg
                            "RESTRICTED_DATA": RESTRICTED_DATA_STATUS,
                            "RESTRICTED_METADATA": RESTRICTED_METADATA_STATUS
                        }
        
        logger.info(f"Parsed {len(station_meta)} stations from raw metadata")

        # --------------- 4. STATION LOOKUP PAYLOAD CREATION ---------------
        station_lookup_payload = generate_metadata_payload(station_meta=station_meta, payload_type='station_lookup')

        # SAVE TO LOCAL DEV (if local_run)
        if args.local_run:
            dev_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../dev'))
            os.makedirs(dev_dir, exist_ok=True)

            # Save station_meta locally
            station_meta_dev_path = os.path.join(dev_dir, f'{INGEST_NAME}_stations_metadata.json')
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
                station_lookup.load_metamgr(station_lookup_payload, logstream=logger, mode='prod', output_location=work_dir)
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

        logger.info(msg=json.dumps({'completion': 1, 'time': time.time() - start_runtime}))
        
    except Exception as e:
        logger.error(msg=json.dumps({'completion': 0, 'time': time.time() - start_runtime}))
        raise 
        
    finally:
        total_runtime = time.time() - start_runtime
        logger.info(f"Total execution time: {total_runtime:.2f} seconds")


        # 2) prod only: upload the *exact* file we just wrote
        if (not args.local_run) and (not args.dev) and log_file and s3_bucket_name:
            s3_log_key = posixpath.join(s3_work_dir, f"{INGEST_NAME}_meta.log")
            aws.S3.upload_file(local_file_path=log_file, bucket=s3_bucket_name, s3_key=s3_log_key)
        
        logging.shutdown()
