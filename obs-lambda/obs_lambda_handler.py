import logging
import os
import time
import json
import posixpath
import requests

from datetime import datetime, timedelta, timezone
from collections import defaultdict
from functools import partial

from ingestlib import poe, parse, aws, validator, metamgr, core
from data_dictionary import variables


########################################################################################################################
# DEFINE LOGSTREAMS AND CONSTANTS
########################################################################################################################
INGEST_NAME = "ingest_singapore"
logger = logging.getLogger(f"{INGEST_NAME}_ingest")

DATA_GOV_BASE = "https://api-open.data.gov.sg/v2/real-time/api"


########################################################################################################################
# DATA FETCH
########################################################################################################################

def fetch_singapore_weather(endpoint: str, api_key: str, timeout: int = 30):
    """
    Fetch weather data from Singapore's data.gov.sg API (v2)
    
    Args:
        endpoint: API endpoint to fetch from (e.g., 'air-temperature')
        api_key: API key for authentication
        timeout: Request timeout in seconds
    
    Returns:
        JSON response data or None if failed
    """
    # Get current date in YYYY-MM-DD format for v2 API
    current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url = f"{DATA_GOV_BASE}/{endpoint}?date={current_date}"
    headers = {
        "X-Api-Key": api_key
    }
    logger.info(f"FETCH: {url}")

    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"FETCH FAILED [{endpoint}]: {e}")
        return None


########################################################################################################################
# RAW DATA CACHE
########################################################################################################################

def cache_raw_data_simple(incoming_data, work_dir, s3_bucket_name, s3_prefix):
    try:
        if not incoming_data:
            logger.debug("CACHE: no incoming data; skipping")
            return False

        # Create timestamp-based filename
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')

        # Create S3 path: prefix/YYYY/MM/YYYYMMDD_HHMMSS.json
        year_month = datetime.now(timezone.utc).strftime('%Y/%m')
        s3_key = f"{s3_prefix}/{year_month}/{timestamp}.json"

        # Local file setup
        os.makedirs(work_dir, exist_ok=True)
        local_file_path = os.path.join(work_dir, f"{timestamp}.json")

        logger.info(f"CACHE: target s3://{s3_bucket_name}/{s3_key}")

        # Save data to local file
        try:
            with open(local_file_path, 'w', encoding='utf-8') as f:
                json.dump(incoming_data, f, indent=2, ensure_ascii=False)
            
            file_size = os.path.getsize(local_file_path)
            logger.debug(f"CACHE: created local file {local_file_path} ({file_size} bytes)")
            
        except Exception as e:
            logger.error(f"CACHE: failed to create local file: {e}")
            return False

        # Upload to S3
        t1 = time.time()
        try:
            logger.debug("CACHE: uploading to S3")
            aws.S3.upload_file(local_file_path, s3_bucket_name, s3_key)
            logger.info(f"CACHE: upload OK in {time.time()-t1:.2f}s; size={file_size}B")
            return True
            
        except Exception as e:
            logger.error(f"CACHE: failed to upload in {time.time()-t1:.2f}s: {e}")
            return False

    except Exception as e:
        logger.error(f"CACHE: unexpected error: {e}")
        return False



########################################################################################################################
# PARSING
########################################################################################################################

def parse_weather_data(incoming_data):
    """
    Convert data.gov.sg responses into grouped_obs_set
    Handles v2 API format where data.readings contains array of readings with array of station data
    """
    grouped = defaultdict(list)

    for dataset, payload in incoming_data.items():
        if not payload:
            continue

        readings = payload.get("data", {}).get("readings", [])
        stations = payload.get("data", {}).get("stations", [])
        station_lookup = {s["id"]: s for s in stations}

        for r in readings:
            timestamp = r["timestamp"]
            # v2 API: r["data"] is an array of {stationId, value} objects
            data_array = r.get("data", [])
            for station_data in data_array:
                station_id = station_data.get("stationId")
                value = station_data.get("value")
                if station_id and value is not None:
                    grouped[station_id].append({
                        "dattim": timestamp,
                        "variable": dataset,
                        "value": value,
                        "station_meta": station_lookup.get(station_id, {})
                    })

    return grouped


########################################################################################################################
# MAIN FUNCTION
########################################################################################################################
def main(event, context):
    from args import args

    # --- decide dirs once ---
    if args.local_run or args.dev:
        log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../dev"))
        work_dir = "../dev/"
        s3_work_dir = "dev_tmp/"
    else:
        log_dir = "/tmp/tmp/"
        work_dir = "/tmp/tmp/"
        s3_work_dir = "tmp/"

    # --- logging (stdout + single file; overwrites each run) ---
    log_file = core.setup_logging(
        logger, INGEST_NAME,
        log_level=getattr(args, "log_level", "INFO"),
        write_logs=True,
        log_dir=log_dir,                         # where the file lives (dev or prod)
        filename=f"{INGEST_NAME}_obs.log",       # stable name for S3 overwrite
    )

    # --- signals ---
    core.setup_signal_handler(logger, args)

    logger.debug(f"poe socket: {args.poe_socket_address}")
    logger.debug(f"poe socket port: {args.poe_socket_port}")

    start_runtime = time.time()
    success_flag = 0

    try:
        logger.info("BOOT: ECS logging path OK")

        # paths
        os.makedirs(work_dir, exist_ok=True)
        s3_bucket_name = os.environ["INTERNAL_BUCKET_NAME"]
        cache_s3_bucket_name = os.environ.get("CACHE_S3_BUCKET_NAME", "synoptic-ingest-provider-data-cache-a4fb6")

        s3_meta_work_dir = "metadata"
        s3_station_meta_file = posixpath.join(s3_meta_work_dir, f"{INGEST_NAME}_stations_metadata.json")
        s3_seen_obs_file   = posixpath.join(s3_work_dir, "seen_obs.txt")
        seen_obs_file      = os.path.join(work_dir, "seen_obs.txt")
        station_meta_file  = os.path.join(work_dir, f"{INGEST_NAME}_stations_metadata.json")

        # Download seen observations file
        try:
            aws.S3.download_file(bucket=s3_bucket_name, object_key=s3_seen_obs_file, local_directory=work_dir)
        except Exception as e:
            logger.warning(f"Warning: Failed to download {s3_seen_obs_file}. Error: {e}")

        # Download station metadata file
        try:
            aws.S3.download_file(bucket=s3_bucket_name, object_key=s3_station_meta_file, local_directory=work_dir)
        except Exception as e:
            logger.warning(f"Warning: Failed to download {s3_station_meta_file}. Error: {e}")

        # Determine the time before which data will not be archived between script runs to identify new data
        PREVIOUS_HOURS_TO_RETAIN = 12
        # Look back for recent data
        data_archive_time = datetime.now(timezone.utc) - timedelta(hours=PREVIOUS_HOURS_TO_RETAIN)

        ####################################################################################################
        # GET API KEY FROM SECRETS MANAGER
        ####################################################################################################
        
        # Retrieve API key from AWS Secrets Manager
        logger.info("Retrieving API key from Secrets Manager...")
        try:
            secret = aws.SecretsManager.get(secret_name="ingest/singapore")
            try:
                secret_dict = json.loads(secret)  # Parse string to dictionary
                api_key = secret_dict.get('api_key')
                if not api_key:
                    logger.error("API key not found in secret")
                    raise ValueError("Missing 'api_key' in secret")
                logger.info("API key retrieved successfully")
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing secret as JSON: {e}")
                raise
        except Exception as e:
            logger.error(f"Failed to retrieve API key: {e}")
            # For local development, fallback to environment variable or hardcoded value
            if args.local_run or args.dev:
                api_key = os.environ.get("SINGAPORE_API_KEY", "v2:d3b930df370d50cc30e866bfd3453950686ed461beb376f7ee9960f06d6bffda:7Oplg4nROHkwafTPraU-IrogfLDdMcf7")
                logger.warning(f"Using fallback API key for local/dev run")
            else:
                raise
        
        # Strip v2: prefix if present (AWS Secrets Manager format)
        if api_key.startswith("v2:"):
            api_key = api_key[3:]
            logger.debug("Stripped v2: prefix from API key")

        ####################################################################################################
        # GET LATEST OBS
        ####################################################################################################

        # load station metadata file
        if os.path.exists(station_meta_file):
            station_meta = parse.load_json_file(file_path=station_meta_file)
        else:
            station_meta = {}

        

        logger.info("FETCH: starting data fetch request")

        incoming_data = {
            "air_temperature": fetch_singapore_weather("air-temperature", api_key),
            "rainfall": fetch_singapore_weather("rainfall", api_key),
            "relative_humidity": fetch_singapore_weather("relative-humidity", api_key),
            "wind_speed": fetch_singapore_weather("wind-speed", api_key),
            "wind_direction": fetch_singapore_weather("wind-direction", api_key)
        }

        # drop failed calls
        incoming_data = {k: v for k, v in incoming_data.items() if v}

        logger.info(f"FETCH: got data? {bool(incoming_data)}")
        
        # store raw raw incoming data in the data provider raw cache bucket
        cache_raw_data_simple(
            incoming_data=incoming_data, 
            work_dir=work_dir, 
            s3_bucket_name=cache_s3_bucket_name, 
            s3_prefix=INGEST_NAME
        )

        
        if incoming_data:
            logger.info(msg=json.dumps({'Incoming_Data_Success': 1}))

            # Write parsing function here!
            grouped_obs_set = parse_weather_data(incoming_data)
            # Format: station_id|unix_timestamp|{"variable": value, ...}
            grouped_obs = []
            for station_id, obs_list in grouped_obs_set.items():
                # Group observations by timestamp
                by_timestamp = defaultdict(dict)
                for obs in obs_list:
                    timestamp_str = obs["dattim"]
                    variable = obs["variable"]
                    value = obs["value"]
                    by_timestamp[timestamp_str][variable] = value
                
                # Create observation strings: station_id|YYYYMMDDHHMM|{data}
                for timestamp_str, data in by_timestamp.items():
                    # Convert ISO 8601 to YYYYMMDDHHMM format for validator
                    # Singapore API returns timestamps in SGT (UTC+8)
                    try:
                        # Parse the timestamp - Singapore API returns SGT (UTC+8)
                        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+08:00"))
                        # Convert to UTC
                        dt_utc = dt.astimezone(timezone.utc)
                        # Format as YYYYMMDDHHMM (12 digits) in UTC
                        dattim_str = dt_utc.strftime("%Y%m%d%H%M")
                    except Exception as e:
                        logger.warning(f"Failed to parse timestamp {timestamp_str}: {e}")
                        # Fallback to current time if parsing fails
                        dattim_str = datetime.now(timezone.utc).strftime("%Y%m%d%H%M")
                    
                    obs_str = "|".join([station_id, dattim_str, json.dumps(data)]).replace(" ", "")
                    grouped_obs.append(obs_str)
            ####################################################################################################
            # VALIDATE DATA
            ####################################################################################################
            # save the grouped obs and station meta if it exists
            if args.local_run or args.dev:
                dev_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../dev'))
                os.makedirs(dev_dir, exist_ok=True)

                # Save grouped_obs as a text file (one line per observation string)
                grouped_obs_path = os.path.join(dev_dir, 'grouped_obs.txt')
                with open(grouped_obs_path, 'w') as f:
                    for obs in grouped_obs:
                        f.write(obs + '\n')
                logger.debug(f"[DEV] Saved grouped_obs to {grouped_obs_path}")

                # Save station_meta if available
                if 'station_meta' in locals():
                    station_meta_path = os.path.join(dev_dir, 'station_meta.json')
                    with open(station_meta_path, 'w') as f:
                        json.dump(station_meta, f, indent=4)
                    logger.debug(f"[DEV] Saved station_meta to {station_meta_path}")
            
            if args.dev or args.local_run:
                # Time window: allow observations from the entire current day + next day
                # API returns observations throughout the day, so we need to be permissive
                now = datetime.now(timezone.utc)
                # Set end_time to end of next day to allow all current day observations
                end_time = (now + timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999)
                # Set start_time to 48 hours ago to cover current and previous day
                start_time = now - timedelta(hours=48)

                # Try to fetch variables_table unless local_run
                variables_table = {}
                if not args.local_run:
                    try:
                        variables_table = metamgr.grab_variables_table(
                            socket_address=args.metamgr_socket_address,
                            socket_port=args.metamgr_socket_port
                        )
                    except Exception as e:
                        logger.warning(f"[VALIDATION] Skipping variable-table-based checks: {e}")

                # Variable validations
                variable_validators = [
                    validator.validate_vargem_vnums,
                    validator.validate_statistic_context_vnum,
                    validator.validate_required_variable_fields,
                    validator.validate_overlapping_variable_names,
                ]

                if variables_table:
                    variable_validators.append(partial(validator.validate_variables, variables_table=variables_table))
                    variable_validators[-1].__name__ = "validate_variables"


                # Observation validations
                obs_validators = [
                    lambda obs: validator.validate_dattim(obs, start_time, end_time)
                ]
                if variables_table:
                    obs_validators.append(partial(validator.validate_observation_ranges, variables_table=variables_table))
                    obs_validators[-1].__name__ = "validate_observation_ranges"

                # Run validations
                all_validation_messages = []
                for vfunc in variable_validators:
                    for m in vfunc(variables):
                        all_validation_messages.append((vfunc.__name__, m))  # tag with func name
                for ofunc in obs_validators:
                    for m in ofunc(grouped_obs):
                        all_validation_messages.append((ofunc.__name__, m))

                if all_validation_messages:
                    grouped = defaultdict(list)
                    for name, msg in all_validation_messages:
                        grouped[name].append(msg)
                    for func_name, msgs in grouped.items():
                        logger.debug(f"[{func_name}] {len(msgs)} occurrences")
                        for m in msgs:
                            logger.debug(f"[{func_name}] {m}")
                else:
                    logger.debug(":: PASSED :: All variable and observation validations clean.")


            ########################################################################################################################
            # DIFF AGAINST DATA CACHE AND SEND TO POE
            ########################################################################################################################
            # Load the cache of recent data
            if os.path.exists(seen_obs_file):
                # open seen_obs file, grab last timestamp sent to POE
                with open(seen_obs_file, 'r') as old_row_file:
                    old_rows = [i.strip() for i in old_row_file.readlines()]
            else:
                old_rows = []
            
            ###### Submit Data to POE in chunks ######
            for chunk in poe.chunk_list(grouped_obs, chunk_size=int(args.poe_chunk_size)):
                # Process each chunk
                io, seen_obs = poe.poe_formatter(chunk, old_rows)
                # Check if there's data to insert
                if io is None:
                    logger.debug("io is empty")
                elif args.local_run:
                    logger.debug("Local Run, therefore NOT sending to any POE")
                else:
                    poe.poe_insertion(io, args)
                    time.sleep(2)

            # Run POE formatter again, but this time we save io.txt and seen_obs locally, and do NOT send to POE
            # this is just more efficient than appending seen_obs above
            io, seen_obs = poe.poe_formatter(grouped_obs, old_rows)
            logger.debug(io)
            # Remove rows older than the archive limit
            seen_obs = poe.seen_obs_formatter(seen_obs, data_archive_time)
            

            ########################################################################################################################
            # UPLOAD TO S3
            ########################################################################################################################
            if not args.local_run:
                # Write an archive file of seen_obs to check for duplicate records in the next run
                with open(seen_obs_file, 'w+') as file:
                    for ob in seen_obs:
                        file.write(ob + '\n')

                #TODO do we save metadata file here? Only if this is the script where we populate the json data
                #with open(station_meta_file, 'w+') as file:
                    #json.dump(station_meta, file, indent=4)
                
                aws.S3.upload_file(local_file_path=seen_obs_file, bucket=s3_bucket_name ,s3_key=s3_seen_obs_file)
                #aws.S3.upload_file(local_file_path=station_meta_file, bucket=s3_bucket_name ,s3_key=s3_station_meta_file)

            success_flag = 1
        else:
            logger.error(msg=json.dumps({'Incoming_Data_Success': 0}))

    except Exception as e:
        logger.exception(e)

    finally:
        total_runtime = time.time() - start_runtime
        logger.info(msg=json.dumps({'completion': success_flag, 'time': total_runtime}))

        # Overwrite the same S3 object each run in prod
        if not (args.local_run or args.dev) and log_file:
            try:
                s3_log_key = posixpath.join(s3_work_dir, f"{INGEST_NAME}_obs.log")
                aws.S3.upload_file(local_file_path=log_file, bucket=s3_bucket_name, s3_key=s3_log_key)
            except Exception as e:
                logger.warning(f"Failed to upload run log to S3: {e}")

        logging.shutdown()
