import logging
from datetime import datetime, timezone, timedelta
from ingestlib import poe, parse, aws, validator, metamgr
from data_dictionary import variables
import os
import time
import json
import posixpath
from collections import defaultdict
########################################################################################################################
# DEFINE LOGSTREAMS AND CONSTANTS
########################################################################################################################
INGEST_NAME = 'INGEST_NAME' #TODO Update Ingest Name
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

def cache_raw_data(incoming_data, work_dir: str, s3_bucket_name: str, s3_prefix: str, fallback_date: str = None):
    """
    ------------------------------
    PLEASE NOTE THIS MIGHT NOT WORK FOR EVERY INGEST. THIS IS A GENERIC BOILER PLATE RAW CACHE DATA UPLOAD 
    BUT IT IS LIKELY NOT A CATCH ALL. YOU MAY NEED TO CHANGE THIS, ESPECIALLY WITH RESPECT TO TIMESTAMPS!!!
    ALSO PLEASE KEEP THIS RAW RAW RAW DATA UNTOUCHED. DON'T EDIT IT AT ALL!!!
    ------------------------------
    
    Caches raw incoming data to S3, attempting to group by date if timestamps are found,
    otherwise using current date or fallback_date. Preserves original data structure.
    Does NOT modify or convert timestamps - uses them only for date grouping.
    
    Args:
        incoming_data: Raw incoming data (any structure - dict, list, etc.)
        work_dir (str): Local directory for staging files (e.g., /tmp/).
        s3_bucket_name (str): Target S3 bucket name.
        s3_prefix (str): Prefix path in S3 (e.g., "raw_data_cache").
        fallback_date (str): Optional date string (YYYY-MM-DD) to use if no timestamps found.
    
    Returns:
        bool: True if successful, False if failed
    """
    try:
        if not incoming_data:
            logger.debug("No incoming data to cache")
            return False

        def extract_date_from_timestamp_formats(value):
            """Extract YYYY-MM-DD date string from various timestamp formats WITHOUT conversion"""
            if not isinstance(value, (str, int, float)):
                return None
                
            # Convert to string for parsing
            str_value = str(value).strip()
            
            try:
                # Format: YYYYMMDDHHMMSS (14 digits) -> extract YYYY-MM-DD
                if str_value.isdigit() and len(str_value) == 14:
                    return f"{str_value[0:4]}-{str_value[4:6]}-{str_value[6:8]}"
                
                # Format: YYYYMMDDHHMM (12 digits) -> extract YYYY-MM-DD
                elif str_value.isdigit() and len(str_value) == 12:
                    return f"{str_value[0:4]}-{str_value[4:6]}-{str_value[6:8]}"
                
                # Format: YYYYMMDD (8 digits) -> extract YYYY-MM-DD
                elif str_value.isdigit() and len(str_value) == 8:
                    return f"{str_value[0:4]}-{str_value[4:6]}-{str_value[6:8]}"
                
                # Unix timestamp - convert ONLY for date extraction, don't modify original
                elif str_value.isdigit():
                    ts_value = float(str_value)
                    
                    # Unix timestamp in seconds (9-10 digits, roughly 2001-2033)
                    if 9 <= len(str_value) <= 10 and 1e9 <= ts_value <= 2e9:
                        dt = datetime.utcfromtimestamp(ts_value)
                        return dt.strftime('%Y-%m-%d')
                    
                    # Unix timestamp in milliseconds (13 digits)
                    elif len(str_value) == 13 and 1e12 <= ts_value <= 2e12:
                        dt = datetime.utcfromtimestamp(ts_value / 1000)
                        return dt.strftime('%Y-%m-%d')
                
                # Already a date string format
                elif '-' in str_value and len(str_value) >= 10:
                    # Extract just the date part from formats like "2024-12-15T14:30:22" or "2024-12-15 14:30:22"
                    date_part = str_value[:10]
                    if len(date_part) == 10 and date_part.count('-') == 2:
                        # Validate it's a proper date format
                        year, month, day = date_part.split('-')
                        if (len(year) == 4 and year.isdigit() and 
                            len(month) == 2 and month.isdigit() and 
                            len(day) == 2 and day.isdigit()):
                            return date_part
                            
            except (ValueError, TypeError, IndexError):
                pass
                
            return None

        def find_date_strings_recursive(obj, path=""):
            """Recursively search for timestamp-like values and extract date strings"""
            date_strings = []
            
            if isinstance(obj, dict):
                for key, value in obj.items():
                    current_path = f"{path}.{key}" if path else key
                    
                    # Check if this looks like a timestamp field by name
                    if any(ts_key in key.lower() for ts_key in ['timestamp', 'time', 'date', 'datetime', 'ts']):
                        extracted_date = extract_date_from_timestamp_formats(value)
                        if extracted_date:
                            date_strings.append(extracted_date)
                    else:
                        # Check if the value itself looks like a timestamp
                        extracted_date = extract_date_from_timestamp_formats(value)
                        if extracted_date:
                            date_strings.append(extracted_date)
                    
                    # Recurse into nested structures
                    date_strings.extend(find_date_strings_recursive(value, current_path))
                    
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    date_strings.extend(find_date_strings_recursive(item, f"{path}[{i}]"))
            else:
                # Check if this individual value is a timestamp
                extracted_date = extract_date_from_timestamp_formats(obj)
                if extracted_date:
                    date_strings.append(extracted_date)
                    
            return date_strings

        # Try to find timestamps and group by date
        date_strings = find_date_strings_recursive(incoming_data)
        
        if date_strings:
            # Use the most recent date found (lexicographically latest for YYYY-MM-DD format)
            date_str = max(date_strings)
            logger.debug(f"Found {len(date_strings)} date references, using latest: {date_str}")
        elif fallback_date:
            date_str = fallback_date
            logger.debug(f"No timestamps found, using fallback date: {date_str}")
        else:
            # Fall back to current date
            date_str = datetime.utcnow().strftime('%Y-%m-%d')
            logger.debug(f"No timestamps found, using current date: {date_str}")
        
        # Create a timestamped entry with metadata for future parsing
        cache_entry = {
            'cached_at': datetime.utcnow().isoformat(),
            'data_source': s3_prefix,
            'found_dates': len(date_strings),
            'date_used': date_str,
            'raw_data': incoming_data  # Preserve original structure completely - NO MODIFICATIONS
        }
        
        year_str, month_str, _ = date_str.split("-")
        s3_key = f"{s3_prefix}/{year_str}/{month_str}/{date_str}.json"
        local_file_path = os.path.join(work_dir, f"{date_str}.json")

        # Ensure work directory exists
        os.makedirs(work_dir, exist_ok=True)

        # Load existing file from S3 if present
        existing_data = []
        try:
            aws.S3.download_file(
                bucket=s3_bucket_name,
                object_key=s3_key,
                local_directory=work_dir
            )
            if os.path.exists(local_file_path):
                with open(local_file_path, 'r') as f:
                    existing_data = json.load(f)
                    # Ensure it's a list for appending
                    if not isinstance(existing_data, list):
                        existing_data = [existing_data] if existing_data else []
        except Exception as e:
            logger.warning(f"Could not load existing file for {date_str}: {e}")

        # Append new data (don't overwrite, preserve all raw ingests)
        existing_data.append(cache_entry)

        # Save locally
        try:
            with open(local_file_path, 'w') as f:
                json.dump(existing_data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save local file {local_file_path}: {e}")
            return False

        # Upload to S3
        try:
            aws.S3.upload_file(local_file_path, s3_bucket_name, s3_key)
            logger.debug(f"Cached raw data for {date_str}: {len(existing_data)} total entries.")
            return True
        except Exception as e:
            logger.error(f"Failed to upload to S3 {s3_key}: {e}")
            return False
            
    except Exception as e:
        logger.error(f"cache_raw_data failed with unexpected error: {e}")
        return False

########################################################################################################################
# MAIN FUNCTION
########################################################################################################################
def main(event, context):
    from args import args

    # set up local logging to save in ../dev folder
    setup_logging()
    logger.debug(f"poe socket: {args.poe_socket_address}")
    logger.debug(f"poe socket port: {args.poe_socket_port}")
    # start time
    start_runtime = time.time()

    # poe chunk size arg integer
    poe_chunk_size = int(args.poe_chunk_size)

    try:
        if args.dev:
            work_dir = "/tmp/dev_tmp/"
            s3_work_dir = "dev_tmp/"
        else:
            work_dir = "/tmp/tmp/"
            s3_work_dir = "tmp/"
            
        # Declare Paths for Metadata Storage
        s3_bucket_name = os.environ.get("INTERNAL_BUCKET_NAME")
        if not s3_bucket_name:
            raise ValueError("Missing required environment variable: INTERNAL_BUCKET_NAME")
        s3_meta_work_dir = "metadata"
        s3_station_meta_file = posixpath.join(s3_meta_work_dir, f"{INGEST_NAME}_stations_metadata.json")
        s3_seen_obs_file = posixpath.join(s3_work_dir, "seen_obs.txt")
        
        # define local filepaths
        os.makedirs(work_dir, exist_ok=True)
        seen_obs_file = os.path.join(work_dir, "seen_obs.txt")
        station_meta_file = os.path.join(work_dir, f"{INGEST_NAME}_stations_metadata.json")

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
        data_archive_time = datetime.utcnow() - timedelta(0, 60 * 60 * PREVIOUS_HOURS_TO_RETAIN)
        
        ########################################################################################################################
        # GET LATEST OBS
        ########################################################################################################################

        # load station metadata file
        if os.path.exists(station_meta_file):
            station_meta = parse.load_json_file(file_path=station_meta_file)
        else:
            station_meta = {}

        # grab incoming/latest data, either through API, S3, http, etc. 
        incoming_data = []

        

        # store raw raw incoming data in the data provider raw cache bucket
        #TODO PLEASE CHECK THIS FUNCTION IS HANDLING YOUR TIMESTAMPS EFFECTIVELY
        cache_raw_data(incoming_data=incoming_data, work_dir=work_dir, s3_bucket_name=s3_bucket_name, s3_prefix=INGEST_NAME)
        
        if incoming_data:
            logger.info(msg=json.dumps({'Incoming_Data_Success': 1}))

            #TODO Write parsing function here!
            # grouped_obs_set, station_meta = TODO use parsing function here
            grouped_obs = ['|'.join([k, json.dumps(v)]).replace(' ', '') for k, v in grouped_obs_set.items()] 

            ########################################################################################################################
            # VALIDATE DATA
            ########################################################################################################################
            # save the grouped obs and station meta if it exists
            if args.local_run:
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
                # Time window: last 24 hours
                end_time = datetime.utcnow()
                start_time = end_time - timedelta(hours=24)

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
                    variable_validators.append(lambda v: validator.validate_variables(v, variables_table))

                # Observation validations
                obs_validators = [
                    lambda obs: validator.validate_dattim(obs, start_time, end_time),
                    validator.validate_station_id_length,
                ]
                if variables_table:
                    obs_validators.append(lambda obs: validator.validate_observation_ranges(obs, variables_table))

                # Run validations
                all_validation_messages = []
                for vfunc in variable_validators:
                    all_validation_messages.extend(vfunc(variables))
                for ofunc in obs_validators:
                    all_validation_messages.extend(ofunc(grouped_obs))

                if all_validation_messages:
                    grouped = defaultdict(list)
                    for msg in all_validation_messages:
                        grouped[msg.split(":")[0]].append(msg)
                    for category, msgs in grouped.items():
                        logger.debug(f"{category}: {len(msgs)} occurrences")
                        for m in msgs:
                            logger.debug(m)
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
            for chunk in poe.chunk_list(grouped_obs, chunk_size=poe_chunk_size):
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

                # save metadata file
                with open(station_meta_file, 'w+') as file:
                    json.dump(station_meta, file, indent=4)
                
                aws.S3.upload_file(local_file_path=seen_obs_file, bucket=s3_bucket_name ,s3_key=s3_seen_obs_file)
                aws.S3.upload_file(local_file_path=station_meta_file, bucket=s3_bucket_name ,s3_key=s3_station_meta_file)

            total_runtime = time.time() - start_runtime
            logger.info(msg=json.dumps({'completion': 1, 'time': total_runtime}))
        else:
            logger.error(msg=json.dumps({'Incoming_Data_Success': 0}))
    except Exception as e:
        message = e
        logger.exception(message)
        logger.error(msg=json.dumps({'completion': 0, 'time': total_runtime}))