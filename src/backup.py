import os
import json
import logging
import time
import src.adsb_exchange as ads
import src.utilities as utils

from datetime import datetime as dt
from datetime import date, timedelta

# GLOBAL
LOGNAME = f"aircraft_{date.today()}.log"
logging.basicConfig(filename=LOGNAME, level=logging.INFO)
START_TIME = dt.now()
SEEN_AIRCRAFT = set()


def check_if_duplicate(identifier: str) -> None:
    """
    Check to see if we've seen this aircraft before. 
    """
    global SEEN_AIRCRAFT
    if identifier in SEEN_AIRCRAFT:
        return True
    else: 
        SEEN_AIRCRAFT.add(identifier)
        return False


def reset_seen_aircraft(current_flight_identifiers: list) -> bool:
    """
    Given a list of current flight identifiers, sanitize SEEN_AIRCRAFT
    and reload with the current aircraft identifiers we've just seen
    in order to mitigate duplication. 
    """
    try:
        global START_TIME, SEEN_AIRCRAFT
        START_TIME = dt.now()
        SEEN_AIRCRAFT = set()
        
        for ident in current_flight_identifiers:
            SEEN_AIRCRAFT.add(ident)

        return True
    except Exception as e:
        logging.error(e)
        return False


def get_time_delta() -> float:
    """
    Calculates the time difference from when the program
    started to the current time
    """
    global START_TIME
    time_delta = dt.now() - START_TIME
    time_delta_hours = divmod(time_delta.total_seconds(), 3600)[0]
    return time_delta_hours


def get_local_aircraft_data(aircraft_file_path: str, using_ads_api: bool, using_aws_api: bool) -> None:
    """
    Takes an boolean value that dictates whether we should attempt
    to enrich our data. Extracts know values from the json stream that
    exists on the Raspberry Pi host file "aircraft.json" and writes
    the cleaned up data to a GZIPPED CSV file.
    """
    try:
        output_filename = f"aircraft_{date.today()}.json.gz"
        current_flight_identifiers = [] # Holds most recent flight identifiers for deduplication

        # Pull in the aircrafts.json file
        with open(aircraft_file_path, "r") as aircraft:
            file_contents = aircraft.read()
            json_data = json.loads(file_contents)

        local_flight_data = {
            "epoch": None,
            "icao": None,
            "ident": None,
            "alt_baro": None,
            "alt_geom": None,
            "track": None,
            "lat": None,
            "lon": None,
        }

        local_flight_data_keys = ["icao", "ident", "alt_baro", "alt_geom", "track", "lat", "lon"]
        local_aircraft_conf_keys = ["hex", "flight", "alt_baro", "alt_geom", "track", "lat", "lon"]
        local_flight_data["epoch"] = json_data["now"]
        for flight in json_data["aircraft"]:

            # load the cleaned values into the local dict
            for i, local_key in enumerate(local_flight_data_keys):
                try:
                    local_flight_data[local_key] = str(flight[local_aircraft_conf_keys[i]]).strip()
                except KeyError as e:
                    local_flight_data[local_key] = None

            flight_unique_identifier = utils.get_string_md5(f"{local_flight_data['icao']};{str(date.today())}")
            current_flight_identifiers.append(flight_unique_identifier)
            duplicate_check = check_if_duplicate(flight_unique_identifier)

            if not duplicate_check:
            # ADSBExchange API       
                if using_ads_api:
                    """
                    Check if we've seen this aircraft/flight before, if we have skip the API query
                    as those API calls cost $$. 
                    """
                    enriched_flight_data = ads.get_aircraft_by_icao(local_flight_data["icao"])

                    """
                    Empty dictionaries evaluate as False and in this case, an empty 
                    dict represents a HTTP error or we lost internet connection. We
                    need to re-verify we have internet connection and and pause until 
                    we have re-established a connection.
                    """
                    if not bool(enriched_flight_data):
                        logging.error("Starting Internet Connections Checks.")
                        while not utils.check_internet_connection():
                            time.sleep(5)
                        enriched_flight_data = ads.get_aircraft_by_icao(local_flight_data["icao"])

                    local_flight_data.update(enriched_flight_data)

                utils.write_to_gzip_file(output_filename, str(local_flight_data))

                if using_aws_api:
                    utils.write_to_s3(output_filename, "local-aircraft-data")

        # Check how long program has been running and reset SEEN_AIRCRAFT if needed
        runtime = get_time_delta()
        if runtime >= 23.0:
            if reset_seen_aircraft(current_flight_identifiers):
                logging.info(f"RUNTIME: {runtime}-Resetting SEEN_AIRCRAFT and START_TIME.")

    except KeyError as e:
        logging.error(f"[{dt.now()}]-KEY_ERROR:{e}")
    except TypeError as e:
        logging.error(f"[{dt.now()}]-TYPE_ERROR:{e}")
