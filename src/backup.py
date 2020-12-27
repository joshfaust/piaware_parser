import os
import json
import csv
import hashlib
import logging
import requests
import time
import random
import configparser
import gzip
import argparse
import src.adsb_exchange as ads
import src.aws as aws
from datetime import datetime as dt
from datetime import date, timedelta

logname = f"aircraft_{date.today()}.log"
logging.basicConfig(filename=logname, level=logging.INFO)

#GLOBAL
START_TIME = dt.now()
SEEN_AIRCRAFT = set()

def write_to_gzip_file(filename: str, data: str) -> None:
    """
    Takes a filename and the data to write a gzip stream to.
    """
    data_bytes = (data + "\n").encode("utf-8")
    with gzip.open(filename, "ab") as f:
        f.write(data_bytes)


def write_to_s3(filename: str, bucket_name: str) -> None:
    """
    Write a file to AWS S3
    """
    if not aws.check_bucket_exists(bucket_name):
        if not aws.create_bucket(bucket_name):
            logging.error(f"Unable to Create bucket")
            exit(1)
    if not aws.upload_to_s3(bucket_name, filename, filename):
        logging.error(f"Unable to upload gzipped file to S3")


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


def build_identifier(icao: str, ident: str) -> str:
    """
    Create an MD5 hash from flight icao and ident.
    """
    hash = hashlib.md5(f"{icao};{ident}".encode("utf-8"))
    return hash.hexdigest()


def get_time_delta() -> float:
    """
    Calculates the time difference from when the program
    started to the current time
    """
    global START_TIME
    time_delta = dt.now() - START_TIME
    time_delta_hours = divmod(time_delta.total_seconds(), 3600)[0]
    return time_delta_hours


def get_file_hash(file_path: str) -> str:
    """
    Obtains a filehash given a file path.
    """
    try:
        if not os.path.isfile(file_path):
            raise Exception(f"{file_path} does not exist!")
        sha256_hash = hashlib.sha256()

        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256_hash.update(chunk)

        file_hash = sha256_hash.hexdigest()

        return file_hash

    except Exception as e:
        logging.error(f"[{dt.now()}]-HASING_ERROR: {e}")


def get_api_aircraft_data(icao: str) -> dict:
    """
    Given an aircraft ident, uses the FLightAware V3 API
    to enrich the data by adding the airline, flightnumber,
    and the tailnumber for the flight.
    """
    try:
        flight_api_data = ads.get_aircraft_by_icao(icao)
        flight_api_data_json = flight_api_data["ac"][0]

        enriched_flight_data = {
            "flightnumber": None,
            "registration": None,
            "aircraft_type": None,
            "military": None,
            "country": None
        }

        ads_keys = ["call", "reg", "type", "mil", "cou"]
        enr_keys = list(enriched_flight_data.keys())
        for i, key in enumerate(enr_keys):
            try:
                enriched_flight_data[key] = flight_api_data_json[ads_keys[i]]
            except KeyError as e:
                logging.error(f"[{dt.now()}]-API_KEY_ERROR: {e}")
                enriched_flight_data[key] = None

        return enriched_flight_data

    except json.decoder.JSONDecodeError as e:
        logging.error(
            f"[{dt.now()}]-JSON_DECODE_ERROR:{e}"
        )
        return flight_data
    except TypeError as e:
        logging.error(f"[{dt.now()}]-TYPE_ERROR:{e}")
        return flight_data


def get_local_aircraft_data(aircraft_file_path: str, using_ads_api: bool, using_aws_api: bool) -> None:
    """
    Takes an boolean value that dictates whether we should attempt
    to enrich our data. Extracts know values from the json stream that
    exists on the Raspberry Pi host file "aircraft.json" and writes
    the cleaned up data to a GZIPPED CSV file.
    """
    try:
        output_filename = f"aircraft_{date.today()}.json.gz"

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
            "ground_speed": None,
            "track": None,
            "lat": None,
            "lon": None,
        }

        local_flight_data_keys = ["icao", "ident", "alt_baro", "alt_geom", "groud_speed", "track", "lat", "lon"]
        local_aircraft_conf_keys = ["hex", "flight", "alt_baro", "alt_geom", "gs", "track", "lat", "lon"]
        flight_epoch = json_data["now"]
        for flight in json_data["aircraft"]:

            # load the cleaned values into the local dict
            for i, local_key in enumerate(local_flight_data_keys):
                try:
                    local_flight_data[local_key] = str(flight[local_aircraft_conf_keys[i]]).strip()
                except KeyError as e:
                    logging.error(f"[{dt.now()}]-LOCAL_KEY_ERROR: {e}")
                    local_flight_data[local_key] = None

            flight_unique_identifier = build_identifier(local_flight_data["icao"], local_flight_data["ident"])
            duplicate_check = check_if_duplicate(flight_unique_identifier)

            if not duplicate_check:
            # ADSBExchange API       
                if using_ads_api:
                    """
                    Check if we've seen this aircraft/flight before, if we have skip the API query
                    as those API calls cost $$. 
                    """
                    enriched_flight_data = get_api_aircraft_data(local_flight_data["icao"])
                    local_flight_data.update(enriched_flight_data)

                write_to_gzip_file(output_filename, str(local_flight_data))

                if using_aws_api:
                    write_to_s3(output_filename, "local-aircraft-data")

    except KeyError as e:
        logging.error(f"[{dt.now()}]-KEY_ERROR:{e}")
    except TypeError as e:
        logging.error(f"[{dt.now()}]-TYPE_ERROR:{e}")
