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
from datetime import datetime as dt
from datetime import date

logname = f"aircraft_{date.today()}.log"
logging.basicConfig(filename=logname, level=logging.INFO)


def write_to_gzip_file(filename: str, data: str) -> None:
    """
    Takes a filename and the data to write a gzip stream to.
    """
    data_bytes = (data + "\n").encode("utf-8")
    with gzip.open(filename, "ab") as f:
        f.write(data_bytes)


def get_flight_aware_credentials() -> tuple:
    """
    Extracts the Flightaware V3 API credentials from the flightaware
    configuration file.
    """
    config = configparser.ConfigParser()
    config.read("flightaware.conf")
    flightaware_user = config.get("flightaware", "user")
    flightaware_key = config.get("flightaware", "key")
    return (flightaware_user, flightaware_key)


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
        logging.error(f"[{dt.now()}] - HASING_ERROR: {e}")


def get_additional_flight_info(ident: str) -> dict:
    """
    Given an aircraft ident, uses the FLightAware V3 API
    to enrich the data by adding the airline, flightnumber,
    and the tailnumber for the flight.
    """
    try:
        ident = ident.strip().replace(" ", "")
        flight_data = {"airline": None, "flightnumber": None, "tailnumber": None}

        # Query the Flight Aware V3 API:
        flightaware_uri = "https://flightxml.flightaware.com/json/FlightXML3/"

        payload = {"ident": ident, "howMany": 1}
        r = requests.get(
            flightaware_uri + "FlightInfoStatus",
            params=payload,
            auth=get_flight_aware_credentials(),
        )
        r_json = r.json()
        logging.info(f"[{dt.now()}]:Flight_Data_Enrichment:HTTP_Status:{r.status_code}")

        flight_keys = ["airline", "flightnumber", "tailnumber"]
        if "error" not in r_json:
            flight = r_json["FlightInfoStatusResult"]["flights"][0]

            for key in flight_keys:
                try:
                    flight_data[key] = str(flight[key]).strip().replace(" ", "")
                except:
                    flight_data[key] = None

        return flight_data

    except json.decoder.JSONDecodeError as e:
        logging.error(
            f"[{dt.now()}]-JSON_DECODE_ERROR:{e}-JSON_STR:{str(r.text).strip()}"
        )
        return flight_data
    except KeyError as e:
        logging.error(f"[{dt.now()}]-KEY_ERROR:{e}")
        return flight_data
    except TypeError as e:
        logging.error(f"[{dt.now()}]-TYPE_ERROR:{e}")
        return flight_data


def backup_new_aircraft_data(aircraft_file_path: str, has_api: bool) -> None:
    """
    Takes an boolean value that dictates whether we should attempt
    to enrich our data. Extracts know values from the json stream that
    exists on the Raspberry Pi host file "aircraft.json" and writes
    the cleaned up data to a GZIPPED CSV file.
    """
    try:
        output_filename = f"aircraft_{date.today()}.csv.gz"
        # Pull in the aircrafts.json file
        with open(aircraft_file_path, "r") as aircraft:
            file_contents = aircraft.read()
            json_data = json.loads(file_contents)

        # Start building the CSV objects
        csv_fields = [
            "epoch",
            "hex",
            "ident",
            "flight_number",
            "tailnumber",
            "airline",
            "alt_baro",
            "alt_geom",
            "ground_speed",
            "heading",
            "lat",
            "lon",
        ]

        # Check if the GZ file exists, if so, we don't want to re-write the csv header.
        if not os.path.isfile(output_filename):
            csv_fields_string = ",".join(csv_fields)
            write_to_gzip_file(output_filename, csv_fields_string)

        flight_epoch = json_data["now"]
        ident = None
        enriched_flight_data = {
            "airline": None,
            "flightnumber": None,
            "tailnumber": None,
        }
        for flight in json_data["aircraft"]:
            if "flight" in flight:
                ident = flight["flight"].strip()
                if has_api:
                    enriched_flight_data = get_additional_flight_info(ident)

            flight_data = [
                flight_epoch,
                flight["hex"],
                ident,
                enriched_flight_data["flightnumber"],
                enriched_flight_data["tailnumber"],
                enriched_flight_data["airline"],
                flight["alt_baro"],
                flight["alt_geom"],
                flight["gs"],
                flight["track"],
                flight["lat"],
                flight["lon"],
            ]
            flight_data_string = ",".join(str(v) for v in flight_data)
            write_to_gzip_file(output_filename, flight_data_string)

    except KeyError as e:
        logging.error(f"[{dt.now()}]-KEY_ERROR:{e}")
    except TypeError as e:
        logging.error(f"[{dt.now()}]-TYPE_ERROR:{e}")
