import socket
import os
import logging
import argparse
import random
import time

import src.backup as backup
import src.utilities as utils
import src.twilio_api as twil

from src.adsb_exchange import adsb_api_key_exists
from src.aws import aws_api_keys_exist
from datetime import datetime as dt
from datetime import date

logname = f"aircraft_{date.today()}.log"
logging.basicConfig(filename=logname, level=logging.INFO)
LAST_MESSAGE_SENT = dt.now()

# The user may have V3 access and therefore can enrich their data if wanted:
parser = argparse.ArgumentParser()
parser.add_argument(
    "-a",
    "--useapi",
    action="store_true",
    required=False,
    dest="adsb_api",
    default=False,
    help="Use the FlightAware V3 api to enrich aircraft data",
)
parser.add_argument(
    "-s",
    "--s3",
    action="store_true",
    required=False,
    dest="aws_api",
    default=False,
    help="Upload compressed flight data to S3"
)
parser.add_argument(
    "-t",
    "--twilio",
    action="store_true",
    required=False,
    dest="twi_api",
    default=False,
    help="Use Twilio to send text messages for outages and uptime info"
)
args = parser.parse_args()

if args.adsb_api:
    if not adsb_api_key_exists():
        print(f"[!] You have not added a valid ADS API key to secrets.conf")
        exit(1)

if args.aws_api:
    if not adsb_api_key_exists():
        print(f"[!] You have not added a valid AWS API key to secrets.conf")
        exit(1)

if args.twi_api:
    if not twil.twilio_api_keys_exist():
        print(f"[!] You have not added a valid Twilio API information to secrets.conf")
        exit(1)
    else:
        twil.send_text_message("PiAware has been started!")


previous_file_hash = ""
aircraft_file_path = "/run/dump1090-fa/aircraft.json"
print("[i] Parser is Running.")

while True:
    file_hash = utils.get_file_sha256(aircraft_file_path)

    if previous_file_hash != file_hash:
        previous_file_hash = file_hash
        backup.get_local_aircraft_data(aircraft_file_path, args.adsb_api, args.aws_api, args.twi_api)
    
    if args.twi_api:
        if utils.get_time_delta_hours(LAST_MESSAGE_SENT, dt.now()) >= 1:
            twil.send_text_message("PiAware is Still Running Successfully!")
            LAST_MESSAGE_SENT = dt.now()

    t = random.uniform(3, 10)
    logging.info(f"[{dt.now()}]:Sleeping for {t} seconds")
    time.sleep(t)
