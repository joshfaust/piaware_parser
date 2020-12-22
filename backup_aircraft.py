import os
import logging
import argparse
import random
import time

import src.backup as backup
from datetime import datetime as dt
from datetime import date

logname = f"aircraft_{date.today()}.log"
logging.basicConfig(filename=logname, level=logging.INFO)

# The user may have V3 access and therefore can enrich their data if wanted:
parser = argparse.ArgumentParser()
parser.add_argument(
    "-a",
    "--useapi",
    action="store_true",
    required=False,
    dest="fa_api",
    default=False,
    help="Use the FlightAware V3 api to enrich aircraft data",
)
args = parser.parse_args()

previous_file_hash = ""
aircraft_file_path = "/run/dump1090-fa/aircraft.json"
print("[i] Parser is Running.")

while True:
    file_hash = backup.get_file_hash(aircraft_file_path)

    if previous_file_hash != file_hash:
        previous_file_hash = file_hash
        logging.info(f"[{dt.now()}]:Backing up new aircraft data")
        backup.backup_new_aircraft_data(aircraft_file_path, args.fa_api)
    else:
        logging.info(f"[{dt.now()}]:aircraft.json has not changed. sleeping")

    t = random.uniform(2, 5)
    logging.info(f"[{dt.now()}]:Sleeping for {t} seconds")
    time.sleep(t)
