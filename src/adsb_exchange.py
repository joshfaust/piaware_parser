import requests
import json
import configparser
import logging
import src.utilities as utils


def _get_api_key() -> str:
    """
    Retrieves the ADSBexchange API key from secrets.conf
    """
    config = configparser.ConfigParser()
    config.read("secrets.conf")
    return config.get("adsbexchange", "key")


def adsb_api_key_exists() -> bool:
    """
    Make sure that the API keys have been entered into secrets.conf
    """
    key= _get_api_key()
    if (key != "None"):
        return True
    return False


def get_aircraft_by_icao(icao: str) -> dict:
    """
    Queries ADSBExchange Aircraft/flight data by icao
    """
    try:
        uri = f"https://adsbexchange-com1.p.rapidapi.com/icao/{icao.upper()}/"
        headers = {
        'x-rapidapi-key': _get_api_key(),
        'x-rapidapi-host': "adsbexchange-com1.p.rapidapi.com"
        }
        r = requests.get(uri, headers=headers)
        aircraft_data = r.json()
        return aircraft_data
    except requests.exceptions.ConnectionError as e:
        logging.error(f"ADSB HTTP Exception:{e}")
        return {}
    except requests.exceptions.HTTPError as e:
        logging.error(f"ADSB HTTP Exception:{e}")
        return {}
    except json.JSONDecodeError as e:
        logging.error(f"JSON_DECODING_ERROR: {r.text}")
        return {"error":r.text}


def get_aircraft_by_registration(reg: str) -> dict:
    """
    Queries ADSBExchange Aircraft/flight data by registration
    """
    try:
        uri = f"https://adsbexchange-com1.p.rapidapi.com/registration/{reg.upper()}/"
        headers = {
        'x-rapidapi-key': _get_api_key(),
        'x-rapidapi-host': "adsbexchange-com1.p.rapidapi.com"
        }
        r = requests.get(uri, headers=headers)
        aircraft_data = r.json()
        return aircraft_data
    except requests.exceptions.ConnectionError as e:
        logging.error(f"ADSB HTTP Exception:{e}")
        return {}
    except requests.exceptions.HTTPError as e:
        logging.error(f"ADSB HTTP Exception:{e}")
        return {}
    except json.JSONDecodeError as e:
        logging.error(f"JSON_DECODING_ERROR: {r.text}")
        return {"error":r.text}
