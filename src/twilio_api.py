import configparser
from twilio.rest import Client


def _get_twilio_creds() -> tuple:
    """
    Obtain the Twilio Creds from secrets.conf
    """
    config = configparser.ConfigParser()
    config.read("secrets.conf")
    account_sid = config.get("twilio", "account_sid")
    auth_token = config.get("twilio", "auth_token")
    to_phone_number = config.get("twilio", "to_phone_number")
    from_phone_number = config.get("twilio", "from_phone_number")
    return (account_sid, auth_token, to_phone_number, from_phone_number)


def twilio_api_keys_exist() -> bool:
    """
    Make sure that the API keys have been entered into secrets.conf
    """
    sk, ak, num_a, num_b = _get_twilio_creds()
    if (sk != "None") and (ak != "None") and (num_a != "None") and (num_b != "None"):
        return True
    return False


def send_text_message(message: str) -> bool:
    """
    Sends a text message
    """
    sid, token, to_number, from_number = _get_twilio_creds()
    client = Client(sid, token)
    message = client.messages.create(
        to=f"+{to_number}",
        from_=f"+{from_number}",
        body=message
    )