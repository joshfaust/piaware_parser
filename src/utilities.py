import requests
import logging
import gzip
import os
import hashlib
import src.aws as aws

from datetime import datetime as dt

def check_internet_connection() -> bool:
    """
    Checks if we have an internet connection by submitting a
    GET request to google.com.
    """
    try:
        r = requests.get("https://google.com")
        if r.status_code == 200:
            return True
        else:
            raise requests.exceptions.ConnectionError

    except requests.exceptions.ConnectionError as e:
        logging.error(f"NO INTERNET CONNECTION:{e}")
        return False
    except requests.exceptions.HTTPError as e:
        logging.error(f"INVALID HTTP RESPONSE:{e}")
        return False


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


def get_file_sha256(file_path: str) -> str:
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


def get_string_md5(data: str) -> str:
    """
    Create an MD5 hash from a string
    """
    hash = hashlib.md5(data.encode("utf-8"))
    return hash.hexdigest()