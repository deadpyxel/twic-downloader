import asyncio
import os
import time
from datetime import timedelta
from zipfile import ZipFile
from concurrent.futures import ThreadPoolExecutor

import requests
from loguru import logger
from tqdm import tqdm

BASE_URL = "https://www.theweekinchess.com/zips/"
headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:73.0) Gecko/20100101 Firefox/73.0",
}


def unzip_completed_file(file_name: str):
    pgn_filename = f"{file_name.split('g')[0]}.pgn"
    with ZipFile(file_name) as zip_f:
        zip_f.extract(pgn_filename)


def request_new_file(session: requests.Session, file_id: int):
    current_file = f"twic{file_id:03d}g.zip"
    current_url = f"{BASE_URL}{current_file}"
    r = requests.get(current_url, headers=headers)
    r.raise_for_status()
    try:
        if r.status_code == requests.codes.ok:
            with open(current_file, "wb") as f:
                f.write(r.content)
                unzip_completed_file(current_file)
            os.remove(current_file)
    except requests.exceptions.HTTPError as err:
        logger.error(f"{err}")
    except requests.exceptions.Timeout:
        logger.error("There has been a timeout, check your connection...")
    except requests.exceptions.TooManyRedirects:
        logger.error("Too many redirects, check if TWIC has been moved")
    except requests.exceptions.RequestException as e:
        logger.error(f"There has been an error with your request: {e}")
            break
    else:
        logger.success("Batch download finished succesfully.")
        logger.info("Removing uneeded files...")
        cleanup()

    logger.info(
        f"Finished. Done in {str(timedelta(seconds=(time.time() - start_time)))}."
    )
    logger.remove(lg)


if __name__ == "__main__":
    main()
