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
    r = session.get(current_url)
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


async def get_data_batch(batch_start: int, batch_end: int):
    """Asynchronously download the batch of data desired.

    We use Threadpool to spawn the requests

    each file has an id, that is passed to the executor
    
    Arguments:
        batch_start {int} -- starting index for the batch 
        batch_end {int} -- ending index for the batch
    """
    with ThreadPoolExecutor(max_workers=32) as executor:
        with requests.Session() as session:
            session.headers = headers
            evt_loop = asyncio.get_event_loop()
            tasks = [
                evt_loop.run_in_executor(
                    executor, request_new_file, *(session, file_id)
                )
                for file_id in tqdm(range(batch_start, batch_end))
            ]
            for response in await asyncio.gather(*tasks):
                pass


def execute_download(batch_start: int, batch_end: int):
    """Main caller
    
    Arguments:
        batch_start {int} -- [description]
        batch_end {int} -- [description]
    """
    future_results = asyncio.ensure_future(get_data_batch(batch_start, batch_end))
    evt_loop = asyncio.get_event_loop()
    evt_loop.run_until_complete(future_results)
    return True


def main():
    lg = logger.add(
        "events.log",
        rotation="25 MB",
        compression="zip",
        format="{time} {level} {message}",
        level="INFO",
    )
    start_time = time.time()
    logger.info("Starting the script...")
    execute_download(920, 921)
    logger.success("Batch download finished succesfully.")

    logger.info(
        f"Finished. Done in {str(timedelta(seconds=(time.time() - start_time)))}."
    )
    logger.remove(lg)


if __name__ == "__main__":
    main()
