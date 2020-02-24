import time
from datetime import timedelta
from tqdm import tqdm

import requests
from loguru import logger

BASE_URL = "https://www.theweekinchess.com/zips/"
headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:73.0) Gecko/20100101 Firefox/73.0",
}


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
    for i in tqdm(range(920, 1320)): # current available range of TWIC
        try:
            current_file = f"twic{i:03d}g.zip"
            current_url = f"{BASE_URL}{current_file}"
            # logger.info(f"Current URL: {current_url}")
            r = requests.get(current_url, headers=headers)
            r.raise_for_status()
            if r.status_code == requests.codes.ok:
                with open(current_file, "wb") as f:
                    f.write(r.content)
        except requests.exceptions.HTTPError as err:
            logger.error(f"{err}")
        except requests.exceptions.Timeout:
            # Maybe set up for a retry, or continue in a retry loop
            logger.error("There has been a timeout, check your connection...")
        except requests.exceptions.TooManyRedirects:
            logger.error("Too many redirects, check if TWIC has been moved")
            # Tell the user their URL was bad and try a different one
        except requests.exceptions.RequestException as e:
            logger.error(f"There has been an error with your request: {e}")
    logger.info(
        f"Finished. Done in {str(timedelta(seconds=(time.time() - start_time)))}."
    )
    logger.remove(lg)


if __name__ == "__main__":
    main()
