# %%
import argparse
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
import requests
import time

# %%
base_urls = {
    "yellow_taxi": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata",
}
output_path = "../../data/landing"

# %%
parser = argparse.ArgumentParser()
parser.add_argument("--start_date", type=str, required=True, default="2021-01")
parser.add_argument("--end_date", type=str, required=True, default="2021-05")
# args = parser.parse_args()


# %%
import sys

sys.argv = [
    "teste.py",
    "--start_date", "2023-01",
    "--end_date", "2023-05",
]

args = parser.parse_args()

# %%
Path(output_path).mkdir(parents=True, exist_ok=True)

# %%
def adding_date_to_base_url(base_url: str, start_date: str, end_date:"str"):
    start = datetime.strptime(start_date, "%Y-%m")
    end = datetime.strptime(end_date, "%Y-%m")
    urls = []
    curr = start
    while curr <= end:
        urls.append(f"{base_url}_{curr.strftime('%Y-%m')}.parquet")
        curr += relativedelta(months=1)
    return urls

# %%
def download_file(url, output_path, table_name):
    print(f"[INFO] Starting download: {url}")
    r = requests.get(url, stream = True)

    Path(f"{output_path}/{table_name}").mkdir(parents=True, exist_ok=True)

    file_name = url.rsplit("/", 1)[-1]
    full_filepath = Path(f"{output_path}/{table_name}/{file_name}")
    print(f"[INFO] Saving to: {full_filepath}")

    bytes_downloaded = 0
    with open(full_filepath,"wb") as file:
        for chunk in r.iter_content(chunk_size=1024):
            # writing one chunk at a time to file
            if chunk:
                file.write(chunk)
                bytes_downloaded += len(chunk)
    
    print(f"[INFO] Download complete: {file_name}")
    print(f"[INFO] Downloaded {bytes_downloaded:,} bytes")

# %%
for key in base_urls:
    urls = adding_date_to_base_url(base_urls[key], args.start_date, args.end_date)
    for url in urls:
        download_file(url, output_path, key)
        time.sleep(2)
    time.sleep(10)



