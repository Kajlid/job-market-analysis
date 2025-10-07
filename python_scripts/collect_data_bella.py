import requests
import getpass
import os
import json
import subprocess
import datetime

# --- 1. Configuration ---
API_URL = "https://jobstream.api.jobtechdev.se/snapshot"
HEADERS = {"accept": "application/json"}
LOCAL_DIR = "data"
LOCAL_FILE = os.path.join(LOCAL_DIR, "data.json")

# Get current UTC date
today = datetime.datetime.now(datetime.timezone.utc)
date_path = f"yyyy={today.year}/mm={today.month:02d}/dd={today.day:02d}"

# HDFS paths
username = getpass.getuser()
print(username)
hdfs_dir = f"/user/{username}/jobstream/snapshot/{date_path}/"
hdfs_file = hdfs_dir + "job_ads.json"

# --- 2. Fetch data from API ---
response = requests.get(API_URL, headers=HEADERS)
print("Status code:", response.status_code)
data = response.json()

# --- 3. Save data locally ---
os.makedirs(LOCAL_DIR, exist_ok=True)
with open(LOCAL_FILE, "w") as f:
    json.dump(data, f)
print(f"✅ Saved {len(data)} records to {LOCAL_FILE}")

# --- 4. Store data in HDFS ---
subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir])
subprocess.run(["hdfs", "dfs", "-put", "-f", LOCAL_FILE, hdfs_file])
print(f"📂 Snapshot stored in HDFS: {hdfs_file}")