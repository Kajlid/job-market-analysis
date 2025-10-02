import requests
import os
import json
import subprocess
import datetime
from hdfs import InsecureClient

jobstream_url = "https://jobstream.api.jobtechdev.se/snapshot"
today = datetime.datetime.now(datetime.timezone.utc)         # 2025-09-29
# print(f"{today.month:02d} {today.day:02d}")

hdfs_dir = f"/user/{os.getlogin()}/jobstream/snapshot/yyyy={today.year}/mm={today.month:02d}/dd={today.day:02d}/"
local_file = "data/data.json"

hdfs_path = hdfs_dir + "job_ads.json"    # /data/jobstream/snapshot/yyyy=2025/mm=09/dd=29/job_ads.json
# print(hdfs_path)        

headers = {"accept": "application/json"}
response = requests.get(jobstream_url, headers=headers)
print("Status code:", response.status_code)

data = response.json()

with open("data/data.json", "w") as f:       # local version
    json.dump(data, f)
    
print("Saved", len(data), "records to data.json")

subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir])

subprocess.run(["hdfs", "dfs", "-put", "-f", local_file, hdfs_path])
print(f"Snapshot stored in HDFS: {hdfs_path}")

# Snapshot stored in HDFS: /user/root/jobstream/snapshot/yyyy=2025/mm=09/dd=29/job_ads.json
    

