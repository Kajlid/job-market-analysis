import subprocess
import json
import os
import datetime

today = datetime.datetime.now(datetime.timezone.utc)  
hdfs_path = f'/user/{os.getlogin()}/jobstream/snapshot/yyyy=2025/mm=09/dd=29/job_ads.json'

# See if file exists
result = subprocess.run(['hdfs', 'dfs', '-test', '-e', hdfs_path])
if result.returncode == 0:
    print(f"The file exists in HDFS: {hdfs_path}")
else:
    print(f"The file does NOT exist in HDFS: {hdfs_path}")

subprocess.run(['hdfs', 'dfs', '-ls', hdfs_path])