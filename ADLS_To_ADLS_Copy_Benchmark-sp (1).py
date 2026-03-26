# Databricks notebook source
# MAGIC %md
# MAGIC # Exploring
# MAGIC ## This should be used as guideline. Review the code do necessary changes including account name, tenant, client id etc.. 
# MAGIC ## Make sure to Test it. Remove unwanted cells 
# MAGIC   - ADLS Mounts with Service Principals and SAS Tokens
# MAGIC   - Data Copy Using AzCopy and dbutils
# MAGIC
# MAGIC This notebook demonstrates how to use a Service Principal (client ID/secret or certificate) to:
# MAGIC - Mount Azure Data Lake Storage (ADLS) Gen2 in Databricks
# MAGIC - Use AzCopy for ADLS-to-ADLS copy operations
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Service Principal with Storage Blob Data Contributor role on both source and destination ADLS accounts
# MAGIC - Directory (tenant) ID, Application (client) ID, and client secret (or certificate)
# MAGIC - Databricks cluster with access to the required libraries
# MAGIC - AzCopy installed (for copy operations)
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC # ADLS to ADLS - Recursive Copy Benchmark
# MAGIC
# MAGIC This Databricks notebook simulates and benchmarks copying files between two Azure Data Lake Storage (ADLS Gen2) account. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Set Service Principal Credentials
# MAGIC
# MAGIC Store your credentials securely (Databricks secrets or environment variables recommended):
# MAGIC - `tenant_id` (Directory ID)
# MAGIC - `client_id` (Application ID)
# MAGIC - `client_secret` (Client Secret)
# MAGIC - `adls_account_name` (ADLS Gen2 account name)
# MAGIC - `container_name` (Container to mount)
# MAGIC
# MAGIC Example (for demo only, use secrets in production):

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install and Import Required Libraries
# MAGIC
# MAGIC Install any required Azure and benchmarking libraries (if not already available) and import necessary Python modules such as `os`, `time`, `pyspark`, and Azure SDKs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1. Mount ADLS Gen2 using Service Principal
# MAGIC
# MAGIC Use the following code to mount an ADLS Gen2 container in Databricks using your Service Principal credentials.

# COMMAND ----------

# MAGIC %md
# MAGIC # Set credentials (replace with Databricks secrets in production)
# MAGIC ```` tenant_id = "<your-tenant-id>"
# MAGIC client_id = "<your-client-id>"
# MAGIC client_secret = "<your-client-secret>"
# MAGIC adls_account_name = "<your-adls-account-name>"
# MAGIC container_name = "<your-container-name>"
# MAGIC
# MAGIC # Optionally, set destination account for copy
# MAGIC adls_account_name_dst = "<your-destination-adls-account>"
# MAGIC container_name_dst = "<your-destination-container>"
# MAGIC ````

# COMMAND ----------

# Install Azure SDKs if needed (uncomment if running outside Databricks pre-installed environment)
# %pip install azure-storage-file-datalake

import os
import time
from pyspark.sql import SparkSession
from azure.storage.filedatalake import DataLakeServiceClient

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Set Up Mount Points for Multiple ADLS i.e bronze, silver, gold
# MAGIC
# MAGIC Configure access to Azure Data Lake Storage (ADLS) using service principal credentials or managed identity. Set Spark configurations for ADLS access.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Setting mount for Bronze with Service Principal

# COMMAND ----------

account_name = "adlsbattellebronze49340"
client_id = "<> "
tenant_id = "< > "
client_secret = "< > "
containerName = "raw"

#configs = {
#  f"fs.azure.account.auth.type.{account_name}.dfs.core.windows.net": "OAuth",
#  f"fs.azure.account.oauth.provider.type.{account_name}.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#  f"fs.azure.account.oauth2.client.id.{account_name}.dfs.core.windows.net": client_id,
#  f"fs.azure.account.oauth2.client.secret.{account_name}.dfs.core.windows.net": client_secret,
#  f"fs.azure.account.oauth2.client.endpoint.{account_name}.dfs.core.windows.net": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
#}

#spark = SparkSession.builder.getOrCreate()
#for key, value in configs.items():
#    spark.conf.set(key, value)

# COMMAND ----------

# Mount with service principal 
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": client_id,
  "fs.azure.account.oauth2.client.secret": client_secret,
  "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# COMMAND ----------

#dbutils.fs.unmount("/mnt/bronzeraw")
#dbutils.fs.ls("/mnt/bronzeraw")

# COMMAND ----------


dbutils.fs.mount(
  source = f"abfss://{containerName}@{account_name}.dfs.core.windows.net/",
  mount_point = "/mnt/bronzeraw",
  extra_configs = configs
)

# COMMAND ----------

# List Files
dbutils.fs.ls("/mnt/bronzeraw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Setting mount for Silver with Service Principal

# COMMAND ----------

account_name = "adlsbattellesilver49340"
client_id = "< > "
tenant_id = "< > "
client_secret = "<>"
containerName = "processed"

#configs = {
#  f"fs.azure.account.auth.type.{account_name}.dfs.core.windows.net": "OAuth",
#  f"fs.azure.account.oauth.provider.type.{account_name}.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
#  f"fs.azure.account.oauth2.client.id.{account_name}.dfs.core.windows.net": client_id,
#  f"fs.azure.account.oauth2.client.secret.{account_name}.dfs.core.windows.net": client_secret,
#  f"fs.azure.account.oauth2.client.endpoint.{account_name}.dfs.core.windows.net": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
#}

#spark = SparkSession.builder.getOrCreate()
#for key, value in configs.items():
#    spark.conf.set(key, value)

# COMMAND ----------

# Mount with service principal 
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": client_id,
  "fs.azure.account.oauth2.client.secret": client_secret,
  "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# COMMAND ----------

dbutils.fs.mount(
  source = f"abfss://{containerName}@{account_name}.dfs.core.windows.net/",
  mount_point = "/mnt/silverprocessed",
  extra_configs = configs
)

# COMMAND ----------

#dbutils.fs.unmount("/mnt/silverprocessed")
dbutils.fs.ls("/mnt/silverprocessed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Use AzCopy for ADLS-to-ADLS Copy
# MAGIC
# MAGIC You can use AzCopy to copy data between ADLS Gen2 accounts using a Service Principal. This requires generating OAuth tokens and passing them to AzCopy.

# COMMAND ----------

# MAGIC %md
# MAGIC ## azcopy setup
# MAGIC ### check current version and reinstall if needed
# MAGIC ### To reinstall changed the marked down to python 

# COMMAND ----------

    # Print azcopy version for reference
    ver = subprocess.run(["/usr/local/bin/azcopy", "--version"], capture_output=True, text=True)
    print(f"azcopy installed: {ver.stdout.strip()}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ```
# MAGIC
# MAGIC     # Step 1: Install azcopy
# MAGIC     print("Installing azcopy...")
# MAGIC     install_cmds = [
# MAGIC         "curl -sL https://aka.ms/downloadazcopy-v10-linux -o /tmp/azcopy.tar.gz",
# MAGIC         "tar -xzf /tmp/azcopy.tar.gz -C /tmp/",
# MAGIC         "cp /tmp/azcopy_linux_amd64_*/azcopy /usr/local/bin/",
# MAGIC         "chmod +x /usr/local/bin/azcopy"
# MAGIC     ]
# MAGIC     for cmd in install_cmds:
# MAGIC         subprocess.run(cmd, shell=True, check=True, capture_output=True)
# MAGIC     
# MAGIC     # Print azcopy version for reference
# MAGIC     ver = subprocess.run(["/usr/local/bin/azcopy", "--version"], capture_output=True, text=True)
# MAGIC     print(f"azcopy installed: {ver.stdout.strip()}\n")
# MAGIC
# MAGIC ```

# COMMAND ----------

    # Step 1: Install azcopy
    print("Installing azcopy...")
    install_cmds = [
        "curl -sL https://aka.ms/downloadazcopy-v10-linux -o /tmp/azcopy.tar.gz",
        "tar -xzf /tmp/azcopy.tar.gz -C /tmp/",
        "cp /tmp/azcopy_linux_amd64_*/azcopy /usr/local/bin/",
        "chmod +x /usr/local/bin/azcopy"
    ]
    for cmd in install_cmds:
        subprocess.run(cmd, shell=True, check=True, capture_output=True)
    
    # Print azcopy version for reference
    ver = subprocess.run(["/usr/local/bin/azcopy", "--version"], capture_output=True, text=True)
    print(f"azcopy installed: {ver.stdout.strip()}\n")


# COMMAND ----------

# MAGIC %md
# MAGIC !#!/bin/bash
# MAGIC !wget -O /tmp/azcopy.tar.gz https://aka.ms/downloadazcopy-v10-linux
# MAGIC !tar -xvf /tmp/azcopy.tar.gz --strip-components=1 -C /usr/local/bin
# MAGIC !cp /dbfs/azcopy/azcopy /usr/local/bin/azcopy
# MAGIC !chmod +x /usr/local/bin/azcopy
# MAGIC !azcopy --version

# COMMAND ----------

# MAGIC %sh
# MAGIC echo "Hello from Databricks"
# MAGIC hostname

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract credential and token if neeed

# COMMAND ----------

# Example: Generate OAuth token for AzCopy (using MSAL)
from azure.identity import ClientSecretCredential

# Get token for https://storage.azure.com/.default
credential = ClientSecretCredential(tenant_id, client_id, client_secret)
token = credential.get_token("https://storage.azure.com/.default").token
print(credential)
print(token)

# COMMAND ----------

# MAGIC
# MAGIC %sh
# MAGIC export AZCOPY_SPA_CLIENT_SECRET="UxI8Q~0QviCHFlyHPA3t.pwIXZUSR_sRtpSZ3b2N"
# MAGIC echo "Secret injected"
# MAGIC echo "AZCOPY_SPA_CLIENT_SECRET=$AZCOPY_SPA_CLIENT_SECRET"

# COMMAND ----------

# MAGIC %sh
# MAGIC export AZCOPY_SPA_CLIENT_SECRET="UxI8Q~0QviCHFlyHPA3t.pwIXZUSR_sRtpSZ3b2N"
# MAGIC azcopy login \
# MAGIC   --service-principal \
# MAGIC   --application-id "541f31cc-480b-462f-8417-98146ba29fa8" \
# MAGIC   --tenant-id "16b3c013-d300-468d-ac64-7eda0820b6d3"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Single thread (Sequencial) copy with Service Principle
# MAGIC
# MAGIC ```
# MAGIC Flag for overwrite
# MAGIC Flag Value | Behavior
# MAGIC true (default) |Overwrites all destination files
# MAGIC false          |Skips files that already exist at destination
# MAGIC ifSourceNewer  |Overwrites only if source has a newer last-modified time
# MAGIC ```

# COMMAND ----------

src_adls_account_name="adlsbattellebronze49340"
dst_adls_account_name="adlsbattellesilver49340"
src_container_name="raw"
dst_container_name="processed"

# COMMAND ----------

import subprocess, os, time
from datetime import datetime

os.environ["AZCOPY_SPA_CLIENT_SECRET"] = "UxI8Q~0QviCHFlyHPA3t.pwIXZUSR_sRtpSZ3b2N"
os.environ["AZCOPY_CONCURRENCY_VALUE"] = "4"

src = "https://adlsbattellebronze49340.dfs.core.windows.net/raw"
dst = "https://adlsbattellesilver49340.dfs.core.windows.net/processed"

start_time = time.time()
print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 100)

proc = subprocess.Popen(
    ["azcopy", "copy", src, dst,
     "--recursive=true", "--overwrite=ifSourceNewer",
     "--log-level=WARNING"],
    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    text=True, bufsize=1,
)

for line in proc.stdout:
    line = line.rstrip()
    if line and "Discarding" not in line:
        print(line, flush=True)

proc.wait()

wall = time.time() - start_time
print("=" * 100)
print(f"End:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Total: {wall:.2f}s ({wall / 60:.2f} min)")
print(f"Exit:  {proc.returncode}")

# COMMAND ----------

import subprocess
import os
import time
import threading
import re
from datetime import datetime

os.environ["AZCOPY_SPA_CLIENT_SECRET"] = "< SAS > "
os.environ["AZCOPY_CONCURRENCY_VALUE"] = "4"

src = "https://adlsbattellebronze49340.dfs.core.windows.net/raw"
dst = "https://adlsbattellesilver49340.dfs.core.windows.net/processed"

# ── List source files using azcopy (no mount required) ─────────────
print("Listing source files via azcopy...")
list_result = subprocess.run(
    ["azcopy", "list", src, "--output-type", "text"],
    capture_output=True, text=True
)

files = []
file_sizes = {}
for line in list_result.stdout.splitlines():
    # Format: "INFO: path/to/file; Content Length: 12345"
    m = re.match(r"INFO:\s*(.+?);\s*Content Length:\s*(\d+)", line.strip())
    if m:
        fname = m.group(1).strip()
        fsize = int(m.group(2))
        files.append((fname, fsize))
        file_sizes[fname] = fsize

total_files = len(files)
total_size = sum(s for _, s in files)
print(f"Files: {total_files}  |  Size: {total_size / (1024**2):,.2f} MB  |  AzCopy threads: {os.environ['AZCOPY_CONCURRENCY_VALUE']}")

start_time = time.time()
print(f"\nStart: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 120)

# ── Shared state for log monitor ────────────────────────────────────
log_path_holder = [None]
completed = [0]
bytes_done = [0]
stop_flag = threading.Event()
print_lock = threading.Lock()

def tail_log():
    """Background thread: tail azcopy log for per-file transfer events."""
    while not stop_flag.is_set():
        if log_path_holder[0] and os.path.exists(log_path_holder[0]):
            break
        time.sleep(0.1)
    if stop_flag.is_set():
        return

    with open(log_path_holder[0], "r") as f:
        while not stop_flag.is_set():
            line = f.readline()
            if not line:
                time.sleep(0.05)
                continue
            if any(kw in line for kw in ("COPYSUCCESSFUL", "UPLOADSUCCESSFUL", "TransferSuccessful")):
                completed[0] += 1
                n = completed[0]
                pct = (n / total_files) * 100 if total_files else 0
                m = re.search(r'/raw/([^\s?;"]+)', line)
                fname = m.group(1) if m else "?"
                fsize = file_sizes.get(fname, 0)
                bytes_done[0] += fsize
                elapsed = time.time() - start_time
                with print_lock:
                    print(
                        f"[Thread-azcopy] [Task {n:>3}/{total_files}] {pct:5.1f}%  "
                        f"{fname:<50} {fsize / (1024**2):>8.1f} MB  +{elapsed:.1f}s",
                        flush=True,
                    )

monitor = threading.Thread(target=tail_log, daemon=True)
monitor.start()

# ── Launch azcopy copy ──────────────────────────────────────────────
proc = subprocess.Popen(
    ["azcopy", "copy", src, dst,
     "--recursive=true", "--overwrite=ifSourceNewer",
     "--block-size-mb=8", "--log-level=INFO"],
    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    text=True, bufsize=1,
)

for line in proc.stdout:
    line = line.rstrip()
    if not line or "Discarding" in line:
        continue
    if "Log file is located at" in line:
        m = re.search(r"at:\s*(.+)", line)
        if m:
            log_path_holder[0] = m.group(1).strip()
    if any(kw in line for kw in ("Final Job", "Elapsed", "Total Number", "Number of File", "Number of Folder")):
        with print_lock:
            print(f"[azcopy] {line}", flush=True)

proc.wait()
time.sleep(1)
stop_flag.set()
monitor.join(timeout=3)

# ── Summary ─────────────────────────────────────────────────────────
end_time = time.time()
wall = end_time - start_time

print("=" * 120)
print(f"End:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"\n--- Summary ---")
print(f"Completed:   {completed[0]}/{total_files} files")
print(f"Transferred: {bytes_done[0] / (1024**2):,.2f} / {total_size / (1024**2):,.2f} MB")
print(f"Total time:  {wall:.2f}s ({wall / 60:.2f} min)")
if wall > 0:
    print(f"Throughput:  {total_size / (1024**2) / wall:.2f} MB/s")
print(f"Exit code:   {proc.returncode}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recursive Parallel copy (full) with Service Principle.
# MAGIC ## By default recursive overright=true. It copies all

# COMMAND ----------

import subprocess
import concurrent.futures
import threading
import time
import os
from datetime import datetime

# ── Config ──────────────────────────────────────────────────────────
src_account   = "adlsbattellebronze49340"
dst_account   = "adlsbattellesilver49340"
src_container = "raw"
dst_container = "processed"
max_workers   = 8

os.environ["AZCOPY_SPA_CLIENT_SECRET"] = "<> "
os.environ["AZCOPY_CONCURRENCY_VALUE"] = "4"  # per-file concurrency

# ── List source files via mount ─────────────────────────────────────
source_mount = "/mnt/bronzeraw/"

def list_files_recursive(path):
    all_files = []
    stack = [path]
    base = ("dbfs:" + path.rstrip("/")) if not path.startswith("dbfs:") else path.rstrip("/")
    while stack:
        current = stack.pop()
        for f in dbutils.fs.ls(current):
            if f.name.endswith("/"):
                stack.append(f.path)
            else:
                rel = f.path.replace(base, "", 1).lstrip("/")
                all_files.append((rel, f.size))
    return all_files

print("Scanning source files...")
files = list_files_recursive(source_mount)
total_size = sum(s for _, s in files)
print(f"Found {len(files)} file(s)  ({total_size / (1024**2):,.2f} MB)")
print(f"Parallel threads: {max_workers}\n")

# ── Thread-safe state ───────────────────────────────────────────────
lock = threading.Lock()
active = []
max_concurrent = [0]
done = [0]
copy_log = []

def copy_file(item):
    rel_path, size = item
    tname = threading.current_thread().name
    tnum  = tname.split("_")[-1]
    t0    = time.time()

    with lock:
        active.append(tname)
        if len(active) > max_concurrent[0]:
            max_concurrent[0] = len(active)

    src = f"https://{src_account}.dfs.core.windows.net/{src_container}/{rel_path}"
    dst = f"https://{dst_account}.dfs.core.windows.net/{dst_container}/{rel_path}"

    r = subprocess.run(
        ["azcopy", "copy", src, dst, "--overwrite=ifSourceNewer", "--log-level=NONE"],
        capture_output=True, text=True
    )

    with lock:
        active.remove(tname)
        done[0] += 1
        n = done[0]

    dur = time.time() - t0
    ok  = r.returncode == 0
    with lock:
        copy_log.append((rel_path, tnum, dur, size, ok))

    tag = "OK" if ok else "FAIL"
    print(f"[Thread-{tnum:>2}] [{n:>4}/{len(files)}] {tag:>4}  {rel_path:<50} {size:>12,} B  {dur:.2f}s")

# ── Run ─────────────────────────────────────────────────────────────
start_time = time.time()
start_dt   = datetime.now()
print(f"Start time: {start_dt.strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 100)

with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
    list(pool.map(copy_file, files))

end_time = time.time()
end_dt   = datetime.now()
wall     = end_time - start_time

print("=" * 100)
print(f"End time:   {end_dt.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"\n--- Summary ---")
ok_count   = sum(1 for *_, s in copy_log if s)
fail_count = len(copy_log) - ok_count
print(f"Files copied:           {ok_count}/{len(files)}")
if fail_count:
    print(f"Failed:                 {fail_count}")
print(f"Total size:             {total_size / (1024**2):,.2f} MB")
print(f"Total wall time:        {wall:.2f}s  ({wall / 60:.2f} min)")
print(f"Max concurrent threads: {max_concurrent[0]}")
seq = sum(d for _, _, d, _, _ in copy_log)
print(f"Sum of durations:       {seq:.2f}s")
print(f"Throughput:             {total_size / (1024**2) / wall:.2f} MB/s")
print(f"Speedup:                {seq / wall:.2f}x")

# COMMAND ----------

# MAGIC %md
# MAGIC ## copy using ADLS mounts ( source and dest)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Define Source and Destination ADLS Paths
# MAGIC
# MAGIC Specify the source and destination container paths in the same ADLS account. Store these as variables for use in file operations.

# COMMAND ----------

# Define source and destination container paths
#source_container = "/mnt/bronzeraw"
#dest_container = "/mnt/silverprocessed"

#source_path = f"abfss://{source_container}@{account_name}.dfs.core.windows.net/benchmark-test-files/"
#dest_path = f"abfss://{dest_container}@{account_name}.dfs.core.windows.net/benchmark-test-files/"

#print(f"Source: {source_path}")
#print(f"Destination: {dest_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Copy Between ADLS Bronze and Silver
# MAGIC
# MAGIC Implement logic to copy files from the source container to the destination container using Spark's file APIs or Azure SDKs.

# COMMAND ----------

import concurrent.futures
import threading
import time
from datetime import datetime

# List all files in the source mount directory
source_dir = "/mnt/bronzeraw/"
dest_dir = "/mnt/silverprocessed/"

def list_files_recursive(path):
    """Recursively list all files (skip directories) under a path."""
    all_files = []
    stack = [path]
    base = ("dbfs:" + path.rstrip("/")) if not path.startswith("dbfs:") else path.rstrip("/")
    while stack:
        current = stack.pop()
        for f in dbutils.fs.ls(current):
            if f.name.endswith("/"):
                stack.append(f.path)
            else:
                rel_path = f.path.replace(base, "", 1).lstrip("/")
                all_files.append(rel_path)
    return all_files

files = list_files_recursive(source_dir)
print(f"Found {len(files)} file(s) recursively in {source_dir}")
print(f"Using max_workers=8\n")

lock = threading.Lock()
active_threads = []
max_concurrent = [0]
copy_log = []

def copy_file(rel_path):
    thread_name = threading.current_thread().name
    file_start = time.time()

    with lock:
        active_threads.append(thread_name)
        current_active = len(active_threads)
        if current_active > max_concurrent[0]:
            max_concurrent[0] = current_active

    src = source_dir.rstrip("/") + "/" + rel_path
    dst = dest_dir.rstrip("/") + "/" + rel_path
    dbutils.fs.cp(src, dst)

    with lock:
        active_threads.remove(thread_name)

    elapsed = time.time() - file_start
    offset = file_start - start_time
    with lock:
        copy_log.append((rel_path, thread_name, offset, elapsed))
    print(f"[{thread_name}]  {rel_path:<45}  start=+{offset:.2f}s  duration={elapsed:.2f}s")

# --- Start ---
start_time = time.time()
start_dt = datetime.now()
print(f"Start time: {start_dt.strftime('%Y-%m-%d %H:%M:%S')}")
print("-" * 100)

with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    list(executor.map(copy_file, files))

end_time = time.time()
end_dt = datetime.now()
total_duration = end_time - start_time

print("-" * 100)
print(f"End time:   {end_dt.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"\n--- Summary ---")
print(f"Total files copied:       {len(files)}")
print(f"Total wall time:          {total_duration:.2f}s")
print(f"Max concurrent threads:   {max_concurrent[0]}")
sum_durations = sum(d for _, _, _, d in copy_log)
print(f"Sum of individual times:  {sum_durations:.2f}s")
print(f"Speedup (sequential/wall): {sum_durations / total_duration:.2f}x")

# COMMAND ----------

# MAGIC %md
# MAGIC # Copy methods with mounts - dbutils and azcopy and sas

# COMMAND ----------

# DBTITLE 1,Recursive copy — alternative methods
import subprocess
import time
from datetime import datetime

source_dir = "/mnt/bronzeraw/"
dest_dir = "/mnt/silverprocessed/"

# Choose copy method: "dbutils_recurse", "azcopy", or "azure_sdk"
#copy_method = "azcopy"
copy_method = "dbutils_recurse"

print(f"=== Recursive Copy: {copy_method.upper()} ===")
print(f"Source:      {source_dir}")
print(f"Destination: {dest_dir}")

start_time = time.time()
start_dt = datetime.now()
print(f"Start time:  {start_dt.strftime('%Y-%m-%d %H:%M:%S')}")
print("-" * 80)

if copy_method == "dbutils_recurse":
    # --- Method 1: dbutils.fs.cp with recurse=True (simplest built-in) ---
    print("Using dbutils.fs.cp(recurse=True)...\n")
    dbutils.fs.cp(source_dir, dest_dir, recurse=True)
    print("Copy completed.")

elif copy_method == "azcopy":
    # --- Method 2: AzCopy (fastest for large Azure-to-Azure transfers) ---
    # AzCopy uses server-side copy between Azure storage accounts

    # Step 1: Install azcopy
    print("Installing azcopy...")
    install_cmds = [
        "curl -sL https://aka.ms/downloadazcopy-v10-linux -o /tmp/azcopy.tar.gz",
        "tar -xzf /tmp/azcopy.tar.gz -C /tmp/",
        "cp /tmp/azcopy_linux_amd64_*/azcopy /usr/local/bin/",
        "chmod +x /usr/local/bin/azcopy"
    ]
    for cmd in install_cmds:
        subprocess.run(cmd, shell=True, check=True, capture_output=True)
    
    # Print azcopy version for reference
    ver = subprocess.run(["/usr/local/bin/azcopy", "--version"], capture_output=True, text=True)
    print(f"azcopy installed: {ver.stdout.strip()}\n")

    # Step 2: Define source and destination Azure URLs with SAS tokens
    # Use DFS endpoint (dfs.core.windows.net) for hierarchical namespace support
    # This resolves the empty folder warning when HNS is enabled on the storage account

    # Bronze (source)
    src_account = "adlsbattellebronze49340"
    src_container = "raw"
    src_sas = "< > "

    # Silver (destination)
    dst_account = "adlsbattellesilver49340"
    dst_container = "processed"
    dst_sas = "< > "

    # DFS endpoint for full folder support (ADLS Gen2 with HNS)
    src_url = f"https://{src_account}.dfs.core.windows.net/{src_container}?{src_sas}"
    dst_url = f"https://{dst_account}.dfs.core.windows.net/{dst_container}?{dst_sas}"

    # Step 3: Run azcopy with --recursive
    # DFS endpoint enables hierarchical namespace awareness for empty folder support
    # If HNS is NOT enabled on the storage accounts, the empty folder INFO message
    # is benign — all files are still copied correctly, only empty dirs are skipped
    print("Running azcopy copy --recursive ...\n")
    azcopy_cmd = [
        "/usr/local/bin/azcopy", "copy",
        src_url, dst_url,
        "--recursive",
        "--overwrite=ifSourceNewer",
        "--log-level=WARNING"
    ]
    result = subprocess.run(azcopy_cmd, capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    if result.returncode != 0:
        print(f"azcopy exited with code {result.returncode}")
    else:
        print("azcopy completed successfully.")

elif copy_method == "azure_sdk":
    # --- Method 3: Azure SDK (DataLakeServiceClient) ---
    from azure.storage.filedatalake import DataLakeServiceClient

    src_account = "adlsbattellebronze49340"
    src_container = "raw"
    src_sas = "<    > "

    dst_account = "adlsbattellesilver49340"
    dst_container = "processed"
    dst_sas = "<   > "

    src_client = DataLakeServiceClient(account_url=f"https://{src_account}.dfs.core.windows.net", credential=src_sas)
    dst_client = DataLakeServiceClient(account_url=f"https://{dst_account}.dfs.core.windows.net", credential=dst_sas)

    src_fs = src_client.get_file_system_client(src_container)
    dst_fs = dst_client.get_file_system_client(dst_container)

    file_count = 0
    for path in src_fs.get_paths(recursive=True):
        if not path.is_directory:
            src_file = src_fs.get_file_client(path.name)
            dst_file = dst_fs.get_file_client(path.name)

            download = src_file.download_file()
            data = download.readall()

            dst_file.create_file()
            dst_file.append_data(data, offset=0, length=len(data))
            dst_file.flush_data(len(data))

            file_count += 1
            print(f"  Copied: {path.name} ({len(data):,} bytes)")

    print(f"\nAzure SDK copy completed: {file_count} files.")

else:
    raise ValueError(f"Invalid copy_method: '{copy_method}'. Use 'dbutils_recurse', 'azcopy', or 'azure_sdk'.")

# --- Timing summary ---
end_time = time.time()
end_dt = datetime.now()
total_duration = end_time - start_time

print("-" * 80)
print(f"End time:    {end_dt.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Total time:  {total_duration:.2f} seconds ({total_duration / 60:.2f} minutes)")

# COMMAND ----------

# DBTITLE 1,Recursive copy (full/incremental)
import concurrent.futures
import time
import hashlib

copy_mode = "full"  # "full" = MD5-check all common files, "incremental" = MD5-check all common files (same check, lighter scan option)

def list_files_recursive(path):
    """Recursively list all files (skip directories) under a path."""
    all_files = []
    stack = [path]
    base = ("dbfs:" + path.rstrip("/")) if not path.startswith("dbfs:") else path.rstrip("/")
    while stack:
        current = stack.pop()
        for f in dbutils.fs.ls(current):
            if f.name.endswith("/"):
                stack.append(f.path)
            else:
                rel_path = f.path.replace(base, "", 1).lstrip("/")
                all_files.append((rel_path, f.size))
    return all_files

def compute_md5(mount_path):
    """Compute MD5 hash of a file via local /dbfs/ access."""
    local_path = "/dbfs" + mount_path
    h = hashlib.md5()
    with open(local_path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def check_md5_pair(rel_path):
    """Compare MD5 of source and destination for a relative path."""
    src_hash = compute_md5(source_dir.rstrip("/") + "/" + rel_path)
    dst_hash = compute_md5(dest_dir.rstrip("/") + "/" + rel_path)
    return (rel_path, src_hash != dst_hash)

# --- Scan directories ---
print("Scanning source directory recursively...")
source_files_map = {rel: size for rel, size in list_files_recursive(source_dir)}
print(f"  Found {len(source_files_map)} file(s) in source.")

print("Scanning destination directory recursively...")
dest_files_map = {rel: size for rel, size in list_files_recursive(dest_dir)}
print(f"  Found {len(dest_files_map)} file(s) in destination.")

# --- Identify missing files ---
missing_files = sorted(rel for rel in source_files_map if rel not in dest_files_map)
common_files = sorted(rel for rel in source_files_map if rel in dest_files_map)

# --- MD5-check all common files in both modes ---
if copy_mode not in ("full", "incremental"):
    raise ValueError(f"Invalid copy_mode: '{copy_mode}'. Use 'full' or 'incremental'.")

files_to_check = common_files  # Both modes MD5-check all common files

print(f"\n=== {copy_mode.upper()} MODE ===")
print(f"Missing in destination: {len(missing_files)} file(s)")
print(f"Files to MD5-check:    {len(files_to_check)} file(s)")

# --- Compute MD5 in parallel ---
modified_files = []
if files_to_check:
    print("\nComputing MD5 checksums...")
    check_start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        md5_results = list(executor.map(check_md5_pair, files_to_check))
    check_duration = time.time() - check_start
    modified_files = sorted(rel for rel, is_different in md5_results if is_different)
    print(f"  MD5 check completed in {check_duration:.2f}s \u2014 {len(modified_files)} modified file(s) found.")

# --- Copy missing + modified files ---
files_to_copy = missing_files + modified_files
print(f"\nTotal files to copy: {len(files_to_copy)} ({len(missing_files)} missing + {len(modified_files)} modified)")

if not files_to_copy:
    print("Destination is up to date. Nothing to copy.")
else:
    total_bytes_to_copy = sum(source_files_map[rel] for rel in files_to_copy)
    print(f"Total size to copy: {total_bytes_to_copy / (1024**2):,.2f} MB")

    def copy_file(rel_path):
        src = source_dir.rstrip("/") + "/" + rel_path
        dst = dest_dir.rstrip("/") + "/" + rel_path
        try:
            dbutils.fs.cp(src, dst)
            return (rel_path, True, None)
        except Exception as e:
            return (rel_path, False, str(e))

    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(copy_file, files_to_copy))
    copy_duration = time.time() - start_time

    copied_count = sum(1 for _, ok, _ in results if ok)
    failed_files = [(rel, err) for rel, ok, err in results if not ok]
    throughput_mb_s = (total_bytes_to_copy / (1024**2)) / copy_duration if copy_duration > 0 else 0

    print(f"\n--- Results ---")
    print(f"Copied:     {copied_count}/{len(files_to_copy)} files")
    print(f"Duration:   {copy_duration:.2f} seconds")
    print(f"Throughput: {throughput_mb_s:,.2f} MB/s")

    if failed_files:
        print(f"\n{len(failed_files)} file(s) FAILED:")
        for rel_path, error in failed_files:
            print(f"  - {rel_path}: {error}")

# COMMAND ----------

# DBTITLE 1,Verify concurrent copying
import concurrent.futures
import threading
import time

source_dir = "/mnt/bronzeraw/"
dest_dir = "/mnt/silverprocessed/"

# Take a small sample of files to test concurrency
sample_files = [f.name for f in dbutils.fs.ls(source_dir) if not f.name.endswith("/")][:6]
print(f"Testing concurrency with {len(sample_files)} files and max_workers=4\n")

lock = threading.Lock()
active_threads = []
max_concurrent = [0]

def copy_file_with_trace(filename):
    thread_name = threading.current_thread().name
    start = time.time()

    with lock:
        active_threads.append(thread_name)
        current_active = len(active_threads)
        if current_active > max_concurrent[0]:
            max_concurrent[0] = current_active

    src = source_dir + filename
    dst = dest_dir + filename
    dbutils.fs.cp(src, dst)

    with lock:
        active_threads.remove(thread_name)

    elapsed = time.time() - start
    print(f"[{thread_name}]  {filename:<35}  start={start - t0:.2f}s  duration={elapsed:.2f}s")

t0 = time.time()
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    list(executor.map(copy_file_with_trace, sample_files))
total = time.time() - t0

print(f"\n--- Concurrency Summary ---")
print(f"Total wall time:          {total:.2f}s")
print(f"Max concurrent threads:   {max_concurrent[0]}")
print(f"Sequential estimate:      sum of individual durations")
print(f"\nIf max concurrent > 1 and wall time < sequential sum, copies ran in parallel.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Measure and Log Copy Performance
# MAGIC
# MAGIC Record the time taken to copy files and log performance metrics such as throughput and latency.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validate File Copy Integrity
# MAGIC
# MAGIC Verify that all files have been copied correctly by comparing file counts and optionally checksums between source and destination.

# COMMAND ----------

# Validate file copy integrity — Total space and file count comparison

source_dir = "/mnt/bronzeraw/"
dest_dir = "/mnt/silverprocessed/"

def list_files_recursive(path):
    """Recursively list all files (skip directories) under a path."""
    all_files = []
    stack = [path]
    base = ("dbfs:" + path.rstrip("/")) if not path.startswith("dbfs:") else path.rstrip("/")
    while stack:
        current = stack.pop()
        for f in dbutils.fs.ls(current):
            if f.name.endswith("/"):
                stack.append(f.path)
            else:
                rel_path = f.path.replace(base, "", 1).lstrip("/")
                all_files.append((rel_path, f.size))
    return all_files

source_files = list_files_recursive(source_dir)
dest_files = list_files_recursive(dest_dir)

source_total_files = len(source_files)
dest_total_files = len(dest_files)
source_total_bytes = sum(size for _, size in source_files)
dest_total_bytes = sum(size for _, size in dest_files)

print("=" * 70)
print(f"{'':>35} {'SOURCE':>15} {'DESTINATION':>15}")
print("-" * 70)
print(f"{'Directory':>35} {source_dir:>15} {dest_dir:>15}")
print(f"{'Total files':>35} {source_total_files:>15,} {dest_total_files:>15,}")
print(f"{'Total size (bytes)':>35} {source_total_bytes:>15,} {dest_total_bytes:>15,}")
print(f"{'Total size (MB)':>35} {source_total_bytes / (1024**2):>15,.2f} {dest_total_bytes / (1024**2):>15,.2f}")
print(f"{'Total size (GB)':>35} {source_total_bytes / (1024**3):>15,.2f} {dest_total_bytes / (1024**3):>15,.2f}")
print("=" * 70)

# File count check
if source_total_files == dest_total_files:
    print(f"\nFile count: MATCH ({source_total_files} files)")
else:
    print(f"\nFile count: MISMATCH (source={source_total_files}, dest={dest_total_files})")

# Size check
if source_total_bytes == dest_total_bytes:
    print(f"Total size: MATCH ({source_total_bytes:,} bytes)")
else:
    diff = abs(source_total_bytes - dest_total_bytes)
    print(f"Total size: MISMATCH (diff={diff:,} bytes / {diff / (1024**2):,.2f} MB)")

# Per-file comparison by name
source_map = {rel: size for rel, size in source_files}
dest_map = {rel: size for rel, size in dest_files}

missing = sorted(set(source_map) - set(dest_map))
extra = sorted(set(dest_map) - set(source_map))
matched = 0
mismatched = []
for name in sorted(set(source_map) & set(dest_map)):
    if source_map[name] == dest_map[name]:
        matched += 1
    else:
        mismatched.append(name)

print(f"\n--- Per-File Summary ---")
print(f"Matched (same size):  {matched}")
print(f"Size mismatch:        {len(mismatched)}")
print(f"Missing in dest:      {len(missing)}")
print(f"Extra in dest:        {len(extra)}")

if mismatched:
    print(f"\nSize mismatches:")
    for name in mismatched:
        print(f"  ~ {name}  (src: {source_map[name]:,} / dest: {dest_map[name]:,})")
if missing:
    print(f"\nMissing in destination:")
    for name in missing:
        print(f"  - {name}  ({source_map[name]:,} bytes)")
if extra:
    print(f"\nExtra in destination:")
    for name in extra:
        print(f"  + {name}  ({dest_map[name]:,} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC **Notes:**
# MAGIC - Always use Databricks secrets or environment variables for credentials in production.
# MAGIC - The Service Principal must have the correct RBAC roles on both source and destination storage accounts.
# MAGIC - AzCopy can be run from a Databricks init script, a notebook cell (with subprocess), or an external VM/compute.
# MAGIC - For large-scale copy, AzCopy is highly performant and supports parallelism, resume, and logging.
# MAGIC
# MAGIC For more details, see:
# MAGIC - [Mount ADLS Gen2 in Databricks](https://learn.microsoft.com/azure/databricks/data/data-sources/azure/azure-datalake-gen2#--mount-azure-data-lake-storage-gen2)
# MAGIC - [AzCopy with OAuth](https://learn.microsoft.com/azure/storage/common/storage-use-azcopy-authorize-access-azure-active-directory)
# MAGIC - [Azure Identity Python SDK](https://learn.microsoft.com/python/api/overview/azure/identity-readme)