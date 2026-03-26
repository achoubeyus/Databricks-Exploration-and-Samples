# Databricks notebook source
# MAGIC %md
# MAGIC # ADLS to ADLS - Recursive Copy Benchmark mounted with SAS
# MAGIC
# MAGIC This Databricks notebook simulates and benchmarks copying files between two Azure Data Lake Storage (ADLS Gen2) account. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install and Import Required Libraries
# MAGIC
# MAGIC Install any required Azure and benchmarking libraries (if not already available) and import necessary Python modules such as `os`, `time`, `pyspark`, and Azure SDKs.

# COMMAND ----------

print("hello")

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
# MAGIC ## Setup for bronze

# COMMAND ----------

# Name of the Storage Account
blobAccountName = "adlsbattellebronze49340"  # Storage Account for BronzeLayer
# Name of the Container in the Blob Storage Account
containerName = "raw"

# the second piece of information described above goes into blobKey
blobKey = "< Enter Storage SAS Key> "

# the first piece of information described above goes into blobEndpoint
blobEndpoint = f"wasbs://{containerName}@{blobAccountName}.blob.core.windows.net/"


# COMMAND ----------

configKeybronze = f"fs.azure.sas.{containerName}.{blobAccountName}.blob.core.windows.net"

dbutils.fs.mount(
  source = blobEndpoint,
  mount_point = "/mnt/bronzeraw",
  extra_configs = {
    configKeybronze: blobKey
  }
)

# COMMAND ----------

# List Files
dbutils.fs.ls("/mnt/bronzeraw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup for Silver

# COMMAND ----------

# Name of the Storage Account
blobAccountName = "adlsbattellesilver49340"  # # Storage Account for Silver Layer
# Name of the Container in the Blob Storage Account
containerName = "processed"

# the second piece of information described above goes into blobKey
blobKey = "< Enter SAS for ADLS for Silver Layer"

# the first piece of information described above goes into blobEndpoint
blobEndpoint = f"wasbs://{containerName}@{blobAccountName}.blob.core.windows.net/"

# COMMAND ----------

configKeysilver = f"fs.azure.sas.{containerName}.{blobAccountName}.blob.core.windows.net"

dbutils.fs.mount(
  source = blobEndpoint,
  mount_point = "/mnt/silverprocessed",
  extra_configs = {
    configKeysilver: blobKey
  }
)

# COMMAND ----------

# List Files
dbutils.fs.ls("/mnt/silverprocessed")

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
# MAGIC # Copy methods recursively between two ADLS Gen 2 with azcopy

# COMMAND ----------

# DBTITLE 1,Recursive copy — alternative methods
import subprocess
import time
from datetime import datetime

source_dir = "/mnt/bronzeraw/"
dest_dir = "/mnt/silverprocessed/"

# Choose copy method: "dbutils_recurse", "azcopy", or "azure_sdk"
copy_method = "azcopy"

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
    src_sas = "< Use variable or SAS Key for source> "

    # Silver (destination)
    dst_account = "adlsbattellesilver49340"
    dst_container = "processed"
    dst_sas = "< Use variable or SAS for dest / Silver Layer > 

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
    src_sas = "< Enter SAS for Source > "

    dst_account = "adlsbattellesilver49340"
    dst_container = "processed"
    dst_sas = " SAS for Destination > "

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