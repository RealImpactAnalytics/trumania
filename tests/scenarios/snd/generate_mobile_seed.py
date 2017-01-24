#!/usr/bin/env python

import os
import argparse
import subprocess
import pandas as pd
import shutil

parser = argparse.ArgumentParser(
    description="Generate the mobile sync seed, based on the generator output")
parser.add_argument('circus', help='name of the circus')
parser.add_argument('--db', '-d', help='_DB folder')
parser.add_argument('--logs', '-l', help='generator output (logs) folder')
parser.add_argument('--target_folder', '-t',
                    help='where to output the seed', default=".")
args = parser.parse_args()

# Create seed folder
seed_dir = os.path.join(args.target_folder, "seed_tmp")
if os.path.isdir(seed_dir):
    shutil.rmtree(seed_dir)

mobile_sync_dir = os.path.join(seed_dir, "mobile_sync")
os.makedirs(mobile_sync_dir)

# Copy point_of_interest.csv to seed folder
shutil.copyfile(
    os.path.join(args.db, args.circus, "points_of_interest.csv"),
    os.path.join(mobile_sync_dir, "points_of_interest.csv"))

# Copy pos_id_msisdn.csv to seed folder
shutil.copyfile(
    os.path.join(args.db, args.circus, "pos_id_msisdn.csv"),
    os.path.join(mobile_sync_dir, "pos_id_msisdn.csv"))

# Load tasks and save to seed folder
tasks = pd.read_csv(os.path.join(args.logs, args.circus, "pos_surveys.csv"))

tasks = tasks.rename(columns={
    "TASK_ID": "id",
    "FA_ID": "user_username",
    "POS_ID": "poi_id",
    "POS_LATITUDE": "latitude",
    "POS_LONGITUDE": "longitude",
    "TIME": "completed_at",
    "STATUS": "status"
})

tasks = tasks.reindex_axis([
    "id", "type", "user_username", "poi_id", "status", "due_date",
    "notified_at", "synchronized_at", "received_at", "started_at",
    "completed_at", "latitude", "longitude", "note"
], axis=1)

target_file = os.path.join(mobile_sync_dir, "tasks.csv")
tasks.to_csv(target_file, index=False)

# Compress archive
subprocess.call(["tar", "-czvf",
                 os.path.join(args.target_folder, "mobile_sync.tar.gz"),
                 "-C", seed_dir, "."])

# Delete temporary seed folder
shutil.rmtree(seed_dir)
