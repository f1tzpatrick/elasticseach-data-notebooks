import os
import json
from pathlib import Path
from datetime import datetime
import pandas as pd

SITE = os.environ.get('SITE')
DATE = os.environ.get('DATE')
BASE_DIR = os.environ.get('BASE_DIR')
ISO_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

snapshot_json = BASE_DIR/"snapshots.json"

# The path to save output to
snapshot_history_file = BASE_DIR/f"{SITE}-{DATE}-snapshot_history.csv"
snapshot_list = BASE_DIR/"snapshot_list.txt"

with open(snapshot_json) as foo:
    snapshot_json = json.load(foo)['snapshots']
print(f"{len(snapshot_json)} snapshots read from json")

snapshot_data = {}
for snapshot in snapshot_json:
    name = snapshot['snapshot']
    snapshot_class = name[:-26] # Everything before "-snapshot-YYYY.mm.DD.HH.MM"

    # datetime.fromisoformat requires python >= 3.7
    # start_time_utc = datetime.fromisoformat(snapshot['start_time'][:-1])
    # end_time_utc = datetime.fromisoformat(snapshot['end_time'][:-1])

    date_format = "%Y-%m-%dT%H:%M:%S.%fZ"

    start_time_utc = datetime.strptime(snapshot['start_time'], ISO_DATETIME_FORMAT)
    end_time_utc = datetime.strptime(snapshot['end_time'], ISO_DATETIME_FORMAT)
    snapshot_date = start_time_utc.date()
    duration_s = snapshot['duration_in_millis'] * 1000
    uuid = snapshot['uuid']

    indices = snapshot['indices']
    failed_indices = [f['index'] for f in snapshot['failures']]
    successful_indices = list(set(indices) - set(failed_indices))

    total_indices_count = len(indices)
    failed_indices_count = len(failed_indices)
    successful_indices_count = len(successful_indices)
    successful_indices_percent = round(100 * (successful_indices_count / total_indices_count), 2)

    snapshot_data[name] = {
        "name" : name,
        "state" : snapshot['state'],
        "snapshot_class" : snapshot_class,
        "snapshot_date" : snapshot_date,
        "start_time_utc" : start_time_utc,
        "end_time_utc" : end_time_utc,
        "duration_s" : duration_s,
        "uuid" : uuid,
        "total_indices_count" :total_indices_count,
        "successful_indices_count" : successful_indices_count,
        "failed_indices_count" : failed_indices_count,
        "successful_indices_percent" : successful_indices_percent,
        "successful_indices" : successful_indices,
        "failed_indices" : failed_indices
    }


df = pd.DataFrame.from_dict(snapshot_data, orient='index')
df.to_csv(snapshot_history_file)

with open(snapshot_list, mode="w") as foo:
    foo.write("\n".join(df.index.to_list()))
