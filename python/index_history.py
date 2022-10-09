import os
import json
from pathlib import Path
import pandas as pd

SITE = os.environ.get('SITE')
DATE = os.environ.get('DATE')
BASE_DIR = os.environ.get('BASE_DIR')

indices_json = BASE_DIR/f"indices.json"
snapshot_json = BASE_DIR/f"snapshots.json"
indices_output_file = BASE_DIR/f"{SITE}-{DATE}-index_history.csv"

with open(indices_json) as foo:
    indices_json = json.load(foo)

with open(snapshot_json) as foo:
    snapshot_json = json.load(foo)['snapshots']

# Loop through snapshot json and build a mapping of index : snapshots,
# such that:
#     snapshot_data.get(index)
# returns a Series describing which snapshots the index is backed up in
#     [("snapshot_name", bool),...]

# This is data structure is referenced later

snapshots = {}
for item in snapshot_json:
    snapshot = item['snapshot']
    indices = item['indices']
    failures = item['failures']
    failed_indices = [i['index'] for i in failures]
    row = {}
    for index in indices:
        if index in failed_indices:
            row[index] = False
        else:
            row[index] = True
    snapshots[snapshot] = row

snapshot_data = pd.DataFrame.from_dict(snapshots, orient='index')
snapshot_data.fillna(value=False, inplace=True)
# Sort rows and columns
snapshot_data.sort_index(axis="rows", ascending=False, inplace=True)
snapshot_data.sort_index(axis="columns", key=lambda index: index.str[-10:], ascending=False, inplace=True)

# Loop through the index json and build a df that resembles
#     GET _cat/indices?v
# output, but with added fields (site name, index type, date)
# and snapshot information; then export it to csv

# Loop through the index json
index_history = {}
for item in indices_json:

    index = item['index']

    if index[0] == '.':
        print(f"skipped index {item['index']}")
        continue

    index_parts = index.split('-')
    if len(index_parts) == 3:
        site, kind, date = index_parts
    elif index_parts == 2:
        site = SITE
        kind, date  = index_parts

    health = item['health']
    status = item['status']
    primary_size = item['pri.store.size']
    total_size = item['store.size']
    document_count = item['docs.count']
    uuid = item['uuid']
    row = {
        'site': site,
        'kind': kind,
        'date': date,
        'health': health,
        'status': status,
        'primary_size': primary_size,
        'total_size': total_size,
        'document_count': document_count,
        'uuid': uuid
    }

    # Pull index's snapshot history from snapshot_data
    snapshot_record = snapshot_data.get(index)
    if snapshot_record is not None:
        snapshot_record = snapshot_record[snapshot_record == True] # Filter the Series to only True values
        snapshots = snapshot_record.index.to_list() # Get a list of snapshot names from the Series index
        snapshot_history = {
            "backed_up" : True,
            "backup_duplicaiton" : len(snapshots),
            "newest_snapshot" : snapshots[0], # This list was sorted by date earlier
            "oldest_snapshot" : snapshots[-1],
            "snapshots" : snapshots
        }
    else:
        snapshot_history = {
            "backed_up" : False,
            "backup_duplicaiton" : 0,
            "newest_snapshot" : "",
            "oldest_snapshot" : "",
            "snapshots" : []
        }

    row.update(snapshot_history)
    index_history[index] = row

df = pd.DataFrame.from_dict(index_history, orient='index')
df.to_csv(indices_output_file)
