from datetime import datetime
import pandas as pd

from .functions import load_json

ISO_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

def read_snapshot_json(snapshots_data_file):
    snapshot_json = load_json(snapshots_data_file)
    snapshot_data = {}
    for snapshot in snapshot_json['snapshots']:
        name = snapshot['snapshot']

        # kind = name[:-26] # Everything before "-snapshot-YYYY.mm.DD.HH.MM"
        kind = name.split('-')[1] # The second word in the name; "security" or "non"
        if kind == "security":
            kind = "security-logs"
        elif kind == "non":
            kind = "non-security-logs"
        else:
            kind = "unknown"


        # start_time_utc = datetime.fromisoformat(snapshot['start_time'][:-1])
        # end_time_utc = datetime.fromisoformat(snapshot['end_time'][:-1])
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

        if start_time_utc.hour != 23: # For SLM snapshots, let's only look at the end of day snapshot
            continue

        snapshot_data[name] = {
            "state" : snapshot['state'],
            "kind" : kind,
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


    return snapshot_data

def lookup_snapshot_size(snapshot_detail_file):
    snapshot_json = load_json(snapshot_detail_file)
    if snapshot_json:
        stats = snapshot_json['snapshots'][0]['stats']
        total_files = stats['total']['file_count']
        total_bytes = stats['total']['size_in_bytes']
        incremental_files = stats['incremental']['file_count']
        incremental_bytes = stats['incremental']['size_in_bytes']
    else:
        total_files = None
        total_bytes = None
        incremental_files = None
        incremental_bytes = None

    return {
        'total_size_files': total_files,
        'total_size_bytes': total_bytes,
        'incremental_size_files': incremental_files,
        'incremental_size_bytes': incremental_bytes
    }
