from datetime import datetime
import re

import pandas as pd

from .functions import load_json

ISO_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

def translate_to_bytes(byte_string):
    mibi_prefixes = {
        "b": 1024 ** 0,
        "k": 1024 ** 1,
        "m": 1024 ** 2,
        "g": 1024 ** 3
    }

    mibi_prefix = byte_string[-2]
    if mibi_prefix.isdigit():
        scale = mibi_prefixes['b']
        size = byte_string[:-1]
    else:
        scale = mibi_prefixes[mibi_prefix]
        size = byte_string[:-2]

    return scale * float(size)

def read_indices_json(indices_data_file):
    indices_json = load_json(indices_data_file)
    index_data = {}
    for index in indices_json:
        name = index['index']
        if name[0] == '.':
            print(f"skipped index {name}")
            continue


        site = indices_data_file.parents[0].name
        if name.count('-') == 1:
            kind, date = name.split('-')
        else:
            kind = name[6:-11]
            date = name[-10:]

        health = index['health']
        status = index['status']
        primary_size = index['pri.store.size']
        total_size = index['store.size']
        document_count = index['docs.count']
        uuid = index['uuid']

        index_data[name] = {
            'site': site,
            'kind': kind,
            'index_date': date,
            'health': health,
            'status': status,
            'primary_size_bytes': translate_to_bytes(primary_size),
            'total_size_bytes': translate_to_bytes(total_size),
            'document_count': document_count,
            'uuid': uuid
        }

    return index_data

def get_index_snapshot_history(snapshots_data_file, index):
    snapshot_data = load_json(snapshots_data_file)

    snapshots = []
    oldest_time = datetime.today()
    oldest_snapshot = ""
    newest_time = datetime(year=1979, month=1, day=1)

    newest_snapshot = ""

    for snapshot in snapshot_data['snapshots']:
        name = snapshot['snapshot']
        indices = snapshot['indices']
        failures = snapshot['failures']

        if index in indices and index not in failures:
            snapshots.append(name)

            # snapshot_date = datetime.fromisoformat(snapshot['start_time'][:-1])
            snapshot_date = datetime.strptime(snapshot['start_time'], ISO_DATETIME_FORMAT)
            if snapshot_date < oldest_time:
                oldest_time = snapshot_date
                oldest_snapshot = name

            if newest_time < snapshot_date:
                newest_time = snapshot_date
                newest_snapshot = name

    return {
        'snapshot_count': len(snapshots),
        'snapshots': snapshots,
        'oldest_snapshot': oldest_snapshot,
        'newest_snapshot': newest_snapshot,
    }

def indices_size_from_history(snapshots_data_file):
    snapshots_data = load_json(snapshots_data_file)

    # for snapshot in snapshots
    #   for index in snapshot['indices']
    #       incremental_size, total_size = check_index_incremental_size_in_snapshot
    #


def check_index_incremental_size_in_snapshot(base_dir, index, snapshot):
    snapshot_detail_file = base_dir/"snapshot_details"/snapshot
    snapshot_detail = load_json(snapshot_detail_file)['snapshots'][0]
    if snapshot_detail:
        indices = snapshot_detail['indices']
        if index in indices:
            incremental_size = indices[index]['stats']['incremental']['size_in_bytes']
            total_size = indices[index]['stats']['total']['size_in_bytes']
            return (incremental_size, total_size)
        else:
            return False
    else:
        return False