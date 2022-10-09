from ast import literal_eval # This is for interpreting [lists] from csv files
from pathlib import Path
import pandas as pd

import .templates

SITE = os.environ.get('SITE')
DATE = os.environ.get('DATE')
BASE_DIR = os.environ.get('BASE_DIR')

# The path to save output to
snapshot_history_file = BASE_DIR/f"{SITE}-{DATE}-snapshot_history.csv"
snapshot_history = pd.read_csv(snapshot_history_file, index_col=0, converters={"successful_indices": literal_eval,"failed_indices": literal_eval})
curator_file = base_dir/f"{SITE}_snapshot_delete_{DATE}_curator.yaml"


# Count total # of indices backed up
indices = set()
for snapshot, data in snapshot_history.iterrows():
    for index in data['successful_indices'] + data['failed_indices']:
        indices.add(index)
total_indices_count = len(indices)
print(f"Read {snapshot_history.shape[0]} snapshots containing {total_indices_count} indices.")

# Keep track of which snapshots are being chosen to keep
# and which indices are contained within
keep_snapshots = set()
backed_up_indices = set()

# Make note of the indices which haven't been backed up
# Add the index to pending_indices_map and use
#     pending_indices_map[index]
# to track which snapshots contain the index
pending_indices_map = {}

# Track snapshot failures
failed_indices_map = {}

for snapshot, data in snapshot_history.iterrows():

    # Skip over test snapshots
    if data['snapshot_class'][:4] == 'test':
        continue

    successful_indices = set(data['successful_indices'])
    failed_indices = set(data['failed_indices'])

    # Track failed indices
    for index in failed_indices:
        if index not in failed_indices_map:
            failed_indices_map[index] = set()
        failed_indices_map[index].add(snapshot)

    # If none of the successful_indices are in backed_up_indices yet
    if successful_indices.isdisjoint(backed_up_indices):

        keep_snapshots.add(snapshot) # Keep this snapshot
        backed_up_indices.update(successful_indices) # Update backed_up_indices

        # Remove these indices from failed and pending_indices_map
        for index in successful_indices:
            if index in failed_indices_map:
                del failed_indices_map[index]

            if index in pending_indices_map:
                del pending_indices_map[index]


    else:
        # Make note of the indices which haven't been backed up
        # Add the index to pending_indices_map and use
        #     pending_indices_map[index]
        # to track which snapshots contain the index
        pending_indices = successful_indices - backed_up_indices
        for index in pending_indices:
            if index not in pending_indices_map:
                pending_indices_map[index] = set()

            pending_indices_map[index].add(snapshot)

            if index in failed_indices_map:
                del failed_indices_map[index]



while pending_indices_map: # Loop until pending_indices_map is empty

    # print(f"{len(pending_indices_map)} pending indices remaining")

    # See which snapshots the remaining indices are stored in
    snapshot_map = {}
    for index in pending_indices_map:
        snapshots = pending_indices_map[index]
        for snapshot in snapshots:
            if snapshot not in snapshot_map:
                snapshot_map[snapshot] = set()
            snapshot_map[snapshot].add(index)

    # Sort the snapshot map by # of pending indices contained in each snapshot
    snapshot_ranking = sorted(snapshot_map, key=lambda x: len(snapshot_map[x]), reverse=True)

    # Keep the highest scoring snapshot.
    snapshot = snapshot_ranking[0]
    indices = snapshot_map[snapshot]

    # Update the tracking lists
    keep_snapshots.add(snapshot)
    backed_up_indices.update(indices)
    for index in indices:
        if index in pending_indices_map:
            del pending_indices_map[index]

    # print(f"Selected {snapshot} with {len(indices)} pending indices. {len(pending_indices_map)} pending indices remaining.")

print(f"Selected {len(keep_snapshots)} snapshots covering {len(backed_up_indices)} / {total_indices_count} indices")
print(f"There are {len(pending_indices_map) + len(failed_indices_map)} / {total_indices_count} indices not yet covered")
print(f"{len(backed_up_indices)} snapshotted + {len(pending_indices_map)} pending + {len(failed_indices_map)} failed == {total_indices_count} total ? {len(backed_up_indices) + len(pending_indices_map) + len(failed_indices_map) == total_indices_count}")

all_snapshots = set(snapshot_history.index.to_list())
delete_snapshots = all_snapshots - keep_snapshots

job = f"{SITE}-{DATE}-delete-snapshots"
repo = os.environ.get('REPO')
keep_snapshots_regex = f"^({'|'.join(keep_snapshots)})$"

with open(curator_file, mode="w") as foo:
    foo.write(templates.make_snapshot_curator_job(job, repo, keep_snapshots_regex))
