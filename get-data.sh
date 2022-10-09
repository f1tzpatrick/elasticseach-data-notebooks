#!/bin/bash
set -ex

# List of snapshot repositories to query against
: "${SNAPSHOT_REPOSITORIES:=""}"

# Run Commands on an Elasticseach Pod
function client_command() { 
    kubectl exec -i -n osh-infra ${client_pod} -c elasticsearch-client -- $1
}

# Get elasticsearch client
client_pod=$(kubectl get pods -n osh-infra -l application=elasticsearch,component=client --no-headers | awk '{ print $1; exit }')
cluster_name=$(client_command "curl -s localhost:9200/" | jq -r '.cluster_name')

# Set up working directory
temp_dir=$(mktemp -d)
base_dir="${temp_dir}/${cluster_name}"
mkdir -p "${base_dir}/snapshot_details"

client_command "curl -s localhost:9200/_cat/indices?format=json&bytes=b" > "${base_dir}/indices.json"

for $repo in $SNAPSHOT_REPOSITORIES; do
    client_command "curl -s localhost:9200/_snapshot/${repo}/_all" > "${base_dir}/${repo}-snapshots.json"
    snapshot_list=$(cat "${base_dir}/${repo}-snapshots.json" | jq -r .snapshots[].snapshot)
    for snapshot in $snapshot_list; do
        client_command "curl localhost:9200/_snapshot/${repo}/${snapshot}/_status" > "${base_dir}/snapshot_details/${snapshot}.json"
    done
done

tar czf "${temp_dir}/${cluster_name}-elasticsearch-data.tar.gz" -C "${temp_dir}" "${cluster_name}"
echo "Done. Results saved to ${temp_dir}/${cluster_name}-elasticsearch-data.tar.gz"
