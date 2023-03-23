import json
import requests
from requests import RequestException
from datetime import datetime


def get_clusters(castai_api_key: str) -> dict:
    url = f"https://api.cast.ai/v1/kubernetes/external-clusters"
    headers = {
        "accept": "application/json",
        "X-API-Key": f"{castai_api_key}"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except RequestException as e:
        print(f'An error has occurred while fetching data from the API: {e}')
    else:
        return json.loads(response.text)


def extract_cluster_ids(clstrs: dict) -> list[str]:
    return [clstr['id'] for clstr in clstrs['items']]


def get_nodes(clstr_id: str, castai_api_key: str) -> dict:
    url = f"https://api.cast.ai/v1/kubernetes/external-clusters/{clstr_id}/nodes"
    headers = {
        "accept": "application/json",
        "X-API-Key": f"{castai_api_key}"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except RequestException as e:
        print(f'An error has occurred while fetching data from the API: {e}')
    else:
        return json.loads(response.text)


def extract_instance_types(nodes: dict) -> list[str]:
    return [node['instanceType'] for node in nodes['items']]


def build_cluster_dictionary(clstr_ids: list[str], castai_api_key) -> dict:
    time = datetime.now()
    cluster_instances = {clstr: extract_instance_types(get_nodes(clstr, castai_api_key)) for clstr in clstr_ids}
    date_cluster_instances = {time: cluster_instances}
    return date_cluster_instances


def display_most_used_instance_types(date_clstr_dict: dict) -> None:
    for date, clstr_instances in date_clstr_dict.items():
        print(f'Date: {date}')
        print(f'Most used instance types per cluster:')

        for clstr_id, instances in clstr_instances.items():
            instance_type_counts = {}

            # Count the instance types in the instances list
            for instance_type in instances:
                if instance_type not in instance_type_counts:
                    instance_type_counts[instance_type] = 0
                instance_type_counts[instance_type] += 1

            # Find the most used instance type(s)
            max_count = max(instance_type_counts.values())
            most_used_types = [instance_type for instance_type, count in instance_type_counts.items() if
                               count == max_count]

            print(f'Cluster ID: {clstr_id}')
            print(f'Most used instance type(s): {most_used_types}')
            print(f'All instances: {instance_type_counts}')
            print(f'Usage count: {max_count}')

        print('\n')

