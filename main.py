import logging
import os
from castai_cluster_utils import *


def main():
    api_key = os.environ['CASTAI_API_KEY']

    clusters = get_clusters(api_key)
    cluster_ids = extract_cluster_ids(clusters)
    date_cluster_instances = build_cluster_dictionary(cluster_ids, api_key)
    display_most_used_instance_types(date_cluster_instances)


if __name__ == '__main__':
    try:
        main()
    except Exception as err:
        logging.error(f'action failed: {str(err)}')
        exit(1)

