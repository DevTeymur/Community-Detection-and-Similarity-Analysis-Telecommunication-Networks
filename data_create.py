import random
import csv
import itertools
from datetime import datetime, timedelta
import spark # type: ignore

def generate_synthetic_call_data(num_clients,
                                   call_frequency_range,
                                   call_duration_range,
                                   num_communities,
                                   time_range,
                                   save_to_csv=False,
                                   output_file=None):
    clients = list(range(1, num_clients + 1))
    min_duration, max_duration = call_duration_range
    start_time_str, end_time_str = time_range

    start_time = datetime.strptime(start_time_str, '%y%m%d%H%M')
    end_time = datetime.strptime(end_time_str, '%y%m%d%H%M')

    time_diff_minutes = int((end_time - start_time).total_seconds() / 60)

    num_communities = random.randint(num_communities[0], num_communities[1])
    communities = [[] for _ in range(num_communities)]
    community_iter = itertools.cycle(communities)

    for client in clients:
        next(community_iter).append(client)

    call_data = []

    for community in communities:
        if len(community) >= 2:
            first_client, second_client = random.sample(community, 2)
            call_data.append(generate_call(first_client, second_client, start_time, time_diff_minutes, min_duration, max_duration))

        for client in community:
            call_frequency = random.randint(call_frequency_range[0], call_frequency_range[1])

            for _ in range(call_frequency):
                other_client = random.choice(community)
                while other_client == client:
                    other_client = random.choice(community)

                call_data.append(generate_call(client, other_client, start_time, time_diff_minutes, min_duration, max_duration))

    if save_to_csv and output_file:
        with open(output_file, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['c1', 'c2', 'start', 'end'])
            writer.writerows(call_data)

    # Create a PySpark DataFrame
    call_df = spark.createDataFrame(call_data, schema=['c1', 'c2', 'start', 'end'])
    return call_df


def generate_call(client_a, client_b, start_time, time_diff_minutes, min_duration, max_duration):
    """Generate a call record between two clients."""
    # Random call start time and duration
    random_minutes = random.randint(0, time_diff_minutes)
    call_start_time = start_time + timedelta(minutes=random_minutes)
    call_duration = random.randint(min_duration, max_duration)
    call_end_time = call_start_time + timedelta(minutes=call_duration)

    call_start_str = call_start_time.strftime('%y%m%d%H%M')
    call_end_str = call_end_time.strftime('%y%m%d%H%M')

    return [client_a, client_b, call_start_str, call_end_str]
