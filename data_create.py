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


def generate_call(client_a,
                  client_b,
                  start_time,
                  time_diff_minutes,
                  min_duration,
                  max_duration):
    # Random call start time and duration
    random_minutes = random.randint(0, time_diff_minutes)
    call_start_time = start_time + timedelta(minutes=random_minutes)
    call_duration = random.randint(min_duration, max_duration)
    call_end_time = call_start_time + timedelta(minutes=call_duration)

    call_start_str = call_start_time.strftime('%y%m%d%H%M')
    call_end_str = call_end_time.strftime('%y%m%d%H%M')

    return [client_a, client_b, call_start_str, call_end_str]


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

    # Randomly pick the number of communities between the provided range
    num_communities = random.randint(num_communities[0], num_communities[1])
    communities = [[] for _ in range(num_communities)]

    # Use random distribution to assign clients to communities
    for client in clients:
        # Randomly assign a client to a community
        random_community = random.choices(communities, k=1)[0]
        random_community.append(client)

    call_data = []

    for community in communities:
        if len(community) >= 2:
            # Pick two random clients from the community and generate a call
            first_client, second_client = random.sample(community, 2)
            call_data.append(generate_call(first_client, second_client, start_time, time_diff_minutes, min_duration, max_duration))

        for client in community:
            # Determine how many calls this client makes
            call_frequency = random.randint(call_frequency_range[0], call_frequency_range[1])

            for _ in range(call_frequency):
                other_client = random.choice(community)
                while other_client == client:
                    other_client = random.choice(community)

                call_data.append(generate_call(client, other_client, start_time, time_diff_minutes, min_duration, max_duration))

    # Optionally save to CSV
    if save_to_csv and output_file:
        with open(output_file, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['c1', 'c2', 'start', 'end'])
            writer.writerows(call_data)

    # Create and return a PySpark DataFrame
    call_df = spark.createDataFrame(call_data, schema=['c1', 'c2', 'start', 'end'])
    return call_df


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

    # Random number of communities between the provided range
    num_communities = random.randint(num_communities[0], num_communities[1])
    communities = [[] for _ in range(num_communities)]

    # Assign clients to communities, ensuring some communities are larger
    for client in clients:
        community_index = random.randint(0, num_communities - 1)
        communities[community_index].append(client)

    call_data = []

    # Uniform time shift applied to all communities
    uniform_time_shift = random.randint(0, time_diff_minutes // 5)
    community_start_time = start_time + timedelta(minutes=uniform_time_shift)

    # Pre-defined activity levels (low, medium, high)
    call_frequency_levels = [(1, 5), (5, 10), (10, 20)]

    # Predefined call duration ranges
    community_min_duration = min_duration
    community_max_duration = max_duration

    for community in communities:
        # Randomly assign call frequency activity level for the community
        community_call_frequency_range = random.choice(call_frequency_levels)

        for client in community:
            # Determine how many calls this client makes
            call_frequency = random.randint(community_call_frequency_range[0], community_call_frequency_range[1])

            for _ in range(call_frequency):
                other_client = random.choice(community)
                while other_client == client:
                    other_client = random.choice(community)

                call_data.append(generate_call(client, other_client, community_start_time, time_diff_minutes, community_min_duration, community_max_duration))

    # Optionally save to CSV
    if save_to_csv and output_file:
        with open(output_file, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['c1', 'c2', 'start', 'end'])
            writer.writerows(call_data)

    # Create and return a PySpark DataFrame
    call_df = spark.createDataFrame(call_data, schema=['c1', 'c2', 'start', 'end'])
    return call_df


def generate_call(client_a,
                  client_b,
                  start_time,
                  time_diff_minutes,
                  min_duration,
                  max_duration):
    # Random call start time within the time range
    random_minutes = random.randint(0, time_diff_minutes)
    call_start_time = start_time + timedelta(minutes=random_minutes)
    
    # Random call duration within the predefined range
    call_duration = random.randint(min_duration, max_duration)
    
    call_end_time = call_start_time + timedelta(minutes=call_duration)

    call_start_str = call_start_time.strftime('%y%m%d%H%M')
    call_end_str = call_end_time.strftime('%y%m%d%H%M')

    return [client_a, client_b, call_start_str, call_end_str]


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

    # Random number of communities between the provided range
    num_communities = random.randint(num_communities[0], num_communities[1])
    communities = [[] for _ in range(num_communities)]

    # Assign clients to communities ensuring some communities are larger
    for client in clients:
        # Randomly assign a client to a community, allowing for uneven distributions
        community_index = random.randint(0, num_communities - 1)
        communities[community_index].append(client)

    call_data = []

    for community in communities:
        # Introduce a community-level time shift to vary start times across communities
        community_time_shift = random.randint(0, time_diff_minutes // 5)  # Larger time shifts
        community_start_time = start_time + timedelta(minutes=community_time_shift)

        # Vary the duration range for each community to create different mean durations
        community_min_duration = random.randint(min_duration, max_duration // 2)
        community_max_duration = random.randint(max_duration // 2, max_duration)

        # Vary call frequency for communities: some are more active, others less
        community_call_frequency_range = (
            random.randint(call_frequency_range[0] // 2, call_frequency_range[0]),  # Less active
            random.randint(call_frequency_range[1], call_frequency_range[1] * 2)    # More active
        )

        if len(community) >= 2:
            # Pick two random clients from the community and generate a call
            first_client, second_client = random.sample(community, 2)
            call_data.append(generate_call(first_client, second_client, \
                                           community_start_time, time_diff_minutes, community_min_duration, community_max_duration))

        for client in community:
            # Determine how many calls this client makes, more varied across communities
            call_frequency = random.randint(community_call_frequency_range[0], community_call_frequency_range[1])

            for _ in range(call_frequency):
                other_client = random.choice(community)
                while other_client == client:
                    other_client = random.choice(community)

                call_data.append(generate_call(client, other_client, community_start_time, \
                                               time_diff_minutes, community_min_duration, community_max_duration))

    # Optionally save to CSV
    if save_to_csv and output_file:
        with open(output_file, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['c1', 'c2', 'start', 'end'])
            writer.writerows(call_data)

    # Create and return a PySpark DataFrame
    call_df = spark.createDataFrame(call_data, schema=['c1', 'c2', 'start', 'end'])
    return call_df


def generate_call(client_a,
                  client_b,
                  start_time,
                  time_diff_minutes,
                  min_duration,
                  max_duration):
    # Random call start time within the time range
    random_minutes = random.randint(0, time_diff_minutes)
    call_start_time = start_time + timedelta(minutes=random_minutes)

    # Vary call duration by adding randomness to the duration range
    call_duration = random.randint(min_duration, max_duration)

    call_end_time = call_start_time + timedelta(minutes=call_duration)

    # Randomly add variability to start and end times (like delays or extensions)
    additional_random_minutes = random.randint(-5, 10)  # Slight adjustments
    call_start_time += timedelta(minutes=additional_random_minutes)
    call_end_time += timedelta(minutes=random.randint(-2, 5))  # Smaller variation

    call_start_str = call_start_time.strftime('%y%m%d%H%M')
    call_end_str = call_end_time.strftime('%y%m%d%H%M')

    return [client_a, client_b, call_start_str, call_end_str]
