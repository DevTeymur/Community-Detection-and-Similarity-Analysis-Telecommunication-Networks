import csv
import random
from datetime import datetime, timedelta
from itertools import cycle

def generate_synthetic_call_data(num_clients,
                                call_frequency, 
                                call_duration_range, 
                                time_range, 
                                output_file, 
                                save_to_csv=False):
    """
    Generate synthetic phone call data with multiple communities and save it to a CSV file.

    Parameters:
    - num_clients (int): The total number of unique clients in the dataset.
    - call_frequency (int): The average number of calls each client makes.
    - call_duration_range (tuple): A tuple containing (min_duration, max_duration) in minutes for call durations.
    - time_range (tuple): A tuple containing (start_time, end_time) in the format 'YYMMDDHHMM' for generating call times.
    - output_file (str): The path to the output CSV file where the data will be saved.

    Returns:
    - call_data (list): A list of generated call records, each represented as a tuple (c1, c2, start, end).

    The function randomly determines the number of communities and assigns clients to these communities.
    It generates call data such that each client makes calls to other clients within the same community.
    """
    clients = list(range(1, num_clients + 1))
    min_duration, max_duration = call_duration_range
    start_time_str, end_time_str = time_range

    start_time = datetime.strptime(start_time_str, '%y%m%d%H%M')
    end_time = datetime.strptime(end_time_str, '%y%m%d%H%M')

    # Precompute time difference in minutes
    time_diff_minutes = int((end_time - start_time).total_seconds() / 60)

    # Step 1: Randomly determine the number of communities
    num_communities = random.randint(3, 10)  # Random number of communities
    communities = [[] for _ in range(num_communities)]  # Create a list for communities

    # Step 2: Assign clients to communities using round-robin
    community_iter = cycle(communities)
    for client in clients:
        next(community_iter).append(client)

    call_data = []  # Initialize a list to store call records

    # Step 3: Generate calls
    for community in communities:
        for client in community:
            for _ in range(call_frequency):
                # Efficient random selection of another client in the same community
                other_client = random.choice(community)
                while other_client == client:
                    other_client = random.choice(community)

                # Random call start time and duration
                random_minutes = random.randint(0, time_diff_minutes)
                call_start_time = start_time + timedelta(minutes=random_minutes)
                call_duration = random.randint(min_duration, max_duration)
                call_end_time = call_start_time + timedelta(minutes=call_duration)

                call_start_str = call_start_time.strftime('%y%m%d%H%M')
                call_end_str = call_end_time.strftime('%y%m%d%H%M')

                # Store the record in memory
                call_data.append([client, other_client, call_start_str, call_end_str])

    # Write to CSV in a single batch after generating all data
    if save_to_csv:
        with open(output_file, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['c1', 'c2', 'start', 'end'])
            writer.writerows(call_data)  # Write all records at once

    return call_data  # Return the generated call data
