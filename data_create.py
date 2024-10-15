import csv
import random
from datetime import datetime, timedelta
from itertools import cycle

def generate_synthetic_call_data(num_clients,
                                   call_frequency_range,  # Changed to a range
                                   call_duration_range,
                                   num_communities,
                                   time_range,
                                   output_file,
                                   save_to_csv=False):
    """
    Generate synthetic phone call data with multiple communities and save it to a CSV file.

    Parameters:
    - num_clients (int): The total number of unique clients in the dataset.
    - call_frequency_range (tuple): A tuple containing (min_frequency, max_frequency) for call frequencies.
    - call_duration_range (tuple): A tuple containing (min_duration, max_duration) in minutes for call durations.
    - time_range (tuple): A tuple containing (start_time, end_time) in the format 'YYMMDDHHMM' for generating call times.
    - output_file (str): The path to the output CSV file where the data will be saved.

    Returns:
    - call_data (list): A list of generated call records, each represented as a tuple (c1, c2, start, end).
    """
    clients = list(range(1, num_clients + 1))
    min_duration, max_duration = call_duration_range
    start_time_str, end_time_str = time_range

    start_time = datetime.strptime(start_time_str, '%y%m%d%H%M')
    end_time = datetime.strptime(end_time_str, '%y%m%d%H%M')

    # Precompute time difference in minutes
    time_diff_minutes = int((end_time - start_time).total_seconds() / 60)

    # Step 1: Randomly determine the number of communities
    num_communities = random.randint(num_communities[0], num_communities[1])  # Random number of communities
    communities = [[] for _ in range(num_communities)]  # Create a list for communities

    # Step 2: Assign clients to communities using round-robin
    community_iter = cycle(communities)
    for client in clients:
        next(community_iter).append(client)

    call_data = []  # Initialize a list to store call records

    # Step 3: Generate calls
    for community in communities:
        # Establish initial connection between the first two clients
        if len(community) >= 2:
            first_client, second_client = random.sample(community, 2)
            call_data.append(generate_call(first_client, second_client, start_time, time_diff_minutes, min_duration, max_duration))

        # Generate calls for each client in the community
        for client in community:
            # Determine a random number of calls for this client within a specified range
            call_frequency = random.randint(call_frequency_range[0], call_frequency_range[1])
            
            for _ in range(call_frequency):
                # Efficient random selection of another client in the same community
                other_client = random.choice(community)
                while other_client == client:
                    other_client = random.choice(community)

                call_data.append(generate_call(client, other_client, start_time, time_diff_minutes, min_duration, max_duration))

    if save_to_csv:
        with open(output_file, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['c1', 'c2', 'start', 'end'])
            writer.writerows(call_data)  # Write all records at once

    return call_data  # Return the generated call data

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

# Example usage
if __name__ == "__main__":
    call_data = generate_synthetic_call_data(
        num_clients=100,
        call_frequency_range=(1, 5),  # Random calls between 1 and 5
        call_duration_range=(1, 60),  # Calls between 1 and 60 minutes
        num_communities=(3, 6),  # Random communities between 3 and 6
        time_range=('2408010000', '2408012359'),
        output_file='synthetic_call_data.csv',
        save_to_csv=True
    )
