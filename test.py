import random
import csv
from datetime import datetime, timedelta
from collections import defaultdict


def generate_synthetic_call_data(num_clients, 
                                 call_frequency, 
                                 call_duration_range, 
                                 time_range, 
                                 output_file, 
                                 ):
    clients = list(range(1, num_clients + 1))
    min_duration, max_duration = call_duration_range
    start_time_str, end_time_str = time_range

    start_time = datetime.strptime(start_time_str, '%y%m%d%H%M')
    end_time = datetime.strptime(end_time_str, '%y%m%d%H%M')

    # Step 1: Randomly determine the number of communities
    num_communities = random.randint(3, 10)  # Random number of communities
    communities = [[] for _ in range(num_communities)]  # Create a list for communities

    # Step 2: Assign clients to communities
    random.shuffle(clients)  # Shuffle clients
    for i, client in enumerate(clients):
        community_id = i % num_communities  # Assign clients to communities in a round-robin fashion
        communities[community_id].append(client)

    # Step 3: Generate calls
    with open(output_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['c1', 'c2', 'start', 'end'])

        for community in communities:
            for client in community:
                for _ in range(call_frequency):
                    other_client = random.choice([c for c in community if c != client])
                    time_diff = end_time - start_time
                    random_minutes = random.randint(0, int(time_diff.total_seconds() / 60))
                    call_start_time = start_time + timedelta(minutes=random_minutes)
                    call_duration = random.randint(min_duration, max_duration)
                    call_end_time = call_start_time + timedelta(minutes=call_duration)

                    call_start_str = call_start_time.strftime('%y%m%d%H%M')
                    call_end_str = call_end_time.strftime('%y%m%d%H%M')

                    writer.writerow([client, other_client, call_start_str, call_end_str])

    print(f"Synthetic call data generated and saved to {output_file}")

# Example usage
num_clients = 700
call_frequency = 5
call_duration_range = (5, 500)
time_range = ('2401010000', '2412312359')
output_file = f'data/{num_clients}.csv'

generate_synthetic_call_data(
    num_clients=num_clients,
    call_frequency=call_frequency,
    call_duration_range=call_duration_range,
    time_range=time_range,
    output_file=output_file
)


from find_similar_comms import find_communities
import pandas as pd
df = pd.read_csv(output_file)
communities = find_communities(df.values, method='dfs')

print(communities)
print(f'{len(communities)} communities found.')