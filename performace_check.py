import numpy as np
import pandas as pd
import time

from data_create import generate_synthetic_call_data
from create_comms_list import find_communities
from evaluate_similarity import group_similar_communities

np.random.seed(42)

num_clients = np.arange(10_000, 200_000, 50_000)
call_frequency = (1, 3)
call_duration_range = (2000, 50000)
num_communities_range = (3, 5)  # Define the range for the number of communities
time_range = ('2401010000', '2412312359')
output_file = 'data/ptest.csv'

results = {
    'num_clients': [],
    'call_freq': [],
    'time_secs_data_creation': [],
    'time_secs_community_finding': [],
    'time_secs_similarity_finding': [],
    'time_secs_total': [],
    'num_comms': [],
}

for nc in num_clients:
    # Start time for data creation
    start_time_data_creation = time.time()
    call_df = generate_synthetic_call_data(
        num_clients=nc,
        call_frequency_range=call_frequency,
        call_duration_range=call_duration_range,
        num_communities=num_communities_range,  # Pass the randomly selected number of communities
        time_range=time_range,
        output_file=output_file,
        save_to_csv=False
    )

    end_time_data_creation = time.time()
    time_secs_data_creation = np.round(end_time_data_creation - start_time_data_creation, 3)
    print(f"Execution time for data creation: {time_secs_data_creation} seconds")

    # Start time for community finding
    start_time_community_finding = time.time()
    print(f"num_clients: {nc}, method: dfs")
    communities = find_communities(call_df, method='dfs')
    end_time_community_finding = time.time()
    time_secs_community_finding = np.round(end_time_community_finding - start_time_community_finding, 3)
    print(f"Number of communities found: {len(communities)}")
    print(f"Execution time for community finding: {time_secs_community_finding} seconds")


    # Start time for grouping similar communities
    start_time_similarity_finding = time.time()
    # call_df_with_comm_number = assign_community_numbers(call_df, communities)
    groups = group_similar_communities(communities, call_df)
    # groups = group_similar_communities(communities, call_df_with_comm_number)

    end_time_similarity_finding = time.time()
    time_secs_similarity_finding = np.round(end_time_similarity_finding - start_time_similarity_finding, 3)
    print(f"Execution time for grouping similar communities: {time_secs_similarity_finding} seconds")

    # Calculate total time
    total_time = np.round(
        time_secs_data_creation + time_secs_community_finding + time_secs_similarity_finding, 3
    )
    print(f"Total execution time: {total_time} seconds")


    # Store the results
    results['num_clients'].append(nc)
    results['call_freq'].append(call_frequency)  # Add call frequency as a fixed value
    results['time_secs_data_creation'].append(time_secs_data_creation)
    results['time_secs_community_finding'].append(time_secs_community_finding)
    results['time_secs_similarity_finding'].append(time_secs_similarity_finding)
    results['time_secs_total'].append(total_time)
    results['num_comms'].append(len(communities))
    print("================================")

# Convert results to DataFrame and save to CSV
result_df = pd.DataFrame(results)
result_df.to_csv('data/performance_results.csv', index=False)


import matplotlib.pyplot as plt

# Sort the DataFrame by number of clients
df = result_df.sort_values(by='num_clients')

# Create the plot
fig, ax = plt.subplots(figsize=(10, 6))

# Plot different execution times
ax.plot(df['num_clients'], df['time_secs_data_creation'], label='Data Creation')
ax.plot(df['num_clients'], df['time_secs_community_finding'], label='Community Finding')
ax.plot(df['num_clients'], df['time_secs_similarity_finding'], label='Similarity Finding')
ax.plot(df['num_clients'], df['time_secs_total'], label='Total Execution Time')

# Set labels and title
ax.set_xlabel('Number of Clients')
ax.set_ylabel('Execution Time (seconds)')
ax.set_title('Performance Comparison of Execution Times')
ax.legend()

# Display the plot
plt.show()
