import numpy as np
import pandas as pd
import time
import itertools

from data_create import generate_synthetic_call_data
from find_similar_comms import find_communities

np.random.seed(42)

num_clients = np.arange(5000, 200000, 5000)
call_frequency = np.arange(20, 30, 5)
methods = ['dfs', 'bfs']
call_duration_range = (5, 500)
time_range = ('2401010000', '2412312359')
output_file = f'data/ptest.csv'

combinations = itertools.product(num_clients, call_frequency)

results = {
    'num_clients': [],
    'call_freq': [],
    'method': [],
    'time_secs': [],
    'num_comms': [],
}


for nc, cf in combinations:
    temp_data = generate_synthetic_call_data(
        num_clients=nc,
        call_frequency=cf,
        call_duration_range=call_duration_range,
        time_range=time_range,
        output_file=output_file
    )
    for method in methods:
        start_time = time.time()
        print(f"num_clients: {nc}, call_frequency: {cf}, method: {method}")
        communities = find_communities(temp_data, method=method)
        end_time = time.time()
        time_secs = round(end_time - start_time, 3)
        print(f"Execution time: {time_secs} seconds")
        print(f"Number of communities found: {len(communities)}")
        results['num_clients'].append(nc)
        results['call_freq'].append(cf)
        results['method'].append(method)
        results['time_secs'].append(time_secs)
        results['num_comms'].append(len(communities))
        print("================================")


df = pd.DataFrame(results)
df.to_csv('data/performance_results.csv', index=False)

# Plotting of bfs and dfs results for num of clients and execution time 2 lines in one grapg
import matplotlib.pyplot as plt

df = df.sort_values(by='num_clients')

fig, ax = plt.subplots(figsize=(10, 6))

for method in df['method'].unique():
    temp_df = df[df['method'] == method]
    ax.plot(temp_df['num_clients'], temp_df['time_secs'], label=method)

ax.set_xlabel('Number of Clients')
ax.set_ylabel('Execution Time (seconds)')
ax.set_title('Performance Comparison of DFS and BFS')
ax.legend()
plt.show()


