from data_create import generate_synthetic_call_data

num_clients = 5000
call_frequency = 5
call_duration_range = (5, 500)
time_range = ('2401010000', '2412312359')
output_file = f'data/{num_clients}.csv'

call_data = generate_synthetic_call_data(
    num_clients=num_clients,
    call_frequency=call_frequency,
    call_duration_range=call_duration_range,
    time_range=time_range,
    output_file=output_file
)

from find_similar_comms import find_communities

comms = find_communities(call_data, method='dfs')
print(comms)
print(f'{len(comms)} communities found.')