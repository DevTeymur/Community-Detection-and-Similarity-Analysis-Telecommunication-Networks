run_type = 'loca'

if run_type == 'local':
    import pandas as pd
    from find_similar_comms import find_communities
    from evaluate_similarity import group_similar_communities

    call_data = pd.read_csv('data/test2.csv')
    comms = find_communities(call_data.values, method='dfs')
    print(f'{len(comms)} communities found.')
    group_similar_communities(comms, call_data.values)

else:
    from data_create import generate_synthetic_call_data
    from find_similar_comms import find_communities
    from evaluate_similarity import group_similar_communities

    num_clients = 10
    call_frequency = 5
    call_duration_range = (100, 500)
    num_communities = (4, 6)
    time_range = ('2401010000', '2412312359')
    output_file = f'data/{num_clients}.csv'

    # call_data = generate_synthetic_call_data(
    #     num_clients=num_clients,
    #     call_frequency=call_frequency,
    #     call_duration_range=call_duration_range,
    #     num_communities=num_communities,
    #     time_range=time_range,
    #     output_file=output_file,
    #     save_to_csv=False,
    # )

    call_data = generate_synthetic_call_data(
        num_clients=100,
        call_frequency_range=(1, 5),  # Random calls between 1 and 5
        call_duration_range=(29, 200),  # Calls between 1 and 60 minutes
        num_communities=(4, 6),  # Random communities between 3 and 6
        time_range=('2408010000', '2408012359'),
        output_file='synthetic_call_data.csv',
        save_to_csv=False
    )

    comms = find_communities(call_data, method='dfs')
    print(f'{len(comms)} communities found.')

    group_similar_communities(comms, call_data)


