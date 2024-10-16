run_type = 'loca'

if run_type == 'local':
    import pandas as pd
    from create_comms_list import find_communities
    from evaluate_similarity import group_similar_communities

    call_data = pd.read_csv('data/test2.csv')
    comms = find_communities(call_data.values, method='dfs')
    print(f'{len(comms)} communities found.')
    group_similar_communities(comms, call_data.values)

else:
    from data_create import generate_synthetic_call_data
    from create_comms_list import find_communities
    from evaluate_similarity import group_similar_communities

    # Step 1: Generate synthetic call data
    call_df = generate_synthetic_call_data(
        num_clients=100,
        call_frequency_range=(1, 5),
        call_duration_range=(1, 60),
        num_communities=(6, 10),
        time_range=('2401010000', '2412312359'),
        save_to_csv=False
    )

    # Checking the DataFrame type
    print(type(call_df))  # Should be <class 'pyspark.sql.dataframe.DataFrame'>

    # Step 2: Find communities and assign community numbers (now returns DataFrame with `comm_number`)
    call_df_with_comm = find_communities(call_df, method='dfs')
    print(f'Total of {call_df_with_comm.select("comm_number").distinct().count()} communities found')
    print(type(call_df_with_comm))  # Should be <class 'pyspark.sql.dataframe.DataFrame'>
    call_df_with_comm.show(truncate=False)  # Display the DataFrame with community numbers

    # Step 3: Group similar communities (this step will still work with the community DataFrame)
    # groups = group_similar_communities(call_df_with_comm, similarity_threshold=0.8)
    # print(groups)
    # Optionally, if you decide to assign group numbers to communities after grouping:
    # call_df_with_groups = assign_group_numbers(call_df_with_comm, groups)
    # call_df_with_groups.show(truncate=False)  # Display the DataFrame with community and group numbers
