import pyspark.sql.functions as F # type: ignore
from pyspark.sql import DataFrame # type: ignore
import numpy as np
import spark # type: ignore
from pyspark.sql.functions import col # type: ignore

def calculate_community_metrics_1(community, call_df, logs=False):  ## This is experiment 1
    # Create a broadcast variable for the community to speed up the isin check
    community_broadcast = spark.sparkContext.broadcast(set(community))
    # Filter the DataFrame for calls within the community
    community_calls = call_df.filter(
        (call_df.c1.isin(community_broadcast.value)) & (call_df.c2.isin(community_broadcast.value))
    )


    # Cache the filtered DataFrame for faster repeated access
    community_calls.cache()

    # Calculate metrics in one go using Spark SQL functions
    metrics = community_calls.agg(
        F.count("*").alias("num_calls"),
        F.countDistinct(F.col("c1")).alias("num_clients"),  # Count distinct clients directly
        (F.avg(F.col("end") - F.col("start"))).alias("mean_duration"),
        (F.stddev(F.col("end") - F.col("start"))).alias("std_duration")
    ).first()

    num_calls = metrics.num_calls
    num_clients = metrics.num_clients
    mean_duration = metrics.mean_duration if metrics.mean_duration is not None else 0
    std_duration = metrics.std_duration if metrics.std_duration is not None else 0

    if logs:
        print("_____" * 10)
        print(f"Community: {community}")
        print(f"Number of Calls: {num_calls}")
        print(f"Number of Clients: {num_clients}")
        print(f"Mean Call Duration: {mean_duration}")
        print(f"Std Dev of Call Duration: {std_duration}")
        print("_____" * 10)

    return {
        'num_calls': num_calls,
        'num_clients': num_clients,
        'mean_duration': mean_duration,
        'std_duration': std_duration
    }

def calculate_community_metrics_2(community, call_df, logs=False):  ## This is experiment 2
    # Convert the community to a DataFrame and broadcast it
    community_df = spark.createDataFrame([(member,) for member in community], ['community'])
    community_broadcast = F.broadcast(community_df)

    # Join the call data with the community broadcast DataFrame
    community_calls = call_df.join(community_broadcast, (call_df.c1 == community_broadcast.community) | (call_df.c2 == community_broadcast.community))

    community_calls.cache()  # Cache the filtered DataFrame for better performance

    # Calculate metrics in one go
    metrics = community_calls.agg(
        F.count("*").alias("num_calls"),
        F.countDistinct(F.col("c1")).alias("num_clients"),
        (F.avg(F.col("end") - F.col("start"))).alias("mean_duration"),
        (F.stddev(F.col("end") - F.col("start"))).alias("std_duration")
    ).first()

    # Extract metrics values
    num_calls = metrics.num_calls
    num_clients = metrics.num_clients
    mean_duration = metrics.mean_duration if metrics.mean_duration is not None else 0
    std_duration = metrics.std_duration if metrics.std_duration is not None else 0

    if logs:
        print("_____" * 10)
        print(f"Community: {community}")
        print(f"Number of Calls: {num_calls}")
        print(f"Number of Clients: {num_clients}")
        print(f"Mean Call Duration: {mean_duration}")
        print(f"Std Dev of Call Duration: {std_duration}")
        print("_____" * 10)

    return {
        'num_calls': num_calls,
        'num_clients': num_clients,
        'mean_duration': mean_duration,
        'std_duration': std_duration
    }

def calculate_community_metrics_3(community, call_df, logs=False):  # This is experiment 3
    # Create a DataFrame for the community
    community_df = spark.createDataFrame([(c,) for c in community], ['community'])
    community_df = community_df.withColumn("community", F.col("community").cast("long"))

    # Perform broadcast join to filter calls within the community
    community_calls = call_df.join(F.broadcast(community_df), 
                                   (call_df.c1 == community_df.community) | 
                                   (call_df.c2 == community_df.community))

    # Cache the filtered DataFrame for faster repeated access
    community_calls.cache()

    # Calculate metrics in one go using Spark SQL functions
    metrics = community_calls.agg(
        F.count("*").alias("num_calls"),
        F.countDistinct(F.col("c1")).alias("num_clients"),  # Count distinct clients directly
        (F.avg(F.col("end") - F.col("start"))).alias("mean_duration"),
        (F.stddev(F.col("end") - F.col("start"))).alias("std_duration")
    ).first()

    num_calls = metrics.num_calls
    num_clients = metrics.num_clients
    mean_duration = metrics.mean_duration if metrics.mean_duration is not None else 0
    std_duration = metrics.std_duration if metrics.std_duration is not None else 0

    if logs:
        print("_____" * 10)
        print(f"Community: {community}")
        print(f"Number of Calls: {num_calls}")
        print(f"Number of Clients: {num_clients}")
        print(f"Mean Call Duration: {mean_duration}")
        print(f"Std Dev of Call Duration: {std_duration}")
        print("_____" * 10)

    return {
        'num_calls': num_calls,
        'num_clients': num_clients,
        'mean_duration': mean_duration,
        'std_duration': std_duration
    }

  
def calculate_community_metrics_4(comm_number, call_df, logs=False):  # This is experiment 4
    # Filter the DataFrame for calls within the community using comm_number
    # Use isin to check if comm_number is in the list
    community_calls = call_df.filter(F.col("comm_number").isin(comm_number)) 
                                                                              
    # Cache the filtered DataFrame for faster repeated access
    community_calls.cache()

    # Calculate metrics in one go using Spark SQL functions
    metrics = community_calls.agg(
        F.count("*").alias("num_calls"),
        F.countDistinct(F.col("c1")).alias("num_clients"),
        (F.avg(F.col("end") - F.col("start"))).alias("mean_duration"),
        (F.stddev(F.col("end") - F.col("start"))).alias("std_duration")
    ).first()

    num_calls = metrics.num_calls
    num_clients = metrics.num_clients
    mean_duration = metrics.mean_duration if metrics.mean_duration is not None else 0
    std_duration = metrics.std_duration if metrics.std_duration is not None else 0

    if logs:
        print("_____" * 10)
        print(f"Community Number: {comm_number}")
        print(f"Number of Calls: {num_calls}")
        print(f"Number of Clients: {num_clients}")
        print(f"Mean Call Duration: {mean_duration}")
        print(f"Std Dev of Call Duration: {std_duration}")
        print("_____" * 10)

    return {
        'num_calls': num_calls,
        'num_clients': num_clients,
        'mean_duration': mean_duration,
        'std_duration': std_duration
    }



def calculate_community_metrics(comm_number, call_df, logs=False):  # This is experiment 5
    # Filter the call data by the community number
    community_calls = call_df.filter(call_df.comm_number == comm_number)

    community_calls.cache()  # Cache the filtered DataFrame for better performance

    # Calculate metrics in one go
    metrics = community_calls.agg(
        F.count("*").alias("num_calls"),
        F.countDistinct(F.col("c1")).alias("num_clients"),
        (F.avg(F.col("end") - F.col("start"))).alias("mean_duration"),
        (F.stddev(F.col("end") - F.col("start"))).alias("std_duration")
    ).first()

    # Extract metrics values
    num_calls = metrics.num_calls
    num_clients = metrics.num_clients
    mean_duration = metrics.mean_duration if metrics.mean_duration is not None else 0
    std_duration = metrics.std_duration if metrics.std_duration is not None else 0

    if logs:
        print("_____" * 10)
        print(f"Community Number: {comm_number}")
        print(f"Number of Calls: {num_calls}")
        print(f"Number of Clients: {num_clients}")
        print(f"Mean Call Duration: {mean_duration}")
        print(f"Std Dev of Call Duration: {std_duration}")
        print("_____" * 10)

    return {
        'num_calls': num_calls,
        'num_clients': num_clients,
        'mean_duration': mean_duration,
        'std_duration': std_duration
    }


def compare_communities(metrics1, metrics2, name1, name2, threshold=0.1, logs=False):
    print(f'Comparison of {name1} and {name2}:') if logs else None

    similarities = {}
    for key in ['num_calls', 'num_clients', 'mean_duration', 'std_duration']:
        sim = np.abs(metrics1[key] - metrics2[key]) / np.max([metrics1[key], metrics2[key], 1])
        similarities[key] = 1 - sim
        print(f'  {key.replace("_", " ").title()} Similarity: {similarities[key]:.2f}') if logs else None

    combined_similarity = np.mean(list(similarities.values()))
    print(f'  Combined Similarity Score: {combined_similarity:.2f}') if logs else None

    return combined_similarity >= threshold

def group_similar_communities(communities, call_df, similarity_threshold=0.8):
    # Calculate metrics for all communities in parallel
    community_metrics = [calculate_community_metrics(community, call_df) for community in communities]

    # Dictionary to track the groups
    groups = []
    visited = [False] * len(communities)
    community_names = [f'c{i + 1}' for i in range(len(communities))]

    for i in range(len(communities)):
        if visited[i]:
            continue
        current_group = [community_names[i]]
        visited[i] = True

        for j in range(i + 1, len(communities)):
            if visited[j]:
                continue

            # Compare the communities i and j
            if compare_communities(community_metrics[i], community_metrics[j], community_names[i], community_names[j], threshold=similarity_threshold):
                current_group.append(community_names[j])
                visited[j] = True

        groups.append(current_group)

    # Print the groups of similar communities
    for idx, group in enumerate(groups, 1):
        print(f"Group {idx}: Communities {', '.join(group)}")

    return groups

def group_similar_communities(call_df, similarity_threshold=0.8):
    # Get the distinct community numbers from the DataFrame
    community_numbers = call_df.select("comm_number").distinct().rdd.flatMap(lambda x: x).collect()
    
    # Calculate metrics for all communities
    community_metrics = {comm_num: calculate_community_metrics(comm_num, call_df) for comm_num in community_numbers}

    # Dictionary to track the groups
    groups = []
    visited = {comm_num: False for comm_num in community_numbers}
    community_names = [f'c{comm_num}' for comm_num in community_numbers]

    for i, comm_num_i in enumerate(community_numbers):
        if visited[comm_num_i]:
            continue
        current_group = [community_names[i]]
        visited[comm_num_i] = True

        for j, comm_num_j in enumerate(community_numbers[i+1:], i+1):
            if visited[comm_num_j]:
                continue

            # Compare the communities i and j
            if compare_communities(community_metrics[comm_num_i], community_metrics[comm_num_j], community_names[i], community_names[j], threshold=similarity_threshold):
                current_group.append(community_names[j])
                visited[comm_num_j] = True

        groups.append(current_group)

    # Print the groups of similar communities
    for idx, group in enumerate(groups, 1):
        print(f"Group {idx}: Communities {', '.join(group)}")

    return groups


def assign_group_numbers(call_df: DataFrame, groups):
    # Create a dictionary mapping each community number to its group number
    comm_to_group = {}
    for group_num, group in enumerate(groups, 1):
        for comm_name in group:
            comm_num = int(comm_name[1:])  # Extract the community number from 'c1', 'c2', etc.
            comm_to_group[comm_num] = group_num  # Assign group number to each community

    # Create a mapping expression to assign group numbers in the DataFrame
    mapping_expr = F.create_map([F.lit(x) for item in comm_to_group.items() for x in item])

    # Add the `group_number` column to the DataFrame by mapping from `comm_number`
    updated_df = call_df.withColumn("group_number", mapping_expr.getItem(col("comm_number")))

    return updated_df
