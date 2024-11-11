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



# 18 oct old code
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


def euclidean(vec1, vec2):
    # Calculate squared differences element-wise using zip and list comprehension
    squared_diffs = [(float(a) - float(b))**2 for a, b in zip(vec1, vec2)]
    return np.sum(squared_diffs) ** 0.25
    

def compute_similarity(num_clients1, mean_duration1, stddev_duration1, total_duration1,
                       num_clients2, mean_duration2, stddev_duration2, total_duration2):
    vec1 = [num_clients1, mean_duration1, stddev_duration1, total_duration1]
    vec2 = [num_clients2, mean_duration2, stddev_duration2, total_duration2]
    # Call the modified euclidean function
    return euclidean(vec1, vec2)

def group_similar_communities(call_df, client_to_community_rdd):
    # Step 1: Convert RDD to DataFrame and join with the original call data
    comm_df = client_to_community_rdd.toDF(["client", "community_number"])

    enriched_df = call_df.join(comm_df, call_df["c1"] == comm_df["client"], "left").drop("client")
    enriched_df = enriched_df.withColumnRenamed("community_number", "comm_number_c1")
    enriched_df = enriched_df.join(comm_df, call_df["c2"] == comm_df["client"], "left").drop("client")
    enriched_df = enriched_df.withColumnRenamed("community_number", "comm_number_c2")

    enriched_df = enriched_df.withColumn(
        "comm_number", F.coalesce(F.col("comm_number_c1"), F.col("comm_number_c2"))
    ).drop("comm_number_c1", "comm_number_c2")

    # Calculate duration column from start and end columns
    enriched_df = enriched_df.withColumn("duration", enriched_df["end"] - enriched_df["start"])
    print('Step 1 done')

    # Step 2: Calculate community metrics
    community_metrics = (
        enriched_df.groupBy("comm_number")
        .agg(
            F.countDistinct("c1").alias("num_clients"),
            F.mean("duration").alias("mean_duration"),
            F.stddev("duration").alias("stddev_duration"),
            F.sum("duration").alias("total_duration")
        )
    )
    print('Step 2 done')

    # Step 3: Compare communities using Euclidean distance
    community_data = community_metrics.collect()

    similar_communities = []
    threshold = 0.1

    for i in range(len(community_data)):
        for j in range(i + 1, len(community_data)):
            comm1 = community_data[i]
            comm2 = community_data[j]

            # Extract attributes from Row objects for similarity calculation
            similarity_value = compute_similarity(
                comm1.num_clients, comm1.mean_duration, comm1.stddev_duration, comm1.total_duration,
                comm2.num_clients, comm2.mean_duration, comm2.stddev_duration, comm2.total_duration
            )

            print(f"Similarity between {comm1.comm_number} and {comm2.comm_number}: {similarity_value}")

            if similarity_value < threshold:
                similar_communities.append((comm1.comm_number, comm2.comm_number))

    print('Step 3 done')

    # Step 4: Return grouped communities
    similar_df = spark.createDataFrame(similar_communities, ["comm1", "comm2"])
    print('Step 4 done')

    return similar_df


def normalized_difference(val1, val2):
    # Convert val1 and val2 to float explicitly
    val1 = float(val1)
    val2 = float(val2)

    # Pass values as a list to np.max
    norm_diff = np.abs(val1 - val2) / np.max([val1, val2]) 
    # print(f'Normalized diff = {norm_diff}')
    return norm_diff

def compute_similarity(num_clients1, mean_duration1, stddev_duration1, total_duration1,
                       num_clients2, mean_duration2, stddev_duration2, total_duration2):
    # Normalize each metric
    norm_num_clients = normalized_difference(num_clients1, num_clients2)
    norm_mean_duration = normalized_difference(mean_duration1, mean_duration2)
    norm_stddev_duration = normalized_difference(stddev_duration1, stddev_duration2)
    norm_total_duration = normalized_difference(total_duration1, total_duration2)
    # print(norm_num_clients, norm_mean_duration, norm_stddev_duration, norm_total_duration)
    # Create normalized vectors for similarity computation
    vec1 = [norm_num_clients, norm_mean_duration, norm_stddev_duration, norm_total_duration]
    eucledian_dist = np.sum(val ** 2 for val in vec1) ** 0.5  # Euclidean distance formula
    # print(f'Euc dist = {eucledian_dist}')
    # Calculate Euclidean distance
    return eucledian_dist

def group_similar_communities(call_df, client_to_community_rdd):
    # Step 1: Convert RDD to DataFrame and join with the original call data
    comm_df = client_to_community_rdd.toDF(["client", "community_number"])

    enriched_df = call_df.join(comm_df, call_df["c1"] == comm_df["client"], "left").drop("client")
    enriched_df = enriched_df.withColumnRenamed("community_number", "comm_number_c1")
    enriched_df = enriched_df.join(comm_df, call_df["c2"] == comm_df["client"], "left").drop("client")
    enriched_df = enriched_df.withColumnRenamed("community_number", "comm_number_c2")

    enriched_df = enriched_df.withColumn(
        "comm_number", F.coalesce(F.col("comm_number_c1"), F.col("comm_number_c2"))
    ).drop("comm_number_c1", "comm_number_c2")

    # Calculate duration column from start and end columns (assuming seconds, convert to minutes if needed)
    enriched_df = enriched_df.withColumn("duration", (enriched_df["end"] - enriched_df["start"]) / 60)
    
    # Step 2: Calculate community metrics
    community_metrics = (
        enriched_df.groupBy("comm_number")
        .agg(
            F.countDistinct("c1").alias("num_clients"),
            F.mean("duration").alias("mean_duration"),
            F.stddev("duration").alias("stddev_duration"),
            F.sum("duration").alias("total_duration")
        )
    )

    # Step 3: Compare communities using the updated similarity formula
    community_data = community_metrics.collect()

    similar_communities = []
    threshold = 0.1  # Set threshold to 0.1 (similarity scores between 0 and 1)

    # Group structure
    groups = []
    community_group_map = {}

    for i in range(len(community_data)):
        for j in range(i + 1, len(community_data)):
            comm1 = community_data[i]
            comm2 = community_data[j]

            # Calculate similarity using normalized metrics
            similarity_value = compute_similarity(
                comm1.num_clients, comm1.mean_duration, comm1.stddev_duration, comm1.total_duration,
                comm2.num_clients, comm2.mean_duration, comm2.stddev_duration, comm2.total_duration
            )

            # Print informative details about the comparison
            print(f"Comparing Community {comm1.comm_number} and Community {comm2.comm_number}:")
            print(f"  Normalized client count difference: {similarity_value}")
            print(f"  Euclidean distance (similarity score): {similarity_value}\n")

            if similarity_value < threshold:
                # Add communities to the same group
                group_found = False
                for group in groups:
                    if comm1.comm_number in group or comm2.comm_number in group:
                        group.update([comm1.comm_number, comm2.comm_number])
                        group_found = True
                        break
                if not group_found:
                    groups.append(set([comm1.comm_number, comm2.comm_number]))

    # Step 4: Format the results as desired
    for idx, group in enumerate(groups):
        group_str = ', '.join(sorted([f"c{g}" for g in group]))
        print(f"group{idx + 1}: {group_str}")

    return groups



def z_score_normalization(value, mean, stddev):
    """Normalize value using Z-score."""
    return (value - mean) / stddev if stddev != 0 else 0

def compute_similarity_v2(num_clients1, mean_duration1, stddev_duration1, total_duration1,
                           num_clients2, mean_duration2, stddev_duration2, total_duration2, logs=False) -> float:
    """Compute the similarity score using enhanced metrics."""
    print("Calculating similarity between communities...") if logs else None
    
    # Compute mean and stddev for normalization
    metrics_mean = {
        "num_clients": (num_clients1 + num_clients2) / 2,
        "mean_duration": (mean_duration1 + mean_duration2) / 2,
        "stddev_duration": (stddev_duration1 + stddev_duration2) / 2,
        "total_duration": (total_duration1 + total_duration2) / 2
    }
    
    metrics_stddev = {
        "num_clients": np.std([num_clients1, num_clients2]),
        "mean_duration": np.std([mean_duration1, mean_duration2]),
        "stddev_duration": np.std([stddev_duration1, stddev_duration2]),
        "total_duration": np.std([total_duration1, total_duration2])
    }

    print("Mean metrics calculated:") if logs else None
    print(f" - Number of clients (mean): {metrics_mean['num_clients']}") if logs else None
    print(f" - Mean duration (mean): {metrics_mean['mean_duration']}") if logs else None
    print(f" - Standard deviation of duration (mean): {metrics_mean['stddev_duration']}") if logs else None
    print(f" - Total duration (mean): {metrics_mean['total_duration']}") if logs else None

    print("\nStandard deviation metrics calculated:") if logs else None
    print(f" - Number of clients (stddev): {metrics_stddev['num_clients']}") if logs else None
    print(f" - Mean duration (stddev): {metrics_stddev['mean_duration']}") if logs else None
    print(f" - Standard deviation of duration (stddev): {metrics_stddev['stddev_duration']}") if logs else None
    print(f" - Total duration (stddev): {metrics_stddev['total_duration']}") if logs else None

    # Z-score normalization
    z_num_clients = z_score_normalization(num_clients1, metrics_mean["num_clients"], metrics_stddev["num_clients"])
    z_mean_duration = z_score_normalization(mean_duration1, metrics_mean["mean_duration"], metrics_stddev["mean_duration"])
    z_stddev_duration = z_score_normalization(stddev_duration1, metrics_mean["stddev_duration"], metrics_stddev["stddev_duration"])
    z_total_duration = z_score_normalization(total_duration1, metrics_mean["total_duration"], metrics_stddev["total_duration"])

    print("\nZ-score normalization results:") if logs else None
    print(f" - Z-score for number of clients: {z_num_clients}") if logs else None
    print(f" - Z-score for mean duration: {z_mean_duration}") if logs else None
    print(f" - Z-score for standard deviation of duration: {z_stddev_duration}") if logs else None
    print(f" - Z-score for total duration: {z_total_duration}") if logs else None

    # Compute similarity score (you can modify weights based on importance)
    similarity_score = 1 - (0.25 * (np.abs(z_num_clients) + np.abs(z_mean_duration) + 
                                      np.abs(z_stddev_duration) + np.abs(z_total_duration)))
    
    print(f"\nCalculated similarity score: {similarity_score}") if logs else None

    return similarity_score


def group_similar_communities_v2(call_df: DataFrame, client_to_community_rdd) -> DataFrame:
    """Group similar communities with adaptive thresholding."""

    # Step 1: Convert RDD to DataFrame and join with the original call data
    comm_df = client_to_community_rdd.toDF(["client", "community_number"])

    enriched_df = call_df.join(comm_df, call_df["c1"] == comm_df["client"], "left").drop("client")
    enriched_df = enriched_df.withColumnRenamed("community_number", "comm_number_c1")
    enriched_df = enriched_df.join(comm_df, call_df["c2"] == comm_df["client"], "left").drop("client")
    enriched_df = enriched_df.withColumnRenamed("community_number", "comm_number_c2")

    enriched_df = enriched_df.withColumn(
        "comm_number", F.coalesce(F.col("comm_number_c1"), F.col("comm_number_c2"))
    ).drop("comm_number_c1", "comm_number_c2")

    # Convert durations to minutes (assuming 'start' and 'end' are in seconds)
    enriched_df = enriched_df.withColumn("duration", (enriched_df["end"] - enriched_df["start"]) / 60.0)

    # Step 2: Calculate community metrics
    community_metrics = (
        enriched_df.groupBy("comm_number")
        .agg(
            F.countDistinct("c1").alias("num_clients"),
            F.mean("duration").alias("mean_duration"),
            F.stddev("duration").alias("stddev_duration"),
            F.sum("duration").alias("total_duration")
        )
    )

    # Check if community_metrics is empty
    if community_metrics.count() == 0:
        print("No community metrics calculated. Please check the input data.")
        return spark.createDataFrame([], StructType([]))  # Return an empty DataFrame with no schema

    # Step 3: Compare communities using similarity formula
    community_data = community_metrics.collect()
    print("Community metrics collected. Number of communities:", len(community_data))

    # Compute similarity scores and gather statistics
    similarity_scores = []
    for i in range(len(community_data)):
        for j in range(i + 1, len(community_data)):
            comm1 = community_data[i]
            comm2 = community_data[j]

            similarity_value = compute_similarity_v2(
                comm1.num_clients, comm1.mean_duration, comm1.stddev_duration, comm1.total_duration,
                comm2.num_clients, comm2.mean_duration, comm2.stddev_duration, comm2.total_duration
            )
            similarity_scores.append(similarity_value)

    # Set dynamic threshold (e.g., 75th percentile)
    if similarity_scores:
        THRESHOLD = np.percentile(similarity_scores, 75)
        print(f"Dynamic similarity threshold set at {THRESHOLD:.2f}.")
    else:
        print("No similarity scores calculated.")
        return spark.createDataFrame([], StructType([]))  # Return an empty DataFrame

    similar_communities = []
    assigned_groups = {}

    for i in range(len(community_data)):
        for j in range(i + 1, len(community_data)):
            comm1 = community_data[i]
            comm2 = community_data[j]

            # Calculate similarity using the combined similarity score
            similarity_value = compute_similarity_v2(
                comm1.num_clients, comm1.mean_duration, comm1.stddev_duration, comm1.total_duration,
                comm2.num_clients, comm2.mean_duration, comm2.stddev_duration, comm2.total_duration
            )

            # Store similarity results
            similar_communities.append((comm1.comm_number, comm2.comm_number, similarity_value))

            if similarity_value >= THRESHOLD:
                # Ensure communities only belong to one group
                group1 = assigned_groups.get(comm1.comm_number, set())
                group2 = assigned_groups.get(comm2.comm_number, set())

                if not group1 and not group2:
                    # Create a new group for both communities
                    new_group = {comm1.comm_number, comm2.comm_number}
                    assigned_groups[comm1.comm_number] = new_group
                    assigned_groups[comm2.comm_number] = new_group
                elif group1 and not group2:
                    # Add comm2 to comm1's group
                    group1.add(comm2.comm_number)
                    assigned_groups[comm2.comm_number] = group1
                elif group2 and not group1:
                    # Add comm1 to comm2's group
                    group2.add(comm1.comm_number)
                    assigned_groups[comm1.comm_number] = group2
                else:
                    # Merge the two groups
                    merged_group = group1.union(group2)
                    for comm in merged_group:
                        assigned_groups[comm] = merged_group

    # Step 4: Group similar communities and print final groups
    final_groups = {}
    for comm, group in assigned_groups.items():
        group_key = tuple(sorted(group))  # Use sorted tuple as a key for grouping
        if group_key not in final_groups:
            final_groups[group_key] = group_key

    # Ensure that ungrouped communities are also included in the final output
    ungrouped_communities = set(comm.comm_number for comm in community_data) - set(assigned_groups.keys())
    for comm in ungrouped_communities:
        final_groups[frozenset({comm})] = frozenset({comm})

    # Print final unique groups
    gr_num = 1
    for group_key in final_groups:
        group_members = ", ".join(str(comm) for comm in group_key)
        print(f"Group {gr_num}: {group_members}")
        gr_num += 1 

    group_data = [(list(group),) for group in final_groups.values()] 
    # Define the schema explicitly
    schema = StructType([StructField("grouped_communities", ArrayType(IntegerType()), True)])
    return spark.createDataFrame(group_data, schema=schema)
