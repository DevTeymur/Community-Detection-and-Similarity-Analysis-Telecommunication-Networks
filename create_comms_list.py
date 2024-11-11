from collections import defaultdict
from pyspark.sql import DataFrame # type: ignore
import pyspark.sql.functions as F # type: ignore
import spark # type: ignore

def dfs_recursive(client, community, visited, graph):
    visited.add(client)
    community.append(int(client))
    for neighbor in graph[client]:
        if neighbor not in visited:
            dfs_recursive(neighbor, community, visited, graph)

def dfs(client, community, visited, graph):
    stack = [client]
    visited.add(client)

    while stack:
        node = stack.pop()
        community.append(int(node))
        for neighbor in graph[node]:
            if neighbor not in visited:
                visited.add(neighbor)
                stack.append(neighbor)

def bfs(client, community, visited, graph):
    visited.add(client)
    queue = [client]

    while queue:
        node = queue.pop(0)
        community.append(int(node))
        for neighbor in graph[node]:
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append(neighbor)

def find_communities_old(call_df: DataFrame, method='dfs'):
    # Step 1: Build the graph from call data
    graph = defaultdict(set)

    # Convert DataFrame to list for graph construction
    call_data = call_df.collect()  # Collect call data from DataFrame to list

    for c1, c2, _, _ in call_data:
        graph[c1].add(c2)
        graph[c2].add(c1)  # Ensure bidirectional connection

    # Step 2: Find connected components using DFS or BFS
    visited = set()
    communities = []

    for client in graph:
        if client not in visited:
            community = []
            dfs(client, community, visited, graph) if method == 'dfs' else bfs(client, community, visited, graph)
            communities.append(community)

    return communities


def assign_community_numbers(call_df, communities):
    # Create a DataFrame that maps clients to community numbers
    client_community_mapping = []
    for idx, community in enumerate(communities):
        comm_number = idx + 1
        for client in community:
            client_community_mapping.append((client, comm_number))

    client_community_df = spark.createDataFrame(client_community_mapping, ["client", "comm_number"])

    # Assign community numbers to the calls in call_df
    # Alias client_community_df in the second join to avoid ambiguity
    call_df = call_df.join(client_community_df, call_df.c1 == client_community_df.client, "left").withColumnRenamed("comm_number", "comm_number_c1")
    call_df = call_df.join(client_community_df.alias("client_community_df_2"), call_df.c2 == F.col("client_community_df_2.client"), "left").withColumnRenamed("comm_number", "comm_number_c2") # Alias client_community_df as client_community_df_2

    # Ensure that each call belongs to the same community (in case clients belong to multiple communities)
    call_df = call_df.withColumn("comm_number", F.coalesce(F.col("comm_number_c1"), F.col("comm_number_c2")))

    # Drop the intermediate columns
    call_df = call_df.drop("comm_number_c1", "comm_number_c2")

    return call_df