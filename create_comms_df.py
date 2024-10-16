from pyspark.sql.functions import col, when # type: ignore
from collections import defaultdict
from pyspark.sql import DataFrame # type: ignore
import pyspark.sql.functions as F # type: ignore

from create_comms_list import dfs, bfs


def find_communities(call_df: DataFrame, method='dfs'):
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
    client_to_community = {}

    for i, client in enumerate(graph):
        if client not in visited:
            community = []
            dfs(client, community, visited, graph) if method == 'dfs' else bfs(client, community, visited, graph)
            communities.append(community)

            # Assign community number to each client in the community
            for member in community:
                client_to_community[member] = i + 1  # Community numbers start from 1

    # Step 3: Create a new column `comm_number` in the original DataFrame
    # We will create this by mapping the `client_to_community` dictionary
    mapping_expr = F.create_map([F.lit(x) for item in client_to_community.items() for x in item])

    
    # Create a `comm_number` column for both clients in the call
    updated_df = call_df.withColumn("comm_number_c1", mapping_expr.getItem(col("c1")))
    updated_df = updated_df.withColumn("comm_number_c2", mapping_expr.getItem(col("c2")))

    # Since both c1 and c2 belong to the same community, use either comm_number_c1 or comm_number_c2
    updated_df = updated_df.withColumn(
        "comm_number", when(col("comm_number_c1").isNotNull(), col("comm_number_c1")).otherwise(col("comm_number_c2"))
    ).drop("comm_number_c1", "comm_number_c2")

    return updated_df