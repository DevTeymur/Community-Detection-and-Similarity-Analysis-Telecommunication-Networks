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



def find_communities(call_df: DataFrame, method='dfs'):
    # Step 1: Build the graph using Spark transformations (avoiding `collect` for large datasets)
    graph_rdd = call_df.rdd.flatMap(lambda row: [(row['c1'], row['c2']), (row['c2'], row['c1'])])
    graph = graph_rdd.groupByKey().mapValues(set).collectAsMap()

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

    # Step 3: Create `comm_number` using the Spark map to avoid excessive shuffling
    mapping_expr = F.create_map([F.lit(x) for item in client_to_community.items() for x in item])

    # Add `comm_number` to the original DataFrame
    updated_df = call_df.withColumn("comm_number_c1", mapping_expr.getItem(col("c1")))
    updated_df = updated_df.withColumn("comm_number_c2", mapping_expr.getItem(col("c2")))

    updated_df = updated_df.withColumn(
        "comm_number", when(col("comm_number_c1").isNotNull(), col("comm_number_c1")).otherwise(col("comm_number_c2"))
    ).drop("comm_number_c1", "comm_number_c2")

    # Cache updated DataFrame to avoid recomputation
    updated_df.cache()

    return updated_df



def find_communities(call_df: DataFrame, method='dfs'):
    # Step 1: Build the graph using Spark transformations (avoiding `collect` for large datasets)
    graph_rdd = call_df.rdd.flatMap(lambda row: [(row['c1'], row['c2']), (row['c2'], row['c1'])])
    graph = graph_rdd.groupByKey().mapValues(set).collectAsMap()

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

    # Step 3: Create `comm_number` using the Spark map to avoid excessive shuffling
    mapping_expr = F.create_map([F.lit(x) for item in client_to_community.items() for x in item])

    # Add `comm_number` to the original DataFrame
    updated_df = call_df.withColumn("comm_number_c1", mapping_expr.getItem(col("c1")))
    updated_df = updated_df.withColumn("comm_number_c2", mapping_expr.getItem(col("c2")))

    updated_df = updated_df.withColumn(
        "comm_number", when(col("comm_number_c1").isNotNull(), col("comm_number_c1")).otherwise(col("comm_number_c2"))
    ).drop("comm_number_c1", "comm_number_c2")

    # Cache updated DataFrame to avoid recomputation
    updated_df.cache()

    return updated_df


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

def find_communities(call_df: DataFrame, method='dfs'):
    # Step 1: Build the graph from call data in a distributed manner
    graph_df = call_df.select("c1", "c2").distinct()

    # Step 2: Aggregate the data to create connections between clients
    client_graph = (
        graph_df
        .withColumn("client_pair", F.array("c1", "c2"))  # Create a column with client pairs
        .select(F.explode("client_pair").alias("client"), "c1", "c2")  # Explode the array to have each client in its own row
        .groupBy("client")  # Group by client
        .agg(F.collect_set(F.when(col("client") == col("c1"), col("c2")).otherwise(col("c1"))).alias("connections"))  # Collect neighbors
    )

    # Collect the graph as a dictionary for DFS/BFS traversal
    graph = defaultdict(set)
    graph_data = client_graph.collect()

    for row in graph_data:
        client = row["client"]
        connections = row["connections"]
        graph[client].update(connections)

    # Step 3: Find connected components using DFS or BFS
    visited = set()
    communities = []
    client_to_community = {}
    community_counter = 1  # Start community numbering from 1

    for client in graph:
        if client not in visited:
            community = []
            dfs(client, community, visited, graph) if method == 'dfs' else bfs(client, community, visited, graph)
            communities.append(community)

            # Assign a sequential community number to each client in the community
            for member in community:
                client_to_community[member] = community_counter  # Sequential community number
            community_counter += 1  # Increment the community number for the next community

    # Step 4: Create a new column `comm_number` in the original DataFrame
    mapping_expr = F.create_map([F.lit(x) for item in client_to_community.items() for x in item])

    # Create a `comm_number` column for both clients in the call
    updated_df = call_df.withColumn("comm_number_c1", mapping_expr.getItem(col("c1")))
    updated_df = updated_df.withColumn("comm_number_c2", mapping_expr.getItem(col("c2")))

    # Since both c1 and c2 belong to the same community, use either comm_number_c1 or comm_number_c2
    updated_df = updated_df.withColumn(
        "comm_number", when(col("comm_number_c1").isNotNull(), col("comm_number_c1")).otherwise(col("comm_number_c2"))
    ).drop("comm_number_c1", "comm_number_c2")

    return updated_df


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

def find_communities(call_df, method='dfs'):
    # Step 1: Build the graph from call data
    graph = defaultdict(set)

    # Collect call data into list
    call_data = call_df.collect()

    # Build bidirectional graph
    for c1, c2, _, _ in call_data:
        graph[c1].add(c2)
        graph[c2].add(c1)

    # Step 2: Find connected components
    visited = set()
    communities = []
    client_to_community = {}
    community_counter = 1

    for client in graph:
        if client not in visited:
            community = []
            if method == 'dfs':
                dfs(client, community, visited, graph)
            else:
                bfs(client, community, visited, graph)
            communities.append(community)

            # Assign community number
            for member in community:
                client_to_community[member] = community_counter
            community_counter += 1

    # Step 3: Create a new column `comm_number` in the original DataFrame
    mapping_expr = F.create_map([F.lit(x) for item in client_to_community.items() for x in item])

    # Create `comm_number` column for both clients
    updated_df = call_df.withColumn("comm_number_c1", mapping_expr.getItem(col("c1")))
    updated_df = updated_df.withColumn("comm_number_c2", mapping_expr.getItem(col("c2")))

    # Use either `comm_number_c1` or `comm_number_c2` to create final `comm_number`
    updated_df = updated_df.withColumn(
        "comm_number", when(col("comm_number_c1").isNotNull(), col("comm_number_c1")).otherwise(col("comm_number_c2"))
    ).drop("comm_number_c1", "comm_number_c2")

    return updated_df
