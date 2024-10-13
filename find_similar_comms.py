import pandas as pd
from collections import defaultdict


def dfs_recursive(client, community, visited, graph):
    visited.add(client)
    community.append(int(client))
    for neighbor in graph[client]:
        if neighbor not in visited:
            dfs(neighbor, community, visited, graph)

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


def find_communities(call_data, method='dfs'):
    graph = defaultdict(set)

    # Step 1: Build the graph from call data
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

if __name__ == '__main__':
    test = pd.read_csv('data/test1.csv')
    communities = find_communities(test.values, method='dfs')

    print(communities)
    print(f'{len(communities)} communities found.')
