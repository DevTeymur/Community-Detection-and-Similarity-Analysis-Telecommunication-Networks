import numpy as np

def calculate_community_metrics(community, call_data):
    community_clients = set(community)
    
    community_calls = [call for call in call_data if call[0] in community_clients and call[1] in community_clients]
    num_calls = len(community_calls)
    
    # Calculate number of clients in the community
    num_clients = len(community)
    
    # Calculate mean and standard deviation of call durations
    durations = [int(call[3]) - int(call[2]) for call in community_calls]
    mean_duration = np.mean(durations) if durations else 0
    std_duration = np.std(durations) if durations else 0

    return {
        'num_calls': num_calls,
        'num_clients': num_clients,
        'mean_duration': mean_duration,
        'std_duration': std_duration
    }

# Function to compare two communities based on the calculated metrics
def compare_communities(metrics1, metrics2, name1, name2, threshold=0.1):
    # Compare the metrics
    num_calls_sim = abs(metrics1['num_calls'] - metrics2['num_calls']) / max(metrics1['num_calls'], metrics2['num_calls'], 1)
    num_clients_sim = abs(metrics1['num_clients'] - metrics2['num_clients']) / max(metrics1['num_clients'], metrics2['num_clients'], 1)
    mean_duration_sim = abs(metrics1['mean_duration'] - metrics2['mean_duration']) / max(metrics1['mean_duration'], metrics2['mean_duration'], 1)
    std_duration_sim = abs(metrics1['std_duration'] - metrics2['std_duration']) / max(metrics1['std_duration'], metrics2['std_duration'], 1)
    
    # Print informative messages with community names
    print(f'Comparison of {name1} and {name2}:')
    print(f'  Number of Calls Similarity: {1 - num_calls_sim:.2f}')
    print(f'  Number of Clients Similarity: {1 - num_clients_sim:.2f}')
    print(f'  Mean Call Duration Similarity: {1 - mean_duration_sim:.2f}')
    print(f'  Std Dev of Call Duration Similarity: {1 - std_duration_sim:.2f}')

    # Compute the combined similarity score
    combined_similarity = (1 - num_calls_sim + 1 - num_clients_sim + 1 - mean_duration_sim + 1 - std_duration_sim) / 4
    print(f'  Combined Similarity Score: {combined_similarity:.2f}')
    
    # Determine if communities are similar based on threshold
    are_similar = combined_similarity >= threshold
    return are_similar

# Function to group similar communities
def group_similar_communities(communities, call_data, similarity_threshold=0.8):
    community_metrics = [calculate_community_metrics(community, call_data) for community in communities]
    
    # Dictionary to track the groups
    groups = []
    visited = [False] * len(communities)

    # Generate community names like c1, c2, etc.
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

# Example usage
if __name__ == "__main__":
    call_data = np.array([
        [1, 2, 2408010800, 2408010830],
        [2, 3, 2408010900, 2408010930],
        [1, 3, 2408011000, 2408011030],
        [4, 5, 2408011100, 2408011130],
        [5, 6, 2408011200, 2408011230],
        [6, 7, 2408011300, 2408011330],
        [4, 7, 2408011400, 2408011430],
        [8, 9, 2408011500, 2408011530]
    ])

    communities = [[1, 3, 2], [4, 7, 6, 5], [8, 9]]
    group_similar_communities(communities, call_data, similarity_threshold=0.8)
