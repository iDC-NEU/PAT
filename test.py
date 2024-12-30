import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
from collections import defaultdict
import random
from scipy.sparse import lil_matrix  # 导入稀疏矩阵

def build_graph_from_file(file_path, max_lines=50000):
    print(f"Starting to read the file: {file_path}")
    graph = nx.Graph()  # 创建无向图
    edge_weights = defaultdict(int)  # 用于存储边的权重
    page_id_mapping = {}  # 用于保存 page_id 到新 ID 的映射
    next_id = 0  # 从 0 开始映射

    # 读取文件并逐行处理
    with open(file_path, 'r') as file:
        print("Processing the transactions...")
        for line_num, line in enumerate(file, 1):
            if line_num > max_lines:  # 限制读取的行数
                break
            if line_num % 100 == 0:  # 每处理100行打印一次
                print(f"Processed {line_num} lines...")

            # 如果行为空，则跳过
            if not line:
                continue

            elements = line.strip().split()
            page_ids = elements[::2]  # 每隔两个取一个 page_id

            # 更新 page_id 映射
            mapped_page_ids = []
            for page_id in page_ids:
                if page_id not in page_id_mapping:
                    page_id_mapping[page_id] = next_id
                    next_id += 1
                mapped_page_ids.append(page_id_mapping[page_id])

            # 创建完全图的边并记录权重
            for i in range(len(mapped_page_ids)):
                for j in range(i + 1, len(mapped_page_ids)):
                    edge = tuple(sorted((mapped_page_ids[i], mapped_page_ids[j])))  # 保证无向图的边唯一性
                    edge_weights[edge] += 1

    print(f"Finished reading the file. Total unique pages: {len(page_id_mapping)}")
    print(f"Total edges added: {len(edge_weights)}")

    # 将边和权重添加到图中
    for edge, weight in edge_weights.items():
        graph.add_edge(edge[0], edge[1], weight=weight)

    return graph, page_id_mapping

def select_random_nodes(graph, top_fraction=0.1):
    # 随机选择图中一部分节点，比例为 top_fraction
    total_nodes = list(graph.nodes())
    top_n = int(len(total_nodes) * top_fraction)  # 计算需要选择的节点数
    selected_nodes = random.sample(total_nodes, top_n)  # 随机选择
    print(f"Selected {len(selected_nodes)} random nodes.")
    return selected_nodes

def plot_graph_heatmap(graph, page_id_mapping, selected_nodes):
    print("Building the sparse adjacency matrix...")
    num_pages = len(page_id_mapping)

    # 创建一个稀疏矩阵（lil_matrix，适合修改）
    adjacency_matrix = lil_matrix((num_pages, num_pages), dtype=np.float64)

    # 填充稀疏矩阵，只有存在的边才会被填充
    for (node1, node2, data) in graph.edges(data=True):
        weight = data['weight']
        adjacency_matrix[node1, node2] = weight
        adjacency_matrix[node2, node1] = weight  # 无向图对称

    print("Finished building the sparse adjacency matrix.")
    
    # 选择与代表性节点相关的子矩阵
    selected_indices = [page_id_mapping[node] for node in selected_nodes]
    sub_matrix = adjacency_matrix[selected_indices, :][:, selected_indices]

    # 转换为稠密矩阵后绘图
    sub_matrix = sub_matrix.toarray()  # 转为密集矩阵进行绘图

    # 绘制热图
    print("Plotting the heatmap...")
    plt.figure(figsize=(8, 6))
    plt.imshow(sub_matrix, cmap='YlGnBu', interpolation='nearest', vmin=0, vmax=np.max(sub_matrix))
    plt.colorbar(label="Weight")

    # 设置坐标轴标签
    plt.xticks(np.arange(len(selected_nodes)), selected_nodes, rotation=90)
    plt.yticks(np.arange(len(selected_nodes)), selected_nodes)

    plt.title("Page Access Heatmap (Randomly Selected Nodes)")
    plt.xlabel("Page ID")
    plt.ylabel("Page ID")

    plt.tight_layout()
    plt.show()
    print("Heatmap plotted successfully.")

# 示例：读取文件并生成图
file_path = "/root/home/AffinityDB_rc/AffinityDB/ScaleStore/Logs/page_Log.txt"  # 替换为实际文件路径
print(f"Starting to build the graph from the file: {file_path}")
graph, page_id_mapping = build_graph_from_file(file_path)

# 随机选择代表性节点（随机选择图中10%的节点）
selected_nodes = select_random_nodes(graph, top_fraction=0.1)

# 绘制热图
print("Starting to plot the heatmap...")
plot_graph_heatmap(graph, page_id_mapping, selected_nodes)
print("Script finished successfully.")
