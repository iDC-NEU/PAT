import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
from collections import defaultdict
from scipy.sparse import dok_matrix
import random

def build_graph_from_file(file_path):
    print(f"Starting to read the file: {file_path}")
    graph = nx.Graph()  # 创建无向图
    edge_weights = defaultdict(int)  # 用于存储边的权重
    page_id_mapping = {}  # 用于保存 page_id 到新 ID 的映射
    next_id = 0  # 从 0 开始映射

    # 读取文件并逐行处理
    with open(file_path, 'r') as file:
        print("Processing the transactions...")
        for line_num, line in enumerate(file, 1):
            if line_num > 30000:  # 限制读取的行数
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

def plot_graph_heatmap(graph, page_id_mapping):
    print("Building the sparse adjacency matrix...")
    # 随机选取 100 个页面
    selected_nodes = random.sample(page_id_mapping.keys(), 50)
    selected_indices = [page_id_mapping[node] for node in selected_nodes]
    
    # 构建稀疏权重矩阵
    num_pages = len(page_id_mapping)
    adjacency_matrix = dok_matrix((num_pages, num_pages), dtype=float)

    # 填充权重矩阵
    for (node1, node2, data) in graph.edges(data=True):
        weight = data['weight']
        adjacency_matrix[node1, node2] = weight
        adjacency_matrix[node2, node1] = weight  # 无向图对称

    print("Finished building the adjacency matrix.")

    # 将稀疏矩阵转换为密集矩阵，并且只保留选中的页面
    adjacency_matrix = adjacency_matrix.toarray()
    adjacency_matrix_selected = adjacency_matrix[np.ix_(selected_indices, selected_indices)]

    # 让对角线的颜色更深
    np.fill_diagonal(adjacency_matrix_selected, adjacency_matrix_selected.max() + 1)  # 加大对角线的值

    # 绘制热图
    print("Plotting the heatmap...")
    plt.figure(figsize=(8, 6))
    plt.imshow(adjacency_matrix_selected, cmap='YlGnBu', interpolation='nearest')
    plt.colorbar(label="Weight")

    # 去掉坐标轴标签
    plt.xticks([])
    plt.yticks([])

    # 反转纵坐标顺序
    plt.gca().invert_yaxis()

    plt.title("Page Access Heatmap")
    plt.xlabel("Page ID")
    plt.ylabel("Page ID")

    # 保存图片
    plt.tight_layout()
    plt.savefig("heatmap.png")
    print("Heatmap saved as 'heatmap.png'.")

# 示例：读取文件并生成图
file_path = "/root/home/AffinityDB_rc/AffinityDB/ScaleStore/Logs/page_Log.txt"  # 替换为实际文件路径
print(f"Starting to build the graph from the file: {file_path}")
graph, page_id_mapping = build_graph_from_file(file_path)

# 绘制热图
print("Starting to plot the heatmap...")
plot_graph_heatmap(graph, page_id_mapping)
print("Script finished successfully.")
