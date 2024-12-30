import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
from collections import defaultdict

def build_graph_from_file(file_path):
    graph = nx.Graph()  # 创建无向图
    edge_weights = defaultdict(int)  # 用于存储边的权重
    page_id_mapping = {}  # 用于保存 page_id 到新 ID 的映射
    next_id = 0  # 从 0 开始映射

    # 读取文件并逐行处理
    with open(file_path, 'r') as file:
        for line in file:
            # 提取事务中的 page_id 列表，忽略表名
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

    # 将边和权重添加到图中
    for edge, weight in edge_weights.items():
        graph.add_edge(edge[0], edge[1], weight=weight)

    return graph, page_id_mapping

def plot_graph_heatmap(graph, page_id_mapping):
    # 构建权重矩阵
    num_pages = len(page_id_mapping)
    adjacency_matrix = np.zeros((num_pages, num_pages))

    # 填充权重矩阵
    for (node1, node2, data) in graph.edges(data=True):
        weight = data['weight']
        adjacency_matrix[node1][node2] = weight
        adjacency_matrix[node2][node1] = weight  # 因为是无向图，所以对称

    # 绘制热图
    plt.figure(figsize=(8, 6))
    plt.imshow(adjacency_matrix, cmap='YlGnBu', interpolation='nearest')
    plt.colorbar(label="Weight")

    # 设置坐标轴标签
    plt.xticks(np.arange(num_pages), [f"{key}" for key in page_id_mapping.keys()], rotation=90)
    plt.yticks(np.arange(num_pages), [f"{key}" for key in page_id_mapping.keys()])

    plt.title("Page Access Heatmap")
    plt.xlabel("Page ID")
    plt.ylabel("Page ID")

    plt.tight_layout()
    plt.show()

# 示例：读取文件并生成图
file_path = "/root/home/AffinityDB_rc/AffinityDB/ScaleStore/Logs/page_Log.txt"  # 替换为实际文件路径
graph, page_id_mapping = build_graph_from_file(file_path)

# 绘制热图
plot_graph_heatmap(graph, page_id_mapping)
