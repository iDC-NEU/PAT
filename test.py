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
    page_id_values = {}  # 用于保存每个 page_id 对应的数字
    next_id = 0  # 从 0 开始映射

    # 读取文件并逐行处理
    with open(file_path, 'r') as file:
        print("Processing the transactions...")
        for line_num, line in enumerate(file, 1):
            if line_num > 500000:  # 限制读取的行数
                break
            if line_num % 10000 == 0:  # 每处理1000行打印一次
                print(f"Processed {line_num} lines...")

            # 如果行为空，则跳过
            if not line:
                continue

            elements = line.strip().split()
            page_ids = elements[::2]  # 每隔两个取一个 page_id
            values = elements[1::2]  # 获取每个 page_id 后面的数字

            # 更新 page_id 映射
            mapped_page_ids = []
            for i, page_id in enumerate(page_ids):
                if page_id not in page_id_mapping:
                    page_id_mapping[page_id] = next_id
                    next_id += 1
                mapped_page_ids.append(page_id_mapping[page_id])
                
                # 保存每个 page_id 对应的数字
                page_id_values[page_id] = int(values[i])

            # 创建完全图的边并记录权重
            for i in range(len(mapped_page_ids)):
                for j in range(i + 1, len(mapped_page_ids)):
                    edge = tuple(sorted((mapped_page_ids[i], mapped_page_ids[j])))  # 保证无向图的边唯一性
                    edge_weights[edge] += 1

                # 处理自环边（i == j）
                edge = (mapped_page_ids[i], mapped_page_ids[i])  # 自环边
                edge_weights[edge] += 1

    print(f"Finished reading the file. Total unique pages: {len(page_id_mapping)}")
    print(f"Total edges added: {len(edge_weights)}")

    # 将边和权重添加到图中
    for edge, weight in edge_weights.items():
        graph.add_edge(edge[0], edge[1], weight=weight)

    return graph, page_id_mapping, page_id_values


def plot_graph_heatmap(graph, page_id_mapping, page_id_values):
    print("Building the sparse adjacency matrix...")

    # 设置固定采样 10 个 page_id_value 为 0 或 1 的页面
    fixed_value_pages_count = 4

    # 筛选出值为 0 或 1 的 page_id
    fixed_value_page_ids = [page_id for page_id, value in page_id_values.items() if value == 0 or value == 1]

    # 检查是否至少有 10 个页面值为 0 或 1
    if len(fixed_value_page_ids) < fixed_value_pages_count:
        raise ValueError(f"Not enough pages with value 0 or 1. Only {len(fixed_value_page_ids)} available.")

    # 从筛选出的页面中随机选择 10 个
    selected_fixed_value_pages = random.sample(fixed_value_page_ids, fixed_value_pages_count)

    # 获取剩余页面（即不包括已选的 10 个）
    remaining_page_ids = [page_id for page_id in page_id_values if page_id not in selected_fixed_value_pages]

    # 随机选取 40 个剩余页面
    selected_remaining_pages = random.sample(remaining_page_ids, 46)

    # 最终的 50 个选中页面
    selected_page_ids = selected_fixed_value_pages + selected_remaining_pages

    # **打乱选中的页面顺序** 
    random.shuffle(selected_page_ids)

    # 获取 selected_indices
    selected_indices = [page_id_mapping[page_id] for page_id in selected_page_ids]
    
    # 构建稀疏权重矩阵
    num_pages = len(page_id_mapping)
    adjacency_matrix = dok_matrix((num_pages, num_pages), dtype=float)

    # 填充权重矩阵，包括自环边
    for (node1, node2, data) in graph.edges(data=True):
        weight = data['weight']
        adjacency_matrix[node1, node2] = weight
        adjacency_matrix[node2, node1] = weight  # 无向图对称

    # 处理自环边（node1 == node2）
    for node in graph.nodes():
        if graph.has_edge(node, node):  # 如果有自环边
            adjacency_matrix[node, node] = graph[node][node]['weight']  # 添加自环边的权重

    print("Finished building the adjacency matrix.")

    # 将稀疏矩阵转换为密集矩阵，并且只保留选中的页面
    adjacency_matrix = adjacency_matrix.toarray()
    adjacency_matrix_selected = adjacency_matrix[np.ix_(selected_indices, selected_indices)]

    # 对权重进行归一化或对数缩放（这里用对数缩放）
    adjacency_matrix_selected = np.log1p(adjacency_matrix_selected)  # 使用对数缩放，避免数值过大

    # 绘制热图
    print("Plotting the heatmap...")
    plt.figure(figsize=(8, 6))

    # 使用 'YlGnBu' 色图显示热图，并设置最低值为 0 以保证区分度
    plt.imshow(adjacency_matrix_selected, cmap='YlGnBu', interpolation='nearest', vmin=0)

    # 添加颜色条
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
    plt.savefig("heatmap.svg", format="svg")
    plt.savefig("heatmap.pdf", format="pdf")
    print("Heatmap saved as 'heatmap.svg'.")
    print("Heatmap saved as 'heatmap.pdf'.")


# 示例：读取文件并生成图
file_path = "/root/home/AffinityDB_rc/AffinityDB/ScaleStore/Logs/page_Log.txt"  # 替换为实际文件路径
print(f"Starting to build the graph from the file: {file_path}")
graph, page_id_mapping, page_id_values = build_graph_from_file(file_path)

# 绘制热图
print("Starting to plot the heatmap...")
plot_graph_heatmap(graph, page_id_mapping, page_id_values)
print("Script finished successfully.")
