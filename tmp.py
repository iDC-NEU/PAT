import networkx as nx
import matplotlib.pyplot as plt
import random
from collections import defaultdict

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

def draw_sampled_graph(graph, page_id_mapping, page_id_values):
    # 固定采样 15 个 page_id_value 为 0 或 1 的页面
    fixed_value_pages_count = 15

    # 筛选出值为 0 或 1 的 page_id
    fixed_value_page_ids = [page_id for page_id, value in page_id_values.items() if value == 0 or value == 1]

    # 检查是否至少有 15 个页面值为 0 或 1
    if len(fixed_value_page_ids) < fixed_value_pages_count:
        raise ValueError(f"Not enough pages with value 0 or 1. Only {len(fixed_value_page_ids)} available.")

    # 从筛选出的页面中随机选择 15 个
    selected_fixed_value_pages = random.sample(fixed_value_page_ids, fixed_value_pages_count)

    # 获取剩余页面（即不包括已选的 15 个）
    remaining_page_ids = [page_id for page_id in page_id_values if page_id not in selected_fixed_value_pages]

    # 随机选取 35 个剩余页面
    selected_remaining_pages = random.sample(remaining_page_ids, 35)

    # 最终的 50 个选中页面
    selected_page_ids = selected_fixed_value_pages + selected_remaining_pages

    # 映射到图的节点索引
    selected_indices = {page_id_mapping[page_id] for page_id in selected_page_ids}

    # 创建子图
    subgraph = graph.subgraph(selected_indices)

    # 绘制子图
    plt.figure(figsize=(12, 8))
    pos = nx.spring_layout(subgraph)  # 计算子图的布局
    node_colors = ["skyblue"] * len(subgraph.nodes)

    # 绘制节点
    nx.draw_networkx_nodes(subgraph, pos, node_size=600, node_color=node_colors, edgecolors="black")

    # 绘制边
    nx.draw_networkx_edges(subgraph, pos, edgelist=subgraph.edges, arrowstyle="-", arrowsize=10, edge_color="gray", width=1)

    # 反转 page_id_mapping 映射
    reversed_mapping = {v: k for k, v in page_id_mapping.items()}
    labels = {node: f"{reversed_mapping[node]}" for node in subgraph.nodes}

    # 绘制节点标签
    nx.draw_networkx_labels(subgraph, pos, labels=labels, font_size=10, font_color="black")

    # 绘制边标签
    edge_labels = nx.get_edge_attributes(subgraph, "weight")
    nx.draw_networkx_edge_labels(subgraph, pos, edge_labels=edge_labels, font_size=8)

    # 显示图形
    plt.title("Sampled Graph Visualization", fontsize=14)
    plt.axis("off")  # 关闭坐标轴
    plt.show()

# 调用你的函数生成图
file_path = "your_file.txt"  # 替换为你的文件路径
graph, page_id_mapping, page_id_values = build_graph_from_file(file_path)

# 调用绘图函数
draw_sampled_graph(graph, page_id_mapping, page_id_values)
