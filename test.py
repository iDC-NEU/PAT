import networkx as nx
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

# 示例：读取文件并生成图
file_path = "transactions.txt"  # 替换为实际文件路径
graph, page_id_mapping = build_graph_from_file(file_path)

# 保存图为 GML 格式
nx.write_gml(graph, "graph.gml")

# 如果需要其他格式，可以取消注释以下行：
# 保存为 GraphML 格式
# nx.write_graphml(graph, "graph.graphml")
# 保存为边列表格式
# nx.write_edgelist(graph, "graph.edgelist", data=["weight"])

# 打印 page_id 映射
print("\nPage ID Mapping:")
for original_id, new_id in page_id_mapping.items():
    print(f"Original ID {original_id} mapped to {new_id}")
