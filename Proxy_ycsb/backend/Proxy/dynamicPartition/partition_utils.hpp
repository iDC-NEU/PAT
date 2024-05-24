// #pragma once

// #include "graph.hpp"
// #include <fstream>
// #include <set>
// #include <ctime>
// #include <unordered_set>
// #include "Proxy/dynamicPartition/tools/include/metis.h"
// ///////REMOVE/////////
// namespace Proxy
// {
//     namespace router
//     {

//         extern int stamp_len;
//         extern int frequent_limit;
//         extern int partition_num;
//         extern int customer_range;
//         extern int oorder_range;
//         extern int stock_range;
//         extern int history_range;
//         extern int warehouse_range;

//         class partiton
//         {
//         public:
//             int customer_offset = warehouse_range * customer_range;
//             int history_offset = warehouse_range * history_range;
//             int oorder_offset = warehouse_range * oorder_range;
//             int stock_offset = warehouse_range * stock_range;

            
//             /*int get_wid(int key)
//             {
//                 int offset = key - customer_offset;
//                 if(offset > 0){
//                     key = key - customer_offset;
//                     offset = key - history_offset;
//                     if(offset > 0){
//                         key = key - history_offset;
//                         offset = key - oorder_offset;
//                         if(offset > 0){
//                             return offset/stock_range;
//                         }
//                         return key/oorder_range;
//                     }
//                     return key/history_range;
//                 }
//                 return key/customer_range;
//             }*/
//             const int w_count = FLAGS_tpcc_warehouse_count;
//             const int customer_key_min = 1'000'000 * 1 + 1'0000 * 1 + 1;
//             const int customer_key_max = 1'000'000 * w_count + 1'0000 * 10 + 3000;
//             const int order_key_min = customer_key_max + 1'000'000 * 1 + 1'0000 * 1 + 1;
//             const int order_key_max = customer_key_max + 1'000'000 * w_count + 1'0000 * 10 + 3000;
//             const int stock_key_min = order_key_max + 1'000'000 * 1 + 1;
//             const int stock_key_max = order_key_max + 1'000'000 * w_count + 100000;
//             int get_wid(int key) {
//                 if(customer_key_min <= key && key <= customer_key_max) {
//                     return key / 1'000'000;
//                 }
//                 if(order_key_min <= key && key <= order_key_max) {
//                     return (key - customer_key_max) / 1'000'000;
//                 }

//                 if(stock_key_min <= key && key <= stock_key_max) {
//                     return (key - order_key_max) / 1'000'000;
//                 }
//                 printf("Invalid Key key%d\n",key);
//                 return -1;
//          }

//             std::unordered_map<int, EdgeInfo> generate_stamps(const std::vector<int> &txn_key_list, std::unordered_map<int, StampInfo> &stamp_map)
//             {
//                 std::unordered_map<int, EdgeInfo> edge_infos;
//                 for (const auto key : txn_key_list)
//                 {
//                     int stamp_id = key / stamp_len;                  // 根据关键字生成唯一的图顶点标识
//                     if (stamp_map.find(stamp_id) == stamp_map.end()) // 如果不存在，创建一个新的 StampInfo 对象，并设置其仓库 ID和总次数。
//                     {
//                         StampInfo stamp(stamp_id);
//                         int w_id = get_wid(key);
//                         stamp.set_w_id(w_id);
//                         stamp.add_all_count(1);
//                         stamp_map.insert({stamp_id, stamp});
//                     }
//                     else
//                     {
//                         stamp_map[stamp_id].add_all_count(1);
//                     }
//                     if (edge_infos.find(stamp_id) == edge_infos.end())
//                     {
//                         EdgeInfo edge_info_t(stamp_id);
//                         for (auto &edge_info : edge_infos)
//                         {
//                             edge_info.second.add_edge(stamp_id);
//                             edge_info_t.add_edge(edge_info.first);
//                         }
//                         edge_infos.insert({stamp_id, edge_info_t});
//                     }
//                     else
//                     {
//                         edge_infos[stamp_id].add_count(1);
//                     }
//                 }
//                 return edge_infos;
//             }

//             // 建立对顶点的索引
//             void GetMap(Graph &graph, std::unordered_map<int, int> &Stamp_Vertix_map, std::unordered_map<int, int> &Vertix_Stamp_map)
//             {
//                 int i = 0;
//                 for (const auto &stamp : graph.get_graph())
//                 {
//                     Stamp_Vertix_map.insert({stamp.first, i});
//                     Vertix_Stamp_map.insert({i, stamp.first});
//                     i++;
//                 }
//             }

//             // 该函数的最终结果是两个向量 Adjncy 和 Xadj，它们分别表示图的邻接顶点列表和每个顶点邻接列表的起始位置。这种表示形式是为了符合 METIS 库的输入要求。
//             void GetEdge_Without_Weight(Graph &graph, std::vector<idx_t> &Adjncy, std::vector<idx_t> &Xadj, const std::unordered_map<int, int> &stamp_map)
//             {
//                 int step = 0;
//                 std::pair<idx_t, idx_t> page_id;
//                 for (const auto &stamp : graph.get_graph())
//                 {
//                     Xadj.push_back(step);
//                     for (const auto &edge_pair : stamp.second)
//                     {
//                         Adjncy.push_back(stamp_map.at(edge_pair.first));
//                         step++;
//                     }
//                 }
//                 Xadj.push_back(step);
//             }

//             // 得到分区结果
//             void get_partition(const std::vector<idx_t> &parts, std::unordered_map<int, std::unordered_set<int>> &cluster, const std::unordered_map<int, int> &vertix_map)
//             {
//                 int i = 0;
//                 for (int j = 0; j < partition_num; j++)
//                 {
//                     std::unordered_set<int> part;
//                     cluster.insert({j, part});
//                 }
//                 for (const auto part_id : parts)
//                 {
//                     cluster[part_id].insert(vertix_map.at(i));
//                     i++;
//                 }
//             }

//             std::unordered_map<int, std::unordered_set<int>> MetisPart(Graph &graph)
//             {
//                 std::unordered_map<int, int> Stamp_Vertix_map;
//                 std::unordered_map<int, int> Vertix_Stamp_map;
//                 std::unordered_map<int, std::unordered_set<int>> cluster;
//                 GetMap(graph, Stamp_Vertix_map, Vertix_Stamp_map);
//                 idx_t nvtxs = graph.number_of_nodes();
//                 idx_t ncon = 1;
//                 // idx_t nparts = partition_num;
//                 idx_t nparts = FLAGS_nodes;
//                 idx_t edgecut;
//                 std::vector<idx_t> Adjncy;
//                 std::vector<idx_t> Xadj;
//                 // std::vector<idx_t> Adjwgt;
//                 // std::vector<idx_t> Vwgt;
//                 // std::map<int, int> new_map;
//                 std::vector<idx_t> parts(nvtxs, -1);
//                 GetEdge_Without_Weight(graph, Adjncy, Xadj, Stamp_Vertix_map);
//                 std::cout << std::endl;
//                 int ret = METIS_PartGraphKway(&nvtxs, &ncon, Xadj.data(), Adjncy.data(), NULL, NULL, NULL,
//                                               &nparts, NULL, NULL, NULL, &edgecut, parts.data());
//                 switch (ret)
//                 {
//                 case rstatus_et::METIS_OK:
//                     std::cout << "METIS_OK" << std::endl;
//                     break;
//                 case rstatus_et::METIS_ERROR_INPUT:
//                     std::cout << "METIS_ERROR_INPUT" << std::endl;
//                     break;
//                 case rstatus_et::METIS_ERROR_MEMORY:
//                     std::cout << "METIS_ERROR_MEMERY" << std::endl;
//                     break;
//                 default:
//                     std::cout << "METIS_ERROR" << std::endl;
//                     break;
//                 }
//                 get_partition(parts, cluster, Vertix_Stamp_map);
//                 return cluster;
//             }
//             void AppendEdgeInfo(std::unordered_map<int, EdgeInfo> &dest, const std::vector<int> &txn_key_list, std::unordered_map<int, StampInfo> &stamp_map)
//             {
//                 std::unordered_set<int> temps;
//                 for (const auto key : txn_key_list)
//                 {
//                     int stamp_id = key / stamp_len;
//                     if (stamp_map.find(stamp_id) == stamp_map.end())
//                     {
//                         StampInfo stamp(stamp_id);
//                         int w_id = get_wid(key);
//                         stamp.set_w_id(w_id);
//                         stamp.add_all_count(1);
//                         stamp_map.insert({stamp_id, stamp});
//                     }
//                     else
//                     {
//                         stamp_map[stamp_id].add_all_count(1);
//                     }
//                     if (dest.find(stamp_id) == dest.end())
//                     {
//                         EdgeInfo edge_info(stamp_id);
//                         for (auto &tmp : temps)
//                         {
//                             dest[tmp].add_edge(stamp_id);
//                             edge_info.add_edge(tmp);
//                         }
//                         dest.insert({stamp_id, edge_info});
//                         temps.insert(stamp_id);
//                     }
//                     else
//                     {
//                         if (temps.find(stamp_id) == temps.end())
//                         {
//                             for (auto &tmp : temps)
//                             {
//                                 dest[tmp].add_edge(stamp_id);
//                                 dest[stamp_id].add_edge(tmp);
//                             }
//                             temps.insert(stamp_id);
//                         }
//                         dest[stamp_id].add_count(1);
//                     }
//                 }
//             }
//         };
//     }
// }
