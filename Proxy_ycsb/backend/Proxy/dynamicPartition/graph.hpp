#pragma once

#include "Proxy/Config.hpp"
#include "../../ScaleStore/shared-headers/Defs.hpp"
#include <iostream>
#include <vector>
#include <map>
#include <unordered_map>
#include <algorithm>
#include <math.h>
#include <string>
#include <fstream>
#include <assert.h>
#include <sstream>
#include <time.h>
#include <queue>
#include <atomic>
#include <unordered_set>
#include <condition_variable>
#include "Proxy/dynamicPartition/tools/include/metis.h"
#include "Proxy/utils/Log.hpp"
/*
typedef struct nbr {
    int v_j;
    double weight;
}neighbors;
*/
namespace Proxy
{
    namespace router
    {
        inline int stamp_len = 50; // 段长
        inline int frequent_limit = 1;
        inline int customer_range = 10000;
        inline int oorder_range = 10000;
        inline int stock_range = 10000;
        inline int history_range = 10000;
        inline int warehouse_range = 100;
        // TODO: 范围不确定，暂定
        inline int neworder_range = 10000;
        inline int orderline_range = 10000;
        class Graph
        {
        public:
            Graph() {}
            Graph(const Graph &other);
            const std::unordered_map<int, std::unordered_map<int, int>> get_graph();
            Graph &operator=(const Graph &other);
            void add_edge(int v_i, int v_j, double weight);
            void add_node(int vid) { graph_.insert({vid, std::unordered_map<int, int>()}); }
            bool has_node(int vid);
            void remove_node(int vid) { graph_.erase(vid); }
            std::unordered_map<int, int> neighbors(int vid);
            int number_of_nodes();
            void clear() { graph_.clear(); }

        private:
            std::unordered_map<int, std::unordered_map<int, int>> graph_;
        };

        // 顶点信息
        class StampInfo
        {

        public:
            StampInfo() {}
            StampInfo(int n) { stamp_number_ = n; }
            ~StampInfo(){};
            int w_id() const { return w_id_; }
            int stamp_number() const { return stamp_number_; }
            int all_count() const { return all_count_; }
            void set_w_id(int w_id) { w_id_ = w_id; }
            void add_all_count(int count) { all_count_ += count; }
            void sub_all_count(int count) { all_count_ -= count; }

        private:
            int w_id_ = -1;
            int stamp_number_ = 0;
            int all_count_ = 0;
        };

        class DynamicPartitioner
        {
        public:
            std::unordered_map<int, int> partmapA;
            std::unordered_map<int, int> partmapB;
            std::unordered_map<int, int> ycsb_map;
            std::mutex update_mutex;
            std::mutex partition_mutex;
            std::vector<bool> old_map_use;
            std::vector<bool> new_map_use;
            std::unordered_set<int> transfer_node;
            int transfer_count = 0;
            int edge_count = 0;
            bool change_map = false;
            bool is_update_map_running = false;
            std::atomic<bool> has_send_new_insert_keys = true;
            bool has_send_metis = false;
            std::vector<std::unordered_set<int>> cluster;
            std::unordered_map<int, int> &partmap;
            DynamicPartitioner(Graph G, double balance_factor = 1.0, int k = 4, double alpha = 0.5, double gamma = 1.5);
            double fennel(int vid, const std::unordered_map<int, int> &neighbors);
            void add_node(int vid, std::unordered_map<int, int> &neighbors);
            void add_node(int vid);
            void run();
            Graph &get_graph() { return G; }
            void cleargraph() { G.clear(); }
            void generate_stamps(const std::vector<TxnNode> &txn_node_list);
            void start_epoch_partition();
            void clear_epoch_set() { epoch_vids.clear(); }
            int get_epoch_set_size() { return epoch_vids.size(); }
            void get_txns(std::string input_filename, std::vector<std::vector<int>> &txns);
            void get_metis_map();
            void update_old_map();
            void create_table_partitioner();
            bool ready_send = true;
            std::unordered_map<int, StampInfo> stampinfo_map;
            std::unordered_map<int, int> new_insert_keys;
            std::unordered_map<int, int> ycsb_insert_keys;
            std::unordered_set<int> new_remove_keys;

        private:
            Graph G;
            double balance_factor;
            int k;
            double alpha;
            double gamma;
            int vertex_num;
            int dypart_txn_count = 0;
            int randompart_txn_count = 0;
            int dypart_dtxn_count = 0;
            int randompart_dtxn_count = 0;
            int file_write_count = 0;
            int partition_num = FLAGS_nodes;

            // statistics
            double change_g_time = 0;
            double fennel_time = 0;
            double fine_tuned_fennel_time = 0;
            double fennel_score_time = 0;
            std::unordered_set<int> epoch_vids; // 记录每个epoch需要划分的节点

        public:

            /*int get_wid(int key)
            {
                int offset = key - customer_offset;
                if(offset > 0){
                    key = key - customer_offset;
                    offset = key - history_offset;
                    if(offset > 0){
                        key = key - history_offset;
                        offset = key - oorder_offset;
                        if(offset > 0){
                            return offset/stock_range;
                        }
                        return key/oorder_range;
                    }
                    return key/history_range;
                }
                return key/customer_range;
            }*/

            // 建立对顶点的索引
            void GetMap(std::unordered_map<int, int> &Stamp_Vertix_map, std::unordered_map<int, int> &Vertix_Stamp_map)
            {
                int i = 0;
                for (const auto &stamp : G.get_graph())
                {
                    Stamp_Vertix_map.insert({stamp.first, i});
                    Vertix_Stamp_map.insert({i, stamp.first});
                    i++;
                }
            }
            void GetEdge_With_Weight(std::vector<idx_t> &Adjncy, std::vector<idx_t> &Adjwgt, std::vector<idx_t> &Xadj, std::vector<idx_t> &Vwgt, const std::unordered_map<int, int> &stamp_map, const std::unordered_map<int, int> &vertix_map)
            {
                int step = 0;
                std::pair<idx_t, idx_t> page_id;
                auto &graph = G.get_graph();
                int size = stamp_map.size();
                Xadj.reserve(size + 1);
                Vwgt.reserve(size + 1);
                for (int i = 0; i < size; i++)
                {
                    Xadj.push_back(step);
                    auto stamp_id = vertix_map.at(i);
                    auto &stamp = graph.at(stamp_id);
                    Vwgt.push_back(stampinfo_map[stamp_id].all_count());
                    for (const auto &edge_pair : stamp)
                    {
                        Adjncy.push_back(stamp_map.at(edge_pair.first));
                        Adjwgt.push_back(edge_pair.second);
                        step++;
                    }
                }
                Xadj.push_back(step);
            }

            // 得到分区结果
            void get_partition(const std::vector<idx_t> &parts, const std::unordered_map<int, int> &vertix_map)
            {
                int i = 0;
                cluster.clear();
                cluster.reserve(partition_num);
                for (int j = 0; j < partition_num; j++)
                {
                    std::unordered_set<int> part;
                    cluster.push_back(part);
                }
                for (const auto part_id : parts)
                {
                    int stamp_id = vertix_map.at(i);
                    cluster[part_id].insert(stamp_id);
                    partmapA.insert({stamp_id, part_id});
                    int key = stamp_id * FLAGS_stamp_len;
                    ycsb_map.insert({key, part_id});
                    i++;
                }
                std::cout << "partmap_size: " << partmapA.size() << std::endl;
            }

            void MetisPart()
            {
                std::cout << "METIS_start" << std::endl;
                std::unordered_map<int, int> Stamp_Vertix_map;
                std::unordered_map<int, int> Vertix_Stamp_map;
                std::unordered_map<int, std::unordered_set<int>> cluster;
                GetMap(Stamp_Vertix_map, Vertix_Stamp_map);
                idx_t nvtxs = G.number_of_nodes();
                idx_t ncon = 1;
                // idx_t nparts = partition_num;
                idx_t nparts = FLAGS_nodes;
                idx_t edgecut;
                std::vector<idx_t> Adjncy;
                std::vector<idx_t> Adjwgt;
                std::vector<idx_t> Xadj;
                std::vector<idx_t> Vwgt;
                // std::vector<idx_t> Adjwgt;
                // std::vector<idx_t> Vwgt;
                // std::map<int, int> new_map;
                std::vector<idx_t> parts(nvtxs, -1);
                GetEdge_With_Weight(Adjncy, Adjwgt, Xadj, Vwgt, Stamp_Vertix_map, Vertix_Stamp_map);
                int ret = METIS_PartGraphKway(&nvtxs, &ncon, Xadj.data(), Adjncy.data(), Vwgt.data(), NULL, Adjwgt.data(),
                                              &nparts, NULL, NULL, NULL, &edgecut, parts.data());
                switch (ret)
                {
                case rstatus_et::METIS_OK:
                    std::cout << "METIS_OK" << std::endl;
                    break;
                case rstatus_et::METIS_ERROR_INPUT:
                    std::cout << "METIS_ERROR_INPUT" << std::endl;
                    break;
                case rstatus_et::METIS_ERROR_MEMORY:
                    std::cout << "METIS_ERROR_MEMERY" << std::endl;
                    break;
                default:
                    std::cout << "METIS_ERROR" << std::endl;
                    break;
                }
                get_partition(parts, Vertix_Stamp_map);
            }
        };

        // 边信息
        class EdgeInfo
        {
        public:
            EdgeInfo() {}
            EdgeInfo(int stamp_id) { stamp_id_ = stamp_id; }
            int stamp_id() const { return stamp_id_; }
            std::unordered_map<int, int> get_edges() const { return edge_weight_; }
            void add_edge(int stamp_id)
            {
                if (edge_weight_.find(stamp_id) == edge_weight_.end())
                {
                    edge_weight_.insert({stamp_id, 1});
                }
                else
                {
                    edge_weight_[stamp_id]++;
                }
            }
            int count() const { return count_; }
            void add_count(int count) { count_ += count; }

        private:
            int stamp_id_ = -1;
            std::unordered_map<int, int> edge_weight_;
            int count_ = 1;
        };

    }
}