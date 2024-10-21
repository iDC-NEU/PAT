#pragma once

#include "graph.hpp"
#include <mutex>
#include <iostream>
#include <shared_mutex>
#include "spdlog/spdlog.h"

namespace Proxy
{
    namespace router
    {

        class Router
        {
        public:
            std::vector<TxnNode> txnnodelist;
            std::queue<std::vector<TxnNode>> txn_queue;
            std::mutex queue_guard;
            DynamicPartitioner DyPartitioner;
            std::vector<int> graph_route_count;
            std::vector<int> random_route_count;
            std::vector<int> match_count;
            int partition_limit = 20000;
            int txn_count = 0;
            int partition_count = 0;
            bool running = true;
            bool metis = false;
            bool can_generate = false;
            int partition_time = 0;
            std::shared_mutex mtx;
            spdlog::logger &router_logger;
            spdlog::logger &txn_logger;
            std::shared_mutex mtx_epoch_router;
            double epoch_router;
            std::chrono::high_resolution_clock::time_point epoch_start;

            Router(Graph G, spdlog::logger &router_logger, spdlog::logger &txn_logger) : DyPartitioner(G, 1.0, FLAGS_nodes, 0.5, 1.5), router_logger(router_logger), txn_logger(txn_logger)
            {
                running = true;
                metis = false;
                graph_route_count.reserve(FLAGS_messageHandlerThreads);
                random_route_count.reserve(FLAGS_messageHandlerThreads);
                match_count.reserve(FLAGS_messageHandlerThreads);
                for (int i = 0; i < int(FLAGS_messageHandlerThreads); i++)
                {
                    graph_route_count.push_back(0);
                    random_route_count.push_back(0);
                    match_count.push_back(0);
                }
                DyPartitioner.cluster.reserve(int(FLAGS_nodes));
                for (int j = 0; j < int(FLAGS_nodes); j++)
                {
                    std::unordered_set<idx_t> part;
                    DyPartitioner.cluster.push_back(part);
                }
                // std::vector<std::vector<int>> graph_txns;
                // DyPartitioner.get_txns("/home/user/project/database/demo/Proxy/Proxy/backend/Proxy/Logs/graph_key", graph_txns);
                // for (const auto &txn : graph_txns)
                // {
                //     DyPartitioner.generate_stamps(txn);
                // }
                // DyPartitioner.MetisPart();
                // DyPartitioner.get_metis_map();
                // metis= true;
                epoch_start = std::chrono::high_resolution_clock::now();
                startPatitionThread();
            }
            ~Router()
            {
                running = false;
            }

            void pushToQueue(std::vector<TxnNode> &txnnodelist);
            void generateStamps();
            void metisPart();
            void dyPartition();
            void startPatitionThread();
            void create_table_partitioner();
            int get_num(const std::vector<int> &a)
            {
                int num = 0;
                for (const auto &ele : a)
                {
                    num += ele;
                }
                return num;
            }
            uint64_t router(std::vector<TxnNode> &txnnodelist, int t_i, bool isRouter, bool &is_route);
            void writeToFile(const std::unordered_map<i64, int> *part_map, const std::string &filename)
            {
                std::ofstream outfile(filename);
                if (!outfile)
                {
                    std::cerr << "Failed to open file: " << filename << std::endl;
                    return;
                }

                int count = 0;
                while(count < 5000)
                {
                    // 只打印前 5000 
                    if(part_map->find(count) != part_map->end()){
                        outfile << count << " : " << part_map->at(count) << std::endl;
                    }
                    count++;
                }

                outfile.close();
            }
        };
    }
}
