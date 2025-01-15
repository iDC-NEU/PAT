#include "router.hpp"
#include <thread>
#include <unistd.h>
#include "../utils/FNVHash.hpp"
namespace Proxy
{
    namespace router
    {
        void Router::generateStamps()
        {
            DyPartitioner.generate_stamps(txnnodelist);
            txnnodelist.clear();
        }

        void Router::metisPart()
        {
            std::unordered_map<idx_t, int> &part_map = DyPartitioner.partmapA;
            std::unique_lock<std::shared_mutex> writeLock(mtx);
            auto dy_start = std::chrono::high_resolution_clock::now();
            // DyPartitioner.WriteCountFile();

            DyPartitioner.MetisPart();
            DyPartitioner.clear_epoch_set();

            int cnt = part_map.size();
            router_logger.info(fmt::format("初始图划分后顶点数_cluster统计:,{};顶点数_number_of_nodes:{}", cnt, DyPartitioner.get_graph().number_of_nodes()));
            auto dy_end = std::chrono::high_resolution_clock::now();
            std::chrono::high_resolution_clock::time_point epoch_end = std::chrono::high_resolution_clock::now();

            router_logger.info(fmt::format("epoch_time ={}s", 1.0 * std::chrono::duration_cast<std::chrono::microseconds>(epoch_end - epoch_start).count() / 1000000));
            router_logger.info(fmt::format("dy_time ={}s dy_time/epochtime={}", 1.0 * std::chrono::duration_cast<std::chrono::microseconds>(dy_end - dy_start).count() / 1000000, (1.0 * std::chrono::duration_cast<std::chrono::microseconds>(dy_end - dy_start).count() / 1000) / (1.0 * std::chrono::duration_cast<std::chrono::microseconds>(epoch_end - epoch_start).count() / 1000)));
            router_logger.info(fmt::format("router_time ={}s router_time/epochtime={}", epoch_router, epoch_router / (1.0 * std::chrono::duration_cast<std::chrono::microseconds>(epoch_end - epoch_start).count() / 1000)));

            epoch_router = 0;
            metis = true;
            epoch_start = std::chrono::high_resolution_clock::now();
        }

        void Router::dyPartition()
        {
            std::unique_lock<std::shared_mutex> writeLock(mtx);
            DyPartitioner.start_epoch_partition();
            DyPartitioner.clear_epoch_set();
            epoch_router = 0;
        }
        
        uint64_t Router::hash_router(std::vector<TxnNode> &txnnodelist, int t_i)
        {

            DyPartitioner.old_map_use[t_i] = true;
            DyPartitioner.new_map_use[t_i] = true;
            DyPartitioner.update_mutex.lock();
            if (!DyPartitioner.change_map)
            {
                DyPartitioner.new_map_use[t_i] = false;
                // std::cout<<"part_mapA_size:" <<part_map->size()<<std::endl;
            }
            else
            {
                DyPartitioner.old_map_use[t_i] = false;
                // std::cout<<"part_mapB_size:" <<part_map->size()<<std::endl;
            }
            DyPartitioner.update_mutex.unlock();
            // 创建并初始化哈希计数数组
            std::vector<int> hash_count(FLAGS_nodes, 0);

            // 根据键值的哈希分布统计计数
            for (const auto& txn_node : txnnodelist) {
                auto hash_key = scalestore::utils::FNV::hash(txn_node.key);
                hash_count[hash_key % FLAGS_nodes]++;
            }

            // 返回哈希分布中最大值对应的索引
            return std::distance(hash_count.begin(), 
                                std::max_element(hash_count.begin(), hash_count.end()));
        }

        uint64_t Router::router(std::vector<TxnNode> &txnnodelist, int t_i, bool isRouter, bool &is_route)
        {

            DyPartitioner.old_map_use[t_i] = true;
            DyPartitioner.new_map_use[t_i] = true;
            bool is_old_map;
            std::unordered_map<idx_t, int> *part_map;
            DyPartitioner.update_mutex.lock();
            if (!DyPartitioner.change_map)
            {
                DyPartitioner.new_map_use[t_i] = false;
                is_old_map = true;
                part_map = &DyPartitioner.partmapB;
                // std::cout<<"part_mapA_size:" <<part_map->size()<<std::endl;
            }
            else
            {
                DyPartitioner.old_map_use[t_i] = false;
                is_old_map = false;
                part_map = &DyPartitioner.partmapA;
                // std::cout<<"part_mapB_size:" <<part_map->size()<<std::endl;
            }
            DyPartitioner.update_mutex.unlock();
            if (!isRouter || txnnodelist.empty())
            {
                is_route = false;
                return rand() % FLAGS_nodes;
            }
            int size = txnnodelist.size();
            if (part_map->empty())
            {
                is_route = false;
                // if (size > 1)
                // {
                //     pushToQueue(txnnodelist);
                // }
                return rand() % FLAGS_nodes;
            }
            // if(size == 1){
            //     int id = txnnodelist[0]/50;
            //     auto iter = part_map->find(id);
            //     if(iter != part_map->end()){
            //         is_route = true;
            //         return iter->second;
            //     }
            //     is_route = false;
            //     int dest_nodeid = rand()% FLAGS_nodes;
            //     part_map->insert({id, dest_nodeid});
            // }
            // std::chrono::high_resolution_clock::time_point start, end;
            // start = std::chrono::high_resolution_clock::now();
            // std::vector<int> partIds(FLAGS_nodes, 0);
            // for(const auto& key : txnnodelist){
            //     int id =key/50;
            //     auto iter = part_map->find(id);
            //     if (iter != part_map->end())
            //     {
            //         partIds[iter->second]++;
            //     }
            // }
            // double ratio = 0;
            // if(partIds[0]==0 || partIds[1]==0){
            //     ratio = 1;
            // }
            // else{
            //     ratio = double(partIds[0])/partIds[1];
            //     ratio = ratio>0.5? ratio : 1-ratio;
            // }
            // router_logger.info(fmt::format("confuse: {}%", ratio*100));
            int result = -1;
            int count = 0;
            while (count < 4)
            {
                int id = rand() % size;
                auto iter = part_map->find((txnnodelist[id].key) / FLAGS_stamp_len);
                if (iter != part_map->end())
                {
                    result = iter->second;
                    break;
                }
                count++;
            }
            if (is_old_map)
                DyPartitioner.old_map_use[t_i] = false;
            else
                DyPartitioner.new_map_use[t_i] = false;

            // pushToQueue(txnnodelist);            // txnlist使用完之后再传给划分线程，此时可以直接传入引用来避免复制开销
            // 请勿在此后继续使用txnnodelist

            if (count == 4)
            {
                is_route = false;
                return rand() % FLAGS_nodes;
            }
            is_route = true;
            return result;
            // auto maxelement = std::max_element(partIds.begin(), partIds.end());
            // if(*maxelement == 0){ is_route = false; return rand()%FLAGS_nodes;}
            // is_route = true;
            // return std::distance(partIds.begin(), maxelement);

            // int k = 0;
            // for(const auto &ele : elementCount){
            //     router_logger.info(fmt::format("part{} partcount{}", k, ele));
            //     k++;
            // }
        }

        void Router::startPatitionThread()
        {
            std::thread partitoner([&]()
                                   {
                while(running){
                    queue_guard.lock();
                    if(!txn_queue.empty()){
                        txnnodelist = txn_queue.front();
                        txn_queue.pop();
                        queue_guard.unlock();
                        generateStamps();
                        partition_count++;
                        if(FLAGS_partition_mode == 2 && metis && partition_count % 1000 == 0){
                            dyPartition();
                        }
                        if(partition_count > partition_limit){
                            partition_count = 0;
                            if(!metis){
                                router_logger.info("Start Metis Part");
                                txn_logger.info("Start Metis Part");
                                metisPart();
                                router_logger.info("Metis OK");
                                txn_logger.info("Metis OK");
                                txn_logger.info("Start Update router map");
                                DyPartitioner.get_metis_map();
                                txn_logger.info("router map updated");
                                router_logger.info("Update Metis map");
                                partition_limit = 20000;
                            }
                            else if(FLAGS_partition_mode == 2){
                                DyPartitioner.update_old_map();
                            }
                        }
                        // if(txn_count >= count_num){
                        //     auto end = std::chrono::high_resolution_clock::now();
                        //     auto txn = txn_count;
                        //     txn_count = 0;
                        //     auto time_cost = 1.0 * std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000000;
                        //     start = std::chrono::high_resolution_clock::now();
                        //     DyPartitioner.update_mutex.lock();
                        //     int graph_count = get_num(graph_route_count);
                        //     int random_count = get_num(random_route_count);
                        //     int match = get_num(match_count);
                        //     DyPartitioner.update_mutex.unlock();
                        //     router_logger.info(fmt::format("random_route_count ={} graph_route_count={}", random_count, graph_count));
                        //     router_logger.info(fmt::format("match_rate ={}",float(match)/graph_count));
                        //     router_logger.info(fmt::format("edge_count ={}",DyPartitioner.edge_count));
                        //     router_logger.info(fmt::format("avg_throughput ={}/s", txn/time_cost));
                        //     router_logger.info(fmt::format("time_cost: ={}/s", time_cost));
                        //     DyPartitioner.edge_count=0;
                        //     DyPartitioner.update_mutex.lock();
                        //     for(int i = 0; i<int(FLAGS_messageHandlerThreads); i++){
                        //         graph_route_count[i] = 0;
                        //         random_route_count[i] = 0;
                        //         match_count[i] = 0;
                        //     }
                        //     DyPartitioner.update_mutex.unlock();
                        // }
                    // if(metis==false){
                    //     if(DyPartitioner.stampinfo_map.size()>=1000)//100000
                    //     {
                    //         //DyPartitioner.partition_mutex.lock();
                    //         metisPart();
                    //         //DyPartitioner.partition_mutex.unlock();
                    //  int graph_count = get_num(graph_route_count);
                    //         router_logger.info(fmt::format("random_route_count ={} graph_route_count={}", get_num(random_route_count), graph_count));
                    //         router_logger.info(fmt::format("match_rate ={}",float(get_num(match_count))/graph_count));
                    //         DyPartitioner.get_metis_map();
                
                    //     }
                    // }else{
                    //     dyPartition();
                    //     partition_time++;
                    //     // if(partition_time>=500){
                    //     //     partition_time=0;
                    //     //     int graph_count = get_num(graph_route_count);
                    //     //     router_logger.info(fmt::format("random_route_count ={} graph_route_count={}", get_num(random_route_count), graph_count));
                    //     //     router_logger.info(fmt::format("match_rate ={}",float(get_num(match_count))/graph_count));
                    //     //     DyPartitioner.update_old_map();
                    //     // }
                    //     if(DyPartitioner.get_epoch_set_size()>=1000){
                    //         //DyPartitioner.partition_mutex.lock();
                    //         dyPartition();
                    //         //DyPartitioner.partition_mutex.unlock();
                    //         DyPartitioner.update_old_map();
                    //         int graph_count = get_num(graph_route_count);
                    //         router_logger.info(fmt::format("random_route_count ={} graph_route_count={}", get_num(random_route_count), graph_count));
                    //         router_logger.info(fmt::format("match_rate ={}",float(get_num(match_count))/graph_count));
                    //     }
                    // }

                    }
                    else{
                        queue_guard.unlock();
                    }
                } });
            partitoner.detach();
        }

        void Router::pushToQueue(std::vector<TxnNode> &txnnodelist)
        {
            // 在函数内部对传入的引用进行移动操作
            if (txn_queue.size() < 2000)
            {
                queue_guard.lock();
                txn_queue.push(std::move(txnnodelist));
                queue_guard.unlock();
            }
            txn_count++;
        }
    }
}