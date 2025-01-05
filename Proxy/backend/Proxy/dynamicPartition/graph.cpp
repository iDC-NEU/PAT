#include "graph.hpp"
#include <unistd.h>
namespace Proxy
{
    namespace router
    {
        int commomValue(const std::unordered_set<int> &a, const std::unordered_map<int, int> &b)
        {
            int count = 0;
            for (const auto elem : b)
            {
                if (a.find(elem.first) != a.end())
                {
                    count+= elem.second;
                }
            }
            return count;
        }
        /*
        Graph definition
        */
        const std::unordered_map<int, std::unordered_map<int, int>> Graph::get_graph()
        {
            return graph_;
        }
        Graph::Graph(const Graph &other)
        {
            this->graph_ = other.graph_;
        }
        Graph &Graph::operator=(const Graph &other)
        {
            this->graph_ = other.graph_;
            return *this;
        }
        void Graph::add_edge(int v_i, int v_j, double weight)
        {
            if (graph_[v_i].find(v_j) == graph_[v_i].end())
            {
                graph_[v_i].insert({v_j, weight});
                graph_[v_j].insert({v_i, weight});
            }
            else
            {
                graph_[v_i][v_j] += weight;
                graph_[v_j][v_i] += weight;
            }
        }
        bool Graph::has_node(int vid)
        {
            return graph_.find(vid) == graph_.end() ? false : true;
        }
        int Graph::number_of_nodes()
        {
            return graph_.size();
        }
        std::unordered_map<int, int> Graph::neighbors(int vid)
        {
            return graph_[vid];
        }
        /*
        DynamicPartitioner definition
        */
        DynamicPartitioner::DynamicPartitioner(Graph G, double balance_factor, int k, double alpha, double gamma)
            : partmap(this->partmapA)
        {
            this->G = G;
            this->balance_factor = balance_factor;
            this->k = k;
            this->alpha = alpha;
            this->gamma = gamma;
            this->vertex_num = 0;

            this->change_g_time = 0;
            this->fennel_time = 0;
            this->fine_tuned_fennel_time = 0;
            this->fennel_score_time = 0;
            old_map_use.reserve(FLAGS_messageHandlerThreads);
            new_map_use.reserve(FLAGS_messageHandlerThreads);
            for (int i = 0; i < int(FLAGS_messageHandlerThreads); i++)
            {
                old_map_use.push_back(false);
                new_map_use.push_back(false);
            }
        }
        double DynamicPartitioner::fennel([[maybe_unused]] int vid, const std::unordered_map<int, int> &neighbors)
        {
            [[maybe_unused]] int load_limit = int(balance_factor * vertex_num / k);
            double *score = new double[k]();

            for (int i = 0; i < k; ++i)
            {
                score[i] = commomValue(cluster[i], neighbors) - alpha * pow(cluster[i].size() + 1.0, gamma) + alpha * pow(cluster[i].size(), gamma);
                // score[i] = commomValue(cluster[i], neighbors);
            }
            int max_position = std::distance(score, std::max_element(score, score + k));
            delete[] score;
            return max_position;
        }
        void DynamicPartitioner::add_node(int vid)
        {
            // 只重新计算了vid 的分区，未计算受影响的neighbors的新分区
            int p = fennel(vid, G.neighbors(vid));
            if (cluster[p].find(vid) != cluster[p].end()) // 如果分区没变直接return
                return;
            for (auto &part : cluster)
            {
                if (part.find(vid) != part.end())
                {
                    transfer_count++;
                    transfer_node.insert(vid);
                    part.erase(vid);
                    partmap.erase(vid);
                    break;
                }
            }
            cluster[p].insert(vid);
            partmap.insert({vid, p});
            if (new_insert_keys.find(vid) == new_insert_keys.end())
            {
                new_insert_keys.insert({vid, p});
            }
            else
            {
                new_insert_keys[vid] = p;
            }
            int key = vid * 50 + 1;
            if (key <= customer_key_max)
            {
                customer_insert_keys.insert({key, p});
            }
            else if (customer_key_max < key && key <= order_key_max)
            {
                order_insert_keys.insert({key - customer_key_max, p});
            }
            else if (order_key_max < key && key <= stock_key_max)
            {
                stock_insert_keys.insert({key - order_key_max, p});
            }
            else if (stock_key_max < key && key <= warehouse_key_max)
            {
                warehouse_insert_keys.insert({(key - stock_key_max - 1) / 50, p});
            }
            else if (warehouse_key_max < key && key <= district_key_max)
            {
                district_insert_keys.insert({key - warehouse_key_max, p});
            }
            else
            {
                neworder_insert_keys.insert({key - district_key_max, p});
            }
        }
        void DynamicPartitioner::add_node(int vid, std::unordered_map<int, int> &neighbors)
        {
            for (auto nbr : neighbors)
            {
                G.add_edge(vid, nbr.first, nbr.second);
            }
            // 只重新计算了vid 的分区，未计算受影响的neighbors的新分区
            int p = fennel(vid, G.neighbors(vid));
            if (cluster[p].find(vid) != cluster[p].end())
                return;
            for (auto &part : cluster)
            {
                if (part.find(vid) != part.end())
                {
                    part.erase(vid);
                    break;
                }
            }
            partmap.erase(vid);
            cluster[p].insert(vid);
            partmap.insert({vid, p});
        }
        void DynamicPartitioner::run()
        {
            // 初始是metis划分结果
            std::string metis_result_path = "/mnt/data_utils/data/metis_par_stamp50_fre=1p0_part4.txt";
            std::fstream fin;
            fin.open(metis_result_path, std::ios::in);
            std::string line;
            assert(fin.is_open() == true);
            while (getline(fin, line))
            {
                line = line.replace(line.find(":"), 1, "");
                // line.find("\n") == -1 ? std::cout << "false\n" : std::cout << "true\n";
                // line = line.replace(line.find("\n"), 1, "");
                std::stringstream ss(line);
                std::vector<int> words;
                int word;
                while (ss >> word)
                {
                    words.push_back(word);
                }
                cluster[words[0]].insert(words.begin() + 1, words.end());
                words.clear();
            }
            fin.close();

            // 添加metis划分的初始图
            std::string metis_graph = "/mnt/data_utils/data/stamp_edge50fre=1part_0.txt";
            fin.open(metis_graph, std::ios::in);
            assert(fin.is_open() == true);
            while (getline(fin, line))
            {
                // line = line.replace(line.find("\n"), 1, "");
                std::stringstream ss(line);
                int word;
                ss >> word;
                int u, v;
                double weight;
                u = word;
                while (ss >> word)
                {
                    v = word;
                    ss >> word;
                    weight = word;
                    G.add_edge(u, v, weight);
                }
            }
            fin.close();
            int cnt = partmap.size();
            std::cout << "初始顶点数_cluster统计：" << cnt << std::endl;
            std::cout << "初始顶点数_number_of_nodes：" << G.number_of_nodes() << std::endl;

            std::string cfg_path = "/mnt/data_utils/data/batch_mode.cfg";
            std::fstream cfg;
            std::string batch_path;
            int batch_count = 0;
            cfg.open(cfg_path, std::ios::in);
            while (getline(cfg, batch_path))
            {
                if (batch_path[0] == '#')
                    continue;
                std::cout << "process batch " + batch_path << std::endl;
                clock_t start, end;
                start = clock();
                fin.open(batch_path, std::ios::in);
                while (getline(fin, line))
                {
                    // line = line.replace(line.find("\n"), 1, "");
                    // line.erase(line.find_last_not_of(' ') + 1, std::string::npos);
                    std::stringstream ss(line);
                    int word;
                    ss >> word;
                    int u = word;
                    double weight;
                    std::unordered_map<int, int> nbrs;
                    while (ss >> word)
                    {
                        ss >> weight;
                        nbrs.insert({word, weight});
                    }
                    add_node(u, nbrs);
                    nbrs.clear();
                }
                fin.close();
                end = clock();
                std::cout << "time = " << double(end - start) / CLOCKS_PER_SEC << "s" << std::endl;
                cnt = partmap.size();
                std::cout << "处理一个batch之后的顶点数_cluster统计：" << cnt << std::endl;
                std::cout << "处理一个batch之后的顶点数_number_of_nodes：" << G.number_of_nodes() << std::endl;
                // 写结果
                std::fstream fout("/mnt/data_utils/output/" + std::to_string(batch_count) + ".txt", std::ios::out);
                for (int i = 0; i < partition_num; i++)
                {
                    fout << i << ':';
                    for (auto vid : cluster[i])
                    {
                        fout << ' ' << vid;
                    }
                    fout << '\n';
                }
                batch_count++;
            }
        }
        void DynamicPartitioner::generate_stamps(const std::vector<TxnNode> &txn_node_list)
        {
            std::unordered_map<int, bool> temps;
            int count = 0;
            for (const auto &node : txn_node_list)
            {
                int stamp_id = (node.key - 1) / stamp_len; // 根据关键字生成唯一的图顶点标识
                epoch_vids.insert(stamp_id);               // 记录epoch需要划分的顶点集
                if (stampinfo_map.find(stamp_id) == stampinfo_map.end())
                {
                    StampInfo stamp(stamp_id);
                    // int w_id = get_wid(key);
                    // stamp.set_w_id(w_id);
                    stamp.add_all_count(node.weight);
                    stampinfo_map.insert({stamp_id, stamp});
                    count+= node.weight;
                }
                else
                {
                    stampinfo_map[stamp_id].add_all_count(node.weight);
                    count+= node.weight;
                }
                if (!G.has_node(stamp_id))
                {
                    G.add_node(stamp_id);
                }
                if (temps.find(stamp_id) == temps.end())
                {
                    for (auto &tmp : temps)
                    {
                        if (tmp.second)
                        {
                            if (node.is_read_only)
                            {
                                G.add_edge(stamp_id, tmp.first, 1);
                            }
                        }
                        else
                        {
                            G.add_edge(stamp_id, tmp.first, 1);
                        }
                        edge_count++;
                    }
                    temps.insert({stamp_id, node.is_read_only});
                }
            }
            if (temps.size() == 1)
            {
                int id = temps.begin()->first;
                StampInfo &stamp = stampinfo_map[id];
                stamp.sub_all_count(count);
                if (stamp.all_count() <= 0)
                {
                    stampinfo_map.erase(id);
                    G.remove_node(id);
                }
            }
        }
        void DynamicPartitioner::start_epoch_partition()
        {
            for (const auto &vid : epoch_vids)
            {
                add_node(vid);
            }
        }

        void DynamicPartitioner::get_txns(std::string input_filename, std::vector<std::vector<int>> &txns)
        {
            std::ifstream txn_file(input_filename);
            std::string line;
            if (!txn_file)
            {
                std::cerr << "无法打开文件" << std::endl;
                return;
            }
            while (std::getline(txn_file, line))
            {
                std::istringstream iss(line);
                std::string tep;
                std::vector<int> txn_key_list;
                int w_id, key;
                iss >> w_id;
                while (iss >> key)
                {
                    txn_key_list.push_back(key);
                }
                txns.push_back(txn_key_list);
            }
            std::cout << "成功生成txns" << std::endl;
        }

        void DynamicPartitioner::get_metis_map()
        {
            change_map = true;
            int i = 0;
            int size = old_map_use.size();
            while (1)
            {
                if (!old_map_use[i])
                    i++;
                if (i == size)
                    break;
            }
            partmapB = partmapA;
            change_map = false;
            i = 0;
            while (1)
            {
                if (!new_map_use[i])
                    i++;
                if (i == size)
                    break;
            }
        }

        void DynamicPartitioner::update_old_map()
        {
            std::cout << "update_old_map" << std::endl;
            has_send_new_insert_keys = false;
            partition_mutex.lock();
            int i = 0;
            change_map = true;
            int size = old_map_use.size();
            while (1)
            {
                if (!old_map_use[i])
                    i++;
                if (i == size)
                    break;
            }
            partmapB.insert(new_insert_keys.begin(), new_insert_keys.end());
            // for (const auto &pair : new_remove_keys)
            // {
            //     partmapB.erase(pair.first);
            // }
            std::cout << partmapB.size() << std::endl;
            change_map = false;
            new_insert_keys.clear();
            new_remove_keys.clear();
            partition_mutex.unlock();
            i = 0;
            while (1)
            {
                if (!new_map_use[i])
                    i++;
                if (i == size)
                    break;
            }
            while (!has_send_new_insert_keys)
            {
                sleep(1);
            }
            partition_mutex.lock();
            std::cout << "clear" << std::endl;
            customer_insert_keys.clear();
            order_insert_keys.clear();
            stock_insert_keys.clear();
            warehouse_insert_keys.clear();
            district_insert_keys.clear();
            neworder_insert_keys.clear();
            partition_mutex.unlock();
        }

        void DynamicPartitioner::create_table_partitioner()
        {
            for (const auto &pair : partmap)
            {
                if (pair.first * 50 <= customer_key_max)
                {
                    customer_map.insert({pair.first, pair.second});
                }
            }
        };

    }
}