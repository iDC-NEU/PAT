#pragma once
#include "types.hpp"
using namespace scalestore;
struct ScaleStoreAdapter
{
    using BTree = storage::BTree<K, V>;
    std::string name;
    PID tree_pid;
    bool created = false;
    bool traversed = false;
    std::map<i64, i64> *partition_map;
    std::unordered_map<i64, i64> *update_map;
    ScaleStoreAdapter(){};
    ScaleStoreAdapter(ScaleStore &db, std::string name) : name(name)
    {
        auto &catalog = db.getCatalog();
        if (db.getNodeID() == 0)
        {
            db.createBTree<K, V>();
        }
        tree_pid = catalog.getCatalogEntry(BTREE_ID).pid;
    };

    void insert(K key, V payload)
    {
        BTree tree(tree_pid);
        tree.insert(key, payload);
    }

    bool lookup_opt(K key, V &payload){
        BTree tree(tree_pid);
        return tree.lookup_opt(key, payload);
    }

    void create_partitioner()
    {
        BTree tree(tree_pid);
        std::cout << "ycsb_partmap_size: " << partition_map->size() << std::endl;
        auto pos = partition_map->begin();
        pos++;
        i64 offset = 50;
        auto last_pair = partition_map->begin()->first;
        auto last_part = partition_map->begin()->second;
        auto pair = last_pair + offset;
        while (pos != partition_map->end())
        {
            if (partition_map->find(pair) != partition_map->end())
            {
                if (partition_map->at(pair) == last_part)
                {
                    pair += offset;
                    pos++;
                }
                else
                {
                    // std::cout <<"pair " << pair << "last_pair " << last_pair <<std::endl;
                    tree.update_metis_index({last_pair}, {pair}, last_part);
                    pos++;
                    last_pair = pair;
                    last_part = partition_map->at(pair);
                    pair = last_pair + offset;
                }
            }
            else
            {
                // std::cout <<"pair " << pair << "last_pair " << last_pair <<std::endl;
                tree.update_metis_index({last_pair}, {pair}, last_part);
                last_pair = pos->first;
                last_part = pos->second;
                pair = last_pair + offset;
                pos++;
            }
        }
        tree.update_metis_index({last_pair}, {pair}, last_part);
        created = true;
    }

    void update_partitioner()
    {
        BTree tree(tree_pid);
        for (const auto &pair : *update_map)
        {
            tree.update_metis_index({pair.first}, {pair.first + 50}, pair.second);
        }
        update_map = nullptr;
    }

    void page_count()
    {

        std::string filename = "/root/home/AffinityDB/ScaleStore/Logs/ycsb_info";
        std::ofstream output(filename);
        BTree tree(tree_pid);
        int size = tree.page_count();
        output << "old_page_size: " << size << std::endl;
        std::cout << "tpcc_partmap_size: " << partition_map->size() << std::endl;
        auto pos = partition_map->begin();
        pos++;
        i64 offset = 50;
        auto last_pair = partition_map->begin()->first;
        auto last_part = partition_map->begin()->second;
        auto pair = last_pair + offset;
        while (pos != partition_map->end())
        {
            if (partition_map->find(pair) != partition_map->end())
            {
                if (partition_map->at(pair) == last_part)
                {
                    pair += offset;
                    pos++;
                }
                else
                {
                    // std::cout <<"pair " << pair << "last_pair " << last_pair <<std::endl;
                    tree.update_metis_index({last_pair}, {pair}, last_part);
                    pos++;
                    last_pair = pair;
                    last_part = partition_map->at(pair);
                    pair = last_pair + offset;
                }
            }
            else
            {
                // std::cout <<"pair " << pair << "last_pair " << last_pair <<std::endl;
                tree.update_metis_index({last_pair}, {pair}, last_part);
                last_pair = pos->first;
                last_part = pos->second;
                pair = last_pair + offset;
                pos++;
            }
        }
        tree.update_metis_index({last_pair}, {pair}, last_part);
        created = true;
        std::cout << "increase_count: " << tree.increase_count << std::endl;
        output << "increase_count: " << tree.increase_count << std::endl;
        output << "new_page_count: " << tree.increase_count + size << std::endl;
    }

    void traverse_tree()
    {
        std::string filename = "/root/home/AffinityDB/ScaleStore/Logs/ycsb_info";
        BTree tree(tree_pid);
        tree.btree_traversal(filename);
        traversed = true;
    }
};