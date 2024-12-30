#pragma once
// -------------------------------------------------------------------------------------
#include "scalestore/Config.hpp"
#include "scalestore/ScaleStore.hpp"
// -------------------------------------------------------------------------------------
#include <cassert>
#include <cstdint>
#include <cstring>
#include <map>
#include <string>
#include "schema.hpp"
#include <gflags/gflags.h>

/*----------------------------------------------------------------------------------------*/
// Generate Transaction Key

// const int w_count = FLAGS_tpcc_warehouse_count;
using Range = std::pair<Integer, Integer>;

// [1-w_count][01-10][0001-3000]
int gen_customer_key(Integer c_w_id, Integer c_d_id, Integer c_id)
{
   return 1'000'000 * c_w_id + 1'0000 * c_d_id + c_id;
}
int gen_customer_key(const customer_t::Key &key)
{
   return gen_customer_key(key.c_w_id, key.c_w_id, key.c_id);
}
// int customer_key_min = 1'000'000 * 1 + 1'0000 * 1 + 1;
// int customer_key_max = 1'000'000 * FLAGS_tpcc_warehouse_count + 1'0000 * 10 + 3000;

// [w_count][10][3000] + [1-w_count][01-10][0001-3000]
int gen_order_key(Integer o_w_id, Integer o_d_id, Integer o_id)
{
   return gen_customer_key(FLAGS_tpcc_warehouse_count, 10, 3000) + 1'000'000 * o_w_id + 1'0000 * o_d_id + o_id;
}
int gen_order_key(const order_t::Key &key)
{
   return gen_order_key(key.o_w_id, key.o_d_id, key.o_id);
}
// int order_key_min = customer_key_max + 1'000'000 * 1 + 1'0000 * 1 + 1;
// int order_key_max = customer_key_max + 1'000'000 * FLAGS_tpcc_warehouse_count + 1'0000 * 10 + 3000;

//                                                                  |--------- ITEM_NO
// [w_count][10][3000] + [w_count][10][3000] + [1-w_count][000001-100000]
int gen_stock_key(Integer s_w_id, Integer s_i_id)
{
   return gen_order_key(FLAGS_tpcc_warehouse_count, 10, 3000) + 1'000'000 * s_w_id + s_i_id;
}
int gen_stock_key(const stock_t::Key &key)
{
   return gen_stock_key(key.s_w_id, key.s_i_id);
}
// int stock_key_min = order_key_max + 1'000'000 * 1 + 1;
// int stock_key_max = order_key_max + 1'000'000 * FLAGS_tpcc_warehouse_count + 100000;

// TODO: 感觉需要模仿上述gen_xx_key()，之后补上

// int get_wid(int key) {
//    if(customer_key_min <= key && key <= customer_key_max) {
//       return key / 1'000'000;
//    }

//    if(order_key_min <= key && key <= order_key_max) {
//       return (key - customer_key_max) / 1'000'000;
//    }

//    if(stock_key_min <= key && key <= stock_key_max) {
//       return (key - order_key_max) / 1'000'000;
//    }
//    printf("Invalid Key\n");
//    return -1;
// }

using namespace scalestore;

template <class Record>
struct ScaleStoreAdapter
{
   using BTree = storage::BTree<typename Record::Key, Record>;
   std::string name;
   PID tree_pid;
   bool created = false;
   int type = -1;
   bool traversed = false;
   std::map<Integer, Integer> *partition_map;
   std::unordered_map<Integer, Integer> *update_map;
   std::unordered_map<u64, int> page_map;
   ScaleStoreAdapter(){};
   ScaleStoreAdapter(ScaleStore &db, std::string name) : name(name)
   {
      auto &catalog = db.getCatalog();
      if (db.getNodeID() == 0)
      {
         db.createBTree<typename Record::Key, Record>();
      }
      tree_pid = catalog.getCatalogEntry(Record::id).pid;
      switch (name)
      {
      case "warehouse":
         type = 0;
         break;
      case "district":
         type = 1;
         break;
      case "customer":
         type = 2;
         break;
      case "history":
         type = 3;
         break;
      case "neworder":
         type = 4;
         break;
      case "order":
         type = 5;
         break;
      case "orderline":
         type = 6;
         break;
      case "item":
         type = 7;
         break;     
      case "stock":
         type = 8;
         break;                          
      default:
         break;
      }
   };

   void create_partitioner()
   {
      BTree tree(tree_pid);
      std::cout << "tpcc_partmap_size: " << partition_map->size() << std::endl;
      auto pos = partition_map->begin();
      pos++;
      Integer offset = 50;
      if (Record::id == 0)
      {
         offset = 1;
      }
      auto last_pair = partition_map->begin()->first;
      auto last_part = partition_map->begin()->second;
      Integer pair = last_pair + offset;
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

   void page_count(){
      std::string filename;
      if (Record::id == 2)
      {
         filename = "/root/home/AffinityDB/ScaleStore/Logs/customer_info";
      }
      else if (Record::id == 0)
      {
         filename = "/root/home/AffinityDB/ScaleStore/Logs/warehouse_info";
      }
      else if (Record::id == 1)
      {
         filename = "/root/home/AffinityDB/ScaleStore/Logs/district_info";
      }
      else if (Record::id == 5)
      {
         filename = "/root/home/AffinityDB/ScaleStore/Logs/neworder_info";
      }
      else if (Record::id == 6)
      {
         filename = "/root/home/AffinityDB/ScaleStore/Logs/order_info";
      }
      else if (Record::id == 10)
      {
         filename = "/root/home/AffinityDB/ScaleStore/Logs/stock_info";
      }
      else
      {
         traversed = true;
         return;
      }
      std::ofstream output(filename);
      BTree tree(tree_pid);
      int size = tree.page_count();
      output << "old_page_size: " << size << std::endl;
      std::cout << "tpcc_partmap_size: " << partition_map->size() << std::endl;
      auto pos = partition_map->begin();
      pos++;
      Integer offset = 50;
      if (Record::id == 0)
      {
         offset = 1;
      }
      auto last_pair = partition_map->begin()->first;
      auto last_part = partition_map->begin()->second;
      Integer pair = last_pair + offset;
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
      std::cout<< "increase_count: " << tree.increase_count << std::endl;
      output<< "increase_count: " << tree.increase_count << std::endl;
      output<< "new_page_count: " << tree.increase_count + size << std::endl;
   }
   void get_page_ids(){
      for(const auto& page_id : tree.page_ids){
         page_map.insert({page_id, type});
      }
      tree.page_ids.clear();
   }

   void traverse_tree()
   {
      std::string filename;
      if (Record::id == 2)
      {
         filename = "/root/home/AffinityDB/ScaleStore/Logs/customer_info";
      }
      else if (Record::id == 0)
      {
         filename = "/root/home/AffinityDB/ScaleStore/Logs/warehouse_info";
      }
      else if (Record::id == 1)
      {
         filename = "/root/home/AffinityDB/ScaleStore/Logs/district_info";
      }
      else if (Record::id == 5)
      {
         filename = "/root/home/AffinityDB/ScaleStore/Logs/neworder_info";
      }
      else if (Record::id == 6)
      {
         filename = "/root/home/AffinityDB/ScaleStore/Logs/order_info";
      }
      else if (Record::id == 10)
      {
         filename = "/root/home/AffinityDB/ScaleStore/Logs/stock_info";
      }
      else
      {
         traversed = true;
         return;
      }
      BTree tree(tree_pid);
      tree.btree_traversal(filename);
      traversed = true;
   }

   void traverse_page()
   {
      std::string filename;
      switch (Record::id)
      {
      case 0:
         filename = "/root/home/AffinityDB/ScaleStore/Logs/warehouse_page";
         break;
      case 1:
         filename = "/root/home/AffinityDB/ScaleStore/Logs/district_page";
         break;
      case 2:
         filename = "/root/home/AffinityDB/ScaleStore/Logs/customer_page";
         break;
      case 3:
         filename = "/root/home/AffinityDB/ScaleStore/Logs/customer_wdl_page";
         break;
      case 4:
         filename = "/root/home/AffinityDB/ScaleStore/Logs/history_page";
         break;
      case 5:
         filename = "/root/home/AffinityDB/ScaleStore/Logs/neworder_page";
         break;
      case 6:
         filename = "/root/home/AffinityDB/ScaleStore/Logs/order_page";
         break;
      case 7:
         filename = "/root/home/AffinityDB/ScaleStore/Logs/order_wdc_page";
         break;
      case 8:
         filename = "/root/home/AffinityDB/ScaleStore/Logs/orderline_page";
         break;
      case 9:
         filename = "/root/home/AffinityDB/ScaleStore/Logs/item_page";
         break;
      default:
         filename = "/root/home/AffinityDB/ScaleStore/Logs/stock_page";
         break;
      }
      BTree tree(tree_pid);
      tree.page_traversal(filename);
      traversed = true;
   }

   void insert(const typename Record::Key &rec_key, const Record &record)
   {
      BTree tree(tree_pid);
      tree.insert(rec_key, record);
   }

   template <class Fn>
   void scan(const typename Record::Key &key, const Fn &fn)
   {
      BTree tree(tree_pid);
      tree.template scan<typename BTree::ASC_SCAN>(key, [&](typename Record::Key &key, Record &record)
                                                   { return fn(key, record); });
   }

   template <class Fn>
   void scanDesc(const typename Record::Key &key, const Fn &fn)
   {
      BTree tree(tree_pid);
      tree.template scan<typename BTree::DESC_SCAN>(key, [&](typename Record::Key &key, Record &record)
                                                    { return fn(key, record); });
   }

   template <class Field>
   auto lookupField(const typename Record::Key &key, [[maybe_unused]] Field Record::*f)
   {
      BTree tree(tree_pid);
      Field local_f;
      auto res = tree.lookup_opt(key, [&](Record &value)
                                 { local_f = value.*f; });
      ensure(res);
      return local_f;
   }

   template <class Fn>
   // void update1(const typename Record::Key& key, const Fn& fn, storage::btree::WALUpdateGenerator wal_update_generator)
   void update1(const typename Record::Key &key, const Fn &fn)
   {
      BTree tree(tree_pid);
      auto res = tree.lookupAndUpdate(key, [&](Record &value)
                                      { fn(value); });
      ensure(res);
   }

   template <class Fn>
   void lookup1(const typename Record::Key &key, const Fn &fn)
   {
      BTree tree(tree_pid);
      const auto res = tree.lookup_opt(key, fn);
      ensure(res);
      tree.page_ids.clear();
   }

   bool erase(const typename Record::Key &key)
   {
      BTree tree(tree_pid);
      const auto res = tree.remove(key);
      return res;
   }

   void traceKey([[maybe_unused]] const typename Record::Key &key, [[maybe_unused]] std::vector<int> &txn_key_trace_list)
   {
      printf("traceKey Failed\n");
      return;
   }

   void insertWithTrace(const typename Record::Key &rec_key, const Record &record,
                        std::vector<int> &txn_key_trace_list)
   {
      traceKey(rec_key, txn_key_trace_list);
      BTree tree(tree_pid);
      tree.insert(rec_key, record);
   }

   template <class Fn>
   void scanWithTrace(const typename Record::Key &key, const Fn &fn,
                      std::vector<int> &txn_key_trace_list)
   {
      traceKey(key, txn_key_trace_list);
      BTree tree(tree_pid);
      tree.template scan<typename BTree::ASC_SCAN>(key, [&](typename Record::Key &key, Record &record)
                                                   { return fn(key, record); });
   }

   template <class Fn>
   void scanDescWithTrace(const typename Record::Key &key, const Fn &fn,
                          std::vector<int> &txn_key_trace_list)
   {
      traceKey(key, txn_key_trace_list);
      BTree tree(tree_pid);
      tree.template scan<typename BTree::DESC_SCAN>(key, [&](typename Record::Key &key, Record &record)
                                                    { return fn(key, record); });
   }

   template <class Field>
   auto lookupFieldWithTrace(const typename Record::Key &key, [[maybe_unused]] Field Record::*f,
                             std::vector<int> &txn_key_trace_list)
   {
      traceKey(key, txn_key_trace_list);
      BTree tree(tree_pid);
      Field local_f;
      auto res = tree.lookup_opt(key, [&](Record &value)
                                 { local_f = value.*f; });
      ensure(res);
      return local_f;
   }

   template <class Fn>
   // void update1(const typename Record::Key& key, const Fn& fn, storage::btree::WALUpdateGenerator wal_update_generator)
   void update1WithTrace(const typename Record::Key &key, const Fn &fn,
                         std::vector<int> &txn_key_trace_list)
   {
      traceKey(key, txn_key_trace_list);
      BTree tree(tree_pid);
      auto res = tree.lookupAndUpdate(key, [&](Record &value)
                                      { fn(value); });
      ensure(res);
   }

   template <class Fn>
   void lookup1WithTrace(const typename Record::Key &key, const Fn &fn,
                         std::vector<int> &txn_key_trace_list)
   {
      traceKey(key, txn_key_trace_list);
      BTree tree(tree_pid);
      const auto res = tree.lookup_opt(key, fn);
      ensure(res);
   }

   bool eraseWithTrace(const typename Record::Key &key,
                       std::vector<int> &txn_key_trace_list)
   {
      traceKey(key, txn_key_trace_list);
      BTree tree(tree_pid);
      const auto res = tree.remove(key);
      return res;
   }
};

template <>
void ScaleStoreAdapter<customer_t>::traceKey(const customer_t::Key &key, std::vector<int> &txn_key_trace_list)
{
   txn_key_trace_list.push_back(gen_customer_key(key));
}

template <>
void ScaleStoreAdapter<order_t>::traceKey(const order_t::Key &key, std::vector<int> &txn_key_trace_list)
{
   txn_key_trace_list.push_back(gen_order_key(key));
}

template <>
void ScaleStoreAdapter<stock_t>::traceKey(const stock_t::Key &key, std::vector<int> &txn_key_trace_list)
{
   txn_key_trace_list.push_back(gen_stock_key(key));
}

template <class Record>
struct new_ScaleStoreAdapter
{
   using BTree = storage::BTree<typename Record::Key, Record>;
   std::string name;
   PID *tree_pid;
   new_ScaleStoreAdapter(){};
   new_ScaleStoreAdapter(ScaleStore &db, std::string name) : name(name)
   {
      auto &catalog = db.getCatalog();
      tree_pid = new PID[FLAGS_tpcc_warehouse_count];
      if (db.getNodeID() == 0)
      {
         for (int i = 0; i < int(FLAGS_tpcc_warehouse_count); i++)
         {
            db.createBTree<typename Record::Key, Record>();
         }
      }
      int id = Record::id;
      for (int i = 0; i < int(FLAGS_tpcc_warehouse_count); i++)
      {
         tree_pid[i] = catalog.getCatalogEntry(id).pid;
         id++;
      }
   };

   void insert(const typename Record::Key &rec_key, const Record &record)
   {
      BTree tree(tree_pid[rec_key.getWId() - 1]);
      tree.insert(rec_key, record);
   }

   template <class Fn>
   void scan(const typename Record::Key &key, const Fn &fn)
   {
      BTree tree(tree_pid[key.getWId() - 1]);
      tree.template scan<typename BTree::ASC_SCAN>(key, [&](typename Record::Key &key, Record &record)
                                                   { return fn(key, record); });
   }

   template <class Fn>
   void scanDesc(const typename Record::Key &key, const Fn &fn)
   {
      BTree tree(tree_pid[key.getWId() - 1]);
      tree.template scan<typename BTree::DESC_SCAN>(key, [&](typename Record::Key &key, Record &record)
                                                    { return fn(key, record); });
   }

   template <class Field>
   auto lookupField(const typename Record::Key &key, [[maybe_unused]] Field Record::*f)
   {
      BTree tree(tree_pid[key.getWId() - 1]);
      Field local_f;
      auto res = tree.lookup_opt(key, [&](Record &value)
                                 { local_f = value.*f; });
      ensure(res);
      return local_f;
   }

   template <class Fn>
   // void update1(const typename Record::Key& key, const Fn& fn, storage::btree::WALUpdateGenerator wal_update_generator)
   void update1(const typename Record::Key &key, const Fn &fn)
   {
      BTree tree(tree_pid[key.getWId() - 1]);
      auto res = tree.lookupAndUpdate(key, [&](Record &value)
                                      { fn(value); });
      ensure(res);
   }

   template <class Fn>
   void lookup1(const typename Record::Key &key, const Fn &fn)
   {
      BTree tree(tree_pid[key.getWId() - 1]);
      const auto res = tree.lookup_opt(key, fn);
      ensure(res);
   }

   bool erase(const typename Record::Key &key)
   {
      BTree tree(tree_pid[key.getWId() - 1]);
      const auto res = tree.remove(key);
      return res;
   }

   void traceKey([[maybe_unused]] const typename Record::Key &key, [[maybe_unused]] std::vector<int> &txn_key_trace_list)
   {
      printf("traceKey Failed\n");
      return;
   }

   void insertWithTrace(const typename Record::Key &rec_key, const Record &record,
                        std::vector<int> &txn_key_trace_list)
   {
      traceKey(rec_key, txn_key_trace_list);
      BTree tree(tree_pid[rec_key.getWId() - 1]);
      tree.insert(rec_key, record);
   }

   template <class Fn>
   void scanWithTrace(const typename Record::Key &key, const Fn &fn,
                      std::vector<int> &txn_key_trace_list)
   {
      traceKey(key, txn_key_trace_list);
      BTree tree(tree_pid[key.getWId() - 1]);
      tree.template scan<typename BTree::ASC_SCAN>(key, [&](typename Record::Key &key, Record &record)
                                                   { return fn(key, record); });
   }

   template <class Fn>
   void scanDescWithTrace(const typename Record::Key &key, const Fn &fn,
                          std::vector<int> &txn_key_trace_list)
   {
      traceKey(key, txn_key_trace_list);
      BTree tree(tree_pid[key.getWId() - 1]);
      tree.template scan<typename BTree::DESC_SCAN>(key, [&](typename Record::Key &key, Record &record)
                                                    { return fn(key, record); });
   }

   template <class Field>
   auto lookupFieldWithTrace(const typename Record::Key &key, [[maybe_unused]] Field Record::*f,
                             std::vector<int> &txn_key_trace_list)
   {
      traceKey(key, txn_key_trace_list);
      BTree tree(tree_pid[key.getWId() - 1]);
      Field local_f;
      auto res = tree.lookup_opt(key, [&](Record &value)
                                 { local_f = value.*f; });
      ensure(res);
      return local_f;
   }

   template <class Fn>
   // void update1(const typename Record::Key& key, const Fn& fn, storage::btree::WALUpdateGenerator wal_update_generator)
   void update1WithTrace(const typename Record::Key &key, const Fn &fn,
                         std::vector<int> &txn_key_trace_list)
   {
      traceKey(key, txn_key_trace_list);
      BTree tree(tree_pid[key.getWId() - 1]);
      auto res = tree.lookupAndUpdate(key, [&](Record &value)
                                      { fn(value); });
      ensure(res);
   }

   template <class Fn>
   void lookup1WithTrace(const typename Record::Key &key, const Fn &fn,
                         std::vector<int> &txn_key_trace_list)
   {
      traceKey(key, txn_key_trace_list);
      BTree tree(tree_pid[key.getWId() - 1]);
      const auto res = tree.lookup_opt(key, fn);
      ensure(res);
   }

   bool eraseWithTrace(const typename Record::Key &key,
                       std::vector<int> &txn_key_trace_list)
   {
      traceKey(key, txn_key_trace_list);
      BTree tree(tree_pid[key.getWId() - 1]);
      const auto res = tree.remove(key);
      return res;
   }
};

template <>
void new_ScaleStoreAdapter<customer_t>::traceKey(const customer_t::Key &key, std::vector<int> &txn_key_trace_list)
{
   txn_key_trace_list.push_back(gen_customer_key(key));
}

template <>
void new_ScaleStoreAdapter<order_t>::traceKey(const order_t::Key &key, std::vector<int> &txn_key_trace_list)
{
   txn_key_trace_list.push_back(gen_order_key(key));
}

template <>
void new_ScaleStoreAdapter<stock_t>::traceKey(const stock_t::Key &key, std::vector<int> &txn_key_trace_list)
{
   txn_key_trace_list.push_back(gen_stock_key(key));
}
