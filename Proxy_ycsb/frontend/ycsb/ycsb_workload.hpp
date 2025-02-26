#pragma once
#include "Proxy/Config.hpp"
#include "ycsb_types.hpp"

// -------------------------------------------------------------------------------------
using namespace Proxy;

Integer urandexcept(Integer low, Integer high, Integer v)
{
   if (high <= low)
      return low;
   Integer r = rnd(high - low) + low;
   if (r >= v)
      return r + 1;
   else
      return r;
}

class ycsb_workload
{
private:
public:
   ycsb_workload()
   {
      for (int i = 0; i < int(FLAGS_nodes); i++)
      {
         partitions.push_back(partition(i, FLAGS_nodes, YCSB_tuple_count));
         zipf_randoms.push_back(std::make_unique<utils::ScrambledZipfGenerator>(partitions[i].begin, partitions[i].end, FLAGS_YCSB_zipf_factor));
      }
      offset = YCSB_tuple_count / FLAGS_nodes;
      tail_range = offset / 5 * 4;
      outfile.open("/root/home/AffinityDB/Proxy_ycsb/backend/Proxy/Logs/ycsb_key");
   };
   ~ycsb_workload();
   std::vector<TxnNode> ycsb_keys_create(int &partition_id);
   std::vector<TxnNode> ycsb_hot_page(int &partition_id);
   std::vector<TxnNode> ycsb_new_hot_page(int &partition_id);
   std::vector<TxnNode> ycsb_hot_workload_change(int &partition_id);
   void hash_key(K &key, int partition_id);
   std::vector<TxnNode> ycsb_workload_change(int &partition_id);
   void key_transfer(std::vector<TxnNode> &keylist);
   void key_transfer_back(std::vector<TxnNode> &keylist);
   std::ofstream outfile;
   std::vector<std::unique_ptr<utils::ScrambledZipfGenerator>> zipf_randoms;
   std::vector<Partition> partitions;
   int64_t zipf_offset = 0;
   i64 offset = 0;
   i64 tail_range = 0;
   i64 YCSB_tuple_count = FLAGS_YCSB_tuple_count;
};

ycsb_workload::~ycsb_workload()
{
}

std::vector<TxnNode> ycsb_workload::ycsb_keys_create(int &partition_id)
{
   std::vector<TxnNode> keylist;
   keylist.reserve(FLAGS_ycsb_num);
   partition_id = utils::RandomGenerator::getRandU64(0, FLAGS_nodes);
   for (int i = 0; i < int(FLAGS_ycsb_num) - 1; i++)
   {
      K key = zipf_randoms[partition_id]->rand(zipf_offset);
      if (FLAGS_YCSB_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_YCSB_read_ratio)
      {
         keylist.emplace_back(TxnNode(key, true, 1));
      }
      else
      {
         keylist.emplace_back(TxnNode(key, false, FLAGS_write_weight));
      }
   }
   if (FLAGS_distribution && urand(1, 100) <= int(FLAGS_distribution_rate))
   {
      int distribution_id = urandexcept(0, FLAGS_nodes - 1, partition_id);
      K key = zipf_randoms[distribution_id]->rand(zipf_offset);
      if (FLAGS_YCSB_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_YCSB_read_ratio)
      {
         keylist.emplace_back(TxnNode(key, true, 1));
      }
      else
      {
         keylist.emplace_back(TxnNode(key, false, FLAGS_write_weight));
      }
   }
   else
   {
      K key = zipf_randoms[partition_id]->rand(zipf_offset);
      if (FLAGS_YCSB_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_YCSB_read_ratio)
      {
         keylist.emplace_back(TxnNode(key, true, 1));
      }
      else
      {
         keylist.emplace_back(TxnNode(key, false, FLAGS_write_weight));
      }
   }
   return keylist;
}

std::vector<TxnNode> ycsb_workload::ycsb_workload_change(int &partition_id)
{
   std::vector<TxnNode> keylist;
   keylist.reserve(FLAGS_ycsb_num);
   partition_id = utils::RandomGenerator::getRandU64(0, FLAGS_nodes);
   i64 key_offset = offset * partition_id;
   for (int i = 0; i < int(FLAGS_ycsb_num) - 1; i++)
   {
      K key = zipf_randoms[partition_id]->rand(zipf_offset);
      if (key - key_offset >= tail_range)
      {
         key = (key + offset * (FLAGS_nodes - 1)) % FLAGS_YCSB_tuple_count;
      }
      if (FLAGS_YCSB_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_YCSB_read_ratio)
      {
         keylist.emplace_back(TxnNode(key, true, 1));
      }
      else
      {
         keylist.emplace_back(TxnNode(key, false, FLAGS_write_weight));
      }
   }
   if (FLAGS_distribution && urand(1, 100) <= int(FLAGS_distribution_rate))
   {
      int distribution_id = urandexcept(0, FLAGS_nodes - 1, partition_id);
      K key = zipf_randoms[distribution_id]->rand(zipf_offset);
      if (key - key_offset >= tail_range)
      {
         key = (key + offset * (FLAGS_nodes - 1)) % FLAGS_YCSB_tuple_count;
      }
      if (FLAGS_YCSB_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_YCSB_read_ratio)
      {
         keylist.emplace_back(TxnNode(key, true, 1));
      }
      else
      {
         keylist.emplace_back(TxnNode(key, false, FLAGS_write_weight));
      }
   }
   else
   {
      K key = zipf_randoms[partition_id]->rand(zipf_offset);
      if (key - key_offset >= tail_range)
      {
         key = (key + offset * (FLAGS_nodes - 1)) % FLAGS_YCSB_tuple_count;
      }
      if (FLAGS_YCSB_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_YCSB_read_ratio)
      {
         keylist.emplace_back(TxnNode(key, true, 1));
      }
      else
      {
         keylist.emplace_back(TxnNode(key, false, FLAGS_write_weight));
      }
   }
   return keylist;
}

std::vector<TxnNode> ycsb_workload::ycsb_hot_workload_change(int &partition_id)
{
   std::vector<TxnNode> keylist;
   keylist.reserve(FLAGS_ycsb_num);
   partition_id = utils::RandomGenerator::getRandU64(0, FLAGS_nodes);
   i64 key_offset = offset * partition_id + FLAGS_stamp_len * FLAGS_ycsb_hot_page_size;
   for (int i = 0; i < int(FLAGS_ycsb_num) - 1; i++)
   {
      K key = zipf_randoms[partition_id]->rand(zipf_offset);
      while (key < FLAGS_ycsb_hot_page_size)
      {
         key = zipf_randoms[partition_id]->rand(zipf_offset);
      }
      if (key - key_offset >= tail_range)
      {
         key = (key + offset * (FLAGS_nodes - 1)) % FLAGS_YCSB_tuple_count;
      }
      key += FLAGS_stamp_len * FLAGS_ycsb_hot_page_size;
      if (FLAGS_YCSB_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_YCSB_read_ratio)
      {
         keylist.emplace_back(TxnNode(key, true, 1));
      }
      else
      {
         keylist.emplace_back(TxnNode(key, false, FLAGS_write_weight));
      }
   }
   K key = utils::RandomGenerator::getRandU64(0, FLAGS_ycsb_hot_page_size);
   int hash_value = key % FLAGS_nodes;
   if (hash_value != partition_id)
   {
      // 计算差值
      int diff = partition_id - hash_value;

      // 调整随机数，决定是加还是减
      if (diff > 0)
      {
         // 尝试加上差值，如果超出范围则改为减
         if (key + diff < FLAGS_ycsb_hot_page_size)
         {
            key += diff;
         }
         else
         {
            key -= (FLAGS_nodes - diff);
         }
      }
      else
      {
         // 尝试减去差值，如果小于0则改为加
         if (key + diff >= 0)
         {
            key += diff;
         }
         else
         {
            key += (FLAGS_nodes + diff);
         }
      }
   }
   key = key * FLAGS_stamp_len;
   if (FLAGS_YCSB_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_YCSB_read_ratio)
   {
      keylist.emplace_back(TxnNode(key, true, 1));
   }
   else
   {
      keylist.emplace_back(TxnNode(key, false, FLAGS_write_weight));
   }
   return keylist;
}

std::vector<TxnNode> ycsb_workload::ycsb_hot_page(int &partition_id)
{
   std::vector<TxnNode> keylist;
   keylist.reserve(FLAGS_ycsb_num);
   partition_id = utils::RandomGenerator::getRandU64(0, FLAGS_nodes);
   for (int i = 0; i < int(FLAGS_ycsb_num) - 2; i++)
   {
      K key = zipf_randoms[partition_id]->rand(zipf_offset);
      while (key < FLAGS_ycsb_hot_page_size)
      {
         key = zipf_randoms[partition_id]->rand(zipf_offset);
      }
      key += FLAGS_stamp_len * FLAGS_ycsb_hot_page_size;
      if (FLAGS_YCSB_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_YCSB_read_ratio)
      {
         keylist.emplace_back(TxnNode(key, true, 1));
      }
      else
      {
         keylist.emplace_back(TxnNode(key, false, FLAGS_write_weight));
      }
   }
   if (FLAGS_distribution && urand(1, 100) <= int(FLAGS_distribution_rate))
   {
      int distribution_id = urandexcept(0, FLAGS_nodes - 1, partition_id);
      K key = zipf_randoms[distribution_id]->rand(zipf_offset);
      while (key < FLAGS_ycsb_hot_page_size)
      {
         key = zipf_randoms[distribution_id]->rand(zipf_offset);
      }
      key += FLAGS_stamp_len * FLAGS_ycsb_hot_page_size;
      if (FLAGS_YCSB_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_YCSB_read_ratio)
      {
         keylist.emplace_back(TxnNode(key, true, 1));
      }
      else
      {
         keylist.emplace_back(TxnNode(key, false, FLAGS_write_weight));
      }
   }
   else
   {
      K key = zipf_randoms[partition_id]->rand(zipf_offset);
      while (key < FLAGS_ycsb_hot_page_size)
      {
         key = zipf_randoms[partition_id]->rand(zipf_offset);
      }
      key += FLAGS_stamp_len * FLAGS_ycsb_hot_page_size;
      if (FLAGS_YCSB_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_YCSB_read_ratio)
      {
         keylist.emplace_back(TxnNode(key, true, 1));
      }
      else
      {
         keylist.emplace_back(TxnNode(key, false, FLAGS_write_weight));
      }
   }
   K key = utils::RandomGenerator::getRandU64(0, FLAGS_ycsb_hot_page_size);
   // if (FLAGS_distribution && urand(1, 100) <= int(FLAGS_distribution_rate))
   // {
   //    int hash_value = key % FLAGS_nodes;
   //    while (hash_value == partition_id)
   //    {
   //       key = utils::RandomGenerator::getRandU64(0, FLAGS_ycsb_hot_page_size);
   //       hash_value = key % FLAGS_nodes;
   //    }
   //    key = key * FLAGS_stamp_len;
   //    if (FLAGS_YCSB_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_YCSB_read_ratio)
   //    {
   //       keylist.emplace_back(TxnNode(key, true, 1));
   //    }
   //    else
   //    {
   //       keylist.emplace_back(TxnNode(key, false, FLAGS_write_weight));
   //    }
   //    return keylist;
   // }
   // 检查 random_number % nodes 是否等于 partition_id
   int hash_value = key % FLAGS_nodes;
   if (hash_value != partition_id)
   {
      // 计算差值
      int diff = partition_id - hash_value;

      // 调整随机数，决定是加还是减
      if (diff > 0)
      {
         // 尝试加上差值，如果超出范围则改为减
         if (key + diff < FLAGS_ycsb_hot_page_size)
         {
            key += diff;
         }
         else
         {
            key -= (FLAGS_nodes - diff);
         }
      }
      else
      {
         // 尝试减去差值，如果小于0则改为加
         if (key + diff >= 0)
         {
            key += diff;
         }
         else
         {
            key += (FLAGS_nodes + diff);
         }
      }
   }
   key = key * FLAGS_stamp_len;
   if (FLAGS_YCSB_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_YCSB_read_ratio)
   {
      keylist.emplace_back(TxnNode(key, true, 1));
   }
   else
   {
      keylist.emplace_back(TxnNode(key, false, FLAGS_write_weight));
   }

   return keylist;
}

void ycsb_workload::hash_key(K &key, int partition_id){
   int hash_value = key % FLAGS_nodes;
   if (hash_value != partition_id)
   {
      // 计算差值
      int diff = partition_id - hash_value;

      // 调整随机数，决定是加还是减
      if (diff > 0)
      {
         // 尝试加上差值，如果超出范围则改为减
         if (key + diff < FLAGS_ycsb_hot_page_size)
         {
            key += diff;
         }
         else
         {
            key -= (FLAGS_nodes - diff);
         }
      }
      else
      {
         // 尝试减去差值，如果小于0则改为加
         if (key + diff >= 0)
         {
            key += diff;
         }
         else
         {
            key += (FLAGS_nodes + diff);
         }
      }
   }
}

std::vector<TxnNode> ycsb_workload::ycsb_new_hot_page(int &partition_id)
{
   std::vector<TxnNode> keylist;
   keylist.reserve(FLAGS_ycsb_num);
   partition_id = utils::RandomGenerator::getRandU64(0, FLAGS_nodes);
   for (int i = 0; i < int(FLAGS_ycsb_num) - 2; i++)
   {
      K key = zipf_randoms[partition_id]->rand(zipf_offset);
      if (FLAGS_YCSB_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_YCSB_read_ratio)
      {
         keylist.emplace_back(TxnNode(key, true, 1));
      }
      else
      {
         keylist.emplace_back(TxnNode(key, false, FLAGS_write_weight));
      }
   }
   if (FLAGS_distribution && urand(1, 100) <= int(FLAGS_distribution_rate))
   {
      int distribution_id = urandexcept(0, FLAGS_nodes - 1, partition_id);
      K key = zipf_randoms[distribution_id]->rand(zipf_offset);
      if (FLAGS_YCSB_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_YCSB_read_ratio)
      {
         keylist.emplace_back(TxnNode(key, true, 1));
      }
      else
      {
         keylist.emplace_back(TxnNode(key, false, FLAGS_write_weight));
      }
   }
   else
   {
      K key = zipf_randoms[partition_id]->rand(zipf_offset);
      if (FLAGS_YCSB_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_YCSB_read_ratio)
      {
         keylist.emplace_back(TxnNode(key, true, 1));
      }
      else
      {
         keylist.emplace_back(TxnNode(key, false, FLAGS_write_weight));
      }
   }

   K key = utils::RandomGenerator::getRandU64(0, FLAGS_ycsb_hot_page_size);
   hash_key(key, partition_id);
   if (FLAGS_YCSB_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_YCSB_read_ratio)
   {
      keylist.emplace_back(TxnNode(key, true, 1));
   }
   else
   {
      keylist.emplace_back(TxnNode(key, false, FLAGS_write_weight));
   }

   return keylist;
}

void ycsb_workload::key_transfer(std::vector<TxnNode> &keylist)
{
   for (auto &key_node : keylist)
   {
      int key = key_node.key / FLAGS_stamp_len;
      if (key < FLAGS_ycsb_hot_page_size)
      {
         key_node.key = key;
      }
      else
      {
         key_node.key -= FLAGS_ycsb_hot_page_size * FLAGS_stamp_len;
      }
   }
}

void ycsb_workload::key_transfer_back(std::vector<TxnNode> &keylist)
{
   for (auto &key_node : keylist)
   {
      int key = key_node.key;
      if (key < FLAGS_ycsb_hot_page_size)
      {
         key_node.key = key * FLAGS_stamp_len;
      }
      else
      {
         key_node.key += FLAGS_ycsb_hot_page_size * FLAGS_stamp_len;
      }
   }
}