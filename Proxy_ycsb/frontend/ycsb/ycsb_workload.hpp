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
   };
   ~ycsb_workload();
   std::vector<TxnNode> ycsb_keys_create(int &partition_id);
   std::vector<std::unique_ptr<utils::ScrambledZipfGenerator>> zipf_randoms;
   std::vector<Partition> partitions;
   int64_t zipf_offset = 0;
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
         keylist.emplace_back(TxnNode(key, false, 2));
      }
   }
   if (FLAGS_distribution && urand(1, 100) <= int(FLAGS_distribution_rate))
   {
      int distribution_id = urandexcept(0, FLAGS_nodes -1, partition_id);
      K key = zipf_randoms[distribution_id]->rand(zipf_offset);
      if (FLAGS_YCSB_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_YCSB_read_ratio)
      {
         keylist.emplace_back(TxnNode(key, true, 1));
      }
      else
      {
         keylist.emplace_back(TxnNode(key, false, 2));
      }
   }
   return keylist;
}
