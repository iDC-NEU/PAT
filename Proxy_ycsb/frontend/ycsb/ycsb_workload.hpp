#pragma once
#include "Proxy/Config.hpp"
#include "ycsb_types.hpp"

// -------------------------------------------------------------------------------------
using namespace Proxy;

class ycsb_workload
{
private:
public:
   ycsb_workload(){
      zipf_random = std::make_unique<utils::ScrambledZipfGenerator>(0, YCSB_tuple_count, FLAGS_YCSB_zipf_factor);
   };
   ~ycsb_workload();
   std::vector<TxnNode> ycsb_keys_create();
   std::unique_ptr<utils::ScrambledZipfGenerator> zipf_random;
   int64_t zipf_offset = 0;
   i64 YCSB_tuple_count = FLAGS_YCSB_tuple_count;

};

ycsb_workload::~ycsb_workload()
{
}

std::vector<TxnNode> ycsb_workload::ycsb_keys_create()
{
   std::vector<TxnNode> keylist;
   keylist.reserve(FLAGS_ycsb_num);
   for(int i =0; i < int(FLAGS_ycsb_num); i++){
      K key = zipf_random->rand(zipf_offset);
      if(FLAGS_YCSB_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_YCSB_read_ratio){
         keylist.emplace_back(TxnNode(key, true, 1));
      }
      else{
         keylist.emplace_back(TxnNode(key, false, 2));
      }
   }
   return keylist;
}
