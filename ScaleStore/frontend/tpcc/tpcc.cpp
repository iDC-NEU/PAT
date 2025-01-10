#include <unistd.h>
#include <iostream>
#include <set>
#include <vector>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>

#include "scalestore/Config.hpp"
#include "scalestore/rdma/CommunicationManager.hpp"
#include "scalestore/storage/datastructures/BTree.hpp"
#include "scalestore/utils/RandomGenerator.hpp"
#include "scalestore/utils/ScrambledZipfGenerator.hpp"
#include "scalestore/utils/Time.hpp"
#include "spdlog/spdlog.h"
#include <unordered_map>
#include "schema.hpp"
#include "tpcc_adapter.hpp"

// -------------------------------------------------------------------------------------

// -------------------------------------------------------------------------------------


// adapter and ids
ScaleStoreAdapter<warehouse_t> warehouse;      // 0
ScaleStoreAdapter<district_t> district;        // 1
ScaleStoreAdapter<customer_t> customer;        // 2
ScaleStoreAdapter<customer_wdl_t> customerwdl; // 3
ScaleStoreAdapter<history_t> history;          // 4
ScaleStoreAdapter<neworder_t> neworder;        // 5
ScaleStoreAdapter<order_t> order;              // 6
ScaleStoreAdapter<order_wdc_t> order_wdc;      // 7
ScaleStoreAdapter<orderline_t> orderline;      // 8
ScaleStoreAdapter<item_t> item;                // 9
ScaleStoreAdapter<stock_t> stock;              // 10
static constexpr uint64_t barrier_id = 11;

// -------------------------------------------------------------------------------------
// yeah, dirty include...
#include "tpcc_run.hpp"
// -------------------------------------------------------------------------------------
double calculateMTPS(std::chrono::high_resolution_clock::time_point begin, std::chrono::high_resolution_clock::time_point end, u64 factor)
{
   double tps = ((factor * 1.0 / (std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() / 1000000.0)));
   return (tps / 1000000.0);
}
// -------------------------------------------------------------------------------------


// -------------------------------------------------------------------------------------
// MAIN
int main(int argc, char *argv[])
{
   gflags::SetUsageMessage("Leanstore TPC-C");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   ScaleStore db;
   auto &catalog = db.getCatalog();

   // -------------------------------------------------------------------------------------
   auto barrier_wait = [&]()
   {
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i)
      {
         db.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]()
                                             {
            storage::DistributedBarrier barrier(catalog.getCatalogEntry(barrier_id).pid);
            barrier.wait(); });
      }
      db.getWorkerPool().joinAll();
   };
   // -------------------------------------------------------------------------------------

   auto partition = [&](uint64_t id, uint64_t participants, uint64_t N) -> Partition
   {
      const uint64_t blockSize = N / participants;
      auto begin = id * blockSize;
      auto end = begin + blockSize;
      if (id == participants - 1)
         end = N;
      return {.begin = begin, .end = end};
   };
   // -------------------------------------------------------------------------------------
   warehouseCount = FLAGS_tpcc_warehouse_count; // xxx remove as it is defined in workload
   // -------------------------------------------------------------------------------------
   // verify configuration
   ensure((uint64_t)warehouseCount >= FLAGS_nodes);
   if (FLAGS_tpcc_warehouse_affinity)
      ensure((uint64_t)warehouseCount >= (FLAGS_worker * FLAGS_nodes));
   // -------------------------------------------------------------------------------------
   // generate tables
   db.getWorkerPool().scheduleJobSync(0, [&]()
                                      {
                                         warehouse = ScaleStoreAdapter<warehouse_t>(db, "warehouse");
                                         district = ScaleStoreAdapter<district_t>(db, "district");
                                         customer = ScaleStoreAdapter<customer_t>(db, "customer");
                                         customerwdl = ScaleStoreAdapter<customer_wdl_t>(db, "customerwdl");
                                         history = ScaleStoreAdapter<history_t>(db, "history");
                                         neworder = ScaleStoreAdapter<neworder_t>(db, "neworder");
                                         order = ScaleStoreAdapter<order_t>(db, "order");
                                         order_wdc = ScaleStoreAdapter<order_wdc_t>(db, "order_wdc");
                                         orderline = ScaleStoreAdapter<orderline_t>(db, "orderline");
                                         item = ScaleStoreAdapter<item_t>(db, "item");
                                         stock = ScaleStoreAdapter<stock_t>(db, "stock");
                                         printf("Stock");
                                         // -------------------------------------------------------------------------------------
                                         if (db.getNodeID() == 0)
                                            db.createBarrier(FLAGS_worker * FLAGS_nodes); // distributed barrier
                                      });
   // -------------------------------------------------------------------------------------
   // load data
   warehouse_range_node = partition(db.getNodeID(), FLAGS_nodes, warehouseCount);

   // load items on node 0 as it is read-only and will be replicated anyway
   if (db.getNodeID() == 0)
   {
      db.getWorkerPool().scheduleJobSync(0, [&]()
                                         { loadItem(); });
   }

   // generate warehouses
   db.getWorkerPool().scheduleJobSync(0, [&]()
                                      { loadWarehouse(warehouse_range_node.begin, warehouse_range_node.end); });

   { // populate rest of the tables from
      std::atomic<uint32_t> g_w_id = warehouse_range_node.begin + 1;
      for (uint32_t t_i = 0; t_i < FLAGS_worker; t_i++)
      {
         db.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]()
                                             {
            thread_id = t_i + (db.getNodeID() * FLAGS_worker);
            ensure((uint64_t)thread_id < MAX_THREADS);
            while (true) {
               uint32_t w_id = g_w_id++;
               if (w_id > warehouse_range_node.end) { return; }
               // start tx
               loadStock(w_id);
               loadDistrinct(w_id);
               for (Integer d_id = 1; d_id <= 10; d_id++) {
                  loadCustomer(w_id, d_id);
                  loadOrders(w_id, d_id);
               }
               // commit tx
            } });
      }
      db.getWorkerPool().joinAll();
   }

   printf("data is loaded\n");
   // consistencyCheck();
   //  -------------------------------------------------------------------------------------
   double gib = (db.getBuffermanager().getConsumedPages() * storage::EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
   std::cout << "data loaded - consumed space in GiB = " << gib << std::endl;

   // -------------------------------------------------------------------------------------
   barrier_wait();
   // -------------------------------------------------------------------------------------
   if(!FLAGS_use_proxy){
      origin_tpcc_run(db);
   }
   else if(FLAGS_use_codesign){
      router_tpcc_run_with_codesign(db);
   }
   else{
      router_tpcc_run_without_codesign(db);
   }
   //consistencyCheck(db);
   return 0;
}
