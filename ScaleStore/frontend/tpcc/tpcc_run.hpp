#include "scalestore/ScaleStore.hpp"
#include "tpcc_workload.hpp"

struct TPCC_workloadInfo : public scalestore::profiling::WorkloadInfo
{
   std::string experiment;
   uint64_t warehouses;
   std::string configuration;

   TPCC_workloadInfo(std::string experiment, uint64_t warehouses, std::string configuration)
       : experiment(experiment), warehouses(warehouses), configuration(configuration)
   {
   }

   virtual std::vector<std::string> getRow()
   {
      return {experiment, std::to_string(warehouses), configuration};
   }

   virtual std::vector<std::string> getHeader()
   {
      return {"experiment", "warehouses", "configuration"};
   }

   virtual void csv(std::ofstream &file) override
   {
      file << experiment << " , ";
      file << warehouses << " , ";
      file << configuration << " , ";
   }
   virtual void csvHeader(std::ofstream &file) override
   {
      file << "Experiment"
           << " , ";
      file << "Warehouses"
           << " , ";
      file << "Configuration"
           << " , ";
   }
};

// SQL Parse
// [txn_id]functionName(arg1,{arg2;arg3},{arg4},arg5)

uint64_t extractTxnId(const std::string &sql)
{
   size_t closeParenthesisPos = sql.find(']');
   return std::stoul(sql.substr(1, closeParenthesisPos - 1));
}

std::string extractFunctionName(const std::string &sql)
{
   std::string functionName;
   size_t leftPos = sql.find(']');
   size_t rightPos = sql.find('(');
   if (leftPos != std::string::npos && rightPos != std::string::npos)
   {
      functionName = sql.substr(leftPos + 1, rightPos - leftPos - 1);
   }
   return functionName;
}

void extractParameters(const std::string &sql, char separator, std::vector<std::string> &parameters)
{
   size_t openParenthesisPos = separator == ',' ? sql.find('(') : sql.find('{');
   size_t closeParenthesisPos = sql.size() - 1;
   if (openParenthesisPos != std::string::npos && closeParenthesisPos != std::string::npos)
   {
      std::string arguments = sql.substr(openParenthesisPos + 1, closeParenthesisPos - openParenthesisPos - 1);
      std::istringstream iss(arguments);
      std::string arg;
      while (getline(iss, arg, separator))
      {
         parameters.push_back(arg);
      }
   }
}

std::vector<Integer> convertStringVectorToIntVector(const std::vector<std::string> &stringVector)
{
   std::vector<Integer> intVector;
   for (const std::string &str : stringVector)
   {
      Integer intValue = std::stoi(str);
      intVector.push_back(intValue);
   }
   return intVector;
}

void excuteFunctionCall(const std::string &functionName, const std::vector<std::string> &args)
{

   if (functionName == "newOrder")
   {
      Integer w_id = std::stoi(args[0]);
      Integer d_id = std::stoi(args[1]);
      Integer c_id = std::stoi(args[2]);
      std::vector<Integer> lineNumbers;
      std::vector<Integer> supwares;
      std::vector<Integer> itemids;
      std::vector<Integer> qtys;
      std::istringstream iss0(args[3].substr(1, args[3].size() - 2));
      std::istringstream iss1(args[4].substr(1, args[4].size() - 2));
      std::istringstream iss2(args[5].substr(1, args[5].size() - 2));
      std::istringstream iss3(args[6].substr(1, args[6].size() - 2));
      std::string arg;
      while (getline(iss0, arg, ';'))
      {
         lineNumbers.push_back(std::stoi(arg));
         arg = "";
         getline(iss1, arg, ';');
         supwares.push_back(std::stoi(arg));
         arg = "";
         getline(iss2, arg, ';');
         itemids.push_back(std::stoi(arg));
         arg = "";
         getline(iss3, arg, ';');
         qtys.push_back(std::stoi(arg));
         arg = "";

         // key_list.push_back(gen_orderline_key(w_id,d_id,o_id,i));
      }
      Timestamp timestamp = std::stoul(args[7]);

      newOrder(w_id, d_id, c_id, lineNumbers, supwares, itemids, qtys, timestamp);
   }

   else if (functionName == "delivery")
   {
      Integer w_id = std::stoi(args[0]);
      Integer carrier_id = std::stoi(args[1]);
      Timestamp datetime = std::stoul(args[2]);

      delivery(w_id, carrier_id, datetime);
   }

   else if (functionName == "stockLevel")
   {
      Integer w_id = std::stoi(args[0]);
      Integer d_id = std::stoi(args[1]);
      Integer threshold = std::stoi(args[2]);
      stockLevel(w_id, d_id, threshold);
   }

   else if (functionName == "orderStatusId")
   {
      Integer w_id = std::stoi(args[0]);
      Integer d_id = std::stoi(args[1]);
      Integer c_id = std::stoi(args[2]);
      orderStatusId(w_id, d_id, c_id);
   }

   else if (functionName == "orderStatusName")
   {
      Integer w_id = std::stoi(args[0]);
      Integer d_id = std::stoi(args[1]);
      Varchar<16> c_last(args[2].c_str());
      orderStatusName(w_id, d_id, c_last);
   }

   else if (functionName == "paymentById")
   {
      Integer w_id = std::stoi(args[0]);
      Integer d_id = std::stoi(args[1]);
      Integer c_w_id = std::stoi(args[2]);
      Integer c_d_id = std::stoi(args[3]);
      Integer c_id = std::stoi(args[4]);
      Timestamp h_date = std::stoul(args[5]);
      Numeric h_amount = std::stod(args[6]);
      Timestamp datetime = std::stoul(args[7]);
      paymentById(w_id, d_id, c_w_id, c_d_id, c_id, h_date, h_amount, datetime);
   }

   else if (functionName == "paymentByName")
   {
      Integer w_id = std::stoi(args[0]);
      Integer d_id = std::stoi(args[1]);
      Integer c_w_id = std::stoi(args[2]);
      Integer c_d_id = std::stoi(args[3]);
      Varchar<16> c_last(args[4].c_str());
      Timestamp h_date = std::stoul(args[5]);
      Numeric h_amount = std::stod(args[6]);
      Timestamp datetime = std::stoul(args[7]);

      paymentByName(w_id, d_id, c_w_id, c_d_id, c_last, h_date, h_amount, datetime);
   }
   else
   {
      std::cout << functionName << std::endl;
      printf("Parse Function Call Failed\n");
   }
}

void consistencyCheck(ScaleStore &db)
{
   db.getWorkerPool().scheduleJobSync(0, [&]()
                                      {
         // -------------------------------------------------------------------------------------
         // consistency check
         uint64_t warehouse_tbl = 0;
         uint64_t district_tbl = 0;
         uint64_t customer_tbl = 0;
         uint64_t customer_wdl_tbl = 0;
         uint64_t history_tbl = 0;
         uint64_t neworder_tbl = 0;
         uint64_t order_tbl = 0;
         uint64_t order_wdc_tbl = 0;
         uint64_t orderline_tbl = 0;
         uint64_t item_tbl = 0;
         uint64_t stock_tbl = 0;
         std::cout << "warehouse.scan" << std::endl;
         warehouse.scan({1}, [&]([[maybe_unused]] warehouse_t::Key key, [[maybe_unused]] warehouse_t record) {
            warehouse_tbl++;
            return true;
         });
         std::cout << "district.scan" << std::endl;
         district.scan({1,1}, [&]([[maybe_unused]] district_t::Key key, [[maybe_unused]] district_t record) {
            district_tbl++;
            return true;
         });
         std::cout << "customer.scan" << std::endl;
         customer.scan({customer_t::Key(1,1,1)},
                       [&]([[maybe_unused]] customer_t::Key key, [[maybe_unused]] customer_t record) {
                          customer_tbl++;
                          return true;
                       });
         std::cout << "customerwdl.scan" << std::endl;
         customerwdl.scan({.c_w_id = 1, .c_d_id = 1, .c_last = "", .c_first = ""},
                          [&]([[maybe_unused]] customer_wdl_t::Key key, [[maybe_unused]] customer_wdl_t record) {
                             customer_wdl_tbl++;
                             return true;
                          });
         std::cout << "history.scan" << std::endl;
         history.scan({.thread_id = 1, .h_pk = 1}, [&]([[maybe_unused]] history_t::Key key, [[maybe_unused]] history_t record) {
            history_tbl++;
            return true;
         });
         std::cout << "neworder.scan" << std::endl;
         neworder.scan({1, 1, 1},
                       [&]([[maybe_unused]] neworder_t::Key key, [[maybe_unused]] neworder_t record) {
                          neworder_tbl++;
                          return true;
                       });
         std::cout << "order.scan" << std::endl;
         order.scan({1, 1, 1}, [&]([[maybe_unused]] order_t::Key key, [[maybe_unused]] order_t record) {
            order_tbl++;
            return true;
         });
         std::cout << "order_wdc.scan" << std::endl;
         order_wdc.scan({.o_w_id = 1, .o_d_id = 1, .o_c_id = 1, .o_id = 1},
                        [&]([[maybe_unused]] order_wdc_t::Key key, [[maybe_unused]] order_wdc_t record) {
                           order_wdc_tbl++;
                           return true;
                        });
         std::cout << "orderline.scan" << std::endl;
         // orderline.scan({.ol_w_id = 1, .ol_d_id = 1, .ol_o_id = 1, .ol_number = 1},
         orderline.scan({1,1,1,1},
                        [&]([[maybe_unused]] orderline_t::Key key, [[maybe_unused]] orderline_t record) {
                           orderline_tbl++;
                           return true;
                        });
         std::cout << "item.scan" << std::endl;
         
         item.scan({.i_id = 1}, [&]([[maybe_unused]] item_t::Key key, [[maybe_unused]] item_t record) {
            item_tbl++;
            return true;
         });
         std::cout << "stock.scan" << std::endl;

         stock.scan({1, 1}, [&]([[maybe_unused]] stock_t::Key key, [[maybe_unused]] stock_t record) {
            stock_tbl++;
            return true;
         });

         std::cout << "#Tuples in tables:" << std::endl;
         std::cout << "#Warehouse " << warehouse_tbl << "\n";
         std::cout << "#district " << district_tbl << "\n";
         std::cout << "#customer " << customer_tbl << "\n";
         std::cout << "#customer wdl " << customer_wdl_tbl << "\n";
         std::cout << "#history " << history_tbl << "\n";
         std::cout << "#neworder " << neworder_tbl << "\n";
         std::cout << "#order " << order_tbl << "\n";
         std::cout << "#order wdl " << order_wdc_tbl << "\n";
         std::cout << "#orderline " << orderline_tbl << "\n";
         std::cout << "#item " << item_tbl << "\n";
         std::cout << "#stock " << stock_tbl << "\n"; });
}

void origin_tpcc_run(ScaleStore &db)
{
   std::atomic<bool> keep_running = true;
   auto &catalog = db.getCatalog();
   // bool keep_getting_sql = true;
   std::atomic<u64> running_threads_counter = 0;
   u64 tx_per_thread[FLAGS_worker];
   u64 txn_per_thread[FLAGS_worker];
   u64 remote_new_order_per_thread[FLAGS_worker]; // per specification
   u64 remote_tx_per_thread[FLAGS_worker];
   u64 delivery_aborts_per_thread[FLAGS_worker];
   u64 txn_profile[FLAGS_worker][transaction_types::MAX];
   u64 txn_lat[FLAGS_worker][transaction_types::MAX];
   u64 txn_pay_lat[FLAGS_worker][10];
   bool change_line[FLAGS_worker];
   std::string configuration;
   if (FLAGS_tpcc_warehouse_affinity)
   {
      configuration = "warehouse_affinity";
   }
   else if (FLAGS_tpcc_warehouse_locality)
   {
      configuration = "warehouse_locality";
   }
   else
   {
      configuration = "warehouse_no_locality";
   }
   TPCC_workloadInfo experimentInfo{"TPCC", (uint64_t)warehouseCount, configuration};
   std::string currentFile = __FILE__;
   std::string abstract_filename = currentFile.substr(0, currentFile.find_last_of("/\\") + 1);
   std::string logFilePath = abstract_filename + "../../Logs/origin_Log." + std::to_string(db.getNodeID()) + "txt";
   std::shared_ptr<spdlog::logger> router_logger = spdlog::basic_logger_mt("router_logger", logFilePath);
   router_logger->set_level(spdlog::level::info);
   router_logger->flush_on(spdlog::level::info);
   router_logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] %v");
   // db.startProfiler(experimentInfo);
   for (uint64_t i = 0; i < FLAGS_worker; i++)
   {
      txn_per_thread[i] = 0;
   }
   for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i)
   {
      db.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]()
                                          {
         std::ofstream output(abstract_filename + "../../TXN_LOG/worker_" + std::to_string(t_i));                                 
         running_threads_counter++;
         thread_id = t_i + (db.getNodeID() * FLAGS_worker);
         volatile u64 tx_acc = 0;
         storage::DistributedBarrier barrier(catalog.getCatalogEntry(barrier_id).pid);
         barrier.wait();
         while (keep_running) {
            auto start = utils::getTimePoint();
            uint32_t w_id;
            if (FLAGS_tpcc_warehouse_affinity) {
               w_id = t_i + 1 + warehouse_range_node.begin;
            } else if (FLAGS_tpcc_warehouse_locality) {
               w_id = urand(warehouse_range_node.begin + 1, warehouse_range_node.end);
            } else {
               w_id = urand(1, FLAGS_tpcc_warehouse_count);
               if (w_id <= (uint32_t)warehouse_range_node.begin || (w_id > (uint32_t)warehouse_range_node.end)) remote_node_new_order++;
            }
            tx(w_id);
            /*
            if (FLAGS_tpcc_abort_pct && urand(0, 100) <= FLAGS_tpcc_abort_pct) {
               // abort
            } else {
               // commit
            }
            */
            auto end = utils::getTimePoint();
            output << (end - start) << " ";
            if(change_line[t_i]){
               output << std::endl;
               change_line[t_i] = false;
            }
            txn_per_thread[t_i]++;
            threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
         }
         if(FLAGS_nodes < 3){
            sleep(5 * int(db.getNodeID() + 1));
            switch (t_i)
            {
               case 0:
                  if(!warehouse.traversed){
                     warehouse.traverse_page();
                     std::cout << "warehouse page traversed" << std::endl;
                  }
                  if(!district.traversed){
                     district.traverse_page();
                     std::cout << "district page traversed" << std::endl;
                  }
                  if(!history.traversed){
                     history.traverse_page();
                     std::cout << "history page traversed" << std::endl;
                  }
                  if(!item.traversed){
                     item.traverse_page();
                     std::cout << "item page traversed" << std::endl;
                  }
                  break;
               case 1:
                  if(!customer.traversed){
                     customer.traverse_page();
                     std::cout << "customer page traversed" << std::endl;
                  }
                  if(!customerwdl.traversed){
                     customerwdl.traverse_page();
                     std::cout << "customerwdl page traversed" << std::endl;
                  }
                  if(!stock.traversed){
                     stock.traverse_page();
                     std::cout << "stock page traversed" << std::endl;
                  }
                  break;
                  break;
               case 2:
                  if(!neworder.traversed){
                     neworder.traverse_page();
                     std::cout << "neworder page traversed" << std::endl;
                  }

                  break;
               case 3:
                  if(!order.traversed){
                     order.traverse_page();
                     std::cout << "order page traversed" << std::endl;
                  }
                  if(!order_wdc.traversed){
                     order_wdc.traverse_page();
                     std::cout << "orderwdc page traversed" << std::endl;
                  }
                  break;
            }
         }
         
         tx_per_thread[t_i] = tx_acc;
         remote_new_order_per_thread[t_i] = remote_new_order;
         remote_tx_per_thread[t_i] = remote_node_new_order;
         delivery_aborts_per_thread[t_i] = delivery_aborts;
         int idx = 0;
         for (auto& tx_count : txns)
            txn_profile[t_i][idx++] = tx_count;
         idx = 0;
         for (auto& tx_l : txn_latencies) 
         {
            txn_lat[t_i][idx] = (tx_l / (double)txn_profile[t_i][idx]);
            idx++;
         }

         idx = 0;
         for (auto& tx_l : txn_paymentbyname_latencies) 
         {
            txn_pay_lat[t_i][idx] = ((tx_l) / (double)txn_profile[t_i][transaction_types::STOCK_LEVEL]);
            idx++;
         }
         running_threads_counter--; });
   }
   auto last_router_statistics = std::chrono::high_resolution_clock::now();
   std::thread statistics([&]()
                          {
      uint64_t all_numbers=0;
      float all_count = 0;
      float trigger_count = 0;
      uint64_t last_all_numbers=0;
      sleep(1);
      auto now_router_statistics = std::chrono::high_resolution_clock::now();
      while(keep_running)
      {
         auto start = std::chrono::high_resolution_clock::now();
         for (uint64_t i = 0; i < FLAGS_worker; i++)
         {
            all_numbers += txn_per_thread[i];
            txn_per_thread[i] = 0;
         }
         now_router_statistics = std::chrono::high_resolution_clock::now();
         [[maybe_unused]] auto duration = std::chrono::duration_cast<std::chrono::microseconds>(now_router_statistics - last_router_statistics);
         trigger_count += 1.0*duration.count()/1000000;
         router_logger->info(fmt::format("all_router_number:{};increase_number:{};time:{}s;increase_number/time={}/s",all_numbers,all_numbers-last_all_numbers,1.0*duration.count()/1000000,(all_numbers-last_all_numbers)/(1.0*duration.count()/1000000)));
         if(trigger_count > 10){
            all_count += trigger_count;
            router_logger->info(fmt::format("time:{}s; throughput = {}/s",trigger_count, all_numbers/all_count));
            trigger_count = 0;
         }
         last_all_numbers=all_numbers;
         auto end = std::chrono::high_resolution_clock::now();
         last_router_statistics=now_router_statistics;
         duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

         std::this_thread::sleep_for(std::chrono::microseconds(1000000) - duration);
      } });
   statistics.detach();
   // Report data size
   // std::thread data_consume([&]()
   // {
   //       while(keep_running)
   //       {
   //          sleep(30);
   //          double gib = (db.getBuffermanager().getConsumedPages() * storage::EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
   //          std::cout << "data loaded - consumed space in GiB = " << gib << std::endl;
   //       }
   // });
   // data_consume.detach();

   std::thread start_txn_count([&]()
                               {
      while(keep_running)
      {
         sleep(10);
         for(int i = 0; i< int(FLAGS_worker); i++){
            change_line[i] = true;
         }                                 
      } });
   start_txn_count.detach();
   sleep(FLAGS_TPCC_run_for_seconds);
   keep_running = false;
   while (running_threads_counter)
   {
      _mm_pause();
   }
   std::cout << "tpcc run over" << std::endl;
   sleep(5);
   db.getWorkerPool().joinAll();
   // -------------------------------------------------------------------------------------
   db.stopProfiler();
   /*
   std::cout << "tx per thread " << std::endl;
   for (u64 t_i = 0; t_i < FLAGS_worker; t_i++)
   {
      std::cout << tx_per_thread[t_i] << ",";
   }
   std::cout << "\n";

   std::cout << "remote node txn " << std::endl;

   for (u64 t_i = 0; t_i < FLAGS_worker; t_i++)
   {
      std::cout << remote_tx_per_thread[t_i] << ",";
   }
   std::cout << "\n";
   std::cout << "remote new order per specification " << std::endl;
   for (u64 t_i = 0; t_i < FLAGS_worker; t_i++)
   {
      std::cout << remote_new_order_per_thread[t_i] << ",";
   }
   std::cout << std::endl;

   std::cout << "aborts " << std::endl;
   for (u64 t_i = 0; t_i < FLAGS_worker; t_i++)
   {
      std::cout << delivery_aborts_per_thread[t_i] << ",";
   }
   std::cout << std::endl;
   std::cout << "txn profile "
             << "\n";

   for (u64 t_i = 0; t_i < FLAGS_worker; t_i++)
   {
      for (u64 i = 0; i < transaction_types::MAX; i++)
      {
         std::cout << txn_profile[t_i][i] << ",";
      }
      std::cout << "\n";
   }
   std::cout << "\n";

   for (u64 t_i = 0; t_i < FLAGS_worker; t_i++)
   {
      for (u64 i = 0; i < transaction_types::MAX; i++)
      {
         std::cout << txn_lat[t_i][i] << ",";
      }
      std::cout << "\n";
   }
   std::cout << "\n";

   for (u64 t_i = 0; t_i < FLAGS_worker; t_i++)
   {
      for (u64 i = 0; i < 10; i++)
      {
         std::cout << txn_pay_lat[t_i][i] << ",";
      }
      std::cout << "\n";
   }
   std::cout << "\n";
   double gib = (db.getBuffermanager().getConsumedPages() * storage::EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
   std::cout << "data loaded - consumed space in GiB = " << gib << std::endl;
   std::cout << "Starting hash table report "
             << "\n";
   */
   db.getBuffermanager().reportHashTableStats();
}

void router_tpcc_run_with_codesign(ScaleStore &db)
{
   std::atomic<bool> keep_running = true;
   auto &catalog = db.getCatalog();
   // bool keep_getting_sql = true;
   std::atomic<u64> running_threads_counter = 0;
   std::string configuration;
   bool change_line[FLAGS_worker];
   bool count_ready = false;
   for (int i = 0; i < int(FLAGS_worker); i++)
   {
      change_line[i] = false;
   }
   if (FLAGS_tpcc_warehouse_affinity)
   {
      configuration = "warehouse_affinity";
   }
   else if (FLAGS_tpcc_warehouse_locality)
   {
      configuration = "warehouse_locality";
   }
   else
   {
      configuration = "warehouse_no_locality";
   }
   TPCC_workloadInfo experimentInfo{"TPCC", (uint64_t)warehouseCount, configuration};
   std::string currentFile = __FILE__;
   std::string abstract_filename = currentFile.substr(0, currentFile.find_last_of("/\\") + 1);
   std::string logFilePath = abstract_filename + "../../Logs/reorganize_time_log." + std::to_string(db.getNodeID()) + "txt";
   std::shared_ptr<spdlog::logger> time_logger = spdlog::basic_logger_mt("router_logger", logFilePath);
   time_logger->set_level(spdlog::level::info);
   time_logger->flush_on(spdlog::level::info);
   time_logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] %v");
   std::vector<std::ofstream> outputs;
   std::vector<std::ofstream> neworder_times;
   for (uint64_t t_i = 0; t_i < FLAGS_worker; t_i++)
   {
      outputs.push_back(std::ofstream(abstract_filename + "../../TXN_LOG/worker_" + std::to_string(t_i)));
      neworder_times.push_back(std::ofstream(abstract_filename + "../../TXN_LOG/neworder_worker_" + std::to_string(t_i)));
   }
   for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i)
   {
      db.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]()
                                          {
         // db.getBuffermanager().local_timer.insert({std::this_thread::get_id(), Timer()});                           
         running_threads_counter++;
         thread_id = t_i + (db.getNodeID() * FLAGS_worker);
         storage::DistributedBarrier barrier(catalog.getCatalogEntry(barrier_id).pid);
         barrier.wait();
         // Timer *local_timer =  &db.getBuffermanager().local_timer[std::this_thread::get_id()];
         while (keep_running) {
            char sql[sqlLength];
            uint64_t src_node;
            if(db.getSql(sql,&src_node)){
               auto start = utils::getTimePoint();
               std::string functionName = extractFunctionName(sql);
               std::vector<std::string> parameters;
               extractParameters(sql, ',', parameters);
               // local_timer->reset(true);
               excuteFunctionCall(functionName, parameters);
               auto end = utils::getTimePoint();
               if (count_ready)
               {
                  outputs[t_i] << (end - start) << " ";
                  // if (functionName == "newOrder")
                  // {
                  //    neworder_times[t_i] << (end - start) << " "
                  //                        << local_timer->local_elapsedMicroseconds() << " "
                  //                        << local_timer->remote_elapsedMicroseconds() << " ";
                  // }
               }
               // local_timer->reset(true);
               if(change_line[t_i] && count_ready){
                  outputs[t_i] << std::endl;
                  // neworder_times[t_i] << std::endl;
                  change_line[t_i] = false;
               }
               if (db.getNodeID() == 0)
               {
                  if (t_i == 0)
                  {
                     if (!customer.created && db.customer_created())
                     {
                        time_logger->info(fmt::format("start create customer partitioner"));
                        auto time_start = utils::getTimePoint();
                        customer.partition_map = db.get_customer_map();
                        customer.create_partitioner();
                        auto time_end = utils::getTimePoint();
                        time_logger->info(fmt::format("customer partitioner created"));
                        time_logger->info(fmt::format("customer partition cost {}ms", float(time_end - time_start)/1000));
                     }
                     if(db.customer_update_ready()){
                        customer.update_map = db.get_customer_update_map();
                        customer.update_partitioner();
                        db.set_customer_update_ready(false);
                        db.customer_clear();
                     }
                     if (!warehouse.created && db.warehouse_created())
                     {
                        time_logger->info(fmt::format("start create warehouse partitioner"));
                        auto time_start = utils::getTimePoint();
                        warehouse.partition_map = db.get_warehouse_map();
                        warehouse.create_partitioner();
                        auto time_end = utils::getTimePoint();
                        time_logger->info(fmt::format("warehouse partitioner created"));
                        time_logger->info(fmt::format("warehouse partition cost {}ms", float(time_end - time_start)/1000));
                     }
                     if (db.warehouse_update_ready())
                     {
                        warehouse.update_map = db.get_warehouse_update_map();
                        warehouse.update_partitioner();
                        db.set_warehouse_update_ready(false);
                        db.warehouse_clear();
                     }
                     // if(customer.created && !customer.traversed){
                     //    std::cout << "start traverse customer tree" << std::endl;
                     //    customer.traverse_tree();
                     //    std::cout << "customer tree traversed" << std::endl;
                     // }
                  }
                  if (t_i == 1)
                  {
                     if (!order.created && db.order_created())
                     {
                        time_logger->info(fmt::format("start create order partitioner"));
                        auto time_start = utils::getTimePoint();
                        order.partition_map = db.get_order_map();
                        order.create_partitioner();
                        auto time_end = utils::getTimePoint();
                        time_logger->info(fmt::format("order partitioner created"));
                        time_logger->info(fmt::format("order partition cost {}ms", float(time_end - time_start)/1000));
                     }
                     if(db.order_update_ready()){
                        order.update_map = db.get_order_update_map();
                        order.update_partitioner();
                        db.set_order_update_ready(false);
                        db.order_clear();
                     }
                     // if(order.created && !order.traversed){
                     //    std::cout << "start traverse order tree" << std::endl;
                     //    order.traverse_tree();
                     //    std::cout << "order tree traversed" << std::endl;
                     // }
                  }
                  if (t_i == 2)
                  {
                     if (!stock.created && db.stock_created())
                     {
                        time_logger->info(fmt::format("start create stock partitioner"));
                        auto time_start = utils::getTimePoint();
                        stock.partition_map = db.get_stock_map();
                        stock.create_partitioner();
                        auto time_end = utils::getTimePoint();
                        time_logger->info(fmt::format("stock partitioner created"));
                        time_logger->info(fmt::format("stock partition cost {}ms", float(time_end - time_start)/1000));
                     }
                     if (db.stock_update_ready())
                     {
                        stock.update_map = db.get_stock_update_map();
                        stock.update_partitioner();
                        db.set_stock_update_ready(false);
                        db.stock_clear();
                     }
                     // if(stock.created && !stock.traversed){
                     //    std::cout << "start traverse stock tree" << std::endl;
                     //    stock.traverse_tree();
                     //    std::cout << "stock tree traversed" << std::endl;
                     // }
                  }
                  if (t_i == 3)
                  {
                     if (!district.created && db.district_created())
                     {
                        time_logger->info(fmt::format("start create district partitioner"));
                        auto time_start = utils::getTimePoint();
                        district.partition_map = db.get_district_map();
                        district.create_partitioner();
                        auto time_end = utils::getTimePoint();
                        time_logger->info(fmt::format("district partitioner created"));
                        time_logger->info(fmt::format("district partition cost {}ms", float(time_end - time_start)/1000));
                     }
                     if (db.district_update_ready())
                     {
                        district.update_map = db.get_district_update_map();
                        district.update_partitioner();
                        db.set_district_update_ready(false);
                        db.district_clear();
                     }
                     if (!neworder.created && db.neworder_created())
                     {
                        time_logger->info(fmt::format("start create neworder partitioner"));
                        auto time_start = utils::getTimePoint();
                        neworder.partition_map = db.get_neworder_map();
                        neworder.create_partitioner();
                        auto time_end = utils::getTimePoint();
                        time_logger->info(fmt::format("neworder partitioner created"));
                        time_logger->info(fmt::format("neworder partition cost {}ms", float(time_end - time_start) / 1000));
                     }
                     if (db.neworder_update_ready())
                     {
                        neworder.update_map = db.get_neworder_update_map();
                        neworder.update_partitioner();
                        db.set_neworder_update_ready(false);
                        db.neworder_clear();
                     }
                  }
                  // if (t_i == 6)
                  // {
                  //    if (!orderline.created && db.orderline_created())
                  //    {
                  //       std::cout << "start create orderline partitioner" << std::endl;
                  //       orderline.partition_map = db.get_orderline_map();
                  //       orderline.create_partitioner();
                  //       std::cout << " orderline partitioner created" << std::endl;
                  //    }
                  //    // if(stock.created && !stock.traversed){
                  //    //    std::cout << "start traverse stock tree" << std::endl;
                  //    //    stock.traverse_tree();
                  //    //    std::cout << "stock tree traversed" << std::endl;
                  //    // }
                  // }
               }

            //    };

            /*
            if (FLAGS_tpcc_abort_pct && urand(0, 100) <= FLAGS_tpcc_abort_pct) {
               // abort
            } else {
               // commit
            }
            */
               threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);

            }
         }
                  if(FLAGS_nodes < 2){
         sleep(5 * int(db.getNodeID() + 1));
         switch (t_i)
         {
         case 0:
            if(!warehouse.traversed){
               warehouse.traverse_page();
               std::cout << "warehouse page traversed" << std::endl;
            }
            if(!district.traversed){
               district.traverse_page();
               std::cout << "district page traversed" << std::endl;
            }
            if(!history.traversed){
               history.traverse_page();
               std::cout << "history page traversed" << std::endl;
            }
            if(!item.traversed){
               item.traverse_page();
               std::cout << "item page traversed" << std::endl;
            }
            break;
         case 1:
            if(!customer.traversed){
               customer.traverse_page();
               std::cout << "customer page traversed" << std::endl;
            }
            if(!customerwdl.traversed){
               customerwdl.traverse_page();
               std::cout << "customerwdl page traversed" << std::endl;
            }
            if(!stock.traversed){
               stock.traverse_page();
               std::cout << "stock page traversed" << std::endl;
            }
            break;
            break;
         case 2:
            if(!neworder.traversed){
               neworder.traverse_page();
               std::cout << "neworder page traversed" << std::endl;
            }

            break;
         case 3:
            if(!order.traversed){
               order.traverse_page();
               std::cout << "order page traversed" << std::endl;
            }
            if(!order_wdc.traversed){
               order_wdc.traverse_page();
               std::cout << "orderwdc page traversed" << std::endl;
            }
            break;
         }
         }
         running_threads_counter--; });
   }

   

   // uint64_t n_i;
   // for (n_i = 0; n_i < FLAGS_sqlSendThreads; n_i++)
   // {
   //    printf("creat sender\n");
   //    std::thread sender([&, n_i]()
   //                       {
   //    uint32_t w_id;

   //    uint64_t senderId=n_i;
   //    while(keep_running){
   //       if (FLAGS_tpcc_warehouse_affinity) {
   //          w_id = senderId + 1 + warehouse_range_node.begin;
   //       } else if (FLAGS_tpcc_warehouse_locality) {
   //          w_id = urand(warehouse_range_node.begin + 1, warehouse_range_node.end);
   //       } else {
   //          w_id = urand(1, FLAGS_tpcc_warehouse_count);
   //          if (w_id <= (uint32_t)warehouse_range_node.begin || (w_id > (uint32_t)warehouse_range_node.end)) remote_node_new_order++;
   //       }

   //       std::string sql_content = txCreate(w_id);

   //       char sql[sqlLength];
   //       strcpy(sql, sql_content.c_str());
   //       while(!db.sendSqltoProxy(sql,senderId)){
   //             _mm_pause();
   //       }
   //    } });
   //    sender.detach();
   // }

   // -------------------------------------------------------------------------------------
   // Join Threads
   // -------------------------------------------------------------------------------------
   // report data size
   // std::thread data_consume([&]()
   // {
   //    while(keep_running){
   //    sleep(30);
   //    double gib = (db.getBuffermanager().getConsumedPages() * storage::EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
   //    std::cout << "data loaded - consumed space in GiB = " << gib << std::endl;
   //                         }
   // });
   // data_consume.detach();
   std::thread start_txn_count([&]()
                               {
      int time_count = 0;
      while(keep_running){
         sleep(10);
         for(int i = 0; i< int(FLAGS_worker); i++){
            if (time_count != 200)
            {
               change_line[i] = true;
            }
         }
         time_count += 10;
         if (time_count == 200)
         {
            count_ready = true;
         }
         else
         {
            count_ready = false;
         }
      } });
   start_txn_count.detach();
   sleep(FLAGS_TPCC_run_for_seconds);
   std::cout << "tpcc run over" << std::endl;
   keep_running = false;
   // keep_getting_sql = false;
   while (running_threads_counter)
   {
      _mm_pause();
   }
   sleep(5);
   db.getWorkerPool().joinAll();
   // -------------------------------------------------------------------------------------
   // db.stopProfiler();
   std::cout << "\n";
   double gib = (db.getBuffermanager().getConsumedPages() * storage::EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
   std::cout << "data loaded - consumed space in GiB = " << gib << std::endl;
   std::cout << "Starting hash table report "
             << "\n";

   db.getBuffermanager().reportHashTableStats();
}

void router_tpcc_run_without_codesign(ScaleStore &db)
{
   std::atomic<bool> keep_running = true;
   auto &catalog = db.getCatalog();
   std::atomic<u64> running_threads_counter = 0;
   bool change_line[FLAGS_worker];
   bool count_ready = false;
   for (int i = 0; i < int(FLAGS_worker); i++)
   {
      change_line[i] = false;
   }
   std::string configuration;
   if (FLAGS_tpcc_warehouse_affinity)
   {
      configuration = "warehouse_affinity";
   }
   else if (FLAGS_tpcc_warehouse_locality)
   {
      configuration = "warehouse_locality";
   }
   else
   {
      configuration = "warehouse_no_locality";
   }
   TPCC_workloadInfo experimentInfo{"TPCC", (uint64_t)warehouseCount, configuration};
   std::string currentFile = __FILE__;
   std::string abstract_filename = currentFile.substr(0, currentFile.find_last_of("/\\") + 1);
   std::vector<std::ofstream> neworder_times;
   std::vector<std::ofstream> outputs;
   for (uint64_t t_i = 0; t_i < FLAGS_worker; t_i++)
   {
      outputs.push_back(std::ofstream(abstract_filename + "../../TXN_LOG/worker_" + std::to_string(t_i)));
      neworder_times.push_back(std::ofstream(abstract_filename + "../../TXN_LOG/neworder_worker_" + std::to_string(t_i)));
   }
   for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i)
   {
      db.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]()
                                          {                                                
         // db.getBuffermanager().local_timer.insert({std::this_thread::get_id(), Timer()});                           
         running_threads_counter++;
         thread_id = t_i + (db.getNodeID() * FLAGS_worker);
         storage::DistributedBarrier barrier(catalog.getCatalogEntry(barrier_id).pid);
         barrier.wait();
         // Timer *local_timer =  &db.getBuffermanager().local_timer[std::this_thread::get_id()];
         while (keep_running) {
            char sql[sqlLength];
            uint64_t src_node;
            if(db.getSql(sql,&src_node)){
               auto start = utils::getTimePoint();
               std::string functionName = extractFunctionName(sql);
               std::vector<std::string> parameters;
               extractParameters(sql, ',', parameters);
               // local_timer->reset(true);
               excuteFunctionCall(functionName, parameters);
               auto end = utils::getTimePoint();
               if (count_ready)
               {
                  outputs[t_i] << (end - start) << " ";
                  // if (functionName == "newOrder")
                  // {
                  //    neworder_times[t_i] << (end - start) << " "
                  //                        << local_timer->local_elapsedMicroseconds() << " "
                  //                        << local_timer->remote_elapsedMicroseconds() << " ";
                  // }
               }
               // local_timer->reset(true);
               if(change_line[t_i] && count_ready){
                  outputs[t_i] << std::endl;
                  // neworder_times[t_i] << std::endl;
                  change_line[t_i] = false;
               }
               threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);

            }
         }
         if(FLAGS_nodes < 2){
         sleep(5 * int(db.getNodeID() + 1));
         switch (t_i)
         {
         case 0:
            if(!warehouse.traversed){
               warehouse.traverse_page();
               std::cout << "warehouse page traversed" << std::endl;
            }
            if(!district.traversed){
               district.traverse_page();
               std::cout << "district page traversed" << std::endl;
            }
            if(!history.traversed){
               history.traverse_page();
               std::cout << "history page traversed" << std::endl;
            }
            if(!item.traversed){
               item.traverse_page();
               std::cout << "item page traversed" << std::endl;
            }
            break;
         case 1:
            if(!customer.traversed){
               customer.traverse_page();
               std::cout << "customer page traversed" << std::endl;
            }
            if(!customerwdl.traversed){
               customerwdl.traverse_page();
               std::cout << "customerwdl page traversed" << std::endl;
            }
            if(!stock.traversed){
               stock.traverse_page();
               std::cout << "stock page traversed" << std::endl;
            }
            break;
            break;
         case 2:
            if(!neworder.traversed){
               neworder.traverse_page();
               std::cout << "neworder page traversed" << std::endl;
            }

            break;
         case 3:
            if(!order.traversed){
               order.traverse_page();
               std::cout << "order page traversed" << std::endl;
            }
            if(!order_wdc.traversed){
               order_wdc.traverse_page();
               std::cout << "orderwdc page traversed" << std::endl;
            }
            break;
         }
         }
         running_threads_counter--; });
   }

   // uint64_t n_i;
   // for (n_i = 0; n_i < FLAGS_sqlSendThreads; n_i++)
   // {
   //    printf("creat sender\n");
   //    std::thread sender([&, n_i]()
   //                       {
   //    uint32_t w_id;

   //    uint64_t senderId=n_i;
   //    while(keep_running){
   //       if (FLAGS_tpcc_warehouse_affinity) {
   //          w_id = senderId + 1 + warehouse_range_node.begin;
   //       } else if (FLAGS_tpcc_warehouse_locality) {
   //          w_id = urand(warehouse_range_node.begin + 1, warehouse_range_node.end);
   //       } else {
   //          w_id = urand(1, FLAGS_tpcc_warehouse_count);
   //          if (w_id <= (uint32_t)warehouse_range_node.begin || (w_id > (uint32_t)warehouse_range_node.end)) remote_node_new_order++;
   //       }

   //       std::string sql_content = txCreate(w_id);

   //       char sql[sqlLength];
   //       strcpy(sql, sql_content.c_str());
   //       while(!db.sendSqltoProxy(sql,senderId)){
   //             _mm_pause();
   //       }
   //    } });
   //    sender.detach();
   // }

   // -------------------------------------------------------------------------------------
   // Join Threads
   // -------------------------------------------------------------------------------------
   std::thread data_consume([&]()
                            {
                           while(keep_running){
  sleep(30);
            double gib = (db.getBuffermanager().getConsumedPages() * storage::EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
   std::cout << "data loaded - consumed space in GiB = " << gib << std::endl;
                           } });
   data_consume.detach();
   std::thread start_txn_count([&]()
                               {
                                 int time_count = 0;
                                 while (keep_running)
                                 {
                                    sleep(10);
                                    for (int i = 0; i < int(FLAGS_worker); i++)
                                    {
                                       if (time_count != 200)
                                       {
                                          change_line[i] = true;
                                       }
                                    }
                                    time_count += 10;
                                    if (time_count == 200)
                                    {
                                       count_ready = true;
                                    }
                                    else{
                                       count_ready = false;
                                    }
                                 }
                            });
   start_txn_count.detach();
   sleep(FLAGS_TPCC_run_for_seconds);
   keep_running = false;
   // keep_getting_sql = false;
   while (running_threads_counter)
   {
      _mm_pause();
   }
   sleep(5);
   db.getWorkerPool().joinAll();
   // -------------------------------------------------------------------------------------
   db.stopProfiler();
   double gib = (db.getBuffermanager().getConsumedPages() * storage::EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
   std::cout << "data loaded - consumed space in GiB = " << gib << std::endl;
   std::cout << "Starting hash table report "
             << "\n";
   db.getBuffermanager().reportHashTableStats();
}

void test_rapo(ScaleStore &db)
{
   std::atomic<bool> keep_running = true;
   auto &catalog = db.getCatalog();
   // bool keep_getting_sql = true;
   std::atomic<u64> running_threads_counter = 0;
   std::string configuration;
   bool change_line[FLAGS_worker];
   bool count_ready = false;
   int total_page_size;
   for (int i = 0; i < int(FLAGS_worker); i++)
   {
      change_line[i] = false;
   }
   if (FLAGS_tpcc_warehouse_affinity)
   {
      configuration = "warehouse_affinity";
   }
   else if (FLAGS_tpcc_warehouse_locality)
   {
      configuration = "warehouse_locality";
   }
   else
   {
      configuration = "warehouse_no_locality";
   }
   TPCC_workloadInfo experimentInfo{"TPCC", (uint64_t)warehouseCount, configuration};
   std::string currentFile = __FILE__;
   std::string abstract_filename = currentFile.substr(0, currentFile.find_last_of("/\\") + 1);
   std::string logFilePath = abstract_filename + "../../Logs/reorganize_time_log." + std::to_string(db.getNodeID()) + "txt";
   std::shared_ptr<spdlog::logger> time_logger = spdlog::basic_logger_mt("router_logger", logFilePath);
   time_logger->set_level(spdlog::level::info);
   time_logger->flush_on(spdlog::level::info);
   time_logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] %v");
   std::vector<std::ofstream> outputs;
   std::vector<std::ofstream> neworder_times;
   for (uint64_t t_i = 0; t_i < FLAGS_worker; t_i++)
   {
      outputs.push_back(std::ofstream(abstract_filename + "../../TXN_LOG/worker_" + std::to_string(t_i)));
      neworder_times.push_back(std::ofstream(abstract_filename + "../../TXN_LOG/neworder_worker_" + std::to_string(t_i)));
   }
   for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i)
   {
      db.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]()
                                          {
         // db.getBuffermanager().local_timer.insert({std::this_thread::get_id(), Timer()});                           
         running_threads_counter++;
         thread_id = t_i + (db.getNodeID() * FLAGS_worker);
         storage::DistributedBarrier barrier(catalog.getCatalogEntry(barrier_id).pid);
         barrier.wait();
         // Timer *local_timer =  &db.getBuffermanager().local_timer[std::this_thread::get_id()];
         while (keep_running) {
            char sql[sqlLength];
            uint64_t src_node;
            if(db.getSql(sql,&src_node)){
               auto start = utils::getTimePoint();
               std::string functionName = extractFunctionName(sql);
               std::vector<std::string> parameters;
               extractParameters(sql, ',', parameters);
               // local_timer->reset(true);
               excuteFunctionCall(functionName, parameters);
               auto end = utils::getTimePoint();
               if (count_ready)
               {
                  outputs[t_i] << (end - start) << " ";
                  // if (functionName == "newOrder")
                  // {
                  //    neworder_times[t_i] << (end - start) << " "
                  //                        << local_timer->local_elapsedMicroseconds() << " "
                  //                        << local_timer->remote_elapsedMicroseconds() << " ";
                  // }
               }
               // local_timer->reset(true);
               if(change_line[t_i] && count_ready){
                  outputs[t_i] << std::endl;
                  // neworder_times[t_i] << std::endl;
                  change_line[t_i] = false;
               }
               if (db.getNodeID() == 0)
               {
                  if (t_i == 0)
                  {
                     if (!warehouse.created)
                     {
                        total_page_size = warehouse.page_size() + history.page_size() + district.page_size() + customer.page_size() + order.page_size() + orderline.page_size() + stock.page_size() + item.page_size() + neworder.page_size();
                        time_logger->info(fmt::format("total_page_size {}", total_page_size));
                     }
                     if (!warehouse.created && db.warehouse_created())
                     {
                        time_logger->info(fmt::format("start create warehouse partitioner"));
                        auto time_start = utils::getTimePoint();
                        warehouse.partition_map = db.get_warehouse_map();
                        warehouse.create_partitioner();
                        auto time_end = utils::getTimePoint();
                        time_logger->info(fmt::format("warehouse partitioner created"));
                        time_logger->info(fmt::format("warehouse partition cost {}ms", float(time_end - time_start)/1000));
                     }
                     if (db.warehouse_update_ready())
                     {
                        warehouse.update_map = db.get_warehouse_update_map();
                        warehouse.update_partitioner();
                        db.set_warehouse_update_ready(false);
                        db.warehouse_clear();
                        time_logger->info(fmt::format("warehouse update ro_page {}", warehouse.page_ro_count));
                        warehouse.page_ro_count = 0;
                     }
                     // if(customer.created && !customer.traversed){
                     //    std::cout << "start traverse customer tree" << std::endl;
                     //    customer.traverse_tree();
                     //    std::cout << "customer tree traversed" << std::endl;
                     // }
                  }
                  if (t_i == 1)
                  {
                     if (!customer.created && db.customer_created())
                     {
                        time_logger->info(fmt::format("start create customer partitioner"));
                        auto time_start = utils::getTimePoint();
                        customer.partition_map = db.get_customer_map();
                        customer.create_partitioner();
                        auto time_end = utils::getTimePoint();
                        time_logger->info(fmt::format("customer partitioner created"));
                        time_logger->info(fmt::format("customer partition cost {}ms", float(time_end - time_start)/1000));
                        time_logger->info(fmt::format("customer init ro_page {}", customer.page_ro_count));
                        customer.page_ro_count = 0;
                     }
                     if(db.customer_update_ready()){
                        customer.update_map = db.get_customer_update_map();
                        customer.update_partitioner();
                        time_logger->info(fmt::format("customer update ro_page {}", customer.page_ro_count));
                        customer.page_ro_count = 0;
                        db.set_customer_update_ready(false);
                        db.customer_clear();
                     }

                     if (!order.created && db.order_created())
                     {
                        time_logger->info(fmt::format("start create order partitioner"));
                        auto time_start = utils::getTimePoint();
                        order.partition_map = db.get_order_map();
                        order.create_partitioner();
                        auto time_end = utils::getTimePoint();
                        time_logger->info(fmt::format("order partitioner created"));
                        time_logger->info(fmt::format("order partition cost {}ms", float(time_end - time_start)/1000));
                        time_logger->info(fmt::format("order init ro_page {}", order.page_ro_count));
                        order.page_ro_count = 0;
                     }
                     if(db.order_update_ready()){
                        order.update_map = db.get_order_update_map();
                        order.update_partitioner();
                        time_logger->info(fmt::format("order update ro_page {}", order.page_ro_count));
                        order.page_ro_count = 0;
                        db.set_order_update_ready(false);
                        db.order_clear();
                     }
                     // if(order.created && !order.traversed){
                     //    std::cout << "start traverse order tree" << std::endl;
                     //    order.traverse_tree();
                     //    std::cout << "order tree traversed" << std::endl;
                     // }
                  }
                  if (t_i == 2)
                  {
                     if (!stock.created && db.stock_created())
                     {
                        time_logger->info(fmt::format("start create stock partitioner"));
                        auto time_start = utils::getTimePoint();
                        stock.partition_map = db.get_stock_map();
                        stock.create_partitioner();
                        auto time_end = utils::getTimePoint();
                        time_logger->info(fmt::format("stock partitioner created"));
                        time_logger->info(fmt::format("stock partition cost {}ms", float(time_end - time_start)/1000));
                        time_logger->info(fmt::format("stock init ro_page {}", stock.page_ro_count));
                        stock.page_ro_count = 0;
                     }
                     if (db.stock_update_ready())
                     {
                        stock.update_map = db.get_stock_update_map();
                        stock.update_partitioner();
                        time_logger->info(fmt::format("stock update ro_page {}", stock.page_ro_count));
                        stock.page_ro_count = 0;
                        db.set_stock_update_ready(false);
                        db.stock_clear();
                     }
                     // if(stock.created && !stock.traversed){
                     //    std::cout << "start traverse stock tree" << std::endl;
                     //    stock.traverse_tree();
                     //    std::cout << "stock tree traversed" << std::endl;
                     // }
                  }
                  if (t_i == 3)
                  {
                     if (!district.created && db.district_created())
                     {
                        time_logger->info(fmt::format("start create district partitioner"));
                        auto time_start = utils::getTimePoint();
                        district.partition_map = db.get_district_map();
                        district.create_partitioner();
                        auto time_end = utils::getTimePoint();
                        time_logger->info(fmt::format("district partitioner created"));
                        time_logger->info(fmt::format("district partition cost {}ms", float(time_end - time_start)/1000));
                        time_logger->info(fmt::format("district init ro_page {}", district.page_ro_count));
                        district.page_ro_count = 0;
                     }
                     if (db.district_update_ready())
                     {
                        district.update_map = db.get_district_update_map();
                        district.update_partitioner();
                        time_logger->info(fmt::format("district update ro_page {}", district.page_ro_count));
                        district.page_ro_count = 0;
                        db.set_district_update_ready(false);
                        db.district_clear();
                     }
                     if (!neworder.created && db.neworder_created())
                     {
                        time_logger->info(fmt::format("start create neworder partitioner"));
                        auto time_start = utils::getTimePoint();
                        neworder.partition_map = db.get_neworder_map();
                        neworder.create_partitioner();
                        auto time_end = utils::getTimePoint();
                        time_logger->info(fmt::format("neworder partitioner created"));
                        time_logger->info(fmt::format("neworder partition cost {}ms", float(time_end - time_start) / 1000));
                        time_logger->info(fmt::format("neworder init ro_page {}", neworder.page_ro_count));
                        neworder.page_ro_count = 0;
                     }
                     if (db.neworder_update_ready())
                     {
                        neworder.update_map = db.get_neworder_update_map();
                        neworder.update_partitioner();
                        time_logger->info(fmt::format("district update ro_page {}", neworder.page_ro_count));
                        neworder.page_ro_count = 0;
                        db.set_neworder_update_ready(false);
                        db.neworder_clear();
                     }
                  }
                  // if (t_i == 6)
                  // {
                  //    if (!orderline.created && db.orderline_created())
                  //    {
                  //       std::cout << "start create orderline partitioner" << std::endl;
                  //       orderline.partition_map = db.get_orderline_map();
                  //       orderline.create_partitioner();
                  //       std::cout << " orderline partitioner created" << std::endl;
                  //    }
                  //    // if(stock.created && !stock.traversed){
                  //    //    std::cout << "start traverse stock tree" << std::endl;
                  //    //    stock.traverse_tree();
                  //    //    std::cout << "stock tree traversed" << std::endl;
                  //    // }
                  // }
               }

            //    };

            /*
            if (FLAGS_tpcc_abort_pct && urand(0, 100) <= FLAGS_tpcc_abort_pct) {
               // abort
            } else {
               // commit
            }
            */
               threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);

            }
         }
                  if(FLAGS_nodes < 2){
         sleep(5 * int(db.getNodeID() + 1));
         switch (t_i)
         {
         case 0:
            if(!warehouse.traversed){
               warehouse.traverse_page();
               std::cout << "warehouse page traversed" << std::endl;
            }
            if(!district.traversed){
               district.traverse_page();
               std::cout << "district page traversed" << std::endl;
            }
            if(!history.traversed){
               history.traverse_page();
               std::cout << "history page traversed" << std::endl;
            }
            if(!item.traversed){
               item.traverse_page();
               std::cout << "item page traversed" << std::endl;
            }
            break;
         case 1:
            if(!customer.traversed){
               customer.traverse_page();
               std::cout << "customer page traversed" << std::endl;
            }
            if(!customerwdl.traversed){
               customerwdl.traverse_page();
               std::cout << "customerwdl page traversed" << std::endl;
            }
            if(!stock.traversed){
               stock.traverse_page();
               std::cout << "stock page traversed" << std::endl;
            }
            break;
            break;
         case 2:
            if(!neworder.traversed){
               neworder.traverse_page();
               std::cout << "neworder page traversed" << std::endl;
            }

            break;
         case 3:
            if(!order.traversed){
               order.traverse_page();
               std::cout << "order page traversed" << std::endl;
            }
            if(!order_wdc.traversed){
               order_wdc.traverse_page();
               std::cout << "orderwdc page traversed" << std::endl;
            }
            break;
         }
         }
         running_threads_counter--; });
   }

   

   // uint64_t n_i;
   // for (n_i = 0; n_i < FLAGS_sqlSendThreads; n_i++)
   // {
   //    printf("creat sender\n");
   //    std::thread sender([&, n_i]()
   //                       {
   //    uint32_t w_id;

   //    uint64_t senderId=n_i;
   //    while(keep_running){
   //       if (FLAGS_tpcc_warehouse_affinity) {
   //          w_id = senderId + 1 + warehouse_range_node.begin;
   //       } else if (FLAGS_tpcc_warehouse_locality) {
   //          w_id = urand(warehouse_range_node.begin + 1, warehouse_range_node.end);
   //       } else {
   //          w_id = urand(1, FLAGS_tpcc_warehouse_count);
   //          if (w_id <= (uint32_t)warehouse_range_node.begin || (w_id > (uint32_t)warehouse_range_node.end)) remote_node_new_order++;
   //       }

   //       std::string sql_content = txCreate(w_id);

   //       char sql[sqlLength];
   //       strcpy(sql, sql_content.c_str());
   //       while(!db.sendSqltoProxy(sql,senderId)){
   //             _mm_pause();
   //       }
   //    } });
   //    sender.detach();
   // }

   // -------------------------------------------------------------------------------------
   // Join Threads
   // -------------------------------------------------------------------------------------
   // report data size
   // std::thread data_consume([&]()
   // {
   //    while(keep_running){
   //    sleep(30);
   //    double gib = (db.getBuffermanager().getConsumedPages() * storage::EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
   //    std::cout << "data loaded - consumed space in GiB = " << gib << std::endl;
   //                         }
   // });
   // data_consume.detach();
   std::thread start_txn_count([&]()
                               {
      int time_count = 0;
      while(keep_running){
         sleep(10);
         for(int i = 0; i< int(FLAGS_worker); i++){
            if (time_count != 200)
            {
               change_line[i] = true;
            }
         }
         time_count += 10;
         if (time_count == 200)
         {
            count_ready = true;
         }
         else
         {
            count_ready = false;
         }
      } });
   start_txn_count.detach();
   sleep(FLAGS_TPCC_run_for_seconds);
   std::cout << "tpcc run over" << std::endl;
   keep_running = false;
   // keep_getting_sql = false;
   while (running_threads_counter)
   {
      _mm_pause();
   }
   sleep(5);
   db.getWorkerPool().joinAll();
   // -------------------------------------------------------------------------------------
   // db.stopProfiler();
   std::cout << "\n";
   double gib = (db.getBuffermanager().getConsumedPages() * storage::EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
   std::cout << "data loaded - consumed space in GiB = " << gib << std::endl;
   std::cout << "Starting hash table report "
             << "\n";

   db.getBuffermanager().reportHashTableStats();
}
