#include "PerfEvent.hpp"
#include "scalestore/Config.hpp"
#include "scalestore/ScaleStore.hpp"
#include "scalestore/rdma/CommunicationManager.hpp"
#include "scalestore/storage/datastructures/BTree.hpp"
#include "scalestore/utils/RandomGenerator.hpp"
#include "scalestore/utils/ScrambledZipfGenerator.hpp"
#include "scalestore/utils/Time.hpp"
#include "ycsb_adapter.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------

// -------------------------------------------------------------------------------------
using namespace scalestore;
int main(int argc, char *argv[])
{

   gflags::SetUsageMessage("Catalog Test");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   // prepare workload
   std::vector<std::string> workload_type; // warm up or benchmark
   std::vector<uint32_t> workloads;
   std::vector<double> zipfs;
   if (FLAGS_YCSB_all_workloads)
   {
      workloads.push_back(5);
      workloads.push_back(50);
      workloads.push_back(95);
      workloads.push_back(100);
   }
   else
   {
      workloads.push_back(FLAGS_YCSB_read_ratio);
   }

   if (FLAGS_YCSB_warm_up)
   {
      workload_type.push_back("YCSB_warm_up");
      workload_type.push_back("YCSB_txn");
   }
   else
   {
      workload_type.push_back("YCSB_txn");
   }

   if (FLAGS_YCSB_all_zipf)
   {
      // zipfs.insert(zipfs.end(), {0.0,1.0,1.25,1.5,1.75,2.0});
      // zipfs.insert(zipfs.end(), {1.05,1.1,1.15,1.20,1.3,1.35,1.4,1.45});
      zipfs.insert(zipfs.end(), {0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0});
   }
   else
   {
      zipfs.push_back(FLAGS_YCSB_zipf_factor);
   }
   // -------------------------------------------------------------------------------------
   ScaleStore scalestore;
   auto &catalog = scalestore.getCatalog();
   // -------------------------------------------------------------------------------------

   auto barrier_wait = [&]()
   {
      for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i)
      {
         scalestore.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]()
                                                     {
            storage::DistributedBarrier barrier(catalog.getCatalogEntry(BARRIER_ID).pid);
            barrier.wait(); });
      }
      scalestore.getWorkerPool().joinAll();
   };
   // -------------------------------------------------------------------------------------
   // create Btree (0), Barrier(1)
   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   i64 YCSB_tuple_count = FLAGS_YCSB_tuple_count;
   // -------------------------------------------------------------------------------------
   ScaleStoreAdapter ycsb_adapter;
   auto nodePartition = partition(scalestore.getNodeID(), FLAGS_nodes, YCSB_tuple_count);
   scalestore.getWorkerPool().scheduleJobSync(0, [&]()
                                              {
                                                 ycsb_adapter = ScaleStoreAdapter(scalestore, "ycsb");
                                                 // -------------------------------------------------------------------------------------
                                                 if (scalestore.getNodeID() == 0)
                                                    scalestore.createBarrier(FLAGS_worker * FLAGS_nodes); // distributed barrier
                                              });
   std::cout << "here" << std::endl;
   // -------------------------------------------------------------------------------------
   // Build YCSB Table / Tree
   // -------------------------------------------------------------------------------------
   YCSB_workloadInfo builtInfo{"Build", YCSB_tuple_count, FLAGS_YCSB_read_ratio, FLAGS_YCSB_zipf_factor, (FLAGS_YCSB_local_zipf ? "local_zipf" : "global_zipf")};
   scalestore.startProfiler(builtInfo);
   std::string currentFile = __FILE__;
   std::string abstract_filename = currentFile.substr(0, currentFile.find_last_of("/\\") + 1);
   std::string logFilePath = abstract_filename + "../../Logs/reorganize_time_log" + std::to_string(scalestore.getNodeID()) + ".txt";
   std::shared_ptr<spdlog::logger> time_logger = spdlog::basic_logger_mt("router_logger", logFilePath);
   time_logger->set_level(spdlog::level::info);
   time_logger->flush_on(spdlog::level::info);
   time_logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] %v");
   for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i)
   {
      scalestore.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]()
                                                  {
         // -------------------------------------------------------------------------------------
         // partition
         auto nodeKeys = nodePartition.end - nodePartition.begin;
         auto threadPartition = partition(t_i, FLAGS_worker, nodeKeys);
         auto begin = nodePartition.begin + threadPartition.begin;
         auto end = nodePartition.begin + threadPartition.end;
         std::cout<< end << std::endl;
         storage::DistributedBarrier barrier(catalog.getCatalogEntry(BARRIER_ID).pid);
         barrier.wait();
         // -------------------------------------------------------------------------------------
         V value;
         for (K k_i = i64(begin); k_i < i64(end); k_i++) {
            utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&value), sizeof(V));
            ycsb_adapter.insert(k_i, value);
            threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
         }
         std::cout<< "done" << std::endl;
         // -------------------------------------------------------------------------------------
         barrier.wait(); });
   }
   scalestore.getWorkerPool().joinAll();
   scalestore.stopProfiler();

   double gib = (scalestore.getBuffermanager().getConsumedPages() * storage::EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
   std::cout << "data loaded - consumed space in GiB = " << gib << std::endl;
   if (FLAGS_YCSB_flush_pages)
   {
      std::cout << "Flushing all pages"
                << "\n";
      scalestore.getBuffermanager().writeAllPages();

      std::cout << "Done"
                << "\n";
   }

   for (auto ZIPF : zipfs)
   {
      // -------------------------------------------------------------------------------------
      // YCSB Transaction
      // -------------------------------------------------------------------------------------
      std::unique_ptr<utils::ScrambledZipfGenerator> zipf_random;
      if (FLAGS_YCSB_partitioned)
         zipf_random = std::make_unique<utils::ScrambledZipfGenerator>(nodePartition.begin, nodePartition.end - 1,
                                                                       ZIPF);
      else
         zipf_random = std::make_unique<utils::ScrambledZipfGenerator>(0, YCSB_tuple_count, ZIPF);

      // -------------------------------------------------------------------------------------
      // zipf creation can take some time due to floating point loop therefore wait with barrier
      barrier_wait();
      // -------------------------------------------------------------------------------------
      // std::vector<std::ofstream> outputs;
      // bool change_line[FLAGS_worker];
      // for (int i = 0; i < int(FLAGS_worker); i++)
      // {
      //    change_line[i] = false;
      // }
      // for (uint64_t t_i = 0; t_i < FLAGS_worker; t_i++)
      // {
      //    outputs.push_back(std::ofstream(abstract_filename + "../../TXN_LOG/worker_" + std::to_string(t_i)));
      // }
      for (auto READ_RATIO : workloads)
      {
         for (auto TYPE : workload_type)
         {
            barrier_wait();
            std::atomic<bool> keep_running = true;
            std::atomic<u64> running_threads_counter = 0;

            // uint64_t zipf_offset = 0;
            // if (FLAGS_YCSB_local_zipf)
            //    zipf_offset = (YCSB_tuple_count / FLAGS_nodes) * scalestore.getNodeID();

            YCSB_workloadInfo experimentInfo{TYPE, YCSB_tuple_count, READ_RATIO, ZIPF, (FLAGS_YCSB_local_zipf ? "local_zipf" : "global_zipf")};
            scalestore.startProfiler(experimentInfo);
            for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i)
            {
               scalestore.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]()
                                                           {
                  running_threads_counter++;
                  storage::DistributedBarrier barrier(catalog.getCatalogEntry(BARRIER_ID).pid);
                  barrier.wait();
                  while (keep_running) {
                     uint64_t src_node;
                     std::vector<TxnNode> keylist;
                     if (scalestore.getKey(keylist, &src_node))
                     {
                        // auto start = utils::getTimePoint();
                        for (const auto keynode : keylist)
                        {
                           if (keynode.is_read_only)
                           {
                              V result;
                              auto success = ycsb_adapter.lookup_opt(keynode.key, result);
                              ensure(success);
                           }
                           else
                           {
                              V payload;
                              utils::RandomGenerator::getRandString(reinterpret_cast<u8 *>(&payload), sizeof(V));
                              ycsb_adapter.insert(keynode.key, payload);
                           }
                        }
                        // auto end = utils::getTimePoint();
                        // outputs[t_i] << (end - start) << " ";
                        // if (change_line[t_i])
                        // {
                        //    outputs[t_i] << std::endl;
                        //    change_line[t_i] = false;
                        // }
                        threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
                     }
                     if (FLAGS_use_codesign && scalestore.getNodeID() == 0){
                        if(t_i == 0){
                           if (scalestore.ycsb_map_created() && !ycsb_adapter.created)
                           {
                              time_logger->info(fmt::format("start create ycsb partitioner thread t{}", t_i));
                              auto time_start = utils::getTimePoint();
                              ycsb_adapter.partition_map = scalestore.get_ycsb_map();
                              ycsb_adapter.start_part = true;
                              ycsb_adapter.create_partitioner();
                              auto time_end = utils::getTimePoint();
                              time_logger->info(fmt::format("ycsb partitioner created thread t{}", t_i));
                              time_logger->info(fmt::format("ycsb partition t{} cost {}ms", t_i, float(time_end - time_start) / 1000));
                           }
                           if (scalestore.ycsb_update_ready())
                           {
                              time_logger->info(fmt::format("start update ycsb partitioner thread t{}", t_i));
                              auto time_start = utils::getTimePoint();
                              ycsb_adapter.update_map = scalestore.get_ycsb_update_map();
                              ycsb_adapter.update_partitioner();
                              auto time_end = utils::getTimePoint();
                              scalestore.set_ycsb_update_ready(false);
                              scalestore.ycsb_map_clear();
                              time_logger->info(fmt::format("ycsb partitioner update thread t{}", t_i));
                              time_logger->info(fmt::format("ycsb partition t{} cost {}ms", t_i, float(time_end - time_start) / 1000));
                           }
                        }
                        // else{
                        //    if (ycsb_adapter.start_part && !ycsb_adapter.creates[t_i])
                        //    {
                        //       time_logger->info(fmt::format("start create ycsb partitioner thread t{}", t_i));
                        //       auto time_start = utils::getTimePoint();
                        //       ycsb_adapter.create_partitioner(t_i);
                        //       auto time_end = utils::getTimePoint();
                        //       time_logger->info(fmt::format("ycsb partitioner created thread t{}", t_i));
                        //       time_logger->info(fmt::format("ycsb partition t{} cost {}ms", t_i, float(time_end - time_start) / 1000));
                        //    }
                        // }
                     }
                  }
                  running_threads_counter--; });
            }
            // -------------------------------------------------------------------------------------
            // Join Threads
            // -------------------------------------------------------------------------------------
            //             std::thread start_txn_count([&]()
            //                                         {
            //                            while(keep_running){
            //   sleep(10);
            //   for(int i = 0; i< int(FLAGS_worker); i++){
            //    change_line[i] = true;
            //   }
            //                            } });
            //             start_txn_count.detach();
            sleep(FLAGS_YCSB_run_for_seconds);
            keep_running = false;
            while (running_threads_counter)
            {
               _mm_pause();
            }
            scalestore.getWorkerPool().joinAll();
            // -------------------------------------------------------------------------------------
            auto a = utils::getTimePoint();
            scalestore.stopProfiler();
            auto b = utils::getTimePoint();
            std::cout << "stop_time" << b - a << std::endl;

            if (FLAGS_YCSB_record_latency)
            {
               std::atomic<bool> keep_running = true;
               constexpr uint64_t LATENCY_SAMPLES = 1e6;
               bool start_timer = false;
               YCSB_workloadInfo experimentInfo{"Latency", YCSB_tuple_count, READ_RATIO, ZIPF, (FLAGS_YCSB_local_zipf ? "local_zipf" : "global_zipf")};
               // scalestore.startProfiler(experimentInfo);
               std::vector<uint64_t> tl_microsecond_latencies[FLAGS_worker];
               for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i)
               {
                  scalestore.getWorkerPool().scheduleJobAsync(t_i, [&, t_i]()
                                                              {
                     running_threads_counter++;
                     uint64_t ops = 0;
                     storage::DistributedBarrier barrier(catalog.getCatalogEntry(BARRIER_ID).pid);
                     barrier.wait();
                     start_timer = true;
                     // use for GAM comparision
                     // V payload;
                     // utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(V));
                     while (keep_running)
                     {
                        uint64_t src_node;
                        std::vector<TxnNode> keylist;
                        if (scalestore.getKey(keylist, &src_node))
                        {
                           auto start = utils::getTimePoint();
                           for (const auto keynode : keylist)
                           {
                              if (keynode.is_read_only)
                              {
                                 V result;
                                 auto success = ycsb_adapter.lookup_opt(keynode.key, result);
                                 ensure(success);
                              }
                              else
                              {
                                 V payload;
                                 utils::RandomGenerator::getRandString(reinterpret_cast<u8 *>(&payload), sizeof(V));
                                 ycsb_adapter.insert(keynode.key, payload);
                              }
                              
                           }
                           auto end = utils::getTimePoint();
                           threads::Worker::my().counters.incr_by(profiling::WorkerCounters::latency, (end - start));
                           threads::Worker::my().counters.incr(profiling::WorkerCounters::tx_p);
                           if (ops < LATENCY_SAMPLES)
                                 tl_microsecond_latencies[t_i].push_back(end - start);
                           ops++;
                        }
                     }
                        running_threads_counter--; });
               }
               while (!start_timer)
               {
                  _mm_pause();
               }

               sleep(10);
               keep_running = false;
               while (running_threads_counter)
               {
                  _mm_pause();
               }
               // -------------------------------------------------------------------------------------
               // Join Threads
               // -------------------------------------------------------------------------------------
               scalestore.getWorkerPool().joinAll();
               // -------------------------------------------------------------------------------------
               scalestore.stopProfiler();
               // -------------------------------------------------------------------------------------
               // start_timer = false;
               // std::cout << "data loaded - consumed space in GiB = " << gib << std::endl;
               // scalestore.getWorkerPool().scheduleJobAsync(0, [&]()
               //                                             {
               //       ycsb_adapter.traverse_page();
               //       storage::DistributedBarrier barrier(catalog.getCatalogEntry(BARRIER_ID).pid);
               //       barrier.wait();
               //       start_timer = true;
               //           });
               // while (!start_timer)
               // {
               //    _mm_pause();
               // }

               // combine vector of threads into one
               std::vector<uint64_t> microsecond_latencies;
               for (uint64_t t_i = 0; t_i < FLAGS_worker; ++t_i)
               {
                  microsecond_latencies.insert(microsecond_latencies.end(), tl_microsecond_latencies[t_i].begin(), tl_microsecond_latencies[t_i].end());
                  std::cout << "tx_lan_size " << tl_microsecond_latencies[t_i].size() << std::endl;
               }

               {
                  std::cout << "Shuffle samples " << microsecond_latencies.size() << std::endl;
                  std::random_device rd;
                  std::mt19937 g(rd());
                  std::shuffle(microsecond_latencies.begin(), microsecond_latencies.end(), g);

                  // write out 400 samples
                  std::ofstream latency_file;
                  std::ofstream::openmode open_flags = std::ios::app;
                  std::string filename = FLAGS_csvFile + "_lantency_sample.csv";
                  bool csv_initialized = std::filesystem::exists(filename);
                  latency_file.open(filename, open_flags);
                  if (!csv_initialized)
                  {
                     latency_file << "workload,tag,ReadRatio,YCSB_tuple_count,zipf,latency" << std::endl;
                  }
                  for (uint64_t s_i = 0; s_i < 1000; s_i++)
                  {
                     latency_file << TYPE << "," << FLAGS_tag << "," << READ_RATIO << "," << YCSB_tuple_count << "," << ZIPF << "," << microsecond_latencies[s_i] << std::endl;
                  }
                  latency_file.close();
               }
               std::cout << "Sorting Latencies"
                         << "\n";

               std::sort(microsecond_latencies.begin(), microsecond_latencies.end());
               std::cout << "Latency (min/median/max/99%): " << (microsecond_latencies[0]) << ","
                         << (microsecond_latencies[microsecond_latencies.size() / 2]) << ","
                         << (microsecond_latencies.back()) << ","
                         << (microsecond_latencies[(int)(microsecond_latencies.size() * 0.99)]) << std::endl;
               // -------------------------------------------------------------------------------------
               // write to csv file
               std::ofstream latency_file;
               std::ofstream::openmode open_flags = std::ios::app;
               std::string filename = FLAGS_csvFile + "_lantency_info.csv";
               bool csv_initialized = std::filesystem::exists(filename);
               latency_file.open(filename, open_flags);
               if (!csv_initialized)
               {
                  latency_file << "workload,tag,ReadRatio,YCSB_tuple_count,zipf,min,median,max,95th,99th,999th" << std::endl;
               }
               latency_file << TYPE << "," << FLAGS_tag << "," << READ_RATIO << "," << YCSB_tuple_count << "," << ZIPF << "," << (microsecond_latencies[0])
                            << "," << (microsecond_latencies[microsecond_latencies.size() / 2]) << ","
                            << (microsecond_latencies.back()) << ","
                            << (microsecond_latencies[(int)(microsecond_latencies.size() * 0.95)]) << ","
                            << (microsecond_latencies[(int)(microsecond_latencies.size() * 0.99)]) << ","
                            << (microsecond_latencies[(int)(microsecond_latencies.size() * 0.999)])
                            << std::endl;
               latency_file.close();
            }
         }
      }
   }
   std::cout << "Starting hash table report "
             << "\n";
   scalestore.getBuffermanager().reportHashTableStats();
   return 0;
}
