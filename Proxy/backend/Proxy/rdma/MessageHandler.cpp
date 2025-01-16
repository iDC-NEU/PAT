#include "MessageHandler.hpp"
#include "Defs.hpp"
#include "Proxy/threads/CoreManager.hpp"
#include <cstring>
#include "../frontend/tpcc/proxy_tpcc_workload.hpp"
#include "../utils/Time.cpp"
// -------------------------------------------------------------------------------------
#include <numeric>

namespace Proxy
{
   namespace rdma
   {
      MessageHandler::MessageHandler(rdma::CM<InitMessage> &cm, router::Router &router, NodeID nodeId, spdlog::logger &router_logger, spdlog::logger &txn_logger)
          : cm(cm), router(router), nodeId(nodeId), mbPartitions(FLAGS_messageHandlerThreads), cctxsmap(FLAGS_nodes), routerCach((FLAGS_nodes * FLAGS_sqlSendThreads) + (FLAGS_nodes * FLAGS_worker)), router_logger(router_logger), txn_logger(txn_logger)
      {
         if (FLAGS_routertimeLog)
         {
            std::string currentFile = __FILE__;
            std::string abstract_filename = currentFile.substr(0, currentFile.find_last_of("/\\") + 1);
            std::string logFilePath = abstract_filename + "../Logs/routertimeLog.txt";
            routertime_logger = spdlog::basic_logger_mt("routertime_logger", logFilePath);
            routertime_logger->set_level(spdlog::level::info);
            routertime_logger->flush_on(spdlog::level::info);
            routertime_logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] %v");
         }

         for (uint64_t i = 0; i < FLAGS_nodes; i++)
         {
            std::mutex *x = new std::mutex;
            mtxs.push_back(x);
         }
         std::string currentFile = __FILE__;
         std::string abstract_filename = currentFile.substr(0, currentFile.find_last_of("/\\") + 1);
         for (uint64_t i = 0; i < FLAGS_messageHandlerThreads; i++)
         {
            proxy_time_per_thread.push_back(0);
            router_number_per_thread.push_back(0);
            graph_number_per_thread.push_back(0);
            random_number_per_thread.push_back(0);
            outputs.push_back(std::ofstream(abstract_filename + "../TXN_LOG/worker_" + std::to_string(i)));
            mail_mtxs.push_back(new std::mutex());
         }
         // partition mailboxes
         size_t n = (FLAGS_nodes * FLAGS_sqlSendThreads) + (FLAGS_nodes * FLAGS_worker);
         if (n > 0)
         {
            ensure(FLAGS_messageHandlerThreads <= n); // avoid over subscribing message handler threads
            const uint64_t blockSize = n / FLAGS_messageHandlerThreads;
            ensure(blockSize > 0);
            for (uint64_t t_i = 0; t_i < FLAGS_messageHandlerThreads; t_i++)
            {
               auto begin = t_i * blockSize;
               auto end = begin + blockSize;
               if (t_i == FLAGS_messageHandlerThreads - 1)
                  end = n;

               // parititon mailboxes
               uint8_t *partition = (uint8_t *)cm.getGlobalBuffer().allocate(end - begin, CACHE_LINE); // CL aligned
               ensure(((uintptr_t)partition) % CACHE_LINE == 0);
               // cannot use emplace because of mutex
               mbPartitions[t_i].mailboxes = partition;
               mbPartitions[t_i].numberMailboxes = end - begin;
               mbPartitions[t_i].beginId = begin;
            }
            startThread();
         };
      }
      // -------------------------------------------------------------------------------------
      void MessageHandler::init()
      {
         InitMessage *initServer = (InitMessage *)cm.getGlobalBuffer().allocate(sizeof(InitMessage));
         // -------------------------------------------------------------------------------------
         size_t numConnections = (FLAGS_nodes * FLAGS_sqlSendThreads + 1) + (FLAGS_nodes * FLAGS_worker);
         connectedClients = numConnections;
         while (cm.getNumberIncomingConnections() != (numConnections))
            ; // block until client is connected
         // -------------------------------------------------------------------------------------
         std::cout << "Number numConnections " << numConnections << std::endl;
         // wait until all mh are connected
         std::vector<RdmaContext *> rdmaCtxs(cm.getIncomingConnections()); // get cm ids of incomming

         // -------------------------------------------------------------------------------------
         // shuffle connections
         // -------------------------------------------------------------------------------------
         auto rng = std::default_random_engine{};
         std::shuffle(std::begin(rdmaCtxs), std::end(rdmaCtxs), rng);

         std::vector<RdmaContext *> ForRouterMaprdmaCctx;
         // 均匀分配MH和worker
         {
            std::vector<RdmaContext *> MHrdmaCctx;
            for (uint64_t i = 0; i < rdmaCtxs.size(); ++i)
            {
               if (rdmaCtxs[i]->type == Type::MESSAGE_HANDLER && rdmaCtxs[i]->typeId == 99)
               {
                  MHrdmaCctx.push_back(rdmaCtxs[i]);
               }
               else if (rdmaCtxs[i]->type == Type::MESSAGE_HANDLER && rdmaCtxs[i]->typeId == 90)
               {
                  ForRouterMaprdmaCctx.push_back(rdmaCtxs[i]);
               }
            }
            rdmaCtxs.erase(
                std::remove_if(rdmaCtxs.begin(), rdmaCtxs.end(), [](RdmaContext *c)
                               { return ((c->type == Type::MESSAGE_HANDLER) && (c->typeId == 90)); }),
                rdmaCtxs.end());

            int averageInterval = rdmaCtxs.size() / MHrdmaCctx.size();

            rdmaCtxs.erase(
                std::remove_if(rdmaCtxs.begin(), rdmaCtxs.end(), [](RdmaContext *c)
                               { return c->type == Type::MESSAGE_HANDLER; }),
                rdmaCtxs.end());

            for (uint64_t i = 0; i < MHrdmaCctx.size(); ++i)
            {
               int insertIndex = averageInterval * i;
               rdmaCtxs.insert(rdmaCtxs.begin() + insertIndex, MHrdmaCctx[i]);
            }
         }
         uint64_t counter = 0;
         uint64_t partitionId = 0;
         uint64_t partitionOffset = 0;
         for (auto *rContext : rdmaCtxs)
         {

            // partially initiallize connection connectxt
            ConnectionContext cctx;
            if (rContext->type == Type::MESSAGE_HANDLER)
            {

               cctx.incoming = (Message *)cm.getGlobalBuffer().allocate(rdma::SIZE_SQLMESSAGE, CACHE_LINE);
               cctx.outcoming = (Message *)cm.getGlobalBuffer().allocate(rdma::SIZE_SQLMESSAGE, CACHE_LINE);
               cctx.mbforBusy = (uint8_t *)cm.getGlobalBuffer().allocate(1, CACHE_LINE);
            }
            else
            {

               cctx.incoming = (Message *)cm.getGlobalBuffer().allocate(rdma::SIZE_TXNKEYSMESSAGE, CACHE_LINE);
               cctx.outcoming = (Message *)cm.getGlobalBuffer().allocate(rdma::SIZE_TXNKEYSMESSAGE, CACHE_LINE);
               cctx.mbforBusy = 0;
            }
            cctx.rctx = rContext;
            // -------------------------------------------------------------------------------------
            // find correct mailbox in partitions
            if ((counter >= (mbPartitions[partitionId].beginId + mbPartitions[partitionId].numberMailboxes)))
            {
               partitionId++;
               partitionOffset = 0;
            }
            auto &mbPartition = mbPartitions[partitionId];
            ensure(mbPartition.beginId + partitionOffset == counter);
            // -------------------------------------------------------------------------------------
            // fill init message
            initServer->mbOffset = (uintptr_t)&mbPartition.mailboxes[partitionOffset];
            initServer->plOffset = (uintptr_t)cctx.incoming;
            initServer->mbOffsetforBusy = (uintptr_t)cctx.mbforBusy;
            initServer->type = rdma::MESSAGE_TYPE::Init;
            // -------------------------------------------------------------------------------------
            cm.exchangeInitialMesssage(*(cctx.rctx), initServer);
            // -------------------------------------------------------------------------------------
            // finish initialization of cctx
            cctx.plOffset = (reinterpret_cast<InitMessage *>((cctx.rctx->applicationData)))->plOffset;
            cctx.mbOffset = (reinterpret_cast<InitMessage *>((cctx.rctx->applicationData)))->mbOffset;
            cctx.dbId = (reinterpret_cast<InitMessage *>((cctx.rctx->applicationData)))->bmId;
            router_logger.info(fmt::format("t_i: {} node_id: {}", counter, cctx.dbId));
            // -------------------------------------------------------------------------------------
            // cctxs[cctx.dbId]=(cctx);
            cctxs.push_back(cctx);
            if (rContext->type == Type::MESSAGE_HANDLER)
            {
               cctxsmap[cctx.dbId].push_back(cctxs.size() - 1);
            }
            // -------------------------------------------------------------------------------------
            // check if ctx is needed as endpoint
            // increment running counter
            counter++;
            partitionOffset++;
         }

         for (auto *rContext : ForRouterMaprdmaCctx)
         {

            // partially initiallize connection connectxt
            ConnectionContextForRouterMap cctx;
            cctx.Received = (uint8_t *)cm.getGlobalBuffer().allocate(1, CACHE_LINE);
            *(cctx.Received) = 1;
            cctx.rctx = rContext;
            // -------------------------------------------------------------------------------------
            // fill init message
            initServer->mbOffset = (uintptr_t)cctx.Received;
            initServer->type = rdma::MESSAGE_TYPE::Init;
            // -------------------------------------------------------------------------------------
            cm.exchangeInitialMesssage(*(cctx.rctx), initServer);
            // -------------------------------------------------------------------------------------
            // finish initialization of cctx
            cctx.plOffset = (reinterpret_cast<InitMessage *>((cctx.rctx->applicationData)))->plOffset;
            cctx.mbOffset = (reinterpret_cast<InitMessage *>((cctx.rctx->applicationData)))->mbOffset;
            cctx.dbId = (reinterpret_cast<InitMessage *>((cctx.rctx->applicationData)))->bmId;
            cctxForRouterMap.push_back(cctx);
         }

         std::thread RouterMapSendThread([&]()
                                         {
            rdma::Message *outcoming;
            outcoming = (Message *)cm.getGlobalBuffer().allocate(rdma::SIZE_ROUTERMAPMESSAGE, CACHE_LINE);

            while(1){
            
            
            while(!router.metis){
               sleep(1);
               continue;
            }
            

            if(!router.DyPartitioner.has_send_metis){
               std::cout<<"send_customer"<<std::endl;   
               send_map_to_node(&router.DyPartitioner.customer_map,outcoming,MESSAGE_TYPE::RouterMap_metis_customer);
               std::cout<<"send_order"<<std::endl;   
               send_map_to_node(&router.DyPartitioner.order_map,outcoming,MESSAGE_TYPE::RouterMap_metis_order);
               std::cout<<"send_stock"<<std::endl;   
               send_map_to_node(&router.DyPartitioner.stock_map,outcoming,MESSAGE_TYPE::RouterMap_metis_stock);    
               std::cout<<"send_warehouse"<<std::endl;   
               send_map_to_node(&router.DyPartitioner.warehouse_map,outcoming,MESSAGE_TYPE::RouterMap_metis_warehouse);     
               std::cout<<"send_district"<<std::endl;   
               send_map_to_node(&router.DyPartitioner.district_map,outcoming,MESSAGE_TYPE::RouterMap_metis_district);    
               std::cout<<"send_neworder"<<std::endl;   
               send_map_to_node(&router.DyPartitioner.neworder_map,outcoming,MESSAGE_TYPE::RouterMap_metis_neworder);    
               // std::cout<<"send_orderline"<<std::endl;   
               // send_map_to_node(&router.DyPartitioner.orderline_map,outcoming,MESSAGE_TYPE::RouterMap_metis_orderline);                 
               router.DyPartitioner.has_send_metis=true;
               continue;
               //break;
            }
            if (!router.DyPartitioner.has_send_new_insert_keys)
            {
               std::cout << "send_dynamic" << std::endl;
               router.DyPartitioner.partition_mutex.lock();
               send_map_to_node(&router.DyPartitioner.customer_insert_keys, outcoming, MESSAGE_TYPE::RouterMap_dynamic_customer);
               send_map_to_node(&router.DyPartitioner.order_insert_keys, outcoming, MESSAGE_TYPE::RouterMap_dynamic_order);
               send_map_to_node(&router.DyPartitioner.stock_insert_keys, outcoming, MESSAGE_TYPE::RouterMap_dynamic_stock);
               send_map_to_node(&router.DyPartitioner.warehouse_insert_keys, outcoming, MESSAGE_TYPE::RouterMap_dynamic_warehouse);
               send_map_to_node(&router.DyPartitioner.district_insert_keys, outcoming, MESSAGE_TYPE::RouterMap_dynamic_district);
               send_map_to_node(&router.DyPartitioner.neworder_insert_keys, outcoming, MESSAGE_TYPE::RouterMap_dynamic_neworder);
               router.DyPartitioner.has_send_new_insert_keys = true;
               router.DyPartitioner.partition_mutex.unlock();
            }
            sleep(1);
            // send_map_to_node(&router.DyPartitioner.orderline_insert_keys, outcoming, MESSAGE_TYPE::RouterMap_dynamic_orderline);
            } });
         RouterMapSendThread.detach();

         last_router_statistics = std::chrono::high_resolution_clock::now();
         std::thread statistics([&]()
                                {
            uint64_t all_numbers=0;
            uint64_t graph_numbers=0;
            float all_count=0;
            uint64_t random_numbers=0;
            uint64_t last_all_numbers=0;
            sleep(10);
            while(true){
               auto start = std::chrono::high_resolution_clock::now();

               for (uint64_t i = 0; i < FLAGS_messageHandlerThreads; i++)
               {
                  all_numbers += router_number_per_thread[i];
                  graph_numbers += graph_number_per_thread[i];
                  random_numbers += random_number_per_thread[i];
                  graph_number_per_thread[i] = 0;
                  random_number_per_thread[i] = 0;
                  router_number_per_thread[i] = 0;
               }
               all_count += 10;
               router_logger.info(fmt::format("time:{}s; avg_throughput = {}/s; all_route_number= {}", all_count, (all_numbers - last_all_numbers) / float(10), all_numbers));
               router_logger.info(fmt::format("random_route_number:{}; graph_route_number:{}", random_numbers, graph_numbers));
               router_logger.info(fmt::format("transfer_count: {}", router.DyPartitioner.transfer_count));
               router_logger.info(fmt::format("transfer_node: {}", router.DyPartitioner.transfer_node.size()));
               // txn_logger.info(fmt::format("avg_route_time:{}/ms", all_numbers/FLAGS_messageHandlerThreads));
               last_all_numbers = all_numbers;

               auto end = std::chrono::high_resolution_clock::now();
               auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

               std::this_thread::sleep_for(std::chrono::seconds(10) - duration);
            } });
         statistics.detach();
         // -------------------------------------------------------------------------------------
      };
      // -------------------------------------------------------------------------------------
      MessageHandler::~MessageHandler()
      {
         stopThread();
      }
      // -------------------------------------------------------------------------------------
      // void MessageHandler::startThread()
      // {
      //    for (uint64_t t_i = 0; t_i < FLAGS_messageHandlerThreads; t_i++)
      //    {
      //       std::thread t([&, t_i]()
      //                     {
      //    // -------------------------------------------------------------------------------------
      //    // -------------------------------------------------------------------------------------
      //    threadCount++;
      //    // protect init only ont thread should do it;
      //    if (t_i == 0) {
      //       init();
      //       finishedInit = true;
      //    } else {
      //       while (!finishedInit)
      //          ;  // block until initialized
      //    }
      //    MailboxPartition& mbPartition = mbPartitions[t_i];
      //    uint8_t* mailboxes = mbPartition.mailboxes;
      //    const uint64_t beginId = mbPartition.beginId;
      //    uint64_t startPosition = 0;  // randomize messages
      //    uint64_t mailboxIdx = 0;

      //    //uint64_t destNodeId=0;
      //    auto start = std::chrono::high_resolution_clock::now();
      //    printf("start_receive\n");
      //    while (threadsRunning || connectedClients.load()) {
      //       for (uint64_t m_i = 0; m_i < mbPartition.numberMailboxes; m_i++, mailboxIdx++) {
      //          // -------------------------------------------------------------------------------------
      //          if (mailboxIdx >= mbPartition.numberMailboxes) mailboxIdx = 0;
      //          //sleep(1);
      //          if (mailboxes[mailboxIdx] == 0) continue;

      //          // -------------------------------------------------------------------------------------
      //          //mailboxes[mailboxIdx] = 0;  // reset mailbox before response is sent
      //          // -------------------------------------------------------------------------------------
      //          // handle message
      //          uint64_t clientId = mailboxIdx + beginId;  // correct for partiton
      //          auto& ctx = cctxs[clientId];

      //          switch (ctx.incoming->type) {
      //             case MESSAGE_TYPE::SQL: {
      //                auto& sqlMessage = *reinterpret_cast<SqlMessage*>(ctx.incoming);
      //                char sql[sqlLength]=" ";
      //                std::strcpy(sql,sqlMessage.sql);
      //                std::string str=sql;
      //                //printf("%s\n",sqlMessage.sql);

      //                //按wid返回
      //                // uint64_t destNodeId;
      //                // if(routerCach[clientId]==0){
      //                //    std::vector<std::string> args = extractParameters(str, ',');
      //                //    int w_id = std::stoi(args[0]);
      //                //    int step = FLAGS_tpcc_warehouse_count/FLAGS_nodes;
      //                //    destNodeId = (w_id-1)/step;
      //                //    // if(count <180000){
      //                //    //    destNodeId = rand()%FLAGS_nodes;
      //                //    // }
      //                // }else{
      //                //    destNodeId=routerCach[clientId]-1;
      //                // }
      //                //按wid反转
      //                // uint64_t destNodeId;
      //                // if(routerCach[clientId]==0){
      //                //    std::vector<std::string> args = extractParameters(str, ',');
      //                //    int w_id = std::stoi(args[0]);
      //                //    int step = FLAGS_tpcc_warehouse_count/FLAGS_nodes;
      //                //    destNodeId = (w_id-1)/step;
      //                //    destNodeId = FLAGS_nodes-1-destNodeId;
      //                //    // if(count <180000){
      //                //    //    destNodeId = rand()%FLAGS_nodes;
      //                //    // }
      //                // }else{
      //                //    destNodeId=routerCach[clientId]-1;
      //                // }

      //                //原路返回
      //                //uint64_t destNodeId=sqlMessage.nodeId;

      //                //随机返回
      //                //uint64_t destNodeId=rand()%FLAGS_nodes;

      //                // 随机返回但是只随机一次
      //                // uint64_t destNodeId;
      //                // if(routerCach[clientId]==0){
      //                //    destNodeId=rand()%FLAGS_nodes;
      //                // }else{
      //                //    destNodeId=routerCach[clientId]-1;
      //                // }
      //                uint64_t destNodeId;
      //                switch (FLAGS_route_mode)
      //                {
      //                case 1:
      //                    //随机路由
      //                    if (routerCach[clientId] == 0)
      //                    {
      //                       bool ishash = false;
      //                       auto router_start = std::chrono::high_resolution_clock::now();
      //                       std::vector<router::TxnNode> txnnodelist;
      //                       gen_txn_key_list_with_weight(txnnodelist, str, destNodeId, int(FLAGS_nodes), ishash);
      //                       bool isroute = false;
      //                       if (FLAGS_routertimeLog)
      //                          routertime_logger->info(fmt::format("received,{},{}", sqlMessage.nodeId, extractTxnId(str)));
      //                       destNodeId = router.router(txnnodelist, t_i, false, isroute);
      //                       auto router_end = std::chrono::high_resolution_clock::now();
      //                       router_time_per_thread[t_i] += (1.0 * std::chrono::duration_cast<std::chrono::microseconds>(router_end - router_start).count() / 1000);
      //                       random_number_per_thread[destNodeId]++;
      //                       // if(count > 180000){
      //                       // std::vector<std::string> args = extractParameters(str, ',');
      //                       // if(std::stoi(args[0])<int(FLAGS_tpcc_warehouse_count/2+1)) destNodeId=0;
      //                       // else destNodeId=1;
      //                       // }
      //                    }
      //                    else
      //                    {
      //                       destNodeId = routerCach[clientId] - 1;
      //                    }
      //                   break;
      //                case 2:
      //                   // 按wid取模

      //                   if (routerCach[clientId] == 0)
      //                   {
      //                      std::vector<std::string> args = extractParameters(str, ',');
      //                      int w_id = std::stoi(args[0]);
      //                      destNodeId = w_id % FLAGS_nodes;
      //                      // if(count <180000){
      //                      //    destNodeId = rand()%FLAGS_nodes;
      //                      // }
      //                   }
      //                   else
      //                   {
      //                      destNodeId = routerCach[clientId] - 1;
      //                   }
      //                   break;
      //                default:
      //                //有路由
      //                if(routerCach[clientId]==0){
      //                   bool ishash = false;
      //                   auto router_start = std::chrono::high_resolution_clock::now();
      //                   std::vector<router::TxnNode> txnnodelist;
      //                   gen_txn_key_list_with_weight(txnnodelist, str, destNodeId, int(FLAGS_nodes), ishash);
      //                   bool isroute = false;
      //                   if(FLAGS_routertimeLog) routertime_logger->info(fmt::format("received,{},{}", sqlMessage.nodeId,extractTxnId(str)));
      //                   destNodeId=router.router(txnnodelist, t_i, true, isroute);
      //                   auto router_end = std::chrono::high_resolution_clock::now();
      //                   router_time_per_thread[t_i] += (1.0 * std::chrono::duration_cast<std::chrono::microseconds>(router_end - router_start).count() / 1000);
      //                   if(isroute)graph_number_per_thread[destNodeId]++;
      //                   else random_number_per_thread[destNodeId]++;
      //                }else{
      //                   destNodeId=routerCach[clientId]-1;
      //                }
      //                   break;
      //                }

      //                auto& sqlMessagetoDispatch = *MessageFabric::createMessage<SqlMessage>(ctx.outcoming,MESSAGE_TYPE::SQL,sql,sqlMessage.nodeId);
      //                //printf("try send:%s from node%ld to node%ld\n",sqlMessagetoDispatch.sql,sqlMessagetoDispatch.nodeId,destNodeId);
      //                bool isDispatch=dispatcherSql(destNodeId, sqlMessagetoDispatch,clientId,&mailboxes[mailboxIdx]);
      //                if(isDispatch) {
      //                   routerCach[clientId]=0;
      //                   std::vector<std::string> args = extractParameters(str, ',');
      //                   //router_logger.info(fmt::format("{},{}", destNodeId, (std::stoi(args[0])-1)/10));
      //                   router_number_per_thread[destNodeId] += 1;
      //                   if(FLAGS_routertimeLog) routertime_logger->info(fmt::format("send,{},{}", sqlMessage.nodeId,extractTxnId(sql)));
      //                   //auto end = std::chrono::high_resolution_clock::now();
      //                   //proxy_time_per_thread[t_i] += (1.0 * std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000);
      //                   // auto end = std::chrono::high_resolution_clock::now();
      //                   // proxy_time_per_thread[t_i] += (1.0 * std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000);
      //                   //printf("success\n");
      //                }
      //                else{
      //                   //router_logger.info("false");
      //                   //printf("defeat%d %ld\n",*(cctxs[cctxsmap[destNodeId]].mbforBusy),destNodeId);
      //                   routerCach[clientId]=destNodeId+1;
      //                   //auto end = std::chrono::high_resolution_clock::now();
      //                   //proxy_time_per_thread[t_i] += (1.0 * std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000);
      //                   //router_number_per_thread[t_i]->fetch_add(1, std::memory_order_relaxed);
      //                   // auto end = std::chrono::high_resolution_clock::now();
      //                   // proxy_time_per_thread[t_i] += (1.0 * std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000);
      //                }
      //                //start = std::chrono::high_resolution_clock::now();
      //                break;
      //             }
      //             case MESSAGE_TYPE::TxnKeys: {
      //                //有路由
      //                // auto& txnKeysMessage = *reinterpret_cast<TxnKeysMessage*>(ctx.incoming);
      //                // size_t len = txnKeysMessage.txnkeysLength;

      //                // int txn_keys[len];
      //                // memcpy(txn_keys, txnKeysMessage.txnkeys, len * sizeof(int));
      //                // std::vector<int> txnnodelist;

      //                // for(size_t i = 0; i < len; ++i) {
      //                //    txnnodelist.push_back(txn_keys[i]);
      //                // }
      //                // router.router(txnnodelist,false);

      //                //-------------------------------------------------------------
      //                mailboxes[mailboxIdx] = 0;
      //                printf("error\n");
      //                uint8_t flag = 0;
      //                auto& wqe_dest = ctx.wqe_flag;
      //                uint64_t SIGNAL_ = FLAGS_pollingInterval -1;
      //                rdma::completion signal = ((wqe_dest & SIGNAL_) == 0) ? rdma::completion::signaled : rdma::completion::unsignaled;
      //                rdma::postWrite(&flag, *(ctx.rctx),signal ,ctx.mbOffset);
      //                if ((wqe_dest & SIGNAL_)==SIGNAL_) {

      //                   int comp{0};
      //                   ibv_wc wcReturn;
      //                   while (comp == 0) {
      //                      comp = rdma::pollCompletion(ctx.rctx->id->qp->send_cq, 1, &wcReturn);
      //                   }
      //                   if (wcReturn.status != IBV_WC_SUCCESS){
      //                      printf("error%d\n",wcReturn.status);
      //                      throw;
      //                   }
      //                }
      //                wqe_dest++;
      //                auto end = std::chrono::high_resolution_clock::now();
      //                proxy_time_per_thread[t_i] += (1.0 * std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() / 1000);

      //                break;
      //             }
      //             default:
      //                throw std::runtime_error("Unexpected Message in MB " + std::to_string(mailboxIdx) + " type " +
      //                                         std::to_string((size_t)ctx.incoming->type));
      //          }
      //       }
      //       mailboxIdx = ++startPosition;

      //    }
      //    threadCount--; });

      //       // threads::CoreManager::getInstance().pinThreadToCore(t.native_handle());
      //       // if ((t_i % 2) == 0)
      //       threads::CoreManager::getInstance().pinThreadToCore(t.native_handle());
      //       // else
      //       //    threads::CoreManager::getInstance().pinThreadToHT(t.native_handle());
      //       t.detach();
      //    }
      // }
      void MessageHandler::startThread()
      {
         for (uint64_t t_i = 0; t_i < FLAGS_messageHandlerThreads; t_i++)
         {
            std::thread t([&, t_i]()
                          {
                           u64 part_count = 0;
                           volatile u64 tx_acc = 0;
         // -------------------------------------------------------------------------------------
         // ------------------------------------------------------------------------------------- 
         threadCount++;
         // protect init only ont thread should do it;
         if (t_i == 0) {
            init();
            finishedInit = true;
         } else {
            while (!finishedInit)
               ;  // block until initialized
         }
         MailboxPartition& mbPartition = mbPartitions[t_i];
         uint8_t* mailboxes = mbPartition.mailboxes;
         const uint64_t beginId = mbPartition.beginId;
         uint64_t startPosition = 0;  // randomize messages
         uint64_t mailboxIdx = 0;
         auto w_id = urand(1, FLAGS_tpcc_warehouse_count);
         std::string sql = txCreate(w_id);

         //uint64_t destNodeId=0;
         while (threadsRunning) {
            for (uint64_t m_i = 0; m_i < mbPartition.numberMailboxes; m_i++, mailboxIdx++) {
               // -------------------------------------------------------------------------------------
               if (mailboxIdx >= mbPartition.numberMailboxes)
                  mailboxIdx = 0;
               // -------------------------------------------------------------------------------------
               // mailboxes[mailboxIdx] = 0;  // reset mailbox before response is sent
               // -------------------------------------------------------------------------------------
               // handle message
               uint64_t clientId = mailboxIdx + beginId; // correct for partiton
               auto &ctx = cctxs[clientId];
               // printf("%s\n",sqlMessage.sql);

               // 按wid返回
               //  uint64_t destNodeId;
               //  if(routerCach[clientId]==0){
               //     std::vector<std::string> args = extractParameters(str, ',');
               //     int w_id = std::stoi(args[0]);
               //     int step = FLAGS_tpcc_warehouse_count/FLAGS_nodes;
               //     destNodeId = (w_id-1)/step;
               //     // if(count <180000){
               //     //    destNodeId = rand()%FLAGS_nodes;
               //     // }
               //  }else{
               //     destNodeId=routerCach[clientId]-1;
               //  }
               // 按wid反转
               //  uint64_t destNodeId;
               //  if(routerCach[clientId]==0){
               //     std::vector<std::string> args = extractParameters(str, ',');
               //     int w_id = std::stoi(args[0]);
               //     int step = FLAGS_tpcc_warehouse_count/FLAGS_nodes;
               //     destNodeId = (w_id-1)/step;
               //     destNodeId = FLAGS_nodes-1-destNodeId;
               //     // if(count <180000){
               //     //    destNodeId = rand()%FLAGS_nodes;
               //     // }
               //  }else{
               //     destNodeId=routerCach[clientId]-1;
               //  }

               // 原路返回
               // uint64_t destNodeId=sqlMessage.nodeId;

               // 随机返回
               // uint64_t destNodeId=rand()%FLAGS_nodes;

               // 随机返回但是只随机一次
               // uint64_t destNodeId;
               // if(routerCach[clientId]==0){
               //    destNodeId=rand()%FLAGS_nodes;
               // }else{
               //    destNodeId=routerCach[clientId]-1;
               // }
               uint64_t destNodeId;
               switch (FLAGS_route_mode)
               {
               case 1:
                  // 随机路由
                  if (routerCach[clientId] == 0)
                  {
                     bool ishash = false;
                     auto router_start = Proxy::utils::getTimePoint();
                     std::vector<router::TxnNode> txnnodelist;
                     gen_txn_key_list_with_weight(txnnodelist, sql, destNodeId, int(FLAGS_nodes), ishash);
                     bool isroute = false;
                     destNodeId = router.router(txnnodelist, t_i, false, isroute);
                     auto router_end = Proxy::utils::getTimePoint();
                     outputs[t_i] << (router_end - router_start) << " ";
                     tx_acc++;
                     if(tx_acc > 10000){
                        outputs[t_i] << std::endl;
                        outputs[t_i].flush();
                        tx_acc = 0;
                     }
                     // if(count > 180000){
                     // std::vector<std::string> args = extractParameters(str, ',');
                     // if(std::stoi(args[0])<int(FLAGS_tpcc_warehouse_count/2+1)) destNodeId=0;
                     // else destNodeId=1;
                     // }
                  }
                  else
                  {
                     destNodeId = routerCach[clientId] - 1;
                  }
                  break;
               case 2:
                  // 按wid取模

                  if (routerCach[clientId] == 0)
                  {
                     std::vector<std::string> args = extractParameters(sql, ',');
                     int w_id = std::stoi(args[0]);
                     destNodeId = w_id % FLAGS_nodes;
                     // if(count <180000){
                     //    destNodeId = rand()%FLAGS_nodes;
                     // }
                  }
                  else
                  {
                     destNodeId = routerCach[clientId] - 1;
                  }
                  break;
               case 3:
                  // 有路由
                  if (routerCach[clientId] == 0)
                  {
                     bool ishash = false;
                     auto router_start = Proxy::utils::getTimePoint();
                     std::vector<router::TxnNode> txnnodelist;
                     gen_txn_key_list_with_weight(txnnodelist, sql, destNodeId, int(FLAGS_nodes), ishash);
                     bool isroute = false;
                     destNodeId = router.router(txnnodelist, t_i, true, isroute);
                     auto router_end = Proxy::utils::getTimePoint();
                     if (router.metis)
                     {
                        outputs[t_i] << (router_end - router_start) << " ";
                        tx_acc++;
                     }
                     if(tx_acc > 10000){
                        outputs[t_i] << std::endl;
                        outputs[t_i].flush();
                        tx_acc = 0;
                     }
                     if (isroute)
                        graph_number_per_thread[destNodeId]++;
                     else
                        random_number_per_thread[destNodeId]++;
                  }
                  else
                  {
                     destNodeId = routerCach[clientId] - 1;
                  }
                  break;
                                    case 4:
                  // schism
                  if (routerCach[clientId] == 0)
                  {
                     bool ishash = false;
                     auto router_start = Proxy::utils::getTimePoint();
                     std::vector<router::TxnNode> txnnodelist;
                     gen_txn_key_list_with_weight(txnnodelist, sql, destNodeId, int(FLAGS_nodes), ishash);
                     bool isroute = false;
                     destNodeId = router.router(txnnodelist, t_i, true, isroute);
                     auto router_end = Proxy::utils::getTimePoint();
                     if (router.metis)
                     {
                        outputs[t_i] << (router_end - router_start) << " ";
                        tx_acc++;
                     }
                     if(tx_acc > 10000){
                        outputs[t_i] << std::endl;
                        outputs[t_i].flush();
                        tx_acc = 0;
                     }
                     if (isroute)
                        graph_number_per_thread[destNodeId]++;
                     else
                        random_number_per_thread[destNodeId]++;
                  }
                  else
                  {
                     destNodeId = routerCach[clientId] - 1;
                  }
                  break;
                  case 5:
                  // hash
                  if (routerCach[clientId] == 0)
                  {
                     bool ishash = false;
                     auto router_start = Proxy::utils::getTimePoint();
                     std::vector<router::TxnNode> txnnodelist;
                     gen_txn_key_list_with_weight(txnnodelist, sql, destNodeId, int(FLAGS_nodes), ishash);
                     destNodeId = router.hash_router(txnnodelist, t_i);
                     auto router_end = Proxy::utils::getTimePoint();
                     outputs[t_i] << (router_end - router_start) << " ";
                     tx_acc++;
                     if(tx_acc > 10000){
                        outputs[t_i] << std::endl;
                        outputs[t_i].flush();
                        tx_acc = 0;
                     }
                  }
                  else
                  {
                     destNodeId = routerCach[clientId] - 1;
                  }
                  break;
                  case 6:
                  // ideal rapo
                  if (routerCach[clientId] == 0)
                  {
                     std::vector<std::string> args = extractParameters(sql, ',');
                     bool ishash = false;
                     auto router_start = Proxy::utils::getTimePoint();
                     std::vector<router::TxnNode> txnnodelist;
                     gen_txn_key_list_with_weight(txnnodelist, sql, destNodeId, int(FLAGS_nodes), ishash);
                     destNodeId = router.hash_router(txnnodelist, t_i);
                     int w_id = std::stoi(args[0]);
                     destNodeId = w_id % FLAGS_nodes;
                     auto router_end = Proxy::utils::getTimePoint();
                     outputs[t_i] << (router_end - router_start) << " ";
                     tx_acc++;
                     if (t_i == 0)
                     {
                        part_count++;
                        if (part_count <= 100000)
                        {
                           for (const auto &txn_node : txnnodelist)
                           {
                              router.DyPartitioner.partmap.insert({txn_node.key, destNodeId});
                              if (txn_node.key <= customer_key_max)
                              {
                                 router.DyPartitioner.customer_map.insert({txn_node.key, destNodeId});
                              }
                              else if (customer_key_max < txn_node.key && txn_node.key <= order_key_max)
                              {
                                 router.DyPartitioner.order_map.insert({txn_node.key - customer_key_max, destNodeId});
                              }
                              else if (order_key_max < txn_node.key && txn_node.key <= stock_key_max)
                              {
                                 router.DyPartitioner.stock_map.insert({txn_node.key - order_key_max, destNodeId});
                              }
                              else if (stock_key_max < txn_node.key && txn_node.key <= warehouse_key_max)
                              {
                                 router.DyPartitioner.warehouse_map.insert({(txn_node.key - stock_key_max) / FLAGS_stamp_len, destNodeId});
                              }
                              else if (warehouse_key_max < txn_node.key && txn_node.key <= district_key_max)
                              {
                                 router.DyPartitioner.district_map.insert({txn_node.key - warehouse_key_max, destNodeId});
                              }
                              else
                              {
                                 router.DyPartitioner.neworder_map.insert({txn_node.key - district_key_max, destNodeId});
                              }
                           }
                        }
                        else if (!router.metis)
                        {
                           router.metis = true;
                        }
                     }
                     if(tx_acc % 10000 == 0){
                        outputs[t_i] << std::endl;
                        outputs[t_i].flush();
                     }
                  break;
                  }
                  else
                  {
                     destNodeId = routerCach[clientId] - 1;
                  }
                  break;
               default:
               // 按wid返回
                if(routerCach[clientId]==0){
                   std::vector<std::string> args = extractParameters(sql, ',');
                   int w_id = std::stoi(args[0]);
                   int step = FLAGS_tpcc_warehouse_count/FLAGS_nodes;
                   destNodeId = (w_id-1)/step;
                   // if(count <180000){
                   //    destNodeId = rand()%FLAGS_nodes;
                   // }
                }else{
                   destNodeId=routerCach[clientId]-1;
                }

                  break;
               }
               auto &sqlMessagetoDispatch = *MessageFabric::createMessage<SqlMessage>(ctx.outcoming, MESSAGE_TYPE::SQL, sql.data(), clientId);
               bool isDispatch = dispatcherSql(destNodeId, sqlMessagetoDispatch, clientId, &mailboxes[mailboxIdx]);
               while (!isDispatch)
               {
                  routerCach[clientId] = 0;
                  isDispatch = dispatcherSql(destNodeId, sqlMessagetoDispatch, clientId, &mailboxes[mailboxIdx]);
               }
               router_number_per_thread[destNodeId] += 1;
               w_id = urand(1, FLAGS_tpcc_warehouse_count);
               sql = txCreate(w_id);
            }
            mailboxIdx = ++startPosition;
         }
         threadCount--; });

            // threads::CoreManager::getInstance().pinThreadToCore(t.native_handle());
            // if ((t_i % 2) == 0)
            threads::CoreManager::getInstance().pinThreadToCore(t.native_handle());
            // else
            // threads::CoreManager::getInstance().pinThreadToHT(t.native_handle());
            t.detach();
         }
         sleep(FLAGS_TPCC_run_for_seconds + 10);
         threadsRunning = false;
      }
      // -------------------------------------------------------------------------------------
      void MessageHandler::stopThread()
      {
         threadsRunning = false;
         while (threadCount)
            ; // wait
         isfinished = true;
      };

   } // namespace rdma
} // namespace Proxy
