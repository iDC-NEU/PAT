#include "MessageHandler.hpp"
#include "Defs.hpp"
#include "scalestore/storage/buffermanager/Page.hpp"
#include "scalestore/threads/CoreManager.hpp"
#include "scalestore/threads/ThreadContext.hpp"
// -------------------------------------------------------------------------------------
#include <numeric>

namespace scalestore
{
   namespace rdma
   {
      MessageHandler::MessageHandler(rdma::CM<InitMessage> &cm, storage::Buffermanager &bm, NodeID nodeId)
          : cm(cm), bm(bm), nodeId(nodeId), mbPartitions(FLAGS_messageHandlerThreads), proxycctxs(FLAGS_sqlSendThreads)
      {
         // partition mailboxes
         isConnectProxy = false;
         if (FLAGS_use_proxy)
         {
            std::thread t([&]()
                          {
            connectProxy();
            isConnectProxy=true; });
            t.detach();
         }
         size_t n = (FLAGS_worker) * (FLAGS_nodes - 1);
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
               mbPartitions[t_i].inflightCRs.resize(end - begin);
            }
            for (uint64_t n_i = 0; n_i < FLAGS_sqlSendThreads; n_i++)
            {
               mail_mtxs.push_back(new std::mutex());
            }
            startThread();
         }
      }
      // -------------------------------------------------------------------------------------
      void MessageHandler::init()
      {
         InitMessage *initServer = (InitMessage *)cm.getGlobalBuffer().allocate(sizeof(InitMessage));
         // -------------------------------------------------------------------------------------
         size_t numConnections = (FLAGS_worker) * (FLAGS_nodes - 1);
         connectedClients = numConnections;
         while (cm.getNumberIncomingConnections() != (numConnections))
            ; // block until client is connected
         // -------------------------------------------------------------------------------------
         std::cout << "Number connections " << numConnections << std::endl;
         // wait until all workers are connected
         std::vector<RdmaContext *> rdmaCtxs; // get cm ids of incomming

         while (true)
         {
            std::vector<RdmaContext *> tmp_rdmaCtxs(cm.getIncomingConnections()); // get cm ids of incomming
            uint64_t workers = 0;
            for (auto *rContext : tmp_rdmaCtxs)
            {
               if (rContext->type != Type::WORKER)
                  continue;
               workers++;
            }
            if (workers == numConnections)
            {
               rdmaCtxs = tmp_rdmaCtxs;
               break;
            }
         }
         // -------------------------------------------------------------------------------------
         // shuffle worker connections
         // -------------------------------------------------------------------------------------
         auto rng = std::default_random_engine{};
         std::shuffle(std::begin(rdmaCtxs), std::end(rdmaCtxs), rng);

         uint64_t counter = 0;
         uint64_t partitionId = 0;
         uint64_t partitionOffset = 0;

         for (auto *rContext : rdmaCtxs)
         {
            // -------------------------------------------------------------------------------------
            if (rContext->type != Type::WORKER)
            {
               continue; // skip no worker connection
            }

            // partially initiallize connection connectxt
            ConnectionContext cctx;
            cctx.request = (Message *)cm.getGlobalBuffer().allocate(rdma::LARGEST_MESSAGE, CACHE_LINE);
            cctx.response = (Message *)cm.getGlobalBuffer().allocate(rdma::LARGEST_MESSAGE, CACHE_LINE);
            cctx.rctx = rContext;
            cctx.activeInvalidationBatch = new InvalidationBatch();
            cctx.passiveInvalidationBatch = new InvalidationBatch();
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
            initServer->plOffset = (uintptr_t)cctx.request;
            initServer->bmId = nodeId;
            initServer->type = rdma::MESSAGE_TYPE::Init;
            // -------------------------------------------------------------------------------------
            cm.exchangeInitialMesssage(*(cctx.rctx), initServer);
            // -------------------------------------------------------------------------------------
            // finish initialization of cctx
            cctx.plOffset = (reinterpret_cast<InitMessage *>((cctx.rctx->applicationData)))->plOffset;
            cctx.bmId = (reinterpret_cast<InitMessage *>((cctx.rctx->applicationData)))->bmId;
            // -------------------------------------------------------------------------------------
            cctx.remoteMbOffsets.resize(FLAGS_nodes);
            cctx.remotePlOffsets.resize(FLAGS_nodes);
            // -------------------------------------------------------------------------------------
            cctxs.push_back(cctx);
            // -------------------------------------------------------------------------------------
            // check if ctx is needed as endpoint
            // increment running counter
            counter++;
            partitionOffset++;
         }

         ensure(counter == numConnections);
         // -------------------------------------------------------------------------------------
      }
      // -------------------------------------------------------------------------------------
      MessageHandler::~MessageHandler()
      {
         stopThread();
      }
      // -------------------------------------------------------------------------------------
      void MessageHandler::startThread()
      {
         for (uint64_t t_i = 0; t_i < FLAGS_messageHandlerThreads; t_i++)
         {
            std::thread t([&, t_i]()
                          {
         // -------------------------------------------------------------------------------------
         std::unique_ptr<threads::ThreadContext> threadContext = std::make_unique<threads::ThreadContext>();
         threads::ThreadContext::tlsPtr = threadContext.get();  // init tl ptr
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
         profiling::WorkerCounters counters;  // create counters
         storage::AsyncReadBuffer async_read_buffer(bm.ssd_fd, PAGE_SIZE, 256);
         // for delegation purpose, i.e., communication to remote message handler
         std::vector<MHEndpoint> mhEndpoints(FLAGS_nodes);
         for (uint64_t n_i = 0; n_i < FLAGS_nodes; n_i++) {
            if (n_i == nodeId) continue;
            auto& ip = NODES[FLAGS_nodes][n_i];
            auto& desport = NODESport[FLAGS_nodes][n_i];
            mhEndpoints[n_i].rctx = &(cm.initiateConnection(ip, rdma::Type::MESSAGE_HANDLER, 99, nodeId,desport));
         }
         // handle

         std::vector<uint64_t> latencies(mbPartition.numberMailboxes);

         while (threadsRunning || connectedClients.load()) {
            for (uint64_t m_i = 0; m_i < mbPartition.numberMailboxes; m_i++, mailboxIdx++) {
               // -------------------------------------------------------------------------------------
               if (mailboxIdx >= mbPartition.numberMailboxes) mailboxIdx = 0;

               if (mailboxes[mailboxIdx] == 0) continue;
               // -------------------------------------------------------------------------------------
               mailboxes[mailboxIdx] = 0;  // reset mailbox before response is sent
               // -------------------------------------------------------------------------------------
               // handle message
               uint64_t clientId = mailboxIdx + beginId;  // correct for partiton
               auto& ctx = cctxs[clientId];
               // reset infligh copy requests if needed
               if (mbPartition.inflightCRs[m_i].inflight) {  // only we modify this entry, however there could be readers
                  std::unique_lock<std::mutex> ulquard(mbPartition.inflightCRMutex);
                  mbPartition.inflightCRs[m_i].inflight = false;
                  mbPartition.inflightCRs[m_i].pid = EMPTY_PID;
                  mbPartition.inflightCRs[m_i].pVersion = 0;
               }

               switch (ctx.request->type) {
                  case MESSAGE_TYPE::Finish: {
                     connectedClients--;
                     break;
                  }
                  case MESSAGE_TYPE::DR: {
                     auto& request = *reinterpret_cast<DelegationRequest*>(ctx.request);
                     ctx.remoteMbOffsets[request.bmId] = request.mbOffset;
                     ctx.remotePlOffsets[request.bmId] = request.mbPayload;
                     auto& response = *MessageFabric::createMessage<DelegationResponse>(ctx.response);
                     writeMsg(clientId, response, threads::ThreadContext::my().page_handle);
                     break;
                  }
                  case MESSAGE_TYPE::PRX: {
                     auto& request = *reinterpret_cast<PossessionRequest*>(ctx.request);
                     handlePossessionRequest<POSSESSION::EXCLUSIVE>(mbPartition, request, ctx, clientId, mailboxIdx, counters,
                                                                    async_read_buffer, threads::ThreadContext::my().page_handle);
                     break;
                  }
                  case MESSAGE_TYPE::PRS: {
                     auto& request = *reinterpret_cast<PossessionRequest*>(ctx.request);
                     handlePossessionRequest<POSSESSION::SHARED>(mbPartition, request, ctx, clientId, mailboxIdx, counters,
                                                                 async_read_buffer, threads::ThreadContext::my().page_handle);
                     break;
                  }
                  case MESSAGE_TYPE::PMR: {
                     auto& request = *reinterpret_cast<PossessionMoveRequest*>(ctx.request);
                     // we are not owner therefore we transfer the page or notify if possession removed
                     auto guard = bm.findFrame<CONTENTION_METHOD::NON_BLOCKING>(PID(request.pid), Invalidation(), ctx.bmId);
                     // -------------------------------------------------------------------------------------
                     if (guard.state == STATE::RETRY) {
                        ensure(guard.latchState != LATCH_STATE::EXCLUSIVE);
                        if (!request.needPage)  // otherwise we need to let the request complete
                           guard.frame->mhWaiting = true;
                        mailboxes[mailboxIdx] = 1;
                        counters.incr(profiling::WorkerCounters::mh_msgs_restarted);
                        continue;
                     }
                     // -------------------------------------------------------------------------------------
                     ensure(guard.state != STATE::NOT_FOUND);
                     ensure(guard.state != STATE::UNINITIALIZED);
                     ensure(guard.frame);
                     ensure(request.pid == guard.frame->pid);
                     ensure(guard.frame->page != nullptr);
                     ensure(guard.frame->latch.isLatched());
                     ensure(guard.latchState == LATCH_STATE::EXCLUSIVE);
                     // -------------------------------------------------------------------------------------
                     auto& response = *MessageFabric::createMessage<PossessionMoveResponse>(ctx.response, RESULT::WithPage);
                     // Respond
                     // -------------------------------------------------------------------------------------
                     if (request.needPage) {
                        writePageAndMsg(clientId, guard.frame->page, request.pageOffset, response,
                                        threads::ThreadContext::my().page_handle);
                        counters.incr(profiling::WorkerCounters::rdma_pages_tx);
                     } else {
                        response.resultType = RESULT::NoPage;
                        writeMsg(clientId, response, threads::ThreadContext::my().page_handle);
                     }
                     // -------------------------------------------------------------------------------------
                     // Invalidate Page
                     // -------------------------------------------------------------------------------------
                     bm.removeFrame(*guard.frame,[&](BufferFrame& frame){
                                                    ctx.activeInvalidationBatch->add(frame.page); 
                                                 });
                     // -------------------------------------------------------------------------------------
                     break;
                  }
                  case MESSAGE_TYPE::PCR: {
                     auto& request = *reinterpret_cast<PossessionCopyRequest*>(ctx.request);
                     auto guard = bm.findFrame<CONTENTION_METHOD::NON_BLOCKING>(PID(request.pid), Copy(), ctx.bmId);
                     auto& response = *MessageFabric::createMessage<rdma::PossessionCopyResponse>(ctx.response, RESULT::WithPage);
                     // -------------------------------------------------------------------------------------
                     // entry already invalidated by someone
                     if ((guard.state == STATE::UNINITIALIZED || guard.state == STATE::NOT_FOUND)) {
                        response.resultType = RESULT::CopyFailedWithInvalidation;
                        writeMsg(clientId, response, threads::ThreadContext::my().page_handle);
                        ctx.retries = 0;
                        counters.incr(profiling::WorkerCounters::mh_msgs_restarted);
                        continue;
                     }
                     // -------------------------------------------------------------------------------------
                     // found entry but could not latch check max restart
                     if (guard.state == STATE::RETRY) {
                        if (guard.frame->pVersion == request.pVersion && ctx.retries < FLAGS_messageHandlerMaxRetries) { // is this abort needed? to check if mh_waiting? 
                           ctx.retries++;
                           mailboxes[mailboxIdx] = 1;
                           counters.incr(profiling::WorkerCounters::mh_msgs_restarted);
                           continue;
                        }
                        response.resultType = RESULT::CopyFailedWithRestart;
                        writeMsg(clientId, response, threads::ThreadContext::my().page_handle);
                        ctx.retries = 0;
                        counters.incr(profiling::WorkerCounters::mh_msgs_restarted);
                        continue;
                     }
                     // -------------------------------------------------------------------------------------
                     // potential deadlock, restart and release latches
                     if (guard.frame->mhWaiting && guard.frame->state != BF_STATE::HOT) {
                        ensure((guard.frame->state == BF_STATE::IO_RDMA) | (guard.frame->state == BF_STATE::FREE));
                        response.resultType = RESULT::CopyFailedWithRestart;
                        writeMsg(clientId, response, threads::ThreadContext::my().page_handle);
                        guard.frame->latch.unlatchShared();
                        ctx.retries = 0;
                        counters.incr(profiling::WorkerCounters::mh_msgs_restarted);
                        continue;
                     }
                     // -------------------------------------------------------------------------------------
                     ensure(guard.frame->possession == POSSESSION::SHARED);
                     ensure(request.pid == guard.frame->pid);
                     ensure(guard.frame->pVersion == request.pVersion);
                     ensure(guard.frame != nullptr);
                     ensure(guard.frame->page != nullptr);
                     // -------------------------------------------------------------------------------------
                     // write back pages
                     writePageAndMsg(clientId, guard.frame->page, request.pageOffset, response, threads::ThreadContext::my().page_handle);
                     counters.incr(profiling::WorkerCounters::rdma_pages_tx);
                     guard.frame->latch.unlatchShared();
                     ctx.retries = 0;

                     break;
                  }
                  case MESSAGE_TYPE::PUR: {
                     auto& request = *reinterpret_cast<PossessionUpdateRequest*>(ctx.request);
                     auto guard = bm.findFrame<CONTENTION_METHOD::NON_BLOCKING>(request.pid, Invalidation(), ctx.bmId);
                     auto& response = *MessageFabric::createMessage<rdma::PossessionUpdateResponse>(ctx.response, RESULT::UpdateSucceed);
                     // -------------------------------------------------------------------------------------
                     if ((guard.state == STATE::RETRY) && (guard.frame->pVersion == request.pVersion)) {
                        guard.frame->mhWaiting = true;
                        ensure(guard.latchState == LATCH_STATE::UNLATCHED);
                        mailboxes[mailboxIdx] = 1;
                        counters.incr(profiling::WorkerCounters::mh_msgs_restarted);
                        continue;
                     }

                     if ((guard.frame->pVersion > request.pVersion)) {
                        response.resultType = rdma::RESULT::UpdateFailed;
                        writeMsg(clientId, response, threads::ThreadContext::my().page_handle);
                        guard.frame->mhWaiting = false;
                        if (guard.latchState == LATCH_STATE::EXCLUSIVE) guard.frame->latch.unlatchExclusive();
                        counters.incr(profiling::WorkerCounters::mh_msgs_restarted);
                        continue;
                     }

                     ensure(guard.state != STATE::UNINITIALIZED);
                     ensure(guard.state != STATE::NOT_FOUND);
                     ensure(guard.state != STATE::RETRY);
                     ensure(request.pid == guard.frame->pid);
                     ensure(guard.frame->pVersion == request.pVersion);
                     // -------------------------------------------------------------------------------------
                     ensure(guard.frame->latch.isLatched());
                     ensure(guard.latchState == LATCH_STATE::EXCLUSIVE);
                     ensure((guard.frame->possessors.shared.test(ctx.bmId)));
                     // -------------------------------------------------------------------------------------
                     // Update states
                     // -------------- -----------------------------------------------------------------------
                     guard.frame->pVersion++;
                     response.pVersion = guard.frame->pVersion;
                     guard.frame->possessors.shared.reset(nodeId);  // reset own node id already
                     // test if there are more shared possessors
                     if (guard.frame->possessors.shared.any()) {
                        response.resultType = RESULT::UpdateSucceedWithSharedConflict;
                        response.conflictingNodeId = guard.frame->possessors.shared;
                     }
                     // -------------------------------------------------------------------------------------
                     // evict page as other node modifys it
                     if (guard.frame->state != BF_STATE::EVICTED) {
                        ensure(guard.frame->page);
                        ctx.activeInvalidationBatch->add(guard.frame->page);
                        guard.frame->page = nullptr;
                        guard.frame->state = BF_STATE::EVICTED;
                     }
                     // -------------------------------------------------------------------------------------
                     guard.frame->possession = POSSESSION::EXCLUSIVE;
                     guard.frame->setPossessor(ctx.bmId);
                     guard.frame->dirty = true;  // set dirty
                     ensure(guard.frame->isPossessor(ctx.bmId));
                     // -------------------------------------------------------------------------------------
                     writeMsg(clientId, response, threads::ThreadContext::my().page_handle);
                     // -------------------------------------------------------------------------------------
                     guard.frame->mhWaiting = false;
                     guard.frame->latch.unlatchExclusive();
                     // -------------------------------------------------------------------------------------
                     break;
                  }
                  case MESSAGE_TYPE::RAR: {
                     [[maybe_unused]] auto& request = *reinterpret_cast<RemoteAllocationRequest*>(ctx.request);
                     // -------------------------------------------------------------------------------------
                     PID pid = bm.pidFreeList.pop(threads::ThreadContext::my().pid_handle);
                     // -------------------------------------------------------------------------------------
                     BufferFrame& frame =bm.insertFrame(pid, [&](BufferFrame& frame){
                                                                frame.latch.latchExclusive();
                                                                frame.setPossession(POSSESSION::EXCLUSIVE);
                                                                frame.setPossessor(ctx.bmId);
                                                                frame.state = BF_STATE::EVICTED;
                                                                frame.pid = pid;
                                                                frame.pVersion = 0;
                                                                frame.dirty = true;
                                                                frame.epoch = bm.globalEpoch.load();
                                                          });  
                     
                     frame.latch.unlatchExclusive();
                     // -------------------------------------------------------------------------------------
                     auto& response = *MessageFabric::createMessage<rdma::RemoteAllocationResponse>(ctx.response, pid);
                     writeMsg(clientId, response, threads::ThreadContext::my().page_handle);
                     break;
                  }
                  default:
                     throw std::runtime_error("Unexpected Message in MB " + std::to_string(mailboxIdx) + " type " +
                                              std::to_string((size_t)ctx.request->type));
               }
               counters.incr(profiling::WorkerCounters::mh_msgs_handled);
            }
            mailboxIdx = ++startPosition;
            // submit
            [[maybe_unused]] auto nsubmit = async_read_buffer.submit();
            const uint64_t polled_events = async_read_buffer.pollEventsSync();
            async_read_buffer.getReadBfs(
                [&](BufferFrame& frame, uint64_t client_id, bool recheck_msg) {
                   // unlatch
                   frame.latch.unlatchExclusive();
                   counters.incr(profiling::WorkerCounters::ssd_pages_read);
                   if (recheck_msg) mailboxes[client_id] = 1;
                },
                polled_events);
            // check async reads
         }
         threadCount--; });

            // threads::CoreManager::getInstance().pinThreadToCore(t.native_handle());
            if ((t_i % 2) == 0)
               threads::CoreManager::getInstance().pinThreadToCore(t.native_handle());
            else
               threads::CoreManager::getInstance().pinThreadToHT(t.native_handle());
            t.detach();
         }
      }
      // -------------------------------------------------------------------------------------
      void MessageHandler::stopThread()
      {
         threadsRunning = false;
         while (threadCount)
            ; // wait
      }

      void MessageHandler::connectProxy()
      {
         printf("connect proxy\n");
         while ((!finishedInit) && (FLAGS_nodes != 1))
            ;
         auto &ip = NODES[FLAGS_nodes][FLAGS_nodes];
         auto &desport = NODESport[FLAGS_nodes][FLAGS_nodes];
         for (uint64_t n_i = 0; n_i < FLAGS_sqlSendThreads; n_i++)
         {
            proxycctxs[n_i].rctx = &(cm.initiateConnection(ip, rdma::Type::MESSAGE_HANDLER, 99, nodeId, desport));
            // -------------------------------------------------------------------------------------
            proxycctxs[n_i].incoming = (rdma::Message *)cm.getGlobalBuffer().allocate(rdma::SIZE_SQLMESSAGE, CACHE_LINE);
            proxycctxs[n_i].outgoing = (rdma::Message *)cm.getGlobalBuffer().allocate(rdma::SIZE_SQLMESSAGE, CACHE_LINE);
            proxycctxs[n_i].mailbox = (uint8_t *)cm.getGlobalBuffer().allocate(1, CACHE_LINE);
            proxycctxs[n_i].wqe = 0;
         }

         if (nodeId == 0)
         {
            cctxForRouterMap.rctx = &(cm.initiateConnection(ip, rdma::Type::MESSAGE_HANDLER, 90, nodeId, desport));
            cctxForRouterMap.incoming = (rdma::Message *)cm.getGlobalBuffer().allocate(rdma::SIZE_ROUTERMAPMESSAGE, CACHE_LINE);
            cctxForRouterMap.mailbox = (uint8_t *)cm.getGlobalBuffer().allocate(1, CACHE_LINE);
            cctxForRouterMap.wqe = 0;
         }

         for (uint64_t n_i = 0; n_i < FLAGS_sqlSendThreads; n_i++)
         {
            std::thread t([&, n_i]()
                          {
            rdma::InitMessage* init = (rdma::InitMessage*)cm.getGlobalBuffer().allocate(sizeof(rdma::InitMessage));
            init->mbOffset = (uintptr_t)proxycctxs[n_i].mailbox;
            init->plOffset = (uintptr_t)proxycctxs[n_i].incoming;
            init->bmId = nodeId;
            init->type = rdma::MESSAGE_TYPE::Init;
            // -------------------------------------------------------------------------------------
            cm.exchangeInitialMesssage(*(proxycctxs[n_i].rctx), init);
            // -------------------------------------------------------------------------------------
            proxycctxs[n_i].plOffset = (reinterpret_cast<rdma::InitMessage*>((proxycctxs[n_i].rctx->applicationData)))->plOffset;
            proxycctxs[n_i].mbOffset = (reinterpret_cast<rdma::InitMessage*>((proxycctxs[n_i].rctx->applicationData)))->mbOffset;
            proxycctxs[n_i].busyOffset=(reinterpret_cast<rdma::InitMessage*>((proxycctxs[n_i].rctx->applicationData)))->mbOffsetforBusy; });
            t.detach();
         }

         std::thread t2([&]()
                        {
            if(nodeId!=0) return;
            rdma::InitMessage* init = (rdma::InitMessage*)cm.getGlobalBuffer().allocate(sizeof(rdma::InitMessage));
            init->plOffset = (uintptr_t)cctxForRouterMap.incoming;
            init->mbOffset = (uintptr_t)cctxForRouterMap.mailbox;
            init->bmId = nodeId;
            init->type = rdma::MESSAGE_TYPE::Init;
            // -------------------------------------------------------------------------------------
            cm.exchangeInitialMesssage(*(cctxForRouterMap.rctx), init);
            // -------------------------------------------------------------------------------------
            cctxForRouterMap.mbOffset = (reinterpret_cast<rdma::InitMessage*>((cctxForRouterMap.rctx->applicationData)))->mbOffset;

            while(1){
               while(*(cctxForRouterMap.mailbox)==0){
                  _mm_pause();
               }
               *(cctxForRouterMap.mailbox)=0;
               //std::cout<<"Routermap"<<std::endl;
               switch (cctxForRouterMap.incoming->type) {
                  case MESSAGE_TYPE::RouterMap_metis_customer: {
                     get_router_map(customer_map,customer_ready);
                     break;
                  }
                  case MESSAGE_TYPE::RouterMap_metis_order:{
                     get_router_map(order_map,order_ready);
                     break;
                  }
                  case MESSAGE_TYPE::RouterMap_metis_stock:{
                     get_router_map(stock_map,stock_ready);
                     break;
                  }
                  case MESSAGE_TYPE::RouterMap_metis_warehouse:{
                     get_router_map(warehouse_map,warehouse_ready);
                     break;
                  }
                  case MESSAGE_TYPE::RouterMap_metis_district:{
                     get_router_map(district_map,district_ready);
                     break;
                  }
                  // TODO:
                  case MESSAGE_TYPE::RouterMap_metis_neworder:{
                     get_router_map(neworder_map,neworder_ready);
                     break;
                  }
                  case MESSAGE_TYPE::RouterMap_metis_orderline:{
                     get_router_map(orderline_map,orderline_ready);
                     break;
                  }
                  case MESSAGE_TYPE::RouterMap_dynamic_customer: {
                     get_router_map(customer_insert_keys,customer_update_ready);
                     break;
                  }
                  case MESSAGE_TYPE::RouterMap_dynamic_order:{
                     get_router_map(order_insert_keys,order_update_ready);
                     break;
                  }
                  case MESSAGE_TYPE::RouterMap_dynamic_stock:{
                     get_router_map(stock_insert_keys,stock_update_ready);
                     break;
                  }
                  case MESSAGE_TYPE::RouterMap_dynamic_warehouse:{
                     get_router_map(warehouse_insert_keys,warehouse_update_ready);
                     break;
                  }
                  case MESSAGE_TYPE::RouterMap_dynamic_district:{
                     get_router_map(district_insert_keys,district_update_ready);
                     break;
                  }
                  // TODO:
                  case MESSAGE_TYPE::RouterMap_dynamic_neworder:{
                     get_router_map(neworder_insert_keys,neworder_update_ready);
                     break;
                  }
                  case MESSAGE_TYPE::RouterMap_dynamic_orderline:{
                     get_router_map(orderline_insert_keys,orderline_update_ready);
                     break;
                  }
                  default:
                     std::cout<<"default"<<std::endl;
                  
                  break;
               }
               
            } });
         t2.detach();
         printf("connect over\n");
      }

      bool MessageHandler::sendSqltoProxy(char *sql, uint64_t sendThreadId)
      {
         while (*(proxycctxs[sendThreadId].mailbox) == 1)
         {
            _mm_pause();
         };
         *(proxycctxs[sendThreadId].mailbox) = 1;
         auto &wqe = proxycctxs[sendThreadId].wqe;
         uint64_t SIGNAL_ = FLAGS_pollingInterval - 1;
         rdma::completion signal = ((wqe & SIGNAL_) == 0) ? rdma::completion::signaled : rdma::completion::unsignaled;
         auto &sqlMessage = *MessageFabric::createMessage<SqlMessage>(proxycctxs[sendThreadId].outgoing, MESSAGE_TYPE::SQL, sql, nodeId);
         rdma::postWrite(&sqlMessage, *(proxycctxs[sendThreadId].rctx), signal, proxycctxs[sendThreadId].plOffset);
         if ((wqe & SIGNAL_) == SIGNAL_)
         {
            int comp{0};
            ibv_wc wcReturn;
            while (comp == 0)
            {
               comp = rdma::pollCompletion(proxycctxs[sendThreadId].rctx->id->qp->send_cq, 1, &wcReturn);
            }
            if (wcReturn.status != IBV_WC_SUCCESS)
            {
               printf("error%d\n", wcReturn.status);
               throw;
            }
         }
         wqe++;
         uint8_t flag = 1;
         signal = ((wqe & SIGNAL_) == 0) ? rdma::completion::signaled : rdma::completion::unsignaled;
         rdma::postWrite(&flag, *(proxycctxs[sendThreadId].rctx), signal, proxycctxs[sendThreadId].mbOffset);
         if ((wqe & SIGNAL_) == SIGNAL_)
         {
            int comp{0};
            ibv_wc wcReturn;
            while (comp == 0)
            {
               comp = rdma::pollCompletion(proxycctxs[sendThreadId].rctx->id->qp->send_cq, 1, &wcReturn);
            }
            if (wcReturn.status != IBV_WC_SUCCESS)
            {
               printf("error%d\n", wcReturn.status);

               throw;
            }
         }
         wqe++;
         return true;
      }

      bool MessageHandler::getSqlfromProxy(char *sql, uint64_t *src_node)
      {

         for (uint64_t n_i = 0; n_i < FLAGS_sqlSendThreads; n_i++)
         {
            // if (*(proxycctxs[n_i].mailbox) == 1)
            // {
            //    continue;
            // }
            // *(proxycctxs[n_i].mailbox) = 1;
            if(!mail_mtxs[n_i]->try_lock()){
               continue;
            }

            auto &sqlMessage = *static_cast<SqlMessage *>(proxycctxs[n_i].incoming);
            volatile uint8_t &received = sqlMessage.receiveFlag;
            if (received == 1)
            {
               received = 0;
               std::strcpy(sql, sqlMessage.sql);
               *src_node = sqlMessage.nodeId;
               uint8_t busy = 0;
               uint64_t SIGNAL_ = FLAGS_pollingInterval - 1;
               auto &wqe = proxycctxs[n_i].wqe_get_sql;
               rdma::completion signal = ((wqe & SIGNAL_) == 0) ? rdma::completion::signaled : rdma::completion::unsignaled;
               rdma::postWrite(&busy, *(proxycctxs[n_i].rctx), signal, proxycctxs[n_i].busyOffset);
               if ((wqe & SIGNAL_) == SIGNAL_)
               {
                  int comp{0};
                  ibv_wc wcReturn;
                  while (comp == 0)
                  {
                     comp = rdma::pollCompletion(proxycctxs[n_i].rctx->id->qp->send_cq, 1, &wcReturn);
                  }
                  if (wcReturn.status != IBV_WC_SUCCESS)
                  {
                     printf("error%d\n", wcReturn.status);
                     throw;
                  }
               }
               wqe++;
               mail_mtxs[n_i]->unlock();
               return true;
            }
            mail_mtxs[n_i]->unlock();
         }

         return false;
      }
      void MessageHandler::get_router_map(std::map<int, int> &router_map, bool &ready)
      {
         auto &MapMessage = *static_cast<RouterMapMessage *>(cctxForRouterMap.incoming);
         for (size_t i = 0; i < MapMessage.length; i++)
         {
            router_map.insert({MapMessage.RouterMap[i][0], MapMessage.RouterMap[i][1]});
         }
         if (MapMessage.overFlag == 1)
         {
            ready = true;
            std::cout << "partmapmetis.over,size:" << router_map.size() << std::endl;
         }

         auto &wqe = cctxForRouterMap.wqe;
         uint64_t SIGNAL_ = FLAGS_pollingInterval - 1;
         uint8_t flag = 1;
         rdma::completion signal = ((wqe & SIGNAL_) == 0) ? rdma::completion::signaled : rdma::completion::unsignaled;
         rdma::postWrite(&flag, *(cctxForRouterMap.rctx), signal, cctxForRouterMap.mbOffset);
         if ((wqe & SIGNAL_) == SIGNAL_)
         {
            int comp{0};
            ibv_wc wcReturn;
            while (comp == 0)
            {
               comp = rdma::pollCompletion(cctxForRouterMap.rctx->id->qp->send_cq, 1, &wcReturn);
            }
            if (wcReturn.status != IBV_WC_SUCCESS)
            {
               printf("error%d\n", wcReturn.status);
               throw;
            }
         }
         wqe++;
      }
      void MessageHandler::get_router_map(std::unordered_map<int, int> &router_map, bool &ready)
      {
         auto &MapMessage = *static_cast<RouterMapMessage *>(cctxForRouterMap.incoming);
         for (size_t i = 0; i < MapMessage.length; i++)
         {
            router_map.insert({MapMessage.RouterMap[i][0], MapMessage.RouterMap[i][1]});
         }
         if (MapMessage.overFlag == 1)
         {
            ready = true;
            std::cout << "partmapmetis.over,size:" << router_map.size() << std::endl;
         }

         auto &wqe = cctxForRouterMap.wqe;
         uint64_t SIGNAL_ = FLAGS_pollingInterval - 1;
         uint8_t flag = 1;
         rdma::completion signal = ((wqe & SIGNAL_) == 0) ? rdma::completion::signaled : rdma::completion::unsignaled;
         rdma::postWrite(&flag, *(cctxForRouterMap.rctx), signal, cctxForRouterMap.mbOffset);
         if ((wqe & SIGNAL_) == SIGNAL_)
         {
            int comp{0};
            ibv_wc wcReturn;
            while (comp == 0)
            {
               comp = rdma::pollCompletion(cctxForRouterMap.rctx->id->qp->send_cq, 1, &wcReturn);
            }
            if (wcReturn.status != IBV_WC_SUCCESS)
            {
               printf("error%d\n", wcReturn.status);
               throw;
            }
         }
         wqe++;
      }
   } // namespace rdma
} // namespace scalestore
