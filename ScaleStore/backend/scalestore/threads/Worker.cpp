#include "Worker.hpp"
// -------------------------------------------------------------------------------------
namespace scalestore
{
namespace threads
{
// -------------------------------------------------------------------------------------
thread_local Worker* Worker::tlsPtr = nullptr;
// -------------------------------------------------------------------------------------
Worker::Worker(uint64_t workerId, std::string name, rdma::CM<rdma::InitMessage>& cm, NodeID nodeId, rdma::Type type) : workerId(workerId), name(name), cpuCounters(name), cm(cm), nodeId(nodeId) , cctxs(FLAGS_nodes), threadContext(std::make_unique<ThreadContext>()){
   ThreadContext::tlsPtr = threadContext.get();
   // -------------------------------------------------------------------------------------
   // Connection to MessageHandler
   // -------------------------------------------------------------------------------------
   // First initiate connection

   for (uint64_t n_i = 0; n_i < FLAGS_nodes; n_i++) {
      // -------------------------------------------------------------------------------------
      if (n_i == nodeId) continue;
      // -------------------------------------------------------------------------------------
      auto& ip = NODES[FLAGS_nodes][n_i];
      auto& desport = NODESport[FLAGS_nodes][n_i];
      cctxs[n_i].rctx = &(cm.initiateConnection(ip, type, workerId, nodeId,desport));
      // -------------------------------------------------------------------------------------
      cctxs[n_i].incoming = (rdma::Message*)cm.getGlobalBuffer().allocate(rdma::LARGEST_MESSAGE, CACHE_LINE);
      cctxs[n_i].outgoing = (rdma::Message*)cm.getGlobalBuffer().allocate(rdma::LARGEST_MESSAGE, CACHE_LINE);
      cctxs[n_i].wqe = 0;
      // -------------------------------------------------------------------------------------
   }
   // -------------------------------------------------------------------------------------
   // Second finish connection
   rdma::InitMessage* init = (rdma::InitMessage*)cm.getGlobalBuffer().allocate(sizeof(rdma::InitMessage));
   for (uint64_t n_i = 0; n_i < FLAGS_nodes; n_i++) {
      // -------------------------------------------------------------------------------------
      if (n_i == nodeId) continue;
      // -------------------------------------------------------------------------------------
      // fill init messages
      init->mbOffset = 0;  // No MB offset
      init->plOffset = (uintptr_t)cctxs[n_i].incoming;
      init->bmId = nodeId;
      init->type = rdma::MESSAGE_TYPE::Init;
      // -------------------------------------------------------------------------------------
      cm.exchangeInitialMesssage(*(cctxs[n_i].rctx), init);
      // -------------------------------------------------------------------------------------
      cctxs[n_i].plOffset = (reinterpret_cast<rdma::InitMessage*>((cctxs[n_i].rctx->applicationData)))->plOffset;
      cctxs[n_i].mbOffset = (reinterpret_cast<rdma::InitMessage*>((cctxs[n_i].rctx->applicationData)))->mbOffset;
      ensure((reinterpret_cast<rdma::InitMessage*>((cctxs[n_i].rctx->applicationData)))->bmId == n_i);
   }

   // -------------------------------------------------------------------------------------
   // "broadcast" remote mailbox information to all message handlers to allow the MH to delegate requests
   // transparently 
   // -------------------------------------------------------------------------------------
   for (uint64_t o_n_i = 0; o_n_i < FLAGS_nodes; o_n_i++) {
      if(o_n_i == nodeId) continue;
      for (uint64_t i_n_i = 0; i_n_i < FLAGS_nodes; i_n_i++) {
         if (o_n_i == i_n_i)
            continue;
         if (nodeId == i_n_i)
            continue;
         auto& request = *MessageFabric::createMessage<DelegationRequest>(cctxs[o_n_i].outgoing, cctxs[i_n_i].mbOffset, cctxs[i_n_i].plOffset, i_n_i);
         assert(request.type == MESSAGE_TYPE::DR);
         [[maybe_unused]] auto& response = writeMsgSync<DelegationResponse>(o_n_i, request);

      }      
   }

   if(FLAGS_txnkeyPostback) connectProxy();

}

// -------------------------------------------------------------------------------------
Worker::~Worker()
{
// finish connection
   for (uint64_t n_i = 0; n_i < FLAGS_nodes; n_i++) {
      // -------------------------------------------------------------------------------------
      if (n_i == nodeId)
         continue;
      auto& request = *MessageFabric::createMessage<FinishRequest>(cctxs[n_i].outgoing);
      assert(request.type == MESSAGE_TYPE::Finish);
      writeMsg(n_i, request);
      // -------------------------------------------------------------------------------------
   }
}

void Worker::connectProxy(){
   printf("worker connect proxy\n");
   auto& ip = NODES[FLAGS_nodes][FLAGS_nodes];
   auto& desport = NODESport[FLAGS_nodes][FLAGS_nodes];
   proxycctx.rctx = &(cm.initiateConnection(ip, rdma::Type::WORKER, workerId, nodeId,desport));
   // -------------------------------------------------------------------------------------
   proxycctx.incoming = (rdma::Message*)cm.getGlobalBuffer().allocate(rdma::SIZE_TXNKEYSMESSAGE, CACHE_LINE);
   proxycctx.outgoing = (rdma::Message*)cm.getGlobalBuffer().allocate(rdma::SIZE_TXNKEYSMESSAGE, CACHE_LINE);
   proxycctx.mailbox=(uint8_t*)cm.getGlobalBuffer().allocate(1,CACHE_LINE);
   proxycctx.wqe = 0;

   // fill init messages
   rdma::InitMessage* init = (rdma::InitMessage*)cm.getGlobalBuffer().allocate(sizeof(rdma::InitMessage));
   init->mbOffset = (uintptr_t)proxycctx.mailbox;
   init->plOffset = (uintptr_t)proxycctx.incoming;
   init->bmId = nodeId;
   init->type = rdma::MESSAGE_TYPE::Init;
   // -------------------------------------------------------------------------------------
   cm.exchangeInitialMesssage(*(proxycctx.rctx), init);
   // -------------------------------------------------------------------------------------
   proxycctx.plOffset = (reinterpret_cast<rdma::InitMessage*>((proxycctx.rctx->applicationData)))->plOffset;
   proxycctx.mbOffset = (reinterpret_cast<rdma::InitMessage*>((proxycctx.rctx->applicationData)))->mbOffset;
   printf("worker connect over\n");
}

// bool Worker::sendTxnKeystoProxy(std::vector<int> &txnkeys_message){
//       while(*(proxycctx.mailbox)==1){
//             _mm_pause();
//       };
//       *(proxycctx.mailbox)=1;
//       auto& wqe = proxycctx.wqe;
//       uint64_t SIGNAL_ = FLAGS_pollingInterval - 1;
//       rdma::completion signal = ((wqe & SIGNAL_) == 0) ? rdma::completion::signaled : rdma::completion::unsignaled;
//       auto& txnKeysMessage = *MessageFabric::createMessage<TxnKeysMessage>(proxycctx.outgoing, MESSAGE_TYPE::TxnKeys,txnkeys_message,nodeId);
//       rdma::postWrite(&txnKeysMessage, *(proxycctx.rctx), signal,proxycctx.plOffset);
//       if ((wqe & SIGNAL_)==SIGNAL_) {
//          int comp{0};
//          ibv_wc wcReturn;
//          while (comp == 0) {
//             comp = rdma::pollCompletion(proxycctx.rctx->id->qp->send_cq, 1, &wcReturn);
//          }
//          if (wcReturn.status != IBV_WC_SUCCESS){
//                printf("error%d\n",wcReturn.status);
//                throw;
//          }
//       }
//       wqe++;

//       uint8_t flag = 1;
//       signal = ((wqe & SIGNAL_) == 0) ? rdma::completion::signaled : rdma::completion::unsignaled;
//       rdma::postWrite(&flag, *(proxycctx.rctx), signal, proxycctx.mbOffset);
//       if ((wqe & SIGNAL_)==SIGNAL_) {
//          int comp{0};
//          ibv_wc wcReturn;
//          while (comp == 0) {
//             comp = rdma::pollCompletion(proxycctx.rctx->id->qp->send_cq, 1, &wcReturn);
//          }
//          if (wcReturn.status != IBV_WC_SUCCESS){
//                printf("error%d\n",wcReturn.status);
//                throw;
//          }
//       }
//       wqe++;
//       return true;
// }
// -------------------------------------------------------------------------------------
}  // namespace threads
}  // namespace scalestore
