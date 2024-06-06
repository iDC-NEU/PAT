#pragma once
// -------------------------------------------------------------------------------------
#include "rdma/CommunicationManager.hpp"
#include "rdma/MessageHandler.hpp"
#include "threads/CoreManager.hpp"
#include "dynamicPartition/router.hpp"
// -------------------------------------------------------------------------------------
#include <memory>

namespace Proxy
{
   // -------------------------------------------------------------------------------------
   // avoids destruction of objects before remote side finished
   struct RemoteGuard
   {
      std::atomic<uint64_t> &numberRemoteConnected;
      RemoteGuard(std::atomic<uint64_t> &numberRemoteConnected) : numberRemoteConnected(numberRemoteConnected){};
      ~RemoteGuard()
      {
         while (numberRemoteConnected)
            ;
      }
   };
   class Proxy
   {
   public:
      //! Default constructor
      Proxy();
      //! Destructor
      ~Proxy();
      // -------------------------------------------------------------------------------------
      // Deleted constructors
      //! Copy constructor
      Proxy(const Proxy &other) = delete;
      //! Move constructor
      Proxy(Proxy &&other) noexcept = delete;
      //! Copy assignment operator
      Proxy &operator=(const Proxy &other) = delete;
      //! Move assignment operator
      Proxy &operator=(Proxy &&other) noexcept = delete;
      // -------------------------------------------------------------------------------------
      rdma::CM<rdma::InitMessage> &getCM() { return *cm; }
      // -------------------------------------------------------------------------------------
      NodeID getNodeID() { return nodeId; }
      bool isfinished(){ return mh->isfinished; }
      // -------------------------------------------------------------------------------------
      // -------------------------------------------------------------------------------------

      // -------------------------------------------------------------------------------------
   private:
      NodeID nodeId = 0;
      std::shared_ptr<spdlog::logger> router_logger;
      std::shared_ptr<spdlog::logger> txn_logger;
      std::unique_ptr<rdma::CM<rdma::InitMessage>> cm;
      std::unique_ptr<router::Router> router;
      std::unique_ptr<rdma::MessageHandler> mh;
   };
   // -------------------------------------------------------------------------------------
} // namespace Proxy
