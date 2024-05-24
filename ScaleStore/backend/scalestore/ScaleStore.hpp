#pragma once
// -------------------------------------------------------------------------------------
#include "profiling/ProfilingThread.hpp"
#include "profiling/counters/BMCounters.hpp"
#include "profiling/counters/RDMACounters.hpp"
#include "rdma/CommunicationManager.hpp"
#include "rdma/MessageHandler.hpp"
#include "storage/buffermanager/BufferFrameGuards.hpp"
#include "storage/buffermanager/Buffermanager.hpp"
#include "storage/buffermanager/Catalog.hpp"
#include "storage/buffermanager/PageProvider.hpp"
#include "storage/datastructures/BTree.hpp"
#include "storage/datastructures/DistributedBarrier.hpp"
#include "storage/datastructures/Vector.hpp"
#include "threads/CoreManager.hpp"
#include "threads/WorkerPool.hpp"
// -------------------------------------------------------------------------------------
#include <memory>

namespace scalestore
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

   class ScaleStore
   {
   public:
      //! Default constructor
      ScaleStore();
      //! Destructor
      ~ScaleStore();
      // -------------------------------------------------------------------------------------
      // Deleted constructors
      //! Copy constructor
      ScaleStore(const ScaleStore &other) = delete;
      //! Move constructor
      ScaleStore(ScaleStore &&other) noexcept = delete;
      //! Copy assignment operator
      ScaleStore &operator=(const ScaleStore &other) = delete;
      //! Move assignment operator
      ScaleStore &operator=(ScaleStore &&other) noexcept = delete;
      // -------------------------------------------------------------------------------------
      storage::Buffermanager &getBuffermanager() { return *bm; }
      // -------------------------------------------------------------------------------------
      threads::WorkerPool &getWorkerPool() { return *workerPool; }
      // -------------------------------------------------------------------------------------
      rdma::CM<rdma::InitMessage> &getCM() { return *cm; }
      // -------------------------------------------------------------------------------------
      NodeID getNodeID() { return nodeId; }
      // -------------------------------------------------------------------------------------
      s32 getSSDFD() { return ssd_fd; }
      // -------------------------------------------------------------------------------------
      void startProfiler(profiling::WorkloadInfo &wlInfo)
      {
         pt.running = true;
         profilingThread.emplace_back(&profiling::ProfilingThread::profile, &pt, nodeId, std::ref(wlInfo), std::ref(*bm));
      }
      // -------------------------------------------------------------------------------------
      void stopProfiler()
      {
         if (pt.running == true)
         {
            pt.running = false;
            for (auto &p : profilingThread)
               p.join();
            profilingThread.clear();
         }
         std::locale::global(std::locale("C")); // hack to restore locale which is messed up in tabulate package
      };

      // -------------------------------------------------------------------------------------
      // Catalog
      // -------------------------------------------------------------------------------------
      storage::Catalog &getCatalog()
      {
         return *catalog;
      }
      // -------------------------------------------------------------------------------------
      template <class Key, class Value>
      storage::BTree<Key, Value> createBTree()
      {
         storage::BTree<Key, Value> tree;
         // register into catalog
         catalog->insertCatalogEntry(storage::DS_TYPE::BTREE, tree.entryPage);
         return tree;
      }
      // -------------------------------------------------------------------------------------
      template <class T>
      storage::LinkedList<T> createLinkedList()
      {
         storage::LinkedList<T> list;
         catalog->insertCatalogEntry(storage::DS_TYPE::LLIST, list.head);
         return list;
      }
      // with data structure id
      template <class T>
      storage::LinkedList<T> createLinkedList(uint64_t &ret_datastructureId)
      {
         storage::LinkedList<T> list;
         ret_datastructureId = catalog->insertCatalogEntry(storage::DS_TYPE::LLIST, list.head);
         return list;
      }
      // -------------------------------------------------------------------------------------
      storage::DistributedBarrier createBarrier(uint64_t threads)
      {
         storage::DistributedBarrier barrier(threads);
         catalog->insertCatalogEntry(storage::DS_TYPE::BARRIER, barrier.pid);
         return barrier;
      }

      // bool getSql(char *sql){
      //    return mh->getSqlfromProxy(sql);
      // }
      // bool sendSqltoProxy(char *sql){
      //    return mh->sendSqltoProxy(sql);
      // }

      bool getSql(char *sql, uint64_t *src_node)
      {
         return mh->getSqlfromProxy(sql, src_node);
      }
      bool sendSqltoProxy(char *sql, uint64_t sendThreadId)
      {
         return mh->sendSqltoProxy(sql, sendThreadId);
      }
      bool customer_created() { return mh->customer_ready; }
      bool order_created() { return mh->order_ready; }
      bool stock_created() { return mh->stock_ready; }
      bool warehouse_created() { return mh->warehouse_ready; }
      bool district_created() { return mh->district_ready; }
      bool neworder_created() { return mh->neworder_ready; }
      bool orderline_created() { return mh->orderline_ready; }
      bool customer_update_ready() { return mh->customer_update_ready; }
      bool order_update_ready() { return mh->order_update_ready; }
      bool stock_update_ready() { return mh->stock_update_ready; }
      bool warehouse_update_ready() { return mh->warehouse_update_ready; }
      bool district_update_ready() { return mh->district_update_ready; }
      bool neworder_update_ready() { return mh->neworder_update_ready; }
      bool orderline_update_ready() { return mh->orderline_update_ready; }
      void set_customer_update_ready(bool ready) { mh->customer_update_ready = ready; }
      void set_order_update_ready(bool ready) { mh->order_update_ready = ready; }
      void set_stock_update_ready(bool ready) { mh->stock_update_ready = ready; }
      void set_warehouse_update_ready(bool ready) { mh->warehouse_update_ready = ready; }
      void set_district_update_ready(bool ready) { mh->district_update_ready = ready; }
      void set_neworder_update_ready(bool ready) { mh->neworder_update_ready = ready; }
      void set_orderline_update_ready(bool ready) { mh->orderline_update_ready = ready; }
      void customer_clear(){ mh->customer_insert_keys.clear(); }
      void order_clear(){ mh->order_insert_keys.clear(); }
      void stock_clear(){ mh->stock_insert_keys.clear(); }
      void warehouse_clear(){ mh->warehouse_insert_keys.clear(); }
      void district_clear(){ mh->district_insert_keys.clear(); }
      void neworder_clear(){ mh->neworder_insert_keys.clear(); }
      std::map<int, int> *get_customer_map()
      {
         return &mh->customer_map;
      }
      std::map<int, int> *get_order_map()
      {
         return &mh->order_map;
      }
      std::map<int, int> *get_stock_map()
      {
         return &mh->stock_map;
      }
      std::map<int, int> *get_warehouse_map()
      {
         return &mh->warehouse_map;
      }
      std::map<int, int> *get_district_map()
      {
         return &mh->district_map;
      }
      std::map<int, int> *get_neworder_map()
      {
         return &mh->neworder_map;
      }
      std::map<int, int> *get_orderline_map()
      {
         return &mh->orderline_map;
      }
      std::unordered_map<int, int> *get_customer_update_map()
      {
         return &mh->customer_insert_keys;
      }
      std::unordered_map<int, int> *get_order_update_map()
      {
         return &mh->order_insert_keys;
      }
      std::unordered_map<int, int> *get_stock_update_map()
      {
         return &mh->stock_insert_keys;
      }
      std::unordered_map<int, int> *get_warehouse_update_map()
      {
         return &mh->warehouse_insert_keys;
      }
      std::unordered_map<int, int> *get_district_update_map()
      {
         return &mh->district_insert_keys;
      }
      std::unordered_map<int, int> *get_neworder_update_map()
      {
         return &mh->neworder_insert_keys;
      }
      std::unordered_map<int, int> *get_orderline_update_map()
      {
         return &mh->orderline_insert_keys;
      }
      // -------------------------------------------------------------------------------------
   private:
      NodeID nodeId = 0;
      s32 ssd_fd;
      std::unique_ptr<rdma::CM<rdma::InitMessage>> cm;
      std::unique_ptr<storage::Buffermanager> bm;
      std::unique_ptr<rdma::MessageHandler> mh;
      std::unique_ptr<storage::PageProvider> pp;
      std::unique_ptr<RemoteGuard> rGuard;
      std::unique_ptr<threads::WorkerPool> workerPool;
      std::unique_ptr<profiling::BMCounters> bmCounters;
      std::unique_ptr<profiling::RDMACounters> rdmaCounters;
      std::unique_ptr<storage::Catalog> catalog;
      profiling::ProfilingThread pt;
      std::vector<std::thread> profilingThread;
   };
   // -------------------------------------------------------------------------------------
} // namespace scalestore
