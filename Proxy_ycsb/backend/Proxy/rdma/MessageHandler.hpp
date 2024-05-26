#pragma once
// -------------------------------------------------------------------------------------
#include "CommunicationManager.hpp"
#include "Proxy/rdma/messages/Messages.hpp"
#include "Proxy/utils/RandomGenerator.hpp"
#include "Proxy/dynamicPartition/router.hpp"
#include "Proxy/utils/Log.hpp"
// -------------------------------------------------------------------------------------
#include <bitset>
#include <iostream>
#include <arpa/inet.h>
#include <libaio.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <string>
#include <sstream>
#include <vector>
#include <utility>
#include "../../ScaleStore/shared-headers/Defs.hpp"
#include "spdlog/spdlog.h"
namespace Proxy
{
   namespace rdma
   {

      // -------------------------------------------------------------------------------------
      struct MessageHandler
      {
         // -------------------------------------------------------------------------------------
         // -------------------------------------------------------------------------------------

         // -------------------------------------------------------------------------------------
         struct alignas(CACHE_LINE) ConnectionContext
         {
            uintptr_t plOffset{0}; // does not have mailbox just payload with flag
            uintptr_t mbOffset{0};
            rdma::Message *incoming{nullptr};  // rdma pointer
            rdma::Message *outcoming{nullptr}; // in current protocol only one message can be outstanding per client
            rdma::RdmaContext *rctx{nullptr};
            uint64_t wqe_sql{0};  // wqe currently outstanding
            uint64_t wqe_flag{0}; // wqe currently outstanding
            NodeID dbId{0};       // id to which incoming client connection belongs
            uint8_t *mbforBusy;
         };
         struct alignas(CACHE_LINE) ConnectionContextForRouterMap
         {
            uintptr_t plOffset{0};
            uintptr_t mbOffset{0};
            rdma::RdmaContext *rctx{nullptr};
            uint64_t wqe{0}; // wqe currently outstanding
            NodeID dbId{0};
            uint8_t *Received;
         };
         // -------------------------------------------------------------------------------------
         // Mailbox partition per thread
         struct MailboxPartition
         {
            MailboxPartition() = default;
            MailboxPartition(uint8_t *mailboxes, uint64_t numberMailboxes, uint64_t beginId)
                : mailboxes(mailboxes), numberMailboxes(numberMailboxes), beginId(beginId){};
            uint8_t *mailboxes = nullptr;
            uint64_t numberMailboxes;
            uint64_t beginId;
         };

         // -------------------------------------------------------------------------------------
         MessageHandler(rdma::CM<InitMessage> &cm, router::Router &router, NodeID nodeId, spdlog::logger &router_logger, spdlog::logger &txn_logger);
         ~MessageHandler();
         // -------------------------------------------------------------------------------------
         void startThread();
         void stopThread();
         void init();
         // -------------------------------------------------------------------------------------
         std::atomic<bool> threadsRunning = true;
         std::atomic<size_t> threadCount = 0;
         rdma::CM<InitMessage> &cm;
         router::Router &router;
         // -------------------------------------------------------------------------------------
         NodeID nodeId;
         std::vector<ConnectionContext> cctxs;
         std::vector<ConnectionContextForRouterMap> cctxForRouterMap;
         std::vector<MailboxPartition> mbPartitions;
         std::atomic<uint64_t> connectedClients = 0;
         std::atomic<bool> finishedInit = false;
         std::vector<std::vector<uint64_t>> cctxsmap;
         std::vector<uint64_t> routerCach;
         int count = 0;
         spdlog::logger &router_logger;
         spdlog::logger &txn_logger;
         std::shared_ptr<spdlog::logger> routertime_logger;
         std::vector<std::mutex *> mtxs;
         std::vector<std::mutex *> mail_mtxs;
         std::vector<int> router_number_per_thread;
         std::vector<double> proxy_time_per_thread;
         std::vector<int> graph_number_per_thread;
         std::vector<int> random_number_per_thread;
         std::vector<std::ofstream> outputs;
         std::chrono::high_resolution_clock::time_point last_router_statistics;
         std::chrono::high_resolution_clock::time_point last_txn_statistics;
         // -------------------------------------------------------------------------------------

         bool dispatcherKey(NodeID destnodeId, TxnKeysMessage &key_message, uint64_t clientId, uint8_t *mailboxe)
         {
            for (uint64_t n_i = 0; n_i < FLAGS_sqlSendThreads; n_i++)
            {
               if(!mail_mtxs[n_i + FLAGS_sqlSendThreads * destnodeId]->try_lock()){
                  continue;
               }
               if (*(cctxs[cctxsmap[destnodeId][n_i]].mbforBusy) == 0)
               {
                  *(cctxs[cctxsmap[destnodeId][n_i]].mbforBusy) = 1;
                  auto &wqe_dest = cctxs[cctxsmap[destnodeId][n_i]].wqe_sql;
                  rdma::completion signal = rdma::completion::signaled;
                  rdma::postWrite(&key_message, *(cctxs[cctxsmap[destnodeId][n_i]].rctx), signal, cctxs[cctxsmap[destnodeId][n_i]].plOffset);

                  int comp{0};
                  ibv_wc wcReturn;
                  while (comp == 0)
                  {
                     comp = rdma::pollCompletion(cctxs[cctxsmap[destnodeId][n_i]].rctx->id->qp->send_cq, 1, &wcReturn);
                  }
                  if (wcReturn.status != IBV_WC_SUCCESS)
                  {
                     throw;
                  }

                  wqe_dest++;
                  clientId++;
                  *mailboxe = 0;
                  // auto &wqe_src = cctxs[cctxsmap[destnodeId][n_i]].wqe_flag;
                  // signal = ((wqe_src & SIGNAL_) == 0) ? rdma::completion::signaled : rdma::completion::unsignaled;
                  // uint8_t flag = 0;
                  // rdma::postWrite(&flag, *(cctxs[cctxsmap[destnodeId][n_i]].rctx), signal, cctxs[cctxsmap[destnodeId][n_i]].mbOffset);
                  // if ((wqe_src & SIGNAL_) == SIGNAL_)
                  // {
                  //    int comp{0};
                  //    ibv_wc wcReturn;
                  //    while (comp == 0)
                  //    {
                  //       comp = rdma::pollCompletion(cctxs[cctxsmap[destnodeId][n_i]].rctx->id->qp->send_cq, 1, &wcReturn);
                  //    }
                  //    if (wcReturn.status != IBV_WC_SUCCESS)
                  //    {
                  //       throw;
                  //    }
                  // }
                  // wqe_src++;
                  mail_mtxs[n_i + FLAGS_sqlSendThreads * destnodeId]->unlock();
                  return true;
               }
               mail_mtxs[n_i + FLAGS_sqlSendThreads * destnodeId]->unlock();
            }
            return false;
         }

         void send_map_to_node(std::unordered_map<int, int> *part_map, rdma::Message *outcoming, MESSAGE_TYPE type)
         {
            // 遍历unordered_map并执行函数
            int count = 0;
            int x[1000][2];
            for (auto it = part_map->begin(); it != part_map->end(); ++it)
            {
               // 处理当前元组
               x[count % 1000][0] = it->first;
               x[count % 1000][1] = it->second;

               // 检查是否达到每一千个元组的条件
               if (++count % 1000 == 0)
               {

                  // 阻塞确认上一轮全部被接受
                  bool all_received = false;
                  while (!all_received)
                  {
                     all_received = true;
                     for (uint64_t i = 0; i < cctxForRouterMap.size(); i++)
                     {
                        volatile uint8_t &received = *(cctxForRouterMap[i].Received);
                        if (received == 0)
                        {
                           all_received = false;
                        }
                     }
                  }

                  // 发送信息
                  auto &mapMessage = *MessageFabric::createMessage<RouterMapMessage>(outcoming, type, x, 1000, 0);

                  for (uint64_t i = 0; i < cctxForRouterMap.size(); i++)
                  {
                     *(cctxForRouterMap[i].Received) = 0;
                     auto &wqe = cctxForRouterMap[i].wqe;
                     uint64_t SIGNAL_ = FLAGS_pollingInterval - 1;
                     rdma::completion signal = ((wqe & SIGNAL_) == 0) ? rdma::completion::signaled : rdma::completion::unsignaled;
                     uint8_t flag = 1;
                     rdma::postWrite(&mapMessage, *(cctxForRouterMap[i].rctx), rdma::completion::unsignaled, cctxForRouterMap[i].plOffset);
                     rdma::postWrite(&flag, *(cctxForRouterMap[i].rctx), signal, cctxForRouterMap[i].mbOffset);
                     if ((wqe & SIGNAL_) == SIGNAL_)
                     {
                        int comp{0};
                        ibv_wc wcReturn;
                        while (comp == 0)
                        {
                           comp = rdma::pollCompletion(cctxForRouterMap[i].rctx->id->qp->send_cq, 1, &wcReturn);
                        }
                        if (wcReturn.status != IBV_WC_SUCCESS)
                           throw;
                     }
                     wqe++;
                  }
               }
            }

            // 处理剩余的元组
            if (1)
            {
               bool all_received = false;
               while (!all_received)
               {
                  all_received = true;
                  for (uint64_t i = 0; i < cctxForRouterMap.size(); i++)
                  {
                     volatile uint8_t &received = *(cctxForRouterMap[i].Received);
                     if (received == 0)
                     {
                        all_received = false;
                     }
                  }
               }

               // 发送信息
               auto &mapMessage = *MessageFabric::createMessage<RouterMapMessage>(outcoming, type, x, count % 1000, 1);
               for (uint64_t i = 0; i < cctxForRouterMap.size(); i++)
               {
                  *(cctxForRouterMap[i].Received) = 0;
                  auto &wqe = cctxForRouterMap[i].wqe;
                  uint64_t SIGNAL_ = FLAGS_pollingInterval - 1;
                  rdma::completion signal = ((wqe & SIGNAL_) == 0) ? rdma::completion::signaled : rdma::completion::unsignaled;
                  uint8_t flag = 1;
                  rdma::postWrite(&mapMessage, *(cctxForRouterMap[i].rctx), rdma::completion::unsignaled, cctxForRouterMap[i].plOffset);
                  rdma::postWrite(&flag, *(cctxForRouterMap[i].rctx), signal, cctxForRouterMap[i].mbOffset);
                  if ((wqe & SIGNAL_) == SIGNAL_)
                  {
                     int comp{0};
                     ibv_wc wcReturn;
                     while (comp == 0)
                     {
                        comp = rdma::pollCompletion(cctxForRouterMap[i].rctx->id->qp->send_cq, 1, &wcReturn);
                     }
                     if (wcReturn.status != IBV_WC_SUCCESS)
                        throw;
                  }
                  wqe++;
               }
            }
            std::cout << "send_num:" << count << std::endl;
         }

         using Integer = int32_t;

         /*----------------------------------------------------------------------------------------*/
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

         std::vector<std::string> extractParameters(const std::string &sql, char separator)
         {
            std::vector<std::string> parameters;
            size_t openParenthesisPos = (separator == ',' ? sql.find('(') : sql.find('{'));
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
            return parameters;
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
         using Range = std::pair<Integer, Integer>;

         // -------------------------------------------------------------------------------------
      }; // namespace rdma
   }     // namespace rdma
} // namespace Proxy
