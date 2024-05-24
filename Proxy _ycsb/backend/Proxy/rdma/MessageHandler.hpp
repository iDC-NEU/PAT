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
#include "Defs.hpp"
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

         bool dispatcherSql(NodeID destnodeId, SqlMessage &sql, uint64_t clientId, uint8_t *mailboxe)
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
                  rdma::postWrite(&sql, *(cctxs[cctxsmap[destnodeId][n_i]].rctx), signal, cctxs[cctxsmap[destnodeId][n_i]].plOffset);

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

         const int w_count = FLAGS_tpcc_warehouse_count; // warehouse 数量，这个要设置
         using Range = std::pair<Integer, Integer>;

         // [1-w_count][01-10][0001-3000]
         int gen_customer_key(Integer c_w_id, Integer c_d_id, Integer c_id)
         {
            return 1'000'000 * c_w_id + 1'0000 * c_d_id + c_id;
         }
         const int customer_key_min = 1'000'000 * 1 + 1'0000 * 1 + 1;
         const int customer_key_max = 1'000'000 * w_count + 1'0000 * 10 + 3000;

         std::vector<int> gen_customer_keys_with_range(Range c_w_id_range, Range c_d_id_range, Range c_id_range)
         {
            std::vector<int> keys;
            for (Integer i = c_w_id_range.first; i <= c_w_id_range.second; ++i)
            {
               for (Integer j = c_d_id_range.first; j <= c_d_id_range.second; ++j)
               {
                  for (Integer k = c_id_range.first; k <= c_id_range.second; ++k)
                  {
                     keys.emplace_back(gen_customer_key(i, j, k));
                  }
               }
            }
            return keys;
         }

         // [w_count][10][3000] + [1-w_count][01-10][0001-3000]
         int gen_order_key(Integer o_w_id, Integer o_d_id, Integer o_id)
         {
            return customer_key_max + 1'000'000 * o_w_id + 1'0000 * o_d_id + o_id;
         }
         const int order_key_min = customer_key_max + 1'000'000 * 1 + 1'0000 * 1 + 1;
         const int order_key_max = customer_key_max + 1'000'000 * w_count + 1'0000 * 10 + 3000;

         //                                                                   |--------- ITEM_NO
         // [w_count][10][3000] + [w_count][10][3000] + [1-w_count][000001-100000]
         int gen_stock_key(Integer s_w_id, Integer s_i_id)
         {
            return order_key_max + 1'000'000 * s_w_id + s_i_id;
         }
         const int stock_key_min = order_key_max + 1'000'000 * 1 + 1;
         const int stock_key_max = order_key_max + 1'000'000 * w_count + 100000;
         int gen_warehouse_key(Integer w_id)
         {
            return stock_key_max + w_id * 50 + 1;
         }
         const int warehouse_key_min = stock_key_max + 50 + 1;
         const int warehouse_key_max = stock_key_max + w_count * 50 + 1;
         int gen_district_key(Integer d_w_id, Integer d_id)
         {
            return warehouse_key_max + 500 * d_w_id + d_id * 50;
         }
         const int district_key_min = warehouse_key_max + 500 * 1 + 50 * 1;
         const int district_key_max = warehouse_key_max + w_count * 500 + 500;

         // TODO:
         int gen_neworder_key(Integer no_w_id, Integer no_d_id, Integer no_o_id)
         {
            return district_key_max + 1'000'000 * no_w_id + 1'0000 * no_d_id + no_o_id;
         }
         const int neworder_key_min = district_key_max + 1'000'000 * 1 + 1'0000 * 1 + 1;
         const int neworder_key_max = district_key_max + 1'000'000 * w_count + 1'0000 * 10 + 3000;

         // TODO:
         int gen_orderline_key(Integer ol_w_id, Integer ol_d_id, Integer ol_o_id, Integer ol_number)
         {
            return neworder_key_max + 5'000'000 * ol_w_id + 300'000 * ol_d_id + 30 * ol_o_id + ol_number;
         }
         const int orderline_key_min = neworder_key_max + 5'000'000 * 1 + 30'0000 * 1 + 10000 * 1 + 1;
         const int orderline_key_max = neworder_key_max + 5'000'000 * w_count + 30'0000 * 10 + 10000 * 30 + 30;

         int get_wid(int key)
         {
            if (customer_key_min <= key && key <= customer_key_max)
            {
               return key / 1'000'000;
            }

            if (order_key_min <= key && key <= order_key_max)
            {
               return (key - customer_key_max) / 1'000'000;
            }

            if (stock_key_min <= key && key <= stock_key_max)
            {
               return (key - order_key_max) / 1'000'000;
            }
            return -1;
         }

         void gen_txn_key_list(std::vector<int> &key_list, const std::string &sql, uint64_t &destNodeId, int mod, bool &ishash)
         {

            std::string functionName = extractFunctionName(sql);
            std::vector<std::string> args = extractParameters(sql, ',');
            if (functionName == "newOrder")
            {
               Integer w_id = std::stoi(args[0]);
               Integer d_id = std::stoi(args[1]);
               Integer c_id = std::stoi(args[2]);
               // int i = 0;
               key_list.reserve(50);
               Integer o_id = rand() % 3000 + 1;
               std::istringstream iss0(args[3].substr(1, args[3].size() - 2));
               std::istringstream iss1(args[4].substr(1, args[4].size() - 2));
               std::istringstream iss2(args[5].substr(1, args[5].size() - 2));
               std::string arg;
               // while (getline(iss1, arg, ';'))
               // {
               //    auto supware = std::stoi(arg);
               //    getline(iss2, arg, ';');
               //    auto itemid = std::stoi(arg);
               //    key_list.push_back(gen_stock_key(supware, itemid));
               //    key_list.push_back(gen_stock_key(w_id, itemid));
               //    key_list.push_back(gen_orderline_key(w_id,d_id,o_id,i));
               // }
               while (getline(iss0, arg, ';'))
               {
                  // i = std::stoi(arg);
                  getline(iss1, arg, ';');
                  auto supware = std::stoi(arg);
                  getline(iss2, arg, ';');
                  auto itemid = std::stoi(arg);
                  key_list.push_back(gen_stock_key(supware, itemid));
                  key_list.push_back(gen_stock_key(w_id, itemid));
                  // key_list.push_back(gen_orderline_key(w_id,d_id,o_id,i));
               }
               // std::vector<Integer> qtys = convertStringVectorToIntVector(extractParameters(args[6], ';'));
               key_list.push_back(gen_customer_key(w_id, d_id, c_id));
               key_list.push_back(gen_warehouse_key(w_id));
               key_list.push_back(gen_district_key(w_id, d_id));

               // tpcc_workload.hpp:316  order.insert({w_id, d_id, o_id}, {c_id, timestamp, carrier_id, cnt, all_local});
               key_list.push_back(gen_order_key(w_id, d_id, o_id));
               //  {c_id, timestamp, carrier_id, cnt, all_local}该如何处理？不需要（主键只有{w_id, d_id, o_id})
               key_list.push_back(gen_neworder_key(w_id, d_id, o_id));
               // Log::getInstance().writekeyLog(w_id, key_list);
            }

            if (functionName == "delivery")
            {
               Integer w_id = std::stoi(args[0]);
               // Integer carrier_id= std::stoi(args[1]);
               // Timestamp datetime = std::stoul(args[2]);
               for (int i = 1; i <= 10; i++)
               {
                  Integer d_id = i;
                  Integer o_id = rand() % 3000 + 1;
                  Integer c_id = rand() % 3000 + 1;
                  Integer no_o_id = rand() % 3000 + 1;
                  // Integer no_o_id = rand() % 3000 + 1;
                  // o_id = no_o_id;
                  Integer ol_cnt = rand() % 15 + 1; // TODO: 此处模仿tpcc_workload.cpp:667-669
                  key_list.push_back(gen_customer_key(w_id, d_id, c_id));
                  key_list.push_back(gen_order_key(w_id, d_id, o_id));
                  key_list.push_back(gen_neworder_key(w_id, d_id, no_o_id));
                  // key_list.push_back(gen_orderline_key(w_id,d_id,o_id,ol_cnt)); // TODO: 正确？
                  for (Integer ol_number = 1; ol_number <= ol_cnt; ol_number++)
                  { // TODO: 此处模仿tpcc_workload.cpp:667-669
                     // key_list.push_back(gen_orderline_key(w_id,d_id,o_id,ol_number));
                  }
               }
               // Log::getInstance().writekeyLog(w_id, key_list);
               destNodeId = w_id % mod;
               ishash = true;
            }

            if (functionName == "stockLevel")
            {
               Integer w_id = std::stoi(args[0]);
               Integer d_id = std::stoi(args[1]);
               // Integer threshold = std::stoi(args[2]);
               key_list.push_back(gen_district_key(w_id, d_id));
               for (int i = 1; i <= 20; i++)
               {
                  Integer i_id = rand() % 100000 + 1;
                  key_list.push_back(gen_stock_key(w_id, i_id));
               }
               // Log::getInstance().writekeyLog(w_id, key_list);
               ishash = true;
               destNodeId = w_id % mod;
            }

            if (functionName == "orderStatusId")
            {
               Integer w_id = std::stoi(args[0]);
               Integer d_id = std::stoi(args[1]);
               Integer c_id = std::stoi(args[2]);
               // customer.lookup1({w_id, d_id, c_id}, [&](const customer_t& rec) {});
               // Integer o_id = rand() % 3000 + 1;
               key_list.push_back(gen_customer_key(w_id, d_id, c_id));
               // key_list.push_back(gen_order_key(w_id, d_id, o_id));
               // Log::getInstance().writekeyLog(w_id, key_list);
               ishash = true;
               destNodeId = w_id % mod;
            }

            if (functionName == "orderStatusName")
            {
               // Integer w_id = std::stoi(args[0]);
               // Integer d_id = std::stoi(args[1]);
               // Varchar<16> c_last(args[3].c_str());
               Integer w_id = std::stoi(args[0]);
               Integer d_id = std::stoi(args[1]);
               Integer c_id = rand() % 3000 + 1;
               Integer o_id = rand() % 3000 + 1;
               key_list.push_back(gen_customer_key(w_id, d_id, c_id));
               key_list.push_back(gen_order_key(w_id, d_id, o_id));
               // Log::getInstance().writekeyLog(w_id, key_list);
               ishash = true;
               destNodeId = w_id % mod;
            }

            if (functionName == "paymentById")
            {
               Integer w_id = std::stoi(args[0]);
               Integer d_id = std::stoi(args[1]);
               Integer c_w_id = std::stoi(args[2]);
               Integer c_d_id = std::stoi(args[3]);
               Integer c_id = std::stoi(args[4]);

               // customer.lookup1({c_w_id, c_d_id, c_id}, [&](const customer_t& rec) {});
               key_list.push_back(gen_customer_key(c_w_id, c_d_id, c_id));

               // if (c_credit == "BC") {
               // customer.update1({c_w_id, c_d_id, c_id}, [&](customer_t& rec) {});
               // } else {
               // customer.update1({c_w_id, c_d_id, c_id}, [&](customer_t& rec) {});
               // }
               key_list.push_back(gen_customer_key(c_w_id, c_d_id, c_id));
               key_list.push_back(gen_warehouse_key(w_id));
               key_list.push_back(gen_warehouse_key(w_id));
               key_list.push_back(gen_district_key(w_id, d_id));
               key_list.push_back(gen_district_key(w_id, d_id));
               // Log::getInstance().writekeyLog(w_id, key_list);
            }

            if (functionName == "paymentByName")
            {
               Integer w_id = std::stoi(args[0]);
               Integer d_id = std::stoi(args[1]);
               Integer c_w_id = std::stoi(args[2]);
               Integer c_d_id = std::stoi(args[3]);
               // Varchar<16> c_last(args[4].c_str());
               // Timestamp h_date = std::stoul(args[5]);
               // Numeric h_amount = std::stod(args[6]);
               // Timestamp datetime = std::stoul(args[7]);
               Integer c_id = rand() % 3000 + 1;
               key_list.push_back(gen_customer_key(c_w_id, c_d_id, c_id));
               key_list.push_back(gen_warehouse_key(w_id));
               key_list.push_back(gen_warehouse_key(w_id));
               key_list.push_back(gen_district_key(w_id, d_id));
               key_list.push_back(gen_district_key(w_id, d_id));
               // Log::getInstance().writekeyLog(w_id, key_list);
               ishash = true;
               destNodeId = w_id % mod;
            }
         }
         void gen_txn_key_list_with_weight(std::vector<router::TxnNode> &key_list, const std::string &sql, uint64_t &destNodeId, int mod, bool &ishash)
         {

            std::string functionName = extractFunctionName(sql);
            std::vector<std::string> args = extractParameters(sql, ',');
            int temp;
            if (functionName == "newOrder")
            {
               Integer w_id = std::stoi(args[0]);
               Integer d_id = std::stoi(args[1]);
               Integer c_id = std::stoi(args[2]);
               // int i = 0;
               // key_list.reserve(50);
               Integer o_id = rand() % 3000 + 1;
               std::istringstream iss0(args[3].substr(1, args[3].size() - 2));
               std::istringstream iss1(args[4].substr(1, args[4].size() - 2));
               std::istringstream iss2(args[5].substr(1, args[5].size() - 2));
               std::string arg;
               // while (getline(iss1, arg, ';'))
               // {
               //    auto supware = std::stoi(arg);
               //    getline(iss2, arg, ';');
               //    auto itemid = std::stoi(arg);
               //    key_list.push_back(gen_stock_key(supware, itemid));
               //    key_list.push_back(gen_stock_key(w_id, itemid));
               //    key_list.push_back(gen_orderline_key(w_id,d_id,o_id,i));
               // }
               while (getline(iss0, arg, ';'))
               {
                  // i = std::stoi(arg);
                  getline(iss1, arg, ';');
                  auto supware = std::stoi(arg);
                  getline(iss2, arg, ';');
                  auto itemid = std::stoi(arg);
                  key_list.push_back({gen_stock_key(supware, itemid), false, 1});
                  key_list.push_back({gen_stock_key(w_id, itemid), false, 1});
                  // key_list.push_back(gen_orderline_key(w_id,d_id,o_id,i));
               }
               // std::vector<Integer> qtys = convertStringVectorToIntVector(extractParameters(args[6], ';'));
               key_list.push_back({gen_customer_key(w_id, d_id, c_id), true, 1});
               key_list.push_back({gen_warehouse_key(w_id), true, 1});
               key_list.push_back({gen_district_key(w_id, d_id), false, 1});

               // tpcc_workload.hpp:316  order.insert({w_id, d_id, o_id}, {c_id, timestamp, carrier_id, cnt, all_local});
               key_list.push_back({gen_order_key(w_id, d_id, o_id), false, 1});
               key_list.push_back({gen_neworder_key(w_id, d_id, o_id), false, 1});
               // Log::getInstance().writekeyLog(w_id, key_list);
            }

            if (functionName == "delivery")
            {
               Integer w_id = std::stoi(args[0]);
               // Integer carrier_id= std::stoi(args[1]);
               // Timestamp datetime = std::stoul(args[2]);
               for (int i = 1; i <= 10; i++)
               {
                  Integer d_id = i;
                  Integer o_id = rand() % 3000 + 1;
                  Integer c_id = rand() % 3000 + 1;
                  Integer no_o_id = rand() % 3000 + 1;
                  // Integer no_o_id = rand() % 3000 + 1;
                  // o_id = no_o_id;
                  Integer ol_cnt = rand() % 15 + 1; // TODO: 此处模仿tpcc_workload.cpp:667-669
                  key_list.push_back({gen_customer_key(w_id, d_id, c_id), false, 1});
                  key_list.push_back({gen_order_key(w_id, d_id, o_id), false, 1});
                  key_list.push_back({gen_neworder_key(w_id, d_id, no_o_id), true, 1});
                  // key_list.push_back(gen_orderline_key(w_id,d_id,o_id,ol_cnt)); // TODO: 正确？
                  for (Integer ol_number = 1; ol_number <= ol_cnt; ol_number++)
                  { // TODO: 此处模仿tpcc_workload.cpp:667-669
                     // key_list.push_back(gen_orderline_key(w_id,d_id,o_id,ol_number));
                  }
               }
               // Log::getInstance().writekeyLog(w_id, key_list);
               destNodeId = w_id % mod;
               ishash = true;
            }

            if (functionName == "stockLevel")
            {
               Integer w_id = std::stoi(args[0]);
               Integer d_id = std::stoi(args[1]);
               // Integer threshold = std::stoi(args[2]);
               key_list.push_back({gen_district_key(w_id, d_id), true, 1});
               for (int i = 1; i <= 20; i++)
               {
                  Integer i_id = rand() % 100000 + 1;
                  key_list.push_back({gen_stock_key(w_id, i_id), true, 1});
               }
               // Log::getInstance().writekeyLog(w_id, key_list);
               ishash = true;
               destNodeId = w_id % mod;
            }

            if (functionName == "orderStatusId")
            {
               Integer w_id = std::stoi(args[0]);
               Integer d_id = std::stoi(args[1]);
               Integer c_id = std::stoi(args[2]);
               // customer.lookup1({w_id, d_id, c_id}, [&](const customer_t& rec) {});
               // Integer o_id = rand() % 3000 + 1;
               key_list.push_back({gen_customer_key(w_id, d_id, c_id), true, 1});
               // key_list.push_back(gen_order_key(w_id, d_id, o_id));
               // Log::getInstance().writekeyLog(w_id, key_list);
               ishash = true;
               destNodeId = w_id % mod;
            }

            if (functionName == "orderStatusName")
            {
               // Integer w_id = std::stoi(args[0]);
               // Integer d_id = std::stoi(args[1]);
               // Varchar<16> c_last(args[3].c_str());
               Integer w_id = std::stoi(args[0]);
               Integer d_id = std::stoi(args[1]);
               Integer c_id = rand() % 3000 + 1;
               Integer o_id = rand() % 3000 + 1;
               key_list.push_back({gen_customer_key(w_id, d_id, c_id), true, 1});
               key_list.push_back({gen_order_key(w_id, d_id, o_id), true, 1});
               // Log::getInstance().writekeyLog(w_id, key_list);
               ishash = true;
               destNodeId = w_id % mod;
            }

            if (functionName == "paymentById")
            {
               Integer w_id = std::stoi(args[0]);
               Integer d_id = std::stoi(args[1]);
               Integer c_w_id = std::stoi(args[2]);
               Integer c_d_id = std::stoi(args[3]);
               Integer c_id = std::stoi(args[4]);

               // customer.lookup1({c_w_id, c_d_id, c_id}, [&](const customer_t& rec) {});
               key_list.push_back({gen_customer_key(c_w_id, c_d_id, c_id), false, 2});

               // if (c_credit == "BC") {
               // customer.update1({c_w_id, c_d_id, c_id}, [&](customer_t& rec) {});
               // } else {
               // customer.update1({c_w_id, c_d_id, c_id}, [&](customer_t& rec) {});
               // }
               temp = gen_warehouse_key(w_id);
               key_list.push_back({gen_warehouse_key(temp), false, 2});
               temp = gen_district_key(w_id, d_id);
               key_list.push_back({gen_district_key(w_id, d_id), false, 2});
               // Log::getInstance().writekeyLog(w_id, key_list);
            }

            if (functionName == "paymentByName")
            {
               Integer w_id = std::stoi(args[0]);
               Integer d_id = std::stoi(args[1]);
               Integer c_w_id = std::stoi(args[2]);
               Integer c_d_id = std::stoi(args[3]);
               // Varchar<16> c_last(args[4].c_str());
               // Timestamp h_date = std::stoul(args[5]);
               // Numeric h_amount = std::stod(args[6]);
               // Timestamp datetime = std::stoul(args[7]);
               Integer c_id = rand() % 3000 + 1;
               key_list.push_back({gen_customer_key(c_w_id, c_d_id, c_id), true, 1});
               key_list.push_back({gen_warehouse_key(w_id), false, 2});
               key_list.push_back({gen_district_key(w_id, d_id), false, 2});
               // Log::getInstance().writekeyLog(w_id, key_list);
               ishash = true;
               destNodeId = w_id % mod;
            }
         }
         // -------------------------------------------------------------------------------------
      }; // namespace rdma
   }     // namespace rdma
} // namespace Proxy
