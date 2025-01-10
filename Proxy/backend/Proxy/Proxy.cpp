#include "Proxy.hpp"
// -------------------------------------------------------------------------------------
#include <linux/fs.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <termios.h>
#include <unistd.h>
#include <fcntl.h>
// -------------------------------------------------------------------------------------
namespace Proxy
{
   Proxy::Proxy()
   {
      // -------------------------------------------------------------------------------------
      nodeId = FLAGS_nodes;
      std::string currentFile = __FILE__;
      std::string abstract_filename = currentFile.substr(0, currentFile.find_last_of("/\\") + 1);
      std::string logFilePath = abstract_filename + "/Logs/routerLog.txt";
      std::string txnlogFilePath = abstract_filename + "/Logs/txnLog.txt";
      std::ofstream routerlogFile(logFilePath);
      std::ofstream txnlogFile(txnlogFilePath);
      routerlogFile.close();
      txnlogFile.close();
      router_logger = spdlog::basic_logger_mt("router_logger", logFilePath);
      router_logger->set_level(spdlog::level::info);
      router_logger->flush_on(spdlog::level::info);
      router_logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] %v");
      txn_logger = spdlog::basic_logger_mt("txn_logger", txnlogFilePath);
      txn_logger->set_level(spdlog::level::info);
      txn_logger->flush_on(spdlog::level::info);
      txn_logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] %v");
      // ------------------------------------------------------------------------------------
      // -------------------------------------------------------------------------------------
      cm = std::make_unique<rdma::CM<rdma::InitMessage>>();
      router::Graph G;
      router = std::make_unique<router::Router>(G, *router_logger, *txn_logger);
      mh = std::make_unique<rdma::MessageHandler>(*cm, *router, nodeId, *router_logger, *txn_logger);
   }

   Proxy::~Proxy()
   {
   }
} // Proxy
