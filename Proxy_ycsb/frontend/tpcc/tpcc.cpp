#include "Proxy/Config.hpp"
#include "Proxy/Proxy.hpp"
#include "Proxy/rdma/CommunicationManager.hpp"
#include "Proxy/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <unistd.h>
#include <iostream>
#include <set>
#include <string>
#include <vector>
// -------------------------------------------------------------------------------------
DEFINE_double(TPCC_run_for_seconds, 10.0, "");
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------

// -------------------------------------------------------------------------------------
// MAIN
int main(int argc, char* argv[]) {
   gflags::SetUsageMessage("Leanstore TPC-C");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   Proxy::Proxy db;
   while(1){
      sleep(10000);
   }
   return 0;
}
