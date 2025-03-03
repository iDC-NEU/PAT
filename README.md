## PAT
**PAT** is a page affinity-based transaction routing scheme based on shared-cache architecture.

## Abstract
Cloud-native databases decouple compute from storage to achieve scalability, elasticity, and high availability. Most such databases use a shared-cache architecture, where compute nodes jointly maintain a shared cache to buffer pages from shared storage. During transaction execution, local cache misses incur significant page transfer overhead, while exclusive access to pages for row writes causes expensive inter-node transaction blocking. Both increase transaction latency. Existing shared-cache databases overlook the distribution of cached pages maintained by each compute node and route transactions to nodes based on the access patterns of the workload, increasing these overheads.

We propose **PAT**, a novel transaction routing mechanism that uses page affinity to route transactions to reduce local cache misses and page contention, significantly improving system performance. **PAT** uses key range to identify *page affinity* and routes transactions that access pages with strong affinity to the same compute node. Through periodic page reorganization, rows accessed by the same node are stored in the same set of pages, allowing different compute nodes to cache distinct pages and further reduce the page contention. Experiments show that **PAT** achieves 2.42 - 14.36 X higher throughput than state-of-the-art approaches under TPC-C and YCSB workloads.

## Building
### Dependencies
**PAT** is developed and tested on Ubuntu 20.04 with the Linux kernel version 5.15.0-101-generic. It should also work on other unix-like distributions. Building libgrape-lite requires the following softwares installed as dependencies.
- gflags
- lib_aio
- ibverbs
- tabulate
- rdma cm
- cmake
- spdlog
### CMake build
```
echo N | sudo tee /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages    
mkdir build
cd build
````
we can build the executable with either in debug mode with address sanitizers enabled:
```
cmake -D CMAKE_C_COMPILER=gcc-10 -D CMAKE_CXX_COMPILER=g++-10 -DCMAKE_BUILD_TYPE=Debug -DSANI=On .. && make -j
```
or in release mode:
```
cmake -D CMAKE_C_COMPILER=gcc-10 -D CMAKE_CXX_COMPILER=g++-10 -DCMAKE_BUILD_TYPE=Release .. && make -j
```
### Run executable
```
make -j && numactl --membind=0 --cpunodebind=0  ./ycsb -ownIp=172.18.94.80 -nodes=1 -YCSB_all_workloads -worker=20 -YCSB_tuple_count=1000000000 -dramGB=150 -csvFile=singlenode_oom_scalestore_ycsb_zipf.csv  -YCSB_run_for_seconds=60 -ssd_path=/dev/md0 --ssd_gib=400 -pageProviderThreads=4 -YCSB_all_zipf
```
### Configuration
To configure the servers and their ips the following configuration needs to be adapted:
```
const std::vector<std::vector<std::string>> NODES{
    {""},                                                                                              // 0 to allow direct offset
    {"172.18.94.80"},                                                                                  // 1
    {"172.18.94.80", "172.18.94.70"},                                                                  // 2
    {"172.18.94.80", "172.18.94.70", "172.18.94.10"},                                                  // 3
    {"172.18.94.80", "172.18.94.70", "172.18.94.10", "172.18.94.20"},                                  // 4
    {"172.18.94.80", "172.18.94.70", "172.18.94.10", "172.18.94.20", "172.18.94.40"},                  // 5
    {"172.18.94.80", "172.18.94.70", "172.18.94.10", "172.18.94.20", "172.18.94.40", "172.18.94.30"},  // 6
};
```
The last column indicates the ip of router, and the other columns indicates the ips of servers.

The main configuration file for our experiments can be found in \*.ini, and the execution scripts can be found in run\*.sh
