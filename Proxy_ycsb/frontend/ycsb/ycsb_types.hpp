#include "../backend/Proxy/utils/RandomGenerator.hpp"
#include "../backend/Proxy/utils/ScrambledZipfGenerator.hpp"
#include "../backend/Proxy/utils/Time.hpp"
#include "PerfEvent.hpp"

// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
DEFINE_uint32(YCSB_read_ratio, 50, "");
DEFINE_bool(YCSB_all_workloads, false, "Execute all workloads i.e. 50 95 100 ReadRatio on same tree");
DEFINE_int64(YCSB_tuple_count, 1, " Tuple count in");
DEFINE_double(YCSB_zipf_factor, 0.0, "Default value according to spec");
DEFINE_double(YCSB_run_for_seconds, 10.0, "");
DEFINE_bool(YCSB_partitioned, false, "");
DEFINE_bool(YCSB_warm_up, false, "");
DEFINE_bool(YCSB_record_latency, false, "");
DEFINE_bool(YCSB_all_zipf, false, "");
DEFINE_bool(YCSB_local_zipf, false, "");
DEFINE_bool(YCSB_flush_pages, false, "");
// -------------------------------------------------------------------------------------
using u64 = uint64_t;
using u8 = uint8_t;
// -------------------------------------------------------------------------------------
static constexpr uint64_t BTREE_ID = 0;
static constexpr uint64_t BARRIER_ID = 1;
// -------------------------------------------------------------------------------------
template <u64 size>
struct BytesPayload
{
    u8 value[size];
    BytesPayload() = default;
    bool operator==(BytesPayload &other) { return (std::memcmp(value, other.value, sizeof(value)) == 0); }
    bool operator!=(BytesPayload &other) { return !(operator==(other)); }
    // BytesPayload(const BytesPayload& other) { std::memcpy(value, other.value, sizeof(value)); }
    // BytesPayload& operator=(const BytesPayload& other)
    // {
    // std::memcpy(value, other.value, sizeof(value));
    // return *this;
    // }
};
// -------------------------------------------------------------------------------------
struct Partition
{
    int64_t begin;
    int64_t end;
};

using K = int64_t;
using V = BytesPayload<128>;
// [0, n)
Integer rnd(Integer n)
{
   return Proxy::utils::RandomGenerator::getRand(0, n);
}

Integer urand(Integer low, Integer high)
{
   return rnd(high - low + 1) + low;
}

auto partition = [](int id, int64_t participants, int64_t N) -> Partition
{
   const int64_t blockSize = N / participants;
   auto begin = id * blockSize;
   auto end = begin + blockSize;
   if (id == participants - 1)
      end = N;
   return {.begin = begin, .end = end};
};