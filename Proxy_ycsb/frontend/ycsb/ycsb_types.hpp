#include "../backend/Proxy/utils/RandomGenerator.hpp"
#include "../backend/Proxy/utils/ScrambledZipfGenerator.hpp"
#include "../backend/Proxy/utils/Time.hpp"
#include "PerfEvent.hpp"

// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
DEFINE_uint32(YCSB_read_ratio, 100, "");
DEFINE_bool(YCSB_all_workloads, false, "Execute all workloads i.e. 50 95 100 ReadRatio on same tree");
DEFINE_uint64(YCSB_tuple_count, 1, " Tuple count in");
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