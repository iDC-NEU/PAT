#pragma once
// -------------------------------------------------------------------------------------
#include "scalestore/Config.hpp"
#include "scalestore/ScaleStore.hpp"
// -------------------------------------------------------------------------------------
#include <cassert>
#include <cstdint>
#include <cstring>
#include <map>
#include <string>
#include <gflags/gflags.h>

DEFINE_uint32(YCSB_read_ratio, 50, "");
DEFINE_bool(YCSB_all_workloads, false, "Execute all workloads i.e. 50 95 100 ReadRatio on same tree");
DEFINE_int64(YCSB_tuple_count, 1000000, " Tuple count in");
DEFINE_double(YCSB_zipf_factor, 0.0, "Default value according to spec");
DEFINE_double(YCSB_run_for_seconds, 10.0, "");
DEFINE_bool(YCSB_partitioned, false, "");
DEFINE_bool(YCSB_warm_up, false, "");
DEFINE_bool(YCSB_record_latency, true, "");
DEFINE_bool(YCSB_all_zipf, false, "");
DEFINE_bool(YCSB_local_zipf, false, "");
DEFINE_bool(YCSB_flush_pages, false, "");
// -------------------------------------------------------------------------------------
using u64 = uint64_t;
using i64 = int64_t;
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
using K = int64_t;
using V = BytesPayload<128>;
// -------------------------------------------------------------------------------------
struct Partition
{
    uint64_t begin;
    uint64_t end;
};
// -------------------------------------------------------------------------------------
struct YCSB_workloadInfo : public scalestore::profiling::WorkloadInfo
{
    std::string experiment;
    int64_t elements;
    uint64_t readRatio;
    double zipfFactor;
    std::string zipfOffset;
    uint64_t timestamp = 0;

    YCSB_workloadInfo(std::string experiment, int64_t elements, uint64_t readRatio, double zipfFactor, std::string zipfOffset)
        : experiment(experiment), elements(elements), readRatio(readRatio), zipfFactor(zipfFactor), zipfOffset(zipfOffset)
    {
    }

    virtual std::vector<std::string> getRow()
    {
        return {
            experiment,
            std::to_string(elements),
            std::to_string(readRatio),
            std::to_string(zipfFactor),
            zipfOffset,
            std::to_string(timestamp++),
        };
    }

    virtual std::vector<std::string> getHeader()
    {
        return {"workload", "elements", "read ratio", "zipfFactor", "zipfOffset", "timestamp"};
    }

    virtual void csv(std::ofstream &file) override
    {
        file << experiment << " , ";
        file << elements << " , ";
        file << readRatio << " , ";
        file << zipfFactor << " , ";
        file << zipfOffset << " , ";
        file << timestamp << " , ";
    }
    virtual void csvHeader(std::ofstream &file) override
    {
        file << "Workload"
             << " , ";
        file << "Elements"
             << " , ";
        file << "ReadRatio"
             << " , ";
        file << "ZipfFactor"
             << " , ";
        file << "ZipfOffset"
             << " , ";
        file << "Timestamp"
             << " , ";
    }
};
