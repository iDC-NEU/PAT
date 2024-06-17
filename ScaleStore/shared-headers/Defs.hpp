#pragma once
// -------------------------------------------------------------------------------------
#include <cstdint>
#include <iostream>
#include <vector>
#include <string>
#include <csignal>
#include <immintrin.h>
#include <experimental/source_location>
#include <sstream>
// -------------------------------------------------------------------------------------
#define GDB() std::raise(SIGINT);
// -------------------------------------------------------------------------------------
#define BACKOFF()                          \
   int const max = 4;                      \
   if (true)                               \
   {                                       \
      for (int i = mask; i; --i)           \
      {                                    \
         _mm_pause();                      \
      }                                    \
      mask = mask < max ? mask << 1 : max; \
   }

enum LOG_LEVEL
{
   RELEASE = 0,
   CSV = 1,
   TEST = 2,
   TRACE = 3,
};

// -------------------------------------------------------------------------------------
// ensure is similar to assert except that it is never out compiled
#define always_check(e)                                                   \
   do                                                                     \
   {                                                                      \
      if (__builtin_expect(!(e), 0))                                      \
      {                                                                   \
         std::stringstream ss;                                            \
         ss << __func__ << " in " << __FILE__ << ":" << __LINE__ << '\n'; \
         ss << " msg: " << std::string(#e);                               \
         throw std::runtime_error(ss.str());                              \
      }                                                                   \
   } while (0)

#define ENSURE_ENABLED 1
#ifdef ENSURE_ENABLED
#define ensure(e) always_check(e);
#else
#define ensure(e) \
   do             \
   {              \
   } while (0);
#endif

template <typename T>
inline void DO_NOT_OPTIMIZE(T const &value)
{
#if defined(__clang__)
   asm volatile("" : : "g"(value) : "memory");
#else
   asm volatile("" : : "i,r,m"(value) : "memory");
#endif
}

#define posix_check(expr) \
   if (!(expr))           \
   {                      \
      perror(#expr);      \
      assert(false);      \
   }

using u64 = uint64_t;
using i64 = int64_t;
using s32 = int32_t;
using Integer = int32_t;
constexpr size_t CACHE_LINE = 64;
constexpr size_t MAX_NODES = 64;   // only supported due to bitmap
constexpr size_t PARTITIONS = 64;  // partitions for partitioned queue
constexpr size_t BATCH_SIZE = 128; // for partitioned queue

constexpr auto ACTIVE_LOG_LEVEL = LOG_LEVEL::RELEASE;

const std::vector<std::vector<std::string>> NODES{//the last of row is proxy
    {""},                                                                                              // 0 to allow direct offset
    {"10.0.0.87", "10.0.0.87"},                                                                                  // 1
    {"10.0.0.87", "10.0.0.88", "10.0.0.89"},                                                                     // 2
    {"10.0.0.87", "10.0.0.88", "10.0.0.90", "10.0.0.89"},                                              // 3
    {"10.0.0.87", "10.0.0.88", "10.0.0.90", "10.0.0.91", "10.0.0.89"},                              // 4
    {"10.0.0.87", "10.0.0.88", "10.0.0.90", "10.0.0.91", "10.0.0.92", "10.0.0.89"},                  // 5
    {"10.0.0.87", "10.0.0.88", "10.0.0.90", "10.0.0.91", "10.0.0.92", "10.0.0.93", "10.0.0.89"},  // 6
};
const std::vector<std::vector<uint64_t>> NODESport{
    {},                                                                                              // 0 to allow direct offset
    {1500, 1401},                                                                                  // 1
    {1400, 1500, 1401},
    {1400, 1500, 1402, 1401},
    {1400, 1500, 1402, 1502, 1401},
    {1400, 1500, 1402, 1502, 1602, 1401},          
    {1400, 1500, 1402, 1502, 1602, 1702, 1401},                                                              // 2
};


const uint32_t sqlLength = 1000;
const uint32_t txnKeyListLength = 50;
const uint32_t KeyListLength = 10;
static std::string rdmaPathRecv = "/sys/class/infiniband/mlx5_0/ports/1/counters/port_rcv_data";
static std::string rdmaPathXmit = "/sys/class/infiniband/mlx5_0/ports/1/counters/port_xmit_data";
using NodeID = uint64_t;
// -------------------------------------------------------------------------------------
constexpr uint64_t PAGEID_BITS_NODEID = 8;
constexpr uint64_t NODEID_MASK = (~uint64_t(0) >> 8);
struct PID
{
   // -------------------------------------------------------------------------------------
   uint64_t id = 0;
   PID() = default;
   explicit PID(uint64_t pid_id) : id(pid_id){};
   constexpr PID(uint64_t owner, uint64_t id) : id(((owner << ((sizeof(uint64_t) * 8) - PAGEID_BITS_NODEID))) | id){};
   NodeID getOwner() { return NodeID(id >> ((sizeof(uint64_t) * 8 - PAGEID_BITS_NODEID))); }
   uint64_t plainPID() { return (id & NODEID_MASK); }
   operator uint64_t() { return id; }
   inline PID &operator=(const uint64_t &other)
   {
      id = other;
      return *this;
   }
   friend bool operator==(const PID &lhs, const PID &rhs) { return (lhs.id == rhs.id); }
   friend bool operator!=(const PID &lhs, const PID &rhs) { return (lhs.id != rhs.id); }
   friend bool operator>=(const PID &lhs, const PID &rhs) { return (lhs.id >= rhs.id); }
   friend bool operator<=(const PID &lhs, const PID &rhs) { return (lhs.id <= rhs.id); }
   friend bool operator<(const PID &lhs, const PID &rhs) { return (lhs.id < rhs.id); }
   friend bool operator>(const PID &lhs, const PID &rhs) { return (lhs.id > rhs.id); }
   // -------------------------------------------------------------------------------------
};

struct TxnNode
{
   int64_t key;
   bool is_read_only;
   int weight;
   // 构造函数
   TxnNode(){};
   TxnNode(int64_t k, bool read_only, int w)
       : key(k), is_read_only(read_only), weight(w) {}
};

constexpr NodeID EMPTY_NODEID(~uint64_t(0));
constexpr PID EMPTY_PID((~uint8_t(0)), (~uint64_t(0)));
constexpr uint64_t EMPTY_PVERSION((~uint64_t(0)));
constexpr uint64_t EMPTY_EPOCH((~uint64_t(0)));
// -------------------------------------------------------------------------------------
// helper functions
// -------------------------------------------------------------------------------------

struct Helper
{
   static unsigned long nextPowerTwo(unsigned long v)
   {
      v--;
      v |= v >> 1;
      v |= v >> 2;
      v |= v >> 4;
      v |= v >> 8;
      v |= v >> 16;
      v++;
      return v;
   }

   static bool powerOfTwo(u64 n)
   {
      return n && (!(n & (n - 1)));
   }
};

// -------------------------------------------------------------------------------------
// Catalog
// -------------------------------------------------------------------------------------
constexpr NodeID CATALOG_OWNER{0};
constexpr PID CATALOG_PID(CATALOG_OWNER, 0);
