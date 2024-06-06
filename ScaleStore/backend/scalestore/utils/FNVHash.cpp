#include "FNVHash.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace scalestore
{
namespace utils
// -------------------------------------------------------------------------------------
{
i64 FNV::hash(i64 val)
{
   // from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
   i64 hash_val = FNV_OFFSET_BASIS_64;
   for (int i = 0; i < 8; i++) {
      i64 octet = val & 0x00ff;
      val = val >> 8;

      hash_val = hash_val ^ octet;
      hash_val = hash_val * FNV_PRIME_64;
   }
   return hash_val;
}
// -------------------------------------------------------------------------------------
}  // namespace utils
}  // namespace scalestore
