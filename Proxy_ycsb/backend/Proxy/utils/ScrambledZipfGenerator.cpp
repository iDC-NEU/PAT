#include "ScrambledZipfGenerator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace Proxy
{
namespace utils
{

// -------------------------------------------------------------------------------------
int64_t ScrambledZipfGenerator::rand()
{
   int64_t zipf_value = zipf_generator(gen);
   return min + (scalestore::utils::FNV::hash(zipf_value) % n);
}
// -------------------------------------------------------------------------------------
int64_t ScrambledZipfGenerator::rand(int64_t offset)
{
   int64_t zipf_value = zipf_generator(gen);
   return (min + ((scalestore::utils::FNV::hash(zipf_value + offset)) % n));
}

}  // namespace utils
}  // namespace scalestore
