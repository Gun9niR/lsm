#ifndef LSM_BLOOM_FILTER_H
#define LSM_BLOOM_FILTER_H

#include "common.h"
#include "murmur_hash_3.h"

template <typename Key>
class BloomFilter {
public:
    BloomFilter();

    void Put(const Key &);

    bool IsProbablyPresent(const Key &) const;

    // write the array into a file
    void ToFile(std::ofstream &) const;

    /// Restore the filter from a file.
    /// The file must contain exclusively the bloom filter.
    void FromFile(std::ifstream &);

    // Set filter array to all false.
    void Reset();

 private:
  bool filter_[kBloomFilterSize] = {false};
};

template <typename Key>
BloomFilter<Key>::BloomFilter() = default;

template <typename Key>
void BloomFilter<Key>::Put(const Key &key) {
  unsigned int hash[4] = {0};
  MurmurHash3_x64_128(&key, sizeof(key), 1, hash);

    filter_[hash[0] % kBloomFilterSize] = filter_[hash[1] % kBloomFilterSize] =
    filter_[hash[2] % kBloomFilterSize] =
    filter_[hash[3] % kBloomFilterSize] = true;
}

template <typename Key>
bool BloomFilter<Key>::IsProbablyPresent(const Key &key) const {
  unsigned int hash[4] = {0};
  MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
  return filter_[hash[0] % kBloomFilterSize] &&
         filter_[hash[1] % kBloomFilterSize] &&
         filter_[hash[2] % kBloomFilterSize] &&
         filter_[hash[3] % kBloomFilterSize];
}

template <typename Key>
inline void BloomFilter<Key>::ToFile(std::ofstream &sst_file) const {
  sst_file.write((char *)filter_, kBloomFilterSize);
}

template <typename Key>
inline void BloomFilter<Key>::Reset() {
  memset(filter_, 0, kBloomFilterSize);
}

template <typename Key>
inline void BloomFilter<Key>::FromFile(std::ifstream &sst_file) {
  sst_file.read((char *)filter_, kBloomFilterSize);
}

#endif