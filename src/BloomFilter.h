#ifndef LSM_BLOOMFILTER_H
#define LSM_BLOOMFILTER_H

#include <vector>
#include "MurmurHash3.h"
#include "consts.h"

using std::vector;

template<typename Key>
class BloomFilter {
private:
    vector<bool> filter;

public:
    BloomFilter();

    void put(const Key &);

    bool isProbablyPresent(const Key &);
};

template<typename Key>
BloomFilter<Key>::BloomFilter(): filter(BLOOM_FILTER_SIZE, false) { }

template<typename Key>
void BloomFilter<Key>::put(const Key& key) {
    unsigned int hash[4] = {0};
    MurmurHash3_x64_128(&key, 8, 1, hash);
    for (auto i : hash) {
        filter[i % BLOOM_FILTER_SIZE] = true;
    }
}

template<typename Key>
bool BloomFilter<Key>::isProbablyPresent(const Key& key) {
    unsigned int hash[4] = {0};
    MurmurHash3_x64_128(&key, 8, 1, hash);
    for (auto i: hash) {
        if (!filter[i % BLOOM_FILTER_SIZE]) {
            return false;
        }
    }
    return true;
}

#endif