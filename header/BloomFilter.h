#ifndef LSM_BLOOMFILTER_H
#define LSM_BLOOMFILTER_H

#include "MurmurHash3.h"
#include "consts.h"

template<typename Key>
class BloomFilter {
public:
    bool filter[MAX_SSTABLE_SIZE] = {false};

public:
    BloomFilter();

    void put(const Key &);

    bool isProbablyPresent(const Key &) const;

    // write the array into a file
    void toFile(ofstream &);

    // read array from a file
    void fromFile(ifstream &);

    void reset();
};

template<typename Key>
BloomFilter<Key>::BloomFilter() = default;

template<typename Key>
void BloomFilter<Key>::put(const Key& key) {
    unsigned int hash[4] = {0};
    MurmurHash3_x64_128(&key, 8, 1, hash);
    for (auto i : hash) {
        filter[i % BLOOM_FILTER_SIZE] = true;
    }
}

template<typename Key>
bool BloomFilter<Key>::isProbablyPresent(const Key& key) const {
    unsigned int hash[4] = {0};
    MurmurHash3_x64_128(&key, 8, 1, hash);
    for (auto i: hash) {
        if (!filter[i % BLOOM_FILTER_SIZE]) {
            return false;
        }
    }
    return true;
}

template<typename Key>
void BloomFilter<Key>::toFile(ofstream &ssTable) {
    ssTable.write((char *)filter, BLOOM_FILTER_SIZE);
}

template<typename Key>
void BloomFilter<Key>::reset() {
    memset(filter, 0, BLOOM_FILTER_SIZE);
}

template<typename Key>
void BloomFilter<Key>::fromFile(ifstream &ssTable) {
    ssTable.read((char *)filter, BLOOM_FILTER_SIZE);
}

#endif