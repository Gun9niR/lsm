#ifndef LSM_BLOOMFILTER_H
#define LSM_BLOOMFILTER_H

#include "MurmurHash3.h"
#include "consts.h"

template<typename Key>
class BloomFilter {
public:
    bool filter[BLOOM_FILTER_SIZE] = {false};

public:
    BloomFilter();

    void put(const Key &);

    bool isProbablyPresent(const Key &) const;

    // write the array into a file
    void toFile(ofstream &) const;

    // read array from a file
    void fromFile(ifstream &);

    // set filter array to all false
    void reset();
};

template<typename Key>
BloomFilter<Key>::BloomFilter() = default;

template<typename Key>
void BloomFilter<Key>::put(const Key& key) {
    unsigned int hash[4] = {0};
    MurmurHash3_x64_128(&key, sizeof(key), 1, hash);

    filter[hash[0] % BLOOM_FILTER_SIZE] =
        filter[hash[1] % BLOOM_FILTER_SIZE] =
            filter[hash[2] % BLOOM_FILTER_SIZE] =
                filter[hash[3] % BLOOM_FILTER_SIZE] = true;
}

template<typename Key>
bool BloomFilter<Key>::isProbablyPresent(const Key& key) const {
    unsigned int hash[4] = {0};
    MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
    return filter[hash[0] % BLOOM_FILTER_SIZE] &&
            filter[hash[1] % BLOOM_FILTER_SIZE] &&
            filter[hash[2] % BLOOM_FILTER_SIZE] &&
            filter[hash[3] % BLOOM_FILTER_SIZE];
}

template<typename Key>
inline void BloomFilter<Key>::toFile(ofstream &ssTable) const {
    ssTable.write((char *)filter, BLOOM_FILTER_SIZE);
}

template<typename Key>
inline void BloomFilter<Key>::reset() {
    memset(filter, 0, BLOOM_FILTER_SIZE);
}

template<typename Key>
inline void BloomFilter<Key>::fromFile(ifstream &ssTable) {
    ssTable.read((char *)filter, BLOOM_FILTER_SIZE);
}

#endif