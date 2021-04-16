#ifndef LSM_SSTABLE_H
#define LSM_SSTABLE_H

#include "consts.h"
#include "BloomFilter.h"

class SSTable;
typedef shared_ptr<SSTable> SSTablePtr;
typedef vector<SSTablePtr> Level;
typedef shared_ptr<vector<SSTablePtr>> LevelPtr;

template<typename Key, typename Value>
class SkipList;

class SSTable {
    friend ostream& operator<<(ostream &, const SSTable &);
    friend bool SSTableComparator(const SSTablePtr &, const SSTablePtr &);
    friend class SkipList<Key, String>;

private:
    String fullPath;

    size_t fileSize;

    TimeStamp timeStamp;

    Size numOfKeys;

    Key minKey;

    Key maxKey;

    BloomFilter<Key> bloomFilter;

    vector<Key> keys;

    vector<size_t> offset;

    size_t binarySearch(const Key&) const;

public:
    SSTable() = default;

    explicit SSTable(const String &);

    bool isProbablyPresent(const Key &) const;

    shared_ptr<String> get(const Key &) const;

    bool contains(const Key&) const;

    Key getMinKey() const;

    Key getMaxKey() const;
};

inline bool SSTableComparator(const SSTablePtr& t1, const SSTablePtr& t2) {
    return t1->timeStamp < t2->timeStamp;
}

#endif //LSM_SSTABLE_H
