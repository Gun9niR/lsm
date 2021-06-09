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
    friend bool SSTableComparatorForSort(const SSTablePtr &t1, const SSTablePtr &t2);
    friend bool SSTableComparatorForSort0(const SSTablePtr &t1, const SSTablePtr &t2);
    friend bool operator<(const pair<SSTablePtr, size_t> &, const pair<SSTablePtr, size_t> &);
    friend bool operator<(const SSTablePtr &, const SSTablePtr &);
    friend class SkipList<Key, String>;
    friend class KVStore;

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

    shared_ptr<vector<StringPtr>> getAllValues() const;
public:
    SSTable();

    explicit SSTable(const String &);

    bool isProbablyPresent(const Key &) const;

    shared_ptr<String> get(const Key &) const;

    shared_ptr<String> getByIdx(size_t idx) const;

    bool contains(const Key&) const;

    Key getMinKey() const;

    Key getMaxKey() const;

    void toFile(vector<shared_ptr<String>>&);
};

inline bool SSTableComparatorForSort(const SSTablePtr& t1, const SSTablePtr& t2) {
    return t1->minKey < t2->minKey;
}

inline bool SSTableComparatorForSort0(const SSTablePtr& t1, const SSTablePtr& t2) {
    return t1->timeStamp < t2->timeStamp;
}

inline bool operator<(const pair<SSTablePtr, size_t>& p1, const pair<SSTablePtr, size_t>& p2) {
    SSTablePtr t1 = p1.first;
    SSTablePtr t2 = p2.first;
    return t1->keys[p1.second] > t2->keys[p2.second] ||
            (t1->keys[p1.second] == t2->keys[p2.second] && t1->timeStamp < t2->timeStamp);
}

inline bool operator<(const SSTablePtr &t1, const SSTablePtr &t2) {
    return t1->minKey < t2->minKey;
}

#endif //LSM_SSTABLE_H
