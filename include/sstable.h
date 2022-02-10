#ifndef LSM_SSTABLE_H
#define LSM_SSTABLE_H

#include "bloom_filter.h"
#include "common.h"

class SSTable;

typedef std::shared_ptr<SSTable> SSTableSPtr;
typedef std::vector<SSTableSPtr> Level;
typedef std::shared_ptr<std::vector<SSTableSPtr>> LevelSPtr;

class SkipList;

class SSTable {
  friend std::ostream &operator<<(std::ostream &, const SSTable &);

  friend bool SSTableComparatorForSort(const SSTableSPtr &t1,
                                       const SSTableSPtr &t2);

  friend bool SSTableComparatorForSort0(const SSTableSPtr &t1,
                                        const SSTableSPtr &t2);

  friend bool operator<(const std::pair<SSTableSPtr, size_t> &,
                        const std::pair<SSTableSPtr, size_t> &);

  friend bool operator<(const SSTableSPtr &, const SSTableSPtr &);

  friend class SkipList;

  friend class KVStore;

 private:
  std::string file_path_;

  size_t file_size_;

  Timestamp timestamp_;

  size_t num_keys_;

  uint64_t min_key_;

  uint64_t max_key_;

  BloomFilter<uint64_t> bloom_filter_;

  std::vector<uint64_t> keys_;

  std::vector<size_t> offset_;

  size_t BinarySearch(uint64_t key) const;

  std::shared_ptr<std::vector<StringSPtr>> Values() const;

 public:
  SSTable() = default;

  SSTable(const std::string &path, Timestamp timestamp);

  static SSTable *FromFile(const std::string &file_path);

  bool IsProbablyPresent(uint64_t) const;

  std::shared_ptr<std::string> ValueByKey(uint64_t) const;

  std::shared_ptr<std::string> ValueByIndex(size_t idx) const;

  bool Contains(uint64_t key) const;

  uint64_t MinKey() const;

  uint64_t MaxKey() const;

  void ToFile(std::vector<std::shared_ptr<std::string>> &values);
};

inline bool SSTableComparatorForSort(const SSTableSPtr &t1,
                                     const SSTableSPtr &t2) {
  return t1->min_key_ < t2->min_key_;
}

inline bool SSTableComparatorForSort0(const SSTableSPtr &t1,
                                      const SSTableSPtr &t2) {
  return t1->timestamp_ < t2->timestamp_;
}

inline bool operator<(const std::pair<SSTableSPtr, size_t> &p1,
                      const std::pair<SSTableSPtr, size_t> &p2) {
  SSTableSPtr t1 = p1.first;
  SSTableSPtr t2 = p2.first;
  return t1->keys_[p1.second] > t2->keys_[p2.second] ||
         (t1->keys_[p1.second] == t2->keys_[p2.second] &&
          t1->timestamp_ < t2->timestamp_);
}

inline bool operator<(const SSTableSPtr &t1, const SSTableSPtr &t2) {
  return t1->min_key_ < t2->min_key_;
}

#endif  // LSM_SSTABLE_H
