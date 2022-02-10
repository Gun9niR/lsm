#ifndef LSM_SKIP_LIST_H
#define LSM_SKIP_LIST_H

#include <fstream>
#include <iostream>
#include <stack>
#include <utility>

#include "bloom_filter.h"
#include "exception.h"
#include "sstable.h"
#include "utils.h"

class SkipList {
  typedef uint64_t Key;
  typedef std::string Value;

 public:
  SkipList();

  ~SkipList();

  __attribute__((unused)) size_t Size() const;

  __attribute__((unused)) size_t FileSize() const;

  Value *Get(const Key &key) const;

  void Put(Key key, const Value &value);

  bool Del(Key key);

  void Reset();

  SSTableSPtr ToFile(Timestamp timestamp, uint64_t sst_no,
                     const std::string &dir) const;

  bool IsEmpty() const { return size_ == 0; }

 private:
  class Node {
   public:
    Key key_;
    Value value_;
    std::shared_ptr<Node> left_, right_, down_;

   public:
    Node(const Key key, Value value, std::shared_ptr<Node> left,
         std::shared_ptr<Node> right, std::shared_ptr<Node> down)
        : key_(key),
          value_(std::move(value)),
          left_(std::move(left)),
          right_(std::move(right)),
          down_(std::move(down)) {}
    Node()
        : key_(0), value_(), left_(nullptr), right_(nullptr), down_(nullptr) {}
  };

  typedef std::shared_ptr<Node> NodeSPtr;

  static bool ShouldInsertUp();

  static int ComputeFileSizeChange(const Value &new_value,
                                   const Value &old_value);

  static int ComputeFileSizeChange(const Value &value);

  NodeSPtr NodeByKey(const Key &key) const;

  NodeSPtr BottomHead() const;

  Key MinKey() const;

  Key MaxKey() const;

  BloomFilter<Key> bloom_filter_;

  NodeSPtr head_;

  size_t size_;

  size_t file_size_;
};

#endif  // LSM_SKIP_LIST_H
