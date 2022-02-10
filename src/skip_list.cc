#include "../include/skip_list.h"

SkipList::SkipList() {
  size_ = 0;
  file_size_ = kSSTHeaderSize + kBloomFilterSize;  // header and bloom filter
  head_ = std::make_shared<Node>();
}

SkipList::~SkipList() {
  // `p` points to the start of the level.
  NodeSPtr p = head_;
  while (p) {
    // `q` points to current level.
    NodeSPtr q = p;
    p = p->down_;
    while (q) {
      if (q->left_) {
        q->left_.reset();
      }
      q = q->right_;
    }
  }
}

__attribute__((unused)) size_t SkipList::Size() const { return size_; }

__attribute__((unused)) size_t SkipList::FileSize() const { return file_size_; }

SkipList::Value *SkipList::Get(const Key &key) const {
  if (!bloom_filter_.IsProbablyPresent(key)) {
    return nullptr;
  }

  NodeSPtr node = NodeByKey(key);

  if (node) {
    return &node->value_;
  }
  return nullptr;
}

void SkipList::Put(const Key key, const Value &value) {
  std::stack<NodeSPtr> path_stack;
  NodeSPtr p = head_;
  while (p) {
    while (p->right_ && p->right_->key_ < key) {
      p = p->right_;
    }
    path_stack.push(p);
    p = p->down_;
  }

  // 替换，_size不变，_fileSize改变
  NodeSPtr node_to_insert =
      path_stack.empty() ? nullptr : path_stack.top()->right_;
  if (node_to_insert && node_to_insert->key_ == key) {
    // 修改文件大小， 不用修改filter
    int fileSizeDifference =
        ComputeFileSizeChange(node_to_insert->value_, value);
    if (file_size_ + fileSizeDifference > kMaxSSTableSize) {
      throw MemTableFull();
    }
    // deletion也要加到filter， 这样才可以找到删除记录
    bloom_filter_.Put(key);
    file_size_ += fileSizeDifference;

    ComputeFileSizeChange(node_to_insert->value_, value);
    while (!path_stack.empty()) {
      NodeSPtr left_to_the_replaced = path_stack.top();
      if (!left_to_the_replaced->right_ ||
          left_to_the_replaced->right_->key_ != key) {
        return;
      }
      path_stack.pop();
      left_to_the_replaced->right_->value_ = value;
    }
    return;
  }

  // Insertion causes both `size_` and `file_size_` to change.
  int fileSizeDifference = ComputeFileSizeChange(value);
  if (file_size_ + fileSizeDifference > kMaxSSTableSize) {
    throw MemTableFull();
  }
  bloom_filter_.Put(key);
  ++size_;
  file_size_ += fileSizeDifference;
  // 插入且当前层依然存在，_size和_fileSize都增加
  bool insertUp = true;
  NodeSPtr downNode = nullptr;
  while (insertUp && !path_stack.empty()) {
    NodeSPtr insert = path_stack.top();
    path_stack.pop();
    insert->right_ = std::make_shared<Node>(key, value, insert, insert->right_,
                                            downNode);  // add新结点
    downNode = insert->right_;
    if (downNode->right_) {
      downNode->right_->left_ = downNode;
    }
    insertUp = ShouldInsertUp();
  }
  // 已经插到顶层，但可能要继续插入
  while (insertUp) {
    //插入新的头结点，加层
    NodeSPtr oldHead = head_;
    head_ = std::make_shared<Node>();
    head_->right_ =
        std::make_shared<Node>(key, value, head_, nullptr, downNode);
    downNode = head_->right_;
    head_->down_ = oldHead;
    insertUp = ShouldInsertUp();
  }
}

bool SkipList::Del(const Key key) {
  // Find the top level of the "tower".
  NodeSPtr top_node = NodeByKey(key);

  // Hitting a deletion mark also indicates not found.
  if (top_node == nullptr || top_node->value_ == kDeletionMark) {
    return false;
  }

  int decremented_file_size = ComputeFileSizeChange(top_node->value_);
  file_size_ -= decremented_file_size;
  --size_;

  // Delete nodes downward.
  NodeSPtr old_node;
  while (top_node) {
    old_node = top_node;
    top_node->left_->right_ = top_node->right_;
    if (top_node->right_) {
      top_node->right_->left_ = top_node->left_;
    }
    top_node = top_node->down_;
    old_node.reset();
  }

  // Delete extraneous levels.
  while (head_->down_ && !head_->right_) {
    NodeSPtr oldHead = head_;
    head_ = head_->down_;
    oldHead.reset();
  }

  return true;
}

void SkipList::Reset() {
  size_ = 0;
  file_size_ = kSSTHeaderSize + kBloomFilterSize;
  bloom_filter_.Reset();

  // p points to the start of the level
  NodeSPtr p = head_;
  while (p) {
    // q points to current level
    NodeSPtr q = p;
    p = p->down_;
    while (q) {
      if (q->left_) {
        q->left_.reset();
      }
      q = q->right_;
    }
  }

  head_ = std::make_shared<Node>();
}

bool SkipList::ShouldInsertUp() { return rand() & 1; }

/**
 * @Description: Compute the file size change for a replacement of value
 *               The return type is int, but it should not overflow, because the
 * index is not that large
 * @param old_value: The old value to be replaced
 * @param new_value: The new value
 * @return: Change of file Size in bytes
 */
inline int SkipList::ComputeFileSizeChange(const Value &old_value,
                                           const Value &new_value) {
  return (int)new_value.size() - (int)old_value.size();
}

/**
 * @Description: Compute the file size change for a value insertion
 * @param value: The new Value
 * @return: Change of file Size in bytes
 */
inline int SkipList::ComputeFileSizeChange(const Value &value) {
  return (int)kIndexSizePerValue + (int)value.size();
}

SkipList::NodeSPtr SkipList::NodeByKey(const Key &key) const {
  NodeSPtr node = head_;
  while (node) {
    while (node->right_ && node->right_->key_ < key) node = node->right_;
    if (node->right_ && key == node->right_->key_) return node->right_;
    node = node->down_;
  }
  return nullptr;
}

/**
 * @Description: Write the content of memory to disk in an SST.
 * @param timestamp: The timestamp of the SST.
 * @param sst_no: The fileName of the SST.
 * @param dir: Base directory to store files in.
 * @return: The in-memory representation of SST that is written to disk.
 */
SSTableSPtr SkipList::ToFile(const Timestamp timestamp, uint64_t sst_no,
                             const std::string &dir) const {
  std::string level0_path = dir + "/level-0";
  std::string file_path = level0_path + "/" + std::to_string(sst_no) + ".sst";

  Key min_key = MinKey();
  Key max_key = MaxKey();

  if (!utils::DirExists(level0_path)) {
    utils::Mkdir(level0_path.c_str());
  }

  std::ofstream sst_file(file_path, std::ios::out | std::ios::binary);

  // Write header.
  sst_file.write((char *)&timestamp, 8)
      .write((char *)&size_, 8)
      .write((char *)&min_key, 8)
      .write((char *)&max_key, 8);

  SSTableSPtr sst_ptr = std::make_shared<SSTable>(file_path, timestamp);
  sst_ptr->num_keys_ = size_;
  sst_ptr->min_key_ = min_key;
  sst_ptr->max_key_ = max_key;

  // Write bloom filter.
  bloom_filter_.ToFile(sst_file);
  sst_ptr->bloom_filter_ = bloom_filter_;

  // offset = header + bloom filter + _size * (key + offset)
  size_t offset =
      kSSTHeaderSize + kBloomFilterSize + size_ * kIndexSizePerValue;
  NodeSPtr node_for_key = BottomHead()->right_;
  NodeSPtr node_for_value = node_for_key;

  while (node_for_key) {
    sst_file.write((char *)&node_for_key->key_, 8).write((char *)&offset, 4);

    sst_ptr->keys_.emplace_back(node_for_key->key_);
    sst_ptr->offset_.emplace_back(offset);

    offset += node_for_key->value_.size();
    node_for_key = node_for_key->right_;
  }

  while (node_for_value) {
    Value &value = node_for_value->value_;
    size_t length = value.size();
    const char *str = value.c_str();
    sst_file.write(str, length);
    node_for_value = node_for_value->right_;
  }

  sst_ptr->file_size_ = offset;

  sst_file.close();

  return sst_ptr;
}

inline SkipList::Key SkipList::MinKey() const {
  NodeSPtr node_ptr = BottomHead();
  node_ptr = node_ptr->right_;
  return node_ptr ? node_ptr->key_ : std::numeric_limits<Key>::quiet_NaN();
}

SkipList::Key SkipList::MaxKey() const {
  // Also take into account deleted keys.
  NodeSPtr node_ptr = head_;

  while (node_ptr->down_) {
    while (node_ptr->right_) {
      node_ptr = node_ptr->right_;
    }
    node_ptr = node_ptr->down_;
  }

  while (node_ptr->right_) {
    node_ptr = node_ptr->right_;
  }

  return node_ptr ? node_ptr->key_ : std::numeric_limits<Key>::quiet_NaN();
}

/**
 * @Description: The utility function that gets the head of the bottom level of
 * the skip list.
 * @return: Pointer to the header node.
 */
SkipList::NodeSPtr SkipList::BottomHead() const {
  NodeSPtr node = head_;
  while (node->down_) {
    node = node->down_;
  }
  return node;
}
