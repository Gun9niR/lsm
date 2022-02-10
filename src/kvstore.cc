#include "../include/kvstore.h"

#include <MacTypes.h>

#include <iostream>

/**
 * @Description: Construct KVStore object with given base directory
 * @param dir: Base directory, where all SSTs are stored
 */
KVStore::KVStore(const std::string &dir)
    : KVStoreAPI(dir), kDir(dir), timestamp_(1), sst_no_(1) {
  // Create the directory first.
  if (!utils::DirExists(dir)) {
    utils::Mkdir(dir.c_str());
  }

  std::vector<std::string> level_list;
  int num_level = utils::ScanDir(dir, level_list);

  std::sort(level_list.begin(), level_list.end());

  ssts_.resize(num_level);

  // If there is no persistent SST, just create one empty level in memory.
  if (!num_level) {
    ssts_.emplace_back(std::make_shared<Level>());
  }

  std::string dir_with_slash = dir + "/";
  for (int i = 0; i < num_level; ++i) {
    std::vector<std::string> file_list;
    int fileNum = utils::ScanDir(dir_with_slash + level_list[i], file_list);

    LevelSPtr level_ptr = std::make_shared<Level>(fileNum);

    std::string level_name_with_slash = dir_with_slash + level_list[i] + "/";
    for (int j = 0; j < fileNum; ++j) {
      std::string &file_name = file_list[j];
      size_t last_index = file_name.find_last_of('.');
      uint64_t file_sst_no = std::stoi(file_name.substr(0, last_index));
      sst_no_ = file_sst_no >= sst_no_ ? file_sst_no + 1 : sst_no_;

      auto sst_ptr =
          std::shared_ptr<SSTable>(SSTable::FromFile(level_name_with_slash + file_name));
      (*level_ptr)[j] = sst_ptr;

      if (sst_ptr->timestamp_ >= timestamp_) {
        timestamp_ = sst_ptr->timestamp_ + 1;
      }
    }

    // The first layer is unsorted, the rest is sorted by key range (disjoint).
    if (i) {
      sort(level_ptr->begin(), level_ptr->end(), SSTableComparatorForSort);
    } else {
      sort(level_ptr->begin(), level_ptr->end(), SSTableComparatorForSort0);
    }

    ssts_[i] = level_ptr;
  }
  Compaction();
#ifdef DEBUG
  cout << "========== Before  ==========" << endl;
  printSSTables();
#endif
}

/**
 * @Description: Destruct `KVStore` object, write the content of memory to disk.
 */
KVStore::~KVStore() {
  if (!mem_table_.IsEmpty()) {
    SSTableSPtr sst = mem_table_.ToFile(timestamp_, sst_no_++, kDir);
    ssts_[0]->emplace_back(sst);
    Compaction();
  }
}

/**
 * @Description: Insert/Update the key-value pair. No return values for
 * simplicity.
 * @param key: Key in the key-value pair.
 * @param s: Value in the key-value pair.
 */
void KVStore::Put(const uint64_t key, const std::string &s) {
  try {
    mem_table_.Put(key, s);
  } catch (const MemTableFull &) {
    SSTableSPtr ssTablePtr = mem_table_.ToFile(timestamp_, sst_no_++, kDir);
    ++timestamp_;
#ifdef DEBUG
    cout << "========== MEM TO DISK ==========" << endl;
    cout << *ssTablePtr << endl;
#endif
    mem_table_.Reset();
    mem_table_.Put(key, s);

    ssts_[0]->emplace_back(ssTablePtr);
    Compaction();
  }
}

/**
 * @Description: Find in KVStore by key
 * @param key: The key to find with
 * @return: the (string) value of the given key. Empty string indicates not
 * found.
 */
std::string KVStore::Get(uint64_t key) {
  std::string *value_in_mem = mem_table_.Get(key);
  if (value_in_mem) {
    return *value_in_mem == kDeletionMark ? "" : *value_in_mem;
  }

  // Not found in mem table, search in SST.
  for (const LevelSPtr &level_ptr : ssts_) {
    if (level_ptr == *ssts_.begin()) {
      // Sequential search in level-0.
      for (auto sst_rit = level_ptr->rbegin(); sst_rit != level_ptr->rend();
           ++sst_rit) {
        // Search a pointer in a `SSTable`. Return `nullptr` if not found.
        SSTableSPtr sst_ptr = *sst_rit;
        std::shared_ptr<std::string> val_ptr = sst_ptr->ValueByKey(key);
        if (val_ptr) {
          std::string value = *val_ptr;
          return (value == kDeletionMark) ? "" : value;
        }
      }
    } else {
      // For other levels, do binary search.
      SSTableSPtr sst_ptr = BinarySearch(level_ptr, key);
      if (sst_ptr) {
        std::shared_ptr<std::string> val_ptr = sst_ptr->ValueByKey(key);
        if (val_ptr) {
          std::string value = *val_ptr;
          return value == kDeletionMark ? "" : value;
        }
      }
    }
  }
  return "";
}

/**
 * @Description: Delete the given key-value pair if it exists.
 * @param key: The key to search with
 * @return: `false` iff the key is not found.
 */
bool KVStore::Del(uint64_t key) {
  // TODO: decouple deletion mark.
  // Find in mem table to see if the key is already deleted.
  bool is_in_memory = mem_table_.Del(key);
  bool is_deleted_in_memory = mem_table_.Get(key) != nullptr;
  // Insert deletion mark.
  Put(key, kDeletionMark);
  if (is_in_memory) {
    return true;
  } else if (is_deleted_in_memory) {
    return false;
  }

  for (const LevelSPtr &level_ptr : ssts_) {
    if (level_ptr == *ssts_.begin()) {
      // Do sequential search in level-0.
      for (auto sst_rit = level_ptr->rbegin(); sst_rit != level_ptr->rend();
           ++sst_rit) {
        SSTableSPtr sst_ptr = *sst_rit;
        std::shared_ptr<std::string> val_ptr = sst_ptr->ValueByKey(key);
        if (val_ptr) {
          std::string value = *val_ptr;
          return !(value == kDeletionMark);
        }
      }
    } else {
      // Do binary search in other levels.
      SSTableSPtr sst_ptr = BinarySearch(level_ptr, key);
      if (sst_ptr) {
        std::shared_ptr<std::string> val_ptr = sst_ptr->ValueByKey(key);
        if (val_ptr) {
          std::string value = *val_ptr;
          return !(value == kDeletionMark);
        }
      }
    }
    // Find in the next level.
  }
  return false;
}

/**
 * @Description: Resets the kvstore. All key-value pairs should be removed,
 *               including mem table and all SST files.
 */
void KVStore::Reset() {
  mem_table_.Reset();
  ssts_.clear();
  ssts_.emplace_back(std::make_shared<Level>());

  // Remove all SST files.
  std::vector<std::string> level_list;
  int num_level = utils::ScanDir(kDir, level_list);
  std::string dir_with_slash = kDir + "/";
  for (int i = 0; i < num_level; ++i) {
    std::vector<std::string> file_list;
    utils::ScanDir(dir_with_slash + level_list[i], file_list);

    std::string level_name_with_slash = dir_with_slash + level_list[i] + "/";
    for (std::string &file_name : file_list) {
      utils::Rmfile((level_name_with_slash + file_name).c_str());
    }

    utils::Rmdir((dir_with_slash + level_list[i]).c_str());
  }
}

/**
 * @Description: Search in a level for a key using binary search
 * @param level_ptr: Pointer to the level to search in
 * @param key: Key to search with
 * @return: Pointer to the SST that is found
 */
SSTableSPtr KVStore::BinarySearch(const LevelSPtr &level_ptr, uint64_t key) {
  long left = 0;
  long right = (long)level_ptr->size() - 1;
  while (left <= right) {
    long mid = left + ((right - left) >> 1);
    SSTableSPtr ret = (*level_ptr)[mid];
    if (ret->Contains(key)) {
      return ret;
    } else if (key < ret->MinKey()) {
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
  return nullptr;
}

/**
 * @Description: Debug utility function, print all SSTs cached in memory.
 */
__attribute__((unused)) void KVStore::PrintSSTables() const {
  int num_levels = (int)ssts_.size();
  for (int i = 0; i < num_levels; ++i) {
    std::cout << "Level " << i << std::endl;
    Level level = *ssts_[i];
    for (const SSTableSPtr &ssTablePtr : level) {
      std::cout << *ssTablePtr << std::endl;
    }
  }
}

/**
 * @Description: Handle Compaction for all levels, level-0 and other levels are
 * handled separately.
 */
void KVStore::Compaction() {
  // handle level0 special case
  if (ssts_[0]->size() > 2) {
    CompactionLevel0();
  }

  size_t num_levels = ssts_.size();
  size_t num_levels_to_iterate = num_levels - 1;
  for (size_t i = 1; i < num_levels_to_iterate; ++i) {
    if (ssts_[i]->size() > (2 << i)) {
      Compaction(i, i == num_levels_to_iterate - 1);
    }
  }

  // Check for the last level, just put the SSTs with the largest timestamps to
  // the next level
  auto last_level_ptr = ssts_[num_levels_to_iterate];
  if (last_level_ptr->size() > (2 << num_levels_to_iterate)) {
    std::string level_name = kDir + "/level-" + std::to_string(num_levels);
    if (!utils::DirExists(kDir + "/level")) {
      utils::Mkdir(level_name.c_str());
    }

    auto cur_level_discard_sst =
        SSTForCompaction(num_levels_to_iterate,
                         last_level_ptr->size() - (2 << num_levels_to_iterate));

    LevelSPtr newLevel = std::make_shared<Level>();

    for (const auto &sst : *cur_level_discard_sst) {
      std::string old_path = sst->file_path_;
      sst->file_path_ = level_name + "/" + std::to_string(sst_no_++) + ".sst";

      std::ifstream src(old_path, std::ios::binary);
      std::ofstream dst(sst->file_path_, std::ios::binary);
      dst << src.rdbuf();
      src.close();
      dst.close();
      utils::Rmfile(old_path.c_str());
      newLevel->emplace_back(sst);
    }
    ssts_.emplace_back(newLevel);
    ReconstructLevel(num_levels_to_iterate, cur_level_discard_sst);
  }
}

/**
 * @Description: Handle Compaction for levels other than level-0
 * @param level: The number of level that is overflowing currently
 * @param remove_deletion_mark: A flag that decides whether "~DELETED~"
 * should be removed It is true only when level is the level above the bottom
 * level
 */
void KVStore::Compaction(const size_t level, bool remove_deletion_mark) {
  LevelSPtr cur_level_ptr = ssts_[level];

  // Max number of SSTs of current level.
  size_t max_size = 2 << level;
  size_t num_sst_to_merge = cur_level_ptr->size() - max_size;

  // Step1: find the SSTs with the least time stamp.
  std::shared_ptr<std::set<SSTableSPtr>> cur_level_discard_sst =
      SSTForCompaction(level, num_sst_to_merge);

  for (const auto &sst_ptr : *cur_level_discard_sst) {
    // Step2: Iterate over these SSTs, find the overlapping sstables, Put
    // them in a vector
    LevelSPtr next_level_ptr = ssts_[level + 1];

    uint64_t min_key = sst_ptr->min_key_;
    uint64_t max_key = sst_ptr->max_key_;

    // SSTs in `overlap` is sorted in ascending order of key
    std::vector<SSTableSPtr> overlap;
    std::set<SSTableSPtr> next_level_discard;
    std::unordered_map<SSTableSPtr, std::shared_ptr<std::vector<StringSPtr>>>
        all_values;

    // Search for overlapping interval.
    long start_idx = LowerBound(next_level_ptr, min_key);
    size_t next_level_size = next_level_ptr->size();
    for (long i = start_idx; i < next_level_size; ++i) {
      SSTableSPtr next_level_sst_ptr = next_level_ptr->at(i);
      if (next_level_sst_ptr->min_key_ <= max_key) {
        overlap.emplace_back(next_level_sst_ptr);
        next_level_discard.insert(next_level_sst_ptr);
        all_values[next_level_sst_ptr] = next_level_sst_ptr->Values();
      } else {
        break;
      }
    }

    // Step3: merge that vector and this singe sstable, files are created along
    // the way but if there's no overlapping sst, just copy the file
    std::vector<SSTableSPtr> merge_res;

    if (overlap.empty()) {
      std::string oldPath = sst_ptr->file_path_;
      sst_ptr->file_path_ = kDir + "/level-" + std::to_string(level + 1) + "/" +
                            std::to_string(sst_no_++) + ".sst";

      std::ifstream src(oldPath, std::ios::binary);
      std::ofstream dst(sst_ptr->file_path_, std::ios::binary);
      dst << src.rdbuf();
      src.close();
      dst.close();
      utils::Rmfile(oldPath.c_str());
      merge_res.emplace_back(sst_ptr);
    } else {
      all_values[sst_ptr] = sst_ptr->Values();
      Timestamp max_timestamp =
          MaxTimestampInCompaction(*cur_level_discard_sst, next_level_discard);
      merge_res = MergeSST(level + 1, max_timestamp, sst_ptr, overlap,
                           all_values, remove_deletion_mark);
    }

#ifdef DEBUG
    cout << "================= merge result =================" << endl;
    for (auto i : mergeResult) {
      cout << *i << endl;
    }
#endif
    // Step4: use reconstruct() to rebuild the next level

#ifdef DEBUG
    cout << "================= before reconstruct =================" << endl;
    LevelPtr nextLevel = ssTables[level + 1];
    for (auto i = nextLevel->begin(); i != nextLevel->end(); ++i) {
      cout << **i << endl;
    }
#endif

    ReconstructLevel(level + 1, next_level_discard, merge_res);

#ifdef DEBUG
    LevelPtr newNextLevel = ssTables[level + 1];
    cout << "================= after reconstruct =================" << endl;
    for (auto i = newNextLevel->begin(); i != newNextLevel->end(); ++i) {
      cout << **i << endl;
    }
#endif
  }

  // step5: after iterating through all SSTs, reconstruct the top level
  ReconstructLevel(level, cur_level_discard_sst);
}

/**
 * @Description: Merge SSTs for level-0 using priority queue
 *               It also handles the creation of level-1, if level-0 is full.
 * @param level: Number of level to merge TO, which must exist already.
 * @param max_timestamp: Gives a hint for the timestamp for the new SST.
 * @param pq: The priority queue that Contains all SSTs to be merged
 * @param all_values: The values that are stored in SSTs in the priority queue.
 * @return: A vector of new SSTs as the result of the merge. Copy elision should
 * handle necessary copies.
 */
std::vector<SSTableSPtr> KVStore::MergeSSTLevel0(
    const size_t level, const size_t max_timestamp,
    std::priority_queue<std::pair<SSTableSPtr, size_t>> &pq,
    std::unordered_map<SSTableSPtr, std::shared_ptr<std::vector<StringSPtr>>>
        &all_values) {
  std::vector<SSTableSPtr> ret;
  std::unordered_set<uint64_t> duplicate_checker;

  while (!pq.empty()) {
    // Initialize fields for new SST.
    SSTableSPtr new_sst_ptr =
        std::make_shared<SSTable>(kDir + "/level-" + std::to_string(level) +
                                      "/" + std::to_string(sst_no_++) + ".sst",
                                  max_timestamp);

    size_t file_size = kSSTHeaderSize + kBloomFilterSize;
    size_t num_keys = 0;
    uint64_t min_key = std::numeric_limits<uint64_t>::max();
    uint64_t max_key = std::numeric_limits<uint64_t>::min();

    std::vector<StringSPtr> values;

    // Start merging data into the new SST.
    while (!pq.empty()) {
      auto sst_and_index = pq.top();
      pq.pop();

      SSTableSPtr sst = sst_and_index.first;
      size_t idx = sst_and_index.second;
      uint64_t key = sst->keys_[idx];
      // Key already exists, do nothing except checking if there's
      // any key left in this SST.
      if (duplicate_checker.count(key)) {
        if (++idx < sst->num_keys_) {
          pq.push(make_pair(sst, idx));
        } else {
          utils::Rmfile(sst->file_path_.c_str());
        }
        continue;
      }

      // Get value, which cannot possibly be null
      StringSPtr value = all_values[sst]->at(idx);

      // Check file size.
      size_t new_file_size = file_size + kIndexSizePerValue + value->size();
      if (new_file_size > kMaxSSTableSize) {
        // Put the value back.
        pq.push(make_pair(sst, idx));
        Save(new_sst_ptr, file_size, num_keys, min_key, max_key, values);
        ret.emplace_back(new_sst_ptr);
        break;
      }
      // Pass all checks, can modify SST in memory and write to disk
      file_size = new_file_size;
      duplicate_checker.insert(key);
      values.emplace_back(value);
      new_sst_ptr->bloom_filter_.Put(key);
      new_sst_ptr->keys_.emplace_back(key);
      ++num_keys;
      min_key = key < min_key ? key : min_key;
      max_key = key > max_key ? key : max_key;
      if (++idx < sst->num_keys_) {
        pq.push(make_pair(sst, idx));
      } else {
        utils::Rmfile(sst->file_path_.c_str());
      }
    }

    // pq is empty, but there's still a bit of data in an sstable
    if (pq.empty() && !values.empty()) {
      Save(new_sst_ptr, file_size, num_keys, min_key, max_key, values);
      ret.emplace_back(new_sst_ptr);
    }
  }
  return ret;
}

/**
 * @Description: Given value of various fields of SSTable, initialize it with
 * these values, and persist the sst object to disk.
 * @param sst_ptr: Pointer to the SST to be initialized and saved.
 * @param file_size: File size of the entire SST files.
 * @param num_key: Number of keys in the SST.
 * @param min_key: Minimum key of the SST.
 * @param max_key: Maximum key of the SST.
 * @param values: Values corresponding to keys in the SST.
 */
void KVStore::Save(SSTableSPtr &sst_ptr, size_t file_size, size_t num_key,
                   const uint64_t min_key, const uint64_t max_key,
                   std::vector<std::shared_ptr<std::string>> &values) {
  sst_ptr->file_size_ = file_size;
  sst_ptr->num_keys_ = num_key;
  sst_ptr->min_key_ = min_key;
  sst_ptr->max_key_ = max_key;

  size_t offset =
      kSSTHeaderSize + kBloomFilterSize + num_key * kIndexSizePerValue;
  sst_ptr->offset_.resize(num_key);
  for (int i = 0; i < num_key; ++i) {
    sst_ptr->offset_[i] = offset;
    offset += values[i]->size();
  }

#ifdef DEBUG
  // check fileSize
  size_t fs =
      kSSTHeaderSize + kBloomFilterSize + numOfKeys * kIndexSizePerValue;
  for (int i = 0; i < numOfKeys; ++i) {
    fs += values[i]->size();
  }
  if (fs != fileSize) {
    cout << "File size error! Difference: " << fileSize - fs << endl;
  }
#endif

  sst_ptr->ToFile(values);
}

void KVStore::ReconstructLevel(
    const size_t level,
    const std::shared_ptr<std::set<SSTableSPtr>> &sst_to_discard) {
  LevelSPtr level_ptr = ssts_[level];

  size_t num_current_level_sst = level_ptr->size();

  LevelSPtr new_level_sst = std::make_shared<Level>();
  for (int i = 0; i < num_current_level_sst; ++i) {
    SSTableSPtr sstPtr = level_ptr->at(i);
    if (!sst_to_discard->count(sstPtr)) {
      new_level_sst->emplace_back(sstPtr);
    }
  }

  ssts_[level] = new_level_sst;
}

/**
 * @Description: Special case handling for Compaction at level-0.
 *               Uses priority queue to do multi-way merge
 *               Handles the creation of the first level, if it does not exist
 */
void KVStore::CompactionLevel0() {
  LevelSPtr level0_ptr = ssts_[0];

  uint64_t min_key = std::numeric_limits<uint64_t>::max();
  uint64_t max_key = std::numeric_limits<uint64_t>::min();

  size_t max_timestamp;

  // Put SSts in level-0 into priority queue.
  std::priority_queue<std::pair<SSTableSPtr, size_t>> pq;
  // Store complete values of all SSTs in advance.
  std::unordered_map<SSTableSPtr, std::shared_ptr<std::vector<StringSPtr>>>
      values;
  for (const auto &sst_ptr : *level0_ptr) {
#ifdef DEBUG
    cout << "================= before merge =================" << endl;
#endif

#ifdef DEBUG
    cout << *sst_ptr << endl;
#endif

    pq.push(std::make_pair(sst_ptr, 0));
    values[sst_ptr] = sst_ptr->Values();

    uint64_t mi_key = sst_ptr->MinKey();
    uint64_t ma_key = sst_ptr->MaxKey();
    min_key = mi_key < min_key ? mi_key : min_key;
    max_key = ma_key > max_key ? ma_key : max_key;
  }

  max_timestamp = level0_ptr->back()->timestamp_;

  // level-1 needs creating.
  if (ssts_.size() == 1) {
    if (!utils::DirExists(kDir + "/level-1")) {
      utils::Mkdir((kDir + "/level-1").c_str());
    }
    std::vector<SSTableSPtr> merge_res =
        MergeSSTLevel0(1, max_timestamp, pq, values);

#ifdef DEBUG
    cout << "================= merge result =================" << endl;
    for (auto i : mergeResult) {
      cout << *i << endl;
    }
#endif

    ssts_.emplace_back(std::make_shared<Level>(merge_res));
  } else {
    LevelSPtr level1_ptr = ssts_[1];
    size_t level1_size = level1_ptr->size();
    std::set<SSTableSPtr> next_level_discard;

    // Search for overlapping interval.
    long start_idx = LowerBound(level1_ptr, min_key);
    for (long i = start_idx; i < level1_size; ++i) {
      SSTableSPtr sst_ptr = level1_ptr->at(i);
      Timestamp ts = sst_ptr->timestamp_;
      if (sst_ptr->MinKey() <= max_key) {
        pq.push(make_pair(sst_ptr, 0));
        values[sst_ptr] = sst_ptr->Values();
        next_level_discard.insert(sst_ptr);
        max_timestamp = ts > max_timestamp ? ts : max_timestamp;
      } else {
        break;
      }
    }

    std::vector<SSTableSPtr> merge_result =
        MergeSSTLevel0(1, max_timestamp, pq, values);
#ifdef DEBUG
    cout << "================= merge result =================" << endl;
    for (auto i : mergeResult) {
      cout << *i << endl;
    }
#endif
    ReconstructLevel(1, next_level_discard, merge_result);
  }

  ssts_[0]->clear();
}

/**
 * @Description Rebuild a level of SSTablePtr with information of SSTs to
 * discard at the upper level and SSTs to add at the lower level.
 * @param level: The number of level to reconstruct, starting at 0.
 * @param sst_to_discard: A set of SSTPtr to delete in this level.
 * @param merge_result: A vector of SSTPtr to add to this level.
 */
void KVStore::ReconstructLevel(const size_t level,
                               std::set<SSTableSPtr> &sst_to_discard,
                               std::vector<SSTableSPtr> &merge_result) {
  LevelSPtr level_ptr = ssts_[level];
  uint64_t min_result_key = merge_result[0]->MinKey();

  LevelSPtr new_level_ptr = std::make_shared<Level>();
  size_t level_size = level_ptr->size();
  int i = 0;
  for (; i < level_size && level_ptr->at(i)->MaxKey() < min_result_key; ++i) {
    if (!sst_to_discard.count(level_ptr->at(i))) {
      new_level_ptr->emplace_back(level_ptr->at(i));
    }
  }

  new_level_ptr->insert(new_level_ptr->end(), merge_result.begin(),
                        merge_result.end());
  for (; i < level_size; ++i) {
    if (!sst_to_discard.count(level_ptr->at(i))) {
      new_level_ptr->emplace_back(level_ptr->at(i));
    }
  }

  ssts_[level] = new_level_ptr;
}

/**
 * @Description: When doing compaction for levels other than level 0,
 * find out the SSTs that overflow, which should be deleted after the merge
 * Strategy for choosing overflowing SST: Pick ones with the least timestamp,
 * if two SSTs have, the same timestamp, pick the one with smaller keys.
 *
 * This is essentially a min-K problem.
 * @param level: The upper level for compaction.
 * @param num_overlapping_sst: Number of SSTs that overflow.
 * @return: A pointer to a set of SSTs, they are sorted by min key.
 */
std::shared_ptr<std::set<SSTableSPtr>> KVStore::SSTForCompaction(
    const size_t level, const size_t num_overlapping_sst) {
  auto ret = std::make_shared<std::set<SSTableSPtr>>();
  auto comparator = [](const SSTableSPtr &t1, const SSTableSPtr &t2) {
    return t1->timestamp_ < t2->timestamp_ ||
           (t1->timestamp_ == t2->timestamp_ && t1->min_key_ < t2->min_key_);
  };

  std::priority_queue<SSTableSPtr, std::vector<SSTableSPtr>,
                      decltype(comparator)>
      pq(comparator);

  LevelSPtr level_ptr = ssts_[level];
  size_t level_size = level_ptr->size();

  for (int i = 0; i < level_size; ++i) {
    pq.push(level_ptr->at(i));
    if (pq.size() > num_overlapping_sst) {
      pq.pop();
    }
  }

  while (!pq.empty()) {
    ret->insert(pq.top());
    pq.pop();
  }

  return ret;
}

/**
 * @Description: Merging routine for levels other than level 0, using 2-way
 * merging
 * @param level: The number of upper level of Compaction
 * @param max_timestamp: The timestamp for all SSTs that are created
 * @param sst: The SST at the upper level
 * @param overlap: The SSTs whose key range overlaps with <sst>
 * @param all_values: The values for all SSTs concerned
 * @param remove_deletion_mark: A flag indicating whether deletion marks
 * should be removed
 * @return A vector of SST pointers as the result of the merge
 */
std::vector<SSTableSPtr> KVStore::MergeSST(
    const size_t level, const size_t max_timestamp, const SSTableSPtr &sst,
    std::vector<SSTableSPtr> &overlap,
    std::unordered_map<SSTableSPtr, std::shared_ptr<std::vector<StringSPtr>>>
        &all_values,
    bool remove_deletion_mark) {
#ifdef DEBUG
  cout << "================= merge from =================" << endl;
  cout << *sst << endl;
  for (auto sstPtr = overlap.begin(); sstPtr != overlap.end(); ++sstPtr) {
    cout << **sstPtr;
#endif

    std::vector<SSTableSPtr> ret;
    std::unordered_set<uint64_t> duplicate_checker;

    size_t num_overlap = overlap.size();

    // The index of the overlapping SST.
    size_t idx_in_overlap = 0;
    // Index in current overlapping SST.
    size_t idx_in_keys_in_overlap = 0;
    // Number of keys in the upper level SST.
    const size_t num_sst_key = sst->num_keys_;
    // Index in upper level SST.
    size_t idx_in_sst = 0;
    // The overlapping SST that is currently being merged.
    SSTableSPtr cur_overlap_sst_ptr = overlap[idx_in_overlap];
    // Lambda function to tell if there's any SST to merge.
    auto should_continue_merge = [&]() {
      return idx_in_overlap < num_overlap || idx_in_sst < num_sst_key;
    };

    // Check if there can still be an SST to merge.
    while (should_continue_merge()) {
      SSTableSPtr new_sst_ptr = std::make_shared<SSTable>(
          kDir + "/level-" + std::to_string(level) + "/" +
              std::to_string(sst_no_++) + ".sst",
          max_timestamp);

      size_t file_size = kSSTHeaderSize + kBloomFilterSize;
      size_t num_keys = 0;
      uint64_t min_key = std::numeric_limits<uint64_t>::max();
      uint64_t max_key = std::numeric_limits<uint64_t>::min();

      std::vector<StringSPtr> values;

      auto increment_idx = [&](bool choose_sst) {
        if (!choose_sst) {
          ++idx_in_keys_in_overlap;
          // Switch to the next overlapping SST.
          if (idx_in_keys_in_overlap >= cur_overlap_sst_ptr->num_keys_) {
            idx_in_keys_in_overlap = 0;
            cur_overlap_sst_ptr = ++idx_in_overlap < num_overlap
                                      ? overlap[idx_in_overlap]
                                      : nullptr;
          }
        } else {
          ++idx_in_sst;
        }
      };

      // In each loop, Put one key into the new SST.
      while (should_continue_merge()) {
        uint64_t key;
        bool choose_sst = false;

        // Decide which sequence to choose the next key from.
        if (idx_in_sst < num_sst_key && idx_in_overlap < num_overlap) {
          uint64_t key1 = sst->keys_[idx_in_sst];
          uint64_t key2 = cur_overlap_sst_ptr->keys_[idx_in_keys_in_overlap];
          if (key1 <= key2) {
            choose_sst = true;
            key = key1;
          } else {
            key = key2;
          }
        } else if (idx_in_sst >= num_sst_key) {
          // Data remaining in SST.
          key = cur_overlap_sst_ptr->keys_[idx_in_keys_in_overlap];
        } else {
          // Data remaining in overlapping SSTs.
          key = sst->keys_[idx_in_sst];
          choose_sst = true;
        }

        // Check for duplicate key, only handle error case.
        if (duplicate_checker.count(key)) {
          increment_idx(choose_sst);
          continue;
        }

        // Get value, which cannot possibly be null.
        StringSPtr value =
            choose_sst
                ? all_values[sst]->at(idx_in_sst)
                : all_values[cur_overlap_sst_ptr]->at(idx_in_keys_in_overlap);

        // Check deletion mark.
        if (remove_deletion_mark && *value == kDeletionMark) {
          increment_idx(choose_sst);
          continue;
        }

        // Check file size.
        size_t new_file_size = file_size + kIndexSizePerValue + value->size();
        if (new_file_size > kMaxSSTableSize) {
          Save(new_sst_ptr, file_size, num_keys, min_key, max_key, values);
          ret.emplace_back(new_sst_ptr);
          break;
        }

        // Pass all check, write data.
        new_sst_ptr->keys_.emplace_back(key);
        new_sst_ptr->bloom_filter_.Put(key);
        ++num_keys;
        max_key = key;
        min_key = key < min_key ? key : min_key;
        values.emplace_back(value);
        file_size = new_file_size;
        increment_idx(choose_sst);
        duplicate_checker.insert(key);
      }

      if (!should_continue_merge() && !values.empty()) {
        Save(new_sst_ptr, file_size, num_keys, min_key, max_key, values);
        ret.emplace_back(new_sst_ptr);
      }
    }

    // after merging, delete all files
    utils::Rmfile(sst->file_path_.c_str());
    for (const SSTableSPtr &sstPtr : overlap) {
      utils::Rmfile(sstPtr->file_path_.c_str());
    }
    return ret;
  }

  /**
   * @Despcription: Get the max timestamp given the SSTs about to undergo
   * compaction.
   * @param curLevelDiscard: The SSTs to discard at the upper level.
   * @param nextLevelDiscard: The SSTs overlapping at the lower level.
   * @return: The maximum timestamp.
   */
  Timestamp KVStore::MaxTimestampInCompaction(
      const std::set<SSTableSPtr> &cur_level_discard_sst,
      const std::set<SSTableSPtr> &next_level_discard_sst) {
    Timestamp ret = 0;
    Timestamp cur_timestamp;

    for (const auto &sst_it : cur_level_discard_sst) {
      cur_timestamp = sst_it->timestamp_;
      ret = cur_timestamp > ret ? cur_timestamp : ret;
    }
    for (const auto &sst_ptr : next_level_discard_sst) {
      cur_timestamp = sst_ptr->timestamp_;
      ret = cur_timestamp > ret ? cur_timestamp : ret;
    }
    return ret;
  }

  /**
   * @Description: Find index of the SST whose max key is greater or equal than
   * `target`.
   * @return: The found index.
   */
  long KVStore::LowerBound(const LevelSPtr &level_ptr, const uint64_t target) {
    long left = 0;
    long right = (long)level_ptr->size();

    while (left < right) {
      long mid = (left + right) >> 1;
      if (level_ptr->at(mid)->max_key_ >= target) {
        right = mid;
      } else {
        left = mid + 1;
      }
    }

    return left;
  }
