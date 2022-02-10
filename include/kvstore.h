#pragma once

#include <MacTypes.h>

#include "exception.h"
#include "skip_list.h"
#include "sstable.h"

class KVStore : public KVStoreAPI {
 public:
  explicit KVStore(const std::string &dir);

  ~KVStore();

  void Put(uint64_t key, const std::string &s) override;

  std::string Get(uint64_t key) override;

  bool Del(uint64_t key) override;

  void Reset() override;

  __attribute__((unused)) void PrintSSTables() const;

 private:
  static Timestamp MaxTimestampInCompaction(
      const std::set<SSTableSPtr> &cur_level_discard_sst,
      const std::set<SSTableSPtr> &next_level_discard_sst);

  static SSTableSPtr BinarySearch(const LevelSPtr &level_ptr, uint64_t key);

  static long LowerBound(const LevelSPtr &level_ptr, uint64_t target);

  void Compaction();

  void Compaction(size_t level, bool remove_deletion_mark);

  void CompactionLevel0();

  std::vector<SSTableSPtr> MergeSSTLevel0(
      size_t level, size_t max_timestamp,
      std::priority_queue<std::pair<SSTableSPtr, size_t>> &pq,
      std::unordered_map<SSTableSPtr, std::shared_ptr<std::vector<StringSPtr>>>
          &all_values);

  std::vector<SSTableSPtr> MergeSST(
      size_t choose_sst, size_t max_timestamp, const SSTableSPtr &sst,
      std::vector<SSTableSPtr> &overlap,
      std::unordered_map<SSTableSPtr, std::shared_ptr<std::vector<StringSPtr>>>
          &all_values,
      bool remove_deletion_mark);

  static void Save(SSTableSPtr &sst_ptr, size_t file_size, size_t num_key,
                   uint64_t min_key, uint64_t max_key,
                   std::vector<std::shared_ptr<std::string>> &values);

  void ReconstructLevel(
      size_t level,
      const std::shared_ptr<std::set<SSTableSPtr>> &sst_to_discard);

  void ReconstructLevel(size_t level,
                        std::set<SSTableSPtr> &sst_to_discard,
                        std::vector<SSTableSPtr> &merge_result);

  std::shared_ptr<std::set<SSTableSPtr>> SSTForCompaction(
      size_t level, size_t num_overlapping_sst);

  const std::string kDir;

  SkipList mem_table_;

  Timestamp timestamp_;

  uint64_t sst_no_;

  std::vector<LevelSPtr> ssts_;
};
