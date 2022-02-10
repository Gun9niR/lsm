#include "../include/sstable.h"

SSTable::SSTable(const std::string &path, const Timestamp timestamp)
    : file_path_(path),
      file_size_(0),
      timestamp_(timestamp),
      num_keys_(0),
      min_key_(std::numeric_limits<uint64_t>::max()),
      max_key_(std::numeric_limits<uint64_t>::min()) {}

/**
 * @Description: Construct an SSTable by reading from a file
 * @param file_path: Full(relative) path to the SST on disk
 */
SSTable *SSTable::FromFile(const std::string &file_path) {
  auto *sst = new SSTable();
  sst->file_path_ = file_path;
  std::ifstream sst_in_file(file_path, std::ios::binary);

  // Read header.
  sst_in_file.read((char *) &sst->timestamp_, 8)
      .read((char *) &sst->num_keys_, 8)
      .read((char *) &sst->min_key_, 8)
      .read((char *) &sst->max_key_, 8);

  sst->keys_.resize(sst->num_keys_);
  sst->offset_.resize(sst->num_keys_);

  sst->bloom_filter_.FromFile(sst_in_file);

  for (int i = 0; i < sst->num_keys_; ++i) {
    sst_in_file.read((char *) &sst->keys_[i], 8).read((char *) &sst->offset_[i], 4);
  }

  sst_in_file.seekg(0, sst_in_file.end);
  sst->file_size_ = sst_in_file.tellg();

  sst_in_file.close();
  return sst;
}

std::ostream &operator<<(std::ostream &ostream, const SSTable &ssTable) {
  auto allValues = ssTable.Values();

  ostream << "Path: " << ssTable.file_path_ << std::endl
          << "Timestamp: " << ssTable.timestamp_ << std::endl
          << "Number of keys: " << ssTable.num_keys_ << std::endl
          << "Min Key: " << ssTable.min_key_ << std::endl
          << "Max Key: " << ssTable.max_key_ << std::endl
          << "Keys: ";

  for (int i = 0; i < ssTable.num_keys_; ++i) {
    ostream << ssTable.keys_[i] << " "
            << "(";
  }
  ostream << std::endl;
  return ostream;
}

inline bool SSTable::IsProbablyPresent(const uint64_t key) const {
  return bloom_filter_.IsProbablyPresent(key);
}

/**
 * @Description: Find by key in a SST using binary search.
 * @param key: Plain to see.
 * @return: The pointer to the value if it exists, nullptr if it is absent.
 */
std::shared_ptr<std::string> SSTable::ValueByKey(const uint64_t key) const {
  if (key >= min_key_ && key <= max_key_ && IsProbablyPresent(key)) {
    size_t idx = BinarySearch(key);
    if (idx != std::numeric_limits<size_t>::max()) {
      return ValueByIndex(idx);
    }
  }
  return nullptr;
}

/**
 * @Description: Find value by index in key.
 * @param idx: The index in values.
 * @return: Pointer to the value.
 */
std::shared_ptr<std::string> SSTable::ValueByIndex(size_t idx) const {
  size_t length = (idx != num_keys_ - 1) ? offset_[idx + 1] - offset_[idx]
                                         : file_size_ - offset_[idx];

  std::shared_ptr<std::string> ret = std::make_shared<std::string>(length, 0);

  std::ifstream file(file_path_, std::ios::binary);

  file.seekg((long long)offset_[idx]);
  file.read(&(*ret)[0], (long)length);
  file.close();

  return ret;
}

/**
 * @Description: Find the key using binary search.
 */
size_t SSTable::BinarySearch(const uint64_t key) const {
  long left = 0;
  long right = (long)keys_.size() - 1;
  while (left <= right) {
    long mid = left + ((right - left) >> 1);
    uint64_t k = keys_[mid];
    if (k == key) {
      return mid;
    } else if (k < key) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }

  return std::numeric_limits<size_t>::max();
}

bool SSTable::Contains(const uint64_t key) const {
  return min_key_ <= key && max_key_ >= key;
}

uint64_t SSTable::MaxKey() const { return max_key_; }

uint64_t SSTable::MinKey() const { return min_key_; }

/* when calling this function, every field is set */
void SSTable::ToFile(std::vector<std::shared_ptr<std::string>> &values) {
  std::ofstream file(file_path_, std::ios::out | std::ios::binary);

  file.write((char *)&timestamp_, 8)
      .write((char *)&num_keys_, 8)
      .write((char *)&min_key_, 8)
      .write((char *)&max_key_, 8);

  bloom_filter_.ToFile(file);

  for (int i = 0; i < num_keys_; ++i) {
    file.write((char *)&keys_[i], 8).write((char *)&offset_[i], 4);
  }

  for (int i = 0; i < num_keys_; ++i) {
    file.write(values[i]->c_str(), (long)values[i]->size());
  }
  file.close();
}

std::shared_ptr<std::vector<StringSPtr>> SSTable::Values() const {
  std::shared_ptr<std::vector<StringSPtr>> ret =
      std::make_shared<std::vector<StringSPtr>>();
  ret->reserve(num_keys_);

  std::ifstream file(file_path_, std::ios::binary);

  file.seekg((long long)offset_[0]);

  for (int i = 0; i < num_keys_ - 1; ++i) {
    size_t length = offset_[i + 1] - offset_[i];
    StringSPtr value = std::make_shared<std::string>(length, 0);
    file.read(&(*value)[0], (long)length);
    ret->emplace_back(value);
  }

  size_t length = file_size_ - offset_[num_keys_ - 1];

  StringSPtr value = std::make_shared<std::string>(length, 0);
  file.read(&(*value)[0], (long)length);
  ret->emplace_back(value);
  file.close();

  return ret;
}
