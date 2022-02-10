#ifndef LSM_COMMON_H
#define LSM_COMMON_H

#include <algorithm>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <queue>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "kvstore_api.h"

typedef uint64_t Timestamp;
typedef std::shared_ptr<std::string> StringSPtr;

const size_t kMaxSSTableSize = 1 << 21;
const size_t kIndexSizePerValue = 12;
const size_t kSSTHeaderSize = 32;
const size_t kBloomFilterSize = 10240;
const std::string kDeletionMark = "~DELETED~";  /* NOLINT */

#endif