#ifndef LSM_CONSTANTS_H
#define LSM_CONSTANTS_H

#include <string>
#include <memory>
#include <vector>
#include <fstream>
#include <iostream>
#include <cstring>
#include <queue>
#include <unordered_set>
#include <unordered_map>
#include <set>
#include "../kvstore_api.h"

using std::shared_ptr;
using std::make_shared;
using std::vector;
using std::priority_queue;
using std::ofstream;
using std::ifstream;
using std::cout;
using std::endl;
using std::ostream;
using std::ios;
using std::sort;
using std::pair;
using std::make_pair;
using std::to_string;
using std::unordered_set;
using std::unordered_map;
using std::set;

typedef std::string String;
typedef shared_ptr<String> StringPtr;
typedef uint64_t Key;
typedef unsigned long long Size;
typedef unsigned long long TimeStamp;

const size_t MAX_SSTABLE_SIZE = 1 << 21;

const size_t INDEX_SIZE_PER_VALUE = 12;

const size_t HEADER_SIZE = 32;

const size_t BLOOM_FILTER_SIZE = 10240;

const String DELETION_MARK = "~DELETED~";

#endif