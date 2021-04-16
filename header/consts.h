#ifndef LSM_CONSTANTS_H
#define LSM_CONSTANTS_H

#include <string>
#include <memory>
#include <vector>
#include <fstream>
#include <iostream>
#include <cstring>
#include <map>
#include "../kvstore_api.h"

typedef std::string String;
typedef uint64_t Key;
typedef unsigned long long Size;
typedef unsigned long long TimeStamp;
using std::shared_ptr;
using std::make_shared;
using std::vector;
using std::ofstream;
using std::ifstream;
using std::cout;
using std::endl;
using std::ostream;
using std::ios;
using std::map;
using std::sort;

// todo: should move them to a .cpp file

const size_t MAX_SSTABLE_SIZE = 1 << 21;

const size_t INDEX_SIZE_PER_VALUE = 12;

const size_t HEADER_SIZE = 32;

const size_t BLOOM_FILTER_SIZE = 10240;

const String DELETION_MARK = "~DELETED~";

#endif