#include "kvstore.h"
#include "header/utils.h"
#include "header/SSTable.h"

#include <iostream>

KVStore::KVStore(const String &dir): KVStoreAPI(dir), dir(dir), timeStamp(1) {
    vector<String> levelList;
    int levelNum = utils::scanDir(dir, levelList);

    ssTables.resize(levelNum);

    // 如果盘上没有文件，就在内存中开一层即可
    if (!levelNum) {
        ssTables.emplace_back(make_shared<Level>());
    }

    String dirWithSlash = dir + "/";
    for (int i = 0; i < levelNum; ++i) {
        vector<String> fileList;
        int fileNum = utils::scanDir(dirWithSlash + levelList[i], fileList);

        LevelPtr levelPtr = make_shared<Level>(fileNum);

        String levelNameWithSlash = dirWithSlash + levelList[i] + "/";
        for (int j = 0; j < fileNum; ++j) {
            (*levelPtr)[j] = make_shared<SSTable>(levelNameWithSlash + fileList[j]);
        }

        // sort in ascending order to simplify compaction
        sort(levelPtr->begin(), levelPtr->end(), SSTableComparator);

        ssTables[i] = levelPtr;
    }
}

KVStore::~KVStore() {
    if (!memTable.isEmpty()) {
        memTable.toFile(timeStamp++, dir);
    }
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(Key key, const String &s) {
    try {
        memTable.put(key, s);
    } catch (const MemTableFull &) {
        SSTablePtr ssTablePtr = memTable.toFile(timeStamp++, dir);
        memTable.reset();
        memTable.put(key, s);

        ssTables.back()->emplace_back(ssTablePtr);
    }
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
String KVStore::get(Key key) {
    String* valueInMemory = memTable.get(key);
    if (valueInMemory) {
        return *valueInMemory == DELETION_MARK ? "" : *valueInMemory;
    }

    // memory中没有找到，在内存中找
    for (const LevelPtr& levelPtr: ssTables) {
        if (levelPtr == *ssTables.begin()) {
            // level-0，在这一层顺序找
            for (auto ssTableItr = levelPtr->rbegin(); ssTableItr != levelPtr->rend(); ++ssTableItr) {
                // 在一个SSTable中查找，为了提高效率返回指针，如果找不到返回nullptr
                SSTablePtr ssTablePtr = *ssTableItr;
                shared_ptr<String> valuePtr = ssTablePtr->get(key);
                if (valuePtr) {
                    String value = *valuePtr;
                    return (value == DELETION_MARK) ? "" : value;
                }
            }
        } else {
            // 其他层，二分查找
            SSTablePtr ssTablePtr = binarySearch(levelPtr, key);
            if (ssTablePtr) {
                shared_ptr<String> valuePtr = ssTablePtr->get(key);
                if (valuePtr) {
                    String value = *valuePtr;
                    return value == DELETION_MARK ? "" : value;
                }
            }
        }
        // 这层找不到，进入下一层
    }
	return "";
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(Key key) {
    // true则一定有，false可能没有
    bool isInMemory = memTable.del(key);
    bool isDeletedInMemory = memTable.get(key) != nullptr;
    put(key, DELETION_MARK);
    if (isInMemory) {
        return true;
    } else if (isDeletedInMemory) {
        return false;
    }

    for (const LevelPtr& levelPtr: ssTables) {
        if (levelPtr == *ssTables.begin()) {
            // level-0，在这一层顺序找
            for (auto ssTableItr = levelPtr->rbegin(); ssTableItr != levelPtr->rend(); ++ssTableItr) {
                // 在一个SSTable中查找，为了提高效率返回指针，如果找不到返回nullptr
                SSTablePtr ssTablePtr = *ssTableItr;
                shared_ptr<String> valuePtr = ssTablePtr->get(key);
                if (valuePtr) {
                    String value = *valuePtr;
                    return !(value == DELETION_MARK);
                }
            }
        } else {
            // 其他层，二分查找
            SSTablePtr ssTablePtr = binarySearch(levelPtr, key);
            if (ssTablePtr) {
                shared_ptr<String> valuePtr = ssTablePtr->get(key);
                if (valuePtr) {
                    String value = *valuePtr;
                    return !(value == DELETION_MARK);
                }
            }
        }
        // 这层找不到，进入下一层
    }
    return false;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
    memTable.reset();
    ssTables.clear();
    ssTables.emplace_back(make_shared<Level>());

    // remove all files
    vector<String> levelList;
    int levelNum = utils::scanDir(dir, levelList);
    String dirWithSlash = dir + "/";
    for (int i = 0; i < levelNum; ++i) {
        vector<String> fileList;
        utils::scanDir(dirWithSlash + levelList[i], fileList);

        String levelNameWithSlash = dirWithSlash + levelList[i] + "/";
        for (String& fileName: fileList) {
            utils::rmfile((levelNameWithSlash + fileName).c_str());
        }

        utils::rmdir((dirWithSlash + levelList[i]).c_str());
    }
}

SSTablePtr KVStore::binarySearch(const LevelPtr& levelPtr,const Key& key) {
    long left = 0;
    long right = levelPtr->size() - 1;
    while (left <= right) {
        long mid = (left + right) >> 1;
        SSTablePtr ret = (*levelPtr)[mid];
        if (ret->contains(key)) {
            return ret;
        } else if (key < ret->getMinKey()) {
            left = mid - 1;
        } else {
            right = mid + 1;
        }
    }
    return nullptr;
}

void KVStore::printSSTables() {
    int numOfLevels = ssTables.size();
    for (int i = 0; i < numOfLevels; ++i) {
        cout << "Level " << i << endl;
        Level level = *ssTables[i]; //
        for (SSTablePtr ssTablePtr: level) {
            cout << *ssTablePtr << endl;
        }
    }
}
