#include "kvstore.h"
#include "header/utils.h"
#include "header/SSTable.h"

#include <iostream>

/**
 * @Description: Construct KVStore object with given base directory
 * @param dir: base directory, where all SSTs are stored
 */
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
            auto ssTablePtr = make_shared<SSTable>(levelNameWithSlash + fileList[j]);
            (*levelPtr)[j] = ssTablePtr;

            if (ssTablePtr->timeStamp > timeStamp) {
                timeStamp = ssTablePtr->timeStamp + 1;
            }
        }

        // 第一层不排序，其他层按照key升序排序
        if (i) {
            sort(levelPtr->begin(), levelPtr->end(), SSTableComparatorForSort);
        }

        ssTables[i] = levelPtr;
    }
}

/**
 * @Description: Destruct KVStor object, write the content of memory to disk
 */
KVStore::~KVStore() {
    if (!memTable.isEmpty()) {
        memTable.toFile(timeStamp++, dir);
        compaction();
    }
}

/**
 * @Description: Insert/Update the key-value pair. No return values for simplicity.
 * @Param key: Key in the key-value pair
 * @Param s: Value in the key-value pair
 */
void KVStore::put(Key key, const String &s) {
    try {
        memTable.put(key, s);
    } catch (const MemTableFull &) {
        SSTablePtr ssTablePtr = memTable.toFile(timeStamp++, dir);
        memTable.reset();
        memTable.put(key, s);

        ssTables[0]->emplace_back(ssTablePtr);
        compaction();
    }
}
/**
 * @Description: Find in KVStore by key
 * @Param key: The key to find with
 * @Return: the (string) value of the given key. Empty string indicates not found.
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
 * @Description: Delete the given key-value pair if it exists.
 * @Param key: The key to search with
 * @Return: false iff the key is not found.
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
 * @Description: Resets the kvstore. All key-value pairs should be removed,
 *               including memtable and all sstables files.
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

/*
 * @Description: Search in a level for a key using binary search
 * @Param levelPtr: Pointer to the level to search in
 * @Param key: Key to search with
 * @Return: Pointer to the SST that is found
 */
SSTablePtr KVStore::binarySearch(const LevelPtr& levelPtr,const Key& key) {
    long left = 0;
    long right = levelPtr->size() - 1;
    while (left <= right) {
        long mid = (left + right) >> 1;
        SSTablePtr ret = (*levelPtr)[mid];
        if (ret->contains(key)) {
            return ret;
        } else if (key < ret->getMinKey()) {
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }
    return nullptr;
}

/*
 * @Description: Debug utility function, print all ssTables cached in memory.
 */
__unused void KVStore::printSSTables() {
    int numOfLevels = ssTables.size();
    for (int i = 0; i < numOfLevels; ++i) {
        cout << "Level " << i << endl;
        Level level = *ssTables[i]; //
        for (const SSTablePtr& ssTablePtr: level) {
            cout << *ssTablePtr << endl;
        }
    }
}

/**
 * @Description: Handle compaction for all levels, level-0 and other levels are handled separately
 */
void KVStore::compaction() {
    // handle level0 special case
    if (ssTables[0]->size() > 2) {
        compactionLevel0();
    }

    size_t numOfLevels = ssTables.size();
    size_t numOfLevelsToIterate = numOfLevels - 1;
    for (size_t i = 1; i < numOfLevelsToIterate; ++i) {
        if (ssTables[i]->size() > (2 << i)) {
            compaction(i, i == numOfLevelsToIterate - 1);
        }
    }

    // check for the last level, just put the sstables with largest timestamp to the next level
    auto lastLevelPtr = ssTables[numOfLevelsToIterate];
    if (lastLevelPtr->size() > (2 << numOfLevelsToIterate)) {
        String levelName = dir + "/level-" + to_string(numOfLevels);
        if (!utils::dirExists(dir + "/level")) {
            utils::mkdir(levelName.c_str());
        }

        auto curLevelDiscard = getSSTForCompaction(numOfLevelsToIterate, lastLevelPtr->size() - (2 << numOfLevelsToIterate));

        LevelPtr newLevel = make_shared<Level>();

        for (auto sstIt = curLevelDiscard->begin(); sstIt != curLevelDiscard->end(); ++sstIt) {
            SSTablePtr sst = *sstIt;
            String oldPath = sst->fullPath;
            sst->fullPath = levelName + "/" + to_string(sst->timeStamp) + ".sst";

            ifstream src(oldPath, ios::binary);
            ofstream dst(sst->fullPath, ios::binary);
            dst << src.rdbuf();
            utils::rmfile(oldPath.c_str());
            newLevel->emplace_back(sst);
        }
        ssTables.emplace_back(newLevel);
        reconstructLevel(numOfLevelsToIterate, curLevelDiscard);
    }
}

/**
 * @Description: Handle compaction for levels other than level-0
 * @param level: The number of level that is overflowing currently
 * @param shouldRemoveDeletionMark: A flag that decides whether "~DELETED~" should be removed
 *                                  It is true only when level is the level above the bottom level
 */
void KVStore::compaction(size_t level, bool shouldRemoveDeletionMark) {
    LevelPtr curLevelPtr = ssTables[level];

    size_t maxSize = 2 << level;                      // max size of current level
    size_t numOfTablesToMerge = curLevelPtr->size() - maxSize;

    // step1: find the sstables with the least time stamp
    shared_ptr<set<SSTablePtr>> curLevelDiscard = getSSTForCompaction(level, numOfTablesToMerge);

    for (auto sstIt = curLevelDiscard->begin(); sstIt != curLevelDiscard->end(); ++sstIt) {
        // step2: Iterate over these sstables, find the overlapping sstables, put them in a vector
        SSTablePtr ssTablePtr = *sstIt;
        LevelPtr nextLevelPtr = ssTables[level + 1];

        Key minKey = ssTablePtr->getMinKey();
        Key maxKey = ssTablePtr->getMaxKey();

        // sst in overlap is sorted in ascending order of key
        vector<SSTablePtr> overlap;
        unordered_set<SSTablePtr> nextLevelDiscard;
        unordered_map<SSTablePtr, shared_ptr<vector<StringPtr>>> allValues;
        allValues[ssTablePtr] = ssTablePtr->getAllValues();

        size_t nextLevelSize = nextLevelPtr->size();
        for (int i = 0; i < nextLevelSize; ++i) {
            SSTablePtr nextLevelSSTPtr = nextLevelPtr->at(i);
            // todo: change to upper and lower bound
            if (nextLevelSSTPtr->getMinKey() <= maxKey && nextLevelSSTPtr->getMaxKey() >= minKey) {
                overlap.emplace_back(nextLevelSSTPtr);
                nextLevelDiscard.insert(nextLevelSSTPtr);
                allValues[nextLevelSSTPtr] = nextLevelSSTPtr->getAllValues();
            }
        }

        // step3: merge that vector and this singe sstable, files are created along the way
        vector<SSTablePtr> mergeResult = startMerge(level + 1, ssTablePtr, overlap, allValues, shouldRemoveDeletionMark);

#ifdef DEBUG
        cout << "================= merge result =================" << endl;
        for (auto i: mergeResult) {
            cout << *i << endl;
        }
#endif
        // step4: use reconstruct() to rebuild the next level

#ifdef DEBUG
        cout << "================= before reconstruct =================" << endl;
        LevelPtr nextLevel = ssTables[level + 1];
        for (auto i = nextLevel->begin(); i != nextLevel->end(); ++i) {
            cout << **i << endl;
        }
#endif

        reconstructLevel(level + 1, nextLevelDiscard, mergeResult);
        LevelPtr newNextLevel = ssTables[level + 1];

#ifdef DEBUG
        cout << "================= after reconstruct =================" << endl;
        for (auto i = newNextLevel->begin(); i != newNextLevel->end(); ++i) {
            cout << **i << endl;
        }
#endif
    }

    // step5: after iterating through all sstables, reconstruct the top level
    reconstructLevel(level, curLevelDiscard);
}

/*
 * @Description: Merge SSTs for level-0 using priority queue
 *               It also handles the creation of level-1, if level-0 is full
 * @Param level: Number of level to merge TO, which must exist already
 * @Param pq: The priority queue that contains all SSTs to be merged
 * @Param allValues: The values that are stored in SSTs in the priority queue
 * @Return: A vector of new SSTs as the result of the merge
 */
vector<SSTablePtr> KVStore::startMerge(size_t level, priority_queue<pair<SSTablePtr, size_t>>& pq, unordered_map<SSTablePtr, shared_ptr<vector<StringPtr>>>& allValues) {
    vector<SSTablePtr> ret;

    while (!pq.empty()) {
        // init fields for new sstable
        SSTablePtr newSSTPtr = make_shared<SSTable>();
        newSSTPtr->fullPath = dir + "/level-" + to_string(level) + "/" + to_string(timeStamp) + ".sst";
        newSSTPtr->timeStamp = timeStamp++;

        size_t fileSize = HEADER_SIZE + BLOOM_FILTER_SIZE;
        Size numOfKeys = 0;
        Key minKey = std::numeric_limits<Key>::max();
        Key maxKey = std::numeric_limits<Key>::min();

        vector<StringPtr> values;

        // start merging data into the new sstable
        while (!pq.empty()) {
            auto sstAndIdx = pq.top();
            pq.pop();

            SSTablePtr sst = sstAndIdx.first;
            size_t idx = sstAndIdx.second;
            // key already exists, do nothing except check if there's any key left in this sst
            if (!newSSTPtr->keys.empty() && newSSTPtr->keys.back() == sst->keys[idx]) {
                if (++idx < sst->numOfKeys) {
                    pq.push(make_pair(sst, idx));
                } else {
                    utils::rmfile(sst->fullPath.c_str());
                }
                continue;
            }

            // get value, which cannot possibly be null
            StringPtr value = allValues[sst]->at(idx);

            // check file size
            if (fileSize + INDEX_SIZE_PER_VALUE + value->size() > MAX_SSTABLE_SIZE) {
                // put the value back
                pq.push(make_pair(sst, idx));
                save(newSSTPtr, fileSize, numOfKeys, minKey, maxKey, values);
                ret.emplace_back(newSSTPtr);
                break;
            }
            // pass all checks, can modify sstable in memory and write to disk
            Key key = sst->keys[idx];
            fileSize += INDEX_SIZE_PER_VALUE + value->size();
            values.emplace_back(value);
            newSSTPtr->bloomFilter.put(key);
            newSSTPtr->keys.emplace_back(key);
            ++numOfKeys;
            minKey = key < minKey ? key : minKey;
            maxKey = key > maxKey ? key : maxKey;
            if (++idx < sst->numOfKeys) {
                pq.push(make_pair(sst, idx));
            } else {
                utils::rmfile(sst->fullPath.c_str());
            }
        }

        // pq is empty, but there's still a bit of data in an sstable
        if (pq.empty() && !values.empty()) {
            save(newSSTPtr, fileSize, numOfKeys, minKey, maxKey, values);
            ret.emplace_back(newSSTPtr);
        }
    }
    return ret;
}

/*
 * @Description: Given value of various fields of SSTable, initialize it with these values, and write it to disk
 * @Param ssTablePtr: Pointer to the SST to be initialized and saved
 */
void KVStore::save(SSTablePtr& ssTablePtr, size_t fileSize, Size numOfKeys, Key minKey, Key maxKey, vector<shared_ptr<String>>& values) {
    ssTablePtr->fileSize = fileSize;
    ssTablePtr->numOfKeys = numOfKeys;
    ssTablePtr->minKey = minKey;
    ssTablePtr->maxKey = maxKey;

    size_t offset = HEADER_SIZE + BLOOM_FILTER_SIZE + numOfKeys * INDEX_SIZE_PER_VALUE;
    ssTablePtr->offset.resize(numOfKeys);
    for (int i = 0; i < numOfKeys; ++i) {
        ssTablePtr->offset[i] = offset;
        offset += values[i]->size();
    }
    ssTablePtr->toFile(values);
}

void KVStore::reconstructLevel(size_t level, const shared_ptr<set<SSTablePtr>>& sstToDiscard) {
    LevelPtr levelPtr = ssTables[level];

    size_t currentLevelSize = levelPtr->size();

    LevelPtr newLevel = make_shared<Level>();
    for (int i = 0; i < currentLevelSize; ++i) {
        SSTablePtr sstPtr = levelPtr->at(i);
        if (!sstToDiscard->count(sstPtr)) {
            newLevel->emplace_back(sstPtr);
        }
    }

    ssTables[level] = newLevel;
}

/*
 * Special case handling for compaction at level0. It handles the creation of level1
 */
void KVStore::compactionLevel0() {
    LevelPtr level0Ptr = ssTables[0];

    Key minKey = std::numeric_limits<Key>::max();
    Key maxKey = std::numeric_limits<Key>::min();

    size_t level0Size = level0Ptr->size();


    // 将level0中的ssTable全部放入优先级队列
    priority_queue<pair<SSTablePtr, size_t>> pq;
    // 提前保存所有要用的value, 先通过sst得到value数组，再通过index得到String*
    unordered_map<SSTablePtr, shared_ptr<vector<StringPtr>>> values;
    for (int i = 0; i < level0Size; ++i) {
        SSTablePtr ssTablePtrToPush = level0Ptr->at(i);
        pq.push(make_pair(ssTablePtrToPush, 0));
        values[ssTablePtrToPush] = ssTablePtrToPush->getAllValues();

        Key miKey = ssTablePtrToPush->getMinKey();
        Key maKey = ssTablePtrToPush->getMaxKey();
        minKey = miKey < minKey ? miKey : minKey;
        maxKey = maKey > maxKey ? maKey : maxKey;
    }

    // 需要创建第二层
    if (ssTables.size() == 1) {
        if (!utils::dirExists(dir + "/level-1")) {
            utils::mkdir((dir + "/level-1").c_str());
        }
        vector<SSTablePtr> mergeResult = startMerge(1, pq, values);
        ssTables.emplace_back(make_shared<Level>(mergeResult));
    } else {
        LevelPtr level1Ptr = ssTables[1];
        size_t level1Size = level1Ptr->size();
        unordered_set<SSTablePtr> nextLevelDiscard;

        for (int i = 0; i < level1Size; ++i) {
            SSTablePtr ssTablePtrToPush = level1Ptr->at(i);
            // find all overlapping sstables at level1
            // todo: could use upper and lower bound
            if (ssTablePtrToPush->getMinKey() <= maxKey && ssTablePtrToPush->getMaxKey() >= minKey) {
                pq.push(make_pair(ssTablePtrToPush, 0));
                values[ssTablePtrToPush] = ssTablePtrToPush->getAllValues();
                nextLevelDiscard.insert(ssTablePtrToPush);
            }
        }

        vector<SSTablePtr> mergeResult = startMerge(1, pq, values);
        reconstructLevel(1, nextLevelDiscard, mergeResult);
    }

    ssTables[0]->clear();
}

/*
 * Given the level, idx to remove and sstables to add, reconstruct that level
 */
void KVStore::reconstructLevel(size_t level, unordered_set<SSTablePtr> &sstToDiscard, vector<SSTablePtr> &mergeResult) {
    LevelPtr levelPtr = ssTables[level];
    Key minResultKey = mergeResult[0]->getMinKey();

    LevelPtr newLevel = make_shared<Level>();
    size_t levelSize = levelPtr->size();
    int i = 0;
    for (; i < levelSize && levelPtr->at(i)->getMaxKey() < minResultKey; ++i) {
        if (!sstToDiscard.count(levelPtr->at(i))) {
            newLevel->emplace_back(levelPtr->at(i));
        }
    }

    newLevel->insert(newLevel->end(), mergeResult.begin(), mergeResult.end());
    for (; i < levelSize; ++i) {
        if (!sstToDiscard.count(levelPtr->at(i))) {
            newLevel->emplace_back(levelPtr->at(i));
        }
    }
    ssTables[level] = newLevel;
}

shared_ptr<set<SSTablePtr>> KVStore::getSSTForCompaction(size_t level, size_t k) {
    auto ret = make_shared<set<SSTablePtr>>();
    auto comparator = [](const SSTablePtr &t1, const SSTablePtr &t2) { return t1->timeStamp < t2->timeStamp; };

    priority_queue<SSTablePtr, vector<SSTablePtr>, decltype(comparator)> pq(comparator);

    LevelPtr levelPtr = ssTables[level];
    size_t levelSize = levelPtr->size();

    for (int i = 0; i < levelSize; ++i) {
        pq.push(levelPtr->at(i));
        if (pq.size() > k) {
            pq.pop();
        }
    }

    while (!pq.empty()) {
        ret->insert(pq.top());
        pq.pop();
    }

    return ret;
}

vector<SSTablePtr> KVStore::startMerge(size_t level,
                                       SSTablePtr sst,
                                       vector<SSTablePtr> &overlap,
                                       unordered_map<SSTablePtr, shared_ptr<vector<StringPtr>>>& allValues,
                                       bool shouldRemoveDeletionMark) {

#ifdef DEBUG
    cout << "================= merge from =================" << endl;
    cout << *sst << endl;
    for (auto sstPtr = overlap.begin(); sstPtr != overlap.end(); ++sstPtr) {
        cout << **sstPtr << endl;
    }
#endif

    vector<SSTablePtr> ret;

    size_t numOfOverlap = overlap.size();

    // which sst in overlap am i merging?
    size_t idxInOverlap = 0;
    SSTablePtr currentOverlappingSST = numOfOverlap ? overlap[idxInOverlap] : nullptr;
    // where am I in current overlapping sst?
    size_t idxInOverlapInKeys = 0;

    size_t numOfSSTKey = sst->numOfKeys;
    // where am I in key of sst?
    size_t idxInSST = 0;

    // lambda function to tell if there's any sst to merge
    auto shouldContinueMerge = [&]() { return idxInOverlap < numOfOverlap || idxInSST < numOfSSTKey; };

    // while there is still a nonempty sst in overlap or on top, start creating a new SST
    while (shouldContinueMerge()) {
        SSTablePtr newSSTPtr = make_shared<SSTable>();
        newSSTPtr->fullPath = dir + "/level-" + to_string(level) + "/" + to_string(timeStamp) + ".sst";
        newSSTPtr->timeStamp = timeStamp++;

        size_t fileSize = HEADER_SIZE + BLOOM_FILTER_SIZE;
        Size numOfKeys = 0;
        Key minKey = std::numeric_limits<Key>::max();
        Key maxKey = std::numeric_limits<Key>::min();

        // store the timestamp of all keys
        unordered_map<Key, TimeStamp> timeStamps;

        vector<StringPtr> values;
        while (shouldContinueMerge()) {
            Key key;
            bool chooseSST = false;
            bool shouldOverwrite = false;
            TimeStamp curTimeStamp;

            auto incrementIdx = [&](bool chooseSST) {
                if (chooseSST) {
                    ++idxInSST;
                } else {
                    ++idxInOverlapInKeys;
                    // switch to the next overlapping sst
                    if (idxInOverlapInKeys >= currentOverlappingSST->numOfKeys) {
                        idxInOverlapInKeys = 0;
                        currentOverlappingSST = ++idxInOverlap < numOfOverlap ? overlap[idxInOverlap] : nullptr;
                    }
                }
            };
            // which key should I choose?
            if (idxInOverlap >= numOfOverlap) {
                // data remaining in sst
                key = sst->keys[idxInSST];
                chooseSST = true;
                curTimeStamp = sst->timeStamp;
            } else if (idxInSST >= numOfSSTKey) {
                // data remaining in overlapping ssts
                key = currentOverlappingSST->keys[idxInOverlapInKeys];
                curTimeStamp = currentOverlappingSST->timeStamp;
            } else {
                Key key1 = sst->keys[idxInSST];
                Key key2 = currentOverlappingSST->keys[idxInOverlapInKeys];
                if (key1 < key2) {
                    chooseSST = true;
                    key = key1;
                    curTimeStamp = sst->timeStamp;
                } else {
                    key = key2;
                    curTimeStamp = currentOverlappingSST->timeStamp;
                }
            }

            // check for duplicate key, only handle error cases
            if (!newSSTPtr->keys.empty() && newSSTPtr->keys.back() == key) {
                // if timeStamp is larger, overwrite
                if (curTimeStamp > timeStamps[key]) {
                    // should overwrite
                    shouldOverwrite = true;
                } else {
                    // increment index, there are two scenarios depending on chooseSST
                    incrementIdx(chooseSST);
                    continue;
                }
            }

            // get value, which cannot possibly be null
            StringPtr value = chooseSST ?
                              allValues[sst]->at(idxInSST) :
                              allValues[currentOverlappingSST]->at(idxInOverlapInKeys);

            // check handling for deletion mark
            if (shouldRemoveDeletionMark && *value == DELETION_MARK) {
                incrementIdx(chooseSST);
                continue;
            }

            // check file size
            // in the case of overwrite, the value being overwritten must be at the end, as is the key
            size_t newFileSize = shouldOverwrite ?
                                 fileSize + INDEX_SIZE_PER_VALUE + value->size() - values.back()->size() :
                                 fileSize + INDEX_SIZE_PER_VALUE + value->size();
            if (newFileSize > MAX_SSTABLE_SIZE) {
                save(newSSTPtr, fileSize, numOfKeys, minKey, maxKey, values);
                ret.emplace_back(newSSTPtr);
                break;
            }

            // pass all check, write data
            // flags to check: chooseSST, shouldOverwrite
            if (shouldOverwrite) {
                values.pop_back();
            } else {
                newSSTPtr->keys.emplace_back(key);
                newSSTPtr->bloomFilter.put(key);
                ++numOfKeys;
                minKey = key < minKey ? key : minKey;
                maxKey = key > maxKey ? key : maxKey;
            }
            values.emplace_back(value);
            fileSize = newFileSize;
            timeStamps[key] = curTimeStamp;
            incrementIdx(chooseSST);

        }

        if (!shouldContinueMerge() && !values.empty()) {
            save(newSSTPtr, fileSize, numOfKeys, minKey, maxKey, values);
            ret.emplace_back(newSSTPtr);
        }
    }

    // after merging, delete all files
    utils::rmfile(sst->fullPath.c_str());
    for (const SSTablePtr& sstPtr: overlap) {
        utils::rmfile(sstPtr->fullPath.c_str());
    }
    return ret;
}
