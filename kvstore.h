#pragma once

#include "header/SSTable.h"
#include "header/SkipList.h"
#include "header/Exception.h"

class KVStore : public KVStoreAPI {
private:
    const String dir;

    SkipList<Key, String> memTable;

    TimeStamp timeStamp;

    uint64_t sstNo;

    vector<LevelPtr> ssTables;

    void compaction();

    void compaction(size_t, bool);

    void compactionLevel0();

    vector<SSTablePtr> startMerge(size_t, size_t, priority_queue<pair<SSTablePtr, size_t>> &, unordered_map<SSTablePtr, shared_ptr<vector<StringPtr>>>&);

    vector<SSTablePtr> startMerge(size_t, size_t, const SSTablePtr&, vector<SSTablePtr>&, unordered_map<SSTablePtr, shared_ptr<vector<StringPtr>>>&, bool);

    static void save(SSTablePtr&, size_t, Size, Key, Key, vector<shared_ptr<String>>&);

    void reconstructLevel(size_t, const shared_ptr<set<SSTablePtr>>&);

    void reconstructLevel(size_t, set<SSTablePtr>&, vector<SSTablePtr>&);

    shared_ptr<set<SSTablePtr>> getSSTForCompaction(size_t, size_t);

    static TimeStamp getMaxTimeStamp(const shared_ptr<set<SSTablePtr>>&, const set<SSTablePtr>&);

    static SSTablePtr binarySearch(const LevelPtr&, const Key&);

    static long lowerBound(const LevelPtr&, Key);

public:
	explicit KVStore(const String &dir);

	~KVStore();

	void put(Key key, const String &s) override;

	String get(Key key) override;

	bool del(Key key) override;

	void reset() override;

    void printSSTables();
};
