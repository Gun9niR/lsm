#pragma once

#include "header/SSTable.h"
#include "header/SkipList.h"
#include "header/Exception.h"


class KVStore : public KVStoreAPI {
private:
    const String dir;

    SkipList<Key, String> memTable;

    TimeStamp timeStamp;

    vector<LevelPtr> ssTables;

    static SSTablePtr binarySearch(const LevelPtr&, const Key&);

public:
	explicit KVStore(const String &dir);

	~KVStore();

	void put(Key key, const String &s) override;

	String get(Key key) override;

	bool del(Key key) override;

	void reset() override;

	void printSSTables();
};
