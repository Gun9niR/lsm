#pragma once

#include "src/consts.h"
#include "src/SkipList.h"
#include "src/Exception.h"

class KVStore : public KVStoreAPI {
private:
    const String dir;

    SkipList<Key, String> memTable;

    TimeStamp timeStamp;

public:
	KVStore(const String &dir);

	~KVStore();

	void put(Key key, const String &s) override;

	String get(Key key) override;

	bool del(Key key) override;

	void reset() override;
};
