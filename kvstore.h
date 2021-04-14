#pragma once

#include "consts.h"
#include "src/SkipList.h"

class KVStore : public KVStoreAPI {
private:
    SkipList<Key, String> memTable;

public:
	KVStore(const String &dir);

	~KVStore();

	void put(Key key, const String &s) override;

	String get(Key key) override;

	bool del(Key key) override;

	void reset() override;
};
