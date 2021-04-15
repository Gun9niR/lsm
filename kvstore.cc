#include "kvstore.h"

KVStore::KVStore(const String &dir): KVStoreAPI(dir), dir(dir), timeStamp(0) {
    // todo: load all SSTables from given directory
}

KVStore::~KVStore() {

}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(Key key, const String &s) {
    try {
        memTable.put(key, s);
    } catch (const MemTableFull &) {
        memTable.toFile(timeStamp, dir);
    }

}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
String KVStore::get(Key key) {
    String* findResult = memTable.get(key);
    if (findResult) {
        return *findResult;
    }
	return "";
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(Key key) {
    // todo: 现在只返回在memTable中的查找结果, memTable中找不到就会返回false
    bool isSuccess = memTable.del(key);
    // todo: put可能会超过文件大小限制
    put(key, DELETION_MARK);
	return isSuccess;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
    memTable.reset();
}
