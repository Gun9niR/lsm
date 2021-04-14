#include "kvstore.h"

KVStore::KVStore(const String &dir): KVStoreAPI(dir) {

}

KVStore::~KVStore() {

}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(Key key, const String &s) {
    memTable.put(key, s);
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
	return memTable.del(key);
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
    memTable.reset();
}
