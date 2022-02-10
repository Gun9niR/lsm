#include "kvstore.h"

int main() {
  KVStore kv("./data");

  kv.Put(0, "0");
  std::cout << "Key 0, Value " << kv.Get(0) << std::endl;

  // Empty value indicates not found.
  kv.Del(0);
  std::cout << "Key 0, Value " << kv.Get(0) << std::endl;

  return 0;
}