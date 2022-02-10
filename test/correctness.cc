#include <cstdint>
#include <iostream>
#include <random>

#include "test.h"

class CorrectnessTest : public Test {
 public:
  explicit CorrectnessTest(const std::string &dir, bool v = true)
      : Test(dir, v) {}

  void StartTest(void *args) override {
    std::cout << "KVStore Correctness DoTest" << std::endl;

    std::cout << "[Simple DoTest]" << std::endl;
    RegularTest(kSimpleTestMax);

    std::cout << "[Large DoTest]" << std::endl;
    RegularTest(kLargeTestMax);

    utils::Rmdir(kDir.data());
  }

 private:
  void RegularTest(uint64_t max) {
    uint64_t i;
    std::random_device rd;
    std::mt19937 g(rd());

    // Test a single key
    EXPECT(not_found_, store_.Get(1));
    store_.Put(1, "SE");
    EXPECT("SE", store_.Get(1));
    EXPECT(true, store_.Del(1));
    EXPECT(not_found_, store_.Get(1));
    EXPECT(false, store_.Del(1));

    Phase();

    // Test multiple key-value pairs
    std::vector<uint64_t> keys(max);
    for (i = 0; i < max; ++i) {
      keys[i] = i;
    }
    std::shuffle(keys.begin(), keys.end(), g);

    for (i = 0; i < max; ++i) {
      store_.Put(keys[i], std::string(keys[i] + 1, 's'));
    }

    Phase();

    // Test after all insertions.
    for (i = 0; i < max; ++i) EXPECT(std::string(i + 1, 's'), store_.Get(i));
    Phase();

    // Test deletions.
    std::shuffle(keys.begin(), keys.end(), g);
    for (i = 0; i < max; i += 2) EXPECT(true, store_.Del(i));

    for (i = 0; i < max; ++i)
      EXPECT((i & 1) ? std::string(i + 1, 's') : not_found_, store_.Get(i));

    for (i = 0; i < max; ++i) EXPECT(keys[i] & 1, store_.Del(keys[i]));

    for (i = 0; i < max; ++i) EXPECT(not_found_, store_.Get(i));

    Phase();

    Report();
  }

  const uint64_t kSimpleTestMax = 512;
  const uint64_t kLargeTestMax = 1024 * 64;
};

int main(int argc, char *argv[]) {
  bool verbose = (argc == 2 && std::string(argv[1]) == "-v");

  std::cout << "Usage: " << argv[0] << " [-v]" << std::endl;
  std::cout << "  -v: print extra info for failed tests [currently ";
  std::cout << (verbose ? "ON" : "OFF") << "]" << std::endl;
  std::cout << std::endl;
  std::cout.flush();

  CorrectnessTest test("./data", verbose);
  test.StartTest(nullptr);
  return 0;
}
