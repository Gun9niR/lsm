#include <cassert>
#include <cstdint>
#include <iostream>
#include <string>

#include "../include/test.h"

class PersistenceTest : public Test {
 public:
  explicit PersistenceTest(const std::string &dir, bool v = true)
      : Test(dir, v) {}

  void StartTest(void *args) override {
    bool test_mode = (args && *static_cast<bool *>(args));

    std::cout << "KVStore Persistence DoTest" << std::endl;

    if (test_mode) {
      std::cout << "<<DoTest Mode>>" << std::endl;
      DoTest(kTestMax);
      utils::Rmdir(kDir.data());
    } else {
      std::cout << "<<Preparation Mode>>" << std::endl;
      DoPrepare(kTestMax);
    }
  }

 private:
  [[noreturn]] void DoPrepare(uint64_t max) {
    uint64_t i;

    // Clean up
    store_.Reset();

    // Test multiple key-value pairs
    for (i = 0; i < max; ++i) {
      store_.Put(i, std::string(i + 1, 's'));
      EXPECT(std::string(i + 1, 's'), store_.Get(i));
    }
    Phase();

    // Test after all insertions
    for (i = 0; i < max; ++i) EXPECT(std::string(i + 1, 's'), store_.Get(i));
    Phase();

    // Test deletions.
    for (i = 0; i < max; i += 2) EXPECT(true, store_.Del(i));

    // Prepare data.
    for (i = 0; i < max; ++i) {
      switch (i & 3) {
        case 0:
          EXPECT(not_found_, store_.Get(i));
          store_.Put(i, std::string(i + 1, 't'));
          break;
        case 1:
          EXPECT(std::string(i + 1, 's'), store_.Get(i));
          store_.Put(i, std::string(i + 1, 't'));
          break;
        case 2:
          EXPECT(not_found_, store_.Get(i));
          break;
        case 3:
          EXPECT(std::string(i + 1, 's'), store_.Get(i));
          break;
        default:
          assert(0);
      }
    }

    Phase();

    Report();

    // Write 10MB data to drain previous data out of memory.
    for (i = 0; i <= 10240; ++i) store_.Put(max + i, std::string(1024, 'x'));

    std::cout << "Data is ready, please press ctrl-c/ctrl-d to"
                 " terminate this program!"
              << std::endl;
    std::cout.flush();

    while (true) {
      volatile int dummy;
      for (i = 0; i <= 1024; ++i) {
        // The loop slows down the program.
        for (i = 0; i <= 1000; ++i) dummy = i;

        store_.Del(max + i);

        for (i = 0; i <= 1000; ++i) dummy = i;

        store_.Put(max + i, std::string(1024, '.'));

        for (i = 0; i <= 1000; ++i) dummy = i; /* NOLINT */

        store_.Put(max + i, std::string(512, 'x'));
      }
    }
  }

  void DoTest(uint64_t max) {
    uint64_t i;

    // Test data
    for (i = 0; i < max; ++i) {
      switch (i & 3) {
        case 0:
          EXPECT(std::string(i + 1, 't'), store_.Get(i));
          break;
        case 1:
          EXPECT(std::string(i + 1, 't'), store_.Get(i));
          break;
        case 2:
          EXPECT(not_found_, store_.Get(i));
          break;
        case 3:
          EXPECT(std::string(i + 1, 's'), store_.Get(i));
          break;
        default:
          assert(0);
      }
    }
    Phase();
    Report();
  }

  const uint64_t kTestMax = 1024 * 32;
};

void Usage(const char *prog, const char *verb, const char *mode) {
  std::cout << "Usage: " << prog << " [-t] [-v]" << std::endl;
  std::cout << "  -t: test mode for persistence DoTest,"
               " if -t is not given, the program only prepares data for DoTest."
               " [currently "
            << mode << "]" << std::endl;
  std::cout << "  -v: print extra info for failed tests [currently ";
  std::cout << verb << "]" << std::endl;
  std::cout << std::endl;
  std::cout << " NOTE: A normal Usage is as follows:" << std::endl;
  std::cout << "    1. invoke `" << prog << "`;" << std::endl;
  std::cout << "    2. terminate (kill) the program when data is ready;";
  std::cout << std::endl;
  std::cout << "    3. invoke `" << prog << "-t ` to DoTest." << std::endl;
  std::cout << std::endl;
  std::cout.flush();
}

int main(int argc, char *argv[]) {
  bool verbose = false;
  bool test_mode = false;

  if (argc == 2) {
    verbose = std::string(argv[1]) == "-v";
    test_mode = std::string(argv[1]) == "-t";
  } else if (argc == 3) {
    verbose = std::string(argv[1]) == "-v" || std::string(argv[2]) == "-v";
    test_mode = std::string(argv[1]) == "-t" || std::string(argv[2]) == "-t";
  } else if (argc > 3) {
    std::cerr << "Too many arguments." << std::endl;
    Usage(argv[0], "OFF", "Preparation Mode");
    exit(-1);
  }

  Usage(argv[0], verbose ? "ON" : "OFF",
        test_mode ? "Test Mode" : "Preparation Mode");

  PersistenceTest test("./data", verbose);
  test.StartTest(static_cast<void *>(&test_mode));
  return 0;
}
