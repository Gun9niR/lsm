#include <ctime>
#include <random>
#include <thread>

#include "../include/test.h"

class PerformanceTest : public Test {
 public:
  typedef enum {
    kRegular, kCompaction
  } TestMode;

  explicit PerformanceTest(const std::string &dir)
      : Test(dir, false) {}

  void StartTest(void *args) override {
    TestMode mode = *(TestMode *) args;
    if (mode == TestMode::kRegular) {
      TestPutGetDelete(store_, *((int *) (args) + 1));
    } else {
      TestCompaction(store_, *((int *) (args) + 1), *((int *) (args) + 2));
    }
    utils::Rmdir(kDir.data());
  }

 private:
  void TestPutGetDelete(KVStore &kv, int val_size) const {
    size_t put_total_time = 0;
    size_t get_total_time = 0;
    size_t del_total_time = 0;

    double avg_delay;
    double throughput;

    std::cout << "========== Value Size : " << val_size
              << " ==========" << std::endl;
    for (int T = 0; T < kRounds; ++T) {
      std::cout << "========== Round " << T + 1 << " ==========" << std::endl;
      std::string val = std::string(val_size, 's');
      std::vector<int> keys(kKeyNum);
      for (int i = 0; i < kKeyNum; ++i) {
        keys[i] = i;
      }
      clock_t start_time, end_time;
      clock_t total_time;

      total_time = 0;
      shuffle(keys.begin(), keys.end(), std::mt19937(std::random_device()()));
      for (int i = 0; i < kKeyNum; ++i) {
        start_time = clock();
        kv.Put(keys[i], val);
        end_time = clock();
        total_time += end_time - start_time;
      }
      put_total_time += total_time;

      total_time = 0;
      shuffle(keys.begin(), keys.end(), std::mt19937(std::random_device()()));
      for (int i = 0; i < kKeyNum; ++i) {
        start_time = clock();
        kv.Get(keys[i]);
        end_time = clock();
        total_time += end_time - start_time;
      }
      get_total_time += total_time;

      total_time = 0;
      shuffle(keys.begin(), keys.end(), std::mt19937(std::random_device()()));
      for (int i = 0; i < kKeyNum; ++i) {
        start_time = clock();
        kv.Del(keys[i]);
        end_time = clock();
        total_time += end_time - start_time;
      }
      del_total_time += total_time;

      kv.Reset();
    }

    avg_delay = (double)put_total_time / kKeyNum / CLOCKS_PER_SEC / kRounds;
    throughput = 1 / (avg_delay);
    std::cout << "<PUT> Average delay: " << avg_delay << "s\t"
              << "Throughput: " << throughput << "ops/s" << std::endl;

    avg_delay = (double)get_total_time / kKeyNum / CLOCKS_PER_SEC / kRounds;
    throughput = 1 / (avg_delay);
    std::cout << "<GET> Average delay: " << avg_delay << "s\t"
              << "Throughput: " << throughput << "ops/s" << std::endl;

    avg_delay = (double)del_total_time / kKeyNum / CLOCKS_PER_SEC / kRounds;
    throughput = 1 / (avg_delay);
    std::cout << "<DEL> Average delay: " << avg_delay << "s\t"
              << "Throughput: " << throughput << "ops/s" << std::endl;
  }

  static void TestCompaction(KVStore &kv, int val_size, int sec) {
    size_t num_puts = 0;
    std::string val = std::string(val_size, 's');
    bool finished = false;

    auto counter = [&]() {
      std::cout << "Counter thread begin (" << sec << " seconds)." << std::endl;
      clock_t last_sec = 0;
      clock_t cur_sec = 0;
      size_t ops_last_sec = 0;

      while (cur_sec < sec) {
        cur_sec = clock() / CLOCKS_PER_SEC;
        if (cur_sec > last_sec) {
          last_sec = cur_sec;
          size_t current_puts = num_puts;
          size_t ops_this_sec = current_puts - ops_last_sec;
          ops_last_sec = current_puts;
          std::cout << ops_this_sec << ", " << std::flush;
        }
      }

      finished = true;
    };

    std::thread t(counter);
    std::default_random_engine r(time(0));

    while (!finished) {
      kv.Put(r(), val);
      ++num_puts;
    }

    t.join();
  }

  const int kKeyNum = 10000;
  const int kRounds = 4;
};

void Usage(const char *prog) {
  std::cout << "Usage: " << prog << " " << "regular | compaction" << std::endl;
  std::cout << "  regular: DoTest the performance of Get, Put and Del interface with different value sizes, as is described in section 3.3.2 of the report." << std::endl;
  std::cout << "  compaction: DoTest the performance of compaction as is described in section 3.3.4 of the report." << std::endl;
}

int main(int argc, char *argv[]) {
  if (argc == 2 && !strcmp(argv[1], "regular")) {
    std::vector<int> val_size = {50, 500, 5000, 50000, 500000};
    for (const int sz : val_size) {
      PerformanceTest test("./data");
      std::vector<int> args = {PerformanceTest::TestMode::kRegular, sz};
      test.StartTest(args.data());
    }
  } else if (argc == 2 &&!strcmp(argv[1], "compaction")) {
    PerformanceTest test("./data");
    std::vector<int> args = {PerformanceTest::TestMode::kCompaction, 128, 60};
    test.StartTest(args.data());
  } else {
    Usage(argv[0]);
  }
}
