#pragma once

#include <cstdint>
#include <iostream>
#include <string>

#include "kvstore.h"
#include "utils.h"

class Test {
 public:
  explicit Test(const std::string &dir, bool v = true)
      : kDir(dir), store_(dir), verbose_(v) {
    nr_tests_ = 0;
    nr_passed_tests_ = 0;
    nr_phases_ = 0;
    nr_passed_phases_ = 0;
  }

#define EXPECT(exp, got) Expect<decltype(got)>((exp), (got), __FILE__, __LINE__)
  template <typename T>
  void Expect(const T &exp, const T &got, const std::string &file, int line) {
    ++nr_tests_;
    if (exp == got) {
      ++nr_passed_tests_;
      return;
    }

    if (verbose_) {
      std::cerr << "TEST Error @" << file << ":" << line;
      std::cerr << ", Expect: " << exp;
      std::cerr << ", got " << got << std::endl;
    }
  }

  void Phase() {
    // Report result of current phase.
    std::cout << "  Phase " << (nr_phases_ + 1) << ": ";
    std::cout << nr_passed_tests_ << "/" << nr_tests_ << " ";

    // Count
    ++nr_phases_;
    if (nr_tests_ == nr_passed_tests_) {
      ++nr_passed_phases_;
      std::cout << "[PASS]" << std::endl;
    } else
      std::cout << "[FAIL]" << std::endl;

    std::cout.flush();

    // Reset
    nr_tests_ = 0;
    nr_passed_tests_ = 0;
  }

  /**
   * Report number of phases passed and Reset counters.
   */
  void Report() {
    std::cout << nr_passed_phases_ << "/" << nr_phases_ << " passed.";
    std::cout << std::endl;
    std::cout.flush();

    nr_phases_ = 0;
    nr_passed_phases_ = 0;
  }

  virtual void StartTest(void *args) {
    std::cout << "No DoTest is implemented." << std::endl;
    utils::Rmdir(kDir.data());
  }

 protected:
  static const std::string not_found_;

  const std::string kDir;

  KVStore store_;
  bool verbose_;

  uint64_t nr_tests_;
  uint64_t nr_passed_tests_;
  uint64_t nr_phases_;
  uint64_t nr_passed_phases_;
};

const std::string Test::not_found_;
