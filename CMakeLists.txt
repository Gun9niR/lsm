cmake_minimum_required(VERSION 3.21)
project(lsm)

set(CMAKE_CXX_STANDARD 14)

add_executable(correctness_test test/correctness.cc src/kvstore.cc src/skip_list.cc src/sstable.cc)
add_executable(persistence_test test/persistence.cc src/kvstore.cc src/skip_list.cc src/sstable.cc)
add_executable(performance_test test/performance.cc src/kvstore.cc src/skip_list.cc src/sstable.cc)
add_executable(demo src/demo.cc src/kvstore.cc src/skip_list.cc src/sstable.cc)

include_directories(include)