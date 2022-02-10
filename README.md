# LSM Persistent K-V Storage

This is an implementation of the log-structured merge tree, one of the course 
project of *SE2322 - Advanced Data Structure* @SJTU-SE.

## Interface

[kvstore_api.h](include/kvstore_api.h) specifies the interfaces provided by the 
storage system.

## Building

### Building Makefile

```bash
rmdir build
cmake ..
```

### Building the tests

Under project root

```bash
cd build
make correctness_test
make persistence_test
make performance_test
```

- `correctness_test` tests the correctness of the system by calling `Put`, 
`Get`, `Del` for a large number of times and in different order.
- `persistence_test` tests whether the system can restore its state from data files.
- `performance_test` performs the benchmarking described in the project 
[report](LSM-report.pdf).

### Building the demo

```bash
cd build
make demo
```

## Benchmarking

See the [report](LSM-report.pdf) (in Chinese). 