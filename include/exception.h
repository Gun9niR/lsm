#ifndef LSM_EXCEPTION_H
#define LSM_EXCEPTION_H

#include <exception>

class MemTableFull: public std::exception {};

#endif