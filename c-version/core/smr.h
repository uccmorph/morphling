#ifndef __CORE_SMR_H__
#define __CORE_SMR_H__

#include <stdint.h>

typedef uint64_t _index_t;

typedef struct {
  void* buf;
  unsigned int len;
} _entry_data_t;

typedef struct {
  _index_t term;
  _index_t id;
  _entry_data_t data;
} _entry_t;

typedef _entry_t _full_log_t;

typedef struct {
  uint32_t size;
  _entry_t *logs;
} _smr_log_t;

_smr_log_t* new_logs(uint32_t _size);

#endif