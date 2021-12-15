#include <stdint.h>


#ifndef __CORE_GUIDANCE_H__
#define __CORE_GUIDANCE_H__
#ifdef __cplusplus
extern "C" {
#endif

#ifndef HARD_CODE_REPLICAS
#define HARD_CODE_REPLICAS 3
#endif

typedef struct __attribute__((__packed__)) {
  uint8_t start_key_pos;
  uint8_t end_key_pos;
  uint8_t alive;
} node_status_t;

typedef struct __attribute__((__packed__)) {
  uint64_t epoch;
  uint8_t alive_num;
  uint8_t cluster_size;
  node_status_t cluster[HARD_CODE_REPLICAS];
} guidance_t;

#ifdef __cplusplus
}
#endif

#endif //__CORE_GUIDANCE_H__