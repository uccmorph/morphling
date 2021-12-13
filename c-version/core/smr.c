#include <stdlib.h>

#include "smr.h"

_smr_log_t* new_logs(uint32_t _size) {
    _smr_log_t *log = (_smr_log_t *)malloc(sizeof(_smr_log_t));
    log->size = _size;
    log->logs = malloc(sizeof(_entry_t) * 10);

    return log;
}