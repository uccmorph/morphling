
#include <cstdint>
#include <vector>

#include "guidance.h"

using group_t = size_t;
struct Entry {
  uint64_t epoch;
  uint64_t index;
  void *data;
};

class SMRLog {
  std::vector<std::vector<Entry>> m_log;

 public:
  SMRLog(size_t size) {
    for (size_t i = 0; i < size; i++) {
      m_log.emplace_back(std::vector<Entry>());
    }
  }

  uint64_t append(Entry &e, group_t group_id);

  Entry &entry_at(uint64_t index, group_t group_id);
};

struct AppendEntriesMessage {
  uint64_t epoch;
  uint64_t prev_term;
  uint64_t prev_index;
  uint64_t commit;

  std::vector<Entry> entries;
};

struct AppenEntriesReplyMessage {
  bool success;
  uint64_t epoch;
  uint64_t index;
};

class MPReplica {
  SMRLog m_log;
  guidance_t *m_guide;
  int m_me;

 public:
  MPReplica();
  ~MPReplica();

  void handle_append_entries();
  void handle_append_entries_reply();
  void handle_operation();
};