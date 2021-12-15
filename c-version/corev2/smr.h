
#ifndef __CORE_SMR_H__
#define __CORE_SMR_H__

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

struct ClientProposalMessage {
  guidance_t guidance;
  uint64_t key_hash;
  void *data;
  size_t data_size;
};

const int DEFAULT_KEY_SPACE = 256;

class MPReplica {
  SMRLog m_log;
  guidance_t m_guide;
  int m_me;
  std::vector<int> m_peers;

private:
  void init_local_guidance(int kay_space = DEFAULT_KEY_SPACE);

public:
  MPReplica(int id, std::vector<int> &peers): m_log(SMRLog(256)), m_me(id), m_peers(peers) {
    init_local_guidance();
  }
  ~MPReplica(){}

  void handle_append_entries(AppendEntriesMessage &msg);
  void handle_append_entries_reply(AppenEntriesReplyMessage &msg);
  void handle_operation(ClientProposalMessage &msg);
};

#endif //__CORE_SMR_H__