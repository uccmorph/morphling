#include <stdlib.h>
#include <loguru.hpp>

#include "smr.h"

void MPReplica::init_local_guidance(int kay_space) {
  m_guide.epoch = 1;
  if (m_peers.size() != HARD_CODE_REPLICAS) {
    LOG_F(ERROR, "peers size %zu should be equal to %d", m_peers.size(), HARD_CODE_REPLICAS);
    exit(-1);
  }
  m_guide.alive_num = m_peers.size();
  m_guide.cluster_size = m_peers.size();
}

void MPReplica::handle_append_entries(AppendEntriesMessage &msg) {

}