#include <vector>
#include <loguru.hpp>
#include "smr.h"

int main(int argc, char **argv) {
    loguru::init(argc, argv);
    std::vector<int> peers{1, 2};
    MPReplica r(1, peers); 
}