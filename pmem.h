//
// Created by YiwenZhang on 2022/3/17.
//

#ifndef RDMA_PMEM_H
#define RDMA_PMEM_H

#include <libpmem.h>
#include <string>

namespace rdma {
    extern uint64_t pmem_size;

    char* map_pmem_file(const std::string pmem_path);
}

#endif //RDMA_PMEM_H
