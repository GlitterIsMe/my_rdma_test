//
// Created by YiwenZhang on 2022/3/17.
//

#ifndef RDMA_PMEM_H
#define RDMA_PMEM_H

#include <libpmem.h>
#include <string>

namespace rdma {
    extern uint64_t pmem_size;
    extern size_t mapped_size;

    char* map_pmem_file(const std::string pmem_path);

    void unmap_file(char* raw);
}

#endif //RDMA_PMEM_H
