//
// Created by YiwenZhang on 2022/3/17.
//

#include "pmem.h"

namespace rdma {
    uint64_t pmem_size;

    int is_pmem;
    size_t mapped_size;

    char* map_pmem_file(const std::string pmem_path) {
        char* raw = (char*)pmem_map_file(pmem_path.c_str(), pmem_size,
                                         PMEM_FILE_CREATE, 0666,
                                         &mapped_size, &is_pmem);
        if (raw == nullptr) {
            fprintf(stderr, "Map pmem file failed\n");
        }
        printf("map pmem file [%ld] GB\n", mapped_size / 1024 / 1024 / 1024);
        return raw;
    }

    void unmap_file(char* raw) {
        pmem_unmap(raw, mapped_size);
    }
}
