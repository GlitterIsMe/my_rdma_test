//
// Created by YiwenZhang on 2022/3/17.
//

#ifndef RDMA_UTIL_H
#define RDMA_UTIL_H

// Utility functions
static inline uint32_t hrd_fastrand(uint64_t* seed) {
    *seed = *seed * 1103515245 + 12345;
    return static_cast<uint32_t>((*seed) >> 32);
}

#endif //RDMA_UTIL_H
