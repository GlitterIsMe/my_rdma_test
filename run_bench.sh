#! /bin/bash

cmd="./rdma_server"

if [ $1 == "server" ]; then
    # for RDMA write and cas
    #cmd="$cmd --addr=0.0.0.0 --use_pmem=1 --pmem_size=0 --is_server=1 --pmem_path=/dev/dax0.2"
    # for RDMA recv
    cmd="$cmd --addr=0.0.0.0
              --use_pmem=0
              --pmem_size=4096
              --is_server=1
              --pmem_path=/dev/dax0.2
              --max_post_list=1
              --num_threads=1
              --block_size=16
              --ops=10000000
              --benchmark=echo"

elif [ $1 == "client" ]; then
    cmd="$cmd --addr=192.168.1.88
              --is_server=0
              --pmem_size=4096
              --max_post_list=1
              --num_threads=1
              --block_size=16
              --ops=10000000
              --benchmark=echo"
fi

echo ${cmd}
eval ${cmd}

