#! /bin/bash

cmd="./rdma_server"

if [ $1 == "server" ]; then
    cmd="$cmd --addr=0.0.0.0 --use_pmem=1 --pmem_size=0 --is_server=1 --pmem_path=/dev/dax0.2"
elif [ $1 == "client" ]; then
    cmd="$cmd --addr=192.168.1.88 --is_server=0 --pmem_size=32768 --max_batch_signal=1 --max_post_list=1 --num_threads=40 --block_size=16 --ops=10000000 --persist=true --random=true"
fi

echo ${cmd}
eval ${cmd}

