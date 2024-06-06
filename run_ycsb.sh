#!/bin/bash
# 读取配置文件并构建命令行参数
args=""
while IFS= read -r line || [ -n "$line" ]; do
    # 构建命令行参数
    if [ ! -z "$line" ]; then
        args+=" $line"
    fi
done < ycsb_config.ini

cd ScaleStore/build && rm -rf ../Logs/* && rm -rf ../TXN_LOG/* &&  touch ../tpcc_data || exit

# 执行命令
make -j && numactl --membind=0 --cpunodebind=0 ./frontend/ycsb $args
