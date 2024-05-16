#!/bin/bash
# 读取配置文件并构建命令行参数
args=""
while IFS= read -r line; do
    # 构建命令行参数
    if [ ! -z "$line" ]; then
        args+=" $line"
    fi
done < proxy_config.ini

cd Proxy/build && rm -rf ../backend/Proxy/Logs/* && rm -rf ../backend/Proxy/TXN_LOG/* exit

# 执行命令
make -j && numactl --membind=0 --cpunodebind=0 ./frontend/tpcc $args