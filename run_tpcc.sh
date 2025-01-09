#!/bin/bash
# 读取配置文件并构建命令行参数
#!/bin/bash

# 从 proxy.ini 文件中读取 --stamp_len 的值
STAMP_LEN=$(grep -oP '^--stamp_len=\K\d+' proxy_config.ini)

# 如果 --stamp_len 存在且有值
if [ -z "$STAMP_LEN" ]; then
    echo "Error: --stamp_len not found in proxy.ini"
    exit 1
fi

# 检查 tpcc_config.ini 中是否已存在 --stamp_len
if grep -q "^--stamp_len=" tpcc_config.ini; then
    # 使用 sed 来更新 --stamp_len 的值
    sed -i "s/^--stamp_len=[^ ]*/--stamp_len=$STAMP_LEN/" tpcc_config.ini
else
    # 如果 --stamp_len 不存在，追加到文件末尾
    echo "--stamp_len=$STAMP_LEN" >> tpcc_config.ini
fi

args=""
while IFS= read -r line || [ -n "$line" ]; do
    # 构建命令行参数
    if [ ! -z "$line" ]; then
        args+=" $line"
    fi
done < tpcc_config.ini

cd ScaleStore/build && rm -rf ../Logs/* && rm -rf ../TXN_LOG/* &&  touch ../tpcc_data || exit

# 执行命令
make -j && numactl --membind=0 --cpunodebind=0 ./frontend/tpcc $args
