#pragma once
#include <iostream>
#include <fstream>
#include <sstream>
#include <mutex>

class Log
{
private:
    std::string txn_key_filename;
    std::ofstream txn_key_File;
    std::mutex txn_lock;
    Log()
    {
        std::string currentFile = __FILE__; // 获取当前源文件的路径
        // 基于源文件路径构建日志文件的相对路径
        std::string abstract_filename = currentFile.substr(0, currentFile.find_last_of("/\\") + 1);
        txn_key_filename = abstract_filename + "../Logs/txn_key_log";
        txn_key_File.open(txn_key_filename); // Open file in append mode
        if (!txn_key_File.is_open())
        {
            std::cerr << "Error opening count file!" << std::endl;
            // Handle error accordingly, maybe throw an exception or exit
        }
    }

    ~Log()
    {
        if (txn_key_File.is_open())
        {
            txn_key_File.close();
        }
    }

public:
    // 禁止拷贝和赋值构造函数
    Log(const Log &) = delete;
    void operator=(const Log &) = delete;
    static Log &getInstance()
    {
        static Log instance; // 创建唯一实例
        return instance;
    }
    template <typename... Args>
    void writeCountLog(Args &&...args);
    void writekeyLog(int w_id, std::vector<int> &key_list){
        txn_lock.lock();
        txn_key_File<< w_id <<" ";
        for(const auto& key : key_list){
            txn_key_File<< key <<" ";
        }
        txn_key_File<<std::endl;
        txn_lock.unlock();
    }
};

template <typename... Args>
void Log::writeCountLog(Args &&...args)
{

    std::ostringstream oss;
    (oss << ... << std::forward<Args>(args)); // Pack expansion: concatenate all args into oss
    txn_lock.lock();
    txn_key_File << oss.str();
    txn_key_File.flush(); // Manually flush the buffer
    txn_lock.unlock();
}