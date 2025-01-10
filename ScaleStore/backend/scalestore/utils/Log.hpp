#pragma once
#include <iostream>
#include <fstream>
#include <sstream>
#include <mutex>

class Log
{
private:
    std::ofstream txnLogFile;
    std::string txn_info_filename;
    std::ofstream rdmasendkey_File;
    std::string rdmasendkey_filename;
    std::ofstream rdmasendsql_File;
    std::string rdmasendsql_filename;
    std::ofstream rdmagetsql_File;
    std::string rdmagetsql_filename;
    std::mutex txnLogMutex;         // 用于保护 txn log 的互斥锁
    std::mutex rdmasendkeyLogMutex; // 用于保护 rdma log 的互斥锁
    std::mutex rdmasendsqlLogMutex; // 用于保护 rdma log 的互斥锁
    std::mutex rdmagetsqlLogMutex; // 用于保护 rdma log 的互斥锁
    Log()
    {
        std::string currentFile = __FILE__; // 获取当前源文件的路径
        // 基于源文件路径构建日志文件的相对路径
        std::string abstract_filename = currentFile.substr(0, currentFile.find_last_of("/\\") + 1);
        txn_info_filename = abstract_filename + "../../../Logs/txn_info_log";
        txnLogFile.open(txn_info_filename); // Open file in append mode
        if (!txnLogFile.is_open())
        {
            std::cerr << "Error opening txn log file!" << std::endl;
            // Handle error accordingly, maybe throw an exception or exit
        }
        rdmasendkey_filename = abstract_filename + "../../../Logs/rdmasendkey_log";
        rdmasendkey_File.open(rdmasendkey_filename); // Open file in append mode
        if (!rdmasendkey_File.is_open())
        {
            std::cerr << "Error opening rdma log file!" << std::endl;
            // Handle error accordingly, maybe throw an exception or exit
        }
        rdmasendsql_filename = abstract_filename + "../../../Logs/rdmasendsql_log";
        rdmasendsql_File.open(rdmasendsql_filename); // Open file in append mode
        if (!rdmasendsql_File.is_open())
        {
            std::cerr << "Error opening rdma log file!" << std::endl;
            // Handle error accordingly, maybe throw an exception or exit
        }
        rdmagetsql_filename = abstract_filename + "../../../Logs/rdmagetsql_log";
        rdmagetsql_File.open(rdmagetsql_filename); // Open file in append mode
        if (!rdmagetsql_File.is_open())
        {
            std::cerr << "Error opening rdma log file!" << std::endl;
            // Handle error accordingly, maybe throw an exception or exit
        }
    }

    ~Log()
    {
        if (txnLogFile.is_open())
        {
            txnLogFile.close();
        }
        if (rdmasendsql_File.is_open())
        {
            rdmasendsql_File.close();
        }
        if (rdmasendkey_File.is_open())
        {
            rdmasendkey_File.close();
        }
        if (rdmagetsql_File.is_open())
        {
            rdmagetsql_File.close();
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
    void writeTxnLog(Args &&...args);

    template <typename... Args>
    void writeRDMAkeyLog(Args &&...args);
    template <typename... Args>
    void writeRDMAsqlLog(Args &&...args);
    template <typename... Args>
    void writeRDMAgetsqlLog(Args &&...args);
};

template <typename... Args>
void Log::writeTxnLog(Args &&...args)
{
    std::lock_guard<std::mutex> lock(txnLogMutex);
    if (txnLogFile.is_open())
    {
        std::ostringstream oss;
        (oss << ... << std::forward<Args>(args)); // Pack expansion: concatenate all args into oss
        txnLogFile << oss.str();
        txnLogFile.flush(); // Manually flush the buffer
    }
    else
    {
        std::cerr << "Txn log file is not open!" << std::endl;
        // Handle error accordingly
    }
}

template <typename... Args>
void Log::writeRDMAkeyLog(Args &&...args)
{
    std::lock_guard<std::mutex> lock(rdmasendkeyLogMutex);
    if (rdmasendkey_File.is_open())
    {
        std::ostringstream oss;
        (oss << ... << std::forward<Args>(args)); // Pack expansion: concatenate all args into oss
        rdmasendkey_File << oss.str();
        rdmasendkey_File.flush(); // Manually flush the buffer
    }
    else
    {
        std::cerr << "Rdma log file is not open!" << std::endl;
        // Handle error accordingly
    }
}

template <typename... Args>
void Log::writeRDMAsqlLog(Args &&...args)
{
    std::lock_guard<std::mutex> lock(rdmasendsqlLogMutex);
    if (rdmasendsql_File.is_open())
    {
        std::ostringstream oss;
        (oss << ... << std::forward<Args>(args)); // Pack expansion: concatenate all args into oss
        rdmasendsql_File << oss.str();
        rdmasendsql_File.flush(); // Manually flush the buffer
    }
    else
    {
        std::cerr << "Rdma log file is not open!" << std::endl;
        // Handle error accordingly
    }
}
template <typename... Args>
void Log::writeRDMAgetsqlLog(Args &&...args)
{
    std::lock_guard<std::mutex> lock(rdmagetsqlLogMutex);
    if (rdmagetsql_File.is_open())
    {
        std::ostringstream oss;
        (oss << ... << std::forward<Args>(args)); // Pack expansion: concatenate all args into oss
        rdmagetsql_File << oss.str();
        rdmagetsql_File.flush(); // Manually flush the buffer
    }
    else
    {
        std::cerr << "Rdma log file is not open!" << std::endl;
        // Handle error accordingly
    }
}