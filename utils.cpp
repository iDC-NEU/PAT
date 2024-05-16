#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <iterator>
#include <string>
#include <sstream>
#include <cmath>

// 用于读取文件并生成有序子文件的函数
void createSortedSubfiles(const std::string &input_filename, const std::string &output_filename_base, long long max_memory)
{
    std::ifstream input_file(input_filename);
    if (!input_file)
    {
        std::cerr << "无法打开输入文件!" << std::endl;
        return;
    }

    int subfile_count = 0;
    std::vector<int> buffer;
    long long buffer_size = 0;
    std::string line;
    while (std::getline(input_file, line))
    {
        std::stringstream ss(line);
        int num;
        while (ss >> num)
        {
            // 检查当前缓冲区大小是否已超过限制
            if (buffer_size + sizeof(int) > max_memory)
            {
                // 超过限制，对缓冲区排序并写入子文件
                std::sort(buffer.begin(), buffer.end());
                std::stringstream filename;
                filename << output_filename_base << "_" << subfile_count << ".txt";
                std::ofstream subfile(filename.str());
                if (!subfile)
                {
                    std::cerr << "无法创建临时文件!" << std::endl;
                    return;
                }
                for (int val : buffer)
                {
                    subfile << val << " ";
                }
                subfile.close();
                subfile_count++;
                buffer.clear();
                buffer_size = 0;
            }
            buffer.push_back(num);
            buffer_size += sizeof(int);
        }
    }

    // 处理剩余数据
    if (!buffer.empty())
    {
        std::sort(buffer.begin(), buffer.end());
        std::stringstream filename;
        filename << output_filename_base << "_" << subfile_count << ".txt";
        std::ofstream subfile(filename.str());
        if (!subfile)
        {
            std::cerr << "无法创建临时文件!" << std::endl;
            return;
        }
        for (int val : buffer)
        {
            subfile << val << " ";
        }
        subfile.close();
        subfile_count++;
    }
}

// 合并有序子文件的函数（带有内存限制）
void mergeSortedFiles(const std::vector<std::string> &filenames, const std::string &output_filename, long long max_memory)
{
    std::vector<std::ifstream> files;
    std::vector<int> buffer;                  // 缓冲区
    buffer.reserve(max_memory / sizeof(int)); // 根据内存限制调整缓冲区大小

    // 打开所有子文件
    for (const auto &filename : filenames)
    {
        files.emplace_back(filename);
    }

    // 创建输出文件
    std::ofstream output_file(output_filename);
    if (!output_file)
    {
        std::cerr << "无法创建输出文件!" << std::endl;
        return;
    }

    // 逐个读取子文件，并将数据放入缓冲区中
    for (auto &file : files)
    {
        int num;
        while (file >> num)
        {
            buffer.push_back(num);
            // 如果缓冲区已满，则对缓冲区进行排序并写入输出文件
            if (buffer.size() * sizeof(int) >= max_memory)
            {
                std::sort(buffer.begin(), buffer.end());
                for (int val : buffer)
                {
                    output_file << val << " ";
                }
                buffer.clear();
            }
        }
        file.close();
    }

    // 对剩余的数据进行排序并写入输出文件
    std::sort(buffer.begin(), buffer.end());
    for (int val : buffer)
    {
        output_file << val << " ";
    }
    buffer.clear();

    // 关闭输出文件
    output_file.close();
}

// 记录延迟的结构体
struct Transaction
{
    int time;
    double delay;

    Transaction(int t, double d) : time(t), delay(d) {}
};

// 计算前百分之几的平均延迟
double calculatePercentileDelay(const std::string &input_filename, double percentile)
{
    std::ifstream input_file(input_filename);
    if (!input_file)
    {
        std::cerr << "无法打开输入文件!" << std::endl;
        return -1;
    }

    std::vector<Transaction> transactions;
    std::string line;
    while (std::getline(input_file, line))
    {
        std::istringstream iss(line);
        int time;
        double delay;
        while (iss >> time >> delay)
        {
            transactions.emplace_back(time, delay);
        }
    }

    if (transactions.empty())
    {
        std::cerr << "文件为空!" << std::endl;
        return -1;
    }

    std::sort(transactions.begin(), transactions.end(), [](const Transaction &a, const Transaction &b)
              { return a.delay < b.delay; });

    int index = static_cast<int>(percentile * transactions.size());
    double sum = 0;
    for (int i = 0; i < index; ++i)
    {
        sum += transactions[i].delay;
    }
    return sum / index;
}

void generate_file(std::string &input_filename, std::string &output_filename)
{

    // 设置内存限制，单位为字节
    long long max_memory = 16LL * 1024 * 1024 * 1024; // 16GB的字节数

    // 创建有序子文件
    createSortedSubfiles(input_filename, "temp", max_memory);

    // 获取临时文件列表
    std::vector<std::string> temp_files;
    for (int i = 0;; ++i)
    {
        std::stringstream filename;
        filename << "temp_" << i << ".txt";
        std::ifstream temp_file(filename.str());
        if (!temp_file)
        {
            break;
        }
        temp_files.push_back(filename.str());
        temp_file.close();
    }

    // 合并有序子文件（带有内存限制）
    mergeSortedFiles(temp_files, output_filename, max_memory);

    // 删除临时文件
    for (const auto &filename : temp_files)
    {
        std::remove(filename.c_str());
    }

    std::cout << "文件已成功排序并保存为 " << output_filename << std::endl;
}

void lantency_count(std::string &input_filename, std::string &output_filename, double percentile){
     // 百分位数（0.9表示取前90%的平均延迟）
     std::ofstream output(output_filename);
    // 计算前百分之几的平均延迟
    double avg_delay = calculatePercentileDelay(input_filename, percentile);
    if (avg_delay != -1) {
        output << "前" << percentile * 100 << "% 的平均延迟为: " << avg_delay << std::endl;
    }

}

int main()
{
    // 输入文件和输出文件名
    std::string input_filename1 = "/root/home/AffinityDB/data/时间分解/哈希路由/原版tpcc/txn_lantency";
    std::string output_filename1 = "/root/home/AffinityDB/data/时间分解/哈希路由/原版tpcc/txn_sort_lantency";
    std::string output_count1 = "/root/home/AffinityDB/data/时间分解/哈希路由/原版tpcc/txn_sort_info";
    std::string input_filename2 = "/root/home/AffinityDB/data/时间分解/哈希路由/原版tpcc/route_lantency";
    std::string output_filename2 = "/root/home/AffinityDB/data/时间分解/哈希路由/原版tpcc/route_sort_lantency";
    std::string output_count2 = "/root/home/AffinityDB/data/时间分解/哈希路由/原版tpcc/route_sort_info";
    generate_file(input_filename1, output_filename1);
    generate_file(input_filename2, output_filename2);
    double percentile = 0.99;
    lantency_count(output_filename1, output_count1, percentile);
    lantency_count(output_filename2, output_count2, percentile);
    return 0;
}
