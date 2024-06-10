#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <iterator>
#include <string>
#include <sstream>
#include <cmath>
#include <regex>

void sortLinesInFile(const std::string &input_filename, const std::string &output_filename)
{
    std::ifstream input_file(input_filename);
    if (!input_file)
    {
        std::cerr << "无法打开输入文件!" << std::endl;
        return;
    }

    std::ofstream output_file(output_filename);
    if (!output_file)
    {
        std::cerr << "无法打开输出文件!" << std::endl;
        return;
    }

    std::string line;
    while (std::getline(input_file, line))
    {
        std::stringstream ss(line);
        std::vector<int> numbers;
        int num;
        while (ss >> num)
        {
            numbers.push_back(num);
        }

        // 对每行数据进行排序
        std::sort(numbers.begin(), numbers.end());

        // 将排序后的数据写入输出文件
        for (size_t i = 0; i < numbers.size(); ++i)
        {
            output_file << numbers[i];
            if (i < numbers.size() - 1)
            {
                output_file << " ";
            }
        }
        output_file << std::endl; // 保留换行符
    }

    input_file.close();
    output_file.close();
    std::cout << "文件已成功排序并保存为 " << output_filename << std::endl;
}

void calculateAndWriteDelays(const std::string &input_filename, const std::string &output_filename, double percentile)
{
    std::ifstream input_file(input_filename);
    if (!input_file)
    {
        std::cerr << "无法打开输入文件!" << std::endl;
        return;
    }

    std::ofstream output_file(output_filename);
    if (!output_file)
    {
        std::cerr << "无法打开输出文件!" << std::endl;
        return;
    }

    std::string line;
    int count = 0;
    while (std::getline(input_file, line))
    {
        std::istringstream iss(line);
        std::vector<double> delays;
        double delay;
        count++;
        while (iss >> delay)
        {
            delays.push_back(delay);
        }

        if (delays.empty())
        {
            continue; // 如果这一行没有数据，跳过处理
        }

        std::sort(delays.begin(), delays.end());

        // 计算平均延迟
        double sum = 0;
        for (double d : delays)
        {
            sum += d;
        }
        double average_delay = sum / delays.size();

        // 计算中位数延迟
        size_t size = delays.size();
        double median_delay;
        if (size % 2 == 0)
        {
            median_delay = (delays[size / 2 - 1] + delays[size / 2]) / 2.0;
        }
        else
        {
            median_delay = delays[size / 2];
        }

        // 计算指定百分位数的尾延迟
        size_t index = static_cast<size_t>(percentile * size);
        if (index >= size)
        {
            index = size - 1;
        }
        double tail_delay = delays[index];

        // 将结果写入文件
        output_file << "time: " << count * 10 << "s" << std::endl;
        output_file << "平均延迟: " << average_delay << std::endl;
        output_file << "中位数延迟: " << median_delay << std::endl;
        output_file << percentile * 100 << "%的尾延迟: " << tail_delay << std::endl;
        output_file << std::endl;
    }

    input_file.close();
    output_file.close();
    std::cout << "结果已写入到 " << output_filename << " 文件中。" << std::endl;
}

void mergeLinesToFile(const std::string &input_filename, const std::string &output_filename)
{
    std::ifstream input_file(input_filename);
    if (!input_file)
    {
        std::cerr << "无法打开输入文件!" << std::endl;
        return;
    }

    std::ofstream output_file(output_filename);
    if (!output_file)
    {
        std::cerr << "无法打开输出文件!" << std::endl;
        return;
    }

    std::string line;
    std::string merged_content;
    while (std::getline(input_file, line))
    {
        merged_content += line + " "; // 添加每行内容并在末尾添加一个空格作为分隔符
    }

    // 移除末尾的多余空格
    if (!merged_content.empty())
    {
        merged_content.pop_back();
    }

    output_file << merged_content;

    input_file.close();
    output_file.close();
    std::cout << "文件已成功合并并保存为 " << output_filename << std::endl;
}

void caculate_txn_lantxncy(std::string path)
{

    // 输入文件和输出文件名
    double percentile = 0.99;
    std::string input_filename1 = path + "txn_lantency";
    std::string output_filename1 = path + "txn_sort_lantency";
    std::string output_count1 = path + "txn_sort_info";
    std::string input_filename2 = path + "route_lantency";
    std::string output_filename2 = path + "route_sort_lantency_tmp";
    std::string output_filename3 = path + "route_sort_lantency";
    std::string output_count2 = path + "route_sort_info";
    sortLinesInFile(input_filename1, output_filename1);
    calculateAndWriteDelays(output_filename1, output_count1, percentile);
    mergeLinesToFile(input_filename2, output_filename2);
    sortLinesInFile(output_filename2, output_filename3);
    calculateAndWriteDelays(output_filename3, output_count2, percentile);
    std::string rm_str = "rm -rf " + output_filename2;
    system(rm_str.c_str());
}

void calculate_remote(std::string path, int nodes)
{
    // 打开输出文件
    std::string output_filename = path + "remote_result";
    std::ofstream output_file(output_filename);
    if (!output_file)
    {
        std::cerr << "无法打开输出文件!" << std::endl;
        return;
    }
    std::vector<std::string> filenames;
    filenames.push_back(path + "node1/Logs/remote_node0.txt");
    filenames.push_back(path + "node2/Logs/remote_node1.txt");
    for(int i = 0; i< nodes; i++){
        filenames.push_back(path + "node" + std::to_string(i+1) + "/Logs/remote_node" + std::to_string(i) + ".txt");
    }

    // 用于存储每行的总和
    std::vector<long long> total_remote_counts;
    std::vector<double> total_increase_counts;
    // 遍历所有文件
    for (const auto &filename : filenames)
    {
        std::ifstream input_file(filename);
        if (!input_file)
        {
            std::cerr << "无法打开输入文件: " << filename << std::endl;
            continue;
        }

        std::string line;
        size_t line_index = 0;
        while (std::getline(input_file, line))
        {
            // 使用正则表达式提取 remote_count 和 increase_count 的值
            std::regex regex(R"(\[.*?\] remote_count\s*=\s*(\d+); increase_count\s*=\s*([\d.]+)/s)");
            std::smatch match;
            if (std::regex_match(line, match, regex))
            {
                long long remote_count = std::stoll(match[1].str());
                double increase_count = std::stod(match[2].str());

                // 如果这是第一次读取这一行，初始化总和
                if (total_remote_counts.size() <= line_index)
                {
                    total_remote_counts.push_back(0);
                    total_increase_counts.push_back(0.0);
                }

                // 累加每行的值
                total_remote_counts[line_index] += remote_count;
                total_increase_counts[line_index] += increase_count;

                // 进入下一行
                line_index++;
            }
        }

        input_file.close();
    }

    // 将累加结果写入输出文件
    for (size_t i = 0; i < total_remote_counts.size(); ++i)
    {
        output_file << total_increase_counts[i] << std::endl;
    }

    output_file.close();
    std::cout << "统计结果已成功保存到 " << output_filename << std::endl;
}

void calculate_remote_batch(int nodes)
{
    std::string path1 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/无图划分路由/随机路由/无分布式/2/";
    calculate_remote(path1, nodes);
    std::string path2 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/无图划分路由/随机路由/原版tpcc/2/";
    calculate_remote(path2, nodes);
    std::string path3 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/无图划分路由/随机路由/分布式30/2/";
    calculate_remote(path3, nodes);
    std::string path4 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/无图划分路由/随机路由/分布式50/2/";
    calculate_remote(path4, nodes);
    std::string path5 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/无图划分路由/哈希路由/无分布式/2/";
    calculate_remote(path5, nodes);
    std::string path6 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/无图划分路由/哈希路由/原版tpcc/2/";
    calculate_remote(path6, nodes);
    std::string path7 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/无图划分路由/哈希路由/分布式30/2/";
    calculate_remote(path7, nodes);
    std::string path8 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/无图划分路由/哈希路由/分布式50/2/";
    calculate_remote(path8, nodes);
    std::string path11 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/静态/无codesign/无分布式/2/";
    calculate_remote(path11, nodes);
    std::string path12 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/静态/无codesign/原版tpcc/2/";
    calculate_remote(path12, nodes);
    std::string path13 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/静态/无codesign/分布式30/2/";
    calculate_remote(path13, nodes);
    std::string path14 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/静态/无codesign/分布式50/2/";
    calculate_remote(path14, nodes);
    std::string path15 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/静态/有codesign/无分布式/2/";
    calculate_remote(path15, nodes);
    std::string path16 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/静态/有codesign/原版tpcc/2/";
    calculate_remote(path16, nodes);
    std::string path17 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/静态/有codesign/分布式30/2/";
    calculate_remote(path17, nodes);
    std::string path18 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/静态/有codesign/分布式50/2/";
    calculate_remote(path18, nodes);
    std::string path21 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/动态/无codesign/无分布式/2/";
    calculate_remote(path21, nodes);
    std::string path22 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/动态/无codesign/原版tpcc/2/";
    calculate_remote(path22, nodes);
    std::string path23 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/动态/无codesign/分布式30/2/";
    calculate_remote(path23, nodes);
    std::string path24 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/动态/无codesign/分布式50/2/";
    calculate_remote(path24, nodes);
    std::string path25 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/动态/有codesign/无分布式/2/";
    calculate_remote(path25, nodes);
    std::string path26 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/动态/有codesign/原版tpcc/2/";
    calculate_remote(path26, nodes);
    std::string path27 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/动态/有codesign/分布式30/2/";
    calculate_remote(path27, nodes);
    std::string path28 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/动态/有codesign/分布式50/2/";
    calculate_remote(path28, nodes);
}
void lantency_batch(int nodes)
{
    std::string path1 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/无图划分路由/随机路由/无分布式/2/";
    caculate_txn_lantxncy(path1);
    std::string path2 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/无图划分路由/随机路由/原版tpcc/2/";
    caculate_txn_lantxncy(path2);
    std::string path3 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/无图划分路由/随机路由/分布式30/2/";
    caculate_txn_lantxncy(path3);
    std::string path4 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/无图划分路由/随机路由/分布式50/2/";
    caculate_txn_lantxncy(path4);
    std::string path5 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/无图划分路由/哈希路由/无分布式/2/";
    caculate_txn_lantxncy(path5);
    std::string path6 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/无图划分路由/哈希路由/原版tpcc/2/";
    caculate_txn_lantxncy(path6);
    std::string path7 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/无图划分路由/哈希路由/分布式30/2/";
    caculate_txn_lantxncy(path7);
    std::string path8 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/无图划分路由/哈希路由/分布式50/2/";
    caculate_txn_lantxncy(path8);
    std::string path11 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/静态/无codesign/无分布式/2/";
    caculate_txn_lantxncy(path11);
    std::string path12 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/静态/无codesign/原版tpcc/2/";
    caculate_txn_lantxncy(path12);
    std::string path13 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/静态/无codesign/分布式30/2/";
    caculate_txn_lantxncy(path13);
    std::string path14 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/静态/无codesign/分布式50/2/";
    caculate_txn_lantxncy(path14);
    std::string path15 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/静态/有codesign/无分布式/2/";
    caculate_txn_lantxncy(path15);
    std::string path16 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/静态/有codesign/原版tpcc/2/";
    caculate_txn_lantxncy(path16);
    std::string path17 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/静态/有codesign/分布式30/2/";
    caculate_txn_lantxncy(path17);
    std::string path18 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/静态/有codesign/分布式50/2/";
    caculate_txn_lantxncy(path18);
    std::string path21 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/动态/无codesign/无分布式/2/";
    caculate_txn_lantxncy(path21);
    std::string path22 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/动态/无codesign/原版tpcc/2/";
    caculate_txn_lantxncy(path22);
    std::string path23 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/动态/无codesign/分布式30/2/";
    caculate_txn_lantxncy(path23);
    std::string path24 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/动态/无codesign/分布式50/2/";
    caculate_txn_lantxncy(path24);
    std::string path25 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/动态/有codesign/无分布式/2/";
    caculate_txn_lantxncy(path25);
    std::string path26 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/动态/有codesign/原版tpcc/2/";
    caculate_txn_lantxncy(path26);
    std::string path27 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/动态/有codesign/分布式30/2/";
    caculate_txn_lantxncy(path27);
    std::string path28 = "/root/home/AffinityDB/data3/" + std::to_string(nodes) + "nodes/图划分路由/动态/有codesign/分布式50/2/";
    caculate_txn_lantxncy(path28);
}

// int main()
// {
//     std::string path28 = "/root/home/AffinityDB/data3/2nodes/图划分路由/动态/有codesign/分布式50/1/";
//     calculate_remote(path28, 2);
//     lantency_batch(2);
//     calculate_remote_batch(2);
//     return 0;
// }
