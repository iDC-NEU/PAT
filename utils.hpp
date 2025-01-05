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

void caculate_router_lantency(std::string path)
{

    // 输入文件和输出文件名
    double percentile = 0.99;
    // std::string input_filename1 = path + "txn_lantency";
    // std::string output_filename1 = path + "txn_sort_lantency";
    // std::string output_count1 = path + "txn_sort_info";
    std::string input_filename2 = path + "route_lantency";
    std::string output_filename2 = path + "route_sort_lantency_tmp";
    std::string output_filename3 = path + "route_sort_lantency";
    std::string output_count2 = path + "route_sort_info";
    // sortLinesInFile(input_filename1, output_filename1);
    // calculateAndWriteDelays(output_filename1, output_count1, percentile);
    mergeLinesToFile(input_filename2, output_filename2);
    sortLinesInFile(output_filename2, output_filename3);
    calculateAndWriteDelays(output_filename3, output_count2, percentile);
    std::string rm_str = "rm -rf " + output_filename2;
    system(rm_str.c_str());
    rm_str = "rm -rf " + input_filename2;
    system(rm_str.c_str());
    rm_str = "rm -rf " + output_filename3;
    system(rm_str.c_str());
}

void caculate_tpcc_txn_lantxncy(std::string path)
{

    // 输入文件和输出文件名
    double percentile = 0.99;
    std::string input_filename1 = path + "txn_lantency";
    std::string output_filename1 = path + "txn_sort_lantency";
    std::string output_count1 = path + "txn_sort_info";
    sortLinesInFile(input_filename1, output_filename1);
    calculateAndWriteDelays(output_filename1, output_count1, percentile);
    std::string rm_str = "rm -rf " + output_filename1;
    system(rm_str.c_str());
    rm_str = "rm -rf " + input_filename1;
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

void mergeFiles(const std::vector<std::string> &filenames, const std::string &outputFilename)
{
    // 打开所有输入文件
    std::vector<std::ifstream> inputFiles;
    for (const auto &filename : filenames)
    {
        std::ifstream inputFile(filename);
        if (!inputFile.is_open())
        {
            std::cerr << "Failed to open input file: " << filename << std::endl;
            return;
        }
        inputFiles.push_back(std::move(inputFile));
    }

    std::ofstream outputFile(outputFilename);
    if (!outputFile.is_open())
    {
        std::cerr << "Failed to open output file." << std::endl;
        return;
    }

    bool allFilesHaveData = true;

    while (allFilesHaveData)
    {
        allFilesHaveData = false;
        std::string mergedLine;

        for (auto &inputFile : inputFiles)
        {
            std::string line;
            if (std::getline(inputFile, line))
            {
                if (!mergedLine.empty())
                {
                    mergedLine += " "; // 可以根据需要更改分隔符
                }
                mergedLine += line;
                allFilesHaveData = true;
            }
        }

        if (allFilesHaveData)
        {
            outputFile << mergedLine << std::endl;
        }
    }

    for (auto &inputFile : inputFiles)
    {
        inputFile.close();
    }

    outputFile.close();
}

void handle_tpcc_txn(std::string &path, int num_nodes)
{
    std::vector<std::string> neworder_filenames;
    std::vector<std::string> txn_filenames;
    std::vector<std::string> proxy_file;

    // Add worker files for each node
    for (int i = 0; i < num_nodes; i++)
    {
        for (int j = 0; j < 4; j++)
        {
            neworder_filenames.push_back(path + "node" + std::to_string(i + 1) + "/TXN_LOG/neworder_worker_" + std::to_string(j));
            txn_filenames.push_back(path + "node" + std::to_string(i + 1) + "/TXN_LOG/worker_" + std::to_string(j));
        }
    }

    // Add proxy worker files
    for (int i = 0; i < num_nodes * 4; i++)
    {
        proxy_file.push_back(path + "proxy/TXN_LOG/worker_" + std::to_string(i));
    }

    std::string neworder_outputFilename = path + "neworder_lantency";
    std::string txn_outputFilename = path + "txn_lantency";
    mergeFiles(neworder_filenames, neworder_outputFilename);
    mergeFiles(txn_filenames, txn_outputFilename);

    std::string outputproxy = path + "route_lantency";
    mergeFiles(proxy_file, outputproxy);
}

void handle_txn(std::string &path, int num_nodes)
{
    std::vector<std::string> filenames;
    std::vector<std::string> proxy_file;

    // Add worker files for each node
    for (int i = 0; i < num_nodes; i++)
    {
        for (int j = 0; j < 4; j++)
        {
            filenames.push_back(path + "node" + std::to_string(i + 1) + "/TXN_LOG/worker_" + std::to_string(j));
        }
    }

    // Add proxy worker files
    for (int i = 0; i < num_nodes * 4; i++)
    {
        proxy_file.push_back(path + "proxy/TXN_LOG/worker_" + std::to_string(i));
    }

    std::string outputFilename = path + "txn_lantency";
    mergeFiles(filenames, outputFilename);

    std::string outputproxy = path + "route_lantency";
    mergeFiles(proxy_file, outputproxy);
}


double calculatePercentile(const std::vector<double>& data, double percentile) {
    if (data.empty()) return 0.0;
    int index = static_cast<int>(percentile * data.size());
    return data[index];
}

// 计算统计信息
void calculateStatistics(const std::vector<double>& data, double& min, double& max, double& mean, double& p, double percentile) {
    if (data.empty()) return;

    double sum = 0;
    min = std::numeric_limits<double>::max();
    max = std::numeric_limits<double>::lowest();

    for (double value : data) {
        sum += value;
        if (value < min) min = value;
        if (value > max) max = value;
    }
    mean = sum / data.size();

    // 排序并计算90分位数
    std::vector<double> sorted_data = data;
    std::sort(sorted_data.begin(), sorted_data.end());
    p = calculatePercentile(sorted_data, percentile);
}

// 读取数据、计算统计信息并写入文件
bool processtpccData(const std::string& input_file, const std::string& output_file) {
    std::ifstream file(input_file);
    if (!file.is_open()) {
        std::cerr << "无法打开输入文件！" << std::endl;
        return false;
    }

    std::vector<double> total_delays, local_delays, remote_delays;
    double total, local, remote;
    double percentle = 0.99;

    // 读取文件并解析数据
    while (file >> total >> local >> remote) {
        total_delays.push_back(total);
        local_delays.push_back(local);
        remote_delays.push_back(remote);
    }
    file.close();

    // 计算总延迟的统计信息
    double min_total, max_total, mean_total, p_total;
    calculateStatistics(total_delays, min_total, max_total, mean_total, p_total, percentle);

    // 计算本地访问延迟的统计信息
    double min_local, max_local, mean_local, p_local;
    calculateStatistics(local_delays, min_local, max_local, mean_local, p_local, percentle);

    // 计算远程访问延迟的统计信息
    double min_remote, max_remote, mean_remote, p_remote;
    calculateStatistics(remote_delays, min_remote, max_remote, mean_remote, p_remote, percentle);

    // 写入结果到输出文件
    std::ofstream output(output_file);
    if (!output.is_open()) {
        std::cerr << "无法打开输出文件！" << std::endl;
        return false;
    }

    output << "总延迟的平均值: " << mean_total << ", 最小值: " << min_total
           << ", 最大值: " << max_total << ", 99分位数: " << p_total << std::endl;

    output << "本地访问延迟的平均值: " << mean_local << ", 最小值: " << min_local
           << ", 最大值: " << max_local << ", 99分位数: " << p_local << std::endl;

    output << "远程访问延迟的平均值: " << mean_remote << ", 最小值: " << min_remote
           << ", 最大值: " << max_remote << ", 99分位数: " << p_remote << std::endl;

    output.close();
    return true;
}

// int main()
// {
//     std::string path28 = "/root/home/AffinityDB/data3/2nodes/图划分路由/动态/有codesign/分布式50/1/";
//     calculate_remote(path28, 2);
//     lantency_batch(2);
//     calculate_remote_batch(2);
//     return 0;
// }
