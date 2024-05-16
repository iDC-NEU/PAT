#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <iterator>
#include <string>
#include <sstream>
#include <cmath>

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

void calculateAndWriteDelays(const std::string &input_filename, const std::string &output_filename, double percentile) {
    std::ifstream input_file(input_filename);
    if (!input_file) {
        std::cerr << "无法打开输入文件!" << std::endl;
        return;
    }

    std::ofstream output_file(output_filename);
    if (!output_file) {
        std::cerr << "无法打开输出文件!" << std::endl;
        return;
    }

    std::string line;
    int count = 0;
    while (std::getline(input_file, line)) {
        std::istringstream iss(line);
        std::vector<double> delays;
        double delay;
        count++;
        while (iss >> delay) {
            delays.push_back(delay);
        }

        if (delays.empty()) {
            continue; // 如果这一行没有数据，跳过处理
        }

        std::sort(delays.begin(), delays.end());

        // 计算平均延迟
        double sum = 0;
        for (double d : delays) {
            sum += d;
        }
        double average_delay = sum / delays.size();

        // 计算中位数延迟
        size_t size = delays.size();
        double median_delay;
        if (size % 2 == 0) {
            median_delay = (delays[size / 2 - 1] + delays[size / 2]) / 2.0;
        } else {
            median_delay = delays[size / 2];
        }

        // 计算指定百分位数的尾延迟
        size_t index = static_cast<size_t>(percentile * size);
        if (index >= size) {
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

int main()
{
    // 输入文件和输出文件名
    double percentile = 0.99;
    std::string input_filename1 = "/root/home/AffinityDB/new_data/图划分路由/动态/有codesign/无分布式/txn_lantency";
    std::string output_filename1 = "1";
    std::string output_count1 = "2";
    // std::string input_filename2 = "/home/user/exp-data/扩展性数据/动态/有codesign/分布式50/route_lantency";
    // std::string output_filename2 = "/home/user/exp-data/扩展性数据/动态/有codesign/分布式50/route_sort_lantency";
    // std::string output_count2 = "/home/user/exp-data/扩展性数据/动态/有codesign/分布式50/route_sort_info" + std::to_string(percentile);
    sortLinesInFile(input_filename1, output_filename1);
    // generate_file(input_filename2, output_filename2);
    calculateAndWriteDelays(output_filename1, output_count1, percentile);
    // calculateAndWriteDelays(output_filename2, output_count2, percentile);
    return 0;
}
