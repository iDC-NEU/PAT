#include <iostream>
#include <fstream>
#include <sstream>
#include <unordered_set>
#include <string>
#include <algorithm>
#include "utils.hpp"
#include <filesystem>

using namespace std;
// Function to read PIDs from a file and store them in a set
void readPIDsFromFile(const string &filename, unordered_set<uint64_t> &pids)
{
    ifstream file(filename);
    if (!file.is_open())
    {
        cerr << "Error opening file: " << filename << endl;
        return;
    }

    string line;
    getline(file, line);
    while (getline(file, line))
    {
        istringstream iss(line);
        string pidValue;
        iss >> pidValue >> pidValue; // Read the second column
        pidValue.erase(remove(pidValue.begin(), pidValue.end(), ','), pidValue.end());
        pids.insert(std::stoul(pidValue));
    }

    file.close();
}

void calculate(const vector<string> &filenames, ofstream &output, uint32_t &up, uint32_t &down)
{
    unordered_set<uint64_t> pidSetUnion, pidSetIntersection;
    bool firstFile = true;
    for (const auto &filename : filenames)
    {
        unordered_set<uint64_t> pidSet;
        readPIDsFromFile(filename, pidSet);
        if (firstFile)
        {
            pidSetUnion = pidSet;
            pidSetIntersection = pidSet;
            firstFile = false;
        }
        else
        {
            unordered_set<uint64_t> tempIntersection;
            for (const auto &pid : pidSetIntersection)
            {
                if (pidSet.find(pid) != pidSet.end())
                {
                    tempIntersection.insert(pid);
                }
            }
            pidSetIntersection = tempIntersection;

            for (const auto &pid : pidSet)
            {
                pidSetUnion.insert(pid);
            }
        }
    }

    up += pidSetIntersection.size();
    down += pidSetUnion.size();

    output << "Intersection count: " << pidSetIntersection.size() << endl;
    output << "Union count: " << pidSetUnion.size() << endl;
    output << "Ratio: " << float(pidSetIntersection.size()) / pidSetUnion.size() * 100 << "%" << endl;
}

void handle_page(const string &path, int numNodes)
{
    vector<string> filenames;
    for (int i = 1; i <= numNodes; ++i)
    {
        filenames.push_back(path + "node" + to_string(i) + "/Logs/ycsb_page");
    }

    ofstream output(path + "result");
    uint32_t a = 0, b = 0;
    calculate(filenames, output, a, b);
    output << "All Nodes Combined" << endl;
    output << "Intersection count: " << a << endl;
    output << "Union count: " << b << endl;
    output << "Ratio: " << float(a) / b * 100 << "%" << endl;
}



std::unordered_map<std::string, std::string> readINIFile(const std::string &filename)
{
    std::unordered_map<std::string, std::string> params;

    std::ifstream file(filename);
    if (!file.is_open())
    {
        std::cerr << "Failed to open INI file: " << filename << std::endl;
        return params;
    }

    std::string line;
    while (std::getline(file, line))
    {
        std::istringstream iss(line);
        std::string token;
        if (iss >> token)
        {
            size_t pos = token.find('=');
            if (pos != std::string::npos)
            {
                std::string key = token.substr(2, pos - 2);
                std::string value = token.substr(pos + 1);
                params[key] = value;
            }
        }
    }

    file.close();

    return params;
}

void createDirectories(const std::string &path)
{
    if (!std::filesystem::exists(path))
    {
        std::filesystem::create_directories(path);
    }
}

int main()
{
    // INI 文件名
    std::string filename1 = "./ycsb_config.ini";
    std::string filename2 = "./proxy_ycsb_config.ini";
    std::string path;
    std::string start = "result/ycsb/hot_page_300/";
    std::string subpath1 = "", subpath2 = "", subpath3 = "", subpath4 = "", subpath5 = "", subpath6 = "", subpath7 = "";

    // 读取INI文件并存储到字典中
    std::unordered_map<std::string, std::string> params = readINIFile(filename1);
    std::unordered_map<std::string, std::string> params2 = readINIFile(filename2);

    // 获取节点数
    int num_nodes = std::stoi(params["nodes"]);
    subpath1 = std::to_string(num_nodes) + "nodes/";

    // 获取路由模式参数
    if (params2["route_mode"] == "1")
    {
        subpath2 = "random/";
    }
    else if (params2["route_mode"] == "2")
    {
        subpath2 = "warehouse/";
    }
    else if (params2["route_mode"] == "4")
    {
        subpath2 = "schism/";
    }
        else if (params2["route_mode"] == "5")
    {
        subpath2 = "hash/";
    }
    else
    {
        if (params2["partition_mode"] == "1")
        {
            if (params["use-codesign"] == "false")
            {
                subpath2 = "static/";
            }
            else
            {
                subpath2 = "static+rfs/";
            }
        }
        else
        {
            if (params["use-codesign"] == "false")
            {
                subpath2 = "dynamic/";
            }
            else
            {
                subpath2 = "dynamic+rfs/";
            }
        }
    }
    subpath5 = "读" + params2["YCSB_read_ratio"] + "/";
    // 获取分布式参数
    if (params2["distribution"] == "false")
    {
        subpath6 = "无分布式/";
    }
    else
    {
        subpath6 = "分布式" + params2["distribution_rate"] + "/";
    }
    // 获取文件数量参数
    if (params2["file_num"] == "1")
    {
        subpath7 = "1/";
    }
    else if (params2["file_num"] == "2")
    {
        subpath7 = "2/";
    }
    else
    {
        subpath7 = "3/";
    }
    if (start == "result/ycsb/read_write/")
    {
        subpath7 = params2["write_weight"] + "/";
    }

    // 构建完整路径
    path = start + subpath1 + subpath2 + subpath3 + subpath4 + subpath5 + subpath6 + subpath7;

    // 创建路径
    createDirectories(path);

    // 创建节点和代理文件夹
    for (int i = 1; i <= num_nodes; ++i)
    {
        createDirectories(path + "node" + std::to_string(i));
    }
    createDirectories(path + "proxy");

    // 定义 SCP 命令模板
    std::vector<std::string> scp_commands;
    scp_commands.push_back("scp -v -o StrictHostKeyChecking=no -r root@10.0.0.88:/root/home/AffinityDB/ScaleStore/Logs " + path + "node2/");
    // scp_commands.push_back("scp -v -o StrictHostKeyChecking=no -r root@10.0.0.88:/root/home/AffinityDB/ScaleStore/TXN_LOG " + path + "node2/");
    scp_commands.push_back("cp -r ScaleStore/Logs " + path + "node1/");
    // scp_commands.push_back("cp -r ScaleStore/TXN_LOG " + path + "node1/");

    // 添加更多节点的 SCP 命令
    if (num_nodes >= 3)
    {
        scp_commands.push_back("scp -v -o StrictHostKeyChecking=no -r root@10.0.0.90:/root/home/AffinityDB/ScaleStore/Logs " + path + "node3/");
        // scp_commands.push_back("scp -v -o StrictHostKeyChecking=no -r root@10.0.0.90:/root/home/AffinityDB/ScaleStore/TXN_LOG " + path + "node3/");
    }
    if (num_nodes >= 4)
    {
        scp_commands.push_back("scp -v -o StrictHostKeyChecking=no -r root@10.0.0.91:/root/home/AffinityDB/ScaleStore/Logs " + path + "node4/");
        // scp_commands.push_back("scp -v -o StrictHostKeyChecking=no -r root@10.0.0.91:/root/home/AffinityDB/ScaleStore/TXN_LOG " + path + "node4/");
    }
    if (num_nodes >= 5)
    {
        scp_commands.push_back("scp -v -o StrictHostKeyChecking=no -r root@10.0.0.92:/root/home/AffinityDB/ScaleStore/Logs " + path + "node5/");
        // scp_commands.push_back("scp -v -o StrictHostKeyChecking=no -r root@10.0.0.92:/root/home/AffinityDB/ScaleStore/TXN_LOG " + path + "node5/");
    }
    if (num_nodes == 6)
    {
        scp_commands.push_back("scp -v -o StrictHostKeyChecking=no -r root@10.0.0.93:/root/home/AffinityDB/ScaleStore/Logs " + path + "node6/");
        // scp_commands.push_back("scp -v -o StrictHostKeyChecking=no -r root@10.0.0.93:/root/home/AffinityDB/ScaleStore/TXN_LOG " + path + "node6/");
    }

    // 添加代理的 SCP 命令
    if (params["use_proxy"] == "true")
    {
        scp_commands.push_back("scp -v -o StrictHostKeyChecking=no -r root@10.0.0.89:/root/home/AffinityDB/Proxy_ycsb/backend/Proxy/Logs " + path + "proxy/");
        scp_commands.push_back("scp -v -o StrictHostKeyChecking=no -r root@10.0.0.89:/root/home/AffinityDB/Proxy_ycsb/backend/Proxy/TXN_LOG " + path + "proxy/");
    }

    // 执行 SCP 命令
    for (const auto &command : scp_commands)
    {
        int result = system(command.c_str());
        if (result == 0)
        {
            std::cout << "File copied successfully: " << command << std::endl;
        }
        else
        {
            std::cerr << "Error copying file: " << command << std::endl;
        }
    }
    handle_txn(path, num_nodes);
    // 计算事务延迟和远程数据
    caculate_txn_lantxncy(path);
    calculate_remote(path, num_nodes);
    // 删除 TXN_LOG 文件
    for (int i = 1; i <= num_nodes; ++i)
    {
        std::string rm_file = "rm -rf " + path + "node" + std::to_string(i) + "/TXN_LOG";
        system(rm_file.c_str());
    }
    if (params["use_proxy"] == "true")
    {
        std::string rm_file = "rm -rf " + path + "proxy/TXN_LOG";
        system(rm_file.c_str());
    }
    std::string rm_tmp_sort = "rm " + path + "route_lantency && rm " + path + "route_sort_lantency";
    system(rm_tmp_sort.c_str());

    return 0;
}