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

void calculate(std::string filename1, std::string filename2, ofstream &output, uint32_t &up, uint32_t &down)
{
    unordered_set<uint64_t> pidSet1, pidSet2;
    readPIDsFromFile(filename1, pidSet1);
    readPIDsFromFile(filename2, pidSet2);
    int intersection = 0;
    for (const auto &pid : pidSet1)
    {
        if (pidSet2.find(pid) != pidSet2.end())
        {
            intersection++;
        }
    }
    // Calculate union
    unordered_set<uint64_t> unionSet(pidSet1);
    for (const auto &pid : pidSet2)
    {
        unionSet.insert(pid);
    }
    up += intersection;
    down += unionSet.size();
    output << "Intersection count: " << intersection << endl;
    output << "Union count: " << unionSet.size() << endl;
    output << "ratio : " << float(intersection) / unionSet.size() * 100 << "%" << endl;
}

void handle_page(std::string path)
{
    unordered_set<uint64_t> pidSet1, pidSet2;
    string customer1 = path + "node1/Logs/customer_page";        // Replace with your first file name
    string customer2 = path + "node2/Logs/customer_page";        // Replace with your second file name
    string warehouse1 = path + "node1/Logs/warehouse_page";      // Replace with your first file name
    string warehouse2 = path + "node2/Logs/warehouse_page";      // Replace with your second file name
    string district1 = path + "node1/Logs/district_page";        // Replace with your first file name
    string district2 = path + "node2/Logs/district_page";        // Replace with your second file name
    string customerwdl1 = path + "node1/Logs/customer_wdl_page"; // Replace with your first file name
    string customerwdl2 = path + "node2/Logs/customer_wdl_page"; // Replace with your second file name
    string history1 = path + "node1/Logs/history_page";          // Replace with your first file name
    string history2 = path + "node2/Logs/history_page";          // Replace with your second file name
    string neworder1 = path + "node1/Logs/neworder_page";        // Replace with your first file name
    string neworder2 = path + "node2/Logs/neworder_page";        // Replace with your second file name
    string order1 = path + "node1/Logs/order_page";              // Replace with your first file name
    string order2 = path + "node2/Logs/order_page";              // Replace with your second file name
    string order_wdc1 = path + "node1/Logs/order_wdc_page";      // Replace with your first file name
    string order_wdc2 = path + "node2/Logs/order_wdc_page";      // Replace with your second file name
    string item1 = path + "node1/Logs/item_page";                // Replace with your first file name
    string item2 = path + "node2/Logs/item_page";                // Replace with your second file name
    string stock1 = path + "node1/Logs/stock_page";              // Replace with your first file name
    string stock2 = path + "node2/Logs/stock_page";              // Replace with your second file name
    // string customer1 = "pages/codesign/node0/Logs/customer_page";    // Replace with your first file name
    // string customer2 = "pages/codesign/node1/Logs/customer_page";    // Replace with your second file name
    ofstream output(path + "result");
    uint32_t a = 0, b = 0;
    output << "warehouse" << endl;
    calculate(warehouse1, warehouse2, output, a, b);
    output << "district" << endl;
    calculate(district1, district2, output, a, b);
    output << "customer" << endl;
    calculate(customer1, customer2, output, a, b);
    output << "history" << endl;
    calculate(history1, history2, output, a, b);
    output << "neworder" << endl;
    calculate(neworder1, neworder2, output, a, b);
    output << "order" << endl;
    calculate(order1, order2, output, a, b);
    output << "item" << endl;
    calculate(item1, item2, output, a, b);
    output << "stock" << endl;
    calculate(stock1, stock2, output, a, b);
    output << "customerwdl" << endl;
    calculate(customerwdl1, customerwdl2, output, a, b);
    output << "order_wdc" << endl;
    calculate(order_wdc1, order_wdc2, output, a, b);
    output << "all" << endl;
    output << "Intersection count: " << a << endl;
    output << "Union count: " << b << endl;
    output << "ratio : " << float(a) / b * 100 << "%" << endl;
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

int main() {
    // INI 文件名
    std::string filename1 = "./tpcc_config.ini";
    std::string filename2 = "./proxy_config.ini";
    std::string path;
    std::string start = "data_tpcc/schism_100w/";
    std::string subpath1 = "", subpath2 = "", subpath3 = "", subpath4 = "", subpath5 = "", subpath6 = "";

    // 读取INI文件并存储到字典中
    std::unordered_map<std::string, std::string> params = readINIFile(filename1);
    std::unordered_map<std::string, std::string> params2 = readINIFile(filename2);

    // 获取节点数
    int num_nodes = std::stoi(params["nodes"]);
    int key_range = std::stoi(params2["stamp_len"]);
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
     if (params2["distribution"] == "false")
    {
        subpath4 = "无分布式/";
    }
    else
    {
        subpath4 = "分布式" + params2["distribution_rate"] + "/";
    }

    // subpath5 = std::to_string(key_range) + "range/";
    // 获取文件数量参数
    if (params2["file_num"] == "1") {
        subpath6 = "1/";
    } else if (params2["file_num"] == "2") {
        subpath6 = "2/";
    } else {
        subpath6 = "3/";
    }

    if(start == "result/new_read_write/"){
        subpath6 = params2["write_weight"] + "/";
    }

    // 构建完整路径
    path = start + subpath1 + subpath2 + subpath3 + subpath4 + subpath5 + subpath6;

    // 创建路径
    createDirectories(path);

    // 创建节点和代理文件夹
    for (int i = 1; i <= num_nodes; ++i) {
        createDirectories(path + "node" + std::to_string(i));
    }
    createDirectories(path + "proxy");

    // 定义 SCP 命令模板
    std::vector<std::string> scp_commands;
    scp_commands.push_back("scp -v -o StrictHostKeyChecking=no -r root@10.0.0.88:/root/home/AffinityDB/ScaleStore/Logs " + path + "node2/");
    scp_commands.push_back("scp -v -o StrictHostKeyChecking=no -r root@10.0.0.88:/root/home/AffinityDB/ScaleStore/TXN_LOG " + path + "node2/");
    scp_commands.push_back("cp -r ScaleStore/Logs " + path + "node1/");
    scp_commands.push_back("cp -r ScaleStore/TXN_LOG " + path + "node1/");

    // 添加更多节点的 SCP 命令
    if (num_nodes >= 3) {
        scp_commands.push_back("scp -v -o StrictHostKeyChecking=no -r root@10.0.0.90:/root/home/AffinityDB/ScaleStore/Logs " + path + "node3/");
        scp_commands.push_back("scp -v -o StrictHostKeyChecking=no -r root@10.0.0.90:/root/home/AffinityDB/ScaleStore/TXN_LOG " + path + "node3/");
    }
    if (num_nodes >= 4) {
        scp_commands.push_back("scp -v -o StrictHostKeyChecking=no -r root@10.0.0.91:/root/home/AffinityDB/ScaleStore/Logs " + path + "node4/");
        scp_commands.push_back("scp -v -o StrictHostKeyChecking=no -r root@10.0.0.91:/root/home/AffinityDB/ScaleStore/TXN_LOG " + path + "node4/");
    }
    if (num_nodes >= 5) {
        scp_commands.push_back("scp -v -o StrictHostKeyChecking=no -r root@10.0.0.92:/root/home/AffinityDB/ScaleStore/Logs " + path + "node5/");
        scp_commands.push_back("scp -v -o StrictHostKeyChecking=no -r root@10.0.0.92:/root/home/AffinityDB/ScaleStore/TXN_LOG " + path + "node5/");
    }
    if (num_nodes == 6) {
        scp_commands.push_back("scp -v -o StrictHostKeyChecking=no -r root@10.0.0.93:/root/home/AffinityDB/ScaleStore/Logs " + path + "node6/");
        scp_commands.push_back("scp -v -o StrictHostKeyChecking=no -r root@10.0.0.93:/root/home/AffinityDB/ScaleStore/TXN_LOG " + path + "node6/");
    }

    // 添加代理的 SCP 命令
    if (params["use_proxy"] == "true") {
        scp_commands.push_back("scp -v -o StrictHostKeyChecking=no -r root@10.0.0.89:/root/home/AffinityDB/Proxy/backend/Proxy/Logs " + path + "proxy/");
        scp_commands.push_back("scp -v -o StrictHostKeyChecking=no -r root@10.0.0.89:/root/home/AffinityDB/Proxy/backend/Proxy/TXN_LOG " + path + "proxy/");
    }

    // 执行 SCP 命令
    for (const auto &command : scp_commands) {
        int result = system(command.c_str());
        if (result == 0) {
            std::cout << "File copied successfully: " << command << std::endl;
        } else {
            std::cerr << "Error copying file: " << command << std::endl;
        }
    }
    handle_tpcc_txn(path, num_nodes);
    // 删除 TXN_LOG 文件
    for (int i = 1; i <= num_nodes; ++i) {
        std::string rm_file = "rm -rf " + path + "node" + std::to_string(i) + "/TXN_LOG";
        system(rm_file.c_str());
    }
    if (params["use_proxy"] == "true") {
        std::string rm_file = "rm -rf " + path + "proxy/TXN_LOG";
        system(rm_file.c_str());
    }

    // 计算事务延迟和远程数据
    std::string neworder_input = path + "neworder_lantency";
    std::string neworder_output = path + "neworder_info";
    std::string txn_input = path + "txn_lantency";
    std::string txn_output = path + "txn_info";
    processtpccData(neworder_input, neworder_output);
    processtpccData(txn_input, txn_output);
    std::string rm_txn = "rm -rf " + neworder_input;
    system(rm_txn.c_str());
    rm_txn = "rm -rf " + txn_input;
    system(rm_txn.c_str());
    caculate_router_lantency(path);
    calculate_remote(path, num_nodes);
    calculate_local(path, num_nodes);


    return 0;
}