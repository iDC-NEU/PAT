#include <iostream>
#include <fstream>
#include <sstream>
#include <unordered_set>
#include <string>
#include <algorithm>

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

void mergeFiles(const std::vector<std::string> &filenames, const std::string &outputFilename)
{
    // 要合并的文件名列表
    std::ofstream outputFile(outputFilename, std::ios::app); // 打开输出文件，以追加模式
    if (!outputFile.is_open())
    {
        std::cerr << "Failed to open output file." << std::endl;
        return;
    }

    for (const auto &filename : filenames)
    {
        std::ifstream inputFile(filename);
        if (!inputFile.is_open())
        {
            std::cerr << "Failed to open input file: " << filename << std::endl;
            continue; // 继续处理下一个文件
        }

        // 从输入文件读取内容，并写入输出文件
        outputFile << inputFile.rdbuf();
        inputFile.close(); // 关闭输入文件
    }

    outputFile.close(); // 关闭输出文件
}

void handle_txn(std::string &path)
{
    std::vector<std::string> filenames;
    std::vector<std::string> proxy_file;
    for (int i = 0; i < 8; i++)
    {
        filenames.push_back(path + "node1/TXN_LOG/worker_" + std::to_string(i));
        filenames.push_back(path + "node2/TXN_LOG/worker_" + std::to_string(i));
    }
    for (int i = 0; i < 16; i++)
    {
        proxy_file.push_back(path + "proxy/TXN_LOG/worker_" + std::to_string(i));
    }
    std::string outputFilename = path + "txn_lantency";
    mergeFiles(filenames, outputFilename);
    std::string outputproxy = path + "route_lantency";
    mergeFiles(proxy_file, outputproxy);
}

void handle_3_txn(std::string &path)
{
    std::vector<std::string> filenames;
    std::vector<std::string> proxy_file;
    for (int i = 0; i < 8; i++)
    {
        filenames.push_back(path + "node1/TXN_LOG/worker_" + std::to_string(i));
        filenames.push_back(path + "node2/TXN_LOG/worker_" + std::to_string(i));
        filenames.push_back(path + "node3/TXN_LOG/worker_" + std::to_string(i));
    }
    for (int i = 0; i < 24; i++)
    {
        proxy_file.push_back(path + "proxy/TXN_LOG/worker_" + std::to_string(i));
    }
    std::string outputFilename = path + "txn_lantency";
    mergeFiles(filenames, outputFilename);
    std::string outputproxy = path + "route_lantency";
    mergeFiles(proxy_file, outputproxy);
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

int main()
{

    // INI 文件名
    std::string filename1 = "./tpcc_config.ini";
    std::string filename2 = "./proxy_config.ini";
    std::string path;
    std::string start = "data/";
    std::string subpath1 = "";
    std::string subpath2 = "";
    std::string subpath3 = "";
    std::string subpath4 = "";
    std::string subpath5 = "";
    // 读取INI文件并存储到字典中
    std::unordered_map<std::string, std::string> params = readINIFile(filename1);
    std::unordered_map<std::string, std::string> params2 = readINIFile(filename2);

    // 要获取的参数
    if (params["use_proxy"] == "false")
    {
        subpath1 = "原版scalestore/";
    }
    else
    {
        if (params2["route_mode"] == "1")
        {
            subpath1 = "page交并/";
            subpath2 = "随机路由/";
        }
        else if (params2["route_mode"] == "2")
        {
            subpath1 = "page交并/";
            subpath2 = "哈希路由/";
        }
        else
        {
            subpath1 = "page交并/";
            if (params2["partition_mode"] == "1")
            {
                subpath2 = "静态/";
            }
            else
            {
                subpath2 = "动态/";
            }
            if (params["use-codesign"] == "false")
            {
                subpath3 = "无codesign/";
            }
            else
            {
                subpath3 = "有codesign/";
            }
        }
    }
    if (params["nodes"] == "3" || params["nodes"] == "4")
    {
        subpath4 = "扩展性/";
    }
    else
    {
        if (params["distribution"] == "false")
        {
            subpath4 = "无分布式/";
        }
        else
        {
            if (params["distribution_rate"] == "10")
            {
                subpath4 = "原版tpcc/";
            }
            else
            {
                subpath4 = "分布式30/";
            }
        }
    }
    path = start + subpath1 + subpath2 + subpath3 + subpath4 + subpath5;
    std::string scpfile_1 = "scp -v -o StrictHostKeyChecking=no -r  root@10.0.0.88:/root/home/AffinityDB/ScaleStore/Logs " + path + "node2/";
    std::string scpfile_2 = "scp -v -o StrictHostKeyChecking=no -r  root@10.0.0.88:/root/home/AffinityDB/ScaleStore/TXN_LOG " + path + "node2/";
    std::string scpfile_3 = "scp -v -o StrictHostKeyChecking=no -r  root@10.0.0.89:/root/home/AffinityDB/Proxy/backend/Proxy/Logs " + path + "proxy/";
    std::string scpfile_4 = "scp -v -o StrictHostKeyChecking=no -r  root@10.0.0.89:/root/home/AffinityDB/Proxy/backend/Proxy/TXN_LOG " + path + "proxy/";
    std::string scpfile_5 = "scp -v -o StrictHostKeyChecking=no -r  root@10.0.0.90:/root/home/AffinityDB/ScaleStore/Logs " + path + "node3/";
    std::string scpfile_6 = "scp -v -o StrictHostKeyChecking=no -r  root@10.0.0.90:/root/home/AffinityDB/ScaleStore/TXN_LOG " + path + "node3/";
    std::string cpfile_1 = "cp -r ScaleStore/Logs " + path + "node1/";
    std::string cpfile_2 = "cp -r ScaleStore/TXN_LOG " + path + "node1/";
    int result_1 = system(scpfile_1.c_str());

    // 检查是否成功执行 cp 命令
    if (result_1 == 0)
    {
        // 成功执行
        std::cout << "File copied successfully." << std::endl;
    }
    else
    {
        // 执行失败
        std::cerr << "Error copying file." << std::endl;
    }
    int result_2 = system(scpfile_2.c_str());

    // 检查是否成功执行 cp 命令
    if (result_2 == 0)
    {
        // 成功执行
        std::cout << "File copied successfully." << std::endl;
    }
    else
    {
        // 执行失败
        std::cerr << "Error copying file." << std::endl;
    }

    int result_3 = system(cpfile_1.c_str());

    // 检查是否成功执行 cp 命令
    if (result_3 == 0)
    {
        // 成功执行
        std::cout << "File copied successfully." << std::endl;
    }
    else
    {
        // 执行失败
        std::cerr << "Error copying file." << std::endl;
    }
    int result_4 = system(cpfile_2.c_str());

    // 检查是否成功执行 cp 命令
    if (result_4 == 0)
    {
        // 成功执行
        std::cout << "File copied successfully." << std::endl;
    }
    else
    {
        // 执行失败
        std::cerr << "Error copying file." << std::endl;
    }

    if (params["use_proxy"] == "true")
    {
        int result_5 = system(scpfile_3.c_str());
        // 检查是否成功执行 cp 命令
        if (result_5 == 0)
        {
            // 成功执行
            std::cout << "File copied successfully." << std::endl;
        }
        else
        {
            // 执行失败
            std::cerr << "Error copying file." << std::endl;
        }
        int result_6 = system(scpfile_4.c_str());

        // 检查是否成功执行 cp 命令
        if (result_6 == 0)
        {
            // 成功执行
            std::cout << "File copied successfully." << std::endl;
        }
        else
        {
            // 执行失败
            std::cerr << "Error copying file." << std::endl;
        }
    }

    if (params["nodes"] == "3" || params["nodes"] == "4")
    {
        int result_7 = system(scpfile_5.c_str());
        // 检查是否成功执行 cp 命令
        if (result_7 == 0)
        {
            // 成功执行
            std::cout << "File copied successfully." << std::endl;
        }
        else
        {
            // 执行失败
            std::cerr << "Error copying file." << std::endl;
        }
        int result_8 = system(scpfile_6.c_str());

        // 检查是否成功执行 cp 命令
        if (result_8 == 0)
        {
            // 成功执行
            std::cout << "File copied successfully." << std::endl;
        }
        else
        {
            // 执行失败
            std::cerr << "Error copying file." << std::endl;
        }
        handle_3_txn(path);
        std::string rm_file1 = "rm -rf " + path + "node1/TXN_LOG";
        system(rm_file1.c_str());
        std::string rm_file2 = "rm -rf " + path + "node2/TXN_LOG";
        system(rm_file2.c_str());
        std::string rm_file3 = "rm -rf " + path + "node3/TXN_LOG";
        system(rm_file3.c_str());
        std::string rm_file4 = "rm -rf " + path + "proxy/TXN_LOG";
        system(rm_file4.c_str());
        return 0;
    }

    handle_txn(path);
    handle_page(path);
    std::string rm_file1 = "rm -rf " + path + "node1/TXN_LOG";
    system(rm_file1.c_str());
    std::string rm_file2 = "rm -rf " + path + "node2/TXN_LOG";
    system(rm_file2.c_str());
    std::string rm_file3 = "rm -rf " + path + "proxy/TXN_LOG";
    system(rm_file3.c_str());
    string customer1 = "rm " + path + "node1/Logs/customer_page";        // Replace with your first file name
    string customer2 = "rm " + path + "node2/Logs/customer_page";        // Replace with your second file name
    string warehouse1 = "rm " + path + "node1/Logs/warehouse_page";      // Replace with your first file name
    string warehouse2 = "rm " + path + "node2/Logs/warehouse_page";      // Replace with your second file name
    string district1 = "rm " + path + "node1/Logs/district_page";        // Replace with your first file name
    string district2 = "rm " + path + "node2/Logs/district_page";        // Replace with your second file name
    string customerwdl1 = "rm " + path + "node1/Logs/customer_wdl_page"; // Replace with your first file name
    string customerwdl2 = "rm " + path + "node2/Logs/customer_wdl_page"; // Replace with your second file name
    string history1 = "rm " + path + "node1/Logs/history_page";          // Replace with your first file name
    string history2 = "rm " + path + "node2/Logs/history_page";          // Replace with your second file name
    string neworder1 = "rm " + path + "node1/Logs/neworder_page";        // Replace with your first file name
    string neworder2 = "rm " + path + "node2/Logs/neworder_page";        // Replace with your second file name
    string order1 = "rm " + path + "node1/Logs/order_page";              // Replace with your first file name
    string order2 = "rm " + path + "node2/Logs/order_page";              // Replace with your second file name
    string order_wdc1 = "rm " + path + "node1/Logs/order_wdc_page";      // Replace with your first file name
    string order_wdc2 = "rm " + path + "node2/Logs/order_wdc_page";      // Replace with your second file name
    string item1 = "rm " + path + "node1/Logs/item_page";                // Replace with your first file name
    string item2 = "rm " + path + "node2/Logs/item_page";                // Replace with your second file name
    string stock1 = "rm " + path + "node1/Logs/stock_page";              // Replace with your first file name
    string stock2 = "rm " + path + "node2/Logs/stock_page";              // Replace with your second file name
    system(customer1.c_str());
    system(customer2.c_str());
    system(warehouse1.c_str());
    system(warehouse2.c_str());
    system(district1.c_str());
    system(district2.c_str());
    system(customerwdl1.c_str());
    system(customerwdl2.c_str());
    system(order1.c_str());
    system(order2.c_str());
    system(order_wdc1.c_str());
    system(order_wdc2.c_str());
    system(history1.c_str());
    system(history2.c_str());
    system(item1.c_str());
    system(item2.c_str());
    system(stock1.c_str());
    system(stock2.c_str());
    system(neworder1.c_str());
    system(neworder2.c_str());
}
