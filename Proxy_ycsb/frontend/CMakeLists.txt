add_executable(ycsb ycsb/ycsb.cpp)
add_dependencies(ycsb Proxy)
target_link_libraries(ycsb Proxy numa)


