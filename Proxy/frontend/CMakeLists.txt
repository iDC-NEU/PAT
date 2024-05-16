add_executable(tpcc tpcc/tpcc.cpp)
add_dependencies(tpcc Proxy)
target_link_libraries(tpcc Proxy numa)


