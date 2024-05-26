# ---------------------------------------------------------------------------
# Proxy
# ---------------------------------------------------------------------------
project(Proxy)

# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------
file(GLOB_RECURSE PROXY_CC **.cpp **/**.cpp **.hpp **/**.hpp)
if (NOT UNIX)
    message(SEND_ERROR "unsupported platform")
endif ()

# ---------------------------------------------------------------------------
# Library
# ---------------------------------------------------------------------------
add_library(Proxy STATIC ${PROXY_CC})

OPTION(SANI "Compile Proxy with sanitizers" OFF)
IF(SANI)
  if (CMAKE_BUILD_TYPE MATCHES Debug)
    message("Compiling with Sanitizers")
    target_compile_options(Proxy PUBLIC -fsanitize=address)
    target_link_libraries(Proxy asan)
    # target_compile_options(Proxy PUBLIC -fsanitize=undefined)
    # target_link_libraries(Proxy ubsan)
  endif ()
ENDIF(SANI)

set(LIBRARY_PATH "${CMAKE_CURRENT_SOURCE_DIR}/Proxy/dynamicPartition/tools/lib")
target_link_libraries(Proxy  
    gflags Threads::Threads atomic numa rdmacm ibverbs aio 
    ${LIBRARY_PATH}/libmetis.a
    ${LIBRARY_PATH}/libGKlib.a
    ${LIBRARY_PATH}/libmetis.a
)
target_include_directories(Proxy PUBLIC ${SHARED_INCLUDE_DIRECTORY})
target_include_directories(Proxy PRIVATE ${CMAKE_CURRENT_LIST_DIR})
# ---------------------------------------------------------------------------
set(PROXY_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR})
set_property(TARGET Proxy APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${PROXY_INCLUDE_DIR})
# ---------------------------------------------------------------------------
# Proxy
# ---------------------------------------------------------------------------
project(Proxy)

# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------
file(GLOB_RECURSE PROXY_CC **.cpp **/**.cpp **.hpp **/**.hpp)
if (NOT UNIX)
    message(SEND_ERROR "unsupported platform")
endif ()

# ---------------------------------------------------------------------------
# Library
# ---------------------------------------------------------------------------
add_library(Proxy STATIC ${PROXY_CC})

OPTION(SANI "Compile Proxy with sanitizers" OFF)
IF(SANI)
  if (CMAKE_BUILD_TYPE MATCHES Debug)
    message("Compiling with Sanitizers")
    target_compile_options(Proxy PUBLIC -fsanitize=address)
    target_link_libraries(Proxy asan)
    # target_compile_options(Proxy PUBLIC -fsanitize=undefined)
    # target_link_libraries(Proxy ubsan)
  endif ()
ENDIF(SANI)

target_link_libraries(Proxy  
                                  gflags Threads::Threads atomic numa rdmacm ibverbs aio /home/user/project/database/demo/Proxy/backend/Proxy/dynamicPartition/tools/lib/libmetis.a
                                  /home/user/project/database/demo/Proxy/backend/Proxy/dynamicPartition/tools/lib/libGKlib.a
                                  /home/user/project/database/demo/Proxy/backend/Proxy/dynamicPartition/tools/lib/libmetis.a) 
target_include_directories(Proxy PUBLIC ${SHARED_INCLUDE_DIRECTORY})
target_include_directories(Proxy PRIVATE ${CMAKE_CURRENT_LIST_DIR})
# ---------------------------------------------------------------------------
set(PROXY_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR})
set_property(TARGET Proxy APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${PROXY_INCLUDE_DIR})
