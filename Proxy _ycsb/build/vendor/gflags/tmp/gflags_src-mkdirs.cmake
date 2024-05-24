# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION 3.5)

file(MAKE_DIRECTORY
  "/home/user/project/database/demo/gflags"
  "/home/user/project/database/demo/Proxy/build/vendor/gflags/src/gflags_src-build"
  "/home/user/project/database/demo/Proxy/build/vendor/gflags"
  "/home/user/project/database/demo/Proxy/build/vendor/gflags/tmp"
  "/home/user/project/database/demo/Proxy/build/vendor/gflags/src/gflags_src-stamp"
  "/home/user/project/database/demo/Proxy/build/vendor/gflags/src"
  "/home/user/project/database/demo/Proxy/build/vendor/gflags/src/gflags_src-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/home/user/project/database/demo/Proxy/build/vendor/gflags/src/gflags_src-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/home/user/project/database/demo/Proxy/build/vendor/gflags/src/gflags_src-stamp${cfgdir}") # cfgdir has leading slash
endif()
