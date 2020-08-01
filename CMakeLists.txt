cmake_minimum_required(VERSION 3.15)
project(kvstore CXX)

include(cmake/base.cmake)
include(cmake/project_options.cmake)
include(cmake/conan.cmake)

conan(
    PACKAGES
        boost/1.72.0
        toml11/3.4.0
        libevent/2.1.11
        tbb/2020.1
)

include(cmake/add_libpaxos.cmake)
include(cmake/add_libkahip.cmake)
include(cmake/add_libmetis.cmake)

add_subdirectory(src)
