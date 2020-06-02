cmake_minimum_required(VERSION 3.15)
project(kvstore CXX)

include(cmake/base.cmake)
include(cmake/project_options.cmake)
include(cmake/conan.cmake)

conan(
    PACKAGES
        toml11/3.4.0
        libevent/2.1.11
)

include(cmake/add_libpaxos.cmake)

add_subdirectory(src)
