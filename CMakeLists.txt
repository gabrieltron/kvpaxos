cmake_minimum_required(VERSION 3.15)
project(kvstore CXX)

#[[
conan(
    PACKAGES
        toml11/3.4.0
)
]]

add_subdirectory(src)
