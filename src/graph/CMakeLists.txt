add_library(graph)

target_sources(
    graph
        PUBLIC
            graph.hpp
            partitioning.h
        PRIVATE
            graph.cpp
            partitioning.cpp
)

target_include_directories(
    graph
        PUBLIC
            "${CMAKE_SOURCE_DIR}/src"
)

target_link_libraries(
    graph
        PUBLIC
            CONAN_PKG::tbb
            metis
            kahip
            scheduler
)

target_compile_options(
    graph
        PRIVATE
            -O3
)
