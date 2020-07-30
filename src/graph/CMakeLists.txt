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

target_link_libraries(
    graph
        PUBLIC
            metis
            kahip
)
