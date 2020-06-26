add_library(graph)

target_sources(
    graph
        PUBLIC
            graph.hpp
        PRIVATE
            graph.cpp
)

target_link_libraries(
    graph
        PRIVATE
            CONAN_PKG::tbb
)
