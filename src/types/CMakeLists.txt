add_library(types)

target_sources(
    types
        PUBLIC
            types.h
        PRIVATE
            types.cpp
)

target_include_directories(
    types
        PUBLIC
            "${CMAKE_SOURCE_DIR}/src"
)

target_link_libraries(
    types
        PUBLIC
            CONAN_PKG::tbb
            constants
            evpaxos
)
