add_library(types)

target_sources(
    types
        PUBLIC
            types.h
        PRIVATE
            types.cpp
)

target_link_libraries(
    types
        PUBLIC
            CONAN_PKG::tbb
)
