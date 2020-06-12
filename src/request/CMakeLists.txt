add_library(request)

target_sources(
    request
        PUBLIC
            request_generation.h
            request.hpp
            random.h
        PRIVATE
            request_generation.cpp
            request.cpp
            random.cpp
)

target_include_directories(
    request
        PUBLIC
            "${CMAKE_SOURCE_DIR}/src"
)

target_link_libraries(
    request
        PUBLIC
            CONAN_PKG::toml11
            types
)
