add_library(request)

target_sources(
    request
        PUBLIC
            request_generation.h
            random.h
        PRIVATE
            request_generation.cpp
            random.cpp
)

target_include_directories(
    request
        PUBLIC
            "${CMAKE_SOURCE_DIR}/src/toml11"
)

target_link_libraries(
    request
        PRIVATE
            toml11
)
