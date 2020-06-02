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

target_link_libraries(
    request
        PUBLIC
            CONAN_PKG::toml11
)
