add_library(storage)

target_sources(
    storage
        PUBLIC
            storage.h
        PRIVATE
            storage.cpp
)

target_include_directories(
    storage
        PUBLIC
            "${CMAKE_SOURCE_DIR}/src"
)

target_link_libraries(
    storage
        PRIVATE
            types
)
