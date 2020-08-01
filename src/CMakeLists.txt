add_subdirectory(compresser)
add_subdirectory(request)
add_subdirectory(types)
add_subdirectory(storage)

add_executable(replica)

target_sources(
    replica
        PRIVATE
            replica.cpp
)

target_link_libraries(
    replica
        PRIVATE
            CONAN_PKG::toml11
            request
            storage
            types
)
