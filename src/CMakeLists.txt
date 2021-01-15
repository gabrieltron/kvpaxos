add_subdirectory(compresser)
add_subdirectory(request)
add_subdirectory(evclient)
add_subdirectory(types)
add_subdirectory(storage)
add_subdirectory(scheduler)
add_subdirectory(graph)
add_subdirectory(constants)

add_executable(client)
add_executable(replica)

target_sources(
    client
        PRIVATE
            client.cpp
)

target_link_libraries(
    client
        PRIVATE
            CONAN_PKG::tbb
            CONAN_PKG::toml11
            constants
            request
            evclient
            evpaxos
            types
)

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
            evclient
            evpaxos
            types
            scheduler
)
