add_subdirectory(request)
add_subdirectory(evclient)
add_subdirectory(types)

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
            CONAN_PKG::toml11
            request
            evclient
            evpaxos

        INTERFACE
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

        INTERFACE
            types
)
