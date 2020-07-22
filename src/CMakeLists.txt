add_subdirectory(request)
add_subdirectory(evclient)
add_subdirectory(types)
add_subdirectory(storage)
add_subdirectory(scheduler)
add_subdirectory(graph)

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
            evclient
            evpaxos
            types
            scheduler
)
