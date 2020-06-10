add_library(types INTERFACE)

target_sources(
    types
        INTERFACE
            types.hpp
)

target_link_libraries(
    types
        INTERFACE
            evpaxos
)
