add_library(scheduler)

target_sources(
    scheduler
        PUBLIC
            scheduler.hpp
            partition.hpp
        PRIVATE
            scheduler.cpp
            partition.cpp
)

target_include_directories(
    scheduler
        PUBLIC
            "${CMAKE_SOURCE_DIR}/src"
)

target_link_libraries(
    scheduler
        PRIVATE
            evpaxos
            storage
            request
            types
)
