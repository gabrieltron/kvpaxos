add_library(scheduler)

target_sources(
    scheduler
        PUBLIC
            scheduler.hpp
            partition.hpp
            pattern_tracker.hpp
        PRIVATE
            scheduler.cpp
            partition.cpp
            pattern_tracker.cpp
)

target_include_directories(
    scheduler
        PUBLIC
            "${CMAKE_SOURCE_DIR}/src"
)

target_link_libraries(
    scheduler
        PUBLIC
            evpaxos
            graph
            storage
            request
            types
)
