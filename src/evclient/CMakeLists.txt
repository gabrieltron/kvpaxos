add_library(evclient)

target_sources(
    evclient
        PUBLIC
            evclient.h
        PRIVATE
            evclient.cpp
)

target_include_directories(
    evclient
        PUBLIC
            "${CMAKE_SOURCE_DIR}/src"
)

target_link_libraries(
    evclient
        PRIVATE
            CONAN_PKG::libevent
            evpaxos
            request
        INTERFACE
            types
)
